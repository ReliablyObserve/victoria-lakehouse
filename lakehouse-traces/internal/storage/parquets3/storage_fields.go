package parquets3

import (
	"context"
	"fmt"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fetchFooterFile returns a metadata-only *parquet.File for fi. Prefers the
// footer cache; on miss it does a small range read (~16 KB) instead of
// downloading the full file. Falls back to a full-file download only when
// the S3 pool is unavailable or the file is below the prefetch threshold.
//
// Mirrors the helper added to the logs module — used by GetFieldNames
// where only the schema (column names) is needed, not column data. Avoids
// downloading a full ~1 MB parquet file just to read its schema.
func (s *Storage) fetchFooterFile(ctx context.Context, fi manifest.FileInfo) (*parquet.File, error) {
	if s.footerCache != nil {
		if cached, ok := s.footerCache.Get(fi.Key); ok && cached.File != nil {
			return cached.File, nil
		}
	}
	if s.pool == nil || fi.Size < minFileSizeForPrefetch {
		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			return nil, err
		}
		cached, f, err := ParseFooterFromData(fi.Key, data)
		if err != nil {
			return nil, err
		}
		if s.footerCache != nil {
			s.footerCache.Put(fi.Key, cached)
		}
		return f, nil
	}
	offset := fi.Size - footerPrefetchSize
	if offset < 0 {
		offset = 0
	}
	length := fi.Size - offset
	metrics.S3GetsByPhase.Inc("footer")
	tail, err := s.pool.DownloadRangeDedup(ctx, "footer", fi.Key, offset, length)
	if err != nil {
		return nil, fmt.Errorf("download footer range: %w", err)
	}
	if len(tail) < 8 {
		return nil, fmt.Errorf("footer tail too short: %d bytes", len(tail))
	}
	footerLen, err := FooterLength(tail[len(tail)-8:])
	if err != nil {
		return nil, err
	}
	totalFooterBytes := footerLen + 8
	if totalFooterBytes > len(tail) {
		// Two-phase fetch — see internal/storage/parquets3/
		// storage_fields.go for the rationale. Mirrored byte-for-byte
		// per the logs↔traces module parity rule.
		footerOffset := fi.Size - int64(totalFooterBytes)
		if footerOffset < 0 {
			return nil, fmt.Errorf("footer length implies negative offset: footer=%d file=%d", totalFooterBytes, fi.Size)
		}
		metrics.S3GetsByPhase.Inc("footer")
		bigTail, err := s.pool.DownloadRangeDedup(ctx, "footer", fi.Key, footerOffset, int64(totalFooterBytes))
		if err != nil {
			return nil, fmt.Errorf("download oversize footer range: %w", err)
		}
		if len(bigTail) < totalFooterBytes {
			return nil, fmt.Errorf("oversize footer fetch short: got %d, want %d", len(bigTail), totalFooterBytes)
		}
		tail = bigTail
	}
	footerSlice := tail[len(tail)-totalFooterBytes:]
	cached, f, err := ParseFooterFromBytes(fi.Key, footerSlice, fi.Size)
	if err != nil {
		return nil, err
	}
	if s.footerCache != nil {
		s.footerCache.Put(fi.Key, cached)
	}
	return f, nil
}

func (s *Storage) GetFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	filter := parseFilterFromQuery(q)

	// pmeta labels read-flip: catalog field names first (range-aware), labelIndex fallback.
	if filter == nil && s.catalog != nil {
		if names := s.catalogFieldNames(q); len(names) > 0 {
			result := make([]logstorage.ValueWithHits, len(names))
			for i, name := range names {
				result[i] = logstorage.ValueWithHits{Value: name, Hits: 1}
			}
			return result, nil
		}
	}
	if filter == nil && s.labelIndex.Len() > 0 {
		names := s.labelIndex.GetFieldNames()
		result := make([]logstorage.ValueWithHits, len(names))
		for i, name := range names {
			result[i] = logstorage.ValueWithHits{Value: name, Hits: 1}
		}
		return result, nil
	}

	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	// Use a footer-only read instead of downloading the full ~1 MB file
	// just to walk its schema. Matches the logs module's GetFieldNames
	// pattern so behaviour stays consistent across signals.
	fi := files[0]
	f, err := s.fetchFooterFile(ctx, fi)
	if err != nil {
		return nil, fmt.Errorf("get footer: %w", err)
	}

	// Footer-only file: cannot safely scan data pages for distinct
	// values (parquet-go falls back to truncated column-index min/max).
	// Register names; defer value extraction to the query path which
	// has the full file open.
	s.updateLabelIndexNamesOnly(f)

	if s.catalog != nil {
		if names := s.catalogFieldNames(q); len(names) > 0 {
			result := make([]logstorage.ValueWithHits, len(names))
			for i, name := range names {
				result[i] = logstorage.ValueWithHits{Value: name, Hits: 1}
			}
			return result, nil
		}
	}
	if s.labelIndex.Len() > 0 {
		names := s.labelIndex.GetFieldNames()
		result := make([]logstorage.ValueWithHits, len(names))
		for i, name := range names {
			result[i] = logstorage.ValueWithHits{Value: name, Hits: 1}
		}
		return result, nil
	}

	return nil, nil
}

// scanProjectedFieldValues iterates a Parquet file extracting values
// from targetParquetCol for rows matching filter, reading only the
// column chunks needed (target + filter-referenced columns) via
// parquet.NewColumnChunkRowReader.
//
// Mirrors the equivalent helper in the logs module. Combined with
// openParquetFile's range-read path this cuts S3 bytes per file
// from the full body down to (footer + projected column chunk
// sizes) — critical for keeping lakehouse-traces' parallel worker
// pool from amplifying full-file downloads under load.
func (s *Storage) scanProjectedFieldValues(
	ctx context.Context,
	fi manifest.FileInfo,
	targetParquetCol string,
	filter *logstorage.Filter,
	seen map[string]uint64,
) error {
	projectedCols := map[string]bool{targetParquetCol: true}
	if filter != nil {
		for internalName := range FilterReferencedFields(filter) {
			if m := s.registry.ResolveToParquet(internalName); m != nil {
				projectedCols[m.ParquetColumn] = true
			} else {
				projectedCols[internalName] = true
			}
		}
	}

	f, err := s.openParquetFile(ctx, fi, projectedCols)
	if err != nil {
		return err
	}

	fullColNames := columnNames(f.Root())
	projectedIndices := make([]int, 0, len(projectedCols))
	projectedNames := make([]string, 0, len(projectedCols))
	targetInProjection := -1
	for i, n := range fullColNames {
		if !projectedCols[n] {
			continue
		}
		if n == targetParquetCol {
			targetInProjection = len(projectedIndices)
		}
		projectedIndices = append(projectedIndices, i)
		projectedNames = append(projectedNames, n)
	}
	if targetInProjection < 0 {
		return nil
	}

	buf := make([]parquet.Row, 256)
	for _, rg := range f.RowGroups() {
		allChunks := rg.ColumnChunks()
		projChunks := make([]parquet.ColumnChunk, 0, len(projectedIndices))
		for _, ci := range projectedIndices {
			if ci >= len(allChunks) {
				continue
			}
			projChunks = append(projChunks, allChunks[ci])
		}
		if len(projChunks) == 0 {
			continue
		}
		rows := parquet.NewColumnChunkRowReader(projChunks)
		for {
			n, err := rows.ReadRows(buf)
			if n > 0 {
				collectFilteredValues(buf[:n], projectedNames, targetInProjection, filter, s, seen)
			}
			if err != nil {
				break
			}
		}
		_ = rows.Close()
	}
	return nil
}

func (s *Storage) GetFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	filter := parseFilterFromQuery(q)

	// pmeta catalog fast-path (--pmeta): union the field's values across the
	// partitions in the query's time range, served from RAM. nil (flag off) or
	// empty (cold) falls through to the labelIndex/scan path unchanged.
	// A no-limit request (limit==0) MUST still use the in-RAM index — it is
	// self-bounded, so this is correct and avoids a full scan. See the logs-module
	// comment: gating on `limit > 0` was the dropdown slowness.
	if filter == nil && s.catalog != nil {
		if s.refuseEnumeration(fieldName) {
			return nil, nil // declared id column: don't enumerate (matches VT), no scan
		}
		if result := s.catalogFieldValues(q, fieldName, limit); len(result) > 0 {
			return result, nil
		}
	}

	if filter == nil && s.labelIndex.Len() > 0 {
		vals := s.labelIndex.GetFieldValues(fieldName, limit)
		if len(vals) == 0 {
			if m := s.registry.ResolveToParquet(fieldName); m != nil && m.InternalName != fieldName {
				vals = s.labelIndex.GetFieldValues(m.InternalName, limit)
			}
		}
		if len(vals) == 0 {
			if m := s.registry.ResolveFromParquet(fieldName); m != nil && m.InternalName != fieldName {
				vals = s.labelIndex.GetFieldValues(m.InternalName, limit)
			}
		}
		if len(vals) > 0 {
			result := make([]logstorage.ValueWithHits, len(vals))
			for i, v := range vals {
				result[i] = logstorage.ValueWithHits{Value: v, Hits: 1}
			}
			return result, nil
		}
	}

	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	mapping := s.registry.ResolveToParquet(fieldName)
	if mapping == nil {
		mapping = s.registry.ResolveFromParquet(fieldName)
	}
	if mapping == nil {
		return nil, nil
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	fileWorkers := s.cfg.Query.FileWorkers
	if fileWorkers <= 0 {
		fileWorkers = 8
	}
	if fileWorkers > len(files) {
		fileWorkers = len(files)
	}

	var mu sync.Mutex
	seen := make(map[string]uint64)

	taskCh := make(chan manifest.FileInfo, len(files))
	for _, fi := range files {
		taskCh <- fi
	}
	close(taskCh)

	var wg sync.WaitGroup
	for i := 0; i < fileWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range taskCh {
				if ctx.Err() != nil {
					return
				}

				// Column-projected read: fetches only (target + filter cols)
				// chunk data from S3 rather than the entire file body.
				localSeen := make(map[string]uint64)
				if err := s.scanProjectedFieldValues(ctx, fi, mapping.ParquetColumn, filter, localSeen); err != nil {
					logger.Warnf("scan projected field values: %s; key=%s", err, fi.Key)
					continue
				}

				mu.Lock()
				for k, v := range localSeen {
					seen[k] += v
				}
				limitReached := limit > 0 && uint64(len(seen)) >= limit
				mu.Unlock()

				if limitReached {
					cancel()
					return
				}
			}
		}()
	}
	wg.Wait()

	result := make([]logstorage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		result = append(result, logstorage.ValueWithHits{Value: v, Hits: hits})
	}
	if limit > 0 && uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *Storage) GetStreamFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	streamFields := s.registry.StreamFields()
	result := make([]logstorage.ValueWithHits, 0, len(streamFields))
	for _, name := range streamFields {
		result = append(result, logstorage.ValueWithHits{Value: name, Hits: 1})
	}
	return result, nil
}

func (s *Storage) GetStreamFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return s.GetFieldValues(ctx, tenantIDs, q, fieldName, limit)
}

func (s *Storage) GetStreams(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	filter := parseFilterFromQuery(q)

	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	streamColName := "_stream"
	if m := s.registry.ResolveToParquet(streamColName); m != nil {
		streamColName = m.ParquetColumn
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if err := s.scanProjectedFieldValues(ctx, fi, streamColName, filter, seen); err != nil {
			logger.Warnf("scan projected streams: %s; key=%s", err, fi.Key)
			continue
		}

		if limit > 0 && uint64(len(seen)) >= limit {
			break
		}
	}

	result := make([]logstorage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		result = append(result, logstorage.ValueWithHits{Value: v, Hits: hits})
	}
	if limit > 0 && uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *Storage) GetStreamIDs(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	filter := parseFilterFromQuery(q)

	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	colName := "_stream_id"
	if m := s.registry.ResolveToParquet(colName); m != nil {
		colName = m.ParquetColumn
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		if err := s.scanProjectedFieldValues(ctx, fi, colName, filter, seen); err != nil {
			logger.Warnf("scan projected stream_ids: %s; key=%s", err, fi.Key)
			continue
		}

		if limit > 0 && uint64(len(seen)) >= limit {
			break
		}
	}

	result := make([]logstorage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		result = append(result, logstorage.ValueWithHits{Value: v, Hits: hits})
	}
	if limit > 0 && uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

// collectFilteredValues collects values from targetColIdx for rows that match the filter.
// Uses VL's Filter.MatchRow() for full LogsQL evaluation.
// When filter is nil, all rows contribute values (no filtering).
func collectFilteredValues(rows []parquet.Row, colNames []string, targetColIdx int, filter *logstorage.Filter, s *Storage, seen map[string]uint64) {
	var targetMapping *schema.FieldMapping
	if s != nil && targetColIdx >= 0 && targetColIdx < len(colNames) {
		targetMapping = s.registry.ResolveFromParquet(colNames[targetColIdx])
	}
	formatTarget := func(v parquet.Value) string {
		if targetMapping != nil {
			return targetMapping.Type.FormatValue(parquetValueToAny(v))
		}
		return valueToString(v)
	}

	if filter == nil {
		for _, row := range rows {
			if targetColIdx < len(row) {
				val := formatTarget(row[targetColIdx])
				if val != "" {
					seen[val]++
				}
			}
		}
		return
	}

	tsColIdx := -1
	for i, name := range colNames {
		if name == "timestamp_unix_nano" {
			tsColIdx = i
			break
		}
	}

	for _, row := range rows {
		fields := parquetRowToFields(row, colNames, tsColIdx, s)
		if filter.MatchRow(fields) {
			if targetColIdx < len(row) {
				val := formatTarget(row[targetColIdx])
				if val != "" {
					seen[val]++
				}
			}
		}
	}
}

// parquetRowToFields converts a raw Parquet row to []logstorage.Field
// for VL filter matching. For TracesProfile columns whose internal
// alias differs from the parquet column name (e.g. parquet
// `service.name` aliases to internal `resource_attr:service.name`),
// we MUST emit the value under BOTH names — otherwise a user
// filter like `service.name:="api-gateway"` looks up a field that
// the filter sees as missing and matches no rows. That's the
// silent-empty path behind every `field_values` / Jaeger search
// query that filtered on service/resource attributes returning 0
// on cold while hot VT (which doesn't alias) worked. Duplicating
// the field is cheap (the slice already lives for the row's
// lifetime) and the VL filter walks fields by name, so the extra
// entry just gives both spellings a chance to match.
func parquetRowToFields(row parquet.Row, colNames []string, tsColIdx int, s *Storage) []logstorage.Field {
	fields := make([]logstorage.Field, 0, len(colNames)*2)
	for i, name := range colNames {
		if i >= len(row) {
			break
		}
		internalName := name
		var val string
		if s != nil {
			if m := s.registry.ResolveFromParquet(name); m != nil {
				internalName = m.InternalName
				native := parquetValueToAny(row[i])
				val = m.Type.FormatValue(native)
			} else {
				val = valueToString(row[i])
			}
		} else {
			val = valueToString(row[i])
		}
		fields = append(fields, logstorage.Field{Name: internalName, Value: val})
		if internalName != name {
			// Same value under the parquet column name so a filter
			// written in either dialect matches. The duplicate is a
			// no-op for the schema that doesn't alias (logs side).
			fields = append(fields, logstorage.Field{Name: name, Value: val})
		}
	}
	return fields
}
