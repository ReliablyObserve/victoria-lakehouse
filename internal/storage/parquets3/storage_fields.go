package parquets3

import (
	"context"
	"fmt"

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
// Used by GetFieldNames where we only need the schema/column-index from
// the footer, not the column data — avoiding the previous behaviour of
// downloading every file in the manifest in full just to read the schema.
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
		// Two-phase fetch: the prefetch tail wasn't large enough to
		// hold the whole footer (typical for trace parquet files whose
		// embedded `_trace_idx` key-value metadata can run multiple
		// megabytes). We now know the exact footer size from the
		// trailer we already read, so issue a single targeted range
		// download. Previously we returned an error here, which
		// cascaded into trace-by-ID fallback-to-full-scan and the
		// observed 5-10 s Grafana timeouts.
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

	startNs, endNs := q.GetFilterTimeRange()
	files := s.manifest.GetFilesForRange(startNs, endNs)

	// Aggregate actual non-null row counts across candidate files.
	// Previously this returned Hits=1 for every field — a stub that fed
	// inaccurate cardinality estimates downstream. We compute per-field
	// hit counts from the Parquet column index without reading data
	// pages: (rowGroupNumRows - nullCount) per row group, summed.
	hits := make(map[string]uint64)

	if len(files) == 0 {
		// No catalog consult here: catalogFieldNames unions over the SAME
		// (empty) file range, so it can never return names in this branch —
		// only the range-independent labelIndex can.
		if filter == nil && s.labelIndex.Len() > 0 {
			return labelIndexNamesWithHits(s.labelIndex.GetFieldNames(), nil), nil
		}
		return nil, nil
	}
	files = dedupOverlappingFiles(files)

	// Pre-warm the footer cache in parallel using small range reads
	// (~16 KB per file) so the sequential loop below hits the cache.
	if s.pool != nil && s.footerCache != nil {
		prefetchFooters(ctx, s.pool, files, s.footerCache, 16)
	}

	// Walk all files; for each, accumulate hits per (internal) field name.
	// fetchFooterFile uses the cache populated above; on a miss it falls
	// back to a single-file footer fetch rather than a full-file download.
	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		f, err := s.fetchFooterFile(ctx, fi)
		if err != nil {
			logger.Warnf("get footer for field names: %s; key=%s", err, fi.Key)
			continue
		}
		// Footer-only file: cannot safely scan data pages for distinct
		// values (parquet-go falls back to truncated column-index min/max).
		// Register names; defer value extraction to the query path which
		// has the full file open.
		s.updateLabelIndexNamesOnly(f)
		s.accumulateFieldHits(f, hits)
	}

	if len(hits) > 0 {
		result := make([]logstorage.ValueWithHits, 0, len(hits))
		for name, n := range hits {
			result = append(result, logstorage.ValueWithHits{Value: name, Hits: n})
		}
		return result, nil
	}

	// Fall back to field names (emitted with Hits=0 to signal "unknown count")
	// so callers that only want names still see them. pmeta read-flip: catalog
	// first, legacy labelIndex second.
	if s.catalog != nil {
		if names := s.catalogFieldNames(q); len(names) > 0 {
			return labelIndexNamesWithHits(names, hits), nil
		}
	}
	if s.labelIndex.Len() > 0 {
		return labelIndexNamesWithHits(s.labelIndex.GetFieldNames(), hits), nil
	}
	return nil, nil
}

// labelIndexNamesWithHits returns each name as a ValueWithHits, populating
// Hits from the provided map (or 0 if absent).
func labelIndexNamesWithHits(names []string, hits map[string]uint64) []logstorage.ValueWithHits {
	result := make([]logstorage.ValueWithHits, len(names))
	for i, name := range names {
		result[i] = logstorage.ValueWithHits{Value: name, Hits: hits[name]}
	}
	return result
}

// dedupOverlappingFiles removes manifest entries that are redundant
// because a higher-level compacted file already covers the same time
// range. Without this, GetFieldValues (and other manifest-walking field
// APIs) inflate value counts by ~2x during the brief overlap window
// between a freshly-compacted output and its still-listed sources.
//
// Heuristic:
//   - For every pair (A, B), if A and B overlap on >= 90% of B's time
//     range AND A has a higher CompactionLevel than B (or equal level
//     with strictly larger Size), B is dropped.
//   - Disjoint or partially overlapping files are preserved.
//
// This is intentionally conservative: equal-level overlaps without a
// size signal are kept (rare; usually only happens for sibling files
// produced by the same compaction round).
func dedupOverlappingFiles(files []manifest.FileInfo) []manifest.FileInfo {
	if len(files) <= 1 {
		return files
	}
	drop := make([]bool, len(files))
	for i := range files {
		if drop[i] {
			continue
		}
		for j := range files {
			if i == j || drop[j] {
				continue
			}
			if shouldDropBecauseCoveredBy(files[j], files[i]) {
				drop[j] = true
			}
		}
	}
	result := files[:0]
	for i, fi := range files {
		if !drop[i] {
			result = append(result, fi)
		}
	}
	return result
}

// shouldDropBecauseCoveredBy reports whether `b` is redundant given the
// presence of `a` — i.e. `a` is the compacted output that subsumes `b`.
func shouldDropBecauseCoveredBy(b, a manifest.FileInfo) bool {
	if a.MinTimeNs == 0 || a.MaxTimeNs == 0 || b.MinTimeNs == 0 || b.MaxTimeNs == 0 {
		return false
	}
	bRange := b.MaxTimeNs - b.MinTimeNs
	if bRange <= 0 {
		return false
	}
	overlapStart := b.MinTimeNs
	if a.MinTimeNs > overlapStart {
		overlapStart = a.MinTimeNs
	}
	overlapEnd := b.MaxTimeNs
	if a.MaxTimeNs < overlapEnd {
		overlapEnd = a.MaxTimeNs
	}
	overlap := overlapEnd - overlapStart
	if overlap <= 0 {
		return false
	}
	// Require >= 90% of B to be inside A.
	if overlap*10 < bRange*9 {
		return false
	}
	// Prefer higher compaction level; if equal, prefer the strictly
	// larger file (the merged output).
	if a.CompactionLevel > b.CompactionLevel {
		return true
	}
	if a.CompactionLevel == b.CompactionLevel && a.Size > b.Size {
		return true
	}
	return false
}

// accumulateFieldHits computes per-field non-null row counts for every
// top-level column in f and adds them into the hits map keyed by the
// registry's internal field name.
//
// Uses the Parquet column index — for each row group and column we sum
// (numRows - nullCount) across pages without reading data pages.
// If the column index is unavailable we fall back to NumValues - 0
// (assumes no nulls), which over-counts but never under-counts.
func (s *Storage) accumulateFieldHits(f *parquet.File, hits map[string]uint64) {
	rgs := f.RowGroups()
	if len(rgs) == 0 {
		return
	}
	names := columnNames(f.Root())
	for ci, parquetName := range names {
		internal := parquetName
		if m := s.registry.ResolveFromParquet(parquetName); m != nil {
			internal = m.InternalName
		}
		var nonNull int64
		for _, rg := range rgs {
			cols := rg.ColumnChunks()
			if ci >= len(cols) {
				continue
			}
			cidx, err := cols[ci].ColumnIndex()
			if err != nil || cidx == nil {
				// No column index — credit the entire chunk as non-null.
				// Slight over-count is preferable to silent under-count.
				nonNull += cols[ci].NumValues()
				continue
			}
			pageCount := cidx.NumPages()
			if pageCount == 0 {
				nonNull += cols[ci].NumValues()
				continue
			}
			var nulls int64
			for p := 0; p < pageCount; p++ {
				nulls += cidx.NullCount(p)
			}
			n := cols[ci].NumValues() - nulls
			if n < 0 {
				n = 0
			}
			nonNull += n
		}
		if nonNull > 0 {
			hits[internal] += uint64(nonNull)
		}
	}
}

// scanProjectedFieldValues iterates a Parquet file extracting values
// from targetParquetCol for rows matching filter, reading only the
// column chunks needed (target + filter-referenced columns) via
// parquet.NewColumnChunkRowReader.
//
// Combined with openParquetFile's range-read path this cuts S3 bytes
// per file from the full body (~hundreds of KB) down to roughly
// (footer + sum of projected column chunk sizes) — typically 30-80 KB
// per file for a 2-column projection over an 8-column schema. The
// savings compound at scale: hundreds of files × hundreds of KB
// saved each = the difference between a healthy lakehouse-logs and
// an OOM-killed one under Grafana drilldown load.
//
// On any error opening the file or finding the target column, returns
// nil so the caller continues with the next file (matches the
// fault-tolerance of the previous full-download path).
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

	// Plan-then-fetch (S3 Tier-2): this path reads EVERY row group's
	// projected chunks, so the plan covers all row groups — armed right
	// after open, before the chunk readers below issue any page reads.
	f, planned, err := s.openParquetFileWithPlan(ctx, fi, projectedCols)
	if err != nil {
		return err
	}
	if planned != nil {
		defer func() { _ = planned.Close() }()
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
		// Target column not present in this file — nothing to collect.
		return nil
	}

	if planned != nil {
		rgIdxs := make([]int, len(f.RowGroups()))
		for i := range rgIdxs {
			rgIdxs[i] = i
		}
		s.armProjectedPlan(ctx, planned, f, rgIdxs, projectedCols, nil)
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
	//
	// NOTE: a no-limit request (limit==0, what a Grafana dropdown sends) MUST still
	// use the in-RAM index — both the catalog and the labelIndex are self-bounded,
	// so serving from them is correct AND avoids a full column scan (which, with S3
	// latency, takes tens of seconds). Gating this on `limit > 0` was the dropdown
	// slowness: it sent every no-limit field_values straight to the scan path.
	if filter == nil && s.catalog != nil {
		if s.refuseEnumeration(fieldName) {
			return nil, nil // declared id column: don't enumerate (matches VL), no scan
		}
		if result := s.catalogFieldValues(q, fieldName, limit); len(result) > 0 {
			return result, nil
		}
	}

	if filter == nil && s.labelIndex.Len() > 0 {
		vals := s.labelIndex.GetFieldValues(fieldName, limit)
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
	// Drop pre-compaction sources whose contents are already in a higher-
	// level merged file to avoid double-counting field values.
	files = dedupOverlappingFiles(files)

	mapping := s.registry.ResolveToParquet(fieldName)
	if mapping == nil {
		mapping = s.registry.ResolveFromParquet(fieldName)
	}
	if mapping == nil {
		return nil, nil
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Column-projected read: fetches only (target + filter cols)
		// chunk data from S3 rather than the entire file body.
		if err := s.scanProjectedFieldValues(ctx, fi, mapping.ParquetColumn, filter, seen); err != nil {
			logger.Warnf("scan projected field values: %s; key=%s", err, fi.Key)
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
	files = dedupOverlappingFiles(files)

	streamColName := "_stream"
	if m := s.registry.ResolveToParquet(streamColName); m != nil {
		streamColName = m.ParquetColumn
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Column-projected read: fetches only (_stream + filter cols)
		// chunk data from S3 rather than the entire file body.
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
	files = dedupOverlappingFiles(files)

	colName := "_stream_id"
	if m := s.registry.ResolveToParquet(colName); m != nil {
		colName = m.ParquetColumn
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Column-projected read: fetches only (_stream_id + filter cols)
		// chunk data from S3 rather than the entire file body.
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

// parquetRowToFields converts a raw Parquet row to []logstorage.Field for VL filter matching.
func parquetRowToFields(row parquet.Row, colNames []string, tsColIdx int, s *Storage) []logstorage.Field {
	fields := make([]logstorage.Field, 0, len(colNames))
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
	}
	return fields
}
