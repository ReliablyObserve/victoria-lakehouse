package parquets3

import (
	"bytes"
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

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
		if filter == nil && s.labelIndex.Len() > 0 {
			return labelIndexNamesWithHits(s.labelIndex.GetFieldNames(), nil), nil
		}
		return nil, nil
	}
	files = dedupOverlappingFiles(files)

	// Walk all files; for each, accumulate hits per (internal) field name.
	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			logger.Warnf("get file data for field names: %s; key=%s", err, fi.Key)
			continue
		}
		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			logger.Warnf("open parquet for field names: %s; key=%s", err, fi.Key)
			continue
		}
		s.updateLabelIndex(f)
		s.accumulateFieldHits(f, hits)
	}

	if len(hits) > 0 {
		result := make([]logstorage.ValueWithHits, 0, len(hits))
		for name, n := range hits {
			result = append(result, logstorage.ValueWithHits{Value: name, Hits: n})
		}
		return result, nil
	}

	// Fall back to label index names (still emitted with Hits=0 to signal
	// "unknown count") so callers that only want field names still see them.
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

func (s *Storage) GetFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	filter := parseFilterFromQuery(q)

	if filter == nil && limit > 0 && s.labelIndex.Len() > 0 {
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

		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			logger.Warnf("get file data for field values: %s; key=%s", err, fi.Key)
			continue
		}

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			logger.Warnf("open parquet for field values: %s; key=%s", err, fi.Key)
			continue
		}

		s.updateLabelIndex(f)

		colNames := columnNames(f.Root())
		colIdx := findColumnIndex(f.Root(), mapping.ParquetColumn)
		if colIdx < 0 {
			continue
		}

		for _, rg := range f.RowGroups() {
			rows := rg.Rows()
			buf := make([]parquet.Row, 256)
			for {
				n, err := rows.ReadRows(buf)
				if n > 0 {
					collectFilteredValues(buf[:n], colNames, colIdx, filter, s, seen)
				}
				if err != nil {
					break
				}
			}
			_ = rows.Close()
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

		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			logger.Warnf("get file data for streams: %s; key=%s", err, fi.Key)
			continue
		}

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			logger.Warnf("open parquet for streams: %s; key=%s", err, fi.Key)
			continue
		}

		colNames := columnNames(f.Root())
		streamIdx := findColumnIndex(f.Root(), streamColName)
		if streamIdx < 0 {
			continue
		}

		for _, rg := range f.RowGroups() {
			rows := rg.Rows()
			buf := make([]parquet.Row, 256)
			for {
				n, err := rows.ReadRows(buf)
				if n > 0 {
					collectFilteredValues(buf[:n], colNames, streamIdx, filter, s, seen)
				}
				if err != nil {
					break
				}
			}
			_ = rows.Close()
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

		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			logger.Warnf("get file data for stream_ids: %s; key=%s", err, fi.Key)
			continue
		}

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			logger.Warnf("open parquet for stream_ids: %s; key=%s", err, fi.Key)
			continue
		}

		colNames := columnNames(f.Root())
		colIdx := findColumnIndex(f.Root(), colName)
		if colIdx < 0 {
			continue
		}

		for _, rg := range f.RowGroups() {
			rows := rg.Rows()
			buf := make([]parquet.Row, 256)
			for {
				n, err := rows.ReadRows(buf)
				if n > 0 {
					collectFilteredValues(buf[:n], colNames, colIdx, filter, s, seen)
				}
				if err != nil {
					break
				}
			}
			_ = rows.Close()
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
