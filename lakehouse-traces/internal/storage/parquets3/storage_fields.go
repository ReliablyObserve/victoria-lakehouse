package parquets3

import (
	"context"
	"fmt"
	"math"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// pageIndexLookBehind is how far before an oversize footer the two-phase footer
// fetch reads, to pick up the page-index stripe in the same request. Measured
// stripe sizes are 3.4 KB (logs, flush-sized objects) and 6.4 KB (traces,
// compacted objects); 32 KB leaves room for wide row-group counts. A larger
// stripe falls back to one extra range GET.
var pageIndexLookBehind = int64(32 << 10)

// fetchFooterFile returns a metadata-only *parquet.File for fi. Prefers the
// footer cache; on miss it does a small range read (~16 KB) instead of
// downloading the full file. Falls back to a full-file download only when
// the S3 pool is unavailable or the file is below the prefetch threshold.
//
// Mirrors the helper added to the logs module — used by GetFieldNames
// where only the schema (column names) is needed, not column data. Avoids
// downloading a full ~1 MB parquet file just to read its schema.
func (s *Storage) fetchFooterFile(ctx context.Context, fi manifest.FileInfo) (*parquet.File, error) {
	cached, err := s.fetchFooterEntry(ctx, fi)
	if err != nil {
		return nil, err
	}
	return cached.File, nil
}

// fetchFooterEntry is fetchFooterFile returning the cache entry, so a caller
// can tell whether it holds the page index (CachedFooter.HasPageIndex).
func (s *Storage) fetchFooterEntry(ctx context.Context, fi manifest.FileInfo) (*CachedFooter, error) {
	if s.footerCache != nil {
		if cached, ok := s.footerCache.GetFor(fi.Key, fi.Size); ok && cached.File != nil {
			return cached, nil
		}
	}
	if s.pool == nil || fi.Size < minFileSizeForPrefetch {
		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			return nil, err
		}
		cached, _, fresh, err := parseObjectFor(s.footerCache, fi.Key, data)
		if err != nil {
			return nil, err
		}
		if fresh && s.footerCache != nil {
			s.footerCache.Put(fi.Key, cached)
		}
		return cached, nil
	}
	cached, _, err := s.fetchFooterTail(ctx, fi, s.pool.DownloadRangeDedup)
	return cached, err
}

// rangeDownloader is the ranged object read the footer fetch is built on:
// DownloadRangeDedup shares concurrent identical reads (and outlives its
// caller), DownloadRange stops with the caller's context.
type rangeDownloader func(ctx context.Context, kind, key string, offset, length int64) ([]byte, error)

// fetchFooterTail reads the footer of fi with a ranged read of the object's
// tail (the whole object when it is smaller than the tail), then a second,
// exact-length ranged read when the footer is larger than the tail (typical for
// trace objects with a large `_trace_idx` key-value), parses it, and caches it.
// It does not consult the cache and never downloads the whole object of a
// large file. Shared by fetchFooterFile and the exact-bounds resolution.
// Twin of internal/storage/parquets3/storage_fields.go.
func (s *Storage) fetchFooterTail(ctx context.Context, fi manifest.FileInfo, dl rangeDownloader) (*CachedFooter, *parquet.File, error) {
	offset := fi.Size - footerPrefetchTail(s.footerPrefetchBytes(), fi.Size)
	if offset < 0 {
		offset = 0
	}
	length := fi.Size - offset
	metrics.S3GetsByPhase.Inc("footer")
	tail, err := dl(ctx, "footer", fi.Key, offset, length)
	if err != nil {
		return nil, nil, fmt.Errorf("download footer range: %w", err)
	}
	if len(tail) < 8 {
		return nil, nil, fmt.Errorf("footer tail too short: %d bytes", len(tail))
	}
	footerLen, err := FooterLength(tail[len(tail)-8:])
	if err != nil {
		return nil, nil, err
	}
	totalFooterBytes := footerLen + 8
	tailOff := offset
	if totalFooterBytes > len(tail) {
		// Two-phase fetch — see internal/storage/parquets3/
		// storage_fields.go for the rationale. Mirrored byte-for-byte
		// per the logs↔traces module parity rule.
		footerOffset := fi.Size - int64(totalFooterBytes)
		if footerOffset < 0 {
			return nil, nil, fmt.Errorf("footer length implies negative offset: footer=%d file=%d", totalFooterBytes, fi.Size)
		}
		// Read pageIndexLookBehind bytes ahead of the footer in the same range:
		// the page-index stripe sits right before it, so a stripe that fits
		// the look-behind costs no third round trip.
		fetchOff := footerOffset - min(footerOffset, pageIndexLookBehind)
		metrics.S3GetsByPhase.Inc("footer")
		bigTail, err := dl(ctx, "footer", fi.Key, fetchOff, fi.Size-fetchOff)
		if err != nil {
			return nil, nil, fmt.Errorf("download oversize footer range: %w", err)
		}
		if int64(len(bigTail)) < fi.Size-fetchOff {
			return nil, nil, fmt.Errorf("oversize footer fetch short: got %d, want %d", len(bigTail), fi.Size-fetchOff)
		}
		tail, tailOff = bigTail, fetchOff
	}
	// cacheFooterFromTail keeps the page-index stripe with the footer (one
	// extra range GET when the stripe lies before the fetched tail, as it does
	// for a footer that did not fit the prefetch range).
	cached, f, err := cacheFooterFromTail(ctx, dl, fi.Key, tail, tailOff, fi.Size)
	if err != nil {
		return nil, nil, err
	}
	if s.footerCache != nil {
		s.footerCache.Put(fi.Key, cached)
	}
	return cached, f, nil
}

func (s *Storage) GetFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	filter := parseFilterFromQuery(q)
	scope := scopeFor(ctx, tenantIDs)

	// pmeta labels read-flip: catalog field names first (range-aware), labelIndex fallback.
	if filter == nil && s.catalog != nil {
		if names := s.catalogFieldNames(q, scope); len(names) > 0 {
			result := make([]logstorage.ValueWithHits, len(names))
			for i, name := range names {
				result[i] = logstorage.ValueWithHits{Value: name, Hits: 1}
			}
			return result, nil
		}
	}
	startNs, endNs := q.GetFilterTimeRange()

	// A window holding no objects has no field names, as on VictoriaTraces.
	// The in-memory label index is not time-scoped — it remembers names from
	// any hour — so it is consulted only below, for a window that has objects
	// whose footers could not name their columns.
	files := s.filesForScope("field_names", startNs, endNs, scope)
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
		if names := s.catalogFieldNames(q, scope); len(names) > 0 {
			result := make([]logstorage.ValueWithHits, len(names))
			for i, name := range names {
				result[i] = logstorage.ValueWithHits{Value: name, Hits: 1}
			}
			return result, nil
		}
	}
	if s.labelIndex.Len() > 0 && s.tenantScopeAllowsGlobalIndex(scope) {
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
	tombstones []tombstone,
	seen map[string]uint64,
	startNs, endNs int64,
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
	// A tombstone predicate can only be evaluated against columns that are in
	// the projection — including the timestamp it is bounded by.
	s.addTombstoneProjection(tombstones, projectedCols)

	// The scan reads whole row groups, and a file can straddle the query
	// window: a row outside the window must contribute neither a value nor a
	// hit. The filter handed in bounds nothing on its own when the query is
	// unfiltered or time-only (parseFilterFromQuery returns nil then), and an
	// unfiltered scan still materialises rows while a tombstone is active. A
	// file wholly inside the window needs no per-row check, so the timestamp
	// column is projected only when it is needed.
	winLo, winHi := int64(math.MinInt64), int64(math.MaxInt64)
	if !fileWithinWindow(fi, startNs, endNs) {
		winLo, winHi = startNs, endNs
		projectedCols[timestampColumn] = true
	}

	// Plan-then-fetch (S3 Tier-2): the plan covers the row groups the window
	// reaches — armed right after open, before the chunk readers below issue
	// any page reads.
	// Mirror of the logs module.
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
		return nil
	}

	// A compacted object spans far more time than a narrow window: only the
	// row groups whose timestamps reach the window are planned and read.
	rgIdxs := windowRowGroups(f, winLo, winHi)
	if len(rgIdxs) == 0 {
		return nil
	}
	if planned != nil {
		s.armProjectedPlan(ctx, planned, f, rgIdxs, projectedCols, nil)
	}

	buf := make([]parquet.Row, 256)
	rowGroups := f.RowGroups()
	for _, ri := range rgIdxs {
		rg := rowGroups[ri]
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
				collectFilteredValuesInWindow(buf[:n], projectedNames, targetInProjection, filter, tombstones, s, seen, winLo, winHi)
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
	scope := scopeFor(ctx, tenantIDs)
	startNs, endNs := q.GetFilterTimeRange()

	// Neither the in-RAM label index (a sample, not time-scoped) nor the pmeta
	// catalog (value sets without counts: every value got hits=1) can answer
	// this exactly; see the logs-module comment. Exact per-file counts come
	// from the manifest's label aggregates instead (collectFieldValues).
	if filter == nil && s.refuseEnumeration(fieldName) {
		return nil, nil // declared id column: don't enumerate (matches VT), no scan
	}

	files := s.filesForScope("field_values", startNs, endNs, scope)
	// No early return on an empty object list: a window nothing has been
	// flushed for yet is answered from the unflushed rows alone.

	// A scan reads every row of every overlapping file, including the rows
	// that lie outside the query window, so it must apply every tombstone
	// overlapping those files — not only the ones overlapping the window. A
	// file a tombstone of its tenant touches is never answered from its
	// aggregate, which predates the delete.
	spanLo, spanHi := filesTimeSpan(files, startNs, endNs)
	tombstones := s.fieldsTombstones(scope, spanLo, spanHi)
	if len(tombstones) > 0 {
		noteFieldsScanFallback("field_values")
	}

	mapping := s.registry.ResolveToParquet(fieldName)
	if mapping == nil {
		mapping = s.registry.ResolveFromParquet(fieldName)
	}
	if mapping == nil {
		return nil, nil
	}

	seen, err := s.collectFieldValues(ctx, files, fieldValuesRequest{
		op:         "field values",
		column:     mapping.ParquetColumn,
		field:      mapping.InternalName,
		tenantIDs:  tenantIDs,
		query:      q,
		aggregates: true,
		filter:     filter,
		tombstones: tombstones,
		parse:      s.keyTenantParser(),
		startNs:    startNs,
		endNs:      endNs,
	})
	if err != nil {
		return nil, err
	}
	return valuesWithHits(seen, limit), nil
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

	files := s.filesForTenants(ctx, "streams", startNs, endNs, tenantIDs)
	scope := scopeFor(ctx, tenantIDs)
	// No early return on an empty object list: a window nothing has been
	// flushed for yet is answered from the unflushed rows alone.

	// Whole files are scanned: apply every tombstone overlapping their rows,
	// not only those overlapping the query window.
	spanLo, spanHi := filesTimeSpan(files, startNs, endNs)
	tombstones := s.fieldsTombstones(scope, spanLo, spanHi)
	// Each object gets only the tombstones of its own tenant.
	parse := s.keyTenantParser()
	if len(tombstones) > 0 {
		noteFieldsScanFallback("streams")
	}

	streamColName := "_stream"
	if m := s.registry.ResolveToParquet(streamColName); m != nil {
		streamColName = m.ParquetColumn
	}

	seen, err := s.collectFieldValues(ctx, files, fieldValuesRequest{
		op:         "streams",
		column:     streamColName,
		field:      "_stream",
		tenantIDs:  tenantIDs,
		query:      q,
		filter:     filter,
		tombstones: tombstones,
		parse:      parse,
		startNs:    startNs,
		endNs:      endNs,
	})
	if err != nil {
		return nil, err
	}
	return valuesWithHits(seen, limit), nil
}

func (s *Storage) GetStreamIDs(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	filter := parseFilterFromQuery(q)

	startNs, endNs := q.GetFilterTimeRange()

	files := s.filesForTenants(ctx, "stream_ids", startNs, endNs, tenantIDs)
	scope := scopeFor(ctx, tenantIDs)
	// No early return on an empty object list: a window nothing has been
	// flushed for yet is answered from the unflushed rows alone.

	// Whole files are scanned: apply every tombstone overlapping their rows,
	// not only those overlapping the query window.
	spanLo, spanHi := filesTimeSpan(files, startNs, endNs)
	tombstones := s.fieldsTombstones(scope, spanLo, spanHi)
	// Each object gets only the tombstones of its own tenant.
	parse := s.keyTenantParser()
	if len(tombstones) > 0 {
		noteFieldsScanFallback("stream_ids")
	}

	colName := "_stream_id"
	if m := s.registry.ResolveToParquet(colName); m != nil {
		colName = m.ParquetColumn
	}

	seen, err := s.collectFieldValues(ctx, files, fieldValuesRequest{
		op:         "stream_ids",
		column:     colName,
		field:      "_stream_id",
		tenantIDs:  tenantIDs,
		query:      q,
		filter:     filter,
		tombstones: tombstones,
		parse:      parse,
		startNs:    startNs,
		endNs:      endNs,
	})
	if err != nil {
		return nil, err
	}
	return valuesWithHits(seen, limit), nil
}

// collectFilteredValues collects values from targetColIdx for rows that match the filter.
// Uses VL's Filter.MatchRow() for full LogsQL evaluation.
// When filter is nil, all rows contribute values (no filtering).
func collectFilteredValues(rows []parquet.Row, colNames []string, targetColIdx int, filter *logstorage.Filter, tombstones []tombstone, s *Storage, seen map[string]uint64) {
	collectFilteredValuesInWindow(rows, colNames, targetColIdx, filter, tombstones, s, seen, math.MinInt64, math.MaxInt64)
}

// collectFilteredValuesInWindow is collectFilteredValues restricted to rows
// whose timestamp lies in [startNs, endNs] (inclusive, as the query's time
// filter). An unbounded window (MinInt64, MaxInt64) skips the per-row check;
// a bounded one needs the timestamp column among colNames — a row whose
// timestamp is not projected counts as outside a bounded window.
func collectFilteredValuesInWindow(rows []parquet.Row, colNames []string, targetColIdx int, filter *logstorage.Filter, tombstones []tombstone, s *Storage, seen map[string]uint64, startNs, endNs int64) {
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

	tsColIdx := -1
	for i, name := range colNames {
		if name == timestampColumn {
			tsColIdx = i
			break
		}
	}
	windowed := startNs != math.MinInt64 || endNs != math.MaxInt64

	// Mirror of the logs module: the unfiltered fast path is available only
	// when no tombstone overlaps, otherwise a deleted value stays visible in
	// exactly the unfiltered enumeration a dropdown sends.
	if filter == nil && len(tombstones) == 0 {
		for _, row := range rows {
			if windowed && !rowInWindow(row, tsColIdx, startNs, endNs) {
				continue
			}
			if targetColIdx < len(row) {
				val := formatTarget(row[targetColIdx])
				if val != "" {
					seen[val]++
				}
			}
		}
		return
	}

	for _, row := range rows {
		if windowed && !rowInWindow(row, tsColIdx, startNs, endNs) {
			continue
		}
		fields := parquetRowToFields(row, colNames, tsColIdx, s)
		if filter != nil && !filter.MatchRow(fields) {
			continue
		}
		if len(tombstones) > 0 && rowTombstoned(tombstones, fields, rowTimestampNs(row, tsColIdx)) {
			metrics.DeleteRowsSuppressed.Add(1)
			continue
		}
		if targetColIdx < len(row) {
			val := formatTarget(row[targetColIdx])
			if val != "" {
				seen[val]++
			}
		}
	}
}

// rowTimestampNs reads the row timestamp out of the projected row. Returns 0
// when the column is not projected, which makes every time-bounded tombstone
// whose range excludes 0 a non-match — hence addTombstoneProjection always
// forces the column in.
func rowTimestampNs(row parquet.Row, tsColIdx int) int64 {
	if tsColIdx < 0 || tsColIdx >= len(row) {
		return 0
	}
	return row[tsColIdx].Int64()
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

// fileWithinWindow reports whether every row of a file lies inside
// [startNs, endNs] by its manifest bounds. Unknown bounds (either end zero)
// never qualify: the file could hold rows from any time.
func fileWithinWindow(fi manifest.FileInfo, startNs, endNs int64) bool {
	if fi.MinTimeNs == 0 || fi.MaxTimeNs == 0 || fi.BoundsInferred {
		return false
	}
	return fi.MinTimeNs >= startNs && fi.MaxTimeNs <= endNs
}

// rowInWindow reports whether a projected row's timestamp lies in
// [startNs, endNs]. A row whose timestamp column is not projected is outside.
func rowInWindow(row parquet.Row, tsColIdx int, startNs, endNs int64) bool {
	if tsColIdx < 0 || tsColIdx >= len(row) {
		return false
	}
	ts := row[tsColIdx].Int64()
	return ts >= startNs && ts <= endNs
}

// windowRowGroups returns the row groups of f whose timestamp range reaches
// [lo, hi] (inclusive both ends, as the query's time filter), from the
// timestamp column index aggregated across every page. An unbounded window,
// a file without a timestamp column and a row group without a column index
// keep every row group: skipping is only ever an optimisation.
func windowRowGroups(f *parquet.File, lo, hi int64) []int {
	rgs := f.RowGroups()
	all := make([]int, 0, len(rgs))
	tsIdx := -1
	if lo != math.MinInt64 || hi != math.MaxInt64 {
		tsIdx = findColumnIndex(f.Root(), timestampColumn)
	}
	for i, rg := range rgs {
		if tsIdx >= 0 {
			if cols := rg.ColumnChunks(); tsIdx < len(cols) {
				if idx, err := cols[tsIdx].ColumnIndex(); err == nil && idx != nil && idx.NumPages() > 0 {
					if rgMin, rgMax := columnIndexTimeBounds(idx); rgMax < lo || rgMin > hi {
						metrics.ParquetRowGroupsSkipped.Inc("stats") // time range, as on the query path
						continue
					}
				}
			}
		}
		all = append(all, i)
	}
	return all
}
