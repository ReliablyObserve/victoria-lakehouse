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
	offset := fi.Size - footerPrefetchTail(s.footerPrefetchBytes(), fi.Size)
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

// remapSlotFieldHits drops UNMAPPED Tier-2 spare-slot columns (ded_sNN) from a
// field-name hit map and renames MAPPED ones to their configured attribute name,
// mirroring remapSlotFields on the row read path. Spare-slot column names are
// exactly 7 chars with the "ded_s" prefix (ded_s01..ded_s99); no real field
// collides with that shape.
func remapSlotFieldHits(hits map[string]uint64) {
	if len(hits) == 0 {
		return
	}
	slotMap := activeSlotResolver.Mapping()
	var drop []string
	add := map[string]uint64{}
	for name, n := range hits {
		if len(name) == 7 && name[:5] == "ded_s" {
			drop = append(drop, name)
			if mapped, ok := slotMap[name]; ok && mapped != "" {
				add[mapped] = n
			}
		}
	}
	for _, k := range drop {
		delete(hits, k)
	}
	for k, n := range add {
		hits[k] = n
	}
}

func (s *Storage) GetFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	filter := parseFilterFromQuery(q)
	scope := scopeFor(ctx, tenantIDs)

	startNs, endNs := q.GetFilterTimeRange()
	files := s.filesForScope("field_names", startNs, endNs, scope)

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
		if filter == nil && s.labelIndex.Len() > 0 && s.tenantScopeAllowsGlobalIndex(scope) {
			return labelIndexNamesWithHits(s.labelIndex.GetFieldNames(), nil), nil
		}
		return nil, nil
	}

	// Pre-warm the footer cache in parallel using small range reads
	// (~16 KB per file) so the sequential loop below hits the cache.
	if s.pool != nil && s.footerCache != nil {
		prefetchFooters(ctx, s.pool, files, s.footerCache, 16, s.footerPrefetchBytes())
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

	// The Tier-2 spare-slot columns (ded_s01..ded_sNN) are internal: an UNMAPPED
	// slot is an empty placeholder column (no operator promoted_attributes config)
	// and must never surface in the field list — it shows up as `ded_sNN` noise in
	// Drilldown/Explore field pickers with no values on any row. A MAPPED slot is
	// reported under its configured attribute name, matching what the row read path
	// emits via remapSlotFields. The raw Parquet column index (accumulateFieldHits)
	// has no way to know this, so apply the same remap/drop here.
	remapSlotFieldHits(hits)

	// Hit counts here come from the Parquet COLUMN INDEX (row count minus null
	// count per page) — no row is ever read, so a tombstone cannot be applied
	// per row without turning a footer walk into a full scan of every candidate
	// file. Reporting the raw counts anyway would hand the caller a number that
	// still includes rows the user deleted.
	//
	// The honest answer is the one this function already uses for its fallback
	// paths: emit the names with Hits=0, the documented "unknown count" signal.
	// Names stay over-inclusive (a field carried only by deleted rows still
	// appears until the rewrite lands and the file is replaced) — that bound is
	// documented in docs/operations.md rather than papered over.
	//
	// The check covers every row the counts came from: the column index of a
	// whole file, whose rows can extend past the query window, so a tombstone
	// just outside the window but inside a counted file still taints the count.
	if tsLo, tsHi := filesTimeSpan(files, startNs, endNs); len(s.fieldsTombstones(scope, tsLo, tsHi)) > 0 {
		noteFieldsScanFallback("field_names")
		names := make([]string, 0, len(hits))
		for name := range hits {
			names = append(names, name)
		}
		if len(names) > 0 {
			return labelIndexNamesWithHits(names, nil), nil
		}
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
		if names := s.catalogFieldNames(q, scope); len(names) > 0 {
			return labelIndexNamesWithHits(names, hits), nil
		}
	}
	if s.labelIndex.Len() > 0 && s.tenantScopeAllowsGlobalIndex(scope) {
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
	tombstones []tombstone,
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
	// A tombstone predicate can only be evaluated against columns that are in
	// the projection — including the timestamp it is bounded by.
	s.addTombstoneProjection(tombstones, projectedCols)

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
				collectFilteredValues(buf[:n], projectedNames, targetInProjection, filter, tombstones, s, seen)
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

	// pmeta catalog fast-path (--pmeta): union the field's values across the
	// partitions in the query's time range, served from RAM. nil (flag off) or
	// empty (cold) falls through to the labelIndex/scan path unchanged.
	//
	// NOTE: a no-limit request (limit==0, what a Grafana dropdown sends) MUST still
	// use the in-RAM index — both the catalog and the labelIndex are self-bounded,
	// so serving from them is correct AND avoids a full column scan (which, with S3
	// latency, takes tens of seconds). Gating this on `limit > 0` was the dropdown
	// slowness: it sent every no-limit field_values straight to the scan path.
	startNs, endNs := q.GetFilterTimeRange()

	// The catalog and the labelIndex are built at write/compaction time and
	// carry no tombstone awareness: a value that exists only on deleted rows is
	// still in both. Serving from them while a tombstone could cover their
	// answer is how a "deleted" value kept appearing in dropdowns, so a fast
	// path is given up whenever a tombstone overlaps what IT answers from, and
	// the answer is verified against rows instead:
	//   - the catalog answers per partition hour → hour-widened window;
	//   - the labelIndex is not time-scoped at all → any active tombstone;
	//   - the row scan reads whole files → the scanned files' time span (below).
	gaveUpFastPath := false

	if filter == nil && s.catalog != nil {
		if hLo, hHi := partitionHourBounds(startNs, endNs); len(s.fieldsTombstones(scope, hLo, hHi)) > 0 {
			gaveUpFastPath = true
		} else {
			if s.refuseEnumeration(fieldName) {
				return nil, nil // declared id column: don't enumerate (matches VL), no scan
			}
			if result := s.catalogFieldValues(q, scope, fieldName, limit); len(result) > 0 {
				return result, nil
			}
		}
	}

	if filter == nil && s.labelIndex.Len() > 0 && s.tenantScopeAllowsGlobalIndex(scope) {
		if len(s.allTombstones(scope)) > 0 {
			gaveUpFastPath = true
		} else if vals := s.labelIndex.GetFieldValues(fieldName, limit); len(vals) > 0 {
			result := make([]logstorage.ValueWithHits, len(vals))
			for i, v := range vals {
				result[i] = logstorage.ValueWithHits{Value: v, Hits: 1}
			}
			return result, nil
		}
	}

	if gaveUpFastPath {
		noteFieldsScanFallback("field_values")
	}

	files := s.filesForScope("field_values", startNs, endNs, scope)
	if len(files) == 0 {
		return nil, nil
	}
	// Every object in the list is scanned. A compaction source whose rows are
	// already inside a merged output never reaches here: the publish removes it
	// from the manifest and retires the key, and the retirement outlives the
	// delete, so a LIST that still returns it cannot put it back (see
	// internal/manifest/retired.go). Guessing redundancy here from time ranges
	// and compaction levels instead used to hide the newest flush of a live
	// partition — its rows fall inside the compacted neighbour's backfilled
	// range — from every enumeration.

	// The scan reads every row of every overlapping file, including the rows
	// that lie outside the query window, so it must apply every tombstone
	// overlapping those files — not only the ones overlapping the window.
	spanLo, spanHi := filesTimeSpan(files, startNs, endNs)
	tombstones := s.fieldsTombstones(scope, spanLo, spanHi)
	// Each object gets only the tombstones of its own tenant.
	parse := s.keyTenantParser()

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
		if err := s.scanProjectedFieldValues(ctx, fi, mapping.ParquetColumn, filter, tombstonesForKey(tombstones, parse, fi.Key), seen); err != nil {
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

	files := s.filesForTenants(ctx, "streams", startNs, endNs, tenantIDs)
	scope := scopeFor(ctx, tenantIDs)
	if len(files) == 0 {
		return nil, nil
	}

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

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Column-projected read: fetches only (_stream + filter cols)
		// chunk data from S3 rather than the entire file body.
		if err := s.scanProjectedFieldValues(ctx, fi, streamColName, filter, tombstonesForKey(tombstones, parse, fi.Key), seen); err != nil {
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

	files := s.filesForTenants(ctx, "stream_ids", startNs, endNs, tenantIDs)
	scope := scopeFor(ctx, tenantIDs)
	if len(files) == 0 {
		return nil, nil
	}

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

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		// Column-projected read: fetches only (_stream_id + filter cols)
		// chunk data from S3 rather than the entire file body.
		if err := s.scanProjectedFieldValues(ctx, fi, colName, filter, tombstonesForKey(tombstones, parse, fi.Key), seen); err != nil {
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
func collectFilteredValues(rows []parquet.Row, colNames []string, targetColIdx int, filter *logstorage.Filter, tombstones []tombstone, s *Storage, seen map[string]uint64) {
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

	// The unfiltered fast path skips row materialisation entirely — and it is
	// the COMMON case (an unfiltered field_values is what a Grafana dropdown
	// sends). Applying the tombstone predicate only on the filtered branch
	// would leave deleted values visible in exactly the request that most users
	// make, so the fast path is available only when there is no tombstone.
	if filter == nil && len(tombstones) == 0 {
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
		if name == timestampColumn {
			tsColIdx = i
			break
		}
	}

	for _, row := range rows {
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
