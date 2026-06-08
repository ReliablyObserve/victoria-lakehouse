package parquets3

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/format"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

func sortFilesByCacheAffinity(files []manifest.FileInfo, cachedKeys map[string]bool) {
	sort.SliceStable(files, func(i, j int) bool {
		iCached := cachedKeys[files[i].Key]
		jCached := cachedKeys[files[j].Key]
		if iCached != jCached {
			return iCached
		}
		return false
	})
}

func (s *Storage) applySelfFilter(files []manifest.FileInfo) []manifest.FileInfo {
	if !s.selfFilterEnabled || s.smartCache == nil {
		return files
	}
	var owned []manifest.FileInfo
	for _, f := range files {
		if _, isLocal := s.smartCache.LookupOwner(f.Key); isLocal {
			owned = append(owned, f)
		}
	}
	if len(owned) > 0 {
		return owned
	}
	return files
}

func (s *Storage) applyCacheAffinity(files []manifest.FileInfo) {
	if s.footerCache == nil {
		return
	}
	cachedKeys := make(map[string]bool, len(files))
	for _, f := range files {
		if s.footerCache.Has(f.Key) {
			cachedKeys[f.Key] = true
		}
	}
	if len(cachedKeys) > 0 && len(cachedKeys) < len(files) {
		sortFilesByCacheAffinity(files, cachedKeys)
	}
}

func (s *Storage) manifestFastPath(files []manifest.FileInfo, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc) []manifest.FileInfo {
	var remaining []manifest.FileInfo
	for _, fi := range files {
		if fi.RowCount > 0 && fi.MinTimeNs > 0 && fi.MaxTimeNs > 0 &&
			fi.MinTimeNs >= startNs && fi.MaxTimeNs <= endNs {
			db := s.syntheticManifestBlock(fi)
			if db != nil && db.RowsCount() > 0 {
				writeBlock(0, db)
				metrics.MetadataOnlyFiles.Inc()
			}
		} else {
			remaining = append(remaining, fi)
		}
	}
	if len(remaining) < len(files) {
		logger.Infof("metadata fast path: resolved %d/%d files from manifest, %d remain for S3",
			len(files)-len(remaining), len(files), len(remaining))
	}
	return remaining
}

func (s *Storage) syntheticManifestBlock(fi manifest.FileInfo) *logstorage.DataBlock {
	const maxSyntheticRows = 50_000_000
	n := int(fi.RowCount)
	if n == 0 {
		return nil
	}
	if n > maxSyntheticRows {
		n = maxSyntheticRows
	}

	tsCol := s.registry.TimestampColumn()
	internalName := tsCol
	if m := s.registry.ResolveFromParquet(tsCol); m != nil {
		internalName = m.InternalName
	}

	values := make([]string, n)
	if n == 1 {
		values[0] = s.registry.FormatField(internalName, fi.MinTimeNs)
	} else {
		step := (fi.MaxTimeNs - fi.MinTimeNs) / int64(n-1)
		if step == 0 {
			step = 1
		}
		for i := range values {
			ts := fi.MinTimeNs + int64(i)*step
			if ts > fi.MaxTimeNs {
				ts = fi.MaxTimeNs
			}
			values[i] = s.registry.FormatField(internalName, ts)
		}
	}

	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{{Name: internalName, Values: values}})
	return db
}

func (s *Storage) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	queryStart := time.Now()
	metrics.ConcurrentSelects.Inc()
	defer func() {
		metrics.ConcurrentSelects.Dec()
		elapsed := time.Since(queryStart).Seconds()
		metrics.QueryDuration.Observe(elapsed)
	}()

	startNs, endNs := q.GetFilterTimeRange()

	q, endNs = widenTraceIDQueryToNow(q, startNs, endNs)

	// The window is wholly inside the hot tier's boundary — hot VT owns it, the
	// cold tier has nothing to add. Skip the scan entirely.
	if s.windowInsideHotBoundary(startNs, endNs) {
		return nil
	}

	if !s.manifest.HasDataForRange(startNs, endNs) {
		metrics.ManifestFastPathTotal.Inc()
		// Don't early-return here. Buffer-bridge may still have rows newer
		// than the latest flushed parquet (Jaeger's GetTrace lookup for
		// trace_ids just observed in a previous search-step lookup is the
		// canonical case). We fall through; the GetFilesForRange branch
		// below detects len(files) == 0 and calls queryBufferBridge before
		// returning.
	}

	queryStr := q.String()
	pipeFields := logstorage.GetQueryPipeFields(q)
	filter := parseFilterFromQuery(q)

	// Per-query memory ceiling for in-flight DataBlock rows. Mirror of the
	// budget in internal/storage/parquets3 (logs module). See that file for
	// the rationale and VL reference.
	maxLiveBytes := s.cfg.Query.MaxLiveBytes
	if maxLiveBytes <= 0 {
		maxLiveBytes = defaultMaxLiveBytes
	}
	var liveBytes atomic.Int64

	var rowsEmitted atomic.Int64
	maxRows := s.cfg.Query.MaxRows

	// Wrap writeBlock to apply LogsQL filter evaluation, tombstone filtering,
	// max_rows enforcement, and panic recovery.
	// Pre-filter runs in each worker goroutine without locks.
	// Only the final writeBlock call is serialized.
	var writeBlockPanic atomic.Bool
	preFilter := func(db *logstorage.DataBlock) *logstorage.DataBlock {
		if writeBlockPanic.Load() {
			return nil
		}
		if maxRows > 0 && rowsEmitted.Load() >= maxRows {
			cancel()
			return nil
		}
		if liveBytes.Load() >= maxLiveBytes {
			metrics.QueryMemoryBudgetExceeded.Inc()
			logger.Warnf("query memory budget exceeded: live=%d, max=%d; cancelling",
				liveBytes.Load(), maxLiveBytes)
			cancel()
			return nil
		}
		db = filterDataBlock(db, filter)
		if db == nil || db.RowsCount() == 0 {
			return nil
		}
		if s.tombstones != nil {
			db = s.filterTombstonedRows(db, startNs, endNs)
			if db == nil || db.RowsCount() == 0 {
				return nil
			}
		}
		return db
	}

	var wbMu sync.Mutex
	filteredWriteBlock := func(workerID uint, db *logstorage.DataBlock) {
		db = preFilter(db)
		if db == nil {
			return
		}
		rowsEmitted.Add(int64(db.RowsCount()))
		sz := dataBlockApproxBytes(db)
		liveBytes.Add(sz)
		wbMu.Lock()
		func() {
			defer func() {
				if r := recover(); r != nil {
					writeBlockPanic.Store(true)
					logger.Warnf("writeBlock panic recovered (unsupported pipe in query): %v", r)
				}
			}()
			writeBlock(workerID, db)
		}()
		wbMu.Unlock()
		liveBytes.Add(-sz)
	}

	// Tenant-scoped file enumeration when exactly one tenant is in scope.
	// The cross-tenant (admin) read path retains the legacy full-manifest
	// walk because it legitimately needs every tenant's files. Most query
	// paths are single-tenant by construction (per-request auth) so this
	// branch wins for the common case.
	var files []manifest.FileInfo
	if len(tenantIDs) == 1 {
		t := tenantIDs[0]
		files = s.manifest.GetFilesForRangeTenant(
			startNs, endNs,
			fmt.Sprintf("%d", t.AccountID),
			fmt.Sprintf("%d", t.ProjectID),
		)
	} else {
		files = s.manifest.GetFilesForRange(startNs, endNs)
	}
	if len(files) == 0 {
		// No cold-tier files cover the requested window, but the in-flight
		// buffer-bridge may still have rows newer than the latest flushed
		// parquet (Jaeger's GetTrace narrow-window lookup against trace_ids
		// just observed in the previous search step is the canonical case).
		// Falling through here keeps the buffer query in the flow.
		s.queryBufferBridge(ctx, startNs, endNs, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)
		return nil
	}

	files = s.applySelfFilter(files)
	s.applyCacheAffinity(files)

	hasTombstones := s.tombstones != nil && len(s.tombstones.ForRange(startNs, endNs)) > 0
	if storage.IsTimestampOnly(ctx) && filter == nil && !hasTombstones {
		remaining := s.manifestFastPath(files, startNs, endNs, filteredWriteBlock)
		if len(remaining) == 0 {
			if n := rowsEmitted.Load(); n > 0 {
				metrics.QueryRowsTotal.Add(int(n))
			}
			s.queryBufferBridge(ctx, startNs, endNs, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)
			return nil
		}
		files = remaining
	}

	files = s.preFilterFiles(files, queryStr)

	if len(files) == 0 {
		s.queryBufferBridge(ctx, startNs, endNs, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)
		return nil
	}

	prefetchFooters(ctx, s.pool, files, s.footerCache, 0)

	// Deterministic trace_id narrowing — runs after bloom because the
	// footer cache is now warm. For any file with a `_trace_idx`
	// metadata block whose trace IDs don't include the queried one,
	// we drop the file with zero ambiguity. Files that lack the index
	// (older parquets, partial writes) stay in the candidate set so
	// the bloom-positive path still wins by default. The cost is one
	// (cached) footer read per remaining file; the payoff is that a
	// non-existent trace ID stops sweeping 50+ files of bloom false
	// positives — previously a 30s Jaeger client timeout per Get.
	if tids := extractFilterValuesAST(queryStr, "trace_id"); len(tids) > 0 {
		files = s.filterFilesByTraceIdx(ctx, files, tids)
		if len(files) == 0 {
			s.queryBufferBridge(ctx, startNs, endNs, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)
			return nil
		}
	}

	// Parallel file worker pool
	fileWorkers := s.cfg.Query.FileWorkers
	if fileWorkers <= 0 {
		fileWorkers = 8
	}
	if fileWorkers > len(files) {
		fileWorkers = len(files)
	}

	queryID := fmt.Sprintf("q-%d", queryStart.UnixNano())

	// K8s-style process-wide query.max_rows budget — mirror of the
	// logs module. Reserve maxRows against the global QueryMaxRows
	// bound up-front (one acquire per query). The bound caps cumulative
	// reserved rows across ALL concurrent queries; the outlier path
	// admits a single oversized query alone. Skip when maxRows is 0
	// (unbounded) or the bound is metric-only (Limit=0).
	if s.bounds != nil && s.bounds.QueryMaxRows != nil && maxRows > 0 {
		relRows, boundErr := s.bounds.QueryMaxRows.Acquire(ctx, maxRows)
		if boundErr != nil {
			return fmt.Errorf("query max-rows budget exhausted: %w", boundErr)
		}
		defer relRows()
	}

	// Pin all files in smart cache before query, defer unpin
	if s.smartCache != nil {
		for _, fi := range files {
			s.smartCache.Pin(fi.Key, queryID)
		}
		defer func() {
			for _, fi := range files {
				s.smartCache.Unpin(fi.Key, queryID)
			}
		}()
	}

	taskCh := make(chan manifest.FileInfo, len(files))
	for _, fi := range files {
		taskCh <- fi
	}
	close(taskCh)

	var wg sync.WaitGroup
	var firstErr atomic.Value

	for i := 0; i < fileWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for fi := range taskCh {
				if err := ctx.Err(); err != nil {
					firstErr.CompareAndSwap(nil, err)
					return
				}
				if maxRows > 0 && rowsEmitted.Load() >= maxRows {
					return
				}
				// K8s-style process-wide file-worker admission. Mirror
				// of the logs module wiring. Acquire 1 count slot per
				// file before any I/O; blocking acquires unstick on
				// ctx cancellation, surfacing rejected_total++ for
				// operator dashboards.
				if s.bounds != nil && s.bounds.FileWorkers != nil {
					rel, boundErr := s.bounds.FileWorkers.Acquire(ctx, 1)
					if boundErr != nil {
						firstErr.CompareAndSwap(nil, fmt.Errorf("file workers limit exceeded: %w", boundErr))
						return
					}
					func() {
						defer rel()
						s.processOneFile(ctx, fi, startNs, endNs, queryStr, pipeFields, filter, hasTombstones, filteredWriteBlock)
					}()
					continue
				}
				s.processOneFile(ctx, fi, startNs, endNs, queryStr, pipeFields, filter, hasTombstones, filteredWriteBlock)
			}
		}()
	}
	wg.Wait()

	if v := firstErr.Load(); v != nil {
		if err, ok := v.(error); ok {
			if maxRows > 0 && rowsEmitted.Load() >= maxRows {
				// Cancelled due to maxRows — not an error.
			} else {
				return err
			}
		}
	}

	s.queryBufferBridge(ctx, startNs, endNs, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)

	return nil
}

// processOneFile is the per-file work unit extracted from the
// file-worker goroutine body so that the K8s-style FileWorkers bound
// can scope its Acquire/Release tightly around one file's I/O.
// Mirror of the logs module helper. Keeping the loop body behind a
// function call also makes the negative-control test
// (TestQueryFileWorkers_BoundEnforced) load-bearing: stripping the
// Acquire makes the bound's Limit unenforced.
func (s *Storage) processOneFile(ctx context.Context, fi manifest.FileInfo, startNs, endNs int64, queryStr string, pipeFields []string, filter *logstorage.Filter, hasTombstones bool, filteredWriteBlock logstorage.WriteDataBlockFunc) {
	if skip, _ := shouldSkipByFooter(ctx, s.pool, fi, queryStr, s.registry, s.footerCache); skip {
		return
	}
	if s.checkFileBloom(ctx, fi, queryStr) {
		return
	}
	if err := s.queryFile(ctx, fi, startNs, endNs, queryStr, pipeFields, filteredWriteBlock); err != nil {
		if isFileNotFoundError(err) {
			s.handle404Recovery(ctx, fi, filter, hasTombstones, filteredWriteBlock)
			return
		}
		metrics.QueryFileErrorsTotal.Inc()
		logger.Warnf("query file error: %s; key=%s", err, fi.Key)
	}
}

func (s *Storage) preFilterFiles(files []manifest.FileInfo, queryStr string) []manifest.FileInfo {
	// We deliberately do NOT use the smartCache `FindFilesByTraceID`
	// result to NARROW the candidate file set for trace_id queries —
	// neither for the single-id `trace_id:="X"` (Jaeger get-trace)
	// shape nor the multi-id `trace_id:in(t1,t2,...)` (VT GetTraceList
	// step 2) shape.
	//
	// Reason: smartCache.FindFilesByTraceID is a LOWER BOUND on the
	// relevant file set. It only returns files whose TraceIDs were
	// RECORDED (RecordTraceIDs runs at the END of queryFile, AFTER a
	// file has been scanned at least once — see storage_query.go
	// queryFile). A recently-flushed file is in the manifest and
	// carries a correct `_trace_idx` footer, but its smartCache
	// TraceIDs set is empty until something queries it. If we narrow
	// to the cache's lower bound, that recently-flushed file is
	// silently dropped BEFORE the deterministic, footer-backed
	// `filterFilesByTraceIdx` (run in RunQuery right after this) ever
	// sees it.
	//
	// Live blast radius (pre-fix): cold Jaeger /api/traces returned 0
	// traces while hot VT returned 20, for any trace whose spans were
	// flushed minutes-to-~1h ago (queryable by `_stream` which has no
	// trace_id filter, but invisible to `trace_id:"X"` which took the
	// fast-path). The band self-healed after the first query (which
	// recorded the file's TraceIDs) — hence ">1h works, fresh fails".
	//
	// The single-id case was the un-fixed residue of the same
	// lower-bound bug that killed the multi-id fast-path in bd838e9.
	// The deterministic `filterFilesByTraceIdx` reads the actual
	// `_trace_idx` footer KV (cached) and is the correct, complete
	// narrowing for both shapes — so dropping this shortcut loses no
	// correctness and ~no performance (footer reads are cached).
	files = s.filterFilesByLabels(files, queryStr)
	if len(files) == 0 {
		return nil
	}

	files = s.filterFilesByBloomIndex(files, queryStr)
	return files
}

// filterFilesByTraceIdx keeps only files whose `_trace_idx` footer
// metadata mentions at least one of the queried trace IDs. Files
// without the index are kept (conservative — older parquets pre-date
// the index feature; dropping them would silently lose results).
// Run in parallel because each footer fetch can still cost an S3
// round-trip on a cache miss; the bound mirrors LookupTraceIndex
// and keeps the read budget in line with query.file-workers.
func (s *Storage) filterFilesByTraceIdx(ctx context.Context, files []manifest.FileInfo, tids []string) []manifest.FileInfo {
	if len(files) == 0 || len(tids) == 0 {
		return files
	}
	tidSet := make(map[string]bool, len(tids))
	for _, t := range tids {
		tidSet[t] = true
	}

	keep := make([]bool, len(files))
	sem := make(chan struct{}, traceIndexLookupParallelism)
	var wg sync.WaitGroup
	for i := range files {
		i := i
		select {
		case sem <- struct{}{}:
		case <-ctx.Done():
		}
		if ctx.Err() != nil {
			keep[i] = true // be safe on cancellation
			continue
		}
		wg.Add(1)
		go func() {
			defer wg.Done()
			defer func() { <-sem }()
			f, err := s.fetchFooterFile(ctx, files[i])
			if err != nil {
				keep[i] = true // can't tell — preserve
				metrics.TraceIdxPreFilterFiles.Inc("kept_error")
				return
			}
			meta := f.Metadata()
			result := traceIdxClassifyFile(meta, tidSet)
			keep[i] = result != "dropped"
			metrics.TraceIdxPreFilterFiles.Inc(result)
		}()
	}
	wg.Wait()

	dropped := 0
	out := files[:0]
	for i, fi := range files {
		if keep[i] {
			out = append(out, fi)
		} else {
			dropped++
		}
	}
	if dropped > 0 {
		logger.Infof("trace_idx pre-filter: dropped %d/%d files for %d trace_id(s)", dropped, len(files), len(tids))
	}
	return out
}

// traceIdxClassifyFile returns one of the metric labels for
// metrics.TraceIdxPreFilterFiles: "dropped", "kept_match",
// "kept_unindexed", or "kept_error". Kept separate from
// traceIdxKeepFile (which returns a bool only) so the bool path
// stays simple while the metric path records WHY a file was kept.
func traceIdxClassifyFile(meta *format.FileMetaData, tidSet map[string]bool) string {
	if meta == nil || len(meta.KeyValueMetadata) == 0 {
		return "kept_unindexed"
	}
	for _, kv := range meta.KeyValueMetadata {
		if kv.Key != traceIndexMetadataKey {
			continue
		}
		entries, ok := traceIndexFromMetadata(map[string]string{traceIndexMetadataKey: kv.Value})
		if !ok {
			return "kept_error" // corrupted index — preserve, count as error
		}
		for _, e := range entries {
			if tidSet[e.TraceID] {
				return "kept_match"
			}
		}
		return "dropped"
	}
	return "kept_unindexed"
}

// traceIdxKeepFile decides whether a file should be kept in the
// candidate set based purely on its parsed footer metadata. Pulled
// out of filterFilesByTraceIdx so the keep/drop policy is testable
// without a parquet.File / S3 / footer cache. The contract:
//
//   - nil meta or no KeyValueMetadata at all  → keep (older parquets
//     that pre-date the _trace_idx feature; dropping would silently
//     hide results).
//   - _trace_idx KV present but unparseable    → keep (defensive;
//     a malformed index is the operator's bug, not a reason to
//     pretend the file isn't there).
//   - _trace_idx KV parses cleanly and contains at least one of the
//     queried trace IDs                        → keep.
//   - _trace_idx KV parses cleanly and contains none of them → DROP.
//   - No _trace_idx KV at all (other KVs only) → keep (same reason
//     as the first case — index just not written for this file).
func traceIdxKeepFile(meta *format.FileMetaData, tidSet map[string]bool) bool {
	if meta == nil || len(meta.KeyValueMetadata) == 0 {
		return true
	}
	for _, kv := range meta.KeyValueMetadata {
		if kv.Key != traceIndexMetadataKey {
			continue
		}
		entries, ok := traceIndexFromMetadata(map[string]string{traceIndexMetadataKey: kv.Value})
		if !ok {
			return true
		}
		for _, e := range entries {
			if tidSet[e.TraceID] {
				return true
			}
		}
		return false
	}
	return true
}

// bufferWatermark returns the timestamp up to which the just-scanned Parquet
// files already cover the data, so the buffer is queried only for STRICTLY
// newer rows — preventing the buffer and the S3-Parquet scan from both emitting
// the same span (a 2× double-count on count()/stats queries over the overlap
// window). It is the max MaxTimeNs of the emitted files. Returns 0 (serve the
// full window) for multi-tenant admin reads, where a global watermark could
// advance past a lagging tenant and lose rows; those reads accept the rare
// double-count instead of risking loss.
func bufferWatermark(files []manifest.FileInfo, tenantIDs []logstorage.TenantID) int64 {
	if len(tenantIDs) != 1 {
		return 0
	}
	var wm int64
	for i := range files {
		if files[i].MaxTimeNs > wm {
			wm = files[i].MaxTimeNs
		}
	}
	return wm
}

// widenTraceIDQueryToNow widens a trace_id-filtered query's upper time bound to
// now. Trace_id queries (Jaeger/Tempo GetTraceList step-2 `trace_id:in(...)`,
// Jaeger/Tempo get-trace `trace_id:"X"`) target specific traces and want ALL
// their spans, exactly as hot VT serves the whole trace from memory. VT caps
// step-2's upper bound at now-latencyOffset, excluding the last ~latencyOffset
// of spans; on the cold tier those freshest spans live in the buffer, and the
// `_time:[start,end]` predicate that filterDataBlock applies to buffer rows
// would drop them. Widening the query's OWN upper bound to now makes the scan
// window, the buffer fetch, and the filter's `_time` predicate agree. The
// trace_id filter bounds the result set, so widening can never over-return —
// it only stops the cold tier from hiding spans the caller asked for by id
// (the 0/404-for-recent-traces symptom). Must be done on the Query (not just a
// local endNs) because parseFilterFromQuery keeps the `_time` predicate, so a
// local-only widen would be re-narrowed. No-op for non-trace_id queries.
// windowInsideHotBoundary reports whether [startNs, endNs] is wholly within the
// hot tier's time boundary (so the cold tier need not scan).
func (s *Storage) windowInsideHotBoundary(startNs, endNs int64) bool {
	boundary := s.discovery.GetHotBoundary()
	if boundary == nil {
		return false
	}
	return time.Unix(0, startNs).After(boundary.MinTime) && time.Unix(0, endNs).Before(boundary.MaxTime)
}

func widenTraceIDQueryToNow(q *logstorage.Query, startNs, endNs int64) (*logstorage.Query, int64) {
	if q == nil || !queryFiltersTraceID(q.String()) {
		return q, endNs
	}
	if nowNs := time.Now().UnixNano(); nowNs > endNs {
		return q.CloneWithTimeFilter(q.GetTimestamp(), startNs, nowNs), nowNs
	}
	return q, endNs
}

func (s *Storage) queryBufferBridge(ctx context.Context, startNs, endNs, watermarkNs int64, q *logstorage.Query, tenantIDs []logstorage.TenantID, filteredWriteBlock logstorage.WriteDataBlockFunc) {
	// The watermark boundary exists ONLY to stop aggregation queries
	// (count()/stats) from counting a span twice — once from the Parquet scan,
	// once from the overlapping buffer. Trace-retrieval queries (Jaeger/Tempo
	// span fetch, trace-by-id) are deduplicated by the reader on
	// (trace_id, span_id), so for them double-emission is harmless — but the
	// watermark would WRONGLY exclude a trace's buffer spans whenever a scanned
	// Parquet file (holding other, newer traces) has a MaxTimeNs above this
	// trace's time. So ignore the watermark for trace_id-filtered queries and
	// serve the buffer's full window.
	if q != nil && queryFiltersTraceID(q.String()) {
		watermarkNs = 0
	}
	// Serve the buffer only for data STRICTLY newer than what the Parquet scan
	// at this call site already emitted (watermarkNs). Call sites where no
	// Parquet was emitted pass watermarkNs=0, so the buffer serves the whole
	// window.
	bufStartNs := startNs
	if watermarkNs > 0 && watermarkNs >= bufStartNs {
		bufStartNs = watermarkNs + 1
	}
	if bufStartNs > endNs {
		return // Parquet already covers this whole window; nothing newer to add.
	}

	// Option B (P3): when a co-located logstorage-native buffer is present,
	// serve the recent/unflushed window from it via the SAME engine the
	// S3-Parquet scan uses — no struct→DataBlock conversion (which is the bug
	// class this whole effort removes). We run a pipe-stripped clone scoped to
	// (watermark, endNs] so the store emits filtered raw blocks into
	// filteredWriteBlock; the outer RunQuery applies pipes once over buffer +
	// Parquet blocks.
	// Use the local logstorage buffer directly (zero-conversion) ONLY when this
	// node has no peers — i.e. single-node role=all, where the local buffer holds
	// ALL unflushed rows. In a multi-pod deployment the local buffer holds only
	// THIS pod's ingested rows; other insert pods' unflushed rows live in their
	// buffers, reachable only via the BufferBridge HTTP fan-out. So with peers we
	// fall through to the fan-out (every pod's handler returns its own rows — no
	// double-count, no need to exclude self from the unfiltered peer list).
	if s.localBuffer != nil && (s.bufferBridge == nil || !s.bufferBridge.HasPeers()) {
		qBuf := q.CloneWithTimeFilter(q.GetTimestamp(), bufStartNs, endNs)
		qBuf.DropAllPipes()
		qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, tenantIDs, qBuf, false, nil)
		if err := s.localBuffer.RunQuery(qctx, filteredWriteBlock); err != nil {
			logger.Warnf("Option B local buffer query failed (cold-tier results may miss the recent window): %s", err)
		}
		return
	}
	if s.bufferBridge == nil {
		return
	}
	switch s.cfg.Mode {
	case config.ModeLogs:
		bufRows, _ := s.bufferBridge.QueryLogs(ctx, bufStartNs, endNs)
		if len(bufRows) > 0 {
			db := s.logRowsToDataBlock(bufRows)
			if db != nil && db.RowsCount() > 0 {
				filteredWriteBlock(0, db)
			}
		}
	case config.ModeTraces:
		bufRows, _ := s.bufferBridge.QueryTraces(ctx, bufStartNs, endNs)
		if len(bufRows) > 0 {
			db := s.traceRowsToDataBlock(bufRows)
			if db != nil && db.RowsCount() > 0 {
				filteredWriteBlock(0, db)
			}
		}
	}
}

// openParquetFile returns a parquet.File for the given FileInfo.
// When a cached footer is available and the query projects few columns,
// it uses S3ReaderAt so parquet-go fetches only the needed column chunks
// via HTTP range requests. Falls back to full download on any error.
func (s *Storage) openParquetFile(ctx context.Context, fi manifest.FileInfo, projectedCols map[string]bool) (*parquet.File, error) {
	// Range-read path: requires footer cache (to know total column count)
	// and a non-empty projection that covers fewer than half the columns.
	if s.footerCache != nil && projectedCols != nil && s.pool != nil {
		if cached, ok := s.footerCache.Get(fi.Key); ok {
			totalCols := len(cached.File.Root().Columns())
			if shouldUseRangeRead(fi.Size, len(projectedCols), totalCols) {
				rawReader := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
				buffered := s3reader.NewBufferedReaderAt(rawReader, fi.Size, int64(s.cfg.S3.ReadAheadBytes))
				readerAt := s3reader.NewCoalescingReaderAt(buffered, fi.Size, int64(s.cfg.S3.CoalesceGapBytes))
				f, err := parquet.OpenFile(readerAt, fi.Size)
				if err == nil {
					metrics.S3RangeReadsTotal.Inc()
					return f, nil
				}
				// Fall through to full download on error.
			}
		}
	}

	// Wildcard range-read path (Goal B): when the query has no
	// projection (projectedCols == nil — wildcard `*` or no field
	// filter), fall back to the lazy S3 ReaderAt for large files
	// instead of pulling the whole body into memory. parquet-go
	// fetches column chunks per row group on demand, so peak
	// resident memory stays at working-set-row-group bytes rather
	// than the cumulative-file-bytes that the buffered path pins.
	// Mirror of the same switch in internal/storage/parquets3.
	if s.pool != nil && projectedCols == nil && shouldUseWildcardRangeRead(fi.Size) {
		_, cached := s.memCache.Get(fi.Key)
		if !cached {
			rawReader := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
			buffered := s3reader.NewBufferedReaderAt(rawReader, fi.Size, int64(s.cfg.S3.ReadAheadBytes))
			readerAt := s3reader.NewCoalescingReaderAt(buffered, fi.Size, int64(s.cfg.S3.CoalesceGapBytes))
			f, err := parquet.OpenFile(readerAt, fi.Size)
			if err == nil {
				metrics.S3RangeReadsTotal.Inc()
				metrics.ParquetFilesOpened.Inc()
				return f, nil
			}
		}
	}

	// Full download path (existing behaviour).
	data, err := s.getFileData(ctx, fi.Key, fi.Size)
	if err != nil {
		return nil, fmt.Errorf("get file data %s: %w", fi.Key, err)
	}

	metrics.ParquetFilesOpened.Inc()
	metrics.ParquetColumnBytesRead.Add(len(data))

	if s.footerCache != nil {
		if cached, ok := s.footerCache.Get(fi.Key); ok && cached.FileSize == int64(len(data)) {
			return cached.File, nil
		}
	}

	if s.footerCache != nil {
		cached, f, parseErr := ParseFooterFromData(fi.Key, data)
		if parseErr != nil {
			return nil, parseErr
		}
		s.footerCache.Put(fi.Key, cached)
		return f, nil
	}

	f, parseErr := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if parseErr != nil {
		return nil, fmt.Errorf("open parquet file %s: %w", fi.Key, parseErr)
	}
	return f, nil
}

func (s *Storage) queryFile(ctx context.Context, fi manifest.FileInfo, startNs, endNs int64, queryStr string, pipeFields []string, writeBlock logstorage.WriteDataBlockFunc) error {
	projectedCols := queryColumns(queryStr, s.registry, pipeFields)
	if projectedCols == nil && storage.IsTimestampOnly(ctx) {
		// Timestamp-only is safe ONLY for an UNFILTERED count/hits. A free-text
		// _msg word filter (e.g. `error | stats count()`) has no bloom to push
		// down, so reducing to timestamp-only drops _msg and the filter silently
		// matches zero rows. VL prepends `_time:[...]`; when any other filter term
		// remains, keep reading all columns. Mirror of the logs-module fix.
		filterPart := queryStr
		if idx := strings.Index(queryStr, " | "); idx >= 0 {
			filterPart = strings.TrimSpace(queryStr[:idx])
		}
		if !hasContentFilter(filterPart) {
			projectedCols = map[string]bool{s.registry.TimestampColumn(): true}
		}
	}
	// Field-enumerating pipes (field_names, field_values, facets,
	// block_stats) must see every column the row carries — projection
	// narrowing would truncate the answer. The adapter signals this via
	// storage.WithAllFieldsHint, which we honour by forcing read-all.
	// Drives `/api/v2/search/tags` parity with VT hot when LH cold
	// answers through ExternalStorage.
	if storage.IsAllFields(ctx) {
		projectedCols = nil
	}

	// Reserve cumulative file-resident bytes against the process-wide budget
	// BEFORE opening (and possibly downloading) the parquet file. Mirror of
	// internal/storage/parquets3/storage_query.go — see that file for the
	// heap-diff rationale and the OOM symptom this budget bounds.
	relFB, fbErr := acquireFileBudget(ctx, fi.Size)
	if fbErr != nil {
		return fbErr
	}
	defer relFB()

	f, err := s.openParquetFile(ctx, fi, projectedCols)
	if err != nil {
		return err
	}

	if s.labelIndex.Len() == 0 {
		s.updateLabelIndex(f)
	}
	s.updateColumnStats(fi.Key, f)

	tsIdx := findColumnIndex(f.Root(), s.registry.TimestampColumn())
	bloomChecks := resolveBloomCheckIndices(f, s.buildBloomChecks(queryStr))
	pdf := resolvePushDownIndices(f, buildPushDownFilter(queryStr, s.registry))

	var collectedTraceIDs []string
	var traceIDsPtr *[]string
	if s.smartCache != nil {
		traceIDsPtr = &collectedTraceIDs
	}

	rowGroups := f.RowGroups()

	// Extract file-level key-value metadata for token bloom checks.
	searchTokens := extractSearchTokens(queryStr)
	var fileKVMeta map[string]string
	if len(searchTokens) > 0 {
		if meta := f.Metadata(); meta != nil {
			fileKVMeta = make(map[string]string, len(meta.KeyValueMetadata))
			for _, kv := range meta.KeyValueMetadata {
				fileKVMeta[kv.Key] = kv.Value
			}
		}
	}

	// Pre-filter row groups using metadata (time range, bloom, pushdown, token bloom).
	var matchedRGs []parquet.RowGroup
	for rgIdx, rg := range rowGroups {
		if tsIdx >= 0 && !rowGroupMatchesTimeRange(rg, tsIdx, startNs, endNs) {
			metrics.ParquetRowGroupsSkipped.Inc("stats")
			continue
		}
		if s.bloomFilterSkip(f, rg, bloomChecks) {
			metrics.ParquetRowGroupsSkipped.Inc("bloom")
			continue
		}
		if pdf != nil && !rowGroupMatchesFilter(f, rg, pdf) {
			metrics.ParquetRowGroupsSkipped.Inc("pushdown")
			continue
		}
		if tokenBloomSkip(fileKVMeta, rgIdx, searchTokens) {
			metrics.ParquetRowGroupsSkipped.Inc("token_bloom")
			continue
		}
		matchedRGs = append(matchedRGs, rg)
	}
	sort.Slice(matchedRGs, func(i, j int) bool {
		return matchedRGs[i].NumRows() < matchedRGs[j].NumRows()
	})

	// Process matched row groups SERIALLY within a single file. See the
	// equivalent change in internal/storage/parquets3/storage_query.go for
	// the rationale: the outer file-worker pool already gives us read
	// concurrency across files; up-to-8x row-group parallelism on top of
	// 16 file workers means up to 128 concurrent row-group decoders, each
	// holding multi-MB column buffers, which has OOM-killed the 2 GiB
	// container on wildcard queries.
	for _, rg := range matchedRGs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		metrics.ParquetRowGroupsScanned.Inc()
		if err := s.readOneRowGroup(f, rg, startNs, endNs, projectedCols, pdf, writeBlock, traceIDsPtr); err != nil {
			return err
		}
	}

	if s.smartCache != nil && len(collectedTraceIDs) > 0 {
		s.smartCache.RecordTraceIDs(fi.Key, collectedTraceIDs)
	}

	if s.crossSignalClient != nil && len(collectedTraceIDs) > 0 {
		s.crossSignalClient.EnqueueHint(collectedTraceIDs, startNs, endNs, string(s.cfg.Mode))
	}

	return nil
}

func (s *Storage) readOneRowGroup(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, projectedCols map[string]bool, pdf *PushDownFilter, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	if projectedCols == nil {
		projectedCols = allLeafColumns(f)
	}
	return s.readRowGroupWithProjection(f, rg, startNs, endNs, projectedCols, pdf, writeBlock, traceIDs)
}

func allLeafColumns(f *parquet.File) map[string]bool {
	cols := make(map[string]bool)
	for _, path := range f.Schema().Columns() {
		cols[path[0]] = true
	}
	return cols
}

func (s *Storage) readRowGroup(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	if s.cfg.Mode == config.ModeTraces {
		return readRowGroupTyped[schema.TraceRow](s, f, rg, startNs, endNs, writeBlock, traceIDs, traceRowToFields)
	}
	return readRowGroupTyped[schema.LogRow](s, f, rg, startNs, endNs, writeBlock, traceIDs, logRowToFields)
}

func readRowGroupTyped[T any](s *Storage, f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string, toFields func(*T, []field) []field) error {
	reader := parquet.NewGenericRowGroupReader[T](rg)
	buf := make([]T, 256)
	for {
		n, err := reader.Read(buf)
		if n > 0 {
			db := typedRowsToDataBlock(s, buf[:n], startNs, endNs, toFields)
			if db != nil && db.RowsCount() > 0 {
				writeBlock(0, db)
				if traceIDs != nil {
					extractTraceIDs(db, traceIDs)
				}
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}
	return nil
}

func (s *Storage) readRowGroupWithProjection(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, cols map[string]bool, pdf *PushDownFilter, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
	// Bound concurrent row-group decoders process-wide. Each decode buffers
	// a full row group's projected columns (~30-50 MiB at production scale
	// per the near-OOM heap-diff). Without this gate, 16 file workers
	// concurrently decoding wide-schema row groups across a 2-day wildcard
	// scan over 200+ files produces 500+ MiB of transient memory on top
	// of the 512 MiB cache, deterministic OOM on a 2 GiB container.
	// Mirrors VL's partitionSearchConcurrencyLimitCh pattern
	// (deps/VictoriaLogs/lib/logstorage/storage_search.go:1424).
	release := acquireRGDecode()
	defer release()

	maxRowsPerBlock := defaultMaxRowsPerBlock

	emit := func(db *logstorage.DataBlock) {
		if db == nil || db.RowsCount() == 0 {
			return
		}
		splitAndEmitDataBlock(db, maxRowsPerBlock, func(chunk *logstorage.DataBlock) {
			writeBlock(0, chunk)
			if traceIDs != nil {
				extractTraceIDs(chunk, traceIDs)
			}
		})
	}

	constants := detectConstantColumns(f, rg, cols)

	readCols := cols
	if len(constants) > 0 {
		readCols = make(map[string]bool, len(cols))
		for k, v := range cols {
			readCols[k] = v
		}
		for _, cc := range constants {
			delete(readCols, cc.name)
		}
	}

	bitmap := prewhereFilter(f, rg, pdf)

	// Fast path: columnar reading when no constant columns need merging.
	if len(constants) == 0 && len(readCols) > 0 {
		db := readRowGroupColumnar(f, rg, readCols, s.registry, startNs, endNs, bitmap)
		emit(db)
		return nil
	}

	// Slow path: row-oriented reading with constant column merging.
	var allFields [][]field
	if len(readCols) > 0 {
		var err error
		allFields, err = readRowGroupProjectedBitmap(f, rg, readCols, bitmap)
		if err != nil {
			return err
		}
	}

	if len(constants) > 0 {
		if allFields == nil {
			n := int(rg.NumRows())
			if bitmap != nil {
				n = 0
				for _, b := range bitmap {
					if b {
						n++
					}
				}
			}
			allFields = make([][]field, n)
		}
		for i := range allFields {
			for _, cc := range constants {
				allFields[i] = append(allFields[i], field(cc))
			}
		}
	}

	db := s.projectedFieldsToDataBlock(allFields, startNs, endNs)
	emit(db)
	return nil
}

func (s *Storage) projectedFieldsToDataBlock(rows [][]field, startNs, endNs int64) *logstorage.DataBlock {
	if len(rows) == 0 {
		return nil
	}

	type colData struct {
		name   string
		values []string
	}
	colMap := make(map[string]int)
	var cols []colData

	getCol := func(name string) int {
		if idx, ok := colMap[name]; ok {
			return idx
		}
		idx := len(cols)
		colMap[name] = idx
		cols = append(cols, colData{name: name, values: make([]string, 0, len(rows))})
		return idx
	}

	rowNum := 0
	var seenBitmap []bool
	for _, fields := range rows {
		// Time-range filter
		skip := false
		for _, fld := range fields {
			if fld.name == "timestamp_unix_nano" {
				if ts, ok := fld.value.(int64); ok {
					if ts < startNs || ts > endNs {
						skip = true
						break
					}
				}
			}
		}
		if skip {
			continue
		}

		if cap(seenBitmap) >= len(cols) {
			seenBitmap = seenBitmap[:len(cols)]
		} else {
			seenBitmap = make([]bool, len(cols), len(cols)*2)
		}
		for i := range seenBitmap {
			seenBitmap[i] = false
		}

		scalarFieldNames := make(map[string]bool)
		for _, fld := range fields {
			if _, ok := fld.value.(map[string]string); !ok {
				scalarFieldNames[fld.name] = true
			}
		}

		for _, fld := range fields {
			if mapVal, ok := fld.value.(map[string]string); ok {
				prefix := mapColumnToAttrPrefix(fld.name)
				for k, v := range mapVal {
					if v == "" {
						continue
					}
					var effectivePrefix string
					if schema.VTTopLevelSpanAttrKeys[k] {
						effectivePrefix = ""
					} else {
						if scalarFieldNames[k] {
							continue
						}
						effectivePrefix = prefix
					}
					attrName := bytesutil.InternString(effectivePrefix + k)
					idx := getCol(attrName)
					for idx >= len(seenBitmap) {
						seenBitmap = append(seenBitmap, false)
					}
					if seenBitmap[idx] {
						continue
					}
					seenBitmap[idx] = true
					for len(cols[idx].values) < rowNum {
						cols[idx].values = append(cols[idx].values, "")
					}
					cols[idx].values = append(cols[idx].values, v)
				}
				continue
			}

			internalName := fld.name
			if m := s.registry.ResolveFromParquet(fld.name); m != nil {
				internalName = m.InternalName
			}

			formatted := s.registry.FormatField(internalName, fld.value)
			if formatted == "" {
				continue
			}
			emitCol := func(colName string) {
				idx := getCol(colName)
				for idx >= len(seenBitmap) {
					seenBitmap = append(seenBitmap, false)
				}
				if seenBitmap[idx] {
					return
				}
				seenBitmap[idx] = true
				for len(cols[idx].values) < rowNum {
					cols[idx].values = append(cols[idx].values, "")
				}
				cols[idx].values = append(cols[idx].values, formatted)
			}
			emitCol(internalName)
			// Dual emission for promoted columns whose parquet name
			// differs from the internal alias (e.g. parquet
			// `service.name` ↔ internal `resource_attr:service.name`).
			// A user-typed filter spelling either dialect must
			// resolve to a column the DataBlock actually carries;
			// without this, `service.name:="X"` resolves to a column
			// that doesn't exist in the block and matches zero rows
			// even though `_stream:{resource_attr:service.name="X"}`
			// finds 78k rows in the same time window. Mirrors a5576bf
			// (which fixed the same asymmetry in parquetRowToFields
			// used by /select/logsql/values) for the slow scan path
			// here, sibling of the same defense in readRowGroupColumnar.
			if fld.name != "" && fld.name != internalName {
				emitCol(fld.name)
			}
		}

		// Fill empty for columns not present in this row
		for i := range cols {
			if i < len(seenBitmap) && !seenBitmap[i] && len(cols[i].values) <= rowNum {
				cols[i].values = append(cols[i].values, "")
			} else if i >= len(seenBitmap) && len(cols[i].values) <= rowNum {
				cols[i].values = append(cols[i].values, "")
			}
		}
		rowNum++
	}

	if len(cols) == 0 {
		return nil
	}

	blockCols := make([]logstorage.BlockColumn, 0, len(cols))
	seen := make(map[string]bool, len(cols))
	for _, col := range cols {
		if seen[col.name] {
			continue
		}
		seen[col.name] = true
		blockCols = append(blockCols, logstorage.BlockColumn{
			Name:   col.name,
			Values: col.values,
		})
	}

	db := &logstorage.DataBlock{}
	db.SetColumns(blockCols)
	return db
}

func mapColumnToAttrPrefix(col string) string {
	switch col {
	case "resource.attributes":
		return "resource_attr:"
	case "log.attributes":
		return "log_attr:"
	case "span.attributes":
		return "span_attr:"
	case "scope.attributes":
		return "scope_attr:"
	default:
		return col + ":"
	}
}

type field struct {
	name  string
	value any
}

func logRowToFields(r *schema.LogRow, buf []field) []field {
	buf = append(buf,
		field{"_time", r.TimestampUnixNano},
		field{"_msg", r.Body},
		field{"level", r.SeverityText},
		field{"severity_number", r.SeverityNumber},
		field{"service.name", r.ServiceName},
		field{"k8s.namespace.name", r.K8sNamespaceName},
		field{"k8s.pod.name", r.K8sPodName},
		field{"k8s.deployment.name", r.K8sDeploymentName},
		field{"k8s.node.name", r.K8sNodeName},
		field{"deployment.environment", r.DeployEnv},
		field{"cloud.region", r.CloudRegion},
		field{"host.name", r.HostName},
		field{"trace_id", r.TraceID},
		field{"span_id", r.SpanID},
		field{"_stream", r.Stream},
		field{"_stream_id", r.StreamID},
		field{"scope.name", r.ScopeName},
	)
	for k, v := range r.ResourceAttributes {
		buf = append(buf, field{k, v})
	}
	for k, v := range r.LogAttributes {
		buf = append(buf, field{k, v})
	}
	return buf
}

func traceRowToFields(r *schema.TraceRow, buf []field) []field {
	buf = append(buf,
		field{"_time", r.TimestampUnixNano},
		field{"start_time_unix_nano", r.StartTimeUnixNano},
		field{"trace_id", r.TraceID},
		field{"span_id", r.SpanID},
		field{"parent_span_id", r.ParentSpanID},
		field{"name", r.SpanName},
		field{"kind", r.SpanKind},
		field{"status_code", r.StatusCode},
		field{"status_message", r.StatusMessage},
		field{"duration", r.DurationNs},
		field{"resource_attr:service.name", r.ServiceName},
		field{"scope_name", r.ScopeName},
		field{"resource_attr:deployment.environment", r.DeployEnv},
		field{"resource_attr:cloud.region", r.CloudRegion},
		field{"resource_attr:host.name", r.HostName},
		field{"resource_attr:k8s.namespace.name", r.K8sNamespaceName},
		field{"resource_attr:k8s.deployment.name", r.K8sDeploymentName},
		field{"resource_attr:k8s.node.name", r.K8sNodeName},
		field{"span_attr:http.method", r.HTTPMethod},
		field{"span_attr:http.status_code", r.HTTPStatusCode},
		field{"span_attr:http.url", r.HTTPUrl},
		field{"span_attr:db.system", r.DBSystem},
		field{"span_attr:db.statement", r.DBStatement},
		field{"_stream", r.Stream},
		field{"_stream_id", r.StreamID},
		// Service-graph edge fields. Populated only on rows tagged
		// {trace_service_graph_stream="-"}; empty on normal span rows
		// (the LogsQL engine omits empty values from its DataBlocks).
		// Must surface here so the upstream Jaeger Dependencies
		// reader's `| fields parent, child, callCount` projection
		// picks them up — without this, the columns exist in
		// Parquet but the query layer never sees them.
		field{"parent", r.ServiceGraphParent},
		field{"child", r.ServiceGraphChild},
		field{"callCount", r.ServiceGraphCallCount},
	)
	for k, v := range r.ResourceAttributes {
		if !tracePromotedResourceKeys[k] {
			buf = append(buf, field{k, v})
		}
	}
	for k, v := range r.SpanAttributes {
		if tracePromotedSpanKeys[k] {
			continue
		}
		buf = append(buf, field{k, v})
	}
	for k, v := range r.ScopeAttributes {
		buf = append(buf, field{k, v})
	}
	return buf
}

var tracePromotedResourceKeys = map[string]bool{
	"service.name":           true,
	"deployment.environment": true,
	"cloud.region":           true,
	"host.name":              true,
	"k8s.namespace.name":     true,
	"k8s.deployment.name":    true,
	"k8s.node.name":          true,
	"k8s.pod.name":           true,
}

var tracePromotedSpanKeys = map[string]bool{
	"http.method":      true,
	"http.status_code": true,
	"http.url":         true,
	"db.system":        true,
	"db.statement":     true,
}

func typedRowsToDataBlock[T any](s *Storage, rows []T, startNs, endNs int64, toFields func(*T, []field) []field) *logstorage.DataBlock {
	if len(rows) == 0 {
		return nil
	}

	type colData struct {
		name   string
		values []string
	}
	colMap := make(map[string]int)
	var cols []colData

	getCol := func(name string) int {
		if idx, ok := colMap[name]; ok {
			return idx
		}
		idx := len(cols)
		colMap[name] = idx
		cols = append(cols, colData{name: name, values: make([]string, 0, len(rows))})
		for len(cols[idx].values) < len(cols[0].values)-1 {
			cols[idx].values = append(cols[idx].values, "")
		}
		return idx
	}

	var seenBitmap []bool
	var fieldBuf []field
	for rowNum, row := range rows {
		fieldBuf = toFields(&row, fieldBuf[:0])
		fields := fieldBuf

		if cap(seenBitmap) >= len(cols) {
			seenBitmap = seenBitmap[:len(cols)]
		} else {
			seenBitmap = make([]bool, len(cols), len(cols)*2)
		}
		for i := range seenBitmap {
			seenBitmap[i] = false
		}

		for _, f := range fields {
			formatted := s.registry.FormatField(f.name, f.value)
			if formatted == "" {
				continue
			}
			idx := getCol(f.name)
			for idx >= len(seenBitmap) {
				seenBitmap = append(seenBitmap, false)
			}
			if seenBitmap[idx] {
				continue
			}
			seenBitmap[idx] = true
			cols[idx].values = append(cols[idx].values, formatted)
		}

		for i := range cols {
			if i < len(seenBitmap) && !seenBitmap[i] && len(cols[i].values) <= rowNum {
				cols[i].values = append(cols[i].values, "")
			} else if i >= len(seenBitmap) && len(cols[i].values) <= rowNum {
				cols[i].values = append(cols[i].values, "")
			}
		}
	}

	if len(cols) == 0 {
		return nil
	}

	blockCols := make([]logstorage.BlockColumn, 0, len(cols))
	seen := make(map[string]bool, len(cols))
	for _, col := range cols {
		if seen[col.name] {
			continue
		}
		seen[col.name] = true
		blockCols = append(blockCols, logstorage.BlockColumn{
			Name:   col.name,
			Values: col.values,
		})
	}

	db := &logstorage.DataBlock{}
	db.SetColumns(blockCols)
	return db
}

// extractTraceIDs collects unique, non-empty trace_id values from a DataBlock
// into the destination slice, capped at 200 entries.
func extractTraceIDs(db *logstorage.DataBlock, dest *[]string) {
	cols := db.GetColumns(false)
	for _, col := range cols {
		if col.Name != "trace_id" {
			continue
		}
		seen := make(map[string]bool)
		for _, v := range col.Values {
			if v != "" && !seen[v] && len(*dest) < 200 {
				seen[v] = true
				*dest = append(*dest, v)
			}
		}
		return
	}
}

func (s *Storage) projectColumns(allCols []string, requested []string) []int {
	if len(requested) == 0 {
		indices := make([]int, len(allCols))
		for i := range indices {
			indices[i] = i
		}
		return indices
	}

	want := make(map[string]bool, len(requested))
	for _, name := range requested {
		want[name] = true
		if m := s.registry.ResolveToParquet(name); m != nil {
			want[m.ParquetColumn] = true
		}
	}
	want["timestamp_unix_nano"] = true

	var indices []int
	for i, name := range allCols {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}
		if want[name] || want[internalName] {
			indices = append(indices, i)
		}
	}
	if len(indices) == 0 {
		indices = make([]int, len(allCols))
		for i := range indices {
			indices[i] = i
		}
	}
	return indices
}

type bloomCheck struct {
	colName string
	colIdx  int
	value   parquet.Value
}

func (s *Storage) buildBloomChecks(queryStr string) []bloomCheck {
	if queryStr == "" {
		return nil
	}

	var checks []bloomCheck
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		vals := extractFilterValues(queryStr, col.InternalName)
		if len(vals) == 0 {
			vals = extractFilterValues(queryStr, col.ParquetColumn)
		}
		for _, val := range vals {
			checks = append(checks, bloomCheck{
				colName: col.ParquetColumn,
				value:   parquet.ValueOf(val),
			})
		}
	}
	return checks
}

func resolveBloomCheckIndices(f *parquet.File, checks []bloomCheck) []bloomCheck {
	resolved := make([]bloomCheck, 0, len(checks))
	for _, check := range checks {
		idx := findColumnIndex(f.Root(), check.colName)
		if idx >= 0 {
			resolved = append(resolved, bloomCheck{
				colName: check.colName,
				colIdx:  idx,
				value:   check.value,
			})
		}
	}
	return resolved
}

func (s *Storage) bloomFilterSkip(_ *parquet.File, rg parquet.RowGroup, checks []bloomCheck) bool {
	if len(checks) == 0 {
		return false
	}

	// Group checks by parquet column. buildBloomChecks expands `field:in(a,b,c)`
	// into multiple checks for the same column — those are disjunctive (any
	// value present keeps the row group). Different columns remain conjunctive
	// (every column must possibly match). Without grouping, a `trace_id:in(a,b,c)`
	// query would skip every row group that does not bloom-contain the FIRST
	// value, even when later values exist in the group.
	cols := rg.ColumnChunks()
	type colGroup struct {
		colIdx int
		values []parquet.Value
	}
	groups := make(map[string]*colGroup, len(checks))
	for _, c := range checks {
		g, ok := groups[c.colName]
		if !ok {
			g = &colGroup{colIdx: c.colIdx}
			groups[c.colName] = g
		}
		g.values = append(g.values, c.value)
	}

	for _, g := range groups {
		if g.colIdx >= len(cols) {
			continue
		}
		bf := cols[g.colIdx].BloomFilter()
		if bf == nil || bf.Size() == 0 {
			continue
		}
		anyFound := false
		for _, v := range g.values {
			found, err := bf.Check(v)
			if err != nil {
				// On any per-value error, treat the group as inconclusive
				// and keep the row group rather than incorrectly skipping.
				anyFound = true
				break
			}
			if found {
				anyFound = true
				break
			}
		}
		if !anyFound {
			return true
		}
	}
	return false
}

func extractInValues(query, fieldName string) []string {
	prefix := fieldName + `:in(`
	idx := strings.Index(query, prefix)
	if idx < 0 {
		return nil
	}
	start := idx + len(prefix)
	end := strings.Index(query[start:], ")")
	if end < 0 {
		return nil
	}
	inner := query[start : start+end]

	var vals []string
	for _, part := range strings.Split(inner, ",") {
		part = strings.TrimSpace(part)
		part = strings.Trim(part, `"`)
		if part != "" {
			vals = append(vals, part)
		}
	}
	return vals
}

func extractFilterValues(query, fieldName string) []string {
	if vals := extractInValues(query, fieldName); len(vals) > 0 {
		return vals
	}
	if val := extractExactMatch(query, fieldName); val != "" {
		return []string{val}
	}
	return nil
}

func extractExactMatch(query, fieldName string) string {
	// Quoted patterns: trace_id:="abc" or trace_id:"abc"
	quotedPatterns := []string{
		fieldName + `:="`,
		fieldName + `:"`,
	}
	for _, prefix := range quotedPatterns {
		idx := strings.Index(query, prefix)
		if idx < 0 {
			continue
		}
		start := idx + len(prefix)
		end := strings.Index(query[start:], `"`)
		if end < 0 {
			continue
		}
		return query[start : start+end]
	}

	// Unquoted pattern: trace_id:=abc123 (produced by q.String())
	unquotedPrefix := fieldName + `:=`
	if idx := strings.Index(query, unquotedPrefix); idx >= 0 {
		start := idx + len(unquotedPrefix)
		if start < len(query) && query[start] == '"' {
			return ""
		}
		end := strings.IndexAny(query[start:], " |)")
		if end < 0 {
			return query[start:]
		}
		return query[start : start+end]
	}

	return ""
}

// filterFilesByLabels uses manifest-level labels to skip files that definitely
// don't contain the queried values. This avoids downloading files from S3.
func (s *Storage) filterFilesByLabels(files []manifest.FileInfo, queryStr string) []manifest.FileInfo {
	pdf := buildPushDownFilter(queryStr, s.registry)
	if pdf == nil || len(pdf.Checks) == 0 {
		return files
	}

	if result := s.filterByLabelIndex(files, pdf); result != nil {
		return result
	}

	// Column stats pre-filter: skip files where exact-match value is outside [min, max].
	// This avoids downloading files that can't possibly contain the queried value.
	statsFiltered := files[:0]
	statsSkipped := 0
	for _, fi := range files {
		skip := false
		for _, check := range pdf.Checks {
			if check.Op == PushDownExact && !fi.ColumnStatsContains(check.Column, check.Value) {
				skip = true
				break
			}
		}
		if skip {
			statsSkipped++
		} else {
			statsFiltered = append(statsFiltered, fi)
		}
	}
	if statsSkipped > 0 {
		metrics.ParquetRowGroupsSkipped.Inc("column_stats")
		logger.Infof("column stats pre-filter: skipped %d/%d files", statsSkipped, len(files))
		files = statsFiltered
	}

	filtered := files[:0]
	skipped := 0
	for _, fi := range files {
		if fi.Labels == nil {
			filtered = append(filtered, fi)
			continue
		}
		skip := false
		for _, check := range pdf.Checks {
			labelValues := fi.Labels[check.Column]
			if len(labelValues) == 0 || len(labelValues) >= maxLabelsPerField {
				continue
			}
			if !fileLabelsMatch(labelValues, check) {
				skip = true
				break
			}
		}
		if skip {
			skipped++
		} else {
			filtered = append(filtered, fi)
		}
	}

	if skipped > 0 {
		metrics.ParquetRowGroupsSkipped.Inc("label_index")
		logger.Infof("label pre-filter: skipped %d/%d files", skipped, len(files))
	}

	return filtered
}

func (s *Storage) filterByLabelIndex(files []manifest.FileInfo, pdf *PushDownFilter) []manifest.FileInfo {
	var candidateKeys map[string]bool

	for _, check := range pdf.Checks {
		if check.Op != PushDownExact {
			return nil
		}
		keys := s.manifest.GetFileKeysByLabel(check.Column, check.Value)
		if keys == nil {
			return nil
		}
		if candidateKeys == nil {
			candidateKeys = keys
		} else {
			for k := range candidateKeys {
				if !keys[k] {
					delete(candidateKeys, k)
				}
			}
		}
	}

	if candidateKeys == nil {
		return nil
	}

	var result []manifest.FileInfo
	for _, fi := range files {
		if candidateKeys[fi.Key] {
			result = append(result, fi)
		}
	}

	skipped := len(files) - len(result)
	if skipped > 0 {
		metrics.ParquetRowGroupsSkipped.Inc("label_index")
		logger.Infof("label index fast-path: matched %d/%d files", len(result), len(files))
	}
	return result
}

func fileLabelsMatch(values []string, check PushDownCheck) bool {
	for _, v := range values {
		switch check.Op {
		case PushDownExact:
			if v == check.Value {
				return true
			}
		case PushDownPrefix:
			if strings.HasPrefix(v, check.Value) {
				return true
			}
		case PushDownGreaterThan:
			if v > check.Value {
				return true
			}
		case PushDownLessThan:
			if v < check.Value {
				return true
			}
		default:
			return true
		}
	}
	return false
}

func (s *Storage) filterFilesByBloomIndex(files []manifest.FileInfo, queryStr string) []manifest.FileInfo {
	if s.bloomIdx == nil || s.bloomIdx.Len() == 0 {
		return files
	}

	// Try OR-branch path first when the query contains a top-level OR
	// of simple field=value predicates (Grafana drilldown shape). Each
	// branch's bloom checks are evaluated independently and UNIONed.
	// Falls through to the single-set path on unsupported shapes.
	if containsOrOperatorAST(queryStr) {
		if result, ok := s.filterFilesByBloomIndexOR(files, queryStr); ok {
			return result
		}
		// OR shape not supported by branch helper — old code path
		// would build a single checks set via extractExactMatch which
		// is unlikely to find anything inside OR clauses, so we'd
		// return `files` anyway. Make that explicit.
		return files
	}

	// Build per-column candidate value sets for all bloom-enabled columns
	// that have exact-match OR in() predicates in the query (e.g.
	// trace_id:in(t1,t2,t3) yields 3 values for the trace_id column).
	//
	// Per-column semantics across multiple values: "any-of" — a file may
	// match the column if ANY listed value may be in its bloom. Implemented
	// by running MayContainAll once per value and unioning the matching
	// file sets (bloom never false-negatives, so union-of-matches is a
	// correct superset; downstream row filter catches false-positives).
	// Cross-column semantics remain AND (intersection).
	type bloomColumn struct {
		Column string
		Values []string
	}
	var perColumn []bloomColumn
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		if isNegatedPredicateAST(queryStr, col.InternalName) || isNegatedPredicateAST(queryStr, col.ParquetColumn) {
			continue
		}
		vals := extractFilterValuesAST(queryStr, col.InternalName)
		if len(vals) == 0 {
			vals = extractFilterValuesAST(queryStr, col.ParquetColumn)
		}
		if len(vals) > 0 {
			perColumn = append(perColumn, bloomColumn{Column: col.ParquetColumn, Values: vals})
		}
	}

	if len(perColumn) == 0 {
		return files
	}

	keys := make([]string, len(files))
	for i, fi := range files {
		keys[i] = fi.Key
	}

	// Intersect per-column union(values) sets (AND across columns).
	var intersection map[string]struct{}
	for _, bc := range perColumn {
		colMatch := make(map[string]struct{})
		for _, v := range bc.Values {
			for _, k := range s.bloomIdx.MayContainAll(keys, []bloomindex.ColumnCheck{{Column: bc.Column, Value: v}}) {
				colMatch[k] = struct{}{}
			}
		}
		if intersection == nil {
			intersection = colMatch
		} else {
			for k := range intersection {
				if _, ok := colMatch[k]; !ok {
					delete(intersection, k)
				}
			}
		}
		if len(intersection) == 0 {
			break
		}
	}

	if len(intersection) == len(files) {
		return files
	}

	filtered := make([]manifest.FileInfo, 0, len(intersection))
	for _, fi := range files {
		if _, ok := intersection[fi.Key]; ok {
			filtered = append(filtered, fi)
		}
	}

	skipped := len(files) - len(filtered)
	if skipped > 0 {
		totalValues := 0
		for _, bc := range perColumn {
			totalValues += len(bc.Values)
		}
		logger.Infof("bloom index pre-filter: skipped %d/%d files (cols=%d values=%d)", skipped, len(files), len(perColumn), totalValues)
	}
	return filtered
}

// filterFilesByBloomIndexOR evaluates each top-level OR branch's
// bloom checks against the bloom index and unions the matching files.
// Returns (files, false) when the filter shape isn't a supported
// top-level OR of simple field=value predicates — caller should fall
// back to its existing behaviour for that case.
func (s *Storage) filterFilesByBloomIndexOR(files []manifest.FileInfo, queryStr string) ([]manifest.FileInfo, bool) {
	filter := parseFilterFromQueryStr(queryStr)
	if filter == nil {
		return files, false
	}
	branches := FilterExtractOrBranches(filter)
	if len(branches) == 0 {
		return files, false
	}

	branchChecks := make([][]bloomindex.ColumnCheck, 0, len(branches))
	for _, branch := range branches {
		var checks []bloomindex.ColumnCheck
		var hasUnindexed bool
		for _, bc := range branch {
			col := s.resolveBloomColumn(bc.FieldName)
			if col == "" {
				hasUnindexed = true
				break
			}
			checks = append(checks, bloomindex.ColumnCheck{Column: col, Value: bc.Value})
		}
		if hasUnindexed || len(checks) == 0 {
			// One branch can't be bloom-evaluated — every file is a
			// potential match for that branch, so fall back rather
			// than over-filter.
			return files, false
		}
		branchChecks = append(branchChecks, checks)
	}

	keys := make([]string, len(files))
	for i, fi := range files {
		keys[i] = fi.Key
	}

	unionMatch := make(map[string]bool, len(keys))
	for _, checks := range branchChecks {
		matching := s.bloomIdx.MayContainAll(keys, checks)
		for _, k := range matching {
			unionMatch[k] = true
		}
	}

	filtered := make([]manifest.FileInfo, 0, len(unionMatch))
	for _, fi := range files {
		if unionMatch[fi.Key] {
			filtered = append(filtered, fi)
		}
	}

	skipped := len(files) - len(filtered)
	if skipped > 0 {
		logger.Infof("bloom OR-branch pre-filter: skipped %d/%d files (branches=%d)", skipped, len(files), len(branches))
	}
	return filtered, true
}

// resolveBloomColumn maps a field name (internal or parquet) to the
// parquet column when the column has a bloom filter configured.
// Returns "" if no bloom is configured.
func (s *Storage) resolveBloomColumn(fieldName string) string {
	if m := s.registry.ResolveToParquet(fieldName); m != nil && m.HasBloom {
		return m.ParquetColumn
	}
	if m := s.registry.ResolveFromParquet(fieldName); m != nil && m.HasBloom {
		return m.ParquetColumn
	}
	return ""
}

func (s *Storage) checkFileBloom(ctx context.Context, fi manifest.FileInfo, queryStr string) bool {
	if queryStr == "" {
		return false
	}

	// Collect per-column candidate value sets (supports exact-match and in())
	type bloomColumn struct {
		Column string
		Values []string
	}
	var perColumn []bloomColumn
	for _, col := range s.registry.PromotedColumns() {
		if !col.HasBloom {
			continue
		}
		if isNegatedPredicateAST(queryStr, col.InternalName) || isNegatedPredicateAST(queryStr, col.ParquetColumn) {
			continue
		}
		vals := extractFilterValuesAST(queryStr, col.InternalName)
		if len(vals) == 0 {
			vals = extractFilterValuesAST(queryStr, col.ParquetColumn)
		}
		if len(vals) > 0 {
			perColumn = append(perColumn, bloomColumn{Column: col.ParquetColumn, Values: vals})
		}
	}
	if len(perColumn) == 0 {
		return false
	}

	bloomKey := fi.Key + ".bloom"
	data, err := s.pool.Download(ctx, bloomKey)
	if err != nil || len(data) == 0 {
		return false // no sidecar, can't skip
	}

	idx, err := bloomindex.Unmarshal(data)
	if err != nil {
		return false
	}

	// AND across columns, OR within a column's values: for each column, the
	// file must possibly contain AT LEAST ONE listed value (any-of). If a
	// column rules out all its values, the file is definitively unmatched.
	for _, bc := range perColumn {
		anyMatch := false
		for _, v := range bc.Values {
			if bloomindex.FileBloomMayContainAll(idx, []bloomindex.ColumnCheck{{Column: bc.Column, Value: v}}) {
				anyMatch = true
				break
			}
		}
		if !anyMatch {
			metrics.ParquetBloomChecks.Inc("file_bloom_skip")
			return true
		}
	}
	return false
}

func rowGroupMatchesTimeRange(rg parquet.RowGroup, tsColIdx int, startNs, endNs int64) bool {
	cols := rg.ColumnChunks()
	if tsColIdx >= len(cols) {
		return true
	}

	idx, err := cols[tsColIdx].ColumnIndex()
	if err != nil || idx == nil {
		return true
	}

	numPages := idx.NumPages()
	if numPages == 0 {
		return true
	}

	// Parquet pages within a row group are NOT guaranteed to be sorted by the
	// timestamp column — traces especially can have spans arrive out of order
	// (e.g. a long-running root span emits AFTER its children, or rows are
	// shuffled by the columnar writer to improve compression). Taking
	// MinValue(0) and MaxValue(N-1) as row-group bounds silently skipped row
	// groups whose smallest/largest timestamps lived in a middle page, which
	// produced empty results for narrow time windows like Jaeger's expansion
	// loop (1m → 6m → 31m queries against trace_id:in(...)). Aggregate across
	// every page index instead, mirroring the per-page scan already done in
	// rowGroupMatchesFilter / detectConstantColumns. Locked by
	// TestRowGroupMatchesTimeRange_OutOfOrderPages in this package.
	rgMin := idx.MinValue(0).Int64()
	rgMax := idx.MaxValue(0).Int64()
	for p := 1; p < numPages; p++ {
		if v := idx.MinValue(p).Int64(); v < rgMin {
			rgMin = v
		}
		if v := idx.MaxValue(p).Int64(); v > rgMax {
			rgMax = v
		}
	}

	return rgMax >= startNs && rgMin < endNs
}

func findColumnIndex(root *parquet.Column, name string) int {
	col := root.Column(name)
	if col != nil && col.Leaf() {
		return col.Index()
	}
	// Fallback: search top-level leaf columns by name
	for _, c := range root.Columns() {
		if c.Name() == name && c.Leaf() {
			return c.Index()
		}
	}
	return -1
}

func columnNames(root *parquet.Column) []string {
	cols := root.Columns()
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = bytesutil.InternString(col.Name())
	}
	return names
}

func parquetValueToAny(v parquet.Value) any {
	if v.IsNull() {
		return ""
	}
	switch v.Kind() {
	case parquet.Int32:
		return v.Int32()
	case parquet.Int64:
		return v.Int64()
	case parquet.Float:
		return float64(v.Float())
	case parquet.Double:
		return v.Double()
	case parquet.Boolean:
		return v.Boolean()
	case parquet.ByteArray, parquet.FixedLenByteArray:
		b := v.ByteArray()
		if isPrintable(b) {
			return bytesutil.InternBytes(b)
		}
		return fmt.Sprintf("%x", b)
	default:
		return v.String()
	}
}

func valueToString(v parquet.Value) string {
	if v.IsNull() {
		return ""
	}
	switch v.Kind() {
	case parquet.Int32:
		return strconv.FormatInt(int64(v.Int32()), 10)
	case parquet.Int64:
		return strconv.FormatInt(v.Int64(), 10)
	case parquet.Int96:
		return v.String()
	case parquet.Float:
		return strconv.FormatFloat(float64(v.Float()), 'g', -1, 32)
	case parquet.Double:
		return strconv.FormatFloat(v.Double(), 'g', -1, 64)
	case parquet.ByteArray, parquet.FixedLenByteArray:
		b := v.ByteArray()
		if isPrintable(b) {
			return bytesutil.InternBytes(b)
		}
		return fmt.Sprintf("%x", b)
	case parquet.Boolean:
		if v.Boolean() {
			return "true"
		}
		return "false"
	default:
		return v.String()
	}
}

func valueToInt64(v parquet.Value) int64 {
	if v.IsNull() {
		return 0
	}
	switch v.Kind() {
	case parquet.Int64:
		return v.Int64()
	case parquet.Int32:
		return int64(v.Int32())
	default:
		return 0
	}
}

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c < 0x20 && c != '\t' && c != '\n' && c != '\r' {
			if !strings.ContainsRune("\t\n\r", rune(c)) {
				return false
			}
		}
	}
	return true
}

// updateColumnStats extracts min/max column statistics from the Parquet file
// footer and stores them in the manifest for use by the column-stats pre-filter.
// It only reads the column index metadata — no data pages are downloaded.
func (s *Storage) updateColumnStats(fileKey string, f *parquet.File) {
	statsColumns := []string{"service.name", "severity_text", "level", "status_code", "service_name", "trace_id", "span.name"}

	stats := make(map[string]manifest.ColumnMinMax)
	for _, col := range statsColumns {
		colIdx := findColumnIndex(f.Root(), col)
		if colIdx < 0 {
			continue
		}
		var globalMin, globalMax string
		for _, rg := range f.RowGroups() {
			cols := rg.ColumnChunks()
			if colIdx >= len(cols) {
				continue
			}
			cidx, err := cols[colIdx].ColumnIndex()
			if err != nil || cidx == nil {
				continue
			}
			for p := 0; p < cidx.NumPages(); p++ {
				minVal := cidx.MinValue(p)
				maxVal := cidx.MaxValue(p)
				if minVal.IsNull() || maxVal.IsNull() {
					continue
				}
				pageMin := string(minVal.Bytes())
				pageMax := string(maxVal.Bytes())
				if len(pageMin) == 0 || len(pageMin) > 256 {
					continue
				}
				if globalMin == "" || pageMin < globalMin {
					globalMin = pageMin
				}
				if globalMax == "" || pageMax > globalMax {
					globalMax = pageMax
				}
			}
		}
		if globalMin != "" {
			stats[col] = manifest.ColumnMinMax{Min: globalMin, Max: globalMax}
		}
	}

	if len(stats) > 0 {
		s.manifest.UpdateFileColumnStats(fileKey, stats)
	}
}

func isFileNotFoundError(err error) bool {
	if err == nil {
		return false
	}
	s := err.Error()
	return strings.Contains(s, "NoSuchKey") ||
		strings.Contains(s, "NotFound") ||
		strings.Contains(s, "404") ||
		strings.Contains(s, "does not exist") ||
		strings.Contains(s, "file not found")
}

func (s *Storage) handle404Recovery(ctx context.Context, fi manifest.FileInfo, filter *logstorage.Filter, hasTombstones bool, filteredWriteBlock func(uint, *logstorage.DataBlock)) {
	metrics.QueryFileNotFoundTotal.Inc()
	if storage.IsTimestampOnly(ctx) && filter == nil && !hasTombstones &&
		fi.RowCount > 0 && fi.MinTimeNs > 0 && fi.MaxTimeNs > 0 {
		db := s.syntheticManifestBlock(fi)
		if db != nil && db.RowsCount() > 0 {
			filteredWriteBlock(0, db)
			metrics.MetadataOnlyFiles.Inc()
		}
		logger.Infof("query recovered compacted file via manifest metadata; key=%s rows=%d", fi.Key, fi.RowCount)
	} else {
		logger.Infof("query skipped compacted/deleted file; key=%s", fi.Key)
	}
}

func (s *Storage) QuerySpecificFiles(ctx context.Context, fileKeys []string, startNs, endNs int64, queryStr string, pipeFields []string, writeBlock logstorage.WriteDataBlockFunc) error {
	if len(fileKeys) == 0 {
		return nil
	}

	keySet := make(map[string]bool, len(fileKeys))
	for _, k := range fileKeys {
		keySet[k] = true
	}

	allFiles := s.manifest.GetFilesForRange(startNs, endNs)

	var files []manifest.FileInfo
	for _, f := range allFiles {
		if keySet[f.Key] {
			files = append(files, f)
		}
	}
	if len(files) == 0 {
		return nil
	}

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.queryFile(ctx, fi, startNs, endNs, queryStr, pipeFields, writeBlock); err != nil {
			logger.Warnf("QuerySpecificFiles: file error: %s; key=%s", err, fi.Key)
			continue
		}
	}

	return nil
}
