package parquets3

import (
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

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

// sortFilesByCacheAffinity sorts files so that those with cached footers come
// first. This improves first-result latency because cached files can be opened
// via cheap range reads instead of full S3 downloads. The sort is stable so
// relative order within cached and non-cached groups is preserved.
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

func (s *Storage) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	queryStart := time.Now()
	metrics.ConcurrentSelects.Inc()
	defer func() {
		metrics.ConcurrentSelects.Dec()
		elapsed := time.Since(queryStart).Seconds()
		metrics.QueryDuration.Observe(elapsed)
	}()

	startNs, endNs := q.GetFilterTimeRange()

	// Hot-boundary suppression is meant to prevent insert-role nodes (which
	// host the hot tier) from double-serving rows that select-role nodes will
	// fetch via the hot path. Select-role and all-role nodes are responsible
	// for the cold (S3/Parquet) tier and MUST NOT suppress — otherwise they
	// would silently drop every row whose time range overlaps the hot boundary.
	if s.cfg != nil && s.cfg.Role == config.RoleInsert {
		if boundary := s.discovery.GetHotBoundary(); boundary != nil {
			if time.Unix(0, startNs).After(boundary.MinTime) && time.Unix(0, endNs).Before(boundary.MaxTime) {
				logger.Infof("hot boundary suppression: query within hot range; start=%v, end=%v, hot_min=%v, hot_max=%v",
					time.Unix(0, startNs), time.Unix(0, endNs), boundary.MinTime, boundary.MaxTime)
				return nil
			}
		}
	}

	if !s.manifest.HasDataForRange(startNs, endNs) {
		metrics.ManifestFastPathTotal.Inc()
		logger.Infof("manifest fast path: no data for range; start=%v, end=%v",
			time.Unix(0, startNs), time.Unix(0, endNs))
		// Don't early-return here. Buffer-bridge may still have rows newer
		// than the latest flushed parquet. The GetFilesForRange branch below
		// detects len(files) == 0 and calls queryBufferBridge before returning.
		// Mirror in lakehouse-traces/internal/storage/parquets3/storage_query.go.
	}

	queryStr := q.String()
	pipeFields := logstorage.GetQueryPipeFields(q)
	filter := parseFilterFromQuery(q)

	// Per-query memory ceiling for in-flight DataBlock rows. Bounds the live
	// memory footprint a single wildcard query can pin: workers backpressure
	// on the mutex when the consumer (LogsQL pipe / HTTP writer) is slow, and
	// the budget triggers context cancellation if rows pile up faster than
	// the consumer can drain them.
	maxLiveBytes := s.cfg.Query.MaxLiveBytes
	if maxLiveBytes <= 0 {
		maxLiveBytes = defaultMaxLiveBytes
	}
	var liveBytes atomic.Int64
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	var rowsEmitted atomic.Int64
	maxRows := s.cfg.Query.MaxRows

	// Wrap writeBlock to apply LogsQL filter evaluation, tombstone filtering,
	// max_rows enforcement, and panic recovery. The synchronous writeBlock
	// (guarded by wbMu) matches VL's searchParallel pattern (see
	// deps/VictoriaLogs/lib/logstorage/storage_search.go:1334) — workers
	// produce one block at a time and backpressure on the consumer, instead
	// of queuing blocks in a deep channel that lets producer fanout balloon
	// resident memory beyond the container's mem_limit.
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
		// Wrong-schema row filter: drop rows whose stream tags identify
		// them as trace spans rather than logs. Trace-style stream tags
		// (using `resource_attr:` prefix from VT's protoparser, or
		// `name="<operation>"` as the partition key) have no place in a
		// LogsProfile query result — they have no _msg, no severity, and
		// VL hot tier never emits them. Their presence in our cold-tier
		// parquets is a pre-existing data quality issue tracked under
		// task #69-class manifest hygiene; the read-side filter here
		// matches what VL upstream's stream selector enforces at write
		// time, so the user-facing query results stay consistent across
		// tiers without us having to surgically clean S3.
		if s.cfg != nil && s.cfg.Mode == config.ModeLogs {
			db = dropTraceShapedRows(db)
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

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		// No cold-tier files cover the requested window, but the in-flight
		// buffer-bridge may still have rows newer than the latest flushed
		// parquet. Keep the buffer query in the flow so narrow recent-window
		// queries don't silently miss data that hasn't been flushed yet.
		// Mirror in lakehouse-traces/internal/storage/parquets3/storage_query.go.
		s.queryBufferBridge(ctx, startNs, endNs, maxRows, &rowsEmitted, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)
		return nil
	}

	// maxFiles <= 0 means unlimited (matches VL upstream which has no
	// such cap). Memory safety is enforced by query.max-live-bytes +
	// the rgDecodeSem semaphore. The hard rejection here is reserved
	// for operators who explicitly opt into a file ceiling.
	maxFiles := s.cfg.Query.MaxFilesPerQuery
	if maxFiles > 0 && len(files) > maxFiles {
		metrics.QueryFileLimitExceeded.Inc()
		logger.Warnf("query file limit exceeded; files=%d, max=%d, range=%v-%v; narrow the time range",
			len(files), maxFiles, time.Unix(0, startNs), time.Unix(0, endNs))
		return fmt.Errorf("query matches %d files (limit %d); narrow the time range or add filters", len(files), maxFiles)
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
			s.queryBufferBridge(ctx, startNs, endNs, maxRows, &rowsEmitted, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)
			return nil
		}
		files = remaining
	}

	// Count-pushdown fast path: an unfiltered single-field query (e.g.
	// `* | stats by (service.name) count()`) is answered from the manifest's
	// LabelAggregates for files fully within the range — zero S3 reads. Boundary
	// / un-aggregated files fall through to the scan below; the buffer bridge
	// still contributes unflushed rows after the watermark.
	if aggField := countByPushdownField(pipeFields, filter); aggField != "" && !hasTombstones {
		remaining := s.manifestCountFastPath(files, startNs, endNs, aggField, filteredWriteBlock)
		if len(remaining) == 0 {
			if n := rowsEmitted.Load(); n > 0 {
				metrics.QueryRowsTotal.Add(int(n))
			}
			s.queryBufferBridge(ctx, startNs, endNs, maxRows, &rowsEmitted, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)
			return nil
		}
		files = remaining
	}

	files = s.preFilterFiles(ctx, files, queryStr)
	if len(files) == 0 {
		s.queryBufferBridge(ctx, startNs, endNs, maxRows, &rowsEmitted, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)
		return nil
	}

	// Prefetch footers for all files in parallel using 16KB range reads.
	// This populates the footer cache so file workers can use range reads
	// instead of full S3 downloads.
	prefetchFooters(ctx, s.pool, files, s.footerCache, 0, s.footerPrefetchBytes())

	// Parallel file worker pool. Default mirrors lakehouse-traces (8) and VL's
	// bounded worker pattern; previously 64 here, which on wildcard queries
	// over many files fanned out S3 downloads + parquet decode buffers wide
	// enough to OOM a 2 GiB container.
	fileWorkers := s.cfg.Query.FileWorkers
	if fileWorkers <= 0 {
		fileWorkers = 8
	}
	if fileWorkers > len(files) {
		fileWorkers = len(files)
	}

	queryID := fmt.Sprintf("q-%d", queryStart.UnixNano())

	relRows, rowsErr := s.acquireQueryMaxRowsBudget(ctx, maxRows)
	if rowsErr != nil {
		return rowsErr
	}
	defer relRows()

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
		go func(workerIdx int) {
			defer wg.Done()
			s.fileWorkerLoop(ctx, taskCh, &firstErr, maxRows, &rowsEmitted, startNs, endNs, queryStr, pipeFields, filter, hasTombstones, filteredWriteBlock)
		}(i)
	}
	wg.Wait()

	s.queryBufferBridge(ctx, startNs, endNs, maxRows, &rowsEmitted, bufferWatermark(files, tenantIDs), q, tenantIDs, filteredWriteBlock)

	if v := firstErr.Load(); v != nil {
		if err, ok := v.(error); ok && ctx.Err() != nil {
			return err
		}
	}

	if n := rowsEmitted.Load(); n > 0 {
		metrics.QueryRowsTotal.Add(int(n))
	}

	return nil
}

// acquireQueryMaxRowsBudget is the K8s-style process-wide query.max_rows
// reservation, extracted from RunQuery to keep its cyclomatic complexity
// inside the 50-line gocyclo budget. Reserves maxRows against the
// global QueryMaxRows bound up-front (one acquire per query, not per
// row — per-row acquire would dominate the row emission hot path with
// mutex contention).
//
// Outlier semantics: a single query with maxRows > Limit is admitted
// alone (matches the Bound type docs) and runs to completion; the
// reservation is internally clamped to Limit. This preserves the
// load-bearing path for ad-hoc large investigations even when the
// operator's process-wide ceiling is conservatively sized.
//
// Returns a release func that the caller MUST invoke (typically via
// defer) when the query completes. Skips bound entirely when maxRows
// is 0 (unbounded) or the bound was constructed with Limit=0 (operator
// opted out) — returns a no-op release in that case.
func (s *Storage) acquireQueryMaxRowsBudget(ctx context.Context, maxRows int64) (func(), error) {
	if s.bounds == nil || s.bounds.QueryMaxRows == nil || maxRows <= 0 {
		return func() {}, nil
	}
	rel, err := s.bounds.QueryMaxRows.Acquire(ctx, maxRows)
	if err != nil {
		return func() {}, fmt.Errorf("query max-rows budget exhausted: %w", err)
	}
	return rel, nil
}

// fileWorkerLoop is the per-goroutine worker body extracted from
// RunQuery to keep RunQuery inside the gocyclo budget. Each iteration
// of the loop drains one file from taskCh, traverses the K8s-style
// FileWorkers bound (when configured), and invokes processOneFile to
// run the I/O. The bound's Acquire is scoped tightly around the
// processOneFile call via an immediately-invoked closure with deferred
// release.
func (s *Storage) fileWorkerLoop(ctx context.Context, taskCh <-chan manifest.FileInfo, firstErr *atomic.Value, maxRows int64, rowsEmitted *atomic.Int64, startNs, endNs int64, queryStr string, pipeFields []string, filter *logstorage.Filter, hasTombstones bool, filteredWriteBlock logstorage.WriteDataBlockFunc) {
	for fi := range taskCh {
		if err := ctx.Err(); err != nil {
			firstErr.CompareAndSwap(nil, err)
			return
		}
		if maxRows > 0 && rowsEmitted.Load() >= maxRows {
			return
		}
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
}

// processOneFile is the per-file work unit extracted from the file-worker
// goroutine body so that the K8s-style FileWorkers bound can scope its
// Acquire/Release tightly around one file's I/O. Keeping the loop body
// behind a function call also makes the negative-control test
// (TestQueryFileWorkers_BoundEnforced) load-bearing: stripping the
// Acquire makes the bound's Limit unenforced.
func (s *Storage) processOneFile(ctx context.Context, fi manifest.FileInfo, startNs, endNs int64, queryStr string, pipeFields []string, filter *logstorage.Filter, hasTombstones bool, filteredWriteBlock logstorage.WriteDataBlockFunc) {
	if skip, _ := shouldSkipByFooter(ctx, s.pool, fi, queryStr, s.registry, s.footerCache, s.footerPrefetchBytes()); skip {
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

// bufferWatermark returns the timestamp up to which the just-scanned Parquet
// files already cover the data, so the buffer is queried only for STRICTLY
// newer rows — preventing the buffer and the S3-Parquet scan from both emitting
// the same row (a 2× double-count on count()/stats over the overlap window). It
// is the max MaxTimeNs of the emitted files. Returns 0 (serve the full window)
// for multi-tenant admin reads, where a global watermark could advance past a
// lagging tenant and lose rows.
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

func (s *Storage) queryBufferBridge(ctx context.Context, startNs, endNs int64, maxRows int64, rowsEmitted *atomic.Int64, watermarkNs int64, q *logstorage.Query, tenantIDs []logstorage.TenantID, writeBlock logstorage.WriteDataBlockFunc) {
	if maxRows > 0 && rowsEmitted.Load() >= maxRows {
		return
	}
	// The watermark boundary exists ONLY to stop aggregation queries
	// (count()/stats) from counting a row twice across the buffer↔Parquet
	// overlap. trace_id-filtered queries are span/log RETRIEVAL (Jaeger/Tempo
	// fetch, log→trace correlation): completeness matters and the watermark
	// would wrongly exclude a trace's buffer rows whenever a scanned Parquet
	// file (holding other, newer data) has a MaxTimeNs above this trace's time.
	// So ignore the watermark for trace_id-filtered queries and serve the full
	// window.
	if q != nil && queryFiltersTraceID(q.String()) {
		watermarkNs = 0
	}
	// Serve the buffer only for data STRICTLY newer than what the Parquet scan
	// at this call site already emitted (watermarkNs). Sites with no Parquet
	// emitted pass 0 → full window.
	bufStartNs := startNs
	if watermarkNs > 0 && watermarkNs >= bufStartNs {
		bufStartNs = watermarkNs + 1
	}
	if bufStartNs > endNs {
		return
	}
	// Option B (P3): use the co-located logstorage-native buffer directly
	// (zero-conversion) ONLY when this node has no peers — single-node role=all,
	// where the local buffer holds ALL unflushed rows. In a multi-pod deployment
	// the local buffer holds only THIS pod's rows; other insert pods' unflushed
	// rows live in their buffers, reachable only via the BufferBridge HTTP
	// fan-out. So with peers we fall through to the fan-out (every pod's handler
	// returns its own rows — no double-count, no need to exclude self from the
	// unfiltered peer list).
	if s.localBuffer != nil && (s.bufferBridge == nil || !s.bufferBridge.HasPeers()) {
		qBuf := q.CloneWithTimeFilter(q.GetTimestamp(), bufStartNs, endNs)
		qBuf.DropAllPipes()
		qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, tenantIDs, qBuf, false, nil)
		if err := s.localBuffer.RunQuery(qctx, writeBlock); err != nil {
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
				writeBlock(0, db)
			}
		}
	case config.ModeTraces:
		bufRows, _ := s.bufferBridge.QueryTraces(ctx, bufStartNs, endNs)
		if len(bufRows) > 0 {
			db := s.traceRowsToDataBlock(bufRows)
			if db != nil && db.RowsCount() > 0 {
				writeBlock(0, db)
			}
		}
	}
}

func (s *Storage) manifestFastPath(files []manifest.FileInfo, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc) []manifest.FileInfo {
	var remaining []manifest.FileInfo
	for _, fi := range files {
		if fi.RowCount > 0 && fi.MinTimeNs > 0 && fi.MaxTimeNs > 0 &&
			fi.MinTimeNs >= startNs && fi.MaxTimeNs <= endNs {
			emitted := false
			s.streamSyntheticManifestBlocks(fi, func(db *logstorage.DataBlock) {
				if db != nil && db.RowsCount() > 0 {
					writeBlock(0, db)
					emitted = true
				}
			})
			if emitted {
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

// countByPushdownField returns the single field a query groups/projects by when
// it is eligible for the manifest count-pushdown fast-path, or "" otherwise. The
// fast-path is SOUND only when the query references exactly one raw field and has
// NO row filter: then synthetic rows that reproduce that field's value
// distribution yield the same result as scanning (count() by, uniq, fields, top —
// any single-field pipe), since the rows are indistinguishable w.r.t. the only
// column read. A filter, multiple fields, or timestamp bucketing → "" (fall
// through to scan). It self-guards further downstream: only files that actually
// carry a LabelAggregate for the field are served from metadata.
func countByPushdownField(pipeFields []string, filter *logstorage.Filter) string {
	if len(pipeFields) > 1 {
		return ""
	}
	if filter == nil {
		if len(pipeFields) == 1 {
			return pipeFields[0]
		}
		return ""
	}
	// FILTERED count pushdown: sound when the filter provably references
	// exactly ONE plain field and the pipes reference no other field. The
	// synthetic rows reproduce that field's full value distribution
	// (including the empty-value group), and preFilter applies the REAL
	// filter to every emitted block downstream — so any filter shape over
	// that one field (word match, exact, prefix, negation, OR of values…)
	// evaluates against the same values a scan would produce. A filter
	// touching any other column (or _time/_stream pseudo-fields, which
	// synthetic rows fabricate) disqualifies; unknown AST nodes disqualify
	// (countPushdownFilterFields is the strict soundness gate).
	fields, ok := countPushdownFilterFields(filter)
	if !ok {
		return ""
	}
	// "_time" is permitted alongside the one plain field: the containment
	// check runs against q.GetFilterTimeRange() — the EFFECTIVE range after
	// VL intersects params with any embedded _time filters — and synthetic
	// timestamps interpolate within the file's [MinTimeNs,MaxTimeNs], which
	// containment places inside that same range. So the time filter passes
	// synthetic rows exactly as it passes a contained file's real rows.
	delete(fields, "_time")
	if len(fields) != 1 {
		return ""
	}
	var f string
	for k := range fields {
		f = k
	}
	if f == "" || f == "_stream" || f == "_stream_id" || f == "_msg" {
		return ""
	}
	if len(pipeFields) == 1 && pipeFields[0] != f {
		return ""
	}
	return f
}

// manifestCountFastPath serves files FULLY within [startNs,endNs] that carry a
// LabelAggregate for aggField by emitting synthetic rows reproducing that field's
// value distribution — zero S3 reads. Files that straddle the range boundary
// (whole-file aggregate would over-count outside the window), lack the aggregate,
// or exceed the synthetic-row cap are returned for normal scanning. Mirrors
// manifestFastPath's boundary contract.
func (s *Storage) manifestCountFastPath(files []manifest.FileInfo, startNs, endNs int64, aggField string, writeBlock logstorage.WriteDataBlockFunc) []manifest.FileInfo {
	var remaining []manifest.FileInfo
	served := 0
	for _, fi := range files {
		contained := fi.RowCount > 0 && fi.MinTimeNs > 0 && fi.MaxTimeNs > 0 &&
			fi.MinTimeNs >= startNs && fi.MaxTimeNs <= endNs
		if contained && s.streamSyntheticAggBlocks(fi, aggField, func(db *logstorage.DataBlock) {
			if db != nil && db.RowsCount() > 0 {
				writeBlock(0, db)
			}
		}) {
			served++
			metrics.MetadataOnlyFiles.Inc()
			continue
		}
		remaining = append(remaining, fi)
	}
	if served > 0 {
		logger.Infof("count pushdown: served %d/%d files from manifest aggregates (field=%s), %d remain for S3",
			served, len(files), aggField, len(remaining))
	}
	return remaining
}

// streamSyntheticAggBlocks emits synthetic DataBlocks reproducing file fi's
// distribution of aggField values: each value V repeated LabelAggregates[aggField][V]
// times, plus the empty-value group (RowCount - sum) so rows with no value form the
// "" group exactly as a real scan would. The field column is named and formatted
// IDENTICALLY to the scan path (registry.ResolveFromParquet + FormatField) so the
// downstream pipe groups the same way. Returns false (emitting nothing) when fi has
// no aggregate for the field or its RowCount exceeds the synthetic cap (avoid the
// undercount a cap would cause) — caller then scans it.
func (s *Storage) streamSyntheticAggBlocks(fi manifest.FileInfo, aggField string, emit func(*logstorage.DataBlock)) bool {
	agg := fi.LabelAggregates[aggField]
	if len(agg) == 0 || fi.RowCount <= 0 || fi.RowCount > maxSyntheticRows || emit == nil {
		return false
	}

	fieldCol := aggField
	if m := s.registry.ResolveFromParquet(aggField); m != nil {
		fieldCol = m.InternalName
	}
	tsCol := s.registry.TimestampColumn()
	tsInternal := tsCol
	if m := s.registry.ResolveFromParquet(tsCol); m != nil {
		tsInternal = m.InternalName
	}

	// Deterministic value order, plus the empty group for the rows whose field
	// value was absent (so sum of all groups == RowCount, matching a scan).
	values := make([]string, 0, len(agg)+1)
	for v := range agg {
		values = append(values, v)
	}
	sort.Strings(values)
	counts := make([]int64, len(values))
	var sum int64
	for i, v := range values {
		counts[i] = agg[v]
		sum += agg[v]
	}
	if empty := fi.RowCount - sum; empty > 0 {
		values = append(values, "")
		counts = append(counts, empty)
	}

	tsStep := int64(0)
	if fi.RowCount > 1 && fi.MaxTimeNs > fi.MinTimeNs {
		tsStep = (fi.MaxTimeNs - fi.MinTimeNs) / (fi.RowCount - 1)
	}

	fieldVals := make([]string, 0, syntheticChunkSize)
	tsVals := make([]string, 0, syntheticChunkSize)
	var rowIdx int64
	flush := func() {
		if len(fieldVals) == 0 {
			return
		}
		db := &logstorage.DataBlock{}
		db.SetColumns([]logstorage.BlockColumn{
			{Name: tsInternal, Values: tsVals},
			{Name: fieldCol, Values: fieldVals},
		})
		emit(db)
		fieldVals = make([]string, 0, syntheticChunkSize)
		tsVals = make([]string, 0, syntheticChunkSize)
	}
	for i, v := range values {
		formatted := s.registry.FormatField(fieldCol, v)
		for n := int64(0); n < counts[i]; n++ {
			ts := fi.MinTimeNs + rowIdx*tsStep
			if ts > fi.MaxTimeNs {
				ts = fi.MaxTimeNs
			}
			tsVals = append(tsVals, s.registry.FormatField(tsInternal, ts))
			fieldVals = append(fieldVals, formatted)
			rowIdx++
			if len(fieldVals) >= syntheticChunkSize {
				flush()
			}
		}
	}
	flush()
	return true
}

func (s *Storage) preFilterFiles(ctx context.Context, files []manifest.FileInfo, queryStr string) []manifest.FileInfo {
	// NOTE: we intentionally do NOT use smartCache.FindFilesByTraceID to
	// NARROW the candidate set for trace_id queries. That mapping is a
	// LOWER BOUND — it only contains files whose TraceIDs were RECORDED
	// (which happens at the END of a scan, never for a just-flushed-not-
	// yet-queried file). Narrowing to it silently drops recently-flushed
	// files that are in the manifest and genuinely carry the trace_id,
	// producing a "0 results for minutes-old data that _stream queries
	// still find" parity gap (cf. the traces module fix and
	// TestS3_preFilterFiles_TraceIDCacheHit). The sound narrowing is
	// label + bloom below, which keeps unindexed/recent files. The
	// smartCache remains a DATA cache (L1/L2 chunk bytes) and a
	// parent-child prefetch HINT — never an authoritative file index.
	files = s.filterFilesByLabels(files, queryStr)
	if len(files) == 0 {
		return nil
	}
	return s.bloomFilterFiles(ctx, files, queryStr)
}

// openParquetFile returns a parquet.File for the given FileInfo.
// When a cached footer is available and the query projects few columns,
// it uses S3ReaderAt so parquet-go fetches only the needed column chunks
// via HTTP range requests. Falls back to full download on any error.
//
// Legacy entry point: always uses the adaptive-window stack for the
// projected range-read path. Callers that can arm a plan-then-fetch view
// (queryFile, scanProjectedFieldValues) use openParquetFileWithPlan.
func (s *Storage) openParquetFile(ctx context.Context, fi manifest.FileInfo, projectedCols map[string]bool) (*parquet.File, error) {
	f, _, err := s.openParquetFileInternal(ctx, fi, projectedCols, false)
	return f, err
}

// openParquetFileWithPlan is openParquetFile for callers that arm a
// plan-then-fetch view (S3 Tier-2 items 8/9). When the projected
// range-read path engages and s3.projected_fetch_mode is "planned"
// (default), the returned view is non-nil: the caller derives the exact
// column-chunk ranges of its matched row groups, arms the view via
// armProjectedPlan, and MUST Close it when the file's processing
// completes. "window" mode (the rollback) always returns a nil view and
// the adaptive-window stack — byte-identical to the previous behavior.
func (s *Storage) openParquetFileWithPlan(ctx context.Context, fi manifest.FileInfo, projectedCols map[string]bool) (*parquet.File, *s3reader.PlannedFetchReaderAt, error) {
	usePlanned := s.cfg.S3.ProjectedFetchMode != config.ProjectedFetchModeWindow
	return s.openParquetFileInternal(ctx, fi, projectedCols, usePlanned)
}

func (s *Storage) openParquetFileInternal(ctx context.Context, fi manifest.FileInfo, projectedCols map[string]bool, usePlanned bool) (*parquet.File, *s3reader.PlannedFetchReaderAt, error) {
	// Range-read path: requires footer cache (to know total column count)
	// and a non-empty projection that covers fewer than half the columns.
	if s.footerCache != nil && projectedCols != nil && s.pool != nil {
		if cached, ok := s.footerCache.Get(fi.Key); ok {
			totalCols := len(cached.File.Root().Columns())
			if shouldUseRangeRead(fi.Size, len(projectedCols), totalCols) {
				if usePlanned {
					// S* ladder (v2 slice 1d): warm footer ⇒ plan is
					// metadata-free and beats whole-file at every size.
					metrics.S3PlannedStrategy.Inc("plan-warm-footer")
				}
				if f, view, err := s.openProjectedParquet(ctx, fi, cached.File.Schema(), usePlanned); err == nil {
					return f, view, nil
				}
			}
		} else if usePlanned {
			// S* + footer-cache-gated strategy (v2 slice 1d) — PLANNED mode
			// only; the window default below stays byte-identical. Cold
			// footer: under the per-signal whole-file threshold one
			// whole-file GET (which doubles as the footer-cache warmup —
			// the full-download path below feeds ParseFooterFromData) beats
			// footer-fetch + span RTTs; at or above it, fetch the footer
			// (single tail range read, two-phase for oversize footers) and
			// plan exact spans.
			if fi.Size < s.wholeFileThresholdBytes() {
				metrics.S3PlannedStrategy.Inc("whole-file-warmup")
				// fall through to the full-download path below.
			} else if f, err := s.fetchFooterFile(ctx, fi); err == nil {
				metrics.S3PlannedStrategy.Inc("plan-cold-footer")
				totalCols := len(f.Root().Columns())
				if shouldUseRangeRead(fi.Size, len(projectedCols), totalCols) {
					if pf, view, rErr := s.openProjectedParquet(ctx, fi, f.Schema(), usePlanned); rErr == nil {
						return pf, view, nil
					}
				}
			} else {
				// The planned path needs the footer for its range plan and
				// could not get one — the file is read via the full-download
				// path below instead.
				metrics.S3ProjectedFetchFallback.Inc("no-footer")
			}
		} else if fi.Size >= minFileSizeForPrefetch && len(projectedCols) <= 3 {
			// WINDOW mode, footer cache miss with narrow projection — fetch
			// footer inline (tail range read, singleflight-deduped across
			// concurrent queries hitting the same cold file) then use range
			// reads for only the needed columns instead of downloading the
			// entire file. Unchanged legacy behavior (the rollback path).
			offset := fi.Size - footerPrefetchTail(s.footerPrefetchBytes(), fi.Size)
			if offset < 0 {
				offset = 0
			}
			metrics.S3GetsByPhase.Inc("footer")
			tail, err := s.pool.DownloadRangeDedup(ctx, "footer", fi.Key, offset, fi.Size-offset)
			if err == nil && len(tail) >= 8 {
				if footerLen, fErr := FooterLength(tail[len(tail)-8:]); fErr == nil {
					totalFooterBytes := footerLen + 8
					if totalFooterBytes <= len(tail) {
						footerSlice := tail[len(tail)-totalFooterBytes:]
						if cachedF, _, pErr := ParseFooterFromBytes(fi.Key, footerSlice, fi.Size); pErr == nil {
							s.footerCache.Put(fi.Key, cachedF)
							totalCols := len(cachedF.File.Root().Columns())
							if shouldUseRangeRead(fi.Size, len(projectedCols), totalCols) {
								if f, view, rErr := s.openProjectedParquet(ctx, fi, cachedF.File.Schema(), usePlanned); rErr == nil {
									return f, view, nil
								}
							}
						}
					}
				}
			}
		}
	}

	// Wildcard range-read path (Goal B): when the query has no
	// projection (projectedCols == nil — wildcard `*` or no field
	// filter), fall back to the lazy S3 ReaderAt for large files
	// instead of pulling the whole body into memory. parquet-go
	// fetches column chunks per row group on demand, so peak
	// resident memory stays at working-set-row-group bytes rather
	// than the cumulative-file-bytes that the buffered path pins
	// for the entire open-decode-emit window.
	//
	// Skip this path when L1/L2 cache already has the file
	// (download is free) — the cache adapter inside getFileData
	// returns the cached data, and the buffered Reader is the
	// fast path. Range-read for wildcards on cache hits would
	// add per-row-group HTTP overhead with no memory benefit.
	if s.pool != nil && projectedCols == nil && shouldUseWildcardRangeRead(fi.Size) {
		_, cached := s.memCache.Get(fi.Key)
		if !cached {
			// Reuse the cached footer's schema when available — wildcard
			// opens still pay the footer parse otherwise.
			var cachedSchema *parquet.Schema
			if s.footerCache != nil {
				if cf, ok := s.footerCache.Get(fi.Key); ok && cf.File != nil {
					cachedSchema = cf.File.Schema()
				}
			}
			f, err := s.openRangedParquet(ctx, fi, cachedSchema)
			if err == nil {
				metrics.S3RangeReadsTotal.Inc()
				metrics.ParquetFilesOpened.Inc()
				return f, nil, nil
			}
			// On open failure fall through to the full download
			// path — same defensive pattern as the projected
			// range-read paths above.
		}
	}

	// Full download path (existing behaviour).
	data, err := s.getFileData(ctx, fi.Key, fi.Size)
	if err != nil {
		return nil, nil, fmt.Errorf("get file data %s: %w", fi.Key, err)
	}

	metrics.ParquetFilesOpened.Inc()
	metrics.ParquetColumnBytesRead.Add(len(data))

	// Always create a fresh *parquet.File per query. Parquet-go's ColumnChunk
	// and Pages readers hold internal state that is not safe to reuse across
	// queries. The footer cache is only used for metadata (column count, footer
	// size) in the range-read path above. ParseFooterFromData feeds the footer
	// cache so the whole-file download IS the warmup (S* ladder, slice 1d):
	// the file's NEXT projected read takes the warm-footer plan route.
	cached, f, parseErr := ParseFooterFromData(fi.Key, data)
	if parseErr != nil {
		return nil, nil, parseErr
	}
	if s.footerCache != nil {
		s.footerCache.Put(fi.Key, cached)
	}
	return f, nil, nil
}

func (s *Storage) queryFile(ctx context.Context, fi manifest.FileInfo, startNs, endNs int64, queryStr string, pipeFields []string, writeBlock logstorage.WriteDataBlockFunc) error {
	projectedCols := queryColumns(queryStr, s.registry, pipeFields)

	// Hits/stats fast path: when the endpoint only needs timestamps (set via
	// context hint) and the query has no column-specific filters, project only
	// the timestamp column to avoid deserializing all row data.
	if projectedCols == nil && storage.IsTimestampOnly(ctx) {
		// Timestamp-only is safe ONLY for an UNFILTERED count/hits. A row filter
		// must be evaluated against its columns at scan time — a free-text _msg
		// word filter (e.g. `error | stats count()`) has no bloom to push down,
		// so reducing to timestamp-only drops _msg and the filter silently matches
		// zero rows (cold full-text search returned 0). VL prepends `_time:[...]`
		// to every query, so look past it: when any other filter term remains,
		// keep reading all columns (file/row-group pushdown still prunes the work).
		filterPart := queryStr
		if idx := strings.Index(queryStr, " | "); idx >= 0 {
			filterPart = strings.TrimSpace(queryStr[:idx])
		}
		if !hasContentFilter(filterPart) {
			projectedCols = map[string]bool{s.registry.TimestampColumn(): true}
		}
	}
	// Field-enumerating pipes (field_names / field_values / facets /
	// block_stats) must see every column the row carries — projection
	// narrowing would truncate the answer. The adapter signals this via
	// storage.WithAllFieldsHint; force read-all here. Mirror of the
	// equivalent path in lakehouse-traces/.../parquets3/storage_query.go.
	if storage.IsAllFields(ctx) {
		projectedCols = nil
	}

	// Reserve cumulative file-resident bytes against the process-wide budget
	// BEFORE opening (and possibly downloading) the parquet file. The file
	// body stays resident — wired through bytes.NewReader(data) into the
	// parquet.File — for the entire open-decode-emit window, NOT just the
	// download. With 16 file workers this is the dominant retention path
	// (7-day heap-diff: io.ReadAll held 808 MiB at OOM peak; that's
	// 512 MiB L1 cache plus 16 workers × ~30 MiB file bodies all pinned
	// concurrently). The budget naturally serializes the worker pool when
	// cumulative file sizes exceed the cap; smaller files admit more
	// concurrency, larger files admit fewer. See defaultMaxFileResidentBytes
	// in query_memory_budget.go for the heap-diff rationale.
	//
	// Range-read paths (footer-cache hit + narrow projection) DO NOT pull
	// the whole body, so the budget would over-account; the file ranges
	// are page-sized and fit in scratch. For correctness across both
	// paths we acquire here at the outer queryFile boundary and trust
	// openParquetFile to either range-read or full-download; the budget
	// is a soft ceiling that bounds the wildcard-fanout case (which
	// always full-downloads, per queryColumns returning nil for `*`).
	relFB, fbErr := acquireFileBudget(ctx, fi.Size)
	if fbErr != nil {
		return fbErr
	}
	defer relFB()

	// Plan-then-fetch (S3 Tier-2): on the projected range-read path the
	// returned view is armed AFTER row-group pruning below with the exact
	// coalesced column-chunk ranges, replacing the speculative window.
	// Close releases the fetched spans and their memory-budget charge.
	f, planned, err := s.openParquetFileWithPlan(ctx, fi, projectedCols)
	if err != nil {
		return err
	}
	if planned != nil {
		defer func() { _ = planned.Close() }()
	}

	if s.labelIndex.Len() == 0 {
		s.updateLabelIndex(f)
	}
	s.updateColumnStats(fi.Key, f)
	s.enrichManifestFromFooter(fi, f)

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
	// The footer ORDINAL of each matched row group travels with it — the
	// plan-then-fetch arming below derives the matched groups' column-chunk
	// byte ranges from meta.RowGroups[idx].
	var matchedRGs []indexedRowGroup
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
		matchedRGs = append(matchedRGs, indexedRowGroup{idx: rgIdx, rg: rg})
	}

	// Sort by estimated cost (smallest first) so workers finish small RGs
	// quickly and pick up larger ones, improving load balance.
	sort.Slice(matchedRGs, func(i, j int) bool {
		return matchedRGs[i].rg.NumRows() < matchedRGs[j].rg.NumRows()
	})

	// Metadata-only fast path: when projecting only the timestamp column
	// (stats/hits on wildcard query), row groups that are fully within the
	// query time range don't need any data reads — emit synthetic DataBlocks
	// using row counts from Parquet metadata.
	tsOnly := len(projectedCols) == 1 && projectedCols[s.registry.TimestampColumn()]
	if tsOnly && tsIdx >= 0 {
		var deferred []indexedRowGroup
		for _, m := range matchedRGs {
			if rowGroupFullyInRange(m.rg, tsIdx, startNs, endNs) {
				metrics.ParquetRowGroupsScanned.Inc()
				db := s.syntheticTimestampBlock(m.rg, tsIdx, startNs, endNs)
				if db != nil && db.RowsCount() > 0 {
					writeBlock(0, db)
				}
			} else {
				deferred = append(deferred, m)
			}
		}
		matchedRGs = deferred
	}

	// Plan-then-fetch: with the surviving row groups known, fetch the exact
	// coalesced byte ranges of (projected + push-down filter) column chunks
	// concurrently. Decode reads below are then served from the fetched
	// spans — no speculative window, no per-window waste.
	if planned != nil && len(matchedRGs) > 0 {
		rgIdxs := make([]int, len(matchedRGs))
		for i, m := range matchedRGs {
			rgIdxs[i] = m.idx
		}
		s.armProjectedPlan(ctx, planned, f, rgIdxs, projectedCols, pdf)
	}

	// Process matched row groups SERIALLY within a single file. The outer
	// file-worker pool already gives us read concurrency across files; adding
	// up-to-8x row-group parallelism on top of 16 file workers means up to
	// 128 concurrent row-group decoders, each holding multi-MB column buffers
	// — easily exceeding the 2 GiB container limit on wildcard queries.
	// VL's searchParallel keeps fanout bounded by workersCount (matches
	// cgroup.AvailableCPUs), and our adapter must do the same.
	for _, m := range matchedRGs {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		metrics.ParquetRowGroupsScanned.Inc()
		if err := s.readOneRowGroup(f, m.rg, startNs, endNs, projectedCols, pdf, writeBlock, traceIDsPtr); err != nil {
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
	// per the near-OOM heap-diff: readMapColumnToBlockCols=154 MiB flat,
	// parquetValueToInterface=104 MiB, readScalarColumnFormatted=36 MiB).
	// Without this gate, 16 file workers concurrently decoding wide-schema
	// row groups across a 2-day wildcard scan over 200+ files produces
	// 500+ MiB of transient memory on top of the 512 MiB cache, deterministic
	// OOM on a 2 GiB container. Mirrors VL's partitionSearchConcurrencyLimitCh
	// pattern (deps/VictoriaLogs/lib/logstorage/storage_search.go:1424).
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
	// Hoist the scalar-name lookup map out of the loop. Previously this
	// was allocated fresh per row, contributing one allocation per row at
	// scan time. We clear via map-delete (faster than make-new for stable
	// schemas because the backing buckets stay hot in L1).
	scalarFieldNames := make(map[string]bool)
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

		// Grow bitmap to match column count; clear previous bits.
		if cap(seenBitmap) >= len(cols) {
			seenBitmap = seenBitmap[:len(cols)]
		} else {
			seenBitmap = make([]bool, len(cols), len(cols)*2)
		}
		for i := range seenBitmap {
			seenBitmap[i] = false
		}

		for k := range scalarFieldNames {
			delete(scalarFieldNames, k)
		}
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
					if scalarFieldNames[k] {
						continue
					}
					attrName := bytesutil.InternString(prefix + k)
					idx := getCol(attrName)
					// Grow bitmap for new columns discovered via MAP.
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
			idx := getCol(internalName)
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
			cols[idx].values = append(cols[idx].values, formatted)
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
	case "resource.attributes", "log.attributes", "span.attributes", "scope.attributes":
		return ""
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
	// Dedicated columns (Tier 1) — emit each ONLY when non-empty. Dual-read
	// invariant: new files carry the value in the column (map cleared at
	// ingest) → emitted here; OLD files (pre-promotion) carry it in the map
	// with an empty column → skipped here, emitted by the map loop below.
	// Exactly one emission either way — no dup, no lost old-file values.
	buf = appendIfSet(buf, "container.id", r.ContainerID)
	buf = appendIfSet(buf, "service.instance.id", r.ServiceInstanceID)
	buf = appendIfSet(buf, "service.version", r.ServiceVersion)
	buf = appendIfSet(buf, "exception.type", r.ExceptionType)
	buf = appendIfSet(buf, "exception.message", r.ExceptionMessage)
	buf = appendIfSet(buf, "k8s.cluster.name", r.K8sClusterName)
	buf = appendIfSet(buf, "telemetry.sdk.name", r.TelemetrySDKName)
	buf = appendIfSet(buf, "telemetry.sdk.language", r.TelemetrySDKLang)
	buf = appendIfSet(buf, "telemetry.sdk.version", r.TelemetrySDKVer)
	buf = appendIfSet(buf, "cloud.account.id", r.CloudAccountID)
	buf = appendIfSet(buf, "cloud.provider", r.CloudProvider)
	buf = appendIfSet(buf, "os.type", r.OSType)
	buf = appendIfSet(buf, "host.arch", r.HostArch)
	buf = appendIfSet(buf, "process.runtime.name", r.ProcessRuntimeName)
	buf = appendIfSet(buf, "process.runtime.version", r.ProcessRuntimeVer)
	for k, v := range r.ResourceAttributes {
		buf = append(buf, field{k, v})
	}
	for k, v := range r.LogAttributes {
		buf = append(buf, field{k, v})
	}
	return buf
}

// appendIfSet appends a {name,value} field only when value is non-empty — the
// dual-read helper for promoted dedicated columns (see logRowToFields).
func appendIfSet(buf []field, name, value string) []field {
	if value == "" {
		return buf
	}
	return append(buf, field{name, value})
}

func traceRowToFields(r *schema.TraceRow, buf []field) []field {
	buf = append(buf,
		field{"_time", r.TimestampUnixNano},
		field{"start_time", r.StartTimeUnixNano},
		field{"trace_id", r.TraceID},
		field{"span_id", r.SpanID},
		field{"parent_span_id", r.ParentSpanID},
		field{"name", r.SpanName},
		field{"kind", r.SpanKind},
		field{"status_code", r.StatusCode},
		field{"status_message", r.StatusMessage},
		field{"duration", r.DurationNs},
		field{"service.name", r.ServiceName},
		field{"otel.library.name", r.ScopeName},
		field{"deployment.environment", r.DeployEnv},
		field{"cloud.region", r.CloudRegion},
		field{"host.name", r.HostName},
		field{"k8s.namespace.name", r.K8sNamespaceName},
		field{"k8s.deployment.name", r.K8sDeploymentName},
		field{"k8s.node.name", r.K8sNodeName},
		field{"http.method", r.HTTPMethod},
		field{"http.status_code", r.HTTPStatusCode},
		field{"http.url", r.HTTPUrl},
		field{"db.system", r.DBSystem},
		field{"db.statement", r.DBStatement},
	)
	for k, v := range r.ResourceAttributes {
		if !tracePromotedResourceKeys[k] {
			buf = append(buf, field{k, v})
		}
	}
	for k, v := range r.SpanAttributes {
		if !tracePromotedSpanKeys[k] {
			buf = append(buf, field{k, v})
		}
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
		if isNegatedPredicate(queryStr, col.InternalName) || isNegatedPredicate(queryStr, col.ParquetColumn) {
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

func (s *Storage) bloomFilterFiles(ctx context.Context, files []manifest.FileInfo, queryStr string) []manifest.FileInfo {
	if s.bloomCache == nil || queryStr == "" {
		return files
	}
	if containsOrOperatorAST(queryStr) {
		// OR queries used to bypass bloom filtering entirely. Try to
		// evaluate each OR branch independently via the bloom index
		// and union the matching files. Falls through to returning
		// all files when the filter shape doesn't fit the supported
		// pattern (top-level OR of simple field=value predicates,
		// optionally distributed with surrounding AND clauses).
		if result, ok := s.bloomFilterFilesByOrBranches(ctx, files, queryStr); ok {
			return result
		}
		return files
	}

	// Build per-column candidate value sets. A column may have multiple
	// values via field:in(v1,v2,...) — VT's spans-lookup query uses this
	// shape (trace_id:in(t1,t2,t3)). Per-column semantics is "any-of"
	// (OR within a column's values), and AND across columns.
	var perColumn []bloomColumnValues
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
			perColumn = append(perColumn, bloomColumnValues{Column: col.ParquetColumn, Values: vals})
		}
	}
	if len(perColumn) == 0 {
		return files
	}

	metrics.BloomQueriesTotal.Inc("attempt")

	byPartition := make(map[string][]manifest.FileInfo)
	for _, fi := range files {
		// manifest.ExtractPartition (pure "dt=.../hour=HH", no prefix) — the pmeta
		// facet AND the _bloom.bin persist path are keyed by it. partitionFromKey
		// keeps the key prefix, which made the facet lookup always miss and the
		// bloomS3Loader fallback build a double-prefixed S3 path.
		partition := manifest.ExtractPartition(fi.Key)
		byPartition[partition] = append(byPartition[partition], fi)
	}

	var result []manifest.FileInfo
	for partition, pFiles := range byPartition {
		keys := make([]string, len(pFiles))
		for i, fi := range pFiles {
			keys[i] = fi.Key
		}

		// pmeta bloom read-flip: the in-RAM bloom facet first, the legacy
		// _bloom.bin partition index (bloomCache) as the fallback. ok=false →
		// no bloom available for this partition, keep its files.
		matchSet, ok := s.bloomColumnIntersect(ctx, partition, keys, perColumn)
		if !ok {
			result = append(result, pFiles...)
			continue
		}

		before := len(pFiles)
		var bytesAvoided int64
		for _, fi := range pFiles {
			if matchSet[fi.Key] {
				result = append(result, fi)
			} else {
				bytesAvoided += fi.Size
			}
		}
		skipped := before - len(matchSet)
		if skipped > 0 {
			metrics.ParquetFilesSkipped.Add(skipped)
			metrics.BloomFilesSkipped.Add(skipped)
			metrics.BloomBytesAvoided.Add(int(bytesAvoided))
			metrics.ParquetBloomChecks.Add("miss", skipped)
		}
		if len(matchSet) > 0 {
			metrics.ParquetBloomChecks.Add("hit", len(matchSet))
		}
	}
	return result
}

// bloomFilterFilesByOrBranches evaluates an OR-shaped query against
// the bloom index per branch and unions the matching files. Returns
// (files, false) when the filter shape isn't supported — caller
// should fall back to its existing logic.
//
// Translates branch field names through the registry (internal ↔
// parquet) so we use the actual parquet column the bloom index keys
// on. Branches with no bloom-checkable columns force a fall-through
// for that branch — we include all files for the partition (because
// the bloom can't prove absence).
func (s *Storage) bloomFilterFilesByOrBranches(ctx context.Context, files []manifest.FileInfo, queryStr string) ([]manifest.FileInfo, bool) {
	filter := parseFilterFromQueryStr(queryStr)
	if filter == nil {
		return files, false
	}
	branches := FilterExtractOrBranches(filter)
	if len(branches) == 0 {
		return files, false
	}

	// Translate each branch's BranchCheck into bloomindex.ColumnCheck,
	// using the registry to find the parquet column name for fields
	// that have a bloom filter configured.
	branchChecks := make([][]bloomindex.ColumnCheck, 0, len(branches))
	for _, branch := range branches {
		var checks []bloomindex.ColumnCheck
		var hasUnindexedField bool
		for _, bc := range branch {
			col := s.resolveBloomColumn(bc.FieldName)
			if col == "" {
				hasUnindexedField = true
				break
			}
			checks = append(checks, bloomindex.ColumnCheck{Column: col, Value: bc.Value})
		}
		if hasUnindexedField || len(checks) == 0 {
			// One branch can't be bloom-evaluated — every file is a
			// potential match for that branch, so the union must
			// include every file. Bail out and let the caller fall
			// through to its full-files default.
			return files, false
		}
		branchChecks = append(branchChecks, checks)
	}

	metrics.BloomQueriesTotal.Inc("attempt")

	byPartition := make(map[string][]manifest.FileInfo)
	for _, fi := range files {
		// manifest.ExtractPartition (pure "dt=.../hour=HH", no prefix) — the pmeta
		// facet AND the _bloom.bin persist path are keyed by it. partitionFromKey
		// keeps the key prefix, which made the facet lookup always miss and the
		// bloomS3Loader fallback build a double-prefixed S3 path.
		partition := manifest.ExtractPartition(fi.Key)
		byPartition[partition] = append(byPartition[partition], fi)
	}

	var result []manifest.FileInfo
	for partition, pFiles := range byPartition {
		keys := make([]string, len(pFiles))
		for i, fi := range pFiles {
			keys[i] = fi.Key
		}

		// pmeta bloom read-flip (OR-branch): the in-RAM bloom facet is consulted
		// first; the legacy _bloom.bin partition index is the fallback. ok=false →
		// no bloom for this partition, keep its files.
		unionMatch, ok := s.bloomUnionMatch(ctx, partition, keys, branchChecks)
		if !ok {
			result = append(result, pFiles...)
			continue
		}

		before := len(pFiles)
		var bytesAvoided int64
		for _, fi := range pFiles {
			if unionMatch[fi.Key] {
				result = append(result, fi)
			} else {
				bytesAvoided += fi.Size
			}
		}
		skipped := before - len(unionMatch)
		if skipped > 0 {
			metrics.ParquetFilesSkipped.Add(skipped)
			metrics.BloomFilesSkipped.Add(skipped)
			metrics.BloomBytesAvoided.Add(int(bytesAvoided))
			metrics.ParquetBloomChecks.Add("miss", skipped)
		}
		if len(unionMatch) > 0 {
			metrics.ParquetBloomChecks.Add("hit", len(unionMatch))
		}
	}
	return result, true
}

// bloomColumnValues is one bloom-indexed column with its candidate values from the
// query (any-of within the column; AND across columns).
type bloomColumnValues struct {
	Column string
	Values []string
}

// bloomColumnIntersect returns the file keys that MAY match every column (AND
// across columns, any-of within a column's values). ok=false means no bloom is
// available for the partition, so the caller keeps every file. pmeta bloom
// read-flip: the in-RAM bloom facet is consulted first; the legacy _bloom.bin
// partition index (bloomCache) is the fallback.
func (s *Storage) bloomColumnIntersect(ctx context.Context, partition string, keys []string, perColumn []bloomColumnValues) (map[string]bool, bool) {
	if s.catalog != nil {
		if match, ok := s.facetBloomColumnIntersect(partition, keys, perColumn); ok {
			return match, true
		}
	}
	if s.bloomCache == nil {
		return nil, false
	}
	idx, err := s.bloomCache.Get(ctx, partition)
	if err != nil || idx == nil {
		return nil, false
	}
	var intersection map[string]bool
	for _, bc := range perColumn {
		colMatch := make(map[string]bool)
		for _, v := range bc.Values {
			for _, k := range idx.MayContainAll(keys, []bloomindex.ColumnCheck{{Column: bc.Column, Value: v}}) {
				colMatch[k] = true
			}
		}
		if intersection == nil {
			intersection = colMatch
		} else {
			for k := range intersection {
				if !colMatch[k] {
					delete(intersection, k)
				}
			}
		}
		if len(intersection) == 0 {
			break
		}
	}
	if intersection == nil {
		intersection = make(map[string]bool)
	}
	return intersection, true
}

// facetBloomColumnIntersect is bloomColumnIntersect over the pmeta bloom facet:
// per column, union the per-value BloomMayContain matches (any-of), then intersect
// across columns. ok=false if the facet lacks the partition's bloom (caller falls
// back to the legacy index). Blooms never false-negative, so a file that holds the
// values is never dropped.
func (s *Storage) facetBloomColumnIntersect(partition string, keys []string, perColumn []bloomColumnValues) (map[string]bool, bool) {
	var intersection map[string]bool
	for _, bc := range perColumn {
		colMatch := make(map[string]bool)
		for _, v := range bc.Values {
			sub, ok := s.catalog.BloomMayContain(partition, keys, bc.Column, v)
			if !ok {
				return nil, false
			}
			for _, k := range sub {
				colMatch[k] = true
			}
		}
		if intersection == nil {
			intersection = colMatch
		} else {
			for k := range intersection {
				if !colMatch[k] {
					delete(intersection, k)
				}
			}
		}
		if len(intersection) == 0 {
			break
		}
	}
	if intersection == nil {
		intersection = make(map[string]bool)
	}
	return intersection, true
}

// bloomUnionMatch returns the file keys that MAY satisfy at least one OR branch
// (the union of per-branch AND-of-checks). ok=false means no bloom is available for
// the partition, so the caller keeps every file. pmeta bloom read-flip: the in-RAM
// bloom facet is consulted first; the legacy _bloom.bin partition index (bloomCache)
// is the fallback for partitions the facet doesn't carry (cold bundle / pre-pmeta).
func (s *Storage) bloomUnionMatch(ctx context.Context, partition string, keys []string, branchChecks [][]bloomindex.ColumnCheck) (map[string]bool, bool) {
	if s.catalog != nil {
		if union, ok := s.facetBloomUnionMatch(partition, keys, branchChecks); ok {
			return union, true
		}
	}
	if s.bloomCache == nil {
		return nil, false
	}
	idx, err := s.bloomCache.Get(ctx, partition)
	if err != nil || idx == nil {
		return nil, false
	}
	union := make(map[string]bool, len(keys))
	for _, checks := range branchChecks {
		for _, k := range idx.MayContainAll(keys, checks) {
			union[k] = true
		}
	}
	return union, true
}

// facetBloomUnionMatch is bloomUnionMatch over the pmeta bloom facet. For each OR
// branch it narrows the key set through the branch's checks (AND) via per-check
// BloomMayContain, then unions across branches. ok=false if the facet lacks the
// partition's bloom (so the caller falls back to the legacy index). A bloom never
// false-negatives, so a file that holds the values is never dropped.
func (s *Storage) facetBloomUnionMatch(partition string, keys []string, branchChecks [][]bloomindex.ColumnCheck) (map[string]bool, bool) {
	union := make(map[string]bool, len(keys))
	for _, checks := range branchChecks {
		matching := keys
		for _, c := range checks {
			sub, ok := s.catalog.BloomMayContain(partition, matching, c.Column, c.Value)
			if !ok {
				return nil, false
			}
			matching = sub
			if len(matching) == 0 {
				break
			}
		}
		for _, k := range matching {
			union[k] = true
		}
	}
	return union, true
}

// resolveBloomColumn maps a query field name (internal or parquet
// form) to the parquet column name when the column has a bloom
// filter. Returns "" when no bloom is configured for the field.
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

	// Per-column candidate value sets, with the traces module's semantics: a
	// query like trace_id:in(t1,t2) means the file may match if it contains ANY
	// of the listed values (OR within a column), AND across columns. Flattening
	// every value into one AND list — the old shape here — wrongly required a
	// file to contain ALL in() values and skipped files holding just one (missing
	// results). Negated predicates are excluded: a bloom proves presence-may,
	// never absence, so `-field:x` cannot prune by bloom.
	var perColumn []bloomColumnValues
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
			perColumn = append(perColumn, bloomColumnValues{Column: col.ParquetColumn, Values: vals})
		}
	}
	if len(perColumn) == 0 {
		return false
	}

	// pmeta bloom read-flip: consult the in-RAM bloom facet (loaded from the
	// partition `_bloom.bin` bundle) before a per-file `.bloom` S3 GET. A bloom
	// filter never false-negatives, so a file that actually holds the value is never
	// excluded by either path — this can only change pruning efficiency, never
	// correctness. The facet is built from the same columnValues + fpRate as the
	// per-file bloom. Falls back to the `.bloom` download for any partition the
	// bundle doesn't carry (cold bundle / pre-pmeta files).
	if s.catalog != nil {
		partition := manifest.ExtractPartition(fi.Key)
		usable, excluded := true, false
	facetLoop:
		for _, bc := range perColumn {
			anyMatch := false
			for _, v := range bc.Values {
				matched, ok := s.catalog.BloomMayContain(partition, []string{fi.Key}, bc.Column, v)
				if !ok {
					usable = false
					break facetLoop
				}
				if len(matched) > 0 {
					anyMatch = true
					break
				}
			}
			if !anyMatch {
				excluded = true
				break
			}
		}
		if usable {
			if excluded {
				metrics.ParquetBloomChecks.Inc("facet_bloom_skip")
				return true
			}
			return false
		}
	}

	bloomKey := fi.Key + ".bloom"

	var idx *bloomindex.Index
	if s.fileBloomCache != nil {
		if cached, ok := s.fileBloomCache.Get(bloomKey); ok {
			if cached == nil {
				return false
			}
			idx = cached
		}
	}
	if idx == nil {
		metrics.S3GetsByPhase.Inc("bloom")
		data, err := s.pool.DownloadDedup(ctx, "bloom", bloomKey)
		if err != nil || len(data) == 0 {
			if s.fileBloomCache != nil {
				s.fileBloomCache.Put(bloomKey, nil)
			}
			return false
		}
		parsed, err := bloomindex.Unmarshal(data)
		if err != nil {
			if s.fileBloomCache != nil {
				s.fileBloomCache.Put(bloomKey, nil)
			}
			return false
		}
		idx = parsed
		if s.fileBloomCache != nil {
			s.fileBloomCache.Put(bloomKey, idx)
		}
	}

	// Same AND-across-columns / OR-within-column semantics on the legacy path.
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

func partitionFromKey(key string) string {
	// Extract "dt=YYYY-MM-DD/hour=HH" from file key
	idx := strings.Index(key, "/hour=")
	if idx < 0 {
		// Try daily partition
		if dtIdx := strings.Index(key, "dt="); dtIdx >= 0 {
			end := strings.IndexByte(key[dtIdx:], '/')
			if end < 0 {
				return key[dtIdx:]
			}
			return key[dtIdx : dtIdx+end]
		}
		return key
	}
	hourEnd := idx + len("/hour=")
	for hourEnd < len(key) && key[hourEnd] != '/' {
		hourEnd++
	}
	return key[:hourEnd]
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
	idx := fieldTokenIndex(query, prefix)
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

// fieldTokenIndex returns the index of the first occurrence of prefix in query
// that starts at a field-token boundary — i.e. not immediately preceded by a
// field-name character ([A-Za-z0-9_.]). Without this, a `name:=` pattern
// substring-matches inside `service.name:="x"` and extracts ANOTHER field's
// value — the bloom pre-filter then builds a bogus span.name=x check and wrongly
// excludes files that do contain x (a bloom false-negative, i.e. missing
// results). Returns -1 when no boundary occurrence exists.
func fieldTokenIndex(query, prefix string) int {
	for from := 0; ; {
		idx := strings.Index(query[from:], prefix)
		if idx < 0 {
			return -1
		}
		idx += from
		if idx == 0 {
			return 0
		}
		p := query[idx-1]
		// Field-name characters per LogsQL usage in this codebase, matching
		// extractQuotedOp's set: schema fields include ':' (resource_attr:service.name)
		// and '-' is legal in attr keys. Treating more characters as field chars is
		// the safe direction — it only suppresses an extraction (less pruning),
		// never fabricates one (which could false-exclude).
		isFieldChar := p == '_' || p == '.' || p == ':' || p == '-' ||
			(p >= 'a' && p <= 'z') || (p >= 'A' && p <= 'Z') || (p >= '0' && p <= '9')
		if !isFieldChar {
			return idx
		}
		from = idx + 1
	}
}

func extractExactMatch(query, fieldName string) string {
	// Quoted patterns: trace_id:="abc" or trace_id:"abc"
	quotedPatterns := []string{
		fieldName + `:="`,
		fieldName + `:"`,
	}
	for _, prefix := range quotedPatterns {
		idx := fieldTokenIndex(query, prefix)
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
	if idx := fieldTokenIndex(query, unquotedPrefix); idx >= 0 {
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

	// Fast path: use inverted label index for exact-match checks.
	// If ALL checks are exact-match and we get index hits, intersect candidate
	// keys to get the result set in O(candidates) instead of O(files).
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

// filterByLabelIndex tries to use the manifest's inverted label index for O(1)
// file lookup. Returns nil if the index can't handle this query (non-exact
// checks, missing index entries), falling back to the O(N) scan.
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

	// Defense in depth: files that have no Labels at all (an
	// unindexed file — e.g. one whose metadata sidecar write failed,
	// or that landed before the indexer reached it) MUST stay in
	// the candidate set. Without this branch, an unindexed file
	// gets silently excluded by the inverted-index fast path and
	// any row matching the filter inside that file gets undercounted.
	// The row-level filter still runs downstream, so including these
	// is the conservative correct answer: include and let the actual
	// match decide. Sibling fileLabelsMatch loop above already uses
	// the same "Labels==nil → include" convention, so this keeps the
	// two file-narrowing paths consistent.
	var result []manifest.FileInfo
	for _, fi := range files {
		if candidateKeys[fi.Key] || fi.Labels == nil {
			result = append(result, fi)
		}
	}

	skipped := len(files) - len(result)
	if skipped > 0 {
		metrics.ParquetRowGroupsSkipped.Inc("label_index")
		logger.Infof("label index fast-path: matched %d/%d files (kept %d unindexed)",
			len(result), len(files), countUnindexedFiles(files))
	}
	return result
}

// countUnindexedFiles is purely diagnostic — surfaces in the
// label-index log line so an operator can tell at a glance whether
// the inverted index has full coverage (zero unindexed) or whether
// a chunk of files are riding the conservative include-anyway path.
func countUnindexedFiles(files []manifest.FileInfo) int {
	n := 0
	for _, fi := range files {
		if fi.Labels == nil {
			n++
		}
	}
	return n
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

// columnIndexTimeBounds returns the row group's true timestamp bounds by
// aggregating min/max across EVERY page of the timestamp column index.
//
// Parquet pages within a row group are NOT guaranteed to be sorted by the
// timestamp column — even for logs the columnar writer can reorder rows to
// improve compression (and the (stream_id, timestamp) row order makes
// out-of-order pages the norm). Taking MinValue(0) and MaxValue(N-1) as
// row-group bounds understates the range whenever the smallest/largest
// timestamps live in a middle page: rowGroupMatchesTimeRange skipped matching
// row groups, rowGroupFullyInRange wrongly declared FULL containment (emitting
// out-of-range rows), and enrichManifestFromFooter / enrichFromCachedFooter
// landed wrong bounds in the manifest. All those helpers now share this
// aggregate scan — mirroring rowGroupMatchesFilter / detectConstantColumns.
// Keep in sync with the traces module
// (lakehouse-traces/internal/storage/parquets3/storage_query.go). Callers must
// ensure idx.NumPages() > 0.
func columnIndexTimeBounds(idx parquet.ColumnIndex) (minNs, maxNs int64) {
	minNs = idx.MinValue(0).Int64()
	maxNs = idx.MaxValue(0).Int64()
	for p := 1; p < idx.NumPages(); p++ {
		if v := idx.MinValue(p).Int64(); v < minNs {
			minNs = v
		}
		if v := idx.MaxValue(p).Int64(); v > maxNs {
			maxNs = v
		}
	}
	return minNs, maxNs
}

// rowGroupFullyInRange returns true when the row group's timestamp range
// is entirely contained within [startNs, endNs]. This means every row in
// the group is within the query range and no per-row filtering is needed.
// The bounds MUST be the aggregate across all pages (columnIndexTimeBounds):
// the old MinValue(0)/MaxValue(N-1) form declared full containment for row
// groups whose out-of-range extremes lived in middle pages, emitting rows
// OUTSIDE the query window. Locked by TestPageAggregateBounds_OutOfOrderPages.
func rowGroupFullyInRange(rg parquet.RowGroup, tsColIdx int, startNs, endNs int64) bool {
	cols := rg.ColumnChunks()
	if tsColIdx >= len(cols) {
		return false
	}
	idx, err := cols[tsColIdx].ColumnIndex()
	if err != nil || idx == nil {
		return false
	}
	if idx.NumPages() == 0 {
		return false
	}
	rgMin, rgMax := columnIndexTimeBounds(idx)
	return rgMin >= startNs && rgMax <= endNs
}

// syntheticTimestampBlock creates a DataBlock with NumRows rows containing
// evenly distributed timestamps derived from row group metadata. Used for
// stats/hits queries on wildcard where the row group is fully in range,
// avoiding any data column reads.
func (s *Storage) syntheticTimestampBlock(rg parquet.RowGroup, tsColIdx int, startNs, endNs int64) *logstorage.DataBlock {
	n := int(rg.NumRows())
	if n == 0 {
		return nil
	}

	cols := rg.ColumnChunks()
	idx, err := cols[tsColIdx].ColumnIndex()
	if err != nil || idx == nil || idx.NumPages() == 0 {
		return nil
	}
	// Aggregate across all pages — see columnIndexTimeBounds. Positional
	// bounds would compress the synthetic timestamps into a subrange that
	// misses the true extremes.
	rgMin, rgMax := columnIndexTimeBounds(idx)

	tsCol := s.registry.TimestampColumn()
	internalName := tsCol
	if m := s.registry.ResolveFromParquet(tsCol); m != nil {
		internalName = m.InternalName
	}

	values := make([]string, n)
	if n == 1 {
		values[0] = s.registry.FormatField(internalName, rgMin)
	} else {
		step := (rgMax - rgMin) / int64(n-1)
		if step == 0 {
			step = 1
		}
		for i := range values {
			ts := rgMin + int64(i)*step
			if ts > rgMax {
				ts = rgMax
			}
			values[i] = s.registry.FormatField(internalName, ts)
		}
	}

	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{{Name: internalName, Values: values}})
	return db
}

// enrichManifestFromFooter populates RowCount and precise MinTimeNs/MaxTimeNs
// in the manifest for a file using its Parquet row group metadata. This ensures
// subsequent queries can use the manifest-only fast path.
func (s *Storage) enrichManifestFromFooter(fi manifest.FileInfo, f *parquet.File) {
	if fi.RowCount > 0 {
		return
	}
	var totalRows int64
	var minTs, maxTs int64
	tsIdx := findColumnIndex(f.Root(), s.registry.TimestampColumn())
	for _, rg := range f.RowGroups() {
		totalRows += rg.NumRows()
		if tsIdx < 0 {
			continue
		}
		cols := rg.ColumnChunks()
		if tsIdx >= len(cols) {
			continue
		}
		idx, err := cols[tsIdx].ColumnIndex()
		if err != nil || idx == nil || idx.NumPages() == 0 {
			continue
		}
		// Aggregate across all pages — see columnIndexTimeBounds. Positional
		// bounds would land an understated time range in the manifest and
		// break range pruning for every later query on this file.
		rgMin, rgMax := columnIndexTimeBounds(idx)
		if minTs == 0 || rgMin < minTs {
			minTs = rgMin
		}
		if rgMax > maxTs {
			maxTs = rgMax
		}
	}
	if totalRows > 0 {
		s.manifest.EnrichFileMetadata(fi.Key, totalRows, minTs, maxTs)
	}
}

// Synthetic manifest block sizing.
//
//   - syntheticChunkSize bounds the per-block allocation so a multi-million
//     row file no longer triggers a single huge []string allocation.
//   - maxSyntheticRows is a defense-in-depth cap on the total row count
//     emitted per file from the manifest fast path. Previously this was
//     50M which could still allocate ~1GB of strings if the registry's
//     timestamp formatter produced long values.
const (
	syntheticChunkSize = 10_000
	maxSyntheticRows   = 1_000_000
)

// syntheticManifestBlock creates a DataBlock with fi.RowCount rows using
// timestamps distributed across [MinTimeNs, MaxTimeNs] from manifest metadata.
//
// Prefer streamSyntheticManifestBlocks for query-path callers — it emits
// multiple smaller blocks instead of materializing the full row count in
// one slice. This single-block variant is preserved for legacy callers
// (tests/benchmarks) and clamps to syntheticChunkSize to avoid surprise
// allocations.
func (s *Storage) syntheticManifestBlock(fi manifest.FileInfo) *logstorage.DataBlock {
	n := int(fi.RowCount)
	if n == 0 {
		return nil
	}
	if n > syntheticChunkSize {
		n = syntheticChunkSize
	}
	return s.buildSyntheticChunk(fi, 0, n)
}

// streamSyntheticManifestBlocks emits one or more DataBlocks covering
// fi.RowCount rows, each of size <= syntheticChunkSize. Total row count
// is capped at maxSyntheticRows as a safety net against pathological
// manifest entries (the manifest fast path is metadata-only, so a wildly
// inflated RowCount would otherwise allocate proportionally).
func (s *Storage) streamSyntheticManifestBlocks(fi manifest.FileInfo, emit func(*logstorage.DataBlock)) {
	total := int(fi.RowCount)
	if total <= 0 || emit == nil {
		return
	}
	if total > maxSyntheticRows {
		total = maxSyntheticRows
	}

	for offset := 0; offset < total; offset += syntheticChunkSize {
		chunk := syntheticChunkSize
		if offset+chunk > total {
			chunk = total - offset
		}
		db := s.buildSyntheticChunkOf(fi, offset, chunk, total)
		if db != nil && db.RowsCount() > 0 {
			emit(db)
		}
	}
}

// buildSyntheticChunk is a thin wrapper around buildSyntheticChunkOf that
// derives the global row count from chunk size — kept for callers that
// only emit a single chunk.
func (s *Storage) buildSyntheticChunk(fi manifest.FileInfo, offset, chunk int) *logstorage.DataBlock {
	return s.buildSyntheticChunkOf(fi, offset, chunk, chunk)
}

// buildSyntheticChunkOf renders `chunk` rows of synthetic timestamps
// starting at the given offset, where the timestamp step is computed
// against the global `total` row count so successive chunks remain
// monotonically increasing across the file's [MinTimeNs, MaxTimeNs] range.
func (s *Storage) buildSyntheticChunkOf(fi manifest.FileInfo, offset, chunk, total int) *logstorage.DataBlock {
	if chunk <= 0 {
		return nil
	}

	tsCol := s.registry.TimestampColumn()
	internalName := tsCol
	if m := s.registry.ResolveFromParquet(tsCol); m != nil {
		internalName = m.InternalName
	}

	values := make([]string, chunk)
	if total == 1 {
		values[0] = s.registry.FormatField(internalName, fi.MinTimeNs)
	} else {
		step := (fi.MaxTimeNs - fi.MinTimeNs) / int64(total-1)
		if step == 0 {
			step = 1
		}
		for i := range values {
			ts := fi.MinTimeNs + int64(offset+i)*step
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
	// timestamp column — even for logs the columnar writer can reorder rows
	// to improve compression. Taking MinValue(0) and MaxValue(N-1) as
	// row-group bounds silently skipped row groups whose smallest/largest
	// timestamps lived in a middle page, which produced empty results for
	// narrow time windows. Aggregate across every page index instead (shared
	// columnIndexTimeBounds — the same scan now also guards
	// rowGroupFullyInRange, syntheticTimestampBlock and the footer-enrich
	// paths). Keep this in sync with the traces module
	// (lakehouse-traces/internal/storage/parquets3/storage_query.go).
	rgMin, rgMax := columnIndexTimeBounds(idx)

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

// QuerySpecificFiles queries only the Parquet files identified by the given
// file keys, rather than discovering files via the manifest time range.
// This is used by the select tier during gap redistribution: when a combined
// node fails during fan-out, surviving nodes can re-query the orphaned files
// by key.
//
// The method looks up files from the manifest's time-range index and filters
// to only those whose Key appears in fileKeys. It then processes each file
// using the existing queryFile infrastructure (bloom checks, row group
// skipping, footer cache, etc.).
//
// If no matching files are found, it returns nil (no error).
func (s *Storage) QuerySpecificFiles(ctx context.Context, fileKeys []string, startNs, endNs int64, queryStr string, pipeFields []string, writeBlock logstorage.WriteDataBlockFunc) error {
	if len(fileKeys) == 0 {
		return nil
	}

	// Build set for O(1) lookup; deduplicate keys to prevent double processing.
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

	// Process matched files using the same per-file query pipeline as RunQuery
	// (footer cache, bloom checks, row group skipping, projected reads).
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
		emitted := false
		s.streamSyntheticManifestBlocks(fi, func(db *logstorage.DataBlock) {
			if db != nil && db.RowsCount() > 0 {
				filteredWriteBlock(0, db)
				emitted = true
			}
		})
		if emitted {
			metrics.MetadataOnlyFiles.Inc()
		}
		logger.Infof("query recovered compacted file via manifest metadata; key=%s rows=%d", fi.Key, fi.RowCount)
	} else {
		logger.Infof("query skipped compacted/deleted file; key=%s", fi.Key)
	}
}
