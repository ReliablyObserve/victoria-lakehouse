package parquets3

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// flusherBuffer is the subset of the logstorage-native buffer the flusher needs:
// enumerate tenants with data in a window, and stream their rows back. The
// membuffer.Store satisfies it. Kept separate from LocalBuffer so the read-path
// interface (and its test spies) stay unchanged.
type flusherBuffer interface {
	RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error
	GetTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error)
	// DebugFlush forces the buffer's in-memory rowsBuffer to a queryable part.
	// The flusher MUST call this before reading: logstorage's RunQuery does NOT
	// see un-flushed rows, so without it the most-recent ingest is invisible and
	// would be skipped past by the advancing watermark (permanent loss).
	DebugFlush()
}

// defaultFlushLatencyOffset is how far behind now() the flusher stops: a window
// is only committed once it is this far in the past, so spans whose _time falls
// in it but which arrive late (ingestion lag, trace completion) have landed
// before the watermark advances past their _time. Rows in (now-offset, now] stay
// in the buffer and are served by the read-merge until then. This is the lateness
// tolerance — set it above the p99 (ingest_time - _time). 2m covers typical trace
// completion + ingest lag; 30s was too small and dropped late spans (the residual
// per-window under-count).
const defaultFlushLatencyOffset = 2 * time.Minute

// FlushRowFilter is the gate-at-flush predicate: it returns true to KEEP a
// reconstructed row, false to drop it. It is injected from main so the buffer
// (a raw query cache) can be filtered to exactly what the legacy authoritative
// path would have written — dropping VT-internal trace_id_idx rows and rows whose
// stream exceeds the per-tenant cardinality limit — WITHOUT this package
// importing the vlstorage gate. A nil filter keeps every row.
type FlushRowFilter func(accountID, projectID uint32, stream string) bool

// BufferFlusher makes the logstorage-native buffer the AUTHORITATIVE Parquet
// producer: on a ticker it queries the buffer for the just-elapsed window,
// reconstructs schema.TraceRow via DataBlockToTraceRows, applies the gate-at-flush
// filter, and uploads the rows through the same per-group machinery the legacy
// flush uses (upload + manifest + bloom + stats + sidecar), so the Parquet it
// writes is identical to the legacy flush.
//
// Durability is the buffer's own restore-on-open plus a persisted watermark.
// The invariant: an object's bytes never change once uploaded, and no key is
// uploaded twice with different content. To keep it:
//
//   - a window is recorded as pending, with a random nonce that names its
//     objects, before its first upload; the keys stored so far are persisted
//     after each upload;
//   - a group that fails in this process is retried exactly (same key, same
//     bytes), without collecting the window again;
//   - after a restart the window is collected again, but only once a bucket
//     listing has been applied, and groups already recorded as stored are not
//     sent again; the writer also skips any key the manifest already has (an
//     object stored but not yet recorded) or has retired (compacted or removed);
//   - the watermark advances only when every group is stored or settled, and a
//     failed watermark save is retried without any upload.
//
// So a crash or a partial failure neither loses rows nor writes them twice, and
// no LH WAL is needed. Rows that arrive for a pending window after its first
// attempt are not added by an in-process retry (rows older than a committed
// window are never collected again either); a window resumed after a restart
// includes them for groups that were never stored.
//
// Wired DORMANT (BufferFlushEnabled defaults false): nothing runs until an
// operator opts in, and the legacy path stays authoritative until the cutover.
type BufferFlusher struct {
	writer        *BatchWriter
	buffer        flusherBuffer
	keep          FlushRowFilter
	watermarkPath string
	latencyOffset int64 // ns; flush only up to now-latencyOffset
	targetBytes   int64 // S3 object-size trigger: flush a window once it reaches this
	maxLinger     int64 // ns; force-flush a window this old even if below targetBytes

	// State of the window being flushed, when there is one (see tick).
	pending   int64               // its end; 0 when none
	nonce     string              // names its objects
	uploaded  map[string]struct{} // keys stored (or settled) so far
	failed    []*traceGroupUpload // groups the last attempt could not write
	recovered bool                // loaded from disk: not yet resumed
}

// estBytesPerTraceRow is a rough raw-bytes-per-span estimate used only to decide
// WHEN a window is big enough to flush (the size gate). It needn't be exact — it
// just keeps the flusher producing ~targetBytes S3 objects instead of one tiny
// object per tick.
const estBytesPerTraceRow = 512

// NewBufferFlusher builds a flusher. watermarkDir should be the buffer's data
// dir (persistent). keep may be nil. targetBytes is the S3 object-size flush
// trigger (e.g. insert.target_file_size); maxLinger caps how long a sub-target
// window waits before being flushed anyway. Both fall back to sane defaults.
func NewBufferFlusher(writer *BatchWriter, buffer flusherBuffer, watermarkDir string, keep FlushRowFilter, targetBytes int64, maxLinger time.Duration) *BufferFlusher {
	if targetBytes <= 0 {
		targetBytes = 128 << 20 // 128 MiB
	}
	if maxLinger <= 0 {
		maxLinger = 5 * time.Minute
	}
	return &BufferFlusher{
		writer:        writer,
		buffer:        buffer,
		keep:          keep,
		watermarkPath: filepath.Join(watermarkDir, "buffer_flush_watermark.json"),
		latencyOffset: int64(defaultFlushLatencyOffset),
		targetBytes:   targetBytes,
		maxLinger:     int64(maxLinger),
	}
}

// flushWatermark is the flusher's persisted state.
//
// A window is committed when LastFlushWindowEndNs reaches its end. While a
// window is being flushed it is also recorded as pending, with the nonce that
// names its objects and the keys already stored, so a failure or a restart
// resumes that window instead of starting a larger one.
type flushWatermark struct {
	LastFlushWindowEndNs int64 `json:"last_flush_window_end_ns"`
	// PendingWindowEndNs is the end of the window being flushed, recorded
	// before its first upload.
	PendingWindowEndNs int64 `json:"pending_window_end_ns,omitempty"`
	// PendingNonce is random, drawn when the window becomes pending. It is
	// part of every object name of the window, so two flushers (or two
	// incarnations of one) never derive the same key for different bytes.
	PendingNonce string `json:"pending_nonce,omitempty"`
	// PendingUploaded is the sorted set of keys of the window that were stored
	// (or found already settled), persisted after each. A restart does not send
	// them again.
	PendingUploaded []string `json:"pending_uploaded,omitempty"`
	Version         int      `json:"version"`
}

// watermarkVersion is the format written. A file of an older version, or a
// pending window without a nonce, is read as having no pending window.
const watermarkVersion = 2

// loadWatermark returns the persisted window-end, or fallbackNs when no (valid)
// watermark exists yet — so a fresh flusher starts at the flip point rather than
// re-flushing ancient data the legacy path already handled. A valid pending
// window is restored and marked recovered: the first tick after it waits for a
// bucket listing and re-collects the window.
func (f *BufferFlusher) loadWatermark(fallbackNs int64) int64 {
	f.clearPending()
	b, err := os.ReadFile(f.watermarkPath)
	if err != nil {
		return fallbackNs
	}
	var wm flushWatermark
	if err := json.Unmarshal(b, &wm); err != nil || wm.LastFlushWindowEndNs <= 0 {
		return fallbackNs
	}
	if wm.Version >= watermarkVersion && wm.PendingNonce != "" && wm.PendingWindowEndNs > wm.LastFlushWindowEndNs {
		f.pending = wm.PendingWindowEndNs
		f.nonce = wm.PendingNonce
		f.uploaded = make(map[string]struct{}, len(wm.PendingUploaded))
		for _, k := range wm.PendingUploaded {
			f.uploaded[k] = struct{}{}
		}
		f.recovered = true
	}
	return wm.LastFlushWindowEndNs
}

// clearPending forgets the in-memory pending window.
func (f *BufferFlusher) clearPending() {
	f.pending = 0
	f.nonce = ""
	f.uploaded = nil
	f.failed = nil
	f.recovered = false
}

// saveWatermark persists the window-end atomically (tempfile + rename) so a crash
// mid-write never leaves a torn watermark. It also clears the pending record.
func (f *BufferFlusher) saveWatermark(endNs int64) error {
	if err := f.writeWatermark(flushWatermark{LastFlushWindowEndNs: endNs, Version: watermarkVersion}); err != nil {
		return err
	}
	metrics.InsertFlushWatermarkNs.Set(endNs)
	return nil
}

// savePending records the pending window: its end, nonce and the keys stored so
// far. Written before the first upload and after each one.
func (f *BufferFlusher) savePending(lastNs int64) error {
	keys := make([]string, 0, len(f.uploaded))
	for k := range f.uploaded {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return f.writeWatermark(flushWatermark{
		LastFlushWindowEndNs: lastNs,
		PendingWindowEndNs:   f.pending,
		PendingNonce:         f.nonce,
		PendingUploaded:      keys,
		Version:              watermarkVersion,
	})
}

func (f *BufferFlusher) writeWatermark(wm flushWatermark) error {
	b, err := json.Marshal(wm)
	if err != nil {
		return err
	}
	tmp := f.watermarkPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o600); err != nil {
		return err
	}
	return os.Rename(tmp, f.watermarkPath)
}

// collectWindow gathers (startNs, endNs] from the buffer, per tenant, applying
// the gate-at-flush filter. It returns the per-tenant rows and the total row
// count (for the size gate). It does NOT write anything — the caller decides
// whether the window is big enough (or old enough) to flush.
func (f *BufferFlusher) collectWindow(ctx context.Context, startNs, endNs int64) (map[logstorage.TenantID][]schema.TraceRow, int, error) {
	tenants, err := f.buffer.GetTenantIDs(ctx, startNs, endNs)
	if err != nil {
		return nil, 0, err
	}
	out := make(map[logstorage.TenantID][]schema.TraceRow, len(tenants))
	total := 0
	for _, tenant := range tenants {
		rows, err := f.collectTenantRows(ctx, tenant, startNs, endNs)
		if err != nil {
			return nil, 0, err
		}
		if len(rows) > 0 {
			out[tenant] = rows
			total += len(rows)
		}
	}
	return out, total, nil
}

// windowBatchID names the object one tenant's rows of one partition get when
// the window (startNs, endNs] is flushed under nonce. The nonce is drawn when
// the window becomes pending and persisted with it, so a resumed window names
// the same objects while a different flusher, or a later window over the same
// range, cannot collide with them. 16 hex characters, like any batch id.
func windowBatchID(nonce string, startNs, endNs int64, accountID, projectID uint32, partition string) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%s-%d-%d-%d-%d-%s", nonce, startNs, endNs, accountID, projectID, partition)))
	return fmt.Sprintf("%x", sum[:8])
}

// buildGroups splits the rows collected for (startNs, endNs] into one upload per
// tenant and partition, keyed by windowBatchID(nonce, ...) and ordered by
// (account, project, partition), so which group an error hits first does not
// depend on map iteration. Each group records itself as stored through
// recordUploaded (afterUpload).
func (f *BufferFlusher) buildGroups(collected map[logstorage.TenantID][]schema.TraceRow, lastNs, startNs, endNs int64, nonce string) []*traceGroupUpload {
	tenants := make([]logstorage.TenantID, 0, len(collected))
	for t := range collected {
		tenants = append(tenants, t)
	}
	sort.Slice(tenants, func(i, j int) bool {
		if tenants[i].AccountID != tenants[j].AccountID {
			return tenants[i].AccountID < tenants[j].AccountID
		}
		return tenants[i].ProjectID < tenants[j].ProjectID
	})
	var groups []*traceGroupUpload
	for _, tenant := range tenants {
		byPartition := map[string][]schema.TraceRow{}
		for _, r := range collected[tenant] {
			p := partitionFromNano(r.TimestampUnixNano)
			byPartition[p] = append(byPartition[p], r)
		}
		parts := make([]string, 0, len(byPartition))
		for p := range byPartition {
			parts = append(parts, p)
		}
		sort.Strings(parts)
		for _, p := range parts {
			name := func(accountID, projectID uint32) string {
				return windowBatchID(nonce, startNs, endNs, accountID, projectID, p)
			}
			for _, up := range f.writer.buildTraceGroups(p, byPartition[p], name) {
				f.writer.assignTraceKey(up)
				up.afterUpload = func(key string) error { return f.recordUploaded(lastNs, key) }
				groups = append(groups, up)
			}
		}
	}
	return groups
}

// recordUploaded adds key to the pending window's stored set and persists it.
// It runs after the object is stored and before the manifest commit: a crash
// after it leaves the key recorded and never sent again; a crash before it
// leaves the object to the listing (the writer skips keys the manifest has).
func (f *BufferFlusher) recordUploaded(lastNs int64, key string) error {
	if f.uploaded == nil {
		f.uploaded = make(map[string]struct{})
	}
	_, had := f.uploaded[key]
	f.uploaded[key] = struct{}{}
	if err := f.savePending(lastNs); err != nil {
		if !had {
			delete(f.uploaded, key)
		}
		return err
	}
	return nil
}

// uploadAll attempts every group, in order, and returns the ones that failed
// with the first error. A failure does not stop the rest of the window.
func (f *BufferFlusher) uploadAll(ctx context.Context, groups []*traceGroupUpload) ([]*traceGroupUpload, error) {
	var failed []*traceGroupUpload
	var firstErr error
	for _, up := range groups {
		err := ctx.Err()
		if err == nil {
			err = f.writer.uploadTraceGroup(ctx, up)
		}
		if err != nil {
			failed = append(failed, up)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return failed, firstErr
}

// collectTenantRows queries the buffer for one tenant over (startNs, endNs],
// reconstructs TraceRows, and applies the gate-at-flush filter (drop trace_id_idx
// + cardinality-exceeding rows) so the authoritative Parquet matches the legacy
// path exactly.
func (f *BufferFlusher) collectTenantRows(ctx context.Context, tenant logstorage.TenantID, startNs, endNs int64) ([]schema.TraceRow, error) {
	q, err := logstorage.ParseQueryAtTimestamp("*", endNs)
	if err != nil {
		return nil, err
	}
	q = q.CloneWithTimeFilter(q.GetTimestamp(), startNs, endNs)
	qctx := logstorage.NewQueryContext(ctx, &logstorage.QueryStats{}, []logstorage.TenantID{tenant}, q, false, nil)

	// logstorage invokes writeBlock from MULTIPLE goroutines concurrently, so the
	// shared slice (and any filter side effects) must be synchronized.
	var mu sync.Mutex
	var rows []schema.TraceRow
	err = f.buffer.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		local := make([]schema.TraceRow, 0, db.RowsCount())
		for _, r := range vlstorage.DataBlockToTraceRows(db, tenant) {
			if f.keep != nil && !f.keep(r.AccountID, r.ProjectID, r.Stream) {
				continue
			}
			local = append(local, r)
		}
		mu.Lock()
		rows = append(rows, local...)
		mu.Unlock()
	})
	return rows, err
}

// Run drives the flusher until ctx is cancelled. checkInterval is how often the
// flusher wakes to (re-)evaluate the window for the size/linger gate — NOT the
// flush cadence (a window flushes on targetBytes OR maxLinger).
//
// CRASH-SAFETY: the watermark advances (and is persisted, atomically) ONLY after
// a window's Parquet is fully written. So a crash mid-window leaves the watermark
// at the last committed boundary; the rows live in the persisted buffer
// (logstorage parts on disk, restored on open), and on restart loadWatermark
// resumes from that boundary: it finishes the pending window if there is one, then
// flushes (last, now-offset] — no loss, no LH WAL. The only requirement is buffer_retention > maxLinger + downtime so the
// un-flushed data hasn't aged out of the buffer before recovery. A FRESH flip
// (no watermark file) starts at nowNs (the flip point) so pre-flip data — already
// owned by the legacy path — is never double-flushed.
func (f *BufferFlusher) Run(ctx context.Context, checkInterval time.Duration, nowNs int64) {
	if checkInterval <= 0 {
		checkInterval = 30 * time.Second
	}
	last := f.loadWatermark(nowNs)
	t := time.NewTicker(checkInterval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-t.C:
			last = f.tick(ctx, last, tick.UnixNano())
		}
	}
}

// tick runs one flush attempt and returns the watermark after it.
//
// A window is either fresh (nothing pending), pending after a restart
// (recovered), or pending in this process after a failed attempt:
//
//   - fresh: gate, collect, record the window as pending under a new nonce,
//     build the groups, attempt them all;
//   - recovered: wait for the first bucket listing, so objects stored before the
//     crash are in the manifest; re-collect (last, pending], drop the groups
//     already recorded as stored, attempt the rest — the writer skips any key
//     the manifest already has or retired;
//   - in-process retry: attempt the groups that failed, as they are (same
//     keys, same bytes), without collecting again.
//
// The watermark moves to the window's end only when every group is stored or
// settled. If saving it fails, the next tick uploads nothing and saves again.
//
// Rows that arrive for a pending window after its first attempt are not added
// by an in-process retry: rows older than a committed watermark are never
// collected again, so they would be lost either way, and rewriting an object to
// include them is exactly what must not happen. A window resumed after a restart
// is collected again, so its groups that were never stored do include them.
func (f *BufferFlusher) tick(ctx context.Context, last, nowNs int64) int64 {
	switch {
	case f.pending == 0:
		return f.tickFresh(ctx, last, nowNs)
	case f.recovered:
		return f.tickRecovered(ctx, last)
	default:
		return f.attempt(ctx, last, f.failed)
	}
}

func (f *BufferFlusher) tickFresh(ctx context.Context, last, nowNs int64) int64 {
	// Stop short of now by latencyOffset so in-flight/late rows land before
	// their window is committed.
	flushEnd := nowNs - f.latencyOffset
	if flushEnd <= last {
		return last
	}
	// Make all ingested rows queryable; RunQuery does NOT see the un-flushed
	// rowsBuffer, so without this the recent window is invisible and the
	// watermark would skip past it (the under-production bug).
	f.buffer.DebugFlush()
	collected, nRows, err := f.collectWindow(ctx, last, flushEnd)
	if err != nil {
		logger.Warnf("buffer flusher: collect (%d,%d] failed, will retry: %s", last, flushEnd, err)
		return last
	}
	aged := flushEnd-last >= f.maxLinger
	// Size gate: hold a sub-target window open across ticks so the S3 object
	// lands at ~targetBytes instead of one tiny file per tick.
	if int64(nRows)*estBytesPerTraceRow < f.targetBytes && !aged {
		return last // accumulate — don't flush, don't advance the watermark
	}
	if nRows == 0 {
		if err := f.saveWatermark(flushEnd); err != nil {
			logger.Warnf("buffer flusher: watermark persist failed, will retry: %s", err)
			return last
		}
		return flushEnd
	}
	f.pending, f.nonce, f.uploaded, f.failed, f.recovered = flushEnd, randomBatchID(), map[string]struct{}{}, nil, false
	if err := f.savePending(last); err != nil {
		logger.Warnf("buffer flusher: cannot record window (%d,%d] before flushing it, will retry: %s", last, flushEnd, err)
		f.clearPending()
		return last
	}
	return f.attempt(ctx, last, f.buildGroups(collected, last, last, flushEnd, f.nonce))
}

func (f *BufferFlusher) tickRecovered(ctx context.Context, last int64) int64 {
	// Until a listing has been applied, "the manifest lacks this key" says
	// nothing about whether an earlier attempt stored it.
	if !f.writer.manifest.Listed() {
		return last
	}
	f.buffer.DebugFlush()
	collected, _, err := f.collectWindow(ctx, last, f.pending)
	if err != nil {
		logger.Warnf("buffer flusher: collect pending window (%d,%d] failed, will retry: %s", last, f.pending, err)
		return last
	}
	var todo []*traceGroupUpload
	for _, up := range f.buildGroups(collected, last, last, f.pending, f.nonce) {
		if _, done := f.uploaded[up.key]; !done {
			todo = append(todo, up)
		}
	}
	f.recovered = false
	return f.attempt(ctx, last, todo)
}

// attempt uploads groups and, if every one is stored or settled, commits the
// pending window. The groups that fail are kept for the next tick.
func (f *BufferFlusher) attempt(ctx context.Context, last int64, groups []*traceGroupUpload) int64 {
	failed, err := f.uploadAll(ctx, groups)
	f.failed = failed
	if err != nil {
		logger.Warnf("buffer flusher: flush (%d,%d] left %d group(s) unwritten, will retry them: %s", last, f.pending, len(failed), err)
		return last
	}
	end := f.pending
	if err := f.saveWatermark(end); err != nil {
		logger.Warnf("buffer flusher: watermark persist failed after the window was stored; the next tick saves it again without uploading: %s", err)
		return last
	}
	f.clearPending()
	return end
}
