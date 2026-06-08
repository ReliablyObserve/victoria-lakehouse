package parquets3

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

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

// defaultFlushLatencyOffset is how far behind now() the flusher stops: rows in
// (now-offset, now] stay in the buffer (served by the read-merge) rather than
// being flushed, giving in-flight / late-arriving spans time to land before
// their window is committed and the watermark advances past it.
const defaultFlushLatencyOffset = 30 * time.Second

// FlushRowFilter is the gate-at-flush predicate: it returns true to KEEP a
// reconstructed row, false to drop it. It is injected from main so the buffer
// (a raw query cache) can be filtered to exactly what the legacy authoritative
// path would have written — dropping VT-internal trace_id_idx rows and rows whose
// stream exceeds the per-tenant cardinality limit — WITHOUT this package
// importing the vlstorage gate. A nil filter keeps every row.
type FlushRowFilter func(accountID, projectID uint32, stream string) bool

// BufferFlusher makes the logstorage-native buffer the AUTHORITATIVE Parquet
// producer: on a ticker it queries the buffer for the just-elapsed window,
// reconstructs schema.TraceRow via DataBlockToTraceRows, applies the
// gate-at-flush filter, and hands the rows to the EXISTING flushTracePartition
// machinery (upload + manifest + _trace_idx + bloom + stats + sidecar) — so the
// Parquet it writes is identical to the legacy []TraceRow flush. Durability is
// the buffer's own restore-on-open plus a persisted flush watermark: the window
// only advances after every partition flush in it succeeds, so a crash re-flushes
// (manifest AddFile is idempotent; the read path dedups), losing nothing with
// no LH WAL.
//
// It is wired DORMANT (BufferFlushEnabled defaults false): nothing runs until an
// operator opts in, and the legacy path stays authoritative until the cutover.
type BufferFlusher struct {
	writer        *BatchWriter
	buffer        flusherBuffer
	keep          FlushRowFilter
	watermarkPath string
	latencyOffset int64 // ns; flush only up to now-latencyOffset
	targetBytes   int64 // S3 object-size trigger: flush a window once it reaches this
	maxLinger     int64 // ns; force-flush a window this old even if below targetBytes
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

type flushWatermark struct {
	LastFlushWindowEndNs int64 `json:"last_flush_window_end_ns"`
	Version              int   `json:"version"`
}

// loadWatermark returns the persisted window-end, or fallbackNs when no (valid)
// watermark exists yet — so a fresh flusher starts at the flip point rather than
// re-flushing ancient data the legacy path already handled.
func (f *BufferFlusher) loadWatermark(fallbackNs int64) int64 {
	b, err := os.ReadFile(f.watermarkPath)
	if err != nil {
		return fallbackNs
	}
	var wm flushWatermark
	if err := json.Unmarshal(b, &wm); err != nil || wm.LastFlushWindowEndNs <= 0 {
		return fallbackNs
	}
	return wm.LastFlushWindowEndNs
}

// saveWatermark persists the window-end atomically (tempfile + rename) so a crash
// mid-write never leaves a torn watermark.
func (f *BufferFlusher) saveWatermark(endNs int64) error {
	b, err := json.Marshal(flushWatermark{LastFlushWindowEndNs: endNs, Version: 1})
	if err != nil {
		return err
	}
	tmp := f.watermarkPath + ".tmp"
	if err := os.WriteFile(tmp, b, 0o640); err != nil {
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

// flushCollected writes the collected rows to authoritative Parquet, per tenant
// and partition, reusing flushTracePartition. Returns nil only when every
// partition of every tenant flushed (so the caller may advance the watermark).
func (f *BufferFlusher) flushCollected(ctx context.Context, collected map[logstorage.TenantID][]schema.TraceRow) error {
	for _, rows := range collected {
		byPartition := map[string][]schema.TraceRow{}
		for _, r := range rows {
			byPartition[partitionFromNano(r.TimestampUnixNano)] = append(byPartition[partitionFromNano(r.TimestampUnixNano)], r)
		}
		parts := make([]string, 0, len(byPartition))
		for p := range byPartition {
			parts = append(parts, p)
		}
		sort.Strings(parts) // deterministic so a retry re-walks the same way
		for _, p := range parts {
			if err := f.writer.flushTracePartition(ctx, p, byPartition[p]); err != nil {
				return err
			}
		}
	}
	return nil
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

	var rows []schema.TraceRow
	err = f.buffer.RunQuery(qctx, func(_ uint, db *logstorage.DataBlock) {
		for _, r := range vlstorage.DataBlockToTraceRows(db, tenant) {
			if f.keep != nil && !f.keep(r.AccountID, r.ProjectID, r.Stream) {
				continue
			}
			rows = append(rows, r)
		}
	})
	return rows, err
}

// Run drives the flusher until ctx is cancelled. interval is the flush cadence;
// the first window starts at the persisted watermark (or now-interval if none).
func (f *BufferFlusher) Run(ctx context.Context, interval time.Duration, nowNs int64) {
	if interval <= 0 {
		interval = time.Minute
	}
	last := f.loadWatermark(nowNs - interval.Nanoseconds())
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case tick := <-t.C:
			// Stop short of now by latencyOffset so in-flight/late spans land
			// before their window is committed.
			flushEnd := tick.UnixNano() - f.latencyOffset
			if flushEnd <= last {
				continue
			}
			// Make all ingested rows queryable; RunQuery does NOT see the
			// un-flushed rowsBuffer, so without this the recent window is
			// invisible and the watermark would skip past it (the under-
			// production bug).
			f.buffer.DebugFlush()
			collected, nRows, err := f.collectWindow(ctx, last, flushEnd)
			if err != nil {
				logger.Warnf("buffer flusher: collect (%d,%d] failed, will retry: %s", last, flushEnd, err)
				continue
			}
			aged := flushEnd-last >= f.maxLinger
			// Size gate: hold a sub-target window open across ticks so the S3
			// object lands at ~targetBytes instead of one tiny file per tick.
			// Flush when the window reaches targetBytes OR has lingered maxLinger.
			if int64(nRows)*estBytesPerTraceRow < f.targetBytes && !aged {
				continue // accumulate — don't flush, don't advance the watermark
			}
			if nRows > 0 {
				if err := f.flushCollected(ctx, collected); err != nil {
					logger.Warnf("buffer flusher: flush (%d,%d] failed, will retry: %s", last, flushEnd, err)
					continue // watermark unmoved → retry the same window next tick
				}
			}
			if err := f.saveWatermark(flushEnd); err != nil {
				logger.Warnf("buffer flusher: watermark persist failed (window will re-flush, harmless): %s", err)
				continue
			}
			last = flushEnd
		}
	}
}
