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
}

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
	tenantsFn     func(ctx context.Context, startNs, endNs int64) ([]logstorage.TenantID, error)
	keep          FlushRowFilter
	watermarkPath string
}

// NewBufferFlusher builds a flusher. watermarkDir should be the buffer's data
// dir (persistent). keep may be nil (no gate-at-flush).
func NewBufferFlusher(writer *BatchWriter, buffer flusherBuffer, watermarkDir string, keep FlushRowFilter) *BufferFlusher {
	return &BufferFlusher{
		writer:        writer,
		buffer:        buffer,
		keep:          keep,
		watermarkPath: filepath.Join(watermarkDir, "buffer_flush_watermark.json"),
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

// flushWindow flushes (startNs, endNs] from the buffer to authoritative Parquet,
// per tenant and partition, reusing flushTracePartition. Returns nil only when
// every partition of every tenant flushed successfully (so the caller may advance
// the watermark); any error leaves the watermark unmoved for a retry next tick.
func (f *BufferFlusher) flushWindow(ctx context.Context, startNs, endNs int64) error {
	tenants, err := f.buffer.GetTenantIDs(ctx, startNs, endNs)
	if err != nil {
		return err
	}
	for _, tenant := range tenants {
		rows, err := f.collectTenantRows(ctx, tenant, startNs, endNs)
		if err != nil {
			return err
		}
		if len(rows) == 0 {
			continue
		}
		byPartition := map[string][]schema.TraceRow{}
		for _, r := range rows {
			byPartition[partitionFromNano(r.TimestampUnixNano)] = append(byPartition[partitionFromNano(r.TimestampUnixNano)], r)
		}
		// Deterministic order so a retry re-walks partitions the same way.
		parts := make([]string, 0, len(byPartition))
		for p := range byPartition {
			parts = append(parts, p)
		}
		sort.Strings(parts)
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
			now := tick.UnixNano()
			if now <= last {
				continue
			}
			if err := f.flushWindow(ctx, last, now); err != nil {
				logger.Warnf("buffer flusher: window (%d,%d] failed, will retry: %s", last, now, err)
				continue // watermark unmoved → retry the same window next tick
			}
			if err := f.saveWatermark(now); err != nil {
				logger.Warnf("buffer flusher: watermark persist failed (window will re-flush, harmless): %s", err)
				continue
			}
			last = now
		}
	}
}
