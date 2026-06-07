// Package membuffer wraps a logstorage.Storage as the lakehouse cold tier's
// queryable in-memory insert buffer (Option B; see
// docs/architecture/buffer-queryable-store-design.md).
//
// Instead of staging ingested rows as []schema.{Log,Trace}Row and
// reconstructing a logstorage.DataBlock at query time (the struct→DataBlock
// converter that kept drifting from the file-scan emission), the buffer is a
// real per-pod logstorage.Storage: ingest feeds the native logstorage.LogRows
// VT/VL already built, queries run via the same engine (RunQuery), and — in a
// later phase — Parquet for the buffer's data is produced by exporting it
// via the exported logstorage.Storage.RunQuery (no VL modification).
//
// Phase 1 uses it only as a write-side shadow behind the BufferEngine flag
// (dual-write), with the legacy Parquet path still authoritative; RunQuery is
// exercised by the parity test and wired into the read path in P3.
package membuffer

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// Config controls the buffer store. Zero values fall back to VT-compatible
// defaults so the store admits exactly the rows hot VT would.
type Config struct {
	// Path is the directory for the store's parts. It should live on a
	// PERSISTENT volume: logstorage.Storage writes its in-memory parts here
	// (every FlushInterval) and reads them back on MustOpenStorage, so this
	// directory IS the buffer's durability + restore — exactly as VT/VL hot
	// persist. No separate WAL is needed; the crash-loss window is the last
	// FlushInterval, matching upstream. Long-term durability is the S3 Parquet
	// flush.
	Path string

	// Retention bounds how long rows live in the buffer before VL drops the
	// oldest per-day partition. Bounds buffer memory; data older than the
	// retention is served from S3 Parquet, not the buffer. Default 1h.
	Retention time.Duration

	// FlushInterval is VL's in-memory rowsBuffer→inmemoryPart→disk interval
	// (NOT the lakehouse Parquet flush). Default 5s, matching VT.
	FlushInterval time.Duration

	// FutureRetention bounds how far into the future an ingested timestamp
	// may be before VL rejects it. Default 2d, matching VT.
	FutureRetention time.Duration
}

func (c *Config) withDefaults() {
	if c.Retention <= 0 {
		c.Retention = time.Hour
	}
	if c.FlushInterval <= 0 {
		c.FlushInterval = 5 * time.Second
	}
	if c.FutureRetention <= 0 {
		c.FutureRetention = 2 * 24 * time.Hour
	}
}

// Store is a thin wrapper over logstorage.Storage. All real work is done by
// the reused upstream engine; this type only owns the lifecycle and narrows
// the surface to what the lakehouse insert/query paths need.
type Store struct {
	s    *logstorage.Storage
	path string
}

// Open creates (or reopens) the buffer store at cfg.Path. It mirrors VT's own
// MustOpenStorage call (StorageConfig fields lifted from
// deps/VictoriaTraces/app/vtstorage/main.go) with MaxBackfillAge=0 (unlimited)
// so no ingested span is silently dropped by the buffer that hot VT would keep.
func Open(cfg Config) (*Store, error) {
	cfg.withDefaults()
	if cfg.Path == "" {
		return nil, fmt.Errorf("membuffer: empty Path")
	}
	if err := os.MkdirAll(cfg.Path, 0o755); err != nil {
		return nil, fmt.Errorf("membuffer: mkdir %q: %w", cfg.Path, err)
	}
	sc := &logstorage.StorageConfig{
		Retention:             cfg.Retention,
		FlushInterval:         cfg.FlushInterval,
		FutureRetention:       cfg.FutureRetention,
		MaxBackfillAge:        0, // 0 == unlimited: accept any backfilled age, like VT
		MinFreeDiskSpaceBytes: 10e6,
	}
	s := logstorage.MustOpenStorage(cfg.Path, sc)
	return &Store{s: s, path: cfg.Path}, nil
}

// MustAddRows appends the native LogRows to the buffer. Safe to call from the
// insert adapter's MustAddRows while lr is still valid (logstorage copies the
// rows into its own parts before returning). Reused verbatim from upstream.
func (st *Store) MustAddRows(lr *logstorage.LogRows) {
	st.s.MustAddRows(lr)
}

// RunQuery runs q against the buffered (in-memory + not-yet-flushed) rows via
// the same logstorage engine the S3-Parquet path uses, so results are
// byte-identical in shape to a file scan. Wired into the read merge in P3.
func (st *Store) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	return st.s.RunQuery(qctx, writeBlock)
}

// DebugFlush forces VL to flush its in-memory rowsBuffer so just-ingested rows
// become immediately queryable / available to the flush sink. Used by tests
// and graceful shutdown.
func (st *Store) DebugFlush() {
	st.s.DebugFlush()
}

// GetTenantIDs returns the tenant IDs with data in [start, end] nanoseconds.
// Reused from the upstream engine so the P5 shadow exporter can enumerate which
// tenants to export per window without LH tracking it separately.
func (st *Store) GetTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error) {
	return st.s.GetTenantIDs(ctx, start, end)
}

// Close releases the store. The on-disk path is left for the OS/tmpfs to
// reclaim; it carries no durable data.
func (st *Store) Close() {
	st.s.MustClose()
}

// Path returns the store's data directory.
func (st *Store) Path() string { return st.path }
