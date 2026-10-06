package parquets3

import (
	"context"
	"io"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// defaultParquetReadBufferSize is parquet-go's page read buffer when
// S3Config.ReadBufferSize is unset. The library default is 4KB — sized for
// local disk; its own docs suggest ~4MiB for network storage. 1MB means one
// buffered fill per underlying GET instead of hundreds of 4KB reads
// thrashing the read-ahead window.
const defaultParquetReadBufferSize = 1024 * 1024

// clampWindowKnobs returns the file-size-clamped (gap, base, max) for the
// adaptive-window reader stack. BDP-priced windows are sized for LARGE
// files at real S3 RTT; on a file smaller than the window they degenerate
// range-projection into a full download (a 412KB file with a 1MB gap/window
// reads everything — caught by TestGetFieldValues_UsesColumnProjectedRead).
// Clamp every knob by file size, the same way ClickHouse bounds remote
// reads by read_until_position: large files keep the round-trip-minimal
// windows, small files keep precise column reads.
func (s *Storage) clampWindowKnobs(fileSize int64) (gap, base, maxW int64) {
	clamp := func(v, lo, hi int64) int64 {
		if v < lo {
			return lo
		}
		if v > hi {
			return hi
		}
		return v
	}
	gap = clamp(int64(s.cfg.S3.CoalesceGapBytes), 0, max64(64<<10, fileSize/8))
	base = clamp(int64(s.cfg.S3.ReadAheadBytes), 0, max64(64<<10, fileSize/4))
	maxW = clamp(int64(s.cfg.S3.ReadAheadMaxBytes), base, fileSize)
	return gap, base, maxW
}

// buildWindowReader assembles the adaptive-window reader stack
// (BufferedS3ReaderAt + CoalescingReaderAt) over the given raw reader —
// the speculative read-ahead path used by full scans and by the
// projected-read rollback (projected_fetch_mode: window, or a plan that
// exceeded projected_fetch_max_bytes).
func (s *Storage) buildWindowReader(inner s3reader.ReaderAtSizer, fileSize int64) io.ReaderAt {
	effGap, effBase, effMax := s.clampWindowKnobs(fileSize)
	buffered := s3reader.NewBufferedReaderAt(inner, fileSize, effBase, effMax)
	// Waste feedback: shrink the adaptive window when evicted windows were
	// mostly never read (sparse forward hops). <=0 keeps the 0.5 default;
	// >=1 disables. The file-size clamps above stay authoritative for the
	// base/max bounds the feedback floors/ceils against.
	buffered.SetWasteThreshold(s.cfg.S3.ReadAheadWasteThreshold)
	return s3reader.NewCoalescingReaderAt(buffered, fileSize, effGap)
}

// rangedOpenOptions returns the parquet.OpenFile options for a ranged S3
// open — the Tier-1 read hygiene set (s3-optimization research, PR 2a):
//
//   - SkipPageIndex(true): we prune row groups via manifest/pmeta facets and
//     footer statistics; the eager column-index+offset-index section GETs at
//     open are pure extra round trips. parquet-go v0.29.0 reads them lazily
//     per column chunk on first ColumnIndex()/OffsetIndex() call, so the
//     row-group time-pruning paths keep working — they just pay only when
//     actually consulted.
//   - SkipBloomFilters(true): VERIFIED — checkFileBloom's legacy fallback
//     reads the per-file `.bloom` SIDECAR object, not the parquet-internal
//     bloom section. The only parquet-internal bloom consumer is
//     bloomFilterSkip via ColumnChunk.BloomFilter(), which in v0.29.0 is
//     lazy (readBloomFilter CAS-caches on first call) — skipping the eager
//     open-time header reads keeps it working, on demand only.
//   - OptimisticRead(true) + ReadBufferSize: one tail GET covers the
//     magic-footer suffix AND the footer body when the footer fits in the
//     read buffer — instead of a serial 8-byte read then a footer read.
//   - FileReadMode (config: s3.parquet_read_mode): "sync" (default) reads
//     pages on the decoding goroutine, so the column readers of a file take
//     turns on its single read-ahead window in a fixed order and a query's
//     GETs and bytes depend only on the files and the query. "async" adds
//     FileReadMode(ReadModeAsync): a read-ahead goroutine per Pages
//     instance. Those goroutines interleave on the shared window (the
//     buffered reader serialises their GETs under one mutex, so they gain
//     no I/O parallelism), evict each other's bytes and re-fetch them —
//     measured on the field-metadata matrix with the default window knobs:
//     27 of 192 cells varied run to run, and the matrix read 15% more GETs
//     and 25% more bytes than sync.
//   - FileSchema(cachedSchema): skips re-deriving the schema from footer
//     metadata when the footer cache already holds it.
func (s *Storage) rangedOpenOptions(fi manifest.FileInfo, cachedSchema *parquet.Schema) []parquet.FileOption {
	readBuf := s.cfg.S3.ReadBufferSize
	if readBuf <= 0 {
		readBuf = defaultParquetReadBufferSize
	}
	if cap := max64(64<<10, fi.Size/4); int64(readBuf) > cap {
		readBuf = int(cap)
	}
	opts := []parquet.FileOption{
		parquet.SkipPageIndex(true),
		parquet.SkipBloomFilters(true),
		parquet.OptimisticRead(true),
		parquet.ReadBufferSize(readBuf),
	}
	if s.cfg.S3.ParquetReadMode == "async" {
		opts = append(opts, parquet.FileReadMode(parquet.ReadModeAsync))
	}
	if cachedSchema != nil {
		opts = append(opts, parquet.FileSchema(cachedSchema))
	}
	return opts
}

// withFooterOverlay puts the cached footer + page-index tail of fi in front of
// raw when the footer cache holds them for exactly this object, and adjusts the
// open options so parquet-go reads nothing from S3 at open:
//
//   - SkipMagicBytes(true): the head magic is not read (the cached footer was
//     parsed from this same immutable object, and the trailer magic is still
//     validated from the cached tail);
//   - OptimisticRead(false): the optimistic read would ask for up to
//     ReadBufferSize bytes of tail, far more than the cached tail, turning a
//     memory hit into a ranged GET for the front part.
//
// Every ColumnIndex()/OffsetIndex() read parquet-go makes later is served from
// the same cached bytes; only data ranges (and bloom sections, which sit
// between the row groups) reach S3. With no usable cache entry raw and opts
// are returned unchanged. The key and the size must both match the cached
// entry; the overlay never synthesises bytes outside the cached tail.
//
// cached is the entry the caller already looked up (one lookup per open: a
// second one counted a second cache hit and could miss an entry evicted in
// between); nil makes withFooterOverlay look it up itself.
func (s *Storage) withFooterOverlay(fi manifest.FileInfo, raw s3reader.ReaderAtSizer, opts []parquet.FileOption, cached *CachedFooter) (s3reader.ReaderAtSizer, []parquet.FileOption) {
	if s.footerCache == nil {
		return raw, opts
	}
	if cached == nil {
		var ok bool
		if cached, ok = s.footerCache.Get(fi.Key); !ok {
			metrics.FooterOverlayOpens.Inc("miss")
			return raw, opts
		}
	}
	if cached.FileSize != fi.Size {
		metrics.FooterOverlayOpens.Inc("miss")
		return raw, opts
	}
	tail, _ := cached.Tail()
	ov := s3reader.NewOverlayReaderAt(raw, fi.Size, tail)
	if ov == nil {
		metrics.FooterOverlayOpens.Inc("miss")
		return raw, opts
	}
	if cached.HasPageIndex() {
		metrics.FooterOverlayOpens.Inc("hit")
	} else {
		metrics.FooterOverlayOpens.Inc("hit_footer_only")
	}
	return ov, append(opts, parquet.SkipMagicBytes(true), parquet.OptimisticRead(false))
}

// openRangedParquet opens fi over ranged S3 reads through the ADAPTIVE
// WINDOW stack (PhaseReaderAt -> BufferedS3ReaderAt -> CoalescingReaderAt)
// with the Tier-1 read hygiene options — the path for full scans and for
// projected reads when projected_fetch_mode is "window".
//
// The raw reader is wrapped in a PhaseReaderAt so every GET is attributed to
// the open or page phase (metrics.S3GetsByPhase) and the per-open GET count
// lands in metrics.S3GetsPerOpen — the research-doc "serial 4-6 GET open"
// baseline, now measurable per open.
func (s *Storage) openRangedParquet(ctx context.Context, fi manifest.FileInfo, cachedSchema *parquet.Schema, cached *CachedFooter) (*parquet.File, error) {
	raw := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
	phased := s3reader.NewPhaseReaderAt(raw)
	inner, opts := s.withFooterOverlay(fi, phased, s.rangedOpenOptions(fi, cachedSchema), cached)
	readerAt := s.buildWindowReader(inner, fi.Size)

	f, err := parquet.OpenFile(readerAt, fi.Size, opts...)
	if err != nil {
		return nil, err
	}
	metrics.S3GetsPerOpen.Observe(float64(phased.OpenGets()))
	phased.SetPhase(s3reader.PhasePage)
	return f, nil
}

// openPlannedParquet opens fi for a PLAN-THEN-FETCH projected read
// (s3-optimization research, Tier-2 items 8/9). The reader stack is
// PhaseReaderAt -> PlannedFetchReaderAt with NO speculative window:
//
//   - open-phase reads (magic probe + optimistic footer tail) pass through
//     the un-armed view as exact-range GETs — strictly cheaper than pulling
//     a read-ahead window for the footer;
//   - after the caller prunes row groups it arms the view with the exact
//     coalesced column-chunk ranges (armProjectedPlan), and every page read
//     of the decode is served from the fetched spans;
//   - a plan above s3.projected_fetch_max_bytes falls back to the window
//     stack via Redirect (fallbackPlannedToWindow) — the same adaptive
//     window openRangedParquet would have built.
//
// The returned view MUST be Closed by the caller when the file's processing
// completes (releases the fetched spans and their memory-budget charge).
func (s *Storage) openPlannedParquet(ctx context.Context, fi manifest.FileInfo, cachedSchema *parquet.Schema, cached *CachedFooter) (*parquet.File, *s3reader.PlannedFetchReaderAt, error) {
	raw := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
	phased := s3reader.NewPhaseReaderAt(raw)
	inner, opts := s.withFooterOverlay(fi, phased, s.rangedOpenOptions(fi, cachedSchema), cached)
	effGap, _, _ := s.clampWindowKnobs(fi.Size)
	view := s3reader.NewPlannedFetchReaderAt(inner, fi.Size, effGap, chargePlannedFetchBytes)
	// v2 slice-1 levers (opt-in planned path only): span concurrency
	// min(k, spans) and the per-SPAN cap (spans above it split — CH
	// bytes_per_read_task scope; the per-plan cap is retired). The gap is
	// re-priced per plan by armProjectedPlan's gap discipline; effGap only
	// covers a Fetch that skips pricing (tests).
	view.SetMaxInFlight(s.cfg.S3.PlannedFetchMaxInflight)
	view.SetSpanCap(int64(s.cfg.S3.PlannedFetchSpanCapBytes))

	f, err := parquet.OpenFile(view, fi.Size, opts...)
	if err != nil {
		return nil, nil, err
	}
	metrics.S3GetsPerOpen.Observe(float64(phased.OpenGets()))
	phased.SetPhase(s3reader.PhasePage)
	return f, view, nil
}

// openProjectedParquet opens fi for a column-projected read in the
// configured projected-fetch mode: planned (plan-then-fetch view, armed
// later by armProjectedPlan) or window (the adaptive-window stack, also
// the rollback when the planned open itself fails — counted under
// lakehouse_s3_projected_fetch_fallback_total{reason="error"}).
func (s *Storage) openProjectedParquet(ctx context.Context, fi manifest.FileInfo, cachedSchema *parquet.Schema, cached *CachedFooter, usePlanned bool) (*parquet.File, *s3reader.PlannedFetchReaderAt, error) {
	if usePlanned {
		f, view, err := s.openPlannedParquet(ctx, fi, cachedSchema, cached)
		if err == nil {
			metrics.S3RangeReadsTotal.Inc()
			return f, view, nil
		}
		metrics.S3ProjectedFetchFallback.Inc("error")
		// Fall through to the window stack — same recovery the
		// pre-planned code used for a failed ranged open.
	}
	f, err := s.openRangedParquet(ctx, fi, cachedSchema, cached)
	if err != nil {
		return nil, nil, err
	}
	metrics.S3RangeReadsTotal.Inc()
	return f, nil, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
