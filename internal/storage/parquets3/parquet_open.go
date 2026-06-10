package parquets3

import (
	"context"

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

// openRangedParquet opens fi over ranged S3 reads with the Tier-1 read
// hygiene options (s3-optimization research, PR 2a):
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
//   - FileReadMode(ReadModeAsync) (config: s3.parquet_read_mode): pages are
//     read ahead by a goroutine per Pages instance (AsyncPages); the read
//     channel is unbuffered, so memory is bounded at ~one page in flight per
//     column reader, and Close() drains the goroutine (all our page readers
//     close via defer). "sync" is the rollback switch.
//   - FileSchema(cachedSchema): skips re-deriving the schema from footer
//     metadata when the footer cache already holds it.
//
// The raw reader is wrapped in a PhaseReaderAt so every GET is attributed to
// the open or page phase (metrics.S3GetsByPhase) and the per-open GET count
// lands in metrics.S3GetsPerOpen — the research-doc "serial 4-6 GET open"
// baseline, now measurable per open.
func (s *Storage) openRangedParquet(ctx context.Context, fi manifest.FileInfo, cachedSchema *parquet.Schema) (*parquet.File, error) {
	raw := s.pool.NewReaderAt(ctx, fi.Key, fi.Size)
	phased := s3reader.NewPhaseReaderAt(raw)

	// BDP-priced windows are sized for LARGE files at real S3 RTT; on a file
	// smaller than the window they degenerate range-projection into a full
	// download (a 412KB file with a 1MB gap/window reads everything — caught by
	// TestGetFieldValues_UsesColumnProjectedRead). Clamp every knob by file
	// size, the same way ClickHouse bounds remote reads by read_until_position:
	// large files keep the round-trip-minimal windows, small files keep
	// precise column reads.
	clamp := func(v, lo, hi int64) int64 {
		if v < lo {
			return lo
		}
		if v > hi {
			return hi
		}
		return v
	}
	effGap := clamp(int64(s.cfg.S3.CoalesceGapBytes), 0, max64(64<<10, fi.Size/8))
	effBase := clamp(int64(s.cfg.S3.ReadAheadBytes), 0, max64(64<<10, fi.Size/4))
	effMax := clamp(int64(s.cfg.S3.ReadAheadMaxBytes), effBase, fi.Size)

	buffered := s3reader.NewBufferedReaderAt(phased, fi.Size, effBase, effMax)
	// Waste feedback: shrink the adaptive window when evicted windows were
	// mostly never read (sparse forward hops). <=0 keeps the 0.5 default;
	// >=1 disables. The file-size clamps above stay authoritative for the
	// base/max bounds the feedback floors/ceils against.
	buffered.SetWasteThreshold(s.cfg.S3.ReadAheadWasteThreshold)
	readerAt := s3reader.NewCoalescingReaderAt(buffered, fi.Size, effGap)

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
	if s.cfg.S3.ParquetReadMode != "sync" {
		opts = append(opts, parquet.FileReadMode(parquet.ReadModeAsync))
	}
	if cachedSchema != nil {
		opts = append(opts, parquet.FileSchema(cachedSchema))
	}

	f, err := parquet.OpenFile(readerAt, fi.Size, opts...)
	if err != nil {
		return nil, err
	}
	metrics.S3GetsPerOpen.Observe(float64(phased.OpenGets()))
	phased.SetPhase(s3reader.PhasePage)
	return f, nil
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
