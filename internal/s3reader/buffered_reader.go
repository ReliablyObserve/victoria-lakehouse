package s3reader

import (
	"io"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ReaderAtSizer combines io.ReaderAt with a Size method,
// matching the interface already implemented by S3ReaderAt.
type ReaderAtSizer interface {
	io.ReaderAt
	Size() int64
}

// BufferedS3ReaderAt wraps a ReaderAtSizer with an ADAPTIVE read-ahead buffer
// (CH-style). Sequential reads within the prefetch window are served from the
// buffer, reducing the number of underlying S3 GetObject calls.
//
// Adaptivity: the window starts at the configured base size. After 2+
// consecutive forward-sequential misses (the reader keeps running off the end
// of the window — a scan), the window doubles up to maxWindow, halving the
// round-trip count of large sequential scans. A random seek (backwards, or a
// forward jump farther than one window) resets the window to base so needle
// queries never pay scan-sized over-fetch.
//
// Waste feedback (S3 batch 2): growth alone is blind to windows that are
// fetched but never read — the combined benchmark measured 46 MB/query of
// never-read window bytes on filtered scans at a 56% hit rate (the reader
// hops forward less than one window at a time, so every hop classifies as
// "forward-sequential" and GROWS the window it then abandons). On every
// window eviction the waste ratio (never-served bytes / window length, via
// the existing servedEnd high-water mark) is compared against
// wasteThreshold: a wasteful window HALVES the next window (floored at
// base) and revokes the growth credit, so the window only grows again after
// consecutive efficient windows. Allocation-free: scalar math on eviction.
//
// Tiny reads at offset 0 (parquet's 4-byte magic-header probe) bypass the
// window entirely and are served by an exact-size ranged GET — previously
// each cold open pulled a full window (~2 MB) just to check 4 bytes.
type BufferedS3ReaderAt struct {
	inner          ReaderAtSizer
	fileSize       int64
	base           int64   // configured window size (read-ahead base)
	maxWindow      int64   // adaptive growth ceiling (>= base)
	wasteThreshold float64 // waste ratio above which an evicted window shrinks the next one

	mu        sync.Mutex
	buf       []byte
	bufStart  int64
	bufEnd    int64
	window    int64 // current adaptive window size
	seqMisses int   // consecutive forward-sequential misses
	servedEnd int64 // high-water mark of bytes served from the current window
}

// headBypassMaxLen is the largest read at offset 0 served via an exact-size
// GET instead of a full window. 8 bytes covers parquet's "PAR1" magic probe
// with headroom; anything larger is a real data read that wants the window.
const headBypassMaxLen = 8

// defaultWasteThreshold is the waste ratio above which an evicted window is
// classified wasteful (config: s3.read_ahead_waste_threshold). 0.5 means a
// window whose bytes were less than half read shrinks the next fetch — the
// benchmark-measured wasteful patterns sit far above it (~90% never-read on
// filtered scans) while genuinely sequential windows sit at ~0%.
const defaultWasteThreshold = 0.5

// NewBufferedReaderAt creates a BufferedS3ReaderAt wrapping inner.
// prefetch is the base read-ahead window size in bytes (default 2MB if <= 0).
// maxPrefetch caps the adaptive window growth (default 8MB if <= 0; clamped
// to at least prefetch so a small max never shrinks the configured base).
func NewBufferedReaderAt(inner ReaderAtSizer, fileSize, prefetch, maxPrefetch int64) *BufferedS3ReaderAt {
	if prefetch <= 0 {
		prefetch = 2 * 1024 * 1024
	}
	const hardCap = 64 * 1024 * 1024 // 64MB safety cap
	if prefetch > hardCap {
		prefetch = hardCap
	}
	if maxPrefetch <= 0 {
		maxPrefetch = 8 * 1024 * 1024
	}
	if maxPrefetch > hardCap {
		maxPrefetch = hardCap
	}
	if maxPrefetch < prefetch {
		maxPrefetch = prefetch
	}
	return &BufferedS3ReaderAt{
		inner:          inner,
		fileSize:       fileSize,
		base:           prefetch,
		maxWindow:      maxPrefetch,
		wasteThreshold: defaultWasteThreshold,
		window:         prefetch,
		bufStart:       -1,
		bufEnd:         -1,
	}
}

// SetWasteThreshold overrides the waste-feedback threshold (config:
// s3.read_ahead_waste_threshold). Values <= 0 are ignored (keep the 0.5
// default); values >= 1 effectively disable waste feedback, because a
// window's waste ratio is always < 1 (every fetch serves at least the
// requesting read). Call before handing the reader to parquet-go — it is
// not synchronized against concurrent ReadAt.
func (b *BufferedS3ReaderAt) SetWasteThreshold(t float64) {
	if t > 0 {
		b.wasteThreshold = t
	}
}

// ReadAt reads len(p) bytes from the underlying source at byte offset off.
// It returns the number of bytes read and any error encountered.
// Reads that fall within the current buffer are served without an underlying call.
func (b *BufferedS3ReaderAt) ReadAt(p []byte, off int64) (int, error) {
	if off >= b.fileSize {
		return 0, io.EOF
	}

	b.mu.Lock()
	defer b.mu.Unlock()

	reqEnd := off + int64(len(p))

	// Buffer hit: requested range is fully contained in the buffer.
	if b.bufStart >= 0 && off >= b.bufStart && reqEnd <= b.bufEnd {
		metrics.S3BufferHits.Inc()
		if reqEnd > b.servedEnd {
			b.servedEnd = reqEnd
		}
		n := copy(p, b.buf[off-b.bufStart:reqEnd-b.bufStart])
		return n, nil
	}

	// Head bypass: a tiny read at offset 0 (parquet magic probe) is served
	// by an exact-size GET — never worth evicting/filling a window for.
	if off == 0 && len(p) <= headBypassMaxLen && b.fileSize > int64(len(p)) {
		metrics.S3HeadBypassReads.Inc()
		return b.inner.ReadAt(p, 0)
	}

	// Buffer miss: classify the access pattern before evicting the window.
	metrics.S3BufferMisses.Inc()
	if b.bufStart >= 0 {
		// Account fetched-but-never-served bytes of the evicted window and
		// derive its waste ratio (allocation-free: the servedEnd high-water
		// mark already tracks the served bytes).
		winLen := b.bufEnd - b.bufStart
		wasted := b.bufEnd - max(b.servedEnd, b.bufStart)
		if wasted > 0 {
			metrics.S3BufferWastedBytes.Add(int(wasted))
		}
		wasteful := winLen > 0 && float64(wasted) > b.wasteThreshold*float64(winLen)
		switch {
		case off >= b.bufEnd && off-b.bufEnd <= b.window:
			// Forward-sequential continuation: the reader ran off the end
			// of the window (or skipped less than one window ahead).
			if wasteful {
				// Waste feedback: the evicted window mostly fetched bytes
				// nobody read (sparse forward hops — the pattern that
				// previously kept GROWING the window it abandoned). Halve
				// the next window toward the base and revoke the growth
				// credit: growth resumes only after consecutive efficient
				// windows.
				b.seqMisses = 0
				if b.window > b.base {
					b.window /= 2
					if b.window < b.base {
						b.window = b.base
					}
					metrics.S3ReadAheadShrinks.Inc()
				}
			} else {
				b.seqMisses++
				if b.seqMisses >= 2 && b.window < b.maxWindow {
					b.window *= 2
					if b.window > b.maxWindow {
						b.window = b.maxWindow
					}
					metrics.S3ReadAheadGrows.Inc()
				}
			}
		default:
			// Random seek: reset to the base window (stronger than the
			// waste-feedback halving — needle queries drop straight back).
			b.seqMisses = 0
			if b.window != b.base {
				b.window = b.base
				metrics.S3ReadAheadResets.Inc()
			}
		}
	}

	// Fetch a new window starting at off.
	// Use max(window, len(p)) so a single fetch always covers the request.
	fetchSize := b.window
	if int64(len(p)) > fetchSize {
		fetchSize = int64(len(p))
	}
	fetchEnd := off + fetchSize
	if fetchEnd > b.fileSize {
		fetchEnd = b.fileSize
	}

	fetchBuf := make([]byte, fetchEnd-off)
	n, err := b.inner.ReadAt(fetchBuf, off)
	if err != nil && err != io.EOF {
		return 0, err
	}

	b.buf = fetchBuf[:n]
	b.bufStart = off
	b.bufEnd = off + int64(n)

	copyEnd := reqEnd
	if copyEnd > b.bufEnd {
		copyEnd = b.bufEnd
	}
	b.servedEnd = copyEnd
	copied := copy(p, b.buf[:copyEnd-off])
	// Only return io.EOF when we actually hit the end of the file.
	// Returning io.EOF for a mid-file short read violates io.ReaderAt's contract.
	if b.bufEnd < b.fileSize {
		return copied, nil
	}
	if copyEnd < reqEnd {
		return copied, io.EOF
	}
	return copied, nil
}

// Window returns the current adaptive read-ahead window size (for tests).
func (b *BufferedS3ReaderAt) Window() int64 {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.window
}

// Size returns the total file size.
func (b *BufferedS3ReaderAt) Size() int64 {
	return b.fileSize
}
