package s3reader

import (
	"context"
	"fmt"
	"io"
	"sort"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Range is one absolute byte range of the underlying object that a
// plan-then-fetch read will need (a column chunk, a dictionary page,
// a page-index section).
type Range struct {
	Off int64
	Len int64
}

// fetchedSpan is one coalesced, fully-fetched byte span held in memory.
type fetchedSpan struct {
	off  int64
	data []byte
}

// plannedFetchMaxInFlight bounds concurrent span GETs per file. Spans are
// few after coalescing (typically 1-4 for a projected read), so a small
// constant keeps per-file fanout bounded while still overlapping the
// round trips; the cross-file bound stays with the file-worker pool.
const plannedFetchMaxInFlight = 4

// PlannedFetchReaderAt is the CH-style plan-then-fetch reader for
// column-projected parquet reads (s3-optimization research, Tier-2
// items 8/9; arrow-rs vectored per-RG pattern).
//
// Lifecycle:
//
//  1. Construct over the RAW ranged reader (each inner ReadAt is exactly
//     one S3 GET). Until Fetch is called the view is a passthrough —
//     parquet.OpenFile's magic/footer reads become exact-range GETs with
//     no speculative window in front of them.
//  2. After the caller knows WHICH bytes the query needs (matched row
//     groups x projected column chunks, derived from the parquet footer),
//     Fetch coalesces the ranges (gap-priced like CoalescingReaderAt,
//     clamped by file size) and downloads the coalesced spans
//     CONCURRENTLY (bounded at min(4, spans) in flight).
//  3. Subsequent ReadAt calls are served from the fetched spans. Reads
//     OUTSIDE the fetched spans fall through to the underlying reader
//     and tick lakehouse_s3_planned_out_of_plan_reads_total — they are
//     never an error (correctness does not depend on plan completeness).
//  4. Redirect routes every subsequent read to a replacement reader —
//     the rollback used when a plan exceeds the configured byte cap and
//     the caller falls back to the adaptive-window stack.
//  5. Close releases the spans and the memory-budget charge.
//
// Memory accounting: the fetched bytes stay resident until Close, so a
// charge hook (see WithCharge / parquets3.chargePlannedFetchBytes) records
// them against the same ledger the decode path's admission control uses.
// The charge is taken before the spans are fetched and released on Close
// or on a failed Fetch.
type PlannedFetchReaderAt struct {
	inner    ReaderAtSizer
	fileSize int64
	gap      int64
	charge   func(n int64) (release func())

	mu       sync.RWMutex
	armed    bool
	spans    []fetchedSpan // sorted by off, disjoint
	redirect io.ReaderAt   // when set, ALL reads route here (window rollback)
	release  func()        // memory-budget release; nil once called
}

// NewPlannedFetchReaderAt wraps inner (the RAW ranged reader — one ReadAt
// = one exact S3 GET) for plan-then-fetch reads of a fileSize-byte object.
//
// gapBytes is the coalescing gap (cfg.S3.CoalesceGapBytes); it is clamped
// by file size exactly like the window path's gap (max(64KB, size/8)) and
// by the 16MB AnyBlob safety cap, so small files keep precise reads.
//
// charge is the memory-governor hook: called once per Fetch with the total
// coalesced span bytes BEFORE downloading; its return is invoked when the
// spans are released (Close, or Fetch failure). nil means no accounting.
func NewPlannedFetchReaderAt(inner ReaderAtSizer, fileSize, gapBytes int64, charge func(n int64) func()) *PlannedFetchReaderAt {
	if gapBytes < 0 {
		gapBytes = 0
	}
	if sizeCap := max64pf(64<<10, fileSize/8); gapBytes > sizeCap {
		gapBytes = sizeCap
	}
	const maxGap = 16 * 1024 * 1024 // AnyBlob cost-throughput-optimal upper bound
	if gapBytes > maxGap {
		gapBytes = maxGap
	}
	return &PlannedFetchReaderAt{
		inner:    inner,
		fileSize: fileSize,
		gap:      gapBytes,
		charge:   charge,
	}
}

// Inner returns the raw reader the view was constructed over. The caller
// uses it to build the adaptive-window rollback stack on a cap fallback
// (Redirect) without issuing a second S3 open.
func (r *PlannedFetchReaderAt) Inner() ReaderAtSizer {
	return r.inner
}

// Size returns the object size.
func (r *PlannedFetchReaderAt) Size() int64 {
	return r.fileSize
}

// PlanRanges coalesces ranges with the view's gap and clamps them to the
// file, returning the spans a Fetch would download and their total bytes.
// Exposed so the caller can price the plan against its byte cap BEFORE
// fetching (cap fallback decision) and so tests can assert coalescing.
// Metric-free — Fetch ticks the coalescing counters exactly once.
func (r *PlannedFetchReaderAt) PlanRanges(ranges []Range) ([]Range, int64) {
	merged, _, _ := coalesceRanges(ranges, r.gap, r.fileSize)
	var total int64
	for _, m := range merged {
		total += m.Len
	}
	return merged, total
}

// coalesceRanges clamps ranges to [0, fileSize), drops empty ones, and
// merges ranges within gap bytes of each other — the same policy as
// CoalescingReaderAt.PreloadRanges (mergeRangesWithOverfetch), reused via
// the readRange representation. Returns the merged spans, the number of
// round trips saved by merging, and the gap bytes over-fetched only
// because of merging.
func coalesceRanges(ranges []Range, gap, fileSize int64) (out []Range, saved int, overfetch int64) {
	rrs := make([]readRange, 0, len(ranges))
	for _, rg := range ranges {
		off, ln := rg.Off, rg.Len
		if off < 0 {
			ln += off
			off = 0
		}
		if fileSize > 0 && off+ln > fileSize {
			ln = fileSize - off
		}
		if ln <= 0 || off >= fileSize {
			continue
		}
		rrs = append(rrs, readRange{off: off, length: int(ln)})
	}
	merged, overfetch := mergeRangesWithOverfetch(rrs, gap)
	out = make([]Range, len(merged))
	for i, m := range merged {
		out[i] = Range{Off: m.off, Len: int64(m.length)}
	}
	return out, len(rrs) - len(merged), overfetch
}

// Fetch downloads the coalesced spans for ranges concurrently and arms the
// view. It must be called at most once; a second call is an error. On any
// download error the view is left un-armed (reads keep falling through to
// the underlying reader) and the memory charge is released — the caller
// decides whether to fall back to the window stack.
func (r *PlannedFetchReaderAt) Fetch(ctx context.Context, ranges []Range) error {
	merged, coalesced, overfetch := coalesceRanges(ranges, r.gap, r.fileSize)
	if len(merged) == 0 {
		return nil // nothing to plan; stay a passthrough
	}
	var total int64
	for _, m := range merged {
		total += m.Len
	}

	r.mu.Lock()
	if r.armed {
		r.mu.Unlock()
		return fmt.Errorf("planned fetch: Fetch called twice")
	}
	r.mu.Unlock()

	var release func()
	if r.charge != nil {
		release = r.charge(total)
	}

	spans := make([]fetchedSpan, len(merged))
	errs := make([]error, len(merged))
	sem := make(chan struct{}, minInt(plannedFetchMaxInFlight, len(merged)))
	var wg sync.WaitGroup
	for i, m := range merged {
		if ctx.Err() != nil {
			errs[i] = ctx.Err()
			break
		}
		wg.Add(1)
		sem <- struct{}{}
		go func(i int, m Range) {
			defer wg.Done()
			defer func() { <-sem }()
			buf := make([]byte, m.Len)
			n, err := r.inner.ReadAt(buf, m.Off)
			if err == io.EOF && m.Off+int64(n) >= r.fileSize {
				err = nil // tail span legitimately ends at EOF
			}
			if err == nil && int64(n) < m.Len {
				err = fmt.Errorf("planned fetch: short read %d/%d at %d", n, m.Len, m.Off)
			}
			if err != nil {
				errs[i] = err
				return
			}
			spans[i] = fetchedSpan{off: m.Off, data: buf[:n]}
		}(i, m)
	}
	wg.Wait()

	for _, err := range errs {
		if err != nil {
			if release != nil {
				release()
			}
			return err
		}
	}

	sort.Slice(spans, func(i, j int) bool { return spans[i].off < spans[j].off })

	r.mu.Lock()
	r.armed = true
	r.spans = spans
	r.release = release
	r.mu.Unlock()

	metrics.S3PlannedFetchesTotal.Inc()
	metrics.S3PlannedFetchSpansTotal.Add(len(merged))
	metrics.S3PlannedFetchBytesTotal.Add(int(total))
	if coalesced > 0 {
		metrics.S3CoalescedRanges.Add(coalesced)
	}
	if overfetch > 0 {
		metrics.S3CoalesceOverfetchBytes.Add(int(overfetch))
	}
	return nil
}

// Redirect routes ALL subsequent reads to fallback and releases any fetched
// spans. Used for the window-path rollback when a plan exceeds the byte cap.
func (r *PlannedFetchReaderAt) Redirect(fallback io.ReaderAt) {
	r.mu.Lock()
	r.redirect = fallback
	r.dropSpansLocked()
	r.mu.Unlock()
}

// ReadAt serves the read from the fetched spans when possible. Reads
// outside the spans (or before Fetch) fall through to the underlying
// reader — never an error. When a Redirect is installed all reads route
// to it.
func (r *PlannedFetchReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.mu.RLock()
	if r.redirect != nil {
		redirect := r.redirect
		r.mu.RUnlock()
		return redirect.ReadAt(p, off)
	}
	if !r.armed {
		r.mu.RUnlock()
		return r.inner.ReadAt(p, off)
	}
	// Binary search the span that could contain off.
	i := sort.Search(len(r.spans), func(i int) bool {
		return r.spans[i].off+int64(len(r.spans[i].data)) > off
	})
	if i < len(r.spans) {
		sp := r.spans[i]
		end := off + int64(len(p))
		if off >= sp.off && end <= sp.off+int64(len(sp.data)) {
			n := copy(p, sp.data[off-sp.off:end-sp.off])
			r.mu.RUnlock()
			return n, nil
		}
	}
	r.mu.RUnlock()
	// Out-of-plan read: fall through to the underlying reader. The plan is
	// a performance contract, not a correctness one.
	metrics.S3PlannedOutOfPlanReads.Inc()
	return r.inner.ReadAt(p, off)
}

// Close releases the fetched spans and the memory-budget charge. Idempotent.
func (r *PlannedFetchReaderAt) Close() error {
	r.mu.Lock()
	r.dropSpansLocked()
	r.mu.Unlock()
	return nil
}

// dropSpansLocked frees spans and releases the budget charge. Caller holds mu.
func (r *PlannedFetchReaderAt) dropSpansLocked() {
	r.spans = nil
	r.armed = false
	if r.release != nil {
		r.release()
		r.release = nil
	}
}

// SpanCount returns the number of fetched spans currently held (tests).
func (r *PlannedFetchReaderAt) SpanCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.spans)
}

func minInt(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func max64pf(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
