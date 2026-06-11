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

// plannedFetchDefaultMaxInFlight bounds concurrent span GETs per file when
// the caller does not configure one (s3.planned_fetch_max_inflight). The
// live planned-v1 verdict measured 13-15 spans/file on real L2 geometry
// being drained 4-at-a-time into ~4 serial RTT waves per file; the offline
// simulator named span concurrency the single biggest v2 lever. k =
// min(16, spans): every span of a typical per-file plan is in flight in
// ONE wave, while the cross-file bound stays with the file-worker pool
// (8 workers x 16 spans = 128 = MaxIdleConnsPerHost, the true HTTP/1.1
// parallelism ceiling against S3/MinIO).
const plannedFetchDefaultMaxInFlight = 16

// plannedFetchDefaultSpanCap is the per-SPAN byte cap when the caller does
// not configure one (s3.planned_fetch_span_cap_bytes). This is ClickHouse's
// bytes_per_read_task scope (16 MiB PER read task, NOT per plan): a merged
// span larger than the cap is SPLIT into cap-sized spans fetched
// concurrently, instead of the whole plan being rejected. Plan-level
// admission is the memory ledger's job (the worker already holds an
// fi.Size admission that subsumes any plan).
const plannedFetchDefaultSpanCap = 16 * 1024 * 1024

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
	spanCap  int64 // per-SPAN byte cap; merged spans above it are SPLIT
	maxInFl  int   // concurrent span GETs: min(maxInFl, spans) in flight
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
	return &PlannedFetchReaderAt{
		inner:    inner,
		fileSize: fileSize,
		gap:      clampPlannedGap(gapBytes, fileSize),
		spanCap:  plannedFetchDefaultSpanCap,
		maxInFl:  plannedFetchDefaultMaxInFlight,
		charge:   charge,
	}
}

// clampPlannedGap bounds a coalescing gap by file size — max(64KB, size/8),
// the same clamp as the window path's gap — and by the 16MB AnyBlob
// cost-throughput-optimal upper bound, so small files keep precise reads.
func clampPlannedGap(gapBytes, fileSize int64) int64 {
	if gapBytes < 0 {
		gapBytes = 0
	}
	if sizeCap := max64pf(64<<10, fileSize/8); gapBytes > sizeCap {
		gapBytes = sizeCap
	}
	const maxGap = 16 * 1024 * 1024
	if gapBytes > maxGap {
		gapBytes = maxGap
	}
	return gapBytes
}

// SetGap replaces the view's coalescing gap (file-size-clamped like the
// constructor). Called by the gap-discipline pricing in armProjectedPlan
// BEFORE Fetch with the cheapest candidate gap.
func (r *PlannedFetchReaderAt) SetGap(gapBytes int64) {
	r.mu.Lock()
	r.gap = clampPlannedGap(gapBytes, r.fileSize)
	r.mu.Unlock()
}

// SetSpanCap sets the per-SPAN byte cap (s3.planned_fetch_span_cap_bytes).
// Merged spans above the cap are split into cap-sized spans — the CH
// bytes_per_read_task scope. n <= 0 keeps the 16MB default.
func (r *PlannedFetchReaderAt) SetSpanCap(n int64) {
	if n <= 0 {
		n = plannedFetchDefaultSpanCap
	}
	r.mu.Lock()
	r.spanCap = n
	r.mu.Unlock()
}

// SetMaxInFlight sets the concurrent span GET bound for Fetch
// (s3.planned_fetch_max_inflight); the effective fanout is
// min(k, spans). k <= 0 keeps the default 16.
func (r *PlannedFetchReaderAt) SetMaxInFlight(k int) {
	if k <= 0 {
		k = plannedFetchDefaultMaxInFlight
	}
	r.mu.Lock()
	r.maxInFl = k
	r.mu.Unlock()
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

// PlanRanges coalesces ranges with the view's gap (splitting spans above
// the per-span cap) and clamps them to the file, returning the spans a
// Fetch would download and their total bytes. Exposed so the caller can
// price the plan BEFORE fetching and so tests can assert coalescing.
// Metric-free — Fetch ticks the coalescing counters exactly once.
func (r *PlannedFetchReaderAt) PlanRanges(ranges []Range) ([]Range, int64) {
	r.mu.RLock()
	gap := r.gap
	r.mu.RUnlock()
	return r.PlanRangesAt(ranges, gap)
}

// PlanRangesAt is PlanRanges at an arbitrary CANDIDATE gap (file-size-
// clamped like the constructor) — the pure in-memory pricing primitive
// behind the gap discipline: armProjectedPlan prices the plan at several
// candidate gaps and Fetches with the cheapest.
func (r *PlannedFetchReaderAt) PlanRangesAt(ranges []Range, gapBytes int64) ([]Range, int64) {
	r.mu.RLock()
	spanCap := r.spanCap
	r.mu.RUnlock()
	merged, _, _ := coalesceRanges(ranges, clampPlannedGap(gapBytes, r.fileSize), r.fileSize, spanCap)
	var total int64
	for _, m := range merged {
		total += m.Len
	}
	return merged, total
}

// coalesceRanges clamps ranges to [0, fileSize), drops empty ones, merges
// ranges within gap bytes of each other — the same policy as
// CoalescingReaderAt.PreloadRanges (mergeRangesWithOverfetch), reused via
// the readRange representation — and then SPLITS any merged span larger
// than spanCap into cap-sized spans (CH bytes_per_read_task: the cap
// bounds one GET, never the plan). Returns the spans, the number of round
// trips saved by merging (counted BEFORE splitting — split spans are
// adjacent ranges, not extra waste), and the gap bytes over-fetched only
// because of merging. spanCap <= 0 means no splitting.
func coalesceRanges(ranges []Range, gap, fileSize, spanCap int64) (out []Range, saved int, overfetch int64) {
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
	saved = len(rrs) - len(merged)
	out = make([]Range, 0, len(merged))
	for _, m := range merged {
		off, ln := m.off, int64(m.length)
		for spanCap > 0 && ln > spanCap {
			out = append(out, Range{Off: off, Len: spanCap})
			off += spanCap
			ln -= spanCap
		}
		out = append(out, Range{Off: off, Len: ln})
	}
	return out, saved, overfetch
}

// Fetch downloads the coalesced spans for ranges concurrently and arms the
// view. It must be called at most once; a second call is an error. On any
// download error the view is left un-armed (reads keep falling through to
// the underlying reader) and the memory charge is released — the caller
// decides whether to fall back to the window stack.
func (r *PlannedFetchReaderAt) Fetch(ctx context.Context, ranges []Range) error {
	r.mu.RLock()
	gap, spanCap, maxInFl := r.gap, r.spanCap, r.maxInFl
	r.mu.RUnlock()
	merged, coalesced, overfetch := coalesceRanges(ranges, gap, r.fileSize, spanCap)
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
	sem := make(chan struct{}, minInt(maxInFl, len(merged)))
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
