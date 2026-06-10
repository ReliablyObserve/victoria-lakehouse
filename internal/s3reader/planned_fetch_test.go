package s3reader

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// slowTrackingReaderAt is a fake S3 reader that records every ReadAt
// (offset+length), counts the in-flight high-water mark, and optionally
// sleeps per read so concurrency is observable.
type slowTrackingReaderAt struct {
	data  []byte
	delay time.Duration
	fail  func(off int64) error // optional injected error per offset

	mu       sync.Mutex
	reads    []Range
	inFlight int64
	maxIn    int64
}

func (r *slowTrackingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	cur := atomic.AddInt64(&r.inFlight, 1)
	defer atomic.AddInt64(&r.inFlight, -1)
	for {
		prev := atomic.LoadInt64(&r.maxIn)
		if cur <= prev || atomic.CompareAndSwapInt64(&r.maxIn, prev, cur) {
			break
		}
	}
	r.mu.Lock()
	r.reads = append(r.reads, Range{Off: off, Len: int64(len(p))})
	r.mu.Unlock()
	if r.delay > 0 {
		time.Sleep(r.delay)
	}
	if r.fail != nil {
		if err := r.fail(off); err != nil {
			return 0, err
		}
	}
	m := &mockReaderAt{data: r.data}
	return m.ReadAt(p, off)
}

func (r *slowTrackingReaderAt) Size() int64 { return int64(len(r.data)) }

func (r *slowTrackingReaderAt) readCount() int {
	r.mu.Lock()
	defer r.mu.Unlock()
	return len(r.reads)
}

func patternedData(n int) []byte {
	data := make([]byte, n)
	for i := range data {
		data[i] = byte((i*7 + i/255) % 256)
	}
	return data
}

// TestPlannedFetch_CoalescingCorrectness pins the plan shaping: ranges
// within the gap merge into one span, ranges beyond it stay separate,
// out-of-file ranges are clamped, and empty ranges are dropped.
func TestPlannedFetch_CoalescingCorrectness(t *testing.T) {
	inner := &slowTrackingReaderAt{data: patternedData(1 << 20)}
	v := NewPlannedFetchReaderAt(inner, inner.Size(), 4096, nil)

	ranges := []Range{
		{Off: 100_000, Len: 1000},
		{Off: 102_000, Len: 1000},   // gap 1000 <= 4096 → merges with previous
		{Off: 500_000, Len: 1000},   // far → own span
		{Off: 1_048_000, Len: 9999}, // tail-clamped to file size
		{Off: 2_000_000, Len: 50},   // beyond EOF → dropped
		{Off: 300_000, Len: 0},      // empty → dropped
	}
	merged, total := v.PlanRanges(ranges)
	want := []Range{
		{Off: 100_000, Len: 3000}, // 1000 + gap 1000 + 1000
		{Off: 500_000, Len: 1000},
		{Off: 1_048_000, Len: 1<<20 - 1_048_000},
	}
	if len(merged) != len(want) {
		t.Fatalf("merged spans = %v, want %v", merged, want)
	}
	for i := range want {
		if merged[i] != want[i] {
			t.Fatalf("span[%d] = %+v, want %+v", i, merged[i], want[i])
		}
	}
	wantTotal := int64(3000 + 1000 + (1<<20 - 1_048_000))
	if total != wantTotal {
		t.Fatalf("planned total = %d, want %d", total, wantTotal)
	}
}

// TestPlannedFetch_GapClamps pins the gap clamping: the configured gap is
// bounded by max(64KB, fileSize/8) and by the 16MB safety cap, so small
// files keep precise reads even under a scan-sized configured gap.
func TestPlannedFetch_GapClamps(t *testing.T) {
	// 256KB file: clamp = max(64KB, 32KB) = 64KB. A 1MB configured gap
	// must NOT merge ranges 100KB apart... but a 64KB gap still merges
	// ranges 60KB apart.
	inner := &slowTrackingReaderAt{data: patternedData(256 << 10)}
	v := NewPlannedFetchReaderAt(inner, inner.Size(), 1<<20, nil)
	if v.gap != 64<<10 {
		t.Fatalf("gap = %d, want clamp to %d (max(64KB, size/8))", v.gap, 64<<10)
	}
	merged, _ := v.PlanRanges([]Range{
		{Off: 0, Len: 1024},
		{Off: 100 << 10, Len: 1024}, // ~99KB gap > 64KB clamp → no merge
	})
	if len(merged) != 2 {
		t.Fatalf("expected clamped gap to keep 2 spans, got %v", merged)
	}

	// Huge file: the 16MB AnyBlob cap bounds the gap even when size/8 is
	// far larger.
	big := NewPlannedFetchReaderAt(&slowTrackingReaderAt{data: nil}, 1<<40, 1<<30, nil)
	if big.gap != 16<<20 {
		t.Fatalf("gap = %d, want 16MB safety cap", big.gap)
	}

	// Negative gap normalizes to 0 (no merging).
	neg := NewPlannedFetchReaderAt(inner, inner.Size(), -1, nil)
	if neg.gap != 0 {
		t.Fatalf("gap = %d, want 0 for negative input", neg.gap)
	}
}

// TestPlannedFetch_ConcurrentSpanFetch verifies the spans of one plan are
// fetched concurrently (overlapping in time on a slow reader) while never
// exceeding the per-file in-flight bound of 4.
func TestPlannedFetch_ConcurrentSpanFetch(t *testing.T) {
	inner := &slowTrackingReaderAt{data: patternedData(8 << 20), delay: 30 * time.Millisecond}
	v := NewPlannedFetchReaderAt(inner, inner.Size(), 0, nil)

	// 6 spans far apart (gap 0 → no merging).
	var ranges []Range
	for i := 0; i < 6; i++ {
		ranges = append(ranges, Range{Off: int64(i) * (1 << 20), Len: 64 << 10})
	}
	start := time.Now()
	if err := v.Fetch(context.Background(), ranges); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	elapsed := time.Since(start)

	if got := atomic.LoadInt64(&inner.maxIn); got < 2 {
		t.Errorf("max in-flight = %d, want >= 2 (spans must fetch concurrently)", got)
	} else if got > 4 {
		t.Errorf("max in-flight = %d, want <= 4 (bounded per-file fanout)", got)
	}
	// 6 spans at 30ms each: serial = 180ms, 4-way concurrent = ~60ms.
	if elapsed > 150*time.Millisecond {
		t.Errorf("Fetch took %v; spans appear to have been fetched serially", elapsed)
	}
	if v.SpanCount() != 6 {
		t.Fatalf("SpanCount = %d, want 6", v.SpanCount())
	}

	// Every in-plan read is served from memory: no further inner reads.
	innerReadsAfterFetch := inner.readCount()
	buf := make([]byte, 64<<10)
	for _, r := range ranges {
		n, err := v.ReadAt(buf, r.Off)
		if err != nil || n != len(buf) {
			t.Fatalf("ReadAt(%d): n=%d err=%v", r.Off, n, err)
		}
		if !bytes.Equal(buf, inner.data[r.Off:r.Off+int64(n)]) {
			t.Fatalf("data mismatch at %d", r.Off)
		}
	}
	if got := inner.readCount(); got != innerReadsAfterFetch {
		t.Errorf("in-plan reads hit the inner reader %d times; want 0", got-innerReadsAfterFetch)
	}
}

// TestPlannedFetch_OutOfPlanFallthrough: reads outside the fetched spans
// fall through to the underlying reader (correct data, never an error)
// and tick the out-of-plan counter; reads before Fetch are a passthrough
// and do NOT tick it.
func TestPlannedFetch_OutOfPlanFallthrough(t *testing.T) {
	inner := &slowTrackingReaderAt{data: patternedData(1 << 20)}
	v := NewPlannedFetchReaderAt(inner, inner.Size(), 0, nil)

	before := metrics.S3PlannedOutOfPlanReads.Get()

	// Un-armed passthrough (the open phase: magic probe, footer reads).
	buf4 := make([]byte, 4)
	if _, err := v.ReadAt(buf4, 0); err != nil {
		t.Fatalf("unarmed ReadAt: %v", err)
	}
	if got := metrics.S3PlannedOutOfPlanReads.Get(); got != before {
		t.Fatalf("un-armed read ticked the out-of-plan counter")
	}

	if err := v.Fetch(context.Background(), []Range{{Off: 4096, Len: 4096}}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	// In-plan read: no counter movement.
	in := make([]byte, 1024)
	if _, err := v.ReadAt(in, 5000); err != nil {
		t.Fatalf("in-plan ReadAt: %v", err)
	}
	if got := metrics.S3PlannedOutOfPlanReads.Get(); got != before {
		t.Fatalf("in-plan read ticked the out-of-plan counter")
	}

	// Out-of-plan read: served correctly from the underlying reader + tick.
	out := make([]byte, 1024)
	n, err := v.ReadAt(out, 700_000)
	if err != nil || n != len(out) {
		t.Fatalf("out-of-plan ReadAt: n=%d err=%v", n, err)
	}
	if !bytes.Equal(out, inner.data[700_000:700_000+1024]) {
		t.Fatal("out-of-plan read returned wrong data")
	}
	if got := metrics.S3PlannedOutOfPlanReads.Get(); got != before+1 {
		t.Fatalf("out-of-plan counter = %d, want %d", got, before+1)
	}

	// A read STRADDLING a span boundary is also out-of-plan (never split
	// across span + fallthrough) and must return correct data.
	straddle := make([]byte, 2048)
	if _, err := v.ReadAt(straddle, 7500); err != nil { // span ends at 8192
		t.Fatalf("straddling ReadAt: %v", err)
	}
	if !bytes.Equal(straddle, inner.data[7500:7500+2048]) {
		t.Fatal("straddling read returned wrong data")
	}
}

// TestPlannedFetch_BudgetAccounting pins the memory-governor contract:
// the charge is taken once per Fetch with the coalesced total, held while
// the spans are resident, and released exactly once on Close — or
// immediately when the Fetch fails.
func TestPlannedFetch_BudgetAccounting(t *testing.T) {
	var outstanding atomic.Int64
	charge := func(n int64) func() {
		outstanding.Add(n)
		var once sync.Once
		return func() { once.Do(func() { outstanding.Add(-n) }) }
	}

	inner := &slowTrackingReaderAt{data: patternedData(1 << 20)}
	v := NewPlannedFetchReaderAt(inner, inner.Size(), 4096, charge)
	ranges := []Range{{Off: 0, Len: 8192}, {Off: 10_000, Len: 8192}} // gap 1808 → merge
	merged, total := v.PlanRanges(ranges)
	if len(merged) != 1 {
		t.Fatalf("expected 1 merged span, got %v", merged)
	}
	if err := v.Fetch(context.Background(), ranges); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	if got := outstanding.Load(); got != total {
		t.Fatalf("outstanding budget = %d, want coalesced total %d", got, total)
	}
	if err := v.Close(); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if got := outstanding.Load(); got != 0 {
		t.Fatalf("budget not released on Close: %d", got)
	}
	_ = v.Close() // idempotent
	if got := outstanding.Load(); got != 0 {
		t.Fatalf("double Close corrupted the budget: %d", got)
	}

	// Error path: a failing span download must release the charge and
	// leave the view un-armed (reads still work as passthrough).
	failing := &slowTrackingReaderAt{
		data: patternedData(1 << 20),
		fail: func(off int64) error {
			if off >= 500_000 {
				return errors.New("injected S3 failure")
			}
			return nil
		},
	}
	v2 := NewPlannedFetchReaderAt(failing, failing.Size(), 0, charge)
	err := v2.Fetch(context.Background(), []Range{
		{Off: 0, Len: 4096},
		{Off: 600_000, Len: 4096}, // this span fails
	})
	if err == nil {
		t.Fatal("Fetch with failing span must return the error")
	}
	if got := outstanding.Load(); got != 0 {
		t.Fatalf("budget not released on Fetch error: %d", got)
	}
	if v2.SpanCount() != 0 {
		t.Fatalf("failed Fetch left %d spans armed", v2.SpanCount())
	}
	buf := make([]byte, 64)
	if _, rErr := v2.ReadAt(buf, 100); rErr != nil {
		t.Fatalf("passthrough read after failed Fetch: %v", rErr)
	}
}

// TestPlannedFetch_FetchTwiceErrors: arming is once-per-view.
func TestPlannedFetch_FetchTwiceErrors(t *testing.T) {
	inner := &slowTrackingReaderAt{data: patternedData(64 << 10)}
	v := NewPlannedFetchReaderAt(inner, inner.Size(), 0, nil)
	if err := v.Fetch(context.Background(), []Range{{Off: 0, Len: 1024}}); err != nil {
		t.Fatalf("first Fetch: %v", err)
	}
	if err := v.Fetch(context.Background(), []Range{{Off: 2048, Len: 1024}}); err == nil {
		t.Fatal("second Fetch must error")
	}
}

// TestPlannedFetch_Redirect: after Redirect (the cap fallback to the
// window stack) every read routes to the fallback reader, the spans are
// dropped, and the budget is released.
func TestPlannedFetch_Redirect(t *testing.T) {
	var outstanding atomic.Int64
	charge := func(n int64) func() {
		outstanding.Add(n)
		return func() { outstanding.Add(-n) }
	}
	inner := &slowTrackingReaderAt{data: patternedData(1 << 20)}
	v := NewPlannedFetchReaderAt(inner, inner.Size(), 0, charge)
	if err := v.Fetch(context.Background(), []Range{{Off: 0, Len: 4096}}); err != nil {
		t.Fatalf("Fetch: %v", err)
	}

	fallback := &slowTrackingReaderAt{data: inner.data}
	v.Redirect(fallback)
	if got := outstanding.Load(); got != 0 {
		t.Fatalf("Redirect must release the span budget, outstanding=%d", got)
	}
	if v.SpanCount() != 0 {
		t.Fatalf("Redirect left %d spans", v.SpanCount())
	}
	buf := make([]byte, 512)
	if _, err := v.ReadAt(buf, 1000); err != nil {
		t.Fatalf("redirected ReadAt: %v", err)
	}
	if fallback.readCount() != 1 {
		t.Fatalf("read did not route to the redirect reader (%d reads)", fallback.readCount())
	}
	if !bytes.Equal(buf, inner.data[1000:1512]) {
		t.Fatal("redirected read returned wrong data")
	}
}

// TestPlannedFetch_ConcurrentReadAt exercises armed reads from many
// goroutines (the ReadModeAsync page readers) — run with -race.
func TestPlannedFetch_ConcurrentReadAt(t *testing.T) {
	inner := &slowTrackingReaderAt{data: patternedData(4 << 20)}
	v := NewPlannedFetchReaderAt(inner, inner.Size(), 0, nil)
	ranges := []Range{
		{Off: 0, Len: 1 << 20},
		{Off: 2 << 20, Len: 1 << 20},
	}
	if err := v.Fetch(context.Background(), ranges); err != nil {
		t.Fatalf("Fetch: %v", err)
	}
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			buf := make([]byte, 4096)
			for i := 0; i < 100; i++ {
				off := int64((g*100 + i) % (1 << 20))
				if g%2 == 1 {
					off += 2 << 20
				}
				if _, err := v.ReadAt(buf, off); err != nil {
					t.Errorf("concurrent ReadAt(%d): %v", off, err)
					return
				}
			}
		}(g)
	}
	wg.Wait()
}

// TestPlannedFetch_FilteredCountAccessPatternMeasurement is the unit-level
// measurement for the Tier-2 plan-then-fetch CHANGELOG entry. It replays
// the post-batch-2 filtered_count shape — 28 files, each 24MB with ~300KB
// of projected column chunks (3 chunks x 100KB in one matched row group)
// — and compares bytes-on-wire:
//
//   - WINDOW mode: the production projected-read stack (2MB base / 8MB max
//     adaptive window + coalescing reader), a FRESH reader per file — the
//     per-reader-instance adaptive state that the live benchmark showed
//     never learns (readahead_shrink_total = 0): every file pays ~a full
//     base window for ~300KB of useful bytes;
//   - PLANNED mode: PlannedFetchReaderAt fetching the exact coalesced
//     chunk ranges.
//
// The asserted bound (>= 75% reduction) is the regression gate for the
// measured ~46 MB/query window waste this feature removes.
func TestPlannedFetch_FilteredCountAccessPatternMeasurement(t *testing.T) {
	const (
		numFiles  = 28
		fileSize  = 24 << 20
		base      = 2 << 20 // production read_ahead_bytes
		maxWin    = 8 << 20 // production read_ahead_max_bytes
		gap       = 1 << 20 // production coalesce_gap_bytes
		chunkLen  = 100 << 10
		numChunks = 3
		pageRead  = 32 << 10 // page-sized reads parquet issues per chunk
	)
	// One matched row group per file; the 3 projected chunks sit adjacent
	// in the row group's column run (non-projected columns between them
	// are small), starting mid-file.
	chunkOffsets := func() []int64 {
		return []int64{8 << 20, (8 << 20) + 110<<10, (8 << 20) + 220<<10}
	}

	readChunks := func(r interface {
		ReadAt(p []byte, off int64) (int, error)
	}) {
		buf := make([]byte, pageRead)
		for _, co := range chunkOffsets() {
			for o := int64(0); o < chunkLen; o += pageRead {
				// parquet bounds chunk reads by TotalCompressedSize —
				// clamp the last page read to the chunk end.
				n := int64(pageRead)
				if o+n > chunkLen {
					n = chunkLen - o
				}
				if _, err := r.ReadAt(buf[:n], co+o); err != nil {
					t.Fatalf("chunk read at %d: %v", co+o, err)
				}
			}
		}
	}

	var windowBytes, windowGets int64
	for i := 0; i < numFiles; i++ {
		inner := &byteCountingReaderAt{data: make([]byte, fileSize)}
		br := NewBufferedReaderAt(inner, inner.Size(), base, maxWin)
		readChunks(NewCoalescingReaderAt(br, inner.Size(), gap))
		windowBytes += inner.fetched
		windowGets += inner.gets
	}

	var plannedBytes, plannedGets int64
	for i := 0; i < numFiles; i++ {
		inner := &byteCountingReaderAt{data: make([]byte, fileSize)}
		v := NewPlannedFetchReaderAt(inner, inner.Size(), gap, nil)
		var ranges []Range
		for _, co := range chunkOffsets() {
			ranges = append(ranges, Range{Off: co, Len: chunkLen})
		}
		if err := v.Fetch(context.Background(), ranges); err != nil {
			t.Fatalf("Fetch: %v", err)
		}
		readChunks(v)
		_ = v.Close()
		plannedBytes += inner.fetched
		plannedGets += inner.gets
	}

	usefulBytes := int64(numFiles * numChunks * chunkLen)
	reduction := 1 - float64(plannedBytes)/float64(windowBytes)
	t.Logf("filtered_count access-pattern sim (%d files x %dMB, %dKB projected chunks/file):", numFiles, fileSize>>20, (numChunks*chunkLen)>>10)
	t.Logf("  window  mode: %6.2f MB on wire in %3d GETs (%.2f MB/file)", float64(windowBytes)/1e6, windowGets, float64(windowBytes)/1e6/numFiles)
	t.Logf("  planned mode: %6.2f MB on wire in %3d GETs (%.2f MB/file)", float64(plannedBytes)/1e6, plannedGets, float64(plannedBytes)/1e6/numFiles)
	t.Logf("  useful bytes: %6.2f MB; reduction = %.1f%%", float64(usefulBytes)/1e6, reduction*100)

	if reduction < 0.75 {
		t.Errorf("bytes-on-wire reduction = %.1f%% , want >= 75%% (the 46MB/q window-waste regression gate)", reduction*100)
	}
	if plannedGets > windowGets*2 {
		t.Errorf("planned mode GETs (%d) exploded vs window mode (%d)", plannedGets, windowGets)
	}
}

var _ = fmt.Sprintf // keep fmt for debugging edits
