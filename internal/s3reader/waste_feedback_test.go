package s3reader

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// These tests pin the S3-batch-2 waste-feedback contract on the adaptive
// read-ahead window. The combined benchmark measured 46 MB/query of
// fetched-but-never-read window bytes on filtered counts (56% hit rate) and
// 17 MB/query on fulltext scans: the reader hops forward by LESS than one
// window at a time, so every hop classifies as "forward-sequential" and the
// old state machine kept GROWING the window it then abandoned. Waste feedback
// halves the window (floored at base) whenever an evicted window's never-read
// ratio exceeds the threshold, and revokes the growth credit so the window
// only grows again after consecutive efficient windows.

// growReader drives br's window to max with fully-consumed sequential 1KB
// reads starting at offset 0 (base 1024 assumed), then consumes the final
// window fully so the NEXT eviction is efficient. Returns the end offset of
// the current (fully consumed) window.
func growReader(t *testing.T, br *BufferedS3ReaderAt, maxWindow int64) (bufEnd int64) {
	t.Helper()
	buf := make([]byte, 1024)
	off := int64(0)
	for br.Window() < maxWindow {
		mustRead(t, br, buf, off)
		off += 1024
	}
	br.mu.Lock()
	start, end := br.servedEnd, br.bufEnd
	br.mu.Unlock()
	for o := start; o < end; o += 1024 {
		mustRead(t, br, buf, o)
	}
	return end
}

// primeWastefulWindow issues one small read at the given offset (an efficient
// eviction of the fully-consumed previous window) so the FRESH window holds
// only that small served prefix — the wasteful state the next hop evicts.
// Returns the fresh window's end offset (the next hop target).
func primeWastefulWindow(t *testing.T, br *BufferedS3ReaderAt, off int64) int64 {
	t.Helper()
	small := make([]byte, 128)
	mustRead(t, br, small, off)
	br.mu.Lock()
	end := br.bufEnd
	br.mu.Unlock()
	return end
}

// TestWasteFeedback_SequentialEfficientReadsStillGrowToMax: (a) the scan
// pattern the window exists for — forward-sequential reads that consume every
// fetched byte — still grows the window to the ceiling and KEEPS it there;
// waste feedback never fires (shrink counter must not move).
func TestWasteFeedback_SequentialEfficientReadsStillGrowToMax(t *testing.T) {
	shrinksBefore := metrics.S3ReadAheadShrinks.Get()

	data := make([]byte, 256*1024)
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024, 8192)

	buf := make([]byte, 1024)
	for off := int64(0); off < 64*1024; off += 1024 {
		mustRead(t, br, buf, off)
	}
	if got := br.Window(); got != 8192 {
		t.Fatalf("window after a fully-consumed sequential scan = %d, want max 8192", got)
	}
	if d := metrics.S3ReadAheadShrinks.Get() - shrinksBefore; d != 0 {
		t.Errorf("shrinks fired on an efficient scan: delta = %d, want 0", d)
	}
}

// TestWasteFeedback_WastefulForwardHopsShrinkToBase: (b) the measured waste
// pattern — read ~1-10% of the window, then hop forward to just past its end
// (forward-sequential classification, so the OLD machine grew). Each wasteful
// eviction must HALVE the window until it floors at the base, and stay there.
func TestWasteFeedback_WastefulForwardHopsShrinkToBase(t *testing.T) {
	shrinksBefore := metrics.S3ReadAheadShrinks.Get()
	wasteBefore := metrics.S3BufferWastedBytes.Get()

	data := make([]byte, 1024*1024)
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024, 8192)

	bufEnd := growReader(t, br, 8192)
	if got := br.Window(); got != 8192 {
		t.Fatalf("setup: window = %d, want max 8192", got)
	}
	// Prime: the first hop evicts the fully-consumed setup window
	// (efficient) and leaves a fresh max-sized window with only 128 bytes
	// served — the wasteful state every following hop evicts.
	off := primeWastefulWindow(t, br, bufEnd)

	// Wasteful loop: hop to each fresh window's end having read only 128
	// of its bytes (~98% never-read). 8192 → 4096 → 2048 → 1024.
	small := make([]byte, 128)
	wantWindows := []int64{4096, 2048, 1024}
	for _, want := range wantWindows {
		mustRead(t, br, small, off)
		if got := br.Window(); got != want {
			t.Fatalf("window after wasteful eviction = %d, want %d", got, want)
		}
		br.mu.Lock()
		off = br.bufEnd // next hop: just past the freshly fetched window
		br.mu.Unlock()
	}

	// At the base the window must FLOOR — more waste neither shrinks below
	// base nor ticks the shrink counter again.
	mustRead(t, br, small, off)
	if got := br.Window(); got != 1024 {
		t.Fatalf("window went below base: %d, want 1024", got)
	}
	if d := metrics.S3ReadAheadShrinks.Get() - shrinksBefore; d != 3 {
		t.Errorf("S3ReadAheadShrinks delta = %d, want 3 (8192→4096→2048→1024)", d)
	}
	// The wasted-bytes counter must account the abandoned window bytes:
	// (8192-128) + (4096-128) + (2048-128) = 13952 at minimum.
	if d := metrics.S3BufferWastedBytes.Get() - wasteBefore; d < 13952 {
		t.Errorf("S3BufferWastedBytes delta = %d, want >= 13952", d)
	}
}

// TestWasteFeedback_GrowthCreditRevoked: after a wasteful eviction the growth
// credit resets — ONE efficient window is not enough to grow again (it takes
// 2+ consecutive efficient forward-sequential misses, same as from cold).
func TestWasteFeedback_GrowthCreditRevoked(t *testing.T) {
	data := make([]byte, 1024*1024)
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024, 8192)

	bufEnd := growReader(t, br, 8192)
	hop := primeWastefulWindow(t, br, bufEnd)

	// One wasteful hop: window 8192 → 4096, seqMisses revoked to 0.
	small := make([]byte, 128)
	mustRead(t, br, small, hop)
	if got := br.Window(); got != 4096 {
		t.Fatalf("window after wasteful eviction = %d, want 4096", got)
	}

	// Consume the rest of the fresh 4096-byte window fully (efficient).
	buf := make([]byte, 1024)
	br.mu.Lock()
	start, end := br.servedEnd, br.bufEnd
	br.mu.Unlock()
	for o := start; o < end-1024; o += 1024 {
		mustRead(t, br, buf, o)
	}
	mustRead(t, br, buf, end-1024)

	// First efficient forward miss after the waste: seqMisses 0→1 — NO grow.
	mustRead(t, br, buf, end)
	if got := br.Window(); got != 4096 {
		t.Fatalf("window grew on the first efficient miss after waste: %d, want 4096", got)
	}

	// Consume that window too; the SECOND consecutive efficient miss is
	// what restores growth (same credit rule as from cold).
	br.mu.Lock()
	start, end = br.servedEnd, br.bufEnd
	br.mu.Unlock()
	for o := start; o < end; o += 1024 {
		mustRead(t, br, buf, o)
	}
	mustRead(t, br, buf, end)
	if got := br.Window(); got != 8192 {
		t.Fatalf("window after 2 consecutive efficient misses = %d, want 8192", got)
	}
}

// TestWasteFeedback_MixedPatternOscillatesSanely: (c) alternating efficient
// stretches and wasteful hops oscillate the window between base and max —
// growth on sustained efficiency, halving on waste, never out of
// [base, maxWindow].
func TestWasteFeedback_MixedPatternOscillatesSanely(t *testing.T) {
	data := make([]byte, 4*1024*1024)
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024, 2048)

	checkBounds := func(stage string) {
		t.Helper()
		if w := br.Window(); w < 1024 || w > 2048 {
			t.Fatalf("%s: window %d escaped [base=1024, max=2048]", stage, w)
		}
	}

	// Efficient stretch: grow 1024 → 2048 (max).
	buf := make([]byte, 1024)
	for off := int64(0); off < 4096; off += 1024 {
		mustRead(t, br, buf, off)
		checkBounds("grow stretch")
	}
	if got := br.Window(); got != 2048 {
		t.Fatalf("window after efficient stretch = %d, want 2048", got)
	}
	// Consume the current [2048, 4096) window fully (read at 3072), then a
	// fresh window at 4096 that we abandon almost untouched.
	small := make([]byte, 128)
	mustRead(t, br, small, 4096) // efficient eviction, fetch [4096, 6144)
	mustRead(t, br, small, 6144) // WASTEFUL eviction (only 128/2048 read) → 1024
	if got := br.Window(); got != 1024 {
		t.Fatalf("window after wasteful hop = %d, want 1024 (halved to base)", got)
	}
	checkBounds("after waste")

	// Recover: consume [6144, 7168) fully, then run efficient again — the
	// window must climb back to max (oscillation, not a one-way ratchet).
	for o := int64(6272); o < 7168; o += 128 {
		mustRead(t, br, small, o)
	}
	for off := int64(7168); off < 13312; off += 1024 {
		mustRead(t, br, buf, off)
		checkBounds("regrow stretch")
	}
	if got := br.Window(); got != 2048 {
		t.Fatalf("window did not regrow after efficiency returned: %d, want 2048", got)
	}
}

// TestWasteFeedback_ThresholdDisableAndDefault pins the knob contract
// (s3.read_ahead_waste_threshold): >=1 disables feedback (the old grow
// behavior returns, because a window's waste ratio is always < 1), and
// non-positive SetWasteThreshold values keep the 0.5 default.
func TestWasteFeedback_ThresholdDisableAndDefault(t *testing.T) {
	shrinksBefore := metrics.S3ReadAheadShrinks.Get()

	data := make([]byte, 1024*1024)
	inner := &mockReaderAt{data: data}
	br := NewBufferedReaderAt(inner, inner.Size(), 1024, 4096)
	br.SetWasteThreshold(1.0) // disable

	// The wasteful hop pattern that would shrink at the default threshold:
	// with feedback disabled it must behave exactly like the pre-batch-2
	// machine and GROW on 2+ forward-sequential misses.
	small := make([]byte, 128)
	mustRead(t, br, small, 0)    // cold fill [0, 1024)
	mustRead(t, br, small, 1024) // 87.5% waste, but disabled → seqMisses=1
	mustRead(t, br, small, 2048) // seqMisses=2 → grow → 2048
	if got := br.Window(); got != 2048 {
		t.Fatalf("threshold>=1 must disable shrink and keep growth; window = %d, want 2048", got)
	}
	if d := metrics.S3ReadAheadShrinks.Get() - shrinksBefore; d != 0 {
		t.Errorf("shrinks fired with feedback disabled: delta = %d, want 0", d)
	}

	// Non-positive values keep the default.
	br2 := NewBufferedReaderAt(inner, inner.Size(), 1024, 4096)
	if br2.wasteThreshold != defaultWasteThreshold {
		t.Fatalf("constructor default = %v, want %v", br2.wasteThreshold, defaultWasteThreshold)
	}
	br2.SetWasteThreshold(0)
	br2.SetWasteThreshold(-0.3)
	if br2.wasteThreshold != defaultWasteThreshold {
		t.Fatalf("non-positive SetWasteThreshold must be ignored; got %v, want %v", br2.wasteThreshold, defaultWasteThreshold)
	}
	br2.SetWasteThreshold(0.9)
	if br2.wasteThreshold != 0.9 {
		t.Fatalf("SetWasteThreshold(0.9) not applied: %v", br2.wasteThreshold)
	}
}

// byteCountingReaderAt sums the bytes actually requested from the inner
// reader — the true S3 bytes-on-wire measure for the simulation below
// (the wasted-bytes COUNTER uses high-water accounting, which credits
// gap bytes "served" when a later read lands mid-window, so it understates
// over-fetch for sparse patterns).
type byteCountingReaderAt struct {
	data    []byte
	fetched int64
	gets    int64
}

func (r *byteCountingReaderAt) ReadAt(p []byte, off int64) (int, error) {
	r.fetched += int64(len(p))
	r.gets++
	m := &mockReaderAt{data: r.data}
	return m.ReadAt(p, off)
}

func (r *byteCountingReaderAt) Size() int64 { return int64(len(r.data)) }

// TestWasteFeedback_SimulatedPageProbeFetchReduction is the unit-level
// measurement for the S3-batch-2 CHANGELOG: replay the benchmark-shaped
// sparse page-probe pattern (read a 256 KB page at every 3 MB chunk start
// across a 64 MB file — the filtered_count shape where pruning leaves only
// a few needed pages per file) at PRODUCTION window parameters (2 MB base,
// 8 MB adaptive max) and compare total bytes fetched from the inner reader
// with waste feedback at the default threshold vs disabled (>=1 — the
// pre-batch-2 behavior). The old machine's wasteful forward hops vote
// "grow", so it tiles the file with 8 MB windows; feedback pins the window
// at base. Asserts >= 20% bytes-on-wire reduction so the effect can't
// silently regress.
func TestWasteFeedback_SimulatedPageProbeFetchReduction(t *testing.T) {
	const (
		fileSize = 64 * 1024 * 1024
		base     = 2 * 1024 * 1024
		maxWin   = 8 * 1024 * 1024
		stride   = 3 * 1024 * 1024
		pageSize = 256 * 1024
	)
	run := func(threshold float64) (fetched, gets int64) {
		inner := &byteCountingReaderAt{data: make([]byte, fileSize)}
		br := NewBufferedReaderAt(inner, inner.Size(), base, maxWin)
		br.SetWasteThreshold(threshold)
		page := make([]byte, pageSize)
		for off := int64(0); off+pageSize <= fileSize; off += stride {
			mustRead(t, br, page, off)
		}
		return inner.fetched, inner.gets
	}

	oldFetched, oldGets := run(1.0) // disabled = pre-batch-2 behavior
	newFetched, newGets := run(0)   // 0 → keep the 0.5 default

	t.Logf("page-probe sim (64MB file, 256KB page / 3MB stride, 2MB base / 8MB max):")
	t.Logf("  waste feedback OFF: fetched %.1f MB in %d GETs", float64(oldFetched)/1e6, oldGets)
	t.Logf("  waste feedback ON:  fetched %.1f MB in %d GETs", float64(newFetched)/1e6, newGets)

	if newFetched >= oldFetched {
		t.Fatalf("waste feedback did not reduce bytes fetched: %d >= %d", newFetched, oldFetched)
	}
	if reduction := 1 - float64(newFetched)/float64(oldFetched); reduction < 0.20 {
		t.Errorf("bytes-on-wire reduction = %.1f%%, want >= 20%%", reduction*100)
	}
}
