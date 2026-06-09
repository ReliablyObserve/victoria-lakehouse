package pmeta

import (
	"bytes"
	"fmt"
	"math"
	"testing"
)

func relErr(est uint64, truth int) float64 {
	return math.Abs(float64(est)-float64(truth)) / float64(truth)
}

// TestHLL_Accuracy verifies the default (LogLog-Beta, p=14) estimator stays within
// a few percent across the full cardinality range — including the mid-range where
// plain HLL is biased.
func TestHLL_Accuracy(t *testing.T) {
	for _, n := range []int{100, 1000, 16000, 50000, 200000, 1000000} {
		h := newHLL(14)
		for i := 0; i < n; i++ {
			h.add(fmt.Sprintf("value-%d", i))
		}
		if e := relErr(h.estimate(), n); e > 0.03 {
			t.Errorf("n=%d: beta estimate=%d relErr=%.3f%% (>3%%)", n, h.estimate(), e*100)
		}
	}
}

// TestHLL_BetaVsClassic compares LogLog-Beta against the classic HLL estimator and
// asserts LogLog-Beta is at least as accurate overall — the whole point of HLL++.
func TestHLL_BetaVsClassic(t *testing.T) {
	var betaWorse int
	for _, n := range []int{500, 5000, 20000, 40000, 60000, 100000, 300000} {
		h := newHLL(14)
		for i := 0; i < n; i++ {
			h.add(fmt.Sprintf("k-%d", i))
		}
		be := relErr(h.estimateBeta(), n)
		ce := relErr(h.estimateClassic(), n)
		t.Logf("n=%-7d  classic=%.3f%%  beta=%.3f%%", n, ce*100, be*100)
		if be > 0.03 {
			t.Errorf("n=%d: beta relErr %.3f%% exceeds 3%%", n, be*100)
		}
		if be > ce+0.005 { // allow tiny noise, but beta must not be materially worse
			betaWorse++
		}
	}
	if betaWorse > 1 {
		t.Errorf("LogLog-Beta was materially worse than classic at %d/7 points", betaWorse)
	}
}

func TestHLL_Empty(t *testing.T) {
	if e := newHLL(14).estimate(); e != 0 {
		t.Fatalf("empty sketch estimate = %d, want 0", e)
	}
}

// TestHLL_MergeIsUnion: merging two sketches estimates the union cardinality
// (overlapping inputs are not double-counted).
func TestHLL_MergeIsUnion(t *testing.T) {
	a, b := newHLL(14), newHLL(14)
	for i := 0; i < 30000; i++ {
		a.add(fmt.Sprintf("v-%d", i)) // 0..29999
	}
	for i := 15000; i < 45000; i++ {
		b.add(fmt.Sprintf("v-%d", i)) // 15000..44999 (15k overlap)
	}
	if err := a.merge(b); err != nil {
		t.Fatal(err)
	}
	if e := relErr(a.estimate(), 45000); e > 0.03 { // union is 0..44999 = 45000
		t.Fatalf("merged estimate=%d relErr=%.3f%% (want ~45000)", a.estimate(), e*100)
	}
}

func TestHLL_MarshalRoundTrip(t *testing.T) {
	h := newHLL(14)
	for i := 0; i < 12345; i++ {
		h.add(fmt.Sprintf("x-%d", i))
	}
	got, err := unmarshalHLL(h.MarshalBinary())
	if err != nil {
		t.Fatal(err)
	}
	if got.estimate() != h.estimate() {
		t.Fatalf("round-trip estimate %d != %d", got.estimate(), h.estimate())
	}
}

func TestHLL_MergePrecisionMismatch(t *testing.T) {
	if err := newHLL(14).merge(newHLL(12)); err == nil {
		t.Fatal("merge across precisions must error")
	}
}

// FuzzHLLUnmarshal: arbitrary bytes must never panic or over-allocate.
func FuzzHLLUnmarshal(f *testing.F) {
	f.Add(newHLL(14).MarshalBinary())
	f.Add([]byte{14})
	f.Add([]byte{99})
	f.Add([]byte(nil))
	f.Fuzz(func(t *testing.T, data []byte) {
		if h, err := unmarshalHLL(data); err == nil {
			_ = h.estimate() // must not panic on a decoded sketch
		}
	})
}

// TestHLL_AccuracyDistributions checks accuracy holds across different value
// shapes — not just sequential ints — so the hash doesn't get unlucky on a
// realistic key distribution.
func TestHLL_AccuracyDistributions(t *testing.T) {
	shapes := map[string]func(i int) string{
		"sequential": func(i int) string { return fmt.Sprintf("v-%d", i) },
		"hex32":      func(i int) string { return fmt.Sprintf("%032x", i*2654435761) },
		"commonpref": func(i int) string { return fmt.Sprintf("service-prod-eu-west-1-pod-%d", i) },
		"sparse":     func(i int) string { return fmt.Sprintf("id_%d", i*7919) },
		"uuid_like":  func(i int) string { return fmt.Sprintf("%08x-%04x-%04x", i, i>>8, i*31) },
	}
	for name, gen := range shapes {
		for _, n := range []int{1000, 50000, 250000} {
			h := newHLL(14)
			for i := 0; i < n; i++ {
				h.add(gen(i))
			}
			if e := relErr(h.estimate(), n); e > 0.03 {
				t.Errorf("%s n=%d: relErr=%.3f%% (>3%%)", name, n, e*100)
			}
		}
	}
}

// TestHLL_NoFalse is the "no error, no false" guard: across the whole range the
// estimate is monotonic-ish and NEVER wildly wrong (no zeros for non-empty input,
// no order-of-magnitude misses), and tiny cardinalities are near-exact.
func TestHLL_NoFalse(t *testing.T) {
	// Tiny cardinalities: near-exact.
	for _, n := range []int{0, 1, 5, 25, 100} {
		h := newHLL(14)
		for i := 0; i < n; i++ {
			h.add(fmt.Sprintf("t-%d", i))
		}
		est := h.estimate()
		if math.Abs(float64(est)-float64(n)) > 3 {
			t.Errorf("tiny n=%d: est=%d (want within ±3)", n, est)
		}
	}
	// Full range: never off by more than 10% (a false/absurd result), and never
	// zero for non-empty input.
	for _, n := range []int{200, 2000, 30000, 80000, 500000, 2000000} {
		h := newHLL(14)
		for i := 0; i < n; i++ {
			h.add(fmt.Sprintf("n-%d", i))
		}
		est := h.estimate()
		if est == 0 {
			t.Fatalf("n=%d: estimate is 0 (false negative)", n)
		}
		if e := relErr(est, n); e > 0.10 {
			t.Fatalf("n=%d: est=%d relErr=%.1f%% — wildly wrong (>10%%)", n, est, e*100)
		}
	}
}

// FuzzHLLAdd: arbitrary value strings must never panic, and the estimate must
// stay sane (≤ number of distinct adds + slack, never NaN/huge).
func FuzzHLLAdd(f *testing.F) {
	f.Add("a", "b", "c")
	f.Add("", "\x00", "trace-12345")
	f.Fuzz(func(t *testing.T, a, b, c string) {
		h := newHLL(14)
		h.add(a)
		h.add(b)
		h.add(c)
		est := h.estimate()
		// at most 3 distinct values added → estimate must be a small, sane number
		if est > 100 {
			t.Fatalf("3 adds produced estimate %d (insane)", est)
		}
	})
}

// TestStore_CardinalityViaFlush is the integration / "our case" test: many flushed
// files each contributing trace_id values into the Store's per-field HLL; the
// Cardinality readout must match the true distinct count within bounds, dedup
// across files, and return 0 for an unknown field.
func TestStore_CardinalityViaFlush(t *testing.T) {
	s := NewStore()
	s.Register(FacetFieldCatalog, NewFieldCatalogFactory(NewDict()))

	const files, perFile = 50, 1000
	const total = files * perFile // 50000 distinct trace_ids across 50 files
	for fIdx := 0; fIdx < files; fIdx++ {
		vals := make([]string, perFile)
		for i := 0; i < perFile; i++ {
			vals[i] = fmt.Sprintf("trace-%d", fIdx*perFile+i)
		}
		s.OnFileFlush(FileContribution{
			Partition:      "logs/dt=2026-06-09/hour=10",
			FileKey:        fmt.Sprintf("f%d", fIdx),
			HighCardValues: map[string][]string{"trace_id": vals},
		})
	}

	if e := relErr(s.Cardinality("trace_id"), total); e > 0.03 {
		t.Fatalf("Cardinality(trace_id)=%d relErr=%.3f%% (true %d)", s.Cardinality("trace_id"), e*100, total)
	}
	if s.Cardinality("unknown_field") != 0 {
		t.Fatal("unknown field must report 0 cardinality")
	}

	// Re-flushing already-seen values (overlap across pods/retries) must NOT
	// inflate the count.
	before := s.Cardinality("trace_id")
	s.OnFileFlush(FileContribution{
		Partition:      "logs/dt=2026-06-09/hour=10",
		HighCardValues: map[string][]string{"trace_id": {"trace-0", "trace-1", "trace-2"}},
	})
	if after := s.Cardinality("trace_id"); after != before {
		t.Fatalf("duplicate values inflated cardinality: %d -> %d", before, after)
	}
}

var _ = bytes.Equal
