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

var _ = bytes.Equal
