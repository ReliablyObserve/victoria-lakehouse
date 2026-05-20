package main

import (
	"sort"
	"testing"
)

func TestBuildQueryMix(t *testing.T) {
	urls := buildQueryMix("http://localhost:9428")

	if len(urls) == 0 {
		t.Fatal("buildQueryMix returned empty slice")
	}

	for i, u := range urls {
		if u == "" {
			t.Fatalf("buildQueryMix returned empty URL at index %d", i)
		}
	}
}

func TestCollectLatencies_Percentiles(t *testing.T) {
	// Build a sorted slice of 100 values: 1.0, 2.0, ..., 100.0
	var latencies []float64
	for i := 1; i <= 100; i++ {
		latencies = append(latencies, float64(i))
	}
	sort.Float64s(latencies)

	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	// With index = int(99 * 0.50) = 49 → value 50.0
	if p50 != 50.0 {
		t.Errorf("expected p50=50.0, got %v", p50)
	}

	// With index = int(99 * 0.95) = 94 → value 95.0
	if p95 != 95.0 {
		t.Errorf("expected p95=95.0, got %v", p95)
	}

	// With index = int(99 * 0.99) = 98 → value 99.0
	if p99 != 99.0 {
		t.Errorf("expected p99=99.0, got %v", p99)
	}
}
