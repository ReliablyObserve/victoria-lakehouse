package main

import (
	"bytes"
	"fmt"
	"io"
	"math"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// calcDegradation returns the percentage absolute difference between baseline and actual.
// Uses the formula (|baseline - actual| / baseline) * 100.
// Returns 0 if baseline <= 0.
func calcDegradation(baseline, actual float64) float64 {
	if baseline <= 0 {
		return 0
	}
	return (math.Abs(baseline-actual) / baseline) * 100
}

// calcLatencyDegradation returns the percentage increase in a latency metric (lower is better).
// Returns 0 if baseline <= 0 or if actual <= baseline (no degradation).
func calcLatencyDegradation(baseline, actual float64) float64 {
	if baseline <= 0 {
		return 0
	}
	d := ((actual - baseline) / baseline) * 100
	if d < 0 {
		return 0
	}
	return d
}

// buildInsertBatch generates n NDJSON log lines as a byte slice.
func buildInsertBatch(n int) []byte {
	line := []byte(`{"_time":"2026-05-02T10:00:00Z","_msg":"load test","service.name":"loadtest","level":"info"}` + "\n")
	buf := make([]byte, 0, len(line)*n)
	for i := 0; i < n; i++ {
		buf = append(buf, line...)
	}
	return buf
}

// measureInsertRPS runs concurrency goroutines posting 100-row batches to /insert/jsonline
// for the given duration and returns the average rows/sec.
func measureInsertRPS(target string, concurrency int, dur time.Duration) float64 {
	var totalRows atomic.Int64
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)

	batch := buildInsertBatch(100)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			for time.Now().Before(deadline) {
				resp, err := client.Post(
					target+"/insert/jsonline",
					"application/x-ndjson",
					bytes.NewReader(batch),
				)
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 300 {
					totalRows.Add(100)
				}
			}
		}()
	}
	wg.Wait()

	if dur.Seconds() == 0 {
		return 0
	}
	return float64(totalRows.Load()) / dur.Seconds()
}

// measureQueryP95 runs concurrency goroutines querying random URLs from buildQueryMix
// for the given duration and returns the p95 latency in milliseconds.
func measureQueryP95(target string, concurrency int, dur time.Duration) float64 {
	deadline := time.Now().Add(dur)
	queryURLs := buildQueryMix(target)

	type workerLatencies struct {
		latencies []float64
	}

	workerResults := make([]workerLatencies, concurrency)
	var wg sync.WaitGroup

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			client := &http.Client{Timeout: 30 * time.Second}
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerIdx)))
			var localLatencies []float64

			for time.Now().Before(deadline) {
				u := queryURLs[rng.Intn(len(queryURLs))]
				start := time.Now()
				resp, err := client.Get(u)
				elapsed := time.Since(start)
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 400 {
					localLatencies = append(localLatencies, float64(elapsed.Microseconds())/1000.0)
				}
			}
			workerResults[workerIdx] = workerLatencies{latencies: localLatencies}
		}(i)
	}
	wg.Wait()

	var allLatencies []float64
	for _, wr := range workerResults {
		allLatencies = append(allLatencies, wr.latencies...)
	}
	if len(allLatencies) == 0 {
		return 0
	}
	sort.Float64s(allLatencies)
	return percentile(allLatencies, 0.95)
}

// measureMixedRW runs insert and query goroutines concurrently for the given duration,
// returning insert RPS and query p95 latency in milliseconds.
func measureMixedRW(target string, insertConc, queryConc int, dur time.Duration) (insertRPS float64, queryP95 float64) {
	deadline := time.Now().Add(dur)
	queryURLs := buildQueryMix(target)
	batch := buildInsertBatch(100)

	var totalRows atomic.Int64
	type workerLatencies struct {
		latencies []float64
	}
	queryWorkerResults := make([]workerLatencies, queryConc)

	var wg sync.WaitGroup

	// Insert goroutines.
	for i := 0; i < insertConc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			client := &http.Client{Timeout: 10 * time.Second}
			for time.Now().Before(deadline) {
				resp, err := client.Post(
					target+"/insert/jsonline",
					"application/x-ndjson",
					bytes.NewReader(batch),
				)
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 300 {
					totalRows.Add(100)
				}
			}
		}()
	}

	// Query goroutines.
	for i := 0; i < queryConc; i++ {
		wg.Add(1)
		go func(workerIdx int) {
			defer wg.Done()
			client := &http.Client{Timeout: 30 * time.Second}
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerIdx)))
			var localLatencies []float64

			for time.Now().Before(deadline) {
				u := queryURLs[rng.Intn(len(queryURLs))]
				start := time.Now()
				resp, err := client.Get(u)
				elapsed := time.Since(start)
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 400 {
					localLatencies = append(localLatencies, float64(elapsed.Microseconds())/1000.0)
				}
			}
			queryWorkerResults[workerIdx] = workerLatencies{latencies: localLatencies}
		}(i)
	}

	wg.Wait()

	if dur.Seconds() > 0 {
		insertRPS = float64(totalRows.Load()) / dur.Seconds()
	}

	var allLatencies []float64
	for _, wr := range queryWorkerResults {
		allLatencies = append(allLatencies, wr.latencies...)
	}
	if len(allLatencies) > 0 {
		sort.Float64s(allLatencies)
		queryP95 = percentile(allLatencies, 0.95)
	}

	return insertRPS, queryP95
}

// runMixedRWBenchmark runs 3 phases: insert-only baseline, query-only baseline,
// and mixed workload. Computes degradation percentages and returns the result.
func runMixedRWBenchmark(target string, durationStr string) *MixedRWResult {
	totalDur, err := time.ParseDuration(durationStr)
	if err != nil {
		totalDur = 90 * time.Second
	}

	phaseDur := totalDur / 3

	fmt.Printf("\n=== Mixed R/W Benchmark ===\n")
	fmt.Printf("Target: %s  Total duration: %s  Phase duration: %s\n\n", target, durationStr, phaseDur)

	fmt.Printf("Phase 1: Insert-only baseline...\n")
	insertBaseline := measureInsertRPS(target, 8, phaseDur)
	fmt.Printf("  Insert baseline: %.1f rows/s\n", insertBaseline)

	fmt.Printf("Phase 2: Query-only baseline...\n")
	queryBaseline := measureQueryP95(target, 8, phaseDur)
	fmt.Printf("  Query baseline p95: %.1fms\n", queryBaseline)

	fmt.Printf("Phase 3: Mixed workload...\n")
	mixedInsertRPS, mixedQueryP95 := measureMixedRW(target, 8, 8, phaseDur)
	fmt.Printf("  Mixed insert: %.1f rows/s  Mixed query p95: %.1fms\n", mixedInsertRPS, mixedQueryP95)

	insertDeg := calcDegradation(insertBaseline, mixedInsertRPS)
	queryDeg := calcLatencyDegradation(queryBaseline, mixedQueryP95)

	return &MixedRWResult{
		Duration:             durationStr,
		InsertBaselineRPS:    insertBaseline,
		QueryBaselineP95Ms:   queryBaseline,
		MixedInsertRPS:       mixedInsertRPS,
		MixedQueryP95Ms:      mixedQueryP95,
		InsertDegradationPct: insertDeg,
		QueryDegradationPct:  queryDeg,
	}
}
