package main

import (
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// buildQueryMix returns a slice of query URLs built from all realistic scenarios.
func buildQueryMix(target string) []string {
	scenarios := buildRealisticScenarios()
	urls := make([]string, 0, len(scenarios))
	for _, sc := range scenarios {
		urls = append(urls, sc.URLFn(target))
	}
	return urls
}

// runConcurrentBenchmark runs the concurrent benchmark at each concurrency level
// and prints a progress table. Returns one ConcurrentResult per level.
func runConcurrentBenchmark(target string, durationStr string, concurrencyLevels []int) []ConcurrentResult {
	dur, err := time.ParseDuration(durationStr)
	if err != nil {
		dur = 30 * time.Second
	}

	queryURLs := buildQueryMix(target)

	fmt.Printf("\n=== Concurrent Query Benchmark ===\n")
	fmt.Printf("Target: %s  Duration per level: %s  Query mix: %d URLs\n\n", target, durationStr, len(queryURLs))
	fmt.Printf("  %-12s %12s %10s %8s %8s %8s %10s %8s\n",
		"Concurrency", "TotalQueries", "Errors", "p50", "p95", "p99", "QPS", "ErrRate%")
	fmt.Printf("  %s\n", repeatStr("-", 90))

	results := make([]ConcurrentResult, 0, len(concurrencyLevels))
	for _, c := range concurrencyLevels {
		res := runAtConcurrency(target, queryURLs, c, dur)
		results = append(results, res)
		fmt.Printf("  %-12d %12d %10d %7.1fms %7.1fms %7.1fms %10.1f %7.2f%%\n",
			res.Concurrency, res.TotalQueries, res.Errors,
			res.P50Ms, res.P95Ms, res.P99Ms, res.QPS, res.ErrorRate)
	}

	return results
}

// repeatStr returns s repeated n times (avoids importing strings for a single use).
func repeatStr(s string, n int) string {
	out := make([]byte, len(s)*n)
	for i := 0; i < n; i++ {
		copy(out[i*len(s):], s)
	}
	return string(out)
}

// runAtConcurrency runs the benchmark at a fixed concurrency level for the given duration.
func runAtConcurrency(target string, queryURLs []string, concurrency int, dur time.Duration) ConcurrentResult {
	deadline := time.Now().Add(dur)

	var (
		totalQueries int64
		errors       int64
		rejected429  int64
	)

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
			rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerIdx))) // #nosec G404
			var localLatencies []float64

			for time.Now().Before(deadline) {
				u := queryURLs[rng.Intn(len(queryURLs))]

				start := time.Now()
				resp, err := client.Get(u)
				elapsed := time.Since(start)

				atomic.AddInt64(&totalQueries, 1)

				if err != nil {
					atomic.AddInt64(&errors, 1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				if resp.StatusCode == http.StatusTooManyRequests {
					atomic.AddInt64(&rejected429, 1)
					continue
				}

				if resp.StatusCode >= 400 {
					atomic.AddInt64(&errors, 1)
					continue
				}

				localLatencies = append(localLatencies, float64(elapsed.Microseconds())/1000.0)
			}

			workerResults[workerIdx] = workerLatencies{latencies: localLatencies}
		}(i)
	}

	wg.Wait()

	// Merge all worker latency slices.
	var allLatencies []float64
	for _, wr := range workerResults {
		allLatencies = append(allLatencies, wr.latencies...)
	}

	sort.Float64s(allLatencies)

	var p50, p95, p99 float64
	if len(allLatencies) > 0 {
		p50 = percentile(allLatencies, 0.50)
		p95 = percentile(allLatencies, 0.95)
		p99 = percentile(allLatencies, 0.99)
	}

	total := atomic.LoadInt64(&totalQueries)
	errs := atomic.LoadInt64(&errors)
	rej := atomic.LoadInt64(&rejected429)

	var qps float64
	if dur > 0 {
		qps = float64(total) / dur.Seconds()
	}

	var errorRate float64
	if total > 0 {
		errorRate = float64(errs) / float64(total) * 100.0
	}

	return ConcurrentResult{
		Concurrency:  concurrency,
		Duration:     dur.String(),
		TotalQueries: total,
		Errors:       errs,
		Rejected429:  rej,
		P50Ms:        p50,
		P95Ms:        p95,
		P99Ms:        p99,
		QPS:          qps,
		ErrorRate:    errorRate,
	}
}
