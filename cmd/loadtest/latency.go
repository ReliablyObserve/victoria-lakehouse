package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"time"
)

func runLatencyBenchmarks(target string, iterations int) map[string]*LatencyResult {
	results := make(map[string]*LatencyResult)

	results["manifest_fast_path"] = benchmarkLatency(target, iterations, func(t string) string {
		future := time.Now().Add(365 * 24 * time.Hour)
		start := fmt.Sprintf("%d", future.UnixNano())
		end := fmt.Sprintf("%d", future.Add(time.Hour).UnixNano())
		return fmt.Sprintf("%s/select/logsql/query?query=*&start=%s&end=%s", t, start, end)
	}, 1.0)

	results["bloom_point_query"] = benchmarkLatency(target, iterations, func(t string) string {
		return fmt.Sprintf("%s/select/logsql/query?query=%s&limit=1", t,
			url.QueryEscape(`trace_id:="0000000000000001"`))
	}, 100.0)

	results["time_range_scan_1h"] = benchmarkLatency(target, iterations, func(t string) string {
		end := time.Now()
		start := end.Add(-1 * time.Hour)
		return fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=100",
			t, start.UnixNano(), end.UnixNano())
	}, 500.0)

	results["stats_aggregation"] = benchmarkLatency(target, iterations, func(t string) string {
		end := time.Now()
		start := end.Add(-1 * time.Hour)
		return fmt.Sprintf("%s/select/logsql/stats_query?query=*&start=%d&end=%d",
			t, start.UnixNano(), end.UnixNano())
	}, 300.0)

	results["field_names"] = benchmarkLatency(target, iterations, func(t string) string {
		return fmt.Sprintf("%s/select/logsql/field_names", t)
	}, 1.0)

	results["field_values"] = benchmarkLatency(target, iterations, func(t string) string {
		return fmt.Sprintf("%s/select/logsql/field_values?field=%s", t, url.QueryEscape("service.name"))
	}, 1.0)

	return results
}

func benchmarkLatency(target string, iterations int, urlFn func(string) string, targetP95 float64) *LatencyResult {
	client := &http.Client{Timeout: 30 * time.Second}
	var latencies []float64

	for i := 0; i < iterations; i++ {
		u := urlFn(target)
		start := time.Now()
		resp, err := client.Get(u)
		elapsed := time.Since(start)
		if err != nil {
			continue
		}
		io.Copy(io.Discard, resp.Body)
		resp.Body.Close()
		latencies = append(latencies, float64(elapsed.Microseconds())/1000.0)
	}

	if len(latencies) == 0 {
		return &LatencyResult{TargetP95Ms: targetP95, Iterations: iterations}
	}

	sort.Float64s(latencies)
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	return &LatencyResult{
		P50Ms:       p50,
		P95Ms:       p95,
		P99Ms:       p99,
		TargetP95Ms: targetP95,
		Pass:        p95 <= targetP95,
		Iterations:  len(latencies),
	}
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}
