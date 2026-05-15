package main

import (
	"fmt"
	"io"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"time"
)

type RealisticScenario struct {
	Name        string
	Category    string
	Description string
	URLFn       func(target string) string
	TargetP95Ms float64
}

type RealisticResult struct {
	Name        string  `json:"name"`
	Category    string  `json:"category"`
	Description string  `json:"description"`
	P50Ms       float64 `json:"p50_ms"`
	P95Ms       float64 `json:"p95_ms"`
	P99Ms       float64 `json:"p99_ms"`
	MaxMs       float64 `json:"max_ms"`
	MinMs       float64 `json:"min_ms"`
	TargetP95Ms float64 `json:"target_p95_ms"`
	Pass        bool    `json:"pass"`
	Iterations  int     `json:"iterations"`
}

func buildRealisticScenarios() []RealisticScenario {
	now := time.Now()
	oneHourAgo := now.Add(-1 * time.Hour)
	sixHoursAgo := now.Add(-6 * time.Hour)
	oneDayAgo := now.Add(-24 * time.Hour)
	twoDaysAgo := now.Add(-48 * time.Hour)
	future := now.Add(365 * 24 * time.Hour)

	return []RealisticScenario{
		// === MANIFEST FAST PATH ===
		{
			Name:        "manifest_nothing_here",
			Category:    "fast_path",
			Description: "Query future time range — manifest returns empty instantly",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d",
					t, future.UnixNano(), future.Add(time.Hour).UnixNano())
			},
			TargetP95Ms: 1.0,
		},

		// === POINT LOOKUPS (Bloom Filter) ===
		{
			Name:        "bloom_trace_id_hit",
			Category:    "point_lookup",
			Description: "Exact trace_id match via bloom filter",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=1",
					t, url.QueryEscape(`trace_id:="0000000000000001"`),
					twoDaysAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 100.0,
		},
		{
			Name:        "bloom_trace_id_miss",
			Category:    "point_lookup",
			Description: "trace_id that doesn't exist — bloom says no",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=1",
					t, url.QueryEscape(`trace_id:="ffffffffffffffffffffffffffffffff"`),
					twoDaysAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 100.0,
		},
		{
			Name:        "bloom_service_exact",
			Category:    "point_lookup",
			Description: "Exact service.name match via bloom filter",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=10",
					t, url.QueryEscape(`service.name:="api-gateway"`),
					oneHourAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 100.0,
		},

		// === SHORT RANGE QUERIES (cached after first hit) ===
		{
			Name:        "short_range_1h_wildcard",
			Category:    "short_range",
			Description: "Last 1 hour, wildcard, limit 100",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=100",
					t, oneHourAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 500.0,
		},
		{
			Name:        "short_range_1h_filtered",
			Category:    "short_range",
			Description: "Last 1 hour, ERROR level filter",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=100",
					t, url.QueryEscape(`level:="ERROR"`),
					oneHourAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 500.0,
		},
		{
			Name:        "short_range_1h_service_level",
			Category:    "short_range",
			Description: "Last 1 hour, service + level compound filter",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=50",
					t, url.QueryEscape(`service.name:="api-gateway" AND level:="ERROR"`),
					oneHourAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 500.0,
		},

		// === MEDIUM RANGE QUERIES ===
		{
			Name:        "medium_range_6h_wildcard",
			Category:    "medium_range",
			Description: "Last 6 hours, wildcard, limit 200",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=200",
					t, sixHoursAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 1000.0,
		},
		{
			Name:        "medium_range_6h_substring",
			Category:    "medium_range",
			Description: "Last 6 hours, substring match in body",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=100",
					t, url.QueryEscape(`"database query"`),
					sixHoursAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 2000.0,
		},

		// === LONG RANGE QUERIES ===
		{
			Name:        "long_range_24h_wildcard",
			Category:    "long_range",
			Description: "Last 24 hours, wildcard, limit 500",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=*&start=%d&end=%d&limit=500",
					t, oneDayAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 3000.0,
		},
		{
			Name:        "long_range_48h_service_filter",
			Category:    "long_range",
			Description: "Full 48 hours, exact service filter, limit 200",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=200",
					t, url.QueryEscape(`service.name:="payment-service"`),
					twoDaysAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 5000.0,
		},
		{
			Name:        "long_range_48h_all_errors",
			Category:    "long_range",
			Description: "Full 48 hours, all ERROR logs, limit 500",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/query?query=%s&start=%d&end=%d&limit=500",
					t, url.QueryEscape(`level:="ERROR"`),
					twoDaysAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 5000.0,
		},

		// === AGGREGATION QUERIES ===
		{
			Name:        "stats_1h_count",
			Category:    "aggregation",
			Description: "stats_query: count over last 1 hour",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query?query=*&start=%d&end=%d",
					t, oneHourAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 300.0,
		},
		{
			Name:        "stats_24h_count",
			Category:    "aggregation",
			Description: "stats_query: count over last 24 hours",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query?query=*&start=%d&end=%d",
					t, oneDayAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 2000.0,
		},
		{
			Name:        "stats_range_1h_step_5m",
			Category:    "aggregation",
			Description: "stats_query_range: 1h with 5m step (12 buckets)",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query_range?query=*&start=%d&end=%d&step=300s",
					t, oneHourAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 500.0,
		},
		{
			Name:        "stats_range_24h_step_1h",
			Category:    "aggregation",
			Description: "stats_query_range: 24h with 1h step (24 buckets)",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/stats_query_range?query=*&start=%d&end=%d&step=3600s",
					t, oneDayAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 3000.0,
		},

		// === METADATA / LABEL QUERIES ===
		{
			Name:        "field_names_cached",
			Category:    "metadata",
			Description: "List all field names (label index)",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/field_names?start=%d&end=%d",
					t, twoDaysAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 1.0,
		},
		{
			Name:        "field_values_service",
			Category:    "metadata",
			Description: "List values for service.name",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/field_values?field=service.name&limit=100&start=%d&end=%d",
					t, twoDaysAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 100.0,
		},
		{
			Name:        "streams_list",
			Category:    "metadata",
			Description: "List log streams",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/streams?start=%d&end=%d",
					t, oneHourAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 500.0,
		},

		// === HITS ENDPOINT (histogram) ===
		{
			Name:        "hits_1h_step_5m",
			Category:    "histogram",
			Description: "Hits histogram: 1h range, 5m buckets",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/hits?query=*&start=%d&end=%d&step=300s",
					t, oneHourAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 500.0,
		},
		{
			Name:        "hits_24h_step_1h",
			Category:    "histogram",
			Description: "Hits histogram: 24h range, 1h buckets",
			URLFn: func(t string) string {
				return fmt.Sprintf("%s/select/logsql/hits?query=*&start=%d&end=%d&step=3600s",
					t, oneDayAgo.UnixNano(), now.UnixNano())
			},
			TargetP95Ms: 3000.0,
		},
	}
}

func runRealisticBenchmarks(target string, iterations int, warmup int) []RealisticResult {
	scenarios := buildRealisticScenarios()
	results := make([]RealisticResult, 0, len(scenarios))

	fmt.Printf("\n=== Realistic Performance Benchmarks ===\n")
	fmt.Printf("Target: %s  Iterations: %d  Warmup: %d\n\n", target, iterations, warmup)

	for _, sc := range scenarios {
		result := runSingleRealistic(target, sc, iterations, warmup)
		results = append(results, result)

		pass := "PASS"
		if !result.Pass {
			pass = "FAIL"
		}
		fmt.Printf("  %-35s p50=%7.1fms p95=%7.1fms p99=%7.1fms target=%7.0fms %s\n",
			result.Name, result.P50Ms, result.P95Ms, result.P99Ms, result.TargetP95Ms, pass)
	}

	return results
}

func runSingleRealistic(target string, sc RealisticScenario, iterations, warmup int) RealisticResult {
	client := &http.Client{Timeout: 30 * time.Second}

	for i := 0; i < warmup; i++ {
		u := sc.URLFn(target)
		resp, err := client.Get(u)
		if err == nil {
			_, _ = io.Copy(io.Discard, resp.Body)
			_ = resp.Body.Close()
		}
	}

	var latencies []float64
	for i := 0; i < iterations; i++ {
		u := sc.URLFn(target)
		start := time.Now()
		resp, err := client.Get(u)
		elapsed := time.Since(start)
		if err != nil {
			continue
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode < 400 {
			latencies = append(latencies, float64(elapsed.Microseconds())/1000.0)
		}
	}

	if len(latencies) == 0 {
		return RealisticResult{
			Name:        sc.Name,
			Category:    sc.Category,
			Description: sc.Description,
			TargetP95Ms: sc.TargetP95Ms,
			Iterations:  0,
		}
	}

	sort.Float64s(latencies)
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	return RealisticResult{
		Name:        sc.Name,
		Category:    sc.Category,
		Description: sc.Description,
		P50Ms:       p50,
		P95Ms:       p95,
		P99Ms:       p99,
		MaxMs:       latencies[len(latencies)-1],
		MinMs:       latencies[0],
		TargetP95Ms: sc.TargetP95Ms,
		Pass:        p95 <= sc.TargetP95Ms,
		Iterations:  len(latencies),
	}
}

func printRealisticSummary(results []RealisticResult) {
	categories := map[string][]RealisticResult{}
	for _, r := range results {
		categories[r.Category] = append(categories[r.Category], r)
	}

	categoryOrder := []string{"fast_path", "point_lookup", "short_range", "medium_range", "long_range", "aggregation", "metadata", "histogram"}

	fmt.Println("\n=== Summary by Category ===")
	fmt.Printf("  %-15s %5s %5s %8s %8s %6s\n", "Category", "Tests", "Pass", "Avg p50", "Avg p95", "Result")
	fmt.Println("  " + strings.Repeat("-", 55))

	totalTests := 0
	totalPass := 0

	for _, cat := range categoryOrder {
		rs, ok := categories[cat]
		if !ok {
			continue
		}
		pass := 0
		var sumP50, sumP95 float64
		for _, r := range rs {
			if r.Pass {
				pass++
			}
			sumP50 += r.P50Ms
			sumP95 += r.P95Ms
		}
		n := len(rs)
		totalTests += n
		totalPass += pass

		result := "PASS"
		if pass < n {
			result = "FAIL"
		}
		fmt.Printf("  %-15s %5d %5d %7.1fms %7.1fms %6s\n",
			cat, n, pass, sumP50/float64(n), sumP95/float64(n), result)
	}

	fmt.Println("  " + strings.Repeat("-", 55))
	overall := "PASS"
	if totalPass < totalTests {
		overall = "FAIL"
	}
	fmt.Printf("  %-15s %5d %5d %8s %8s %6s\n", "TOTAL", totalTests, totalPass, "", "", overall)
}
