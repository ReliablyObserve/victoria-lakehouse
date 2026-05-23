# Phase 3: Concurrency Stress Testing — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Validate that Lakehouse query latency targets hold under concurrent load, mixed read/write workloads, and at production-near scale, with documented configuration recommendations.

**Architecture:** Extends the existing `cmd/loadtest/` CLI with two new modes (`concurrent` and `mixed-rw`) that measure latency degradation under parallel load. A config-sweep shell script automates testing different `MaxConcurrent` and `FileWorkers` values by restarting the server between runs. Results are reported as JSON with pass/fail gates from the spec: ≤2x degradation at 50 concurrent queries, ≤20% mutual interference for mixed R/W.

**Tech Stack:** Go 1.24, `net/http`, `sync/atomic`, existing `cmd/loadtest/` report infrastructure, shell script for config sweep

**Existing infrastructure to reuse:**
- `cmd/loadtest/realistic.go` — `buildRealisticScenarios()` returns 27 query scenarios with `URLFn` callbacks
- `cmd/loadtest/latency.go` — `percentile()` helper for sorted float slices
- `cmd/loadtest/report.go` — `Report`, `LatencyResult`, `ThroughputResult` types with JSON + printing
- `cmd/loadtest/throughput.go` — `measureInsertRate()` and `measureQueryQPS()` patterns
- `internal/selectapi/handler.go:83-89` — semaphore with HTTP 429 on overflow, metric `QueryRejectedTotal`
- `internal/config/config.go:259` — `QueryConfig{MaxConcurrent: 32, FileWorkers: 8}`

**IMPORTANT:** All Go commands MUST use `GOWORK=off` environment variable.

---

### Task 1: Report Types for Concurrent and Mixed R/W Results

**Files:**
- Modify: `cmd/loadtest/report.go`
- Modify: `cmd/loadtest/report_test.go`

- [ ] **Step 1: Write failing tests for ConcurrentResult and MixedRWResult serialization**

Add to `cmd/loadtest/report_test.go`:

```go
func TestConcurrentResult_JSON(t *testing.T) {
	r := &Report{
		Mode:     "concurrent",
		Duration: "30s",
		Target:   "http://localhost:9428",
		ConcurrentResults: []ConcurrentResult{
			{
				Concurrency:  10,
				Duration:     "30s",
				TotalQueries: 500,
				Errors:       2,
				Rejected429:  0,
				P50Ms:        12.5,
				P95Ms:        45.0,
				P99Ms:        120.0,
				QPS:          16.7,
				ErrorRate:    0.004,
			},
		},
		Pass: true,
	}
	data, err := r.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("empty JSON output")
	}
}

func TestMixedRWResult_JSON(t *testing.T) {
	r := &Report{
		Mode:     "mixed-rw",
		Duration: "60s",
		Target:   "http://localhost:9428",
		MixedRWResults: &MixedRWResult{
			Duration:             "60s",
			InsertBaselineRPS:    5000.0,
			QueryBaselineP95Ms:   45.0,
			MixedInsertRPS:       4200.0,
			MixedQueryP95Ms:      52.0,
			InsertDegradationPct: 16.0,
			QueryDegradationPct:  15.6,
			Pass:                 true,
		},
		Pass: true,
	}
	data, err := r.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("empty JSON output")
	}
}

func TestConcurrentResult_PassCheck(t *testing.T) {
	r := &Report{
		ConcurrentResults: []ConcurrentResult{
			{Concurrency: 1, P95Ms: 50.0},
			{Concurrency: 50, P95Ms: 90.0},
		},
	}
	r.ComputePass()
	if !r.Pass {
		t.Fatal("should pass: 90ms at C=50 is < 2x of 50ms at C=1")
	}

	r2 := &Report{
		ConcurrentResults: []ConcurrentResult{
			{Concurrency: 1, P95Ms: 50.0},
			{Concurrency: 50, P95Ms: 110.0},
		},
	}
	r2.ComputePass()
	if r2.Pass {
		t.Fatal("should fail: 110ms at C=50 is > 2x of 50ms at C=1")
	}
}

func TestMixedRWResult_PassCheck(t *testing.T) {
	r := &Report{
		MixedRWResults: &MixedRWResult{
			InsertDegradationPct: 15.0,
			QueryDegradationPct:  18.0,
		},
	}
	r.ComputePass()
	if !r.Pass {
		t.Fatal("should pass: both degradations < 20%")
	}

	r2 := &Report{
		MixedRWResults: &MixedRWResult{
			InsertDegradationPct: 25.0,
			QueryDegradationPct:  10.0,
		},
	}
	r2.ComputePass()
	if r2.Pass {
		t.Fatal("should fail: insert degradation > 20%")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd cmd/loadtest && GOWORK=off go test -run "TestConcurrentResult|TestMixedRWResult" -v`
Expected: FAIL — `ConcurrentResult` and `MixedRWResult` types not defined

- [ ] **Step 3: Add ConcurrentResult and MixedRWResult types to report.go**

Add the following types and update `Report`, `ComputePass`, and `PrintSummary` in `cmd/loadtest/report.go`:

```go
type ConcurrentResult struct {
	Concurrency  int     `json:"concurrency"`
	Duration     string  `json:"duration"`
	TotalQueries int64   `json:"total_queries"`
	Errors       int64   `json:"errors"`
	Rejected429  int64   `json:"rejected_429"`
	P50Ms        float64 `json:"p50_ms"`
	P95Ms        float64 `json:"p95_ms"`
	P99Ms        float64 `json:"p99_ms"`
	QPS          float64 `json:"qps"`
	ErrorRate    float64 `json:"error_rate"`
}

type MixedRWResult struct {
	Duration             string  `json:"duration"`
	InsertBaselineRPS    float64 `json:"insert_baseline_rps"`
	QueryBaselineP95Ms   float64 `json:"query_baseline_p95_ms"`
	MixedInsertRPS       float64 `json:"mixed_insert_rps"`
	MixedQueryP95Ms      float64 `json:"mixed_query_p95_ms"`
	InsertDegradationPct float64 `json:"insert_degradation_pct"`
	QueryDegradationPct  float64 `json:"query_degradation_pct"`
	Pass                 bool    `json:"pass"`
}
```

Add to `Report` struct:
```go
ConcurrentResults []ConcurrentResult `json:"concurrent_results,omitempty"`
MixedRWResults    *MixedRWResult     `json:"mixed_rw_results,omitempty"`
```

Update `ComputePass()` — add after the existing realistic results check:
```go
// Concurrent: p95 at C=50 must be ≤ 2x p95 at C=1 (or lowest concurrency)
if len(r.ConcurrentResults) > 0 {
	baseP95 := r.ConcurrentResults[0].P95Ms
	for i := range r.ConcurrentResults {
		if r.ConcurrentResults[i].Concurrency <= 1 {
			baseP95 = r.ConcurrentResults[i].P95Ms
		}
	}
	for i := range r.ConcurrentResults {
		if r.ConcurrentResults[i].Concurrency >= 50 && baseP95 > 0 {
			if r.ConcurrentResults[i].P95Ms > baseP95*2 {
				r.Pass = false
			}
		}
	}
}

// Mixed R/W: both degradations must be < 20%
if r.MixedRWResults != nil {
	r.MixedRWResults.Pass = r.MixedRWResults.InsertDegradationPct < 20 &&
		r.MixedRWResults.QueryDegradationPct < 20
	if !r.MixedRWResults.Pass {
		r.Pass = false
	}
}
```

Update `PrintSummary()` — add after the existing throughput section:
```go
if len(r.ConcurrentResults) > 0 {
	fmt.Println("\nConcurrent Query Results:")
	fmt.Printf("  %-12s %8s %8s %8s %8s %8s %8s %6s\n",
		"Concurrency", "Queries", "Errors", "429s", "p50", "p95", "QPS", "ErrRt")
	fmt.Println("  " + strings.Repeat("-", 72))
	for _, cr := range r.ConcurrentResults {
		fmt.Printf("  %-12d %8d %8d %8d %7.1fms %7.1fms %7.1f %5.1f%%\n",
			cr.Concurrency, cr.TotalQueries, cr.Errors, cr.Rejected429,
			cr.P50Ms, cr.P95Ms, cr.QPS, cr.ErrorRate*100)
	}
}

if r.MixedRWResults != nil {
	m := r.MixedRWResults
	pass := "PASS"
	if !m.Pass {
		pass = "FAIL"
	}
	fmt.Println("\nMixed Read/Write Results:")
	fmt.Printf("  Insert baseline: %.0f rows/s → under load: %.0f rows/s (%.1f%% degradation)\n",
		m.InsertBaselineRPS, m.MixedInsertRPS, m.InsertDegradationPct)
	fmt.Printf("  Query baseline:  %.1fms p95  → under load: %.1fms p95  (%.1f%% degradation)\n",
		m.QueryBaselineP95Ms, m.MixedQueryP95Ms, m.QueryDegradationPct)
	fmt.Printf("  Target: <20%% mutual interference  Result: %s\n", pass)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd cmd/loadtest && GOWORK=off go test -run "TestConcurrentResult|TestMixedRWResult" -v`
Expected: PASS (all 4 tests)

- [ ] **Step 5: Commit**

```bash
git add cmd/loadtest/report.go cmd/loadtest/report_test.go
git commit -m "feat(loadtest): add ConcurrentResult and MixedRWResult report types"
```

---

### Task 2: Concurrent Query Benchmark

**Files:**
- Create: `cmd/loadtest/concurrent.go`
- Create: `cmd/loadtest/concurrent_test.go`

- [ ] **Step 1: Write failing tests for concurrent benchmark logic**

Create `cmd/loadtest/concurrent_test.go`:

```go
package main

import (
	"testing"
)

func TestBuildQueryMix(t *testing.T) {
	mix := buildQueryMix("http://localhost:9428")
	if len(mix) == 0 {
		t.Fatal("expected non-empty query mix")
	}
	for i, u := range mix {
		if u == "" {
			t.Fatalf("empty URL at index %d", i)
		}
	}
}

func TestCollectLatencies_Percentiles(t *testing.T) {
	latencies := make([]float64, 100)
	for i := range latencies {
		latencies[i] = float64(i + 1)
	}
	p50 := percentile(latencies, 0.50)
	p95 := percentile(latencies, 0.95)
	p99 := percentile(latencies, 0.99)

	if p50 < 49 || p50 > 51 {
		t.Fatalf("p50 should be ~50, got %.1f", p50)
	}
	if p95 < 94 || p95 > 96 {
		t.Fatalf("p95 should be ~95, got %.1f", p95)
	}
	if p99 < 98 || p99 > 100 {
		t.Fatalf("p99 should be ~99, got %.1f", p99)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd cmd/loadtest && GOWORK=off go test -run "TestBuildQueryMix|TestCollectLatencies" -v`
Expected: FAIL — `buildQueryMix` not defined

- [ ] **Step 3: Implement concurrent query benchmark**

Create `cmd/loadtest/concurrent.go`:

```go
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

func buildQueryMix(target string) []string {
	scenarios := buildRealisticScenarios()
	urls := make([]string, 0, len(scenarios))
	for _, sc := range scenarios {
		urls = append(urls, sc.URLFn(target))
	}
	return urls
}

func runConcurrentBenchmark(target string, durationStr string, concurrencyLevels []int) []ConcurrentResult {
	dur, _ := time.ParseDuration(durationStr)
	if dur == 0 {
		dur = 30 * time.Second
	}

	queryURLs := buildQueryMix(target)
	if len(queryURLs) == 0 {
		fmt.Println("WARNING: no query scenarios available")
		return nil
	}

	results := make([]ConcurrentResult, 0, len(concurrencyLevels))

	fmt.Printf("\n=== Concurrent Query Benchmark ===\n")
	fmt.Printf("Target: %s  Duration per level: %s  Query mix: %d scenarios\n\n", target, dur, len(queryURLs))

	for _, conc := range concurrencyLevels {
		result := runAtConcurrency(target, queryURLs, conc, dur)
		results = append(results, result)

		fmt.Printf("  C=%-4d queries=%-6d errors=%-4d 429s=%-4d p50=%7.1fms p95=%7.1fms qps=%7.1f err=%.2f%%\n",
			result.Concurrency, result.TotalQueries, result.Errors, result.Rejected429,
			result.P50Ms, result.P95Ms, result.QPS, result.ErrorRate*100)
	}

	return results
}

func runAtConcurrency(target string, queryURLs []string, concurrency int, dur time.Duration) ConcurrentResult {
	var totalQueries atomic.Int64
	var errors atomic.Int64
	var rejected429 atomic.Int64

	var mu sync.Mutex
	var allLatencies []float64

	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			client := &http.Client{Timeout: 30 * time.Second}
			rng := rand.New(rand.NewSource(int64(workerID) + time.Now().UnixNano()))
			var localLatencies []float64

			for time.Now().Before(deadline) {
				u := queryURLs[rng.Intn(len(queryURLs))]
				start := time.Now()
				resp, err := client.Get(u)
				elapsed := float64(time.Since(start).Microseconds()) / 1000.0

				if err != nil {
					errors.Add(1)
					totalQueries.Add(1)
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()

				totalQueries.Add(1)
				if resp.StatusCode == 429 {
					rejected429.Add(1)
				} else if resp.StatusCode >= 400 {
					errors.Add(1)
				} else {
					localLatencies = append(localLatencies, elapsed)
				}
			}

			mu.Lock()
			allLatencies = append(allLatencies, localLatencies...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	total := totalQueries.Load()
	errs := errors.Load()
	r429 := rejected429.Load()

	var p50, p95, p99 float64
	if len(allLatencies) > 0 {
		sort.Float64s(allLatencies)
		p50 = percentile(allLatencies, 0.50)
		p95 = percentile(allLatencies, 0.95)
		p99 = percentile(allLatencies, 0.99)
	}

	var errorRate float64
	if total > 0 {
		errorRate = float64(errs+r429) / float64(total)
	}

	return ConcurrentResult{
		Concurrency:  concurrency,
		Duration:     dur.String(),
		TotalQueries: total,
		Errors:       errs,
		Rejected429:  r429,
		P50Ms:        p50,
		P95Ms:        p95,
		P99Ms:        p99,
		QPS:          float64(total) / dur.Seconds(),
		ErrorRate:    errorRate,
	}
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd cmd/loadtest && GOWORK=off go test -run "TestBuildQueryMix|TestCollectLatencies" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/loadtest/concurrent.go cmd/loadtest/concurrent_test.go
git commit -m "feat(loadtest): add concurrent query benchmark mode"
```

---

### Task 3: Mixed Read/Write with Degradation Measurement

**Files:**
- Create: `cmd/loadtest/mixed_rw.go`
- Create: `cmd/loadtest/mixed_rw_test.go`

- [ ] **Step 1: Write failing tests for degradation calculation**

Create `cmd/loadtest/mixed_rw_test.go`:

```go
package main

import (
	"math"
	"testing"
)

func TestCalcDegradation(t *testing.T) {
	tests := []struct {
		name     string
		baseline float64
		actual   float64
		wantPct  float64
	}{
		{"no degradation", 100.0, 100.0, 0.0},
		{"20% throughput drop", 1000.0, 800.0, 20.0},
		{"50% latency increase", 50.0, 75.0, 50.0},
		{"zero baseline", 0.0, 100.0, 0.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calcDegradation(tt.baseline, tt.actual)
			if math.Abs(got-tt.wantPct) > 0.1 {
				t.Errorf("calcDegradation(%v, %v) = %v, want %v", tt.baseline, tt.actual, got, tt.wantPct)
			}
		})
	}
}

func TestCalcLatencyDegradation(t *testing.T) {
	tests := []struct {
		name     string
		baseline float64
		actual   float64
		wantPct  float64
	}{
		{"no degradation", 50.0, 50.0, 0.0},
		{"doubled latency", 50.0, 100.0, 100.0},
		{"20% slower", 100.0, 120.0, 20.0},
		{"zero baseline", 0.0, 50.0, 0.0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := calcLatencyDegradation(tt.baseline, tt.actual)
			if math.Abs(got-tt.wantPct) > 0.1 {
				t.Errorf("calcLatencyDegradation(%v, %v) = %v, want %v", tt.baseline, tt.actual, got, tt.wantPct)
			}
		})
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `cd cmd/loadtest && GOWORK=off go test -run "TestCalcDegradation|TestCalcLatencyDegradation" -v`
Expected: FAIL — `calcDegradation` and `calcLatencyDegradation` not defined

- [ ] **Step 3: Implement mixed R/W benchmark**

Create `cmd/loadtest/mixed_rw.go`:

```go
package main

import (
	"bytes"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

func calcDegradation(baseline, actual float64) float64 {
	if baseline <= 0 {
		return 0
	}
	return ((baseline - actual) / baseline) * 100
}

func calcLatencyDegradation(baseline, actual float64) float64 {
	if baseline <= 0 {
		return 0
	}
	return ((actual - baseline) / baseline) * 100
}

func runMixedRWBenchmark(target string, durationStr string) *MixedRWResult {
	dur, _ := time.ParseDuration(durationStr)
	if dur == 0 {
		dur = 60 * time.Second
	}

	phaseDur := dur / 3

	fmt.Printf("\n=== Mixed Read/Write Benchmark ===\n")
	fmt.Printf("Target: %s  Phase duration: %s\n\n", target, phaseDur)

	fmt.Printf("  Phase 1: Insert-only baseline (%s)...\n", phaseDur)
	insertBaseline := measureInsertRPS(target, 8, phaseDur)
	fmt.Printf("    Insert baseline: %.0f rows/s\n", insertBaseline)

	fmt.Printf("  Phase 2: Query-only baseline (%s)...\n", phaseDur)
	queryBaseline := measureQueryP95(target, 8, phaseDur)
	fmt.Printf("    Query baseline: %.1fms p95\n", queryBaseline)

	fmt.Printf("  Phase 3: Mixed workload (%s)...\n", phaseDur)
	mixedInsert, mixedQueryP95 := measureMixedRW(target, 8, 8, phaseDur)
	fmt.Printf("    Mixed insert: %.0f rows/s, query: %.1fms p95\n", mixedInsert, mixedQueryP95)

	insertDeg := calcDegradation(insertBaseline, mixedInsert)
	queryDeg := calcLatencyDegradation(queryBaseline, mixedQueryP95)

	if insertDeg < 0 {
		insertDeg = 0
	}
	if queryDeg < 0 {
		queryDeg = 0
	}

	result := &MixedRWResult{
		Duration:             dur.String(),
		InsertBaselineRPS:    insertBaseline,
		QueryBaselineP95Ms:   queryBaseline,
		MixedInsertRPS:       mixedInsert,
		MixedQueryP95Ms:      mixedQueryP95,
		InsertDegradationPct: insertDeg,
		QueryDegradationPct:  queryDeg,
	}

	fmt.Printf("\n  Insert degradation: %.1f%%  Query degradation: %.1f%%\n", insertDeg, queryDeg)

	return result
}

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
	return float64(totalRows.Load()) / dur.Seconds()
}

func measureQueryP95(target string, concurrency int, dur time.Duration) float64 {
	queryURLs := buildQueryMix(target)
	if len(queryURLs) == 0 {
		return 0
	}

	var mu sync.Mutex
	var allLatencies []float64
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := &http.Client{Timeout: 30 * time.Second}
			rng := rand.New(rand.NewSource(int64(id) + time.Now().UnixNano()))
			var local []float64

			for time.Now().Before(deadline) {
				u := queryURLs[rng.Intn(len(queryURLs))]
				start := time.Now()
				resp, err := client.Get(u)
				elapsed := float64(time.Since(start).Microseconds()) / 1000.0
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 400 {
					local = append(local, elapsed)
				}
			}

			mu.Lock()
			allLatencies = append(allLatencies, local...)
			mu.Unlock()
		}(i)
	}
	wg.Wait()

	if len(allLatencies) == 0 {
		return 0
	}
	sort.Float64s(allLatencies)
	return percentile(allLatencies, 0.95)
}

func measureMixedRW(target string, insertConc, queryConc int, dur time.Duration) (insertRPS float64, queryP95 float64) {
	var totalRows atomic.Int64
	var mu sync.Mutex
	var allLatencies []float64
	var wg sync.WaitGroup
	deadline := time.Now().Add(dur)

	batch := buildInsertBatch(100)

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

	queryURLs := buildQueryMix(target)
	for i := 0; i < queryConc; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			client := &http.Client{Timeout: 30 * time.Second}
			rng := rand.New(rand.NewSource(int64(id) + time.Now().UnixNano()))
			var local []float64

			for time.Now().Before(deadline) {
				if len(queryURLs) == 0 {
					return
				}
				u := queryURLs[rng.Intn(len(queryURLs))]
				start := time.Now()
				resp, err := client.Get(u)
				elapsed := float64(time.Since(start).Microseconds()) / 1000.0
				if err != nil {
					continue
				}
				_, _ = io.Copy(io.Discard, resp.Body)
				_ = resp.Body.Close()
				if resp.StatusCode < 400 {
					local = append(local, elapsed)
				}
			}

			mu.Lock()
			allLatencies = append(allLatencies, local...)
			mu.Unlock()
		}(i)
	}

	wg.Wait()

	insertRPS = float64(totalRows.Load()) / dur.Seconds()
	if len(allLatencies) > 0 {
		sort.Float64s(allLatencies)
		queryP95 = percentile(allLatencies, 0.95)
	}
	return
}

func buildInsertBatch(n int) []byte {
	var buf bytes.Buffer
	for i := 0; i < n; i++ {
		fmt.Fprintf(&buf,
			`{"_time":"%s","_msg":"mixed rw test row %d","service.name":"loadtest","level":"info"}`+"\n",
			time.Now().Format(time.RFC3339Nano), i)
	}
	return buf.Bytes()
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `cd cmd/loadtest && GOWORK=off go test -run "TestCalcDegradation|TestCalcLatencyDegradation" -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/loadtest/mixed_rw.go cmd/loadtest/mixed_rw_test.go
git commit -m "feat(loadtest): add mixed read/write benchmark with degradation measurement"
```

---

### Task 4: Wire New Modes into main.go and Add CLI Flags

**Files:**
- Modify: `cmd/loadtest/main.go`

- [ ] **Step 1: Verify current main.go compiles**

Run: `cd cmd/loadtest && GOWORK=off go build .`
Expected: Build succeeds

- [ ] **Step 2: Add concurrency flag and new mode cases**

Update `cmd/loadtest/main.go` — add the `concurrency` flag after existing flags:

```go
concurrency := flag.String("concurrency", "1,10,50,100", "Comma-separated concurrency levels for concurrent mode")
```

Add new cases in the switch block, after the existing `"mixed"` case:

```go
case "concurrent":
	levels := parseConcurrencyLevels(*concurrency)
	report.ConcurrentResults = runConcurrentBenchmark(*target, *duration, levels)
case "mixed-rw":
	report.MixedRWResults = runMixedRWBenchmark(*target, *duration)
```

Update the `"all"` case to also run concurrent and mixed-rw:

```go
case "all":
	report.LatencyBenchmarks = runLatencyBenchmarks(*target, *iterations)
	report.ThroughputTests = runThroughputTests(*target, *duration)
	levels := parseConcurrencyLevels(*concurrency)
	report.ConcurrentResults = runConcurrentBenchmark(*target, *duration, levels)
	report.MixedRWResults = runMixedRWBenchmark(*target, *duration)
```

Update the help text for mode flag:

```go
mode := flag.String("mode", "all", "Test mode: latency, throughput, mixed, mixed-rw, concurrent, all, realistic, benchmark, verify, e2e, compare")
```

Add `parseConcurrencyLevels` helper at the bottom of `main.go`:

```go
func parseConcurrencyLevels(s string) []int {
	parts := strings.Split(s, ",")
	var levels []int
	for _, p := range parts {
		p = strings.TrimSpace(p)
		if p == "" {
			continue
		}
		n, err := strconv.Atoi(p)
		if err != nil || n <= 0 {
			continue
		}
		levels = append(levels, n)
	}
	if len(levels) == 0 {
		return []int{1, 10, 50, 100}
	}
	return levels
}
```

Add imports: `"strconv"` and `"strings"`.

- [ ] **Step 3: Verify build succeeds**

Run: `cd cmd/loadtest && GOWORK=off go build .`
Expected: Build succeeds

- [ ] **Step 4: Run all existing tests to verify no regressions**

Run: `cd cmd/loadtest && GOWORK=off go test ./... -v`
Expected: All tests PASS

- [ ] **Step 5: Commit**

```bash
git add cmd/loadtest/main.go
git commit -m "feat(loadtest): wire concurrent and mixed-rw modes into CLI"
```

---

### Task 5: Config Sweep Script

**Files:**
- Create: `scripts/config-sweep.sh`

- [ ] **Step 1: Create the config sweep automation script**

Create `scripts/config-sweep.sh`:

```bash
#!/usr/bin/env bash
set -euo pipefail

# Config sweep: test different MaxConcurrent and FileWorkers values.
# Requires: lakehouse binary, MinIO running, data already seeded.
#
# Usage:
#   ./scripts/config-sweep.sh http://localhost:9428 ./lakehouse-logs config.yaml results/
#
# The script:
# 1. Stops any running lakehouse
# 2. Generates a modified config with the sweep values
# 3. Starts lakehouse with that config
# 4. Runs the concurrent benchmark
# 5. Saves results
# 6. Repeats for each (MaxConcurrent, FileWorkers) pair

TARGET="${1:-http://localhost:9428}"
BINARY="${2:-./lakehouse-logs}"
BASE_CONFIG="${3:-config.yaml}"
RESULTS_DIR="${4:-results/config-sweep}"

LOADTEST_DURATION="${LOADTEST_DURATION:-30s}"
CONCURRENCY_LEVELS="${CONCURRENCY_LEVELS:-1,10,50,100}"

# (MaxConcurrent, FileWorkers) pairs to test
SWEEP_PAIRS=(
  "16,4"    # 0.5x defaults
  "32,8"    # 1x defaults (baseline)
  "64,16"   # 2x defaults
)

mkdir -p "$RESULTS_DIR"

echo "=== Config Sweep ==="
echo "Target: $TARGET"
echo "Binary: $BINARY"
echo "Base config: $BASE_CONFIG"
echo "Results: $RESULTS_DIR"
echo "Pairs: ${SWEEP_PAIRS[*]}"
echo ""

stop_lakehouse() {
  pkill -f "$BINARY" 2>/dev/null || true
  sleep 1
}

for pair in "${SWEEP_PAIRS[@]}"; do
  IFS=',' read -r max_conc file_workers <<< "$pair"
  label="mc${max_conc}_fw${file_workers}"
  echo "--- Testing MaxConcurrent=$max_conc FileWorkers=$file_workers ---"

  stop_lakehouse

  # Generate modified config
  sweep_config="${RESULTS_DIR}/config_${label}.yaml"
  sed -e "s/max_concurrent:.*/max_concurrent: ${max_conc}/" \
      -e "s/file_workers:.*/file_workers: ${file_workers}/" \
      "$BASE_CONFIG" > "$sweep_config"

  # Start lakehouse with modified config
  "$BINARY" -config="$sweep_config" &
  LAKEHOUSE_PID=$!
  echo "  Started lakehouse (PID=$LAKEHOUSE_PID)"

  # Wait for health
  for i in $(seq 1 30); do
    if curl -sf "${TARGET}/health" > /dev/null 2>&1; then
      break
    fi
    sleep 1
  done

  if ! curl -sf "${TARGET}/health" > /dev/null 2>&1; then
    echo "  ERROR: lakehouse failed to start"
    stop_lakehouse
    continue
  fi

  # Run concurrent benchmark
  output="${RESULTS_DIR}/result_${label}.json"
  GOWORK=off go run ./cmd/loadtest \
    -mode=concurrent \
    -target="$TARGET" \
    -duration="$LOADTEST_DURATION" \
    -concurrency="$CONCURRENCY_LEVELS" \
    -output="$output" || true

  echo "  Results: $output"
  stop_lakehouse
  echo ""
done

# Print comparison summary
echo "=== Comparison Summary ==="
printf "%-20s %8s %8s %8s %8s\n" "Config" "C=1 p95" "C=50 p95" "C=100 p95" "QPS@100"
echo "$(printf '%0.s-' {1..60})"

for pair in "${SWEEP_PAIRS[@]}"; do
  IFS=',' read -r max_conc file_workers <<< "$pair"
  label="mc${max_conc}_fw${file_workers}"
  result="${RESULTS_DIR}/result_${label}.json"

  if [ ! -f "$result" ]; then
    printf "%-20s %8s %8s %8s %8s\n" "$label" "N/A" "N/A" "N/A" "N/A"
    continue
  fi

  c1_p95=$(python3 -c "
import json, sys
d = json.load(open('$result'))
for r in d.get('concurrent_results', []):
    if r['concurrency'] == 1: print(f\"{r['p95_ms']:.1f}\"); sys.exit()
print('N/A')
" 2>/dev/null || echo "N/A")

  c50_p95=$(python3 -c "
import json, sys
d = json.load(open('$result'))
for r in d.get('concurrent_results', []):
    if r['concurrency'] == 50: print(f\"{r['p95_ms']:.1f}\"); sys.exit()
print('N/A')
" 2>/dev/null || echo "N/A")

  c100_p95=$(python3 -c "
import json, sys
d = json.load(open('$result'))
for r in d.get('concurrent_results', []):
    if r['concurrency'] == 100: print(f\"{r['p95_ms']:.1f}\"); sys.exit()
print('N/A')
" 2>/dev/null || echo "N/A")

  qps100=$(python3 -c "
import json, sys
d = json.load(open('$result'))
for r in d.get('concurrent_results', []):
    if r['concurrency'] == 100: print(f\"{r['qps']:.1f}\"); sys.exit()
print('N/A')
" 2>/dev/null || echo "N/A")

  printf "%-20s %8s %8s %8s %8s\n" "$label" "$c1_p95" "$c50_p95" "$c100_p95" "$qps100"
done

echo ""
echo "Done. Detailed results in $RESULTS_DIR/"
```

- [ ] **Step 2: Make the script executable and verify syntax**

Run: `chmod +x scripts/config-sweep.sh && bash -n scripts/config-sweep.sh`
Expected: No syntax errors

- [ ] **Step 3: Commit**

```bash
git add scripts/config-sweep.sh
git commit -m "feat: add config sweep script for concurrency tuning validation"
```

---

### Task 6: Performance Documentation Update

**Files:**
- Modify: `docs/performance.md`

- [ ] **Step 1: Add concurrency stress testing section**

Add the following section to `docs/performance.md` after the existing "Manifest partition index" section (after line 142):

```markdown
### Concurrency stress testing

The `cmd/loadtest` CLI includes modes for validating performance under concurrent load:

```bash
# Concurrent queries at 10, 50, 100 parallel workers
go run ./cmd/loadtest -mode=concurrent -target=http://localhost:9428 \
  -concurrency=1,10,50,100 -duration=30s

# Mixed read/write: measures insert and query degradation under concurrent load
go run ./cmd/loadtest -mode=mixed-rw -target=http://localhost:9428 -duration=60s

# Full suite (includes latency, throughput, concurrent, and mixed R/W)
go run ./cmd/loadtest -mode=all -target=http://localhost:9428 -duration=60s
```

#### Pass criteria

| Test | Threshold | Rationale |
|---|---|---|
| Concurrent queries (C=50) | p95 ≤ 2x baseline (C=1) | Linear degradation acceptable; super-linear means contention |
| Mixed R/W interference | ≤ 20% degradation | Insert and query paths should be largely independent |

#### Configuration sweep

Use `scripts/config-sweep.sh` to test different `query.max_concurrent` and `query.file_workers` values:

```bash
./scripts/config-sweep.sh http://localhost:9428 ./lakehouse-logs config.yaml results/
```

The script tests 0.5x, 1x, and 2x of the default values and produces a comparison table.

#### Recommended settings by deployment size

| Deployment | Cores | `max_concurrent` | `file_workers` | Notes |
|---|---|---|---|---|
| Dev / CI | 2–4 | 8 | 4 | Low resource, prevent OOM |
| Small prod | 4–8 | 32 | 8 | Default — good balance |
| Medium prod | 8–16 | 64 | 16 | Higher parallelism for wide scans |
| Large prod | 16+ | 128 | 32 | Scale with available cores |

`file_workers` should generally be ≤ half the CPU cores. `max_concurrent` can be higher since queries often block on S3 I/O, not CPU.
```

- [ ] **Step 2: Verify docs render correctly**

Run: `head -200 docs/performance.md | tail -60`
Expected: New section visible and well-formatted

- [ ] **Step 3: Commit**

```bash
git add docs/performance.md
git commit -m "docs: add concurrency stress testing and config tuning section"
```

---

### Task 7: CHANGELOG Entry

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add Phase 3 entry under [Unreleased]**

Add the following under the `## [Unreleased]` section in `CHANGELOG.md`:

```markdown
### Performance
- Concurrent query benchmark: validates latency targets at 10/50/100 parallel queries with mixed endpoint types
- Mixed read/write benchmark: measures mutual interference with ≤20% degradation target
- Config sweep script for automated `max_concurrent` / `file_workers` tuning validation
- Deployment-size recommendations for query concurrency settings
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "docs: add Phase 3 concurrency stress testing changelog entry"
```

---

## Exit Criteria Mapping

| Spec requirement | Task | Verification |
|---|---|---|
| 10, 50, 100 parallel queries | Task 2 | `runConcurrentBenchmark` with configurable levels |
| Mix of endpoint types and filter patterns | Task 2 | `buildQueryMix` reuses 27 realistic scenarios |
| p50/p95/p99 latency, throughput, error rate | Task 1 + 2 | `ConcurrentResult` fields, `runAtConcurrency` collection |
| Concurrent ingest + query stream | Task 3 | `measureMixedRW` with insert + query goroutines |
| Write throughput degradation under query load | Task 3 | `InsertDegradationPct` from baseline comparison |
| Query latency degradation under write load | Task 3 | `QueryDegradationPct` from baseline comparison |
| Validate MaxConcurrent=32, FileWorkers=8 defaults | Task 5 | `config-sweep.sh` tests 0.5x, 1x, 2x values |
| Test with 2x and 0.5x values | Task 5 | Sweep pairs: (16,4), (32,8), (64,16) |
| Document recommended settings per deployment size | Task 6 | Deployment table in performance.md |
| Latency targets met at production-near scale | Task 2 | Run with medium/large tier data |
| Concurrency ≤ 2x degradation at C=50 | Task 1 | `ComputePass` checks ConcurrentResults |
| Mixed R/W < 20% mutual interference | Task 1 | `ComputePass` checks MixedRWResults |
| Configuration recommendations documented | Task 6 | Performance.md tuning section |
