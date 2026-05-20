package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strings"
)

type LatencyResult struct {
	P50Ms       float64 `json:"p50_ms"`
	P95Ms       float64 `json:"p95_ms"`
	P99Ms       float64 `json:"p99_ms"`
	TargetP95Ms float64 `json:"target_p95_ms"`
	Pass        bool    `json:"pass"`
	Iterations  int     `json:"iterations"`
}

type ThroughputResult struct {
	MaxRate        float64 `json:"max_rate"`
	ConcurrencyMax int     `json:"concurrency_at_max"`
	Unit           string  `json:"unit"`
}

type BenchmarkResult struct {
	FileSize       string  `json:"file_size"`
	RowGroupSize   int     `json:"row_group_size"`
	CompressionLvl int     `json:"compression_level"`
	WriteTimeMs    float64 `json:"write_time_ms"`
	ReadTimeMs     float64 `json:"read_time_ms"`
	FileSizeBytes  int64   `json:"file_size_bytes"`
	RawSizeBytes   int64   `json:"raw_size_bytes"`
	Ratio          float64 `json:"compression_ratio"`
	RowCount       int     `json:"row_count"`
}

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

type Report struct {
	Mode              string                       `json:"mode"`
	Duration          string                       `json:"duration"`
	Target            string                       `json:"target"`
	LatencyBenchmarks map[string]*LatencyResult    `json:"latency_benchmarks,omitempty"`
	ThroughputTests   map[string]*ThroughputResult `json:"throughput_tests,omitempty"`
	Benchmarks        []BenchmarkResult            `json:"benchmarks,omitempty"`
	RealisticResults  []RealisticResult            `json:"realistic_results,omitempty"`
	VerifyResults     *VerifyResult                `json:"verify_results,omitempty"`
	ConcurrentResults []ConcurrentResult           `json:"concurrent_results,omitempty"`
	MixedRWResults    *MixedRWResult               `json:"mixed_rw_results,omitempty"`
	Pass              bool                         `json:"pass"`
}

func (r *Report) ComputePass() {
	r.Pass = true
	for _, lr := range r.LatencyBenchmarks {
		lr.Pass = lr.P95Ms <= lr.TargetP95Ms
		if !lr.Pass {
			r.Pass = false
		}
	}
	for i := range r.RealisticResults {
		r.RealisticResults[i].Pass = r.RealisticResults[i].P95Ms <= r.RealisticResults[i].TargetP95Ms
		if !r.RealisticResults[i].Pass {
			r.Pass = false
		}
	}
	if r.VerifyResults != nil && !r.VerifyResults.Pass {
		r.Pass = false
	}
	// Concurrent: p95 at C>=50 must be <= 2x p95 at lowest concurrency (baseline).
	if len(r.ConcurrentResults) > 0 {
		baselineP95 := r.ConcurrentResults[0].P95Ms
		for _, cr := range r.ConcurrentResults {
			if cr.Concurrency >= 50 && cr.P95Ms > 2*baselineP95 {
				r.Pass = false
				break
			}
		}
	}
	// Mixed R/W: both InsertDegradationPct and QueryDegradationPct must be < 20.
	if r.MixedRWResults != nil {
		mwr := r.MixedRWResults
		mwr.Pass = mwr.InsertDegradationPct < 20 && mwr.QueryDegradationPct < 20
		if !mwr.Pass {
			r.Pass = false
		}
	}
}

func (r *Report) JSON() ([]byte, error) {
	return json.MarshalIndent(r, "", "  ")
}

func (r *Report) PrintSummary() {
	fmt.Println("\n=== Load Test Results ===")
	fmt.Printf("Target: %s  Mode: %s  Duration: %s\n\n", r.Target, r.Mode, r.Duration)

	if len(r.LatencyBenchmarks) > 0 {
		fmt.Println("Latency Benchmarks:")
		fmt.Printf("  %-30s %8s %8s %8s %10s %6s\n", "Test", "p50", "p95", "p99", "target", "pass")
		fmt.Println("  " + strings.Repeat("-", 78))
		for name, lr := range r.LatencyBenchmarks {
			pass := "PASS"
			if !lr.Pass {
				pass = "FAIL"
			}
			fmt.Printf("  %-30s %7.1fms %7.1fms %7.1fms %9.1fms %6s\n",
				name, lr.P50Ms, lr.P95Ms, lr.P99Ms, lr.TargetP95Ms, pass)
		}
	}

	if len(r.ThroughputTests) > 0 {
		fmt.Println("\nThroughput Tests:")
		for name, tr := range r.ThroughputTests {
			fmt.Printf("  %-30s max=%.0f %s @ concurrency=%d\n",
				name, tr.MaxRate, tr.Unit, tr.ConcurrencyMax)
		}
	}

	if len(r.Benchmarks) > 0 {
		fmt.Println("\nBenchmark Results:")
		fmt.Printf("  %-8s %8s %6s %12s %12s %8s %8s\n", "Size", "RowGroup", "ZSTD", "FileBytes", "RawBytes", "Ratio", "WriteMs")
		fmt.Println("  " + strings.Repeat("-", 72))
		for _, b := range r.Benchmarks {
			fmt.Printf("  %-8s %8d %6d %12d %12d %7.2fx %7.1fms\n",
				b.FileSize, b.RowGroupSize, b.CompressionLvl, b.FileSizeBytes, b.RawSizeBytes, b.Ratio, b.WriteTimeMs)
		}
	}

	if len(r.ConcurrentResults) > 0 {
		fmt.Println("\nConcurrency Stress Results:")
		fmt.Printf("  %-12s %12s %12s %8s %8s %8s %10s %8s\n",
			"Concurrency", "TotalQueries", "Errors", "p50", "p95", "p99", "QPS", "ErrRate%")
		fmt.Println("  " + strings.Repeat("-", 90))
		for _, cr := range r.ConcurrentResults {
			fmt.Printf("  %-12d %12d %12d %7.1fms %7.1fms %7.1fms %10.1f %7.2f%%\n",
				cr.Concurrency, cr.TotalQueries, cr.Errors,
				cr.P50Ms, cr.P95Ms, cr.P99Ms, cr.QPS, cr.ErrorRate)
		}
	}

	if r.MixedRWResults != nil {
		mwr := r.MixedRWResults
		pass := "PASS"
		if !mwr.Pass {
			pass = "FAIL"
		}
		fmt.Println("\nMixed R/W Results:")
		fmt.Printf("  Insert baseline RPS:   %.1f\n", mwr.InsertBaselineRPS)
		fmt.Printf("  Query baseline p95:    %.1fms\n", mwr.QueryBaselineP95Ms)
		fmt.Printf("  Mixed insert RPS:      %.1f  (degradation: %.1f%%)\n", mwr.MixedInsertRPS, mwr.InsertDegradationPct)
		fmt.Printf("  Mixed query p95:       %.1fms  (degradation: %.1f%%)\n", mwr.MixedQueryP95Ms, mwr.QueryDegradationPct)
		fmt.Printf("  Result: %s\n", pass)
	}

	if r.Pass {
		fmt.Println("\nOverall: PASS")
	} else {
		fmt.Println("\nOverall: FAIL")
	}
}

func (r *Report) WriteToFile(path string) error {
	data, err := r.JSON()
	if err != nil {
		return err
	}
	return os.WriteFile(path, data, 0o600)
}
