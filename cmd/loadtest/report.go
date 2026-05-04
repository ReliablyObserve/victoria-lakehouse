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

type Report struct {
	Mode              string                       `json:"mode"`
	Duration          string                       `json:"duration"`
	Target            string                       `json:"target"`
	LatencyBenchmarks map[string]*LatencyResult    `json:"latency_benchmarks,omitempty"`
	ThroughputTests   map[string]*ThroughputResult `json:"throughput_tests,omitempty"`
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
	return os.WriteFile(path, data, 0o644)
}
