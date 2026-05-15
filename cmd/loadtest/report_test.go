package main

import (
	"testing"
)

func TestReport_JSON(t *testing.T) {
	r := &Report{
		Mode:     "latency",
		Duration: "60s",
		Target:   "http://localhost:9428",
		LatencyBenchmarks: map[string]*LatencyResult{
			"manifest_fast_path": {
				P50Ms: 0.2, P95Ms: 0.8, P99Ms: 1.1,
				TargetP95Ms: 1.0, Pass: true,
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

func TestReport_PassCheck(t *testing.T) {
	r := &Report{
		LatencyBenchmarks: map[string]*LatencyResult{
			"a": {P95Ms: 0.8, TargetP95Ms: 1.0, Pass: true},
			"b": {P95Ms: 2.0, TargetP95Ms: 1.0, Pass: false},
		},
	}
	r.ComputePass()
	if r.Pass {
		t.Fatal("should fail when any benchmark fails")
	}
}
