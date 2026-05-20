package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestConcurrentResult_JSON(t *testing.T) {
	r := &Report{
		Mode:     "concurrent",
		Duration: "60s",
		Target:   "http://localhost:9428",
		ConcurrentResults: []ConcurrentResult{
			{
				Concurrency:  1,
				Duration:     "30s",
				TotalQueries: 1000,
				Errors:       5,
				Rejected429:  0,
				P50Ms:        1.2,
				P95Ms:        2.5,
				P99Ms:        4.0,
				QPS:          33.3,
				ErrorRate:    0.5,
			},
			{
				Concurrency:  50,
				Duration:     "30s",
				TotalQueries: 5000,
				Errors:       20,
				Rejected429:  10,
				P50Ms:        2.0,
				P95Ms:        4.8,
				P99Ms:        8.0,
				QPS:          166.7,
				ErrorRate:    0.4,
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
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	results, ok := decoded["concurrent_results"]
	if !ok {
		t.Fatal("missing concurrent_results field in JSON")
	}
	arr, ok := results.([]interface{})
	if !ok || len(arr) != 2 {
		t.Fatalf("expected 2 concurrent_results, got %v", results)
	}
	if !strings.Contains(string(data), `"concurrency"`) {
		t.Fatal("missing concurrency field in JSON output")
	}
}

func TestMixedRWResult_JSON(t *testing.T) {
	r := &Report{
		Mode:     "mixed",
		Duration: "120s",
		Target:   "http://localhost:9428",
		MixedRWResults: &MixedRWResult{
			Duration:             "120s",
			InsertBaselineRPS:    1000.0,
			QueryBaselineP95Ms:   2.5,
			MixedInsertRPS:       850.0,
			MixedQueryP95Ms:      3.1,
			InsertDegradationPct: 15.0,
			QueryDegradationPct:  24.0,
			Pass:                 false,
		},
		Pass: false,
	}
	data, err := r.JSON()
	if err != nil {
		t.Fatal(err)
	}
	if len(data) == 0 {
		t.Fatal("empty JSON output")
	}
	var decoded map[string]interface{}
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("invalid JSON: %v", err)
	}
	mrw, ok := decoded["mixed_rw_results"]
	if !ok {
		t.Fatal("missing mixed_rw_results field in JSON")
	}
	obj, ok := mrw.(map[string]interface{})
	if !ok {
		t.Fatal("mixed_rw_results is not an object")
	}
	if _, ok := obj["insert_degradation_pct"]; !ok {
		t.Fatal("missing insert_degradation_pct field")
	}
	if _, ok := obj["query_degradation_pct"]; !ok {
		t.Fatal("missing query_degradation_pct field")
	}
}

func TestConcurrentResult_PassCheck(t *testing.T) {
	// Pass case: C=50 p95 (4.8) <= 2x C=1 p95 (2.5) => 4.8 <= 5.0
	rPass := &Report{
		ConcurrentResults: []ConcurrentResult{
			{Concurrency: 1, P95Ms: 2.5},
			{Concurrency: 10, P95Ms: 3.0},
			{Concurrency: 50, P95Ms: 4.8},
		},
	}
	rPass.ComputePass()
	if !rPass.Pass {
		t.Fatal("expected pass when C=50 p95 (4.8) <= 2x C=1 p95 (5.0)")
	}

	// Fail case: C=50 p95 (5.2) > 2x C=1 p95 (2.5) => 5.2 > 5.0
	rFail := &Report{
		ConcurrentResults: []ConcurrentResult{
			{Concurrency: 1, P95Ms: 2.5},
			{Concurrency: 10, P95Ms: 3.0},
			{Concurrency: 50, P95Ms: 5.2},
		},
	}
	rFail.ComputePass()
	if rFail.Pass {
		t.Fatal("expected fail when C=50 p95 (5.2) > 2x C=1 p95 (5.0)")
	}
}

func TestMixedRWResult_PassCheck(t *testing.T) {
	// Pass case: both degradations < 20%
	rPass := &Report{
		MixedRWResults: &MixedRWResult{
			InsertDegradationPct: 10.0,
			QueryDegradationPct:  15.0,
		},
	}
	rPass.ComputePass()
	if !rPass.Pass {
		t.Fatal("expected pass when both degradations < 20%")
	}
	if !rPass.MixedRWResults.Pass {
		t.Fatal("expected MixedRWResults.Pass = true when both degradations < 20%")
	}

	// Fail case: query degradation >= 20%
	rFailQuery := &Report{
		MixedRWResults: &MixedRWResult{
			InsertDegradationPct: 10.0,
			QueryDegradationPct:  20.0,
		},
	}
	rFailQuery.ComputePass()
	if rFailQuery.Pass {
		t.Fatal("expected fail when query degradation >= 20%")
	}
	if rFailQuery.MixedRWResults.Pass {
		t.Fatal("expected MixedRWResults.Pass = false when query degradation >= 20%")
	}

	// Fail case: insert degradation >= 20%
	rFailInsert := &Report{
		MixedRWResults: &MixedRWResult{
			InsertDegradationPct: 25.0,
			QueryDegradationPct:  10.0,
		},
	}
	rFailInsert.ComputePass()
	if rFailInsert.Pass {
		t.Fatal("expected fail when insert degradation >= 20%")
	}
	if rFailInsert.MixedRWResults.Pass {
		t.Fatal("expected MixedRWResults.Pass = false when insert degradation >= 20%")
	}
}

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
