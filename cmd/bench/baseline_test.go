package main

import (
	"encoding/json"
	"testing"
	"time"
)

func TestBaseline_JSON_RoundTrip(t *testing.T) {
	b := Baseline{
		Timestamp: time.Date(2026, 5, 20, 12, 0, 0, 0, time.UTC),
		GitSHA:    "abc1234",
		Tier:      "small",
		Signal:    "logs",
		FileCount: 500,
		Write: map[string]WriteResult{
			"jsonline_1000": {RowsPerSec: 45000, P50Ms: 12, P95Ms: 28, FlushMs: 340, CompressionRatio: 7.2},
		},
		Read: []ReadResult{
			{Endpoint: "/select/logsql/hits", Filter: "*", ColdMs: 4850, WarmMs: 1200, HotMs: 890},
		},
	}
	data, err := json.MarshalIndent(b, "", "  ")
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var decoded Baseline
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if decoded.Tier != "small" {
		t.Errorf("tier: got %q", decoded.Tier)
	}
	if decoded.Write["jsonline_1000"].RowsPerSec != 45000 {
		t.Errorf("write rows/sec: got %v", decoded.Write["jsonline_1000"].RowsPerSec)
	}
	if decoded.Read[0].ColdMs != 4850 {
		t.Errorf("cold_ms: got %v", decoded.Read[0].ColdMs)
	}
}

func TestBaseline_FilePath(t *testing.T) {
	path := baselineFilePath("benchmarks", "logs", "small")
	if path != "benchmarks/baseline-logs-small.json" {
		t.Errorf("got %q", path)
	}
}
