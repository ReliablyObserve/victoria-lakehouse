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

func TestBuildInsertBatch(t *testing.T) {
	batch := buildInsertBatch(10)
	if len(batch) == 0 {
		t.Fatal("buildInsertBatch returned empty slice")
	}
	// Each line is JSON and ends with newline; count newlines to verify row count.
	newlines := 0
	for _, b := range batch {
		if b == '\n' {
			newlines++
		}
	}
	if newlines != 10 {
		t.Errorf("expected 10 newlines in batch, got %d", newlines)
	}
}
