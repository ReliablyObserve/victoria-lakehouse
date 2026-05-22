package manifest

import "testing"

func TestFileInfo_ColumnStatsContains(t *testing.T) {
	fi := FileInfo{
		Key:  "test.parquet",
		Size: 1000,
		ColumnStats: map[string]ColumnMinMax{
			"service.name": {Min: "api-gateway", Max: "worker-service"},
			"level":        {Min: "DEBUG", Max: "WARN"},
		},
	}

	// Value in range
	if !fi.ColumnStatsContains("service.name", "order-service") {
		t.Error("order-service should be in range [api-gateway, worker-service]")
	}

	// Value below range
	if fi.ColumnStatsContains("service.name", "aaa-service") {
		t.Error("aaa-service should be below range")
	}

	// Value above range
	if fi.ColumnStatsContains("service.name", "zzz-service") {
		t.Error("zzz-service should be above range")
	}

	// Unknown column — no stats, assume match
	if !fi.ColumnStatsContains("unknown_col", "anything") {
		t.Error("unknown column should assume match")
	}

	// Nil ColumnStats — assume match
	fi2 := FileInfo{Key: "no-stats.parquet"}
	if !fi2.ColumnStatsContains("service.name", "api-gateway") {
		t.Error("nil ColumnStats should assume match")
	}
}

func TestFileInfo_ColumnStatsContains_BoundaryValues(t *testing.T) {
	fi := FileInfo{
		ColumnStats: map[string]ColumnMinMax{
			"service.name": {Min: "beta", Max: "delta"},
		},
	}

	// Exact min value — should match
	if !fi.ColumnStatsContains("service.name", "beta") {
		t.Error("exact min value should match")
	}
	// Exact max value — should match
	if !fi.ColumnStatsContains("service.name", "delta") {
		t.Error("exact max value should match")
	}
	// Just below min
	if fi.ColumnStatsContains("service.name", "alpha") {
		t.Error("value below min should not match")
	}
	// Just above max
	if fi.ColumnStatsContains("service.name", "epsilon") {
		t.Error("value above max should not match")
	}
}
