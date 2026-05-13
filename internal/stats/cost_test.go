package stats

import (
	"math"
	"testing"
)

const (
	gb = 1 << 30 // 1 GiB in bytes
)

// Standard AWS S3 storage prices ($/GB/month) used across tests.
var testStoragePrices = map[string]float64{
	"STANDARD":     0.023,
	"STANDARD_IA":  0.0125,
	"GLACIER":      0.004,
	"DEEP_ARCHIVE": 0.00099,
}

// Standard AWS S3 request prices ($/1000 requests).
var testRequestPrices = map[string]float64{
	"PUT":  0.005,
	"GET":  0.0004,
	"LIST": 0.005,
}

func almostEqual(a, b float64) bool {
	return math.Abs(a-b) < 0.001
}

func TestCostCalculatorStorageCost(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	tests := []struct {
		name  string
		class string
		bytes int64
		want  float64
	}{
		{"STANDARD 1GB", "STANDARD", 1 * gb, 0.023},
		{"STANDARD_IA 10GB", "STANDARD_IA", 10 * gb, 0.125},
		{"GLACIER 100GB", "GLACIER", 100 * gb, 0.4},
		{"DEEP_ARCHIVE 1TB", "DEEP_ARCHIVE", 1024 * gb, 1.01376},
		{"unknown class", "UNKNOWN_CLASS", 50 * gb, 0},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cc.MonthlyStorageCost(tt.class, tt.bytes)
			if !almostEqual(got, tt.want) {
				t.Errorf("MonthlyStorageCost(%q, %d) = %f, want %f", tt.class, tt.bytes, got, tt.want)
			}
		})
	}
}

func TestCostCalculatorTotalCost(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	byClass := map[string]int64{
		"STANDARD":    10 * gb,
		"STANDARD_IA": 50 * gb,
		"GLACIER":     100 * gb,
	}
	got := cc.TotalMonthlyCost(byClass)
	// 10*0.023 + 50*0.0125 + 100*0.004 = 0.23 + 0.625 + 0.4 = 1.255
	want := 1.255
	if !almostEqual(got, want) {
		t.Errorf("TotalMonthlyCost = %f, want %f", got, want)
	}
}

func TestCostCalculatorRequestCost(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	tests := []struct {
		name      string
		operation string
		count     int64
		want      float64
	}{
		{"PUT 10000", "PUT", 10000, 0.05},
		{"GET 100000", "GET", 100000, 0.04},
		{"LIST 5000", "LIST", 5000, 0.025},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := cc.RequestCost(tt.operation, tt.count)
			if !almostEqual(got, tt.want) {
				t.Errorf("RequestCost(%q, %d) = %f, want %f", tt.operation, tt.count, got, tt.want)
			}
		})
	}
}

func TestCostCalculatorLifecycleSavings(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	byClass := map[string]int64{
		"STANDARD":     10 * gb,
		"STANDARD_IA":  50 * gb,
		"GLACIER":      100 * gb,
		"DEEP_ARCHIVE": 200 * gb,
	}
	// All as STANDARD: 360 * 0.023 = 8.28
	// Actual: 10*0.023 + 50*0.0125 + 100*0.004 + 200*0.00099
	//       = 0.23 + 0.625 + 0.4 + 0.198 = 1.453
	// Savings = 8.28 - 1.453 = 6.827
	got := cc.LifecycleSavings(byClass)
	want := 6.827
	if !almostEqual(got, want) {
		t.Errorf("LifecycleSavings = %f, want %f", got, want)
	}
}

func TestCostCalculatorLifecycleSavingsNoSavings(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	byClass := map[string]int64{
		"STANDARD": 100 * gb,
	}
	// All STANDARD: same cost either way → savings = 0
	got := cc.LifecycleSavings(byClass)
	if !almostEqual(got, 0) {
		t.Errorf("LifecycleSavings (all STANDARD) = %f, want 0", got)
	}
}

func TestCostCalculatorProjectCost30d(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	// 7 days of 10GB/day
	dailyBytes := make([]int64, 7)
	for i := range dailyBytes {
		dailyBytes[i] = 10 * gb
	}
	got := cc.ProjectCost30d(dailyBytes, "STANDARD")
	// avg daily = 10 GB, avg storage = 10*30/2 = 150 GB, cost = 150 * 0.023 = 3.45
	want := 3.45
	if !almostEqual(got, want) {
		t.Errorf("ProjectCost30d = %f, want %f", got, want)
	}
}

func TestCostCalculatorProjectCost30dEmpty(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	got := cc.ProjectCost30d(nil, "STANDARD")
	if got != 0 {
		t.Errorf("ProjectCost30d(empty) = %f, want 0", got)
	}

	got = cc.ProjectCost30d([]int64{}, "STANDARD")
	if got != 0 {
		t.Errorf("ProjectCost30d(empty slice) = %f, want 0", got)
	}
}

func TestCostCalculatorZeroBytes(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	got := cc.MonthlyStorageCost("STANDARD", 0)
	if got != 0 {
		t.Errorf("MonthlyStorageCost(0 bytes) = %f, want 0", got)
	}

	got = cc.TotalMonthlyCost(map[string]int64{"STANDARD": 0, "GLACIER": 0})
	if got != 0 {
		t.Errorf("TotalMonthlyCost(all zero) = %f, want 0", got)
	}
}

func TestCostCalculatorNilPrices(t *testing.T) {
	cc := NewCostCalculator(nil, nil)

	// Must not panic
	got := cc.MonthlyStorageCost("STANDARD", 10*gb)
	if got != 0 {
		t.Errorf("MonthlyStorageCost(nil prices) = %f, want 0", got)
	}

	got = cc.RequestCost("PUT", 1000)
	if got != 0 {
		t.Errorf("RequestCost(nil prices) = %f, want 0", got)
	}

	got = cc.TotalMonthlyCost(map[string]int64{"STANDARD": 10 * gb})
	if got != 0 {
		t.Errorf("TotalMonthlyCost(nil prices) = %f, want 0", got)
	}

	got = cc.LifecycleSavings(map[string]int64{"STANDARD": 10 * gb})
	if got != 0 {
		t.Errorf("LifecycleSavings(nil prices) = %f, want 0", got)
	}

	prices := cc.StoragePrices()
	if prices == nil || len(prices) != 0 {
		t.Errorf("StoragePrices(nil init) should return empty map, got %v", prices)
	}
}

func TestCostCalculatorRequestCostUnknownOp(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	got := cc.RequestCost("DELETE", 1000)
	if got != 0 {
		t.Errorf("RequestCost(unknown op) = %f, want 0", got)
	}
}

func TestCostCalculatorCostPerTenant(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	tenantData := map[string]int64{
		"STANDARD":    5 * gb,
		"STANDARD_IA": 20 * gb,
	}
	got := cc.CostPerTenant(tenantData)
	// 5*0.023 + 20*0.0125 = 0.115 + 0.25 = 0.365
	want := 0.365
	if !almostEqual(got, want) {
		t.Errorf("CostPerTenant = %f, want %f", got, want)
	}
}

func TestCostCalculatorStoragePricesCopy(t *testing.T) {
	cc := NewCostCalculator(testStoragePrices, testRequestPrices)

	prices := cc.StoragePrices()

	// Verify it contains expected values
	if !almostEqual(prices["STANDARD"], 0.023) {
		t.Errorf("StoragePrices[STANDARD] = %f, want 0.023", prices["STANDARD"])
	}

	// Mutate the returned map and verify the original is unchanged
	prices["STANDARD"] = 999.0
	original := cc.StoragePrices()
	if !almostEqual(original["STANDARD"], 0.023) {
		t.Errorf("StoragePrices mutation leaked: got %f, want 0.023", original["STANDARD"])
	}
}
