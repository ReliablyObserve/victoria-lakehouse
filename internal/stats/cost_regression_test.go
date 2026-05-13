package stats

import (
	"math"
	"testing"
)

func TestRegressionCostCalculatorBasicPricing(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{
		"STANDARD":     0.023,
		"STANDARD_IA":  0.0125,
		"GLACIER":      0.004,
		"DEEP_ARCHIVE": 0.00099,
	}, nil)

	// 100 GiB in STANDARD = 100 * 0.023 = $2.30
	cost := cc.MonthlyStorageCost("STANDARD", 100<<30)
	expected := 100.0 * 0.023
	if math.Abs(cost-expected) > 0.01 {
		t.Errorf("STANDARD cost = %f, want ~%f", cost, expected)
	}

	// 100 GiB in GLACIER = 100 * 0.004 = $0.40
	cost = cc.MonthlyStorageCost("GLACIER", 100<<30)
	expected = 100.0 * 0.004
	if math.Abs(cost-expected) > 0.01 {
		t.Errorf("GLACIER cost = %f, want ~%f", cost, expected)
	}
}

func TestRegressionCostCalculatorZeroBytes(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	cost := cc.MonthlyStorageCost("STANDARD", 0)
	if cost != 0 {
		t.Errorf("0 bytes should cost $0, got %f", cost)
	}
}

func TestRegressionCostCalculatorNilMaps(t *testing.T) {
	cc := NewCostCalculator(nil, nil)
	cost := cc.MonthlyStorageCost("STANDARD", 100<<30)
	if cost != 0 {
		t.Errorf("nil price map should return $0, got %f", cost)
	}
}

func TestRegressionCostCalculatorLifecycleSavings(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{
		"STANDARD":    0.023,
		"STANDARD_IA": 0.0125,
		"GLACIER":     0.004,
	}, nil)

	byClass := map[string]int64{
		"STANDARD":    50 << 30,
		"STANDARD_IA": 30 << 30,
		"GLACIER":     20 << 30,
	}

	savings := cc.LifecycleSavings(byClass)
	if savings <= 0 {
		t.Errorf("lifecycle savings should be positive, got %f", savings)
	}

	// Savings = allStandard - actual
	totalGB := float64(100)
	allStandard := totalGB * 0.023
	actual := 50.0*0.023 + 30.0*0.0125 + 20.0*0.004
	expectedSavings := allStandard - actual
	if math.Abs(savings-expectedSavings) > 0.01 {
		t.Errorf("savings = %f, want ~%f", savings, expectedSavings)
	}
}

func TestRegressionCostCalculatorLifecycleSavingsNeverNegative(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	// All data in standard — no savings
	byClass := map[string]int64{"STANDARD": 100 << 30}
	savings := cc.LifecycleSavings(byClass)
	if savings != 0 {
		t.Errorf("no lifecycle = no savings, got %f", savings)
	}
}

func TestRegressionCostCalculatorRequestCost(t *testing.T) {
	cc := NewCostCalculator(nil, map[string]float64{
		"GET":    0.0004,
		"PUT":    0.005,
		"DELETE": 0.0,
	})

	getCost := cc.RequestCost("GET", 10000)
	expected := 0.0004 * 10000.0 / 1000.0
	if math.Abs(getCost-expected) > 0.0001 {
		t.Errorf("GET cost = %f, want %f", getCost, expected)
	}

	deleteCost := cc.RequestCost("DELETE", 10000)
	if deleteCost != 0 {
		t.Errorf("DELETE should be free, got %f", deleteCost)
	}

	unknownCost := cc.RequestCost("UNKNOWN_OP", 10000)
	if unknownCost != 0 {
		t.Errorf("unknown op should cost $0, got %f", unknownCost)
	}
}

func TestRegressionCostCalculatorProjectCost30d(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)

	daily := make([]int64, 7)
	for i := range daily {
		daily[i] = 10 << 30 // 10 GiB/day
	}

	cost := cc.ProjectCost30d(daily, "STANDARD")
	if cost <= 0 {
		t.Errorf("projected 30d cost should be > 0, got %f", cost)
	}
}

func TestRegressionCostCalculatorProjectCost30dEmptySlice(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)
	cost := cc.ProjectCost30d(nil, "STANDARD")
	if cost != 0 {
		t.Errorf("empty daily data should project $0, got %f", cost)
	}
}

func TestRegressionCostCalculatorStoragePricesCopy(t *testing.T) {
	original := map[string]float64{"STANDARD": 0.023}
	cc := NewCostCalculator(original, nil)
	copy := cc.StoragePrices()

	// Modify the copy — should not affect internal state
	copy["STANDARD"] = 999.0

	cost := cc.MonthlyStorageCost("STANDARD", 1<<30)
	expected := 0.023
	if math.Abs(cost-expected) > 0.001 {
		t.Errorf("modifying StoragePrices copy affected calculator: cost=%f, want ~%f", cost, expected)
	}
}

func TestRegressionCostPerTenantMatchesTotalMonthlyCost(t *testing.T) {
	cc := NewCostCalculator(map[string]float64{
		"STANDARD":    0.023,
		"STANDARD_IA": 0.0125,
	}, nil)

	byClass := map[string]int64{
		"STANDARD":    50 << 30,
		"STANDARD_IA": 20 << 30,
	}

	perTenant := cc.CostPerTenant(byClass)
	total := cc.TotalMonthlyCost(byClass)

	if perTenant != total {
		t.Errorf("CostPerTenant should equal TotalMonthlyCost: %f vs %f", perTenant, total)
	}
}
