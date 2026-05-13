package stats

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// defaultTestRules provides a standard three-tier lifecycle:
// 30d → STANDARD_IA, 90d → GLACIER, 365d → DEEP_ARCHIVE
var defaultTestRules = []config.LifecycleRuleConfig{
	{TransitionDays: 30, StorageClass: "STANDARD_IA"},
	{TransitionDays: 90, StorageClass: "GLACIER"},
	{TransitionDays: 365, StorageClass: "DEEP_ARCHIVE"},
}

func daysAgo(now time.Time, days int) time.Time {
	return now.AddDate(0, 0, -days)
}

func TestLifecyclePrediction(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	sct := NewStorageClassTracker(defaultTestRules, nil)

	tests := []struct {
		name     string
		ageDays  int
		wantClass string
	}{
		{"fresh_1d", 1, "STANDARD"},
		{"before_first_29d", 29, "STANDARD"},
		{"at_first_30d", 30, "STANDARD_IA"},
		{"between_first_second_89d", 89, "STANDARD_IA"},
		{"at_second_90d", 90, "GLACIER"},
		{"between_second_third_364d", 364, "GLACIER"},
		{"at_third_365d", 365, "DEEP_ARCHIVE"},
		{"well_past_730d", 730, "DEEP_ARCHIVE"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			createdAt := daysAgo(now, tc.ageDays)
			got := sct.PredictClass(createdAt, now)
			if got != tc.wantClass {
				t.Errorf("PredictClass(age=%dd) = %q, want %q", tc.ageDays, got, tc.wantClass)
			}
		})
	}
}

func TestLifecyclePredictionNoRules(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	sct := NewStorageClassTracker(nil, nil)

	for _, days := range []int{0, 1, 30, 90, 365, 1000} {
		got := sct.PredictClass(daysAgo(now, days), now)
		if got != "STANDARD" {
			t.Errorf("PredictClass(age=%dd, no rules) = %q, want STANDARD", days, got)
		}
	}
}

func TestNearTransitionBoundary(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	sct := NewStorageClassTracker(defaultTestRules, nil)

	tests := []struct {
		name    string
		ageDays int
		want    bool
	}{
		{"28d_near_30d_boundary", 28, true},   // diff=2, within window
		{"29d_near_30d_boundary", 29, true},   // diff=1, within window
		{"10d_not_near", 10, false},           // diff=20 to nearest
		{"32d_past_30d_boundary", 32, false},  // past the 30d boundary
		{"30d_exactly_at", 30, false},         // at boundary, diff=0, not "before"
		{"88d_near_90d", 88, true},            // diff=2
		{"363d_near_365d", 363, true},         // diff=2
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			got := sct.NearBoundary(daysAgo(now, tc.ageDays), now)
			if got != tc.want {
				t.Errorf("NearBoundary(age=%dd) = %v, want %v", tc.ageDays, got, tc.want)
			}
		})
	}
}

func TestPerTenantRuleOverride(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	tenantRules := map[string][]config.LifecycleRuleConfig{
		"premium": {
			{TransitionDays: 60, StorageClass: "STANDARD_IA"},
			{TransitionDays: 180, StorageClass: "GLACIER"},
		},
	}
	sct := NewStorageClassTracker(defaultTestRules, tenantRules)

	// At 45 days: default=STANDARD_IA (30d rule), premium=STANDARD (60d rule not hit)
	createdAt := daysAgo(now, 45)
	defaultClass := sct.PredictClass(createdAt, now)
	tenantClass := sct.PredictClassForTenant(createdAt, now, "premium")

	if defaultClass != "STANDARD_IA" {
		t.Errorf("default at 45d = %q, want STANDARD_IA", defaultClass)
	}
	if tenantClass != "STANDARD" {
		t.Errorf("premium tenant at 45d = %q, want STANDARD", tenantClass)
	}

	// At 60 days: default=STANDARD_IA, premium=STANDARD_IA
	createdAt60 := daysAgo(now, 60)
	if got := sct.PredictClassForTenant(createdAt60, now, "premium"); got != "STANDARD_IA" {
		t.Errorf("premium tenant at 60d = %q, want STANDARD_IA", got)
	}

	// At 180 days: default=GLACIER (90d rule), premium=GLACIER (180d rule)
	createdAt180 := daysAgo(now, 180)
	if got := sct.PredictClassForTenant(createdAt180, now, "premium"); got != "GLACIER" {
		t.Errorf("premium tenant at 180d = %q, want GLACIER", got)
	}
}

func TestPerTenantFallsBackToDefault(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	tenantRules := map[string][]config.LifecycleRuleConfig{
		"premium": {
			{TransitionDays: 60, StorageClass: "STANDARD_IA"},
		},
	}
	sct := NewStorageClassTracker(defaultTestRules, tenantRules)

	// "basic" tenant has no override, should use default rules
	createdAt := daysAgo(now, 45)
	got := sct.PredictClassForTenant(createdAt, now, "basic")
	if got != "STANDARD_IA" {
		t.Errorf("basic tenant (no override) at 45d = %q, want STANDARD_IA", got)
	}
}

func TestNearBoundaryForTenant(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)

	tenantRules := map[string][]config.LifecycleRuleConfig{
		"premium": {
			{TransitionDays: 60, StorageClass: "STANDARD_IA"},
		},
	}
	sct := NewStorageClassTracker(defaultTestRules, tenantRules)

	// 28d: near default 30d boundary, but NOT near premium 60d boundary
	if got := sct.NearBoundaryForTenant(daysAgo(now, 28), now, "premium"); got != false {
		t.Errorf("premium at 28d near boundary = %v, want false (60d rule, diff=32)", got)
	}
	// 58d: near premium 60d boundary
	if got := sct.NearBoundaryForTenant(daysAgo(now, 58), now, "premium"); got != true {
		t.Errorf("premium at 58d near boundary = %v, want true (60d rule, diff=2)", got)
	}
	// "basic" tenant falls back to defaults: 28d near 30d boundary
	if got := sct.NearBoundaryForTenant(daysAgo(now, 28), now, "basic"); got != true {
		t.Errorf("basic at 28d near boundary = %v, want true (fallback, 30d rule)", got)
	}
}

func TestLifecyclePredictionSingleRule(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 90, StorageClass: "GLACIER"},
	}
	sct := NewStorageClassTracker(rules, nil)

	if got := sct.PredictClass(daysAgo(now, 50), now); got != "STANDARD" {
		t.Errorf("single rule, 50d = %q, want STANDARD", got)
	}
	if got := sct.PredictClass(daysAgo(now, 90), now); got != "GLACIER" {
		t.Errorf("single rule, 90d = %q, want GLACIER", got)
	}
	if got := sct.PredictClass(daysAgo(now, 200), now); got != "GLACIER" {
		t.Errorf("single rule, 200d = %q, want GLACIER", got)
	}
}

func TestLifecyclePredictionExactBoundary(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	sct := NewStorageClassTracker(defaultTestRules, nil)

	// Exactly at each boundary should match the rule
	if got := sct.PredictClass(daysAgo(now, 30), now); got != "STANDARD_IA" {
		t.Errorf("exact 30d = %q, want STANDARD_IA", got)
	}
	if got := sct.PredictClass(daysAgo(now, 90), now); got != "GLACIER" {
		t.Errorf("exact 90d = %q, want GLACIER", got)
	}
	if got := sct.PredictClass(daysAgo(now, 365), now); got != "DEEP_ARCHIVE" {
		t.Errorf("exact 365d = %q, want DEEP_ARCHIVE", got)
	}
}

func TestLifecyclePredictionZeroDays(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 0, StorageClass: "REDUCED_REDUNDANCY"},
		{TransitionDays: 30, StorageClass: "GLACIER"},
	}
	sct := NewStorageClassTracker(rules, nil)

	// Even a brand-new object (0 days old) matches the 0-day rule
	if got := sct.PredictClass(now, now); got != "REDUCED_REDUNDANCY" {
		t.Errorf("zero-day rule, age=0 = %q, want REDUCED_REDUNDANCY", got)
	}
	// 1 day old also matches 0-day rule (ageDays=1 >= 0)
	if got := sct.PredictClass(daysAgo(now, 1), now); got != "REDUCED_REDUNDANCY" {
		t.Errorf("zero-day rule, age=1 = %q, want REDUCED_REDUNDANCY", got)
	}
	// 30+ days matches the 30-day rule (sorted desc: 30 checked first)
	if got := sct.PredictClass(daysAgo(now, 30), now); got != "GLACIER" {
		t.Errorf("zero-day rule, age=30 = %q, want GLACIER", got)
	}
}

func TestNearBoundaryNoRules(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	sct := NewStorageClassTracker(nil, nil)

	for _, days := range []int{0, 1, 28, 29, 30, 88, 90, 365} {
		if got := sct.NearBoundary(daysAgo(now, days), now); got {
			t.Errorf("NearBoundary(age=%dd, no rules) = true, want false", days)
		}
	}
}

func TestNearBoundaryMultipleRules(t *testing.T) {
	now := time.Date(2026, 1, 15, 12, 0, 0, 0, time.UTC)
	sct := NewStorageClassTracker(defaultTestRules, nil)

	// 88d: near the 90d boundary (diff=2), but NOT near 30d (diff=-58) or 365d (diff=277)
	if got := sct.NearBoundary(daysAgo(now, 88), now); got != true {
		t.Errorf("88d near 90d boundary = %v, want true", got)
	}

	// 35d: not near any boundary (30d past, 90d far away diff=55)
	if got := sct.NearBoundary(daysAgo(now, 35), now); got != false {
		t.Errorf("35d not near any boundary = %v, want false", got)
	}

	// 363d: near 365d boundary (diff=2), not near others
	if got := sct.NearBoundary(daysAgo(now, 363), now); got != true {
		t.Errorf("363d near 365d boundary = %v, want true", got)
	}
}

func TestDefaultRulesAccessor(t *testing.T) {
	sct := NewStorageClassTracker(defaultTestRules, nil)
	rules := sct.DefaultRules()

	// Should return correct count
	if len(rules) != 3 {
		t.Fatalf("DefaultRules() returned %d rules, want 3", len(rules))
	}

	// Should be sorted descending by TransitionDays
	if rules[0].TransitionDays != 365 || rules[1].TransitionDays != 90 || rules[2].TransitionDays != 30 {
		t.Errorf("DefaultRules() not sorted desc: %v", rules)
	}

	// Mutating the returned slice should not affect the tracker
	rules[0].StorageClass = "MUTATED"
	fresh := sct.DefaultRules()
	if fresh[0].StorageClass == "MUTATED" {
		t.Error("DefaultRules() returned a reference, not a copy")
	}
}
