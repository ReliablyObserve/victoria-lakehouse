package parquets3

import "testing"

func TestCanSkipRowGroupByServiceStats(t *testing.T) {
	// Row group with service.name min="alpha", max="gamma"
	// Query for service.name="zeta" — can skip (zeta > gamma)
	if !canSkipByColumnStats("zeta", "alpha", "gamma") {
		t.Fatal("expected skip: zeta outside [alpha, gamma]")
	}

	// Query for service.name="beta" — cannot skip (beta inside [alpha, gamma])
	if canSkipByColumnStats("beta", "alpha", "gamma") {
		t.Fatal("expected no skip: beta inside [alpha, gamma]")
	}

	// Query for service.name="alpha" — cannot skip (exact boundary match)
	if canSkipByColumnStats("alpha", "alpha", "gamma") {
		t.Fatal("expected no skip: alpha at min boundary")
	}
}

func TestCanSkipRowGroupByServiceStats_EmptyRange(t *testing.T) {
	if canSkipByColumnStats("anything", "", "") {
		t.Fatal("expected no skip: both stats empty")
	}
}

func TestCanSkipRowGroupByServiceStats_PartialEmpty(t *testing.T) {
	// Only minVal empty — cannot skip (incomplete stats)
	if canSkipByColumnStats("zeta", "", "gamma") {
		t.Fatal("expected no skip: minVal empty")
	}
	// Only maxVal empty — cannot skip (incomplete stats)
	if canSkipByColumnStats("aaa", "alpha", "") {
		t.Fatal("expected no skip: maxVal empty")
	}
}

func TestCanSkipRowGroupByServiceStats_MaxBoundary(t *testing.T) {
	if canSkipByColumnStats("gamma", "alpha", "gamma") {
		t.Fatal("expected no skip: gamma at max boundary")
	}
}

func TestCanSkipRowGroupByServiceStats_BelowMin(t *testing.T) {
	if !canSkipByColumnStats("aaa", "alpha", "gamma") {
		t.Fatal("expected skip: aaa below min alpha")
	}
}

func TestCanSkipRowGroupByServiceStats_SingleValueRange(t *testing.T) {
	// All rows have same service.name — min == max
	if canSkipByColumnStats("api", "api", "api") {
		t.Fatal("expected no skip: value equals single-value range")
	}
	if !canSkipByColumnStats("web", "api", "api") {
		t.Fatal("expected skip: web outside single-value range [api, api]")
	}
	if !canSkipByColumnStats("aaa", "api", "api") {
		t.Fatal("expected skip: aaa below single-value range [api, api]")
	}
}
