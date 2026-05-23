package parquets3

import "testing"

func TestCanSkipRowGroupByServiceStats(t *testing.T) {
	// Row group with service.name min="alpha", max="gamma"
	// Query for service.name="zeta" — can skip (zeta > gamma)
	if !canSkipByColumnStats("service.name", "zeta", "alpha", "gamma") {
		t.Fatal("expected skip: zeta outside [alpha, gamma]")
	}

	// Query for service.name="beta" — cannot skip (beta inside [alpha, gamma])
	if canSkipByColumnStats("service.name", "beta", "alpha", "gamma") {
		t.Fatal("expected no skip: beta inside [alpha, gamma]")
	}

	// Query for service.name="alpha" — cannot skip (exact boundary match)
	if canSkipByColumnStats("service.name", "alpha", "alpha", "gamma") {
		t.Fatal("expected no skip: alpha at min boundary")
	}
}

func TestCanSkipRowGroupByServiceStats_EmptyRange(t *testing.T) {
	// Empty min/max — cannot skip (no stats available)
	if canSkipByColumnStats("service.name", "anything", "", "") {
		t.Fatal("expected no skip: empty stats")
	}
}

func TestCanSkipRowGroupByServiceStats_MaxBoundary(t *testing.T) {
	// Value at max boundary — cannot skip
	if canSkipByColumnStats("service.name", "gamma", "alpha", "gamma") {
		t.Fatal("expected no skip: gamma at max boundary")
	}
}

func TestCanSkipRowGroupByServiceStats_BelowMin(t *testing.T) {
	// Value below min — can skip
	if !canSkipByColumnStats("service.name", "aaa", "alpha", "gamma") {
		t.Fatal("expected skip: aaa below min alpha")
	}
}
