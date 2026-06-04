package tenant

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestPolicyRegistry_NumericKey_ResolvesImmediately(t *testing.T) {
	pr, err := NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Retention: config.TenantRetentionOverride{Keep: "7d"}},
	}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	eff := pr.For(1, 1)
	if eff == nil {
		t.Fatal("expected override for 1:1, got nil")
	}
	if eff.Retention != 7*24*time.Hour {
		t.Errorf("retention = %s, want 168h", eff.Retention)
	}
	if pr.For(2, 2) != nil {
		t.Error("expected nil for unknown tenant 2:2")
	}
}

func TestPolicyRegistry_AliasKey_ResolvesAtConstruction(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("acme-corp", TenantID{AccountID: 1002, ProjectID: 0})

	pr, err := NewPolicyRegistry(map[string]config.TenantOverride{
		"acme-corp": {
			Cardinality: config.TenantCardinalityOverride{MaxFields: 500000},
			Ingest:      config.TenantIngestOverride{MaxBytesPerSec: 10 * 1024 * 1024},
		},
	}, r)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if len(pr.PendingAliases()) != 0 {
		t.Errorf("pending = %v, want empty (alias already registered)", pr.PendingAliases())
	}
	eff := pr.For(1002, 0)
	if eff == nil {
		t.Fatal("expected override under resolved tenant 1002:0")
	}
	if eff.MaxFields != 500000 {
		t.Errorf("max_fields = %d, want 500000", eff.MaxFields)
	}
	if eff.MaxBytesPerSec != 10*1024*1024 {
		t.Errorf("max_bytes_per_sec = %d, want 10MiB", eff.MaxBytesPerSec)
	}
}

func TestPolicyRegistry_AliasKey_PendingThenRefreshed(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	pr, err := NewPolicyRegistry(map[string]config.TenantOverride{
		"late-team": {Retention: config.TenantRetentionOverride{Keep: "90d"}},
	}, r)
	if err != nil {
		t.Fatalf("new: %v", err)
	}
	if got := pr.PendingAliases(); len(got) != 1 || got[0] != "late-team" {
		t.Errorf("pending = %v, want [late-team]", got)
	}
	if pr.For(2002, 0) != nil {
		t.Error("expected nil before alias registers")
	}

	// Alias registers after config load — refresh should pick it up.
	_ = r.AddAlias("late-team", TenantID{AccountID: 2002, ProjectID: 0})
	pr.Refresh()

	if got := pr.PendingAliases(); len(got) != 0 {
		t.Errorf("pending still %v after refresh", got)
	}
	eff := pr.For(2002, 0)
	if eff == nil || eff.Retention != 90*24*time.Hour {
		t.Errorf("late-resolved override missing or wrong retention: %+v", eff)
	}
}

func TestPolicyRegistry_RejectsInvalidRetention(t *testing.T) {
	_, err := NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Retention: config.TenantRetentionOverride{Keep: "not-a-duration"}},
	}, nil)
	if err == nil {
		t.Fatal("expected validation error for bad retention duration")
	}
}

func TestPolicyRegistry_RejectsNegativeLimits(t *testing.T) {
	_, err := NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Cardinality: config.TenantCardinalityOverride{MaxFields: -1}},
	}, nil)
	if err == nil {
		t.Fatal("expected validation error for negative cardinality limit")
	}
	_, err = NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Ingest: config.TenantIngestOverride{MaxBytesPerSec: -1}},
	}, nil)
	if err == nil {
		t.Fatal("expected validation error for negative rate cap")
	}
}

func TestPolicyRegistry_NilSafeForEmptyOverrides(t *testing.T) {
	pr, err := NewPolicyRegistry(nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if pr.For(1, 1) != nil {
		t.Error("empty registry should return nil for any tenant")
	}
	// Nil receiver paths shouldn't panic.
	var nilPR *PolicyRegistry
	if nilPR.For(1, 1) != nil {
		t.Error("nil registry should return nil")
	}
	nilPR.Refresh()
	if nilPR.PendingAliases() != nil {
		t.Error("nil registry should report no pending aliases")
	}
}

func TestParseDayDuration(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		err  bool
	}{
		{"30d", 30 * 24 * time.Hour, false},
		{"1h", time.Hour, false},
		{"720h", 720 * time.Hour, false},
		{"", 0, true},
		{"7", 0, true},
		{"7days", 0, true},
	}
	for _, tc := range cases {
		got, err := ParseDayDuration(tc.in)
		if tc.err {
			if err == nil {
				t.Errorf("%q: expected error, got %s", tc.in, got)
			}
			continue
		}
		if err != nil {
			t.Errorf("%q: unexpected error: %v", tc.in, err)
		}
		if got != tc.want {
			t.Errorf("%q: got %s, want %s", tc.in, got, tc.want)
		}
	}
}
