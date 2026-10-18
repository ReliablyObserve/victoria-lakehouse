package main

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/retention"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// The read path's retention cut and the eviction loop are built from one
// retentionConfig (global default and rules plus the tenant overrides), so they
// judge every file the same way. This pins that equivalence: for each kind of
// file the policy says "expired" exactly when the eviction manager would delete.
func TestRetentionPolicy_AgreesWithTheEvictionManager(t *testing.T) {
	cfg := &config.Config{}
	cfg.Retention.Enabled = true
	cfg.Retention.Default = "30d"
	cfg.Retention.CheckInterval = "1h"
	cfg.Retention.Rules = []config.RetentionRule{{Match: map[string]string{"service.name": "audit"}, Keep: "365d"}}
	reg, err := tenant.NewPolicyRegistry(map[string]config.TenantOverride{
		"1001:0": {Retention: config.TenantRetentionOverride{Keep: "7d"}},
		"2002:7": {Retention: config.TenantRetentionOverride{Keep: "90d"}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}

	pol, err := retentionPolicy(cfg, reg)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := retention.New(retentionConfig(cfg, reg), nil, nil, "b", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !pol.Enabled() {
		t.Fatal("retention is enabled but the policy is not")
	}

	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	for _, c := range []struct {
		name   string
		labels map[string][]string
	}{
		{"default tenant", map[string][]string{"account_id": {"0"}, "project_id": {"0"}}},
		{"7d tenant", map[string][]string{"account_id": {"1001"}, "project_id": {"0"}}},
		{"90d tenant", map[string][]string{"account_id": {"2002"}, "project_id": {"7"}}},
		{"rule on a label", map[string][]string{"account_id": {"0"}, "project_id": {"0"}, "service.name": {"audit"}}},
		{"no labels", nil},
	} {
		for _, days := range []int{0, 1, 6, 7, 8, 29, 30, 31, 89, 91, 364, 366} {
			fi := manifest.FileInfo{Labels: c.labels, MaxTimeNs: now.Add(-time.Duration(days)*24*time.Hour - time.Minute).UnixNano()}
			ttl := mgr.ResolveTTL(fi)
			evicts := now.Sub(time.Unix(0, fi.MaxTimeNs)) > ttl
			if got := pol.Expired(fi, now); got != evicts {
				t.Errorf("%s at %d days: the read cut says expired=%v, eviction (ttl %v) says %v", c.name, days, got, ttl, evicts)
			}
		}
	}

	// Retention off: no cut at all.
	cfg.Retention.Enabled = false
	off, err := retentionPolicy(cfg, reg)
	if err != nil {
		t.Fatal(err)
	}
	if off.Enabled() || off.Expired(manifest.FileInfo{MaxTimeNs: 1}, now) {
		t.Fatal("a disabled retention config must not cut anything")
	}
}

// An invalid retention config is refused (the binary exits on it).
func TestRetentionPolicy_RejectsAnInvalidConfig(t *testing.T) {
	cfg := &config.Config{}
	cfg.Retention.Enabled = true
	cfg.Retention.Default = "soon"
	if _, err := retentionPolicy(cfg, nil); err == nil {
		t.Fatal("an unparsable default retention was accepted")
	}
}
