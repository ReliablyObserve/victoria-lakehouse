package main

import (
	"flag"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

func setTenantFlag(t *testing.T, name, value string) {
	t.Helper()
	f := flag.Lookup(name)
	if f == nil {
		t.Fatalf("flag -%s is not defined", name)
	}
	if err := flag.Set(name, value); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = flag.Set(name, f.DefValue) })
}

func TestAutoRegisterRangeFlagsReachTheConfig(t *testing.T) {
	cfg := config.Default()
	applyFlags(cfg)
	if cfg.Tenant.AutoRegisterMinID != 2147483648 || cfg.Tenant.AutoRegisterMaxID != 4294967294 {
		t.Fatalf("defaults changed by applyFlags: [%d, %d]", cfg.Tenant.AutoRegisterMinID, cfg.Tenant.AutoRegisterMaxID)
	}

	setTenantFlag(t, "lakehouse.tenant.auto-register-min-id", "5000")
	setTenantFlag(t, "lakehouse.tenant.auto-register-max-id", "6000")
	cfg = config.Default()
	applyFlags(cfg)
	if cfg.Tenant.AutoRegisterMinID != 5000 || cfg.Tenant.AutoRegisterMaxID != 6000 {
		t.Fatalf("flags not applied: [%d, %d]", cfg.Tenant.AutoRegisterMinID, cfg.Tenant.AutoRegisterMaxID)
	}
}

func TestStartupRefusesCollidingAliases(t *testing.T) {
	aliasFlagConflicts = nil
	t.Cleanup(func() { aliasFlagConflicts = nil })

	// One OrgID given two targets on the command line is recorded, not silently overwritten.
	setTenantFlag(t, "lakehouse.tenant.alias", "acme-corp:1001:0,acme-corp:1002:0,staging-team:1002:0")
	cfg := config.Default()
	applyFlags(cfg)
	if len(aliasFlagConflicts) != 1 || !strings.Contains(aliasFlagConflicts[0], "acme-corp") {
		t.Fatalf("aliasFlagConflicts = %v", aliasFlagConflicts)
	}
	if got := cfg.Tenant.Aliases["acme-corp"]; got.AccountID != 1001 {
		t.Fatalf("the first target must stay: %+v", got)
	}

	// Two OrgIDs on one ID, and aliases inside the reserved range, fail the
	// validation both binaries run before serving.
	rng := tenant.DefaultAutoRange()
	entries := []tenant.AliasEntry{{OrgID: "a", AccountID: 7}, {OrgID: "b", AccountID: 7}}
	if err := tenant.ValidateConfiguredAliases(entries, rng); err == nil {
		t.Fatal("two OrgIDs on one ID accepted")
	}
	cfg = config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.Bucket = "b"
	cfg.Tenant.Aliases = map[string]config.AliasTarget{"squatter": {AccountID: rng.Min}}
	if err := cfg.Validate(); err == nil {
		t.Fatal("an alias inside the auto-register range passed config validation")
	}
}
