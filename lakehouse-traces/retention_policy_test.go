package main

import (
	"go/ast"
	"go/parser"
	"go/token"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// retentionTestConfig is a retention config with a 30 day default, a 365 day rule
// on a label, and two tenant overrides (1001: 7 days, 2002:7: 90 days).
func retentionTestConfig(t *testing.T, role config.Role) (*config.Config, *tenant.PolicyRegistry) {
	t.Helper()
	cfg := podConfig(t, role, "http://127.0.0.1:1")
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
	return cfg, reg
}

// The policy the binary builds has the values the config says, written out: a 7
// day tenant is expired at 8 days, a 90 day tenant is not, the default expires at
// 31 days and not at 29, a label rule keeps 365 days. (A mutant that drops the
// tenant overrides, or the label rules, from the policy fails here.)
func TestRetentionPolicy_ExpectedValues(t *testing.T) {
	cfg, reg := retentionTestConfig(t, config.RoleAll)
	rp, err := installRetention(cfg, reg, newPod(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 10, 18, 12, 0, 0, 0, time.UTC)
	labels := func(acct, proj string, svc string) map[string][]string {
		m := map[string][]string{"account_id": {acct}, "project_id": {proj}}
		if svc != "" {
			m["service.name"] = []string{svc}
		}
		return m
	}
	for _, c := range []struct {
		name string
		l    map[string][]string
		days int
		want bool
	}{
		{"7d tenant at 8 days", labels("1001", "0", ""), 8, true},
		{"7d tenant at 6 days", labels("1001", "0", ""), 6, false},
		{"90d tenant at 8 days", labels("2002", "7", ""), 8, false},
		{"90d tenant at 89 days", labels("2002", "7", ""), 89, false},
		{"90d tenant at 91 days", labels("2002", "7", ""), 91, true},
		{"default tenant at 29 days", labels("0", "0", ""), 29, false},
		{"default tenant at 31 days", labels("0", "0", ""), 31, true},
		{"label rule at 100 days", labels("0", "0", "audit"), 100, false},
		{"label rule at 366 days", labels("0", "0", "audit"), 366, true},
		{"no labels at 31 days", nil, 31, true},
		{"no labels at 29 days", nil, 29, false},
	} {
		fi := manifest.FileInfo{Labels: c.l, MaxTimeNs: now.Add(-time.Duration(c.days) * 24 * time.Hour).UnixNano()}
		if got := rp.Expired(fi, now); got != c.want {
			t.Errorf("%s: expired = %v, want %v", c.name, got, c.want)
		}
	}

	// Retention off: no cut at all.
	cfg.Retention.Enabled = false
	off, err := installRetention(cfg, reg, newPod(t, cfg))
	if err != nil {
		t.Fatal(err)
	}
	if off.Enabled() || off.Expired(manifest.FileInfo{MaxTimeNs: 1}, now) {
		t.Fatal("a disabled retention config must not cut anything")
	}
}

// Every role installs the policy, including select, which never runs the eviction
// loop: the storage of a select-only pod cuts its reads.
func TestRetentionCut_EveryRoleInstallsThePolicy(t *testing.T) {
	for _, role := range []config.Role{config.RoleAll, config.RoleInsert, config.RoleSelect} {
		t.Run(string(role), func(t *testing.T) {
			cfg, reg := retentionTestConfig(t, role)
			store := newPod(t, cfg)
			if store.RetentionPolicy() != nil {
				t.Fatal("fixture: a new storage already has a policy")
			}
			if _, err := installRetention(cfg, reg, store); err != nil {
				t.Fatal(err)
			}
			rp := store.RetentionPolicy()
			if rp == nil || !rp.Enabled() {
				t.Fatalf("role %s: the storage has no enabled retention policy", role)
			}
			old := manifest.FileInfo{MaxTimeNs: time.Now().Add(-60 * 24 * time.Hour).UnixNano()}
			if !rp.Expired(old, time.Now()) {
				t.Fatalf("role %s: a 60 day old file is not cut under a 30 day retention", role)
			}
		})
	}
}

// run() installs the policy unconditionally (not behind a role check): its body
// calls installRetention as a top-level statement. A refactor that drops the call
// or moves it under a role condition leaves select pods serving expired data.
func TestRun_InstallsTheRetentionPolicyForEveryRole(t *testing.T) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "main.go", nil, 0)
	if err != nil {
		t.Fatal(err)
	}
	var found, startsAfter bool
	for _, d := range f.Decls {
		fd, ok := d.(*ast.FuncDecl)
		if !ok || fd.Name.Name != "run" {
			continue
		}
		for _, st := range fd.Body.List {
			if call := topLevelCall(st, "installRetention"); call {
				found = true
			}
			if topLevelCall(st, "startRetention") && found {
				startsAfter = true
			}
		}
	}
	if !found {
		t.Fatal("run() does not call installRetention as a top-level statement: select pods would serve data past its retention")
	}
	if !startsAfter {
		t.Fatal("run() must install the policy before it starts the eviction loop, so the loop uses the installed object")
	}
}

// topLevelCall reports whether the statement is (or initialises an if with) a
// call of the named function.
func topLevelCall(st ast.Stmt, name string) bool {
	found := false
	ast.Inspect(st, func(n ast.Node) bool {
		switch v := n.(type) {
		case *ast.BlockStmt:
			return false // do not look into nested blocks
		case *ast.CallExpr:
			if id, ok := v.Fun.(*ast.Ident); ok && id.Name == name {
				found = true
			}
		}
		return true
	})
	return found
}

// What the eviction deletes and what the read cut hides are decided by one
// object: the manager is built from the policy the storage was given.
func TestRetention_EvictionAndCutShareOnePolicy(t *testing.T) {
	cfg, reg := retentionTestConfig(t, config.RoleAll)
	store := newPod(t, cfg)
	rp, err := installRetention(cfg, reg, store)
	if err != nil {
		t.Fatal(err)
	}
	mgr, err := buildRetentionManager(cfg, store, rp, "traces")
	if err != nil {
		t.Fatal(err)
	}
	if mgr.Policy() != store.RetentionPolicy() || mgr.Policy() != rp {
		t.Fatal("the eviction manager and the storage's read cut use different Policy objects")
	}
	// startRetention takes the installed object too (it does not build another).
	if stop := startRetention(cfg, store, reg); stop != nil {
		stop()
	}
	if store.RetentionPolicy() != rp {
		t.Fatal("startRetention replaced the installed policy")
	}
}

// An invalid retention config is refused (the binary exits on it).
func TestRetentionPolicy_RejectsAnInvalidConfig(t *testing.T) {
	cfg, reg := retentionTestConfig(t, config.RoleAll)
	cfg.Retention.Default = "soon"
	if _, err := installRetention(cfg, reg, newPod(t, cfg)); err == nil {
		t.Fatal("an unparsable default retention was accepted")
	}
}
