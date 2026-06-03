package retention

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func TestSynthesizeRules_NoEntries(t *testing.T) {
	if got := SynthesizeRules(nil); got != nil {
		t.Errorf("nil input: got %v, want nil", got)
	}
	if got := SynthesizeRules([]TenantRetentionEntry{}); got != nil {
		t.Errorf("empty input: got %v, want nil", got)
	}
}

func TestSynthesizeRules_SkipsBlankKeep(t *testing.T) {
	rules := SynthesizeRules([]TenantRetentionEntry{
		{AccountID: 1, ProjectID: 1, Keep: ""},
		{AccountID: 1002, ProjectID: 0, Keep: "30d"},
	})
	if len(rules) != 1 {
		t.Fatalf("got %d rules, want 1 (blank-keep entries must drop)", len(rules))
	}
	if rules[0].Keep != "30d" {
		t.Errorf("rules[0].Keep = %q, want 30d", rules[0].Keep)
	}
	if rules[0].Match["account_id"] != "1002" {
		t.Errorf("rules[0].Match[account_id] = %q, want 1002", rules[0].Match["account_id"])
	}
}

func TestSynthesizeRules_AppliedByResolveTTL(t *testing.T) {
	// End-to-end check that a synthesized tenant rule overrides the
	// default TTL when a file carries the right account/project label.
	rules := SynthesizeRules([]TenantRetentionEntry{
		{AccountID: 1002, ProjectID: 0, Keep: "90d"},
	})
	cfg := Config{
		Enabled:       true,
		Default:       "30d",
		CheckInterval: "1h",
		Rules:         rules,
	}
	mf := newMockManifest(nil)
	mgr, err := New(cfg, mf, nil, "test-bucket", testLogger())
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// File belongs to tenant 1002:0 → expects 90d.
	tenantFile := manifest.FileInfo{
		Labels: map[string][]string{
			"account_id": {"1002"},
			"project_id": {"0"},
		},
	}
	got := mgr.ResolveTTL(tenantFile)
	if got.Hours() != 90*24 {
		t.Errorf("tenant file TTL = %s, want 2160h (90d)", got)
	}

	// File belongs to some other tenant → falls back to default.
	otherFile := manifest.FileInfo{
		Labels: map[string][]string{
			"account_id": {"42"},
			"project_id": {"3"},
		},
	}
	got = mgr.ResolveTTL(otherFile)
	if got.Hours() != 30*24 {
		t.Errorf("other-tenant file TTL = %s, want 720h (default 30d)", got)
	}
}
