package delete

import "testing"

func TestFirstNonRewritableTransition_Rules(t *testing.T) {
	// Guards the rule-list form: the earliest transition into a class that
	// CanRewrite()==false. Rewritable classes (STANDARD, INTELLIGENT_TIERING)
	// are skipped; without that, a day-1 INTELLIGENT_TIERING rule would freeze
	// compaction a day after upload.
	cases := []struct {
		name  string
		rules []LifecycleRule
		days  int
		ok    bool
	}{
		{"none", nil, 0, false},
		{"rewritable only", []LifecycleRule{{1, ClassIntelligentTiering}, {2, ClassStandard}}, 0, false},
		{"single", []LifecycleRule{{30, ClassStandardIA}}, 30, true},
		{"earliest wins, any order", []LifecycleRule{{90, ClassGlacier}, {30, ClassStandardIA}, {365, ClassDeepArchive}}, 30, true},
		{"rewritable earlier is ignored", []LifecycleRule{{1, ClassIntelligentTiering}, {45, ClassGlacierIR}}, 45, true},
		{"zero days", []LifecycleRule{{0, ClassOnezoneIA}, {10, ClassGlacier}}, 0, true},
	}
	for _, tc := range cases {
		days, ok := FirstNonRewritableTransition(tc.rules)
		if days != tc.days || ok != tc.ok {
			t.Errorf("%s: got (%d,%v), want (%d,%v)", tc.name, days, ok, tc.days, tc.ok)
		}
	}
}

func TestDetector_FirstNonRewritableTransition_PerTenant(t *testing.T) {
	// Guards the key-aware form: a tenant with its own rules uses them (even
	// when they hold no frozen class), every other key uses the global rules.
	d := NewStorageClassDetector([]LifecycleRule{{60, ClassStandardIA}})
	d.SetTenantRules(map[uint32]map[uint32][]LifecycleRule{
		1001: {0: {{10, ClassGlacier}}},
		1003: {0: {{5, ClassIntelligentTiering}}},
	})
	cases := []struct {
		key  string
		days int
		ok   bool
	}{
		{"1001/0/logs/dt=2026-01-01/hour=00/a.parquet", 10, true},
		{"1002/0/logs/dt=2026-01-01/hour=00/a.parquet", 60, true},
		{"1003/0/traces/dt=2026-01-01/hour=00/a.parquet", 0, false},
		{"1001/7/logs/dt=2026-01-01/hour=00/a.parquet", 60, true}, // other project: global
		{"logs/dt=2026-01-01/hour=00/a.parquet", 60, true},        // legacy key: global
	}
	for _, tc := range cases {
		days, ok := d.FirstNonRewritableTransition(tc.key)
		if days != tc.days || ok != tc.ok {
			t.Errorf("%s: got (%d,%v), want (%d,%v)", tc.key, days, ok, tc.days, tc.ok)
		}
	}
	empty := NewStorageClassDetector(nil)
	if _, ok := empty.FirstNonRewritableTransition("1/0/logs/x"); ok {
		t.Error("a detector with no rules reported a transition")
	}
}

// TestParseTenantFromKey_Edges guards the allocation-free parser: it must
// accept exactly what strconv.ParseUint(_, 10, 32) accepted, and need only two
// '/' (the third segment may be empty or missing its tail).
func TestParseTenantFromKey_Edges(t *testing.T) {
	cases := []struct {
		key      string
		acc, prj uint32
		ok       bool
	}{
		{"1001/0/logs/dt=x/a", 1001, 0, true},
		{"4294967295/4294967295/logs/x", 4294967295, 4294967295, true},
		{"4294967296/0/logs/x", 0, 0, false},
		{"0/4294967296/logs/x", 0, 0, false},
		{"99999999999/0/logs/x", 0, 0, false},
		{"/0/logs/x", 0, 0, false},
		{"1//logs/x", 0, 0, false},
		{"1/0/", 1, 0, true}, // two segments and a slash are enough
		{"1/0", 0, 0, false}, // second slash missing
		{"1", 0, 0, false},
		{"", 0, 0, false},
		{"-1/0/x", 0, 0, false},
		{"+1/0/x", 0, 0, false},
		{"0x1/0/x", 0, 0, false},
		{"007/008/x", 7, 8, true},
		{"logs/dt=2026-01-01/hour=00/a.parquet", 0, 0, false},
	}
	for _, c := range cases {
		a, p, ok := parseTenantFromKey(c.key)
		if ok != c.ok || (ok && (a != c.acc || p != c.prj)) {
			t.Errorf("parseTenantFromKey(%q) = %d,%d,%v; want %d,%d,%v", c.key, a, p, ok, c.acc, c.prj, c.ok)
		}
	}
	if n := testing.AllocsPerRun(100, func() { parseTenantFromKey("1001/0/logs/dt=x/a") }); n != 0 {
		t.Errorf("parseTenantFromKey allocates %v per call", n)
	}
}

// TestDetector_HasTenantRulesAndGlobalTransition guards the two accessors the
// compaction scan uses to decide whether it needs per-tenant lookups at all.
func TestDetector_HasTenantRulesAndGlobalTransition(t *testing.T) {
	d := NewStorageClassDetector([]LifecycleRule{{60, ClassStandardIA}, {10, ClassIntelligentTiering}})
	if d.HasTenantRules() {
		t.Fatal("HasTenantRules true with no overrides")
	}
	if days, ok := d.FirstGlobalNonRewritableTransition(); !ok || days != 60 {
		t.Fatalf("global transition = %d,%v; want 60,true", days, ok)
	}
	d.SetTenantRules(map[uint32]map[uint32][]LifecycleRule{1001: {0: {{5, ClassGlacier}}}})
	if !d.HasTenantRules() {
		t.Fatal("HasTenantRules false after SetTenantRules")
	}
	if days, ok := d.FirstGlobalNonRewritableTransition(); !ok || days != 60 {
		t.Fatalf("a tenant override must not change the global transition: %d,%v", days, ok)
	}
	if _, ok := NewStorageClassDetector(nil).FirstGlobalNonRewritableTransition(); ok {
		t.Fatal("no rules, but a transition was reported")
	}
}
