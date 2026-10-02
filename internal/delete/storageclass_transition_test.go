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
