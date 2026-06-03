package delete

import (
	"testing"
)

func TestParseTenantFromKey(t *testing.T) {
	cases := []struct {
		key        string
		acc, proj  uint32
		ok         bool
	}{
		{"1002/0/logs/dt=2026-06-03/hour=10/abc.parquet", 1002, 0, true},
		{"42/3/traces/dt=2026-06-03/foo.parquet", 42, 3, true},
		{"0/0/logs/dt=2026-06-03/_bloom.bin", 0, 0, true},
		// Legacy / non-tenant keys: leading non-numeric segment.
		{"obs-archive/logs/foo.parquet", 0, 0, false},
		// Too few path segments.
		{"1002", 0, 0, false},
		{"1002/0", 0, 0, false},
		// Malformed numerics.
		{"abc/0/logs/foo.parquet", 0, 0, false},
		{"1/xyz/logs/foo.parquet", 0, 0, false},
	}
	for _, tc := range cases {
		acc, proj, ok := parseTenantFromKey(tc.key)
		if ok != tc.ok || acc != tc.acc || proj != tc.proj {
			t.Errorf("%q: got (%d,%d,%v); want (%d,%d,%v)",
				tc.key, acc, proj, ok, tc.acc, tc.proj, tc.ok)
		}
	}
}

func TestStorageClassDetector_DetectForKey_FallsBackToGlobal(t *testing.T) {
	d := NewStorageClassDetector([]LifecycleRule{
		{TransitionDays: 30, Class: ClassStandardIA},
	})
	// No per-tenant rules installed: every tenant uses global.
	got := d.DetectForKey(31*24, "1002/0/logs/dt=foo/x.parquet")
	if got != ClassStandardIA {
		t.Errorf("global fallback class = %q, want %q", got, ClassStandardIA)
	}
	// Unparseable key path → still global.
	got = d.DetectForKey(31*24, "broken-key")
	if got != ClassStandardIA {
		t.Errorf("unparseable key class = %q, want %q", got, ClassStandardIA)
	}
}

func TestStorageClassDetector_DetectForKey_TenantOverrideWins(t *testing.T) {
	d := NewStorageClassDetector([]LifecycleRule{
		{TransitionDays: 30, Class: ClassStandardIA},
		{TransitionDays: 365, Class: ClassGlacier},
	})
	// Tenant 1002:0 keeps everything in STANDARD forever — no transitions.
	d.SetTenantRules(map[uint32]map[uint32][]LifecycleRule{
		1002: {0: {}},
		// Tenant 42:3 transitions much sooner: ONEZONE_IA at 7 days.
		42: {3: {{TransitionDays: 7, Class: ClassOnezoneIA}}},
	})

	// 1002:0 file aged 60 days — should stay STANDARD per its override.
	if got := d.DetectForKey(60*24, "1002/0/logs/dt=foo/x.parquet"); got != ClassStandard {
		t.Errorf("1002:0 override class = %q, want %q", got, ClassStandard)
	}
	// 42:3 file aged 14 days — should be ONEZONE_IA per tenant rule.
	if got := d.DetectForKey(14*24, "42/3/traces/dt=foo/x.parquet"); got != ClassOnezoneIA {
		t.Errorf("42:3 override class = %q, want %q", got, ClassOnezoneIA)
	}
	// Unconfigured tenant — global applies.
	if got := d.DetectForKey(60*24, "999/9/logs/dt=foo/x.parquet"); got != ClassStandardIA {
		t.Errorf("unconfigured-tenant class = %q, want %q (global)", got, ClassStandardIA)
	}
}
