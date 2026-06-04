package tenant

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// TestPolicyRegistry_BareAccountKey_TreatedAsProjectZero verifies the
// account-only override-key form matches upstream VL/VT's
// ParseTenantID semantics: a missing project segment defaults to 0,
// so "42" and "42:0" address the same tenant.
//
// This is the operator-facing contract — single-account tenants
// shouldn't have to spell out the redundant ":0".
func TestPolicyRegistry_BareAccountKey_TreatedAsProjectZero(t *testing.T) {
	pr, err := NewPolicyRegistry(map[string]config.TenantOverride{
		"42": {Retention: config.TenantRetentionOverride{Keep: "14d"}},
	}, nil)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	// Resolves under (42, 0) — same as if "42:0" had been written.
	if eff := pr.For(42, 0); eff == nil {
		t.Fatal("bare account key did not resolve under (42, 0)")
	} else if eff.Retention != 14*24*time.Hour {
		t.Errorf("retention = %s, want 336h", eff.Retention)
	}

	// And NOT under any other project id.
	if pr.For(42, 1) != nil {
		t.Error("bare account key must not match (42, 1)")
	}
}

func TestPolicyRegistry_ParseAccountProject_BareForm(t *testing.T) {
	cases := []struct {
		in       string
		want     TenantID
		wantOK   bool
	}{
		{"42:3", TenantID{AccountID: 42, ProjectID: 3}, true},
		{"42", TenantID{AccountID: 42, ProjectID: 0}, true},
		{"  42  ", TenantID{AccountID: 42, ProjectID: 0}, true},
		{"abc", TenantID{}, false},
		{"42:abc", TenantID{}, false},
		{"", TenantID{}, false},
	}
	for _, tc := range cases {
		got, ok := parseAccountProject(tc.in)
		if ok != tc.wantOK || got != tc.want {
			t.Errorf("parseAccountProject(%q) = (%+v, %v); want (%+v, %v)",
				tc.in, got, ok, tc.want, tc.wantOK)
		}
	}
}
