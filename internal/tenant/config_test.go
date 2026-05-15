package tenant

import "testing"

func TestPrefixTemplateSegments(t *testing.T) {
	tests := []struct {
		template string
		want     int
	}{
		{"{AccountID}/{ProjectID}/", 2},
		{"{OrgID}/", 1},
		{"{OrgID}/{ProjectID}/", 2},
		{"", 0},
		{"static-prefix/", 0},
	}
	for _, tc := range tests {
		got := CountTemplateSegments(tc.template)
		if got != tc.want {
			t.Errorf("CountTemplateSegments(%q) = %d, want %d", tc.template, got, tc.want)
		}
	}
}

func TestHasOrgIDTemplate(t *testing.T) {
	if !HasOrgIDTemplate("{OrgID}/") {
		t.Error("expected true for {OrgID}/")
	}
	if !HasOrgIDTemplate("{OrgID}/{ProjectID}/") {
		t.Error("expected true for {OrgID}/{ProjectID}/")
	}
	if HasOrgIDTemplate("{AccountID}/{ProjectID}/") {
		t.Error("expected false for integer template")
	}
}
