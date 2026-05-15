package tenant

import "testing"

func TestValidateOrgID(t *testing.T) {
	valid := []string{
		"prod-team-eu_staging",
		"prod-team-eu_prod",
		"dev_default",
		"acme-corp_us-east_production",
		"a",
		"abc123",
		"my-org!special_proj",
		"has.dots.in.it",
		"parens(ok)",
		"star*ok",
		"quote'ok",
	}
	for _, id := range valid {
		if err := ValidateOrgID(id); err != nil {
			t.Errorf("ValidateOrgID(%q) = %v, want nil", id, err)
		}
	}

	invalid := []struct {
		id   string
		desc string
	}{
		{"", "empty"},
		{".", "dot only"},
		{"..", "double dot"},
		{"has/slash", "slash"},
		{"has|pipe", "pipe"},
		{"has:colon", "colon"},
		{"has space", "space"},
		{"has@at", "at sign"},
		{string(make([]byte, 151)), "too long"},
	}
	for _, tc := range invalid {
		if err := ValidateOrgID(tc.id); err == nil {
			t.Errorf("ValidateOrgID(%q) [%s] = nil, want error", tc.id, tc.desc)
		}
	}
}
