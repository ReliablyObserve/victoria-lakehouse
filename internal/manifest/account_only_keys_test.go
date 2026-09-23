package manifest

import "testing"

// AccountOnlyTenantKeys reports the {OrgID}/ key layout, where a key carries
// the account alone; the delete path refuses per-project deletes there.
func TestAccountOnlyTenantKeys(t *testing.T) {
	cases := []struct {
		template string
		set      bool
		want     bool
	}{
		{"{AccountID}/{ProjectID}/", true, false},
		{"{OrgID}/", true, true},
		{"{OrgID}/{ProjectID}/", true, false},
		{"", false, false},
	}
	for _, tc := range cases {
		m := New("b", "")
		if tc.set {
			m.SetPrefixTemplate(tc.template)
		}
		if got := m.AccountOnlyTenantKeys(); got != tc.want {
			t.Errorf("template %q: AccountOnlyTenantKeys = %v, want %v", tc.template, got, tc.want)
		}
	}
	// A template recorded without SetPrefixTemplate (no parsed segment count)
	// is read from the template string itself.
	m := New("b", "")
	m.prefixTemplate = "{OrgID}/"
	if !m.AccountOnlyTenantKeys() {
		t.Error("an unparsed {OrgID}/ template must report account-only keys")
	}
}
