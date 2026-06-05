package manifest

import (
	"testing"
)

// TestIsValidTenantSegment pins the character whitelist used for
// account/project segments parsed out of S3 keys. Anything outside
// [0-9A-Za-z_-] must be rejected so a malicious key like
// "0/0%2F../1/x" or "0/../1/x" can't be attributed to a tenant
// it doesn't belong to.
func TestIsValidTenantSegment(t *testing.T) {
	good := []string{
		"0", "1001", "acme", "team-a", "team_b",
		"a", "Z", "ABC123", "with-hyphens", "with_underscore",
	}
	for _, s := range good {
		if !isValidTenantSegment(s) {
			t.Errorf("isValidTenantSegment(%q) = false; want true", s)
		}
	}

	bad := []string{
		"",          // empty — no tenant id
		"/",         // path separator
		"..",        // path traversal
		"a/b",       // slash inside segment
		"a..b",      // dots are not in whitelist
		"a b",       // whitespace
		"a\tb",      // tab
		"a\nb",      // newline
		"a\x00b",    // null byte
		"team%2Fa",  // URL-encoded slash
		"team.org",  // dot
		"админ",     // non-ASCII — Unicode confusable risk
		"a@b",       // shell special
		"a;b",       // shell separator
		`a"b`,       // quote
		"a<b",       // angle bracket
		"тест-тест", // Cyrillic confusable
	}
	for _, s := range bad {
		if isValidTenantSegment(s) {
			t.Errorf("isValidTenantSegment(%q) = true; want false (security: tenant-path injection)", s)
		}
	}

	// Length bound — 64 chars max, anything longer is rejected.
	longSeg := make([]byte, 65)
	for i := range longSeg {
		longSeg[i] = 'a'
	}
	if isValidTenantSegment(string(longSeg)) {
		t.Errorf("isValidTenantSegment(<65 chars>) = true; want false (length bound)")
	}
	// 64 is the boundary — must still pass.
	if !isValidTenantSegment(string(longSeg[:64])) {
		t.Errorf("isValidTenantSegment(<64 chars>) = false; want true (boundary case)")
	}
}

// TestTenantKeyFromFileKey_RejectsMaliciousKeys pins the upstream
// guard at tenantKeyFromFileKey — a crafted S3 key with non-whitelist
// characters in the tenant segments must be rejected before getting
// near the tenantAggregates map or the GetFilesForRangeTenant
// prefix check.
func TestTenantKeyFromFileKey_RejectsMaliciousKeys(t *testing.T) {
	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")

	bad := []string{
		"0/../1/traces/x.parquet",         // path traversal
		"0/0\x00/traces/x.parquet",        // null byte
		"0/0 with space/traces/x.parquet", // whitespace
		"0/0%2Fadmin/traces/x.parquet",    // URL-encoded slash
		"0/tенант/traces/x.parquet",       // Unicode confusable
		"0/team@evil/traces/x.parquet",    // shell metachar
	}
	for _, k := range bad {
		if _, ok := m.tenantKeyFromFileKey(k); ok {
			t.Errorf("tenantKeyFromFileKey accepted malicious key %q", k)
		}
	}

	// Sanity: normal integer keys still parse.
	if k, ok := m.tenantKeyFromFileKey("0/0/traces/x.parquet"); !ok || k.account != "0" || k.project != "0" {
		t.Errorf("legit integer key rejected: ok=%v key=%+v", ok, k)
	}
	if k, ok := m.tenantKeyFromFileKey("1001/0/traces/x.parquet"); !ok || k.account != "1001" {
		t.Errorf("legit four-digit account key rejected: ok=%v key=%+v", ok, k)
	}
}

// TestTenantKeyFromFileKey_OrgIDTemplate covers the single-segment
// path with the whitelist applied.
func TestTenantKeyFromFileKey_OrgIDTemplate(t *testing.T) {
	m := New("b", "")
	m.SetPrefixTemplate("{OrgID}/")

	if k, ok := m.tenantKeyFromFileKey("acme/data/x.parquet"); !ok || k.account != "acme" || k.project != "" {
		t.Errorf("legit OrgID key rejected: ok=%v key=%+v", ok, k)
	}
	if _, ok := m.tenantKeyFromFileKey("acme org/data/x.parquet"); ok {
		t.Error("OrgID with space accepted")
	}
}
