package tenant

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// TestPolicyRegistry_RetentionEntries_RoundtripsThroughSynthesizer
// guards the adapter shape that main.go relies on: every retention
// override registered via TenantConfig.Overrides must surface from
// RetentionEntries() with a Go-duration string the retention package
// can parse without modification.
func TestPolicyRegistry_RetentionEntries_RoundtripsThroughSynthesizer(t *testing.T) {
	r := NewResolver(ResolverConfig{})
	_ = r.AddAlias("acme-corp", TenantID{AccountID: 1002, ProjectID: 0})

	pr, err := NewPolicyRegistry(map[string]config.TenantOverride{
		"acme-corp": {Retention: config.TenantRetentionOverride{Keep: "30d"}},
		"1:1":       {Retention: config.TenantRetentionOverride{Keep: "7d"}},
		// Cardinality-only entry — RetentionEntries must skip it.
		"5:0": {Cardinality: config.TenantCardinalityOverride{MaxFields: 100}},
	}, r)
	if err != nil {
		t.Fatalf("new: %v", err)
	}

	entries := pr.RetentionEntries()
	if len(entries) != 2 {
		t.Fatalf("got %d entries, want 2 (cardinality-only override must drop)", len(entries))
	}

	byKey := map[string]string{}
	for _, e := range entries {
		key := tenantKeyString(e.AccountID, e.ProjectID)
		byKey[key] = e.Retention
	}
	if got := byKey["1002:0"]; got != (30 * 24 * time.Hour).String() {
		t.Errorf("acme-corp keep = %q, want %s", got, 30*24*time.Hour)
	}
	if got := byKey["1:1"]; got != (7 * 24 * time.Hour).String() {
		t.Errorf("1:1 keep = %q, want %s", got, 7*24*time.Hour)
	}
}

func tenantKeyString(a, p uint32) string {
	return formatU(a) + ":" + formatU(p)
}

func formatU(x uint32) string {
	if x == 0 {
		return "0"
	}
	digits := []byte{}
	for x > 0 {
		digits = append([]byte{byte('0' + x%10)}, digits...)
		x /= 10
	}
	return string(digits)
}
