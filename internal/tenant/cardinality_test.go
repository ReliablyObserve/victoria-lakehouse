package tenant

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestCardinalityLimiter_NoPolicy_AllowsEverything(t *testing.T) {
	l := NewCardinalityLimiter(nil)
	for i := 0; i < 1000; i++ {
		if !l.AllowStream(1, 1, sprintfStream(i)) {
			t.Fatalf("stream %d rejected with no policy", i)
		}
	}
	if l.Rejected() != 0 {
		t.Errorf("rejected=%d, want 0 without policy", l.Rejected())
	}
}

func TestCardinalityLimiter_StreamLimit_EnforcesPerTenant(t *testing.T) {
	pr, err := NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Cardinality: config.TenantCardinalityOverride{MaxStreams: 3}},
	}, nil)
	if err != nil {
		t.Fatal(err)
	}
	l := NewCardinalityLimiter(pr)

	if !l.AllowStream(1, 1, "a") || !l.AllowStream(1, 1, "b") || !l.AllowStream(1, 1, "c") {
		t.Fatal("first 3 streams should be admitted")
	}
	if l.AllowStream(1, 1, "d") {
		t.Error("4th stream should be rejected (over limit)")
	}
	// Re-admitting a known stream is always allowed.
	if !l.AllowStream(1, 1, "b") {
		t.Error("known stream must not be rejected on second sight")
	}
	if l.Rejected() != 1 {
		t.Errorf("rejected=%d, want 1", l.Rejected())
	}
	if got := l.StreamCount(1, 1); got != 3 {
		t.Errorf("stream count = %d, want 3", got)
	}
	// Another tenant without an override is unbounded.
	for i := 0; i < 50; i++ {
		if !l.AllowStream(2, 2, sprintfStream(i)) {
			t.Fatalf("tenant 2:2 has no override but stream %d rejected", i)
		}
	}
}

func TestCardinalityLimiter_FieldLimit_EnforcesPerTenant(t *testing.T) {
	pr, _ := NewPolicyRegistry(map[string]config.TenantOverride{
		"1002:0": {Cardinality: config.TenantCardinalityOverride{MaxFields: 2}},
	}, nil)
	l := NewCardinalityLimiter(pr)

	if !l.AllowField(1002, 0, "f1") || !l.AllowField(1002, 0, "f2") {
		t.Fatal("first 2 fields should be admitted")
	}
	if l.AllowField(1002, 0, "f3") {
		t.Error("3rd field should be rejected")
	}
	if l.AllowField(1002, 0, "f1") != true {
		t.Error("known field must always be admitted")
	}
}

func TestCardinalityLimiter_TenantWithoutOverride_Unbounded(t *testing.T) {
	pr, _ := NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Cardinality: config.TenantCardinalityOverride{MaxStreams: 1}},
	}, nil)
	l := NewCardinalityLimiter(pr)

	// Tenant 0:0 has no override even though policy has other entries.
	for i := 0; i < 1000; i++ {
		if !l.AllowStream(0, 0, sprintfStream(i)) {
			t.Fatalf("tenant 0:0 should be unbounded, stream %d rejected", i)
		}
	}
}

func sprintfStream(i int) string {
	// Tiny inline formatter — keeps the test free of fmt overhead in
	// the inner loops.
	digits := []byte{}
	if i == 0 {
		return "s0"
	}
	for i > 0 {
		digits = append([]byte{byte('0' + i%10)}, digits...)
		i /= 10
	}
	return "s" + string(digits)
}
