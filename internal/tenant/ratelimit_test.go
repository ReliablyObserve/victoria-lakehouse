package tenant

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestIngestRateLimiter_NoPolicy_AllowsEverything(t *testing.T) {
	l := NewIngestRateLimiter(nil)
	for i := 0; i < 10; i++ {
		if !l.Allow(1, 1, 10*1024*1024, 100000) {
			t.Fatalf("nil policy must accept; iteration %d rejected", i)
		}
	}
	if l.Rejected() != 0 {
		t.Errorf("rejected=%d, want 0", l.Rejected())
	}
}

func TestIngestRateLimiter_BytesCap_RejectsOverflow(t *testing.T) {
	pr, _ := NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Ingest: config.TenantIngestOverride{MaxBytesPerSec: 1000}},
	}, nil)
	now := time.Now()
	l := NewIngestRateLimiter(pr)
	l.now = func() time.Time { return now }

	// First request takes 800 of the 1000-byte bucket.
	if !l.Allow(1, 1, 800, 0) {
		t.Fatal("800 bytes should fit in fresh 1000-byte bucket")
	}
	// Second request takes 199 of remaining 200 — fine.
	if !l.Allow(1, 1, 199, 0) {
		t.Fatal("199 bytes should fit in remaining 201")
	}
	// Third request asking for 100 should fail (only ~1 byte left).
	if l.Allow(1, 1, 100, 0) {
		t.Error("100 bytes should fail when ~1 byte remains")
	}
	if l.Rejected() != 1 {
		t.Errorf("rejected=%d, want 1", l.Rejected())
	}

	// After half a second, refill 500 more — 500 total available.
	now = now.Add(500 * time.Millisecond)
	if !l.Allow(1, 1, 500, 0) {
		t.Fatal("500 bytes should fit after refill")
	}
}

func TestIngestRateLimiter_RowsCap_RejectsOverflow(t *testing.T) {
	pr, _ := NewPolicyRegistry(map[string]config.TenantOverride{
		"42:3": {Ingest: config.TenantIngestOverride{MaxRowsPerSec: 100}},
	}, nil)
	now := time.Now()
	l := NewIngestRateLimiter(pr)
	l.now = func() time.Time { return now }

	if !l.Allow(42, 3, 0, 90) {
		t.Fatal("90 rows must fit in fresh 100-row bucket")
	}
	if l.Allow(42, 3, 0, 50) {
		t.Error("50 rows must be rejected when only ~10 remain")
	}
}

func TestIngestRateLimiter_BothCaps_StrictAnd(t *testing.T) {
	pr, _ := NewPolicyRegistry(map[string]config.TenantOverride{
		"7:0": {Ingest: config.TenantIngestOverride{
			MaxBytesPerSec: 10000,
			MaxRowsPerSec:  10,
		}},
	}, nil)
	now := time.Now()
	l := NewIngestRateLimiter(pr)
	l.now = func() time.Time { return now }

	// Plenty of bytes, but row cap rejects.
	if l.Allow(7, 0, 100, 11) {
		t.Error("row cap (10) should reject 11 rows even if bytes fit")
	}
	if !l.Allow(7, 0, 100, 10) {
		t.Error("10 rows must fit at exactly the cap")
	}
}

func TestIngestRateLimiter_TenantWithoutOverride_Unlimited(t *testing.T) {
	pr, _ := NewPolicyRegistry(map[string]config.TenantOverride{
		"1:1": {Ingest: config.TenantIngestOverride{MaxBytesPerSec: 1}},
	}, nil)
	l := NewIngestRateLimiter(pr)
	// Other tenant (0:0) has no override → always allowed.
	for i := 0; i < 100; i++ {
		if !l.Allow(0, 0, 1<<30, 1<<20) {
			t.Fatalf("tenant 0:0 must be unlimited; rejected at %d", i)
		}
	}
}
