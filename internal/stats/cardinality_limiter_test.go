package stats

import (
	"fmt"
	"sync"
	"testing"
)

func TestCardinalityLimiterAllow(t *testing.T) {
	cl := NewCardinalityLimiter(3)

	// Allow up to cap.
	for i := 0; i < 3; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		if !cl.Allow(tenant) {
			t.Fatalf("Allow(%q) = false; want true (under cap)", tenant)
		}
	}

	// Reject after cap.
	if cl.Allow("tenant-extra") {
		t.Fatal("Allow beyond cap should return false")
	}

	// Existing tenants still allowed.
	for i := 0; i < 3; i++ {
		tenant := fmt.Sprintf("tenant-%d", i)
		if !cl.Allow(tenant) {
			t.Fatalf("Allow(%q) = false; want true (already tracked)", tenant)
		}
	}
}

func TestCardinalityLimiterZeroDisables(t *testing.T) {
	cl := NewCardinalityLimiter(0)

	if cl.Allow("any-tenant") {
		t.Fatal("Allow should return false when maxTenants == 0")
	}
	if cl.Allow("") {
		t.Fatal("Allow should return false for empty tenant when maxTenants == 0")
	}
	if cl.TrackedCount() != 0 {
		t.Fatalf("TrackedCount = %d; want 0", cl.TrackedCount())
	}
	if cl.OverflowCount() != 2 {
		t.Fatalf("OverflowCount = %d; want 2", cl.OverflowCount())
	}
}

func TestCardinalityLimiterNegativeUnlimited(t *testing.T) {
	cl := NewCardinalityLimiter(-1)

	const n = 1000
	for i := 0; i < n; i++ {
		tenant := fmt.Sprintf("t-%d", i)
		if !cl.Allow(tenant) {
			t.Fatalf("Allow(%q) = false; want true (unlimited mode)", tenant)
		}
	}
	if cl.TrackedCount() != n {
		t.Fatalf("TrackedCount = %d; want %d", cl.TrackedCount(), n)
	}
	if cl.OverflowCount() != 0 {
		t.Fatalf("OverflowCount = %d; want 0", cl.OverflowCount())
	}
}

func TestCardinalityLimiterConcurrent(t *testing.T) {
	const (
		cap        = 100
		goroutines = 200
	)
	cl := NewCardinalityLimiter(cap)

	var wg sync.WaitGroup
	wg.Add(goroutines)
	for i := 0; i < goroutines; i++ {
		go func(id int) {
			defer wg.Done()
			tenant := fmt.Sprintf("concurrent-%d", id)
			cl.Allow(tenant)
		}(i)
	}
	wg.Wait()

	tracked := cl.TrackedCount()
	if tracked != cap {
		t.Fatalf("TrackedCount = %d; want exactly %d", tracked, cap)
	}

	overflow := cl.OverflowCount()
	expectedOverflow := int64(goroutines - cap)
	if overflow != expectedOverflow {
		t.Fatalf("OverflowCount = %d; want %d", overflow, expectedOverflow)
	}
}

func TestCardinalityLimiterReset(t *testing.T) {
	cl := NewCardinalityLimiter(2)

	cl.Allow("a")
	cl.Allow("b")
	cl.Allow("rejected") // overflow +1

	overflowBefore := cl.OverflowCount()
	if overflowBefore != 1 {
		t.Fatalf("OverflowCount before reset = %d; want 1", overflowBefore)
	}

	cl.Reset()

	if cl.TrackedCount() != 0 {
		t.Fatalf("TrackedCount after reset = %d; want 0", cl.TrackedCount())
	}

	// Overflow counter persists across reset.
	if cl.OverflowCount() != overflowBefore {
		t.Fatalf("OverflowCount after reset = %d; want %d (cumulative)", cl.OverflowCount(), overflowBefore)
	}

	// Can admit new tenants after reset.
	if !cl.Allow("c") {
		t.Fatal("Allow after reset should succeed")
	}
	if !cl.Allow("d") {
		t.Fatal("Allow after reset should succeed (second tenant)")
	}
	if cl.Allow("e") {
		t.Fatal("Allow should fail after cap reached post-reset")
	}
}

func TestCardinalityLimiterAllowIdempotent(t *testing.T) {
	cl := NewCardinalityLimiter(5)

	for i := 0; i < 100; i++ {
		if !cl.Allow("same-tenant") {
			t.Fatalf("Allow(same-tenant) call %d = false; want true", i)
		}
	}
	if cl.TrackedCount() != 1 {
		t.Fatalf("TrackedCount = %d; want 1 (idempotent)", cl.TrackedCount())
	}
	if cl.OverflowCount() != 0 {
		t.Fatalf("OverflowCount = %d; want 0", cl.OverflowCount())
	}
}

func TestCardinalityLimiterOverflowIncrementsPerReject(t *testing.T) {
	cl := NewCardinalityLimiter(1)

	cl.Allow("only-tenant")

	for i := 0; i < 5; i++ {
		tenant := fmt.Sprintf("rejected-%d", i)
		if cl.Allow(tenant) {
			t.Fatalf("Allow(%q) = true; want false (over cap)", tenant)
		}
	}
	if cl.OverflowCount() != 5 {
		t.Fatalf("OverflowCount = %d; want 5", cl.OverflowCount())
	}
}

func TestCardinalityLimiterEmptyTenantString(t *testing.T) {
	cl := NewCardinalityLimiter(2)

	if !cl.Allow("") {
		t.Fatal("Allow(\"\") = false; empty string should be a valid tenant")
	}
	if cl.TrackedCount() != 1 {
		t.Fatalf("TrackedCount = %d; want 1", cl.TrackedCount())
	}

	// Second call for empty string is idempotent.
	if !cl.Allow("") {
		t.Fatal("Allow(\"\") second call = false; want true")
	}
	if cl.TrackedCount() != 1 {
		t.Fatalf("TrackedCount = %d; want 1 (idempotent)", cl.TrackedCount())
	}
}

func TestCardinalityLimiterCapOne(t *testing.T) {
	cl := NewCardinalityLimiter(1)

	if !cl.Allow("first") {
		t.Fatal("Allow(first) = false; want true (cap=1)")
	}
	if cl.Allow("second") {
		t.Fatal("Allow(second) = true; want false (cap=1, already full)")
	}
	// First tenant remains allowed.
	if !cl.Allow("first") {
		t.Fatal("Allow(first) after rejection of second = false; want true")
	}
	if cl.TrackedCount() != 1 {
		t.Fatalf("TrackedCount = %d; want 1", cl.TrackedCount())
	}
	if cl.OverflowCount() != 1 {
		t.Fatalf("OverflowCount = %d; want 1", cl.OverflowCount())
	}
}
