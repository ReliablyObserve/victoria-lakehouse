package smartcache

import (
	"fmt"
	"runtime"
	"sync"
	"testing"
)

// l2Has checks whether a key exists in the mockL2 store (defined in controller_test.go).
func l2Has(m *mockL2, key string) bool {
	_, ok := m.Get(key)
	return ok
}

func TestBudgetedL1_EnforcesMaxBytes(t *testing.T) {
	l2 := newMockL2()
	c := NewBudgetedL1(100, l2)

	// Each value is 40 bytes. After 3 puts (120 bytes), budget (100) is exceeded.
	c.Put("a", make([]byte, 40))
	c.Put("b", make([]byte, 40))
	c.Put("c", make([]byte, 40))

	if used := c.UsedBytes(); used > 100 {
		t.Errorf("UsedBytes()=%d, want <= 100", used)
	}

	// The oldest key ("a") should have been evicted.
	if _, ok := c.Get("a"); ok {
		t.Error("key 'a' should have been evicted")
	}

	// "b" and "c" should still be present (80 bytes <= 100).
	if _, ok := c.Get("b"); !ok {
		t.Error("key 'b' should still be in L1")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("key 'c' should still be in L1")
	}
}

func TestBudgetedL1_SpillToL2OnEviction(t *testing.T) {
	l2 := newMockL2()
	c := NewBudgetedL1(80, l2)

	data := []byte("hello-from-a")
	c.Put("a", data)
	c.Put("b", make([]byte, 80)) // forces "a" out (80 > budget means evict until fits)

	// "a" should have been spilled to L2.
	if !l2Has(l2, "a") {
		t.Error("evicted key 'a' not found in L2")
	}

	// Verify the data in L2 matches what was stored.
	l2Data, ok := l2.Get("a")
	if !ok {
		t.Fatal("expected to find 'a' in L2")
	}
	if string(l2Data) != string(data) {
		t.Errorf("L2 data mismatch: got %q, want %q", l2Data, data)
	}
}

func TestBudgetedL1_GetPromotesToFront(t *testing.T) {
	l2 := newMockL2()
	c := NewBudgetedL1(100, l2)

	c.Put("a", make([]byte, 40))
	c.Put("b", make([]byte, 40))

	// Access "a" to promote it to front; "b" becomes LRU.
	c.Get("a")

	// Insert "c" which forces eviction of the LRU entry ("b").
	c.Put("c", make([]byte, 40))

	if _, ok := c.Get("b"); ok {
		t.Error("key 'b' should have been evicted (it was LRU)")
	}
	if _, ok := c.Get("a"); !ok {
		t.Error("key 'a' should still be present (promoted to front)")
	}
	if _, ok := c.Get("c"); !ok {
		t.Error("key 'c' should still be present")
	}
}

func TestBudgetedL1_PutOverwrite(t *testing.T) {
	l2 := newMockL2()
	c := NewBudgetedL1(100, l2)

	c.Put("a", make([]byte, 60))
	if used := c.UsedBytes(); used != 60 {
		t.Fatalf("UsedBytes()=%d, want 60", used)
	}

	// Overwrite "a" with smaller value.
	c.Put("a", make([]byte, 30))
	if used := c.UsedBytes(); used != 30 {
		t.Errorf("UsedBytes()=%d after overwrite, want 30", used)
	}

	// Overwrite "a" with larger value.
	c.Put("a", make([]byte, 90))
	if used := c.UsedBytes(); used != 90 {
		t.Errorf("UsedBytes()=%d after second overwrite, want 90", used)
	}

	if n := c.Len(); n != 1 {
		t.Errorf("Len()=%d, want 1 after overwrites", n)
	}
}

func TestBudgetedL1_EmptyGet(t *testing.T) {
	c := NewBudgetedL1(100, nil)

	data, ok := c.Get("nonexistent")
	if ok {
		t.Error("Get on non-existent key should return false")
	}
	if data != nil {
		t.Errorf("Get on non-existent key should return nil, got %v", data)
	}
}

func TestBudgetedL1_ZeroBudget(t *testing.T) {
	l2 := newMockL2()
	c := NewBudgetedL1(0, l2)

	c.Put("a", []byte("data"))

	// With zero budget, everything gets evicted immediately.
	if _, ok := c.Get("a"); ok {
		t.Error("key 'a' should not be in L1 with zero budget")
	}

	// But it should have been spilled to L2.
	if !l2Has(l2, "a") {
		t.Error("evicted key 'a' should be in L2")
	}

	if used := c.UsedBytes(); used != 0 {
		t.Errorf("UsedBytes()=%d, want 0 with zero budget", used)
	}
}

func TestBudgetedL1_LargerThanBudget(t *testing.T) {
	l2 := newMockL2()
	c := NewBudgetedL1(50, l2)

	// Pre-fill with a small entry.
	c.Put("small", make([]byte, 30))

	// Put a single item larger than the budget. It should evict everything
	// including itself since the single item exceeds budget.
	c.Put("huge", make([]byte, 100))

	// "small" was evicted to L2 first, then "huge" was evicted too since
	// it alone exceeds the budget.
	if !l2Has(l2, "small") {
		t.Error("'small' should have been spilled to L2")
	}
	if !l2Has(l2, "huge") {
		t.Error("'huge' should have been spilled to L2")
	}

	if used := c.UsedBytes(); used != 0 {
		t.Errorf("UsedBytes()=%d, want 0 (item larger than budget)", used)
	}
}

func TestBudgetedL1_ConcurrentAccess(t *testing.T) {
	l2 := newMockL2()
	c := NewBudgetedL1(1000, l2)

	var wg sync.WaitGroup
	const goroutines = 10
	const opsPerGoroutine = 100

	wg.Add(goroutines)
	for g := 0; g < goroutines; g++ {
		go func(id int) {
			defer wg.Done()
			for i := 0; i < opsPerGoroutine; i++ {
				key := fmt.Sprintf("g%d-k%d", id, i)
				c.Put(key, make([]byte, 10))
				c.Get(key)
				_ = c.UsedBytes()
				_ = c.Len()
			}
		}(g)
	}
	wg.Wait()

	// If we got here without panics or deadlocks, concurrent access is safe.
	// Verify budget invariant holds.
	if used := c.UsedBytes(); used > 1000 {
		t.Errorf("UsedBytes()=%d, exceeds budget after concurrent access", used)
	}
}

func TestBudgetedL1_NoGoroutineLeak(t *testing.T) {
	runtime.GC()
	before := runtime.NumGoroutine()

	l2 := newMockL2()
	for cycle := 0; cycle < 100; cycle++ {
		c := NewBudgetedL1(100, l2)
		for i := 0; i < 50; i++ {
			c.Put(fmt.Sprintf("k%d", i), make([]byte, 20))
		}
		// Read some to trigger promotions.
		for i := 0; i < 25; i++ {
			c.Get(fmt.Sprintf("k%d", i))
		}
	}

	runtime.GC()
	after := runtime.NumGoroutine()

	if diff := after - before; diff > 5 {
		t.Errorf("goroutine leak: before=%d after=%d delta=%d", before, after, diff)
	}
}

func TestBudgetedL1_NoMemoryLeak(t *testing.T) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	l2 := newMockL2()
	c := NewBudgetedL1(500, l2)

	for i := 0; i < 1000; i++ {
		key := fmt.Sprintf("key-%d", i)
		c.Put(key, make([]byte, 100))
	}

	// Force GC so evicted entries are collected.
	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	const maxGrowth = 10 * 1024 * 1024 // 10 MB
	growth := int64(after.HeapInuse) - int64(before.HeapInuse)

	if growth > maxGrowth {
		t.Errorf("memory leak: heap grew by %d bytes (limit %d)", growth, maxGrowth)
	}
}

func TestBudgetedL1_NilL2(t *testing.T) {
	// A nil L2 must not panic on eviction.
	c := NewBudgetedL1(40, nil)

	c.Put("a", make([]byte, 30))
	c.Put("b", make([]byte, 30))

	// "a" was evicted, "b" remains. No panic.
	if _, ok := c.Get("a"); ok {
		t.Error("key 'a' should have been evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("key 'b' should still be in L1")
	}
}
