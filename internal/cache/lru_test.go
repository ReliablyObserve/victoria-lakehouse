package cache

import (
	"fmt"
	"testing"
)

func TestLRU_PutGet(t *testing.T) {
	c := NewLRU(1024)
	c.Put("k1", []byte("hello"))
	val, ok := c.Get("k1")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(val) != "hello" {
		t.Errorf("got %q, want %q", val, "hello")
	}
}

func TestLRU_GetMiss(t *testing.T) {
	c := NewLRU(1024)
	_, ok := c.Get("missing")
	if ok {
		t.Error("expected miss")
	}
}

// TestLRU_GetReturnsSharedBuffer documents the share-by-reference contract:
// Get returns the cache-owned slice directly, so mutating it mutates the
// cached value. This is a deliberate change to avoid per-Get allocations
// that summed to >1 GiB heap pressure on 24h wildcard log queries (see
// the LRU.Get docstring and the OOM regression locked in
// internal/storage/parquets3/query_memory_budget_test.go).
func TestLRU_GetReturnsSharedBuffer(t *testing.T) {
	c := NewLRU(1024)
	c.Put("k1", []byte("original"))
	val, _ := c.Get("k1")
	// Mutating val mutates the cached buffer — this documents the contract;
	// real call sites must not mutate Get results.
	val[0] = 'X'
	val2, _ := c.Get("k1")
	if string(val2) != "Xriginal" {
		t.Errorf("Get should return the shared cache buffer, got %q", val2)
	}
}

func TestLRU_PutOverwrite(t *testing.T) {
	c := NewLRU(1024)
	c.Put("k1", []byte("v1"))
	c.Put("k1", []byte("v2"))

	val, ok := c.Get("k1")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(val) != "v2" {
		t.Errorf("got %q, want %q", val, "v2")
	}
	if c.Len() != 1 {
		t.Errorf("len = %d, want 1", c.Len())
	}
}

func TestLRU_Eviction(t *testing.T) {
	c := NewLRU(100)

	c.Put("a", make([]byte, 60))
	c.Put("b", make([]byte, 60))

	if _, ok := c.Get("a"); ok {
		t.Error("'a' should be evicted")
	}
	if _, ok := c.Get("b"); !ok {
		t.Error("'b' should exist")
	}
}

func TestLRU_LRUOrder(t *testing.T) {
	c := NewLRU(150)

	c.Put("a", make([]byte, 50))
	c.Put("b", make([]byte, 50))
	c.Put("c", make([]byte, 50))

	c.Get("a")

	c.Put("d", make([]byte, 50))

	if _, ok := c.Get("a"); !ok {
		t.Error("'a' should survive (recently accessed)")
	}
	if _, ok := c.Get("b"); ok {
		t.Error("'b' should be evicted (LRU)")
	}
}

func TestLRU_Delete(t *testing.T) {
	c := NewLRU(1024)
	c.Put("k1", []byte("v1"))
	c.Delete("k1")

	if _, ok := c.Get("k1"); ok {
		t.Error("deleted key should not exist")
	}
	if c.Len() != 0 {
		t.Errorf("len = %d, want 0", c.Len())
	}
}

func TestLRU_DeleteNonexistent(t *testing.T) {
	c := NewLRU(1024)
	c.Delete("missing")
}

func TestLRU_Clear(t *testing.T) {
	c := NewLRU(1024)
	c.Put("k1", []byte("v1"))
	c.Put("k2", []byte("v2"))
	c.Clear()

	if c.Len() != 0 {
		t.Errorf("len after clear = %d, want 0", c.Len())
	}
	if c.Size() != 0 {
		t.Errorf("size after clear = %d, want 0", c.Size())
	}
}

func TestLRU_Size(t *testing.T) {
	c := NewLRU(1024)
	c.Put("k1", []byte("12345"))
	if c.Size() != 5 {
		t.Errorf("size = %d, want 5", c.Size())
	}
	c.Put("k2", []byte("abc"))
	if c.Size() != 8 {
		t.Errorf("size = %d, want 8", c.Size())
	}
}

func TestLRU_MaxSize(t *testing.T) {
	c := NewLRU(999)
	if c.MaxSize() != 999 {
		t.Errorf("max size = %d, want 999", c.MaxSize())
	}
}

func TestLRU_Stats(t *testing.T) {
	c := NewLRU(1024)
	c.Put("k1", []byte("v1"))
	c.Get("k1")
	c.Get("missing")

	stats := c.Stats()
	if stats.Entries != 1 {
		t.Errorf("entries = %d, want 1", stats.Entries)
	}
	if stats.Hits != 1 {
		t.Errorf("hits = %d, want 1", stats.Hits)
	}
	if stats.Misses != 1 {
		t.Errorf("misses = %d, want 1", stats.Misses)
	}
	if stats.MaxSize != 1024 {
		t.Errorf("max size = %d, want 1024", stats.MaxSize)
	}
}

func TestLRU_EvictionStats(t *testing.T) {
	c := NewLRU(50)
	c.Put("a", make([]byte, 30))
	c.Put("b", make([]byte, 30))

	stats := c.Stats()
	if stats.Evictions == 0 {
		t.Error("expected evictions > 0")
	}
}

func TestLRU_ManyItems(t *testing.T) {
	c := NewLRU(10000)
	for i := 0; i < 200; i++ {
		c.Put(fmt.Sprintf("key-%d", i), []byte(fmt.Sprintf("val-%d", i)))
	}
	if c.Len() != 200 {
		t.Errorf("len = %d, want 200", c.Len())
	}
}
