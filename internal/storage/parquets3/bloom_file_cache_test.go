package parquets3

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
)

// TestBloomFileCache_EvictsOldestPastLimit verifies the bounded LRU
// semantics: once we exceed maxItems, the oldest entries are evicted.
func TestBloomFileCache_EvictsOldestPastLimit(t *testing.T) {
	const cap = 1024
	c := NewBloomFileCache(cap)

	// Insert cap+10 distinct keys. First 10 should be evicted.
	for i := 0; i < cap+10; i++ {
		key := fmt.Sprintf("file-%d.bloom", i)
		c.Put(key, &bloomindex.Index{})
	}

	if c.Len() > cap {
		t.Fatalf("cache size %d exceeded cap %d", c.Len(), cap)
	}

	// First 10 (oldest) should be evicted.
	for i := 0; i < 10; i++ {
		key := fmt.Sprintf("file-%d.bloom", i)
		if _, ok := c.Get(key); ok {
			t.Errorf("expected key %q to be evicted, but still present", key)
		}
	}

	// Most recent should still be present.
	for i := cap; i < cap+10; i++ {
		key := fmt.Sprintf("file-%d.bloom", i)
		if _, ok := c.Get(key); !ok {
			t.Errorf("expected key %q to be present", key)
		}
	}
}

// TestBloomFileCache_NilSentinel verifies storing nil (negative cache
// for missing/failed bloom files) round-trips correctly.
func TestBloomFileCache_NilSentinel(t *testing.T) {
	c := NewBloomFileCache(16)
	c.Put("missing.bloom", nil)
	got, ok := c.Get("missing.bloom")
	if !ok {
		t.Fatal("expected nil entry to be retrievable")
	}
	if got != nil {
		t.Fatalf("expected nil index, got %v", got)
	}
}

// TestBloomFileCache_DefaultCap verifies that passing <=0 yields a
// non-zero default capacity.
func TestBloomFileCache_DefaultCap(t *testing.T) {
	c := NewBloomFileCache(0)
	if c.maxItems <= 0 {
		t.Fatalf("expected positive default capacity, got %d", c.maxItems)
	}
}
