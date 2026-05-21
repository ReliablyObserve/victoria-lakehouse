package parquets3

import (
	"testing"
)

func TestFooterCache_BasicOps(t *testing.T) {
	fc := NewFooterCache(3)

	if fc.Len() != 0 {
		t.Fatalf("expected empty cache, got %d", fc.Len())
	}

	cf := &CachedFooter{FileSize: 1000}
	fc.Put("key1", cf)
	fc.Put("key2", &CachedFooter{FileSize: 2000})
	fc.Put("key3", &CachedFooter{FileSize: 3000})

	if fc.Len() != 3 {
		t.Fatalf("expected 3 entries, got %d", fc.Len())
	}

	got, ok := fc.Get("key1")
	if !ok || got.FileSize != 1000 {
		t.Fatalf("expected key1 with size 1000, got ok=%v size=%d", ok, got.FileSize)
	}

	_, ok = fc.Get("nonexistent")
	if ok {
		t.Fatal("expected miss for nonexistent key")
	}
}

func TestFooterCache_Eviction(t *testing.T) {
	fc := NewFooterCache(2)

	fc.Put("a", &CachedFooter{FileSize: 1})
	fc.Put("b", &CachedFooter{FileSize: 2})
	fc.Put("c", &CachedFooter{FileSize: 3})

	if fc.Len() != 2 {
		t.Fatalf("expected 2 entries after eviction, got %d", fc.Len())
	}

	_, ok := fc.Get("a")
	if ok {
		t.Fatal("expected 'a' to be evicted (LRU)")
	}

	_, ok = fc.Get("b")
	if !ok {
		t.Fatal("expected 'b' to still be present")
	}
	_, ok = fc.Get("c")
	if !ok {
		t.Fatal("expected 'c' to still be present")
	}
}

func TestFooterCache_LRUOrder(t *testing.T) {
	fc := NewFooterCache(2)

	fc.Put("a", &CachedFooter{FileSize: 1})
	fc.Put("b", &CachedFooter{FileSize: 2})

	// Access 'a' to make it most recently used
	fc.Get("a")

	// Adding 'c' should evict 'b' (least recently used)
	fc.Put("c", &CachedFooter{FileSize: 3})

	_, ok := fc.Get("b")
	if ok {
		t.Fatal("expected 'b' to be evicted after 'a' was accessed")
	}
	_, ok = fc.Get("a")
	if !ok {
		t.Fatal("expected 'a' to still be present (was recently accessed)")
	}
}

func TestFooterCache_Remove(t *testing.T) {
	fc := NewFooterCache(10)
	fc.Put("x", &CachedFooter{FileSize: 100})
	fc.Remove("x")
	if fc.Len() != 0 {
		t.Fatal("expected empty after remove")
	}
	_, ok := fc.Get("x")
	if ok {
		t.Fatal("expected miss after remove")
	}
}

func TestFooterCache_Update(t *testing.T) {
	fc := NewFooterCache(10)
	fc.Put("x", &CachedFooter{FileSize: 100})
	fc.Put("x", &CachedFooter{FileSize: 200})
	if fc.Len() != 1 {
		t.Fatalf("expected 1 entry after update, got %d", fc.Len())
	}
	got, ok := fc.Get("x")
	if !ok || got.FileSize != 200 {
		t.Fatalf("expected updated size 200, got %d", got.FileSize)
	}
}

func TestFooterLength(t *testing.T) {
	// Valid parquet footer: 4 bytes length (little-endian) + "PAR1"
	tail := []byte{0x10, 0x00, 0x00, 0x00, 'P', 'A', 'R', '1'}
	length, err := FooterLength(tail)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if length != 16 {
		t.Fatalf("expected footer length 16, got %d", length)
	}
}

func TestFooterLength_BadMagic(t *testing.T) {
	tail := []byte{0x10, 0x00, 0x00, 0x00, 'N', 'O', 'T', '!'}
	_, err := FooterLength(tail)
	if err == nil {
		t.Fatal("expected error for bad magic")
	}
}

func TestFooterLength_TooShort(t *testing.T) {
	_, err := FooterLength([]byte{1, 2, 3})
	if err == nil {
		t.Fatal("expected error for short input")
	}
}
