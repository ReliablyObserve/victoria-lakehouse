package parquets3

import (
	"fmt"
	"sync"
	"testing"
)

// fakeFooterWeight returns a CachedFooter whose cache charge is exactly w
// bytes: a raw tail of w-cachedFooterOverhead bytes and no footerSize.
func fakeFooterWeight(t testing.TB, w int64) *CachedFooter {
	t.Helper()
	if w <= cachedFooterOverhead {
		t.Fatalf("weight %d must exceed the fixed overhead %d", w, cachedFooterOverhead)
	}
	n := w - cachedFooterOverhead
	cf := &CachedFooter{FileSize: 1 << 30, tail: make([]byte, n), tailOff: 1<<30 - n}
	if got := cf.Weight(); got != w {
		t.Fatalf("fake footer weight = %d, want %d", got, w)
	}
	return cf
}

const unitW = cachedFooterOverhead + 2000 // one fake entry

func TestFooterCache_BasicOps(t *testing.T) {
	fc := NewFooterCache(3 * unitW)

	if fc.Len() != 0 {
		t.Fatalf("expected empty cache, got %d", fc.Len())
	}

	fc.Put("key1", fakeFooterWeight(t, unitW))
	fc.Put("key2", fakeFooterWeight(t, unitW))
	fc.Put("key3", fakeFooterWeight(t, unitW))

	if fc.Len() != 3 || fc.Bytes() != 3*unitW {
		t.Fatalf("expected 3 entries / %d bytes, got %d / %d", 3*unitW, fc.Len(), fc.Bytes())
	}
	if _, ok := fc.Get("key1"); !ok {
		t.Fatal("expected key1")
	}
	if _, ok := fc.Get("nonexistent"); ok {
		t.Fatal("expected miss for nonexistent key")
	}
}

func TestFooterCache_Eviction(t *testing.T) {
	fc := NewFooterCache(2 * unitW)
	fc.Put("a", fakeFooterWeight(t, unitW))
	fc.Put("b", fakeFooterWeight(t, unitW))
	fc.Put("c", fakeFooterWeight(t, unitW))

	if fc.Len() != 2 {
		t.Fatalf("expected 2 entries after eviction, got %d", fc.Len())
	}
	if _, ok := fc.Get("a"); ok {
		t.Fatal("expected 'a' to be evicted (LRU)")
	}
	if _, ok := fc.Get("b"); !ok {
		t.Fatal("expected 'b' to still be present")
	}
	if _, ok := fc.Get("c"); !ok {
		t.Fatal("expected 'c' to still be present")
	}
}

func TestFooterCache_LRUOrder(t *testing.T) {
	fc := NewFooterCache(2 * unitW)
	fc.Put("a", fakeFooterWeight(t, unitW))
	fc.Put("b", fakeFooterWeight(t, unitW))
	fc.Get("a") // make 'a' most recently used
	fc.Put("c", fakeFooterWeight(t, unitW))

	if _, ok := fc.Get("b"); ok {
		t.Fatal("expected 'b' to be evicted after 'a' was accessed")
	}
	if _, ok := fc.Get("a"); !ok {
		t.Fatal("expected 'a' to still be present (was recently accessed)")
	}
}

func TestFooterCache_Remove(t *testing.T) {
	fc := NewFooterCache(10 * unitW)
	fc.Put("x", fakeFooterWeight(t, unitW))
	fc.Remove("x")
	if fc.Len() != 0 || fc.Bytes() != 0 {
		t.Fatalf("expected empty after remove, got %d entries / %d bytes", fc.Len(), fc.Bytes())
	}
	if _, ok := fc.Get("x"); ok {
		t.Fatal("expected miss after remove")
	}
}

func TestFooterCache_Update(t *testing.T) {
	fc := NewFooterCache(10 * unitW)
	fc.Put("x", fakeFooterWeight(t, unitW))
	fc.Put("x", fakeFooterWeight(t, 2*unitW)) // replace with a heavier entry
	if fc.Len() != 1 {
		t.Fatalf("expected 1 entry after update, got %d", fc.Len())
	}
	if fc.Bytes() != 2*unitW {
		t.Fatalf("bytes after replace = %d, want %d (old charge released)", fc.Bytes(), 2*unitW)
	}
}

// The cache is bounded by BYTES: one heavy footer evicts several light ones,
// and the entry count is whatever fits — the opposite of an entry bound.
func TestFooterCache_ByteBoundNotEntryBound(t *testing.T) {
	fc := NewFooterCache(10 * unitW)
	for i := 0; i < 10; i++ {
		fc.Put(fmt.Sprintf("light-%d", i), fakeFooterWeight(t, unitW))
	}
	if fc.Len() != 10 {
		t.Fatalf("10 light entries must fit exactly, got %d", fc.Len())
	}
	// One entry as heavy as 6 light ones evicts the 6 oldest.
	fc.Put("heavy", fakeFooterWeight(t, 6*unitW))
	if fc.Bytes() > fc.MaxBytes() {
		t.Fatalf("resident bytes %d exceed budget %d", fc.Bytes(), fc.MaxBytes())
	}
	if _, ok := fc.Get("heavy"); !ok {
		t.Fatal("heavy entry must be cached")
	}
	for i := 0; i < 6; i++ {
		if fc.Has(fmt.Sprintf("light-%d", i)) {
			t.Fatalf("light-%d should have been evicted by bytes", i)
		}
	}
	for i := 6; i < 10; i++ {
		if !fc.Has(fmt.Sprintf("light-%d", i)) {
			t.Fatalf("light-%d should have survived", i)
		}
	}
	if fc.Len() != 5 {
		t.Fatalf("entries = %d, want 5 (4 light + 1 heavy)", fc.Len())
	}
}

// An entry bigger than the whole budget is not cached, and a stale entry for
// the same key is removed rather than left to be served.
func TestFooterCache_OversizeEntryDropped(t *testing.T) {
	fc := NewFooterCache(4 * unitW)
	fc.Put("k", fakeFooterWeight(t, unitW))
	fc.Put("k", fakeFooterWeight(t, 5*unitW))
	if fc.Has("k") || fc.Len() != 0 || fc.Bytes() != 0 {
		t.Fatalf("oversize put must drop the entry: has=%v len=%d bytes=%d", fc.Has("k"), fc.Len(), fc.Bytes())
	}
}

func TestFooterCache_DefaultBudget(t *testing.T) {
	for _, in := range []int64{0, -5} {
		if got := NewFooterCache(in).MaxBytes(); got != defaultFooterMaxBytes {
			t.Fatalf("NewFooterCache(%d).MaxBytes() = %d, want default %d", in, got, defaultFooterMaxBytes)
		}
	}
	if got := NewFooterCache(12345).MaxBytes(); got != 12345 {
		t.Fatalf("explicit budget lost: %d", got)
	}
}

// Concurrent Put/Get/Remove must keep the accounting exact: after the dust
// settles the charged bytes equal the sum of the surviving entries and never
// exceeded the budget. Run with -race.
func TestFooterCache_ConcurrentByteAccounting(t *testing.T) {
	const budget = 40 * unitW
	fc := NewFooterCache(budget)
	var wg sync.WaitGroup
	for g := 0; g < 16; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			for i := 0; i < 2000; i++ {
				key := fmt.Sprintf("k-%d", (g*7+i)%97)
				switch i % 5 {
				case 0, 1, 2:
					fc.Put(key, fakeFooterWeight(t, unitW+int64(i%4)*1000))
				case 3:
					fc.Get(key)
				default:
					fc.Remove(key)
				}
				if b := fc.Bytes(); b > budget {
					t.Errorf("resident %d exceeds budget %d", b, budget)
					return
				}
			}
		}(g)
	}
	wg.Wait()

	var sum int64
	for _, k := range fc.Keys() {
		cf, ok := fc.Get(k)
		if !ok {
			t.Fatalf("key %s listed but not gettable", k)
		}
		sum += cf.Weight()
	}
	if sum != fc.Bytes() {
		t.Fatalf("accounting drift: entries sum to %d, cache charges %d", sum, fc.Bytes())
	}
	if len(fc.Keys()) != fc.Len() {
		t.Fatalf("Keys/Len mismatch: %d vs %d", len(fc.Keys()), fc.Len())
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
