package parquets3

import (
	"os"
	"path/filepath"
	"testing"
)

// TestFooterCacheSnapshot_RoundTrip pins the on-disk format. Three
// keys go in, the snapshot is written and read back, and the loaded
// list matches in most-recently-used order (head of the LRU first).
// Without ordering preserved, the next pod's async prefetch would
// hydrate older files first and pay the cold S3 cost on the most
// recent ones — exactly the case we're trying to avoid.
func TestFooterCacheSnapshot_RoundTrip(t *testing.T) {
	fc := NewFooterCache(10)
	// Inserting in reverse so the LRU front ends up with "c" first.
	fc.Put("a", &CachedFooter{FileSize: 100})
	fc.Put("b", &CachedFooter{FileSize: 200})
	fc.Put("c", &CachedFooter{FileSize: 300})

	path := filepath.Join(t.TempDir(), "snapshot.bin")
	if err := SaveFooterCacheKeys(fc, path); err != nil {
		t.Fatalf("SaveFooterCacheKeys: %v", err)
	}

	keys, err := LoadFooterCacheKeys(path)
	if err != nil {
		t.Fatalf("LoadFooterCacheKeys: %v", err)
	}

	want := []string{"c", "b", "a"} // LRU order: newest first
	if len(keys) != len(want) {
		t.Fatalf("loaded %d keys, want %d", len(keys), len(want))
	}
	for i := range want {
		if keys[i] != want[i] {
			t.Errorf("loaded[%d] = %q, want %q (LRU order violated)", i, keys[i], want[i])
		}
	}
}

// TestFooterCacheSnapshot_MissingFileNoError pins the cold-start
// contract: a non-existent snapshot is a clean (nil, nil), not an
// error. The lifecycle code treats absence as "no prior cache" and
// falls through to the slow first-hit path on every key.
func TestFooterCacheSnapshot_MissingFileNoError(t *testing.T) {
	keys, err := LoadFooterCacheKeys("/nonexistent/path/snapshot.bin")
	if err != nil {
		t.Errorf("LoadFooterCacheKeys missing file = %v, want nil — cold-start regression", err)
	}
	if len(keys) != 0 {
		t.Errorf("LoadFooterCacheKeys missing file = %d keys, want 0", len(keys))
	}
}

// TestFooterCacheSnapshot_EmptyCache verifies the no-op write path:
// a cache with no entries still produces a valid snapshot (magic +
// version + count=0), so the next process loads cleanly. Without
// this case the loader would see a truncated file on day-one ops
// where the pod is bounced before any query populates the cache.
func TestFooterCacheSnapshot_EmptyCache(t *testing.T) {
	fc := NewFooterCache(10)
	path := filepath.Join(t.TempDir(), "snapshot.bin")
	if err := SaveFooterCacheKeys(fc, path); err != nil {
		t.Fatalf("SaveFooterCacheKeys empty: %v", err)
	}
	keys, err := LoadFooterCacheKeys(path)
	if err != nil {
		t.Errorf("LoadFooterCacheKeys empty-cache snapshot = %v, want nil", err)
	}
	if len(keys) != 0 {
		t.Errorf("got %d keys from empty-cache snapshot, want 0", len(keys))
	}
}

// TestFooterCacheSnapshot_TruncatedFileRejectsCleanly guards against
// a half-written snapshot: a crash mid-write must not poison the
// next start with a partial decode. The reader should error
// rather than return an incomplete key list.
func TestFooterCacheSnapshot_TruncatedFileRejectsCleanly(t *testing.T) {
	fc := NewFooterCache(10)
	fc.Put("a", &CachedFooter{FileSize: 100})
	fc.Put("b", &CachedFooter{FileSize: 200})

	path := filepath.Join(t.TempDir(), "snapshot.bin")
	if err := SaveFooterCacheKeys(fc, path); err != nil {
		t.Fatalf("SaveFooterCacheKeys: %v", err)
	}

	// Truncate the file to just past the header (magic + count).
	if err := os.Truncate(path, int64(len(footerCacheSnapshotMagic))+8); err != nil {
		t.Fatalf("Truncate: %v", err)
	}

	_, err := LoadFooterCacheKeys(path)
	if err == nil {
		t.Error("LoadFooterCacheKeys truncated file = nil error, want error — corruption must surface, not silent partial decode")
	}
}

// TestFooterCacheSnapshot_RejectsBadMagic guards against loading a
// file that isn't a footer-cache snapshot (operator pointed the
// flag at the wrong path, leftover file from an unrelated tool).
// The magic check must fail rather than treat random bytes as a
// key count and OOM on the allocation.
func TestFooterCacheSnapshot_RejectsBadMagic(t *testing.T) {
	path := filepath.Join(t.TempDir(), "garbage.bin")
	if err := os.WriteFile(path, []byte("not a snapshot, random bytes etc"), 0o600); err != nil {
		t.Fatalf("WriteFile: %v", err)
	}
	_, err := LoadFooterCacheKeys(path)
	if err == nil {
		t.Error("LoadFooterCacheKeys bad-magic file = nil error, want error")
	}
}
