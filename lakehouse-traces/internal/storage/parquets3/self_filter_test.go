package parquets3

import (
	"context"
	"runtime"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/smartcache"
)

// mockPeerLookup implements smartcache.PeerLookup for testing self-filtering.
// localKeys defines which keys this node "owns".
type mockPeerLookup struct {
	localKeys map[string]bool
}

func (m *mockPeerLookup) Lookup(key string) (string, bool) {
	if m.localKeys[key] {
		return "self", true
	}
	return "peer-1", false
}
func (m *mockPeerLookup) Members() []string { return []string{"self", "peer-1"} }
func (m *mockPeerLookup) MemberCount() int  { return 2 }

// mockL1 implements smartcache.L1Cache as a no-op.
type mockL1 struct{}

func (m *mockL1) Get(string) ([]byte, bool) { return nil, false }
func (m *mockL1) Put(string, []byte)        {}
func (m *mockL1) PutNoCopy(string, []byte)  {}

// mockL2 implements smartcache.L2Cache as a no-op.
type mockL2 struct{}

func (m *mockL2) Get(string) ([]byte, bool) { return nil, false }
func (m *mockL2) Put(string, []byte) error  { return nil }
func (m *mockL2) Delete(string)             {}
func (m *mockL2) Size() int64               { return 0 }

// mockS3Fetcher implements smartcache.S3Fetcher as a no-op.
type mockS3Fetcher struct{}

func (m *mockS3Fetcher) Download(_ context.Context, _ string) ([]byte, error) {
	return nil, nil
}

// newSmartCacheWithLocalKeys creates a smartcache.Controller where the given
// keys are considered locally owned.
func newSmartCacheWithLocalKeys(localKeys []string) *smartcache.Controller {
	keyMap := make(map[string]bool, len(localKeys))
	for _, k := range localKeys {
		keyMap[k] = true
	}
	return smartcache.NewController(smartcache.ControllerConfig{
		L1:          &mockL1{},
		L2:          &mockL2{},
		PeerLookup:  &mockPeerLookup{localKeys: keyMap},
		S3Fetcher:   &mockS3Fetcher{},
		Metadata:    smartcache.NewMetadataMap(),
		GracePeriod: 5 * time.Minute,
	})
}

func TestSelfFilterEnabled_DefaultFalse(t *testing.T) {
	s := newMinimalStorage()
	if s.selfFilterEnabled {
		t.Fatal("selfFilterEnabled should default to false")
	}
}

func TestSetSelfFilterEnabled(t *testing.T) {
	s := newMinimalStorage()

	s.SetSelfFilterEnabled(true)
	if !s.selfFilterEnabled {
		t.Fatal("selfFilterEnabled should be true after SetSelfFilterEnabled(true)")
	}

	s.SetSelfFilterEnabled(false)
	if s.selfFilterEnabled {
		t.Fatal("selfFilterEnabled should be false after SetSelfFilterEnabled(false)")
	}
}

func TestSelfFilter_DisabledDoesNotFilter(t *testing.T) {
	s := newMinimalStorage()
	s.selfFilterEnabled = false
	s.smartCache = newSmartCacheWithLocalKeys([]string{"file-a.parquet"})

	// Simulate the self-filtering logic inline to test the filtering behavior
	// without needing a full RunQuery setup.
	files := []string{"file-a.parquet", "file-b.parquet", "file-c.parquet"}

	var filtered []string
	if s.selfFilterEnabled && s.smartCache != nil {
		for _, f := range files {
			if _, isLocal := s.smartCache.LookupOwner(f); isLocal {
				filtered = append(filtered, f)
			}
		}
		if len(filtered) > 0 {
			files = filtered
		}
	}

	if len(files) != 3 {
		t.Errorf("expected 3 files (no filtering), got %d", len(files))
	}
}

func TestSelfFilter_NoSmartCache_DoesNotFilter(t *testing.T) {
	s := newMinimalStorage()
	s.selfFilterEnabled = true
	s.smartCache = nil

	files := []string{"file-a.parquet", "file-b.parquet", "file-c.parquet"}

	var filtered []string
	if s.selfFilterEnabled && s.smartCache != nil {
		for _, f := range files {
			if _, isLocal := s.smartCache.LookupOwner(f); isLocal {
				filtered = append(filtered, f)
			}
		}
		if len(filtered) > 0 {
			files = filtered
		}
	}

	if len(files) != 3 {
		t.Errorf("expected 3 files (no filtering), got %d", len(files))
	}
}

func TestSelfFilter_EnabledFiltersToOwned(t *testing.T) {
	s := newMinimalStorage()
	s.selfFilterEnabled = true
	s.smartCache = newSmartCacheWithLocalKeys([]string{"file-a.parquet", "file-c.parquet"})

	files := []string{"file-a.parquet", "file-b.parquet", "file-c.parquet"}

	var filtered []string
	if s.selfFilterEnabled && s.smartCache != nil {
		for _, f := range files {
			if _, isLocal := s.smartCache.LookupOwner(f); isLocal {
				filtered = append(filtered, f)
			}
		}
		if len(filtered) > 0 {
			files = filtered
		}
	}

	if len(files) != 2 {
		t.Fatalf("expected 2 files after self-filter, got %d", len(files))
	}
	if files[0] != "file-a.parquet" || files[1] != "file-c.parquet" {
		t.Errorf("unexpected filtered files: %v", files)
	}
}

func TestSelfFilter_AllRemote_KeepsOriginal(t *testing.T) {
	s := newMinimalStorage()
	s.selfFilterEnabled = true
	// No local keys -- all remote.
	s.smartCache = newSmartCacheWithLocalKeys(nil)

	files := []string{"file-a.parquet", "file-b.parquet"}

	var filtered []string
	if s.selfFilterEnabled && s.smartCache != nil {
		for _, f := range files {
			if _, isLocal := s.smartCache.LookupOwner(f); isLocal {
				filtered = append(filtered, f)
			}
		}
		if len(filtered) > 0 {
			files = filtered
		}
	}

	// When no files are owned, the original list is preserved (fallback).
	if len(files) != 2 {
		t.Errorf("expected 2 files (all-remote fallback), got %d", len(files))
	}
}

func TestSelfFilter_NoGoroutineLeak(t *testing.T) {
	// Baseline goroutine count.
	runtime.GC()
	baseline := runtime.NumGoroutine()

	for i := 0; i < 100; i++ {
		s := newMinimalStorage()
		s.SetSelfFilterEnabled(true)
		s.smartCache = newSmartCacheWithLocalKeys([]string{"key-" + string(rune('a'+i%26))})
		s.SetSelfFilterEnabled(false)
		s.smartCache = nil
	}

	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	after := runtime.NumGoroutine()

	// Allow a small margin for runtime goroutines.
	if after > baseline+5 {
		t.Errorf("goroutine leak: baseline=%d, after=%d (delta=%d)", baseline, after, after-baseline)
	}
}

func TestSelfFilter_NoMemoryLeak(t *testing.T) {
	runtime.GC()
	var before runtime.MemStats
	runtime.ReadMemStats(&before)

	for i := 0; i < 1000; i++ {
		s := newMinimalStorage()
		s.SetSelfFilterEnabled(true)
		s.smartCache = newSmartCacheWithLocalKeys([]string{"key-1", "key-2", "key-3"})
		s.SetSelfFilterEnabled(false)
		s.smartCache = nil
	}

	runtime.GC()
	var after runtime.MemStats
	runtime.ReadMemStats(&after)

	// Check that heap growth is under 10MB.
	growth := int64(after.HeapAlloc) - int64(before.HeapAlloc)
	const maxGrowthBytes = 10 * 1024 * 1024
	if growth > maxGrowthBytes {
		t.Errorf("memory leak: heap grew by %d bytes (limit %d)", growth, maxGrowthBytes)
	}
}
