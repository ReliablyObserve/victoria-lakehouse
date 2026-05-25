package smartcache

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func TestRunEvictionOnce_WatermarkLRU(t *testing.T) {
	l2 := newMockL2()
	meta := NewMetadataMap()

	now := time.Now()
	// Add 3 entries totaling 600 bytes. With diskLimit=500, watermark is 450.
	// L2.Size() will be 600 > 450, so LRU should evict enough to get below 450.
	for i, name := range []string{"oldest", "middle", "newest"} {
		data := make([]byte, 200)
		_ = l2.Put(name, data)
		meta.Set(name, EntryMeta{
			CreatedAt:         now.Add(-time.Duration(3-i) * time.Hour),
			LastAccess:        now.Add(-time.Duration(3-i) * time.Minute),
			AccessCount:       1,
			AccessWindowStart: now.Add(-time.Duration(3-i) * time.Minute),
			Signal:            "logs",
			Size:              200,
		})
	}

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
		DiskLimit:    500,
	})

	evicted := ctrl.RunEvictionOnce()

	if len(evicted) == 0 {
		t.Fatal("expected watermark LRU to evict at least one entry")
	}
	if !contains(evicted, "oldest") {
		t.Error("expected 'oldest' (least recently accessed) to be evicted first")
	}
	if _, ok := l2.Get("oldest"); ok {
		t.Error("expected 'oldest' to be removed from L2")
	}
	if _, ok := meta.Get("oldest"); ok {
		t.Error("expected 'oldest' to be removed from metadata")
	}
}

func TestRunEvictionOnce_WatermarkNotTriggered(t *testing.T) {
	l2 := newMockL2()
	meta := NewMetadataMap()

	_ = l2.Put("small", make([]byte, 100))
	meta.Set("small", EntryMeta{
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		Signal:     "logs",
		Size:       100,
	})

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
		DiskLimit:    1000,
	})

	evicted := ctrl.RunEvictionOnce()
	if len(evicted) != 0 {
		t.Errorf("expected no evictions when below watermark, got %d", len(evicted))
	}
}

func TestReconcileWithMtime(t *testing.T) {
	m := NewMetadataMap()
	m.Set("existing", EntryMeta{Signal: "logs", Size: 100, CreatedAt: time.Now().Add(-time.Hour)})

	mtime := time.Date(2025, 6, 1, 12, 0, 0, 0, time.UTC)
	diskFiles := map[string]DiskFile{
		"existing":  {Size: 100, Mtime: mtime},
		"new-file":  {Size: 200, Mtime: mtime},
		"zero-time": {Size: 300},
	}

	m.ReconcileWithMtime(diskFiles)

	got, ok := m.Get("new-file")
	if !ok {
		t.Fatal("expected new-file to be added")
	}
	if !got.CreatedAt.Equal(mtime) {
		t.Errorf("new-file CreatedAt = %v, want %v (from mtime)", got.CreatedAt, mtime)
	}
	if !got.LastAccess.Equal(mtime) {
		t.Errorf("new-file LastAccess = %v, want %v (from mtime)", got.LastAccess, mtime)
	}

	gotZero, ok := m.Get("zero-time")
	if !ok {
		t.Fatal("expected zero-time to be added")
	}
	if gotZero.CreatedAt.IsZero() {
		t.Error("zero-time CreatedAt should fall back to now, not zero")
	}
}

func TestSnapshotVersioning_LoadLegacyFormat(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "legacy.json")

	now := time.Now().Truncate(time.Millisecond)
	legacy := map[string]EntryMeta{
		"file1": {
			CreatedAt:   now,
			LastAccess:  now,
			AccessCount: 3,
			Signal:      "logs",
			Size:        512,
		},
	}

	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatal(err)
	}
	if err := writeTestFile(path, data); err != nil {
		t.Fatal(err)
	}

	m := NewMetadataMap()
	if err := m.LoadSnapshot(path); err != nil {
		t.Fatalf("LoadSnapshot legacy format: %v", err)
	}
	if m.Len() != 1 {
		t.Fatalf("loaded len = %d, want 1", m.Len())
	}
	got, ok := m.Get("file1")
	if !ok {
		t.Fatal("expected file1 in loaded legacy snapshot")
	}
	if got.AccessCount != 3 {
		t.Errorf("access count = %d, want 3", got.AccessCount)
	}
}

func TestSnapshotVersioning_SaveLoadRoundtrip(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "versioned.json")

	m := NewMetadataMap()
	now := time.Now().Truncate(time.Millisecond)
	m.Set("v1-file", EntryMeta{
		CreatedAt:   now,
		LastAccess:  now,
		AccessCount: 7,
		Signal:      "traces",
		Size:        2048,
		TraceIDs:    []string{"t1", "t2"},
	})

	if err := m.SaveSnapshot(path); err != nil {
		t.Fatal(err)
	}

	raw, err := readTestFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var env snapshotEnvelope
	if err := json.Unmarshal(raw, &env); err != nil {
		t.Fatalf("unmarshal envelope: %v", err)
	}
	if env.Version != snapshotVersion {
		t.Errorf("saved version = %d, want %d", env.Version, snapshotVersion)
	}

	loaded := NewMetadataMap()
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatal(err)
	}
	got, ok := loaded.Get("v1-file")
	if !ok {
		t.Fatal("expected v1-file in loaded snapshot")
	}
	if got.AccessCount != 7 {
		t.Errorf("access count = %d, want 7", got.AccessCount)
	}
	if len(got.TraceIDs) != 2 {
		t.Errorf("trace_ids len = %d, want 2", len(got.TraceIDs))
	}
}

func TestConcurrentPinUnpin(t *testing.T) {
	meta := NewMetadataMap()
	for i := 0; i < 50; i++ {
		meta.Set(fmt.Sprintf("file-%d", i), EntryMeta{Signal: "logs", Size: 100})
	}

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		GracePeriod:  5 * time.Minute,
		Signal:       "logs",
	})

	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("file-%d", i%50)
			queryID := fmt.Sprintf("q-%d", i)
			ctrl.Pin(key, queryID)
			ctrl.Unpin(key, queryID)
		}()
	}
	wg.Wait()

	for i := 0; i < 50; i++ {
		key := fmt.Sprintf("file-%d", i)
		if _, ok := meta.Get(key); !ok {
			t.Errorf("expected %s to still exist after concurrent pin/unpin", key)
		}
	}
}

func TestFindFilesByTraceID(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("log-a", EntryMeta{Signal: "logs", Size: 100, TraceIDs: []string{"t1", "t2"}})
	meta.Set("log-b", EntryMeta{Signal: "logs", Size: 100, TraceIDs: []string{"t2", "t3"}})
	meta.Set("log-c", EntryMeta{Signal: "logs", Size: 100, TraceIDs: []string{"t4"}})
	meta.Set("log-d", EntryMeta{Signal: "logs", Size: 100})

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	keys := ctrl.FindFilesByTraceID("t2")
	if len(keys) != 2 {
		t.Fatalf("FindFilesByTraceID(t2) = %d keys, want 2", len(keys))
	}

	keys = ctrl.FindFilesByTraceID("t4")
	if len(keys) != 1 || keys[0] != "log-c" {
		t.Errorf("FindFilesByTraceID(t4) = %v, want [log-c]", keys)
	}

	keys = ctrl.FindFilesByTraceID("nonexistent")
	if len(keys) != 0 {
		t.Errorf("FindFilesByTraceID(nonexistent) = %v, want empty", keys)
	}

	keys = ctrl.FindFilesByTraceID("")
	if keys != nil {
		t.Errorf("FindFilesByTraceID('') = %v, want nil", keys)
	}
}

func TestPutL2(t *testing.T) {
	l2 := newMockL2()
	meta := NewMetadataMap()

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	data := []byte("write-through-data")
	err := ctrl.PutL2("ingest-file", data)
	if err != nil {
		t.Fatalf("PutL2: %v", err)
	}

	got, ok := l2.Get("ingest-file")
	if !ok {
		t.Fatal("expected ingest-file in L2")
	}
	if string(got) != "write-through-data" {
		t.Errorf("L2 data = %q, want %q", string(got), "write-through-data")
	}

	m, ok := meta.Get("ingest-file")
	if !ok {
		t.Fatal("expected ingest-file in metadata")
	}
	if m.Signal != "logs" {
		t.Errorf("signal = %q, want logs", m.Signal)
	}
	if m.Size != int64(len(data)) {
		t.Errorf("size = %d, want %d", m.Size, len(data))
	}
	if m.AccessCount != 1 {
		t.Errorf("access count = %d, want 1", m.AccessCount)
	}
}

func TestPutL2_NilL2(t *testing.T) {
	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           nil,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     NewMetadataMap(),
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	err := ctrl.PutL2("key", []byte("data"))
	if err != nil {
		t.Errorf("PutL2 with nil L2 should return nil, got %v", err)
	}
}

func TestLookupOwner_IsLocal(t *testing.T) {
	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     NewMetadataMap(),
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	if !ctrl.IsLocal("any-key") {
		t.Error("expected IsLocal=true for single-node mock")
	}

	peer, isLocal := ctrl.LookupOwner("any-key")
	if peer != "self:9428" || !isLocal {
		t.Errorf("LookupOwner = %s/%v, want self:9428/true", peer, isLocal)
	}
}

func TestMetadataMap_All_DeepCopy(t *testing.T) {
	m := NewMetadataMap()
	m.Set("file1", EntryMeta{
		Signal:   "logs",
		Size:     100,
		PinnedBy: map[string]time.Time{"q1": time.Now().Add(5 * time.Minute)},
		TraceIDs: []string{"t1"},
	})

	all := m.All()
	all["file1"] = EntryMeta{Signal: "mutated"}

	got, _ := m.Get("file1")
	if got.Signal != "logs" {
		t.Error("All() returned a reference instead of a deep copy — mutation propagated")
	}
}

func TestConcurrentGetAndEviction(t *testing.T) {
	l2 := newMockL2()
	s3 := newMockS3()
	meta := NewMetadataMap()

	for i := 0; i < 20; i++ {
		key := fmt.Sprintf("file-%d", i)
		s3.data[key] = []byte(fmt.Sprintf("data-%d", i))
	}

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    s3,
		Metadata:     meta,
		MaxAge:       50 * time.Millisecond,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
		DiskLimit:    1000,
	})

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 100; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			key := fmt.Sprintf("file-%d", i%20)
			_, _ = ctrl.Get(ctx, key, 10)
		}()
	}

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			time.Sleep(25 * time.Millisecond)
			ctrl.RunEvictionOnce()
		}()
	}

	wg.Wait()
}

func TestCollectLRU_AllPinned(t *testing.T) {
	m := NewMetadataMap()
	now := time.Now()
	m.Set("p1", EntryMeta{
		LastAccess: now.Add(-time.Hour),
		Size:       100,
		PinnedBy:   map[string]time.Time{"q1": now.Add(10 * time.Minute)},
	})
	m.Set("p2", EntryMeta{
		LastAccess: now.Add(-30 * time.Minute),
		Size:       200,
		PinnedBy:   map[string]time.Time{"q2": now.Add(10 * time.Minute)},
	})

	toEvict := CollectLRU(m, 500, 3, 10*time.Minute)
	if len(toEvict) != 0 {
		t.Errorf("expected 0 evictions when all entries pinned, got %d: %v", len(toEvict), toEvict)
	}
}

func TestCollectExpired_HotEntryRecentAccess(t *testing.T) {
	now := time.Now()
	m := NewMetadataMap()
	m.Set("hot-recent", EntryMeta{
		CreatedAt:         now.Add(-2 * time.Hour),
		LastAccess:        now.Add(-1 * time.Minute),
		AccessCount:       10,
		AccessWindowStart: now.Add(-5 * time.Minute),
		Size:              100,
	})

	expired := CollectExpired(m, 1*time.Hour, 3, 10*time.Minute)
	if len(expired) != 0 {
		t.Errorf("hot entry with recent access should not be expired, got %v", expired)
	}
}

func TestCollectExpired_HotEntryStaleAccess(t *testing.T) {
	now := time.Now()
	m := NewMetadataMap()
	m.Set("hot-stale", EntryMeta{
		CreatedAt:         now.Add(-3 * time.Hour),
		LastAccess:        now.Add(-2 * time.Hour),
		AccessCount:       10,
		AccessWindowStart: now.Add(-5 * time.Minute),
		Size:              100,
	})

	expired := CollectExpired(m, 1*time.Hour, 3, 10*time.Minute)
	if len(expired) != 1 {
		t.Errorf("hot entry with stale access should be expired, got %d", len(expired))
	}
}

func writeTestFile(path string, data []byte) error {
	return os.WriteFile(path, data, 0600)
}

func readTestFile(path string) ([]byte, error) {
	return os.ReadFile(path)
}
