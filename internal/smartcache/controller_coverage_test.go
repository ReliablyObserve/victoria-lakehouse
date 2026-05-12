package smartcache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestRunEvictionOnce_RemovesExpiredFromL2AndMetadata(t *testing.T) {
	l2 := newMockL2()
	meta := NewMetadataMap()

	// Add an expired entry
	_ = l2.Put("old-key", []byte("old-data"))
	meta.Set("old-key", EntryMeta{
		CreatedAt:         time.Now().Add(-3 * time.Hour),
		LastAccess:        time.Now().Add(-3 * time.Hour),
		AccessCount:       1,
		AccessWindowStart: time.Now().Add(-3 * time.Hour),
		Signal:            "logs",
		Size:              8,
	})

	// Add a fresh entry that should survive
	_ = l2.Put("new-key", []byte("new-data"))
	meta.Set("new-key", EntryMeta{
		CreatedAt:         time.Now(),
		LastAccess:        time.Now(),
		AccessCount:       1,
		AccessWindowStart: time.Now(),
		Signal:            "logs",
		Size:              8,
	})

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       1 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	evicted := ctrl.RunEvictionOnce()

	// old-key should be evicted
	foundOld := false
	for _, k := range evicted {
		if k == "old-key" {
			foundOld = true
		}
	}
	if !foundOld {
		t.Error("expected old-key to be evicted")
	}

	// old-key should be removed from L2
	if _, ok := l2.Get("old-key"); ok {
		t.Error("expected old-key to be removed from L2")
	}

	// old-key should be removed from metadata
	if _, ok := meta.Get("old-key"); ok {
		t.Error("expected old-key to be removed from metadata")
	}

	// new-key should survive
	if _, ok := meta.Get("new-key"); !ok {
		t.Error("expected new-key to survive eviction")
	}
	if _, ok := l2.Get("new-key"); !ok {
		t.Error("expected new-key to survive in L2")
	}
}

func TestRunEvictionOnce_NoExpired(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("fresh", EntryMeta{
		CreatedAt:         time.Now(),
		LastAccess:        time.Now(),
		AccessCount:       1,
		AccessWindowStart: time.Now(),
		Signal:            "logs",
		Size:              100,
	})

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

	evicted := ctrl.RunEvictionOnce()
	if len(evicted) != 0 {
		t.Errorf("expected 0 evictions, got %d", len(evicted))
	}
}

func TestStartEvictionLoop_RunsAndStops(t *testing.T) {
	l2 := newMockL2()
	meta := NewMetadataMap()

	// Add an expired entry
	_ = l2.Put("expired", []byte("data"))
	meta.Set("expired", EntryMeta{
		CreatedAt:         time.Now().Add(-2 * time.Hour),
		LastAccess:        time.Now().Add(-2 * time.Hour),
		AccessCount:       0,
		AccessWindowStart: time.Now().Add(-2 * time.Hour),
		Signal:            "logs",
		Size:              4,
	})

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       1 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	stop := make(chan struct{})
	ctrl.StartEvictionLoop(50*time.Millisecond, stop)

	// Wait for at least one tick
	time.Sleep(150 * time.Millisecond)

	// Stop the loop
	close(stop)

	// Give goroutine time to exit
	time.Sleep(50 * time.Millisecond)

	// The expired entry should have been evicted
	if _, ok := meta.Get("expired"); ok {
		t.Error("expected expired entry to be evicted by loop")
	}
}

func TestStartSnapshotLoop_CreatesFile(t *testing.T) {
	tmpDir := t.TempDir()
	snapshotPath := filepath.Join(tmpDir, "metadata.json")

	meta := NewMetadataMap()
	meta.Set("key1", EntryMeta{
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		Signal:     "logs",
		Size:       100,
	})

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

	stop := make(chan struct{})
	ctrl.StartSnapshotLoop(snapshotPath, 50*time.Millisecond, stop)

	// Wait for at least one tick to save
	time.Sleep(150 * time.Millisecond)

	// Stop the loop (triggers final save)
	close(stop)

	// Give goroutine time to exit
	time.Sleep(100 * time.Millisecond)

	// Verify snapshot file was created
	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Error("expected snapshot file to be created")
	}
}

func TestStartSnapshotLoop_EmptyPath_NoOp(t *testing.T) {
	meta := NewMetadataMap()

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

	stop := make(chan struct{})

	// With empty path, StartSnapshotLoop should return immediately (no-op)
	ctrl.StartSnapshotLoop("", 50*time.Millisecond, stop)

	// Close stop to be clean
	close(stop)

	// If we get here without hanging, the no-op path works
}

func TestDeprioritizeByTraceIDs_MultipleMatches(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("file-a", EntryMeta{
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 10,
		Signal:      "logs",
		Size:        100,
		TraceIDs:    []string{"trace-1", "trace-2"},
	})
	meta.Set("file-b", EntryMeta{
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 5,
		Signal:      "logs",
		Size:        200,
		TraceIDs:    []string{"trace-2", "trace-3"},
	})
	meta.Set("file-c", EntryMeta{
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 3,
		Signal:      "logs",
		Size:        50,
		TraceIDs:    []string{"trace-4"},
	})

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

	// trace-2 matches both file-a and file-b
	n := ctrl.DeprioritizeByTraceIDs([]string{"trace-2"})
	if n != 2 {
		t.Errorf("deprioritized = %d, want 2", n)
	}

	gotA, _ := meta.Get("file-a")
	if gotA.AccessCount != 0 {
		t.Errorf("file-a access count = %d, want 0", gotA.AccessCount)
	}
	if !gotA.LastAccess.IsZero() {
		t.Error("file-a LastAccess should be zeroed")
	}

	gotB, _ := meta.Get("file-b")
	if gotB.AccessCount != 0 {
		t.Errorf("file-b access count = %d, want 0", gotB.AccessCount)
	}

	// file-c should be unaffected
	gotC, _ := meta.Get("file-c")
	if gotC.AccessCount != 3 {
		t.Errorf("file-c access count = %d, want 3 (unaffected)", gotC.AccessCount)
	}
}

func TestDeprioritizeByTraceIDs_EmptyTraceIDs(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("file-1", EntryMeta{
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 5,
		Signal:      "logs",
		Size:        100,
		TraceIDs:    []string{"trace-abc"},
	})

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

	n := ctrl.DeprioritizeByTraceIDs(nil)
	if n != 0 {
		t.Errorf("deprioritized = %d, want 0 for nil trace IDs", n)
	}
}

func TestDeprioritizeByTraceIDs_EntryWithNoTraceIDs(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("file-no-traces", EntryMeta{
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 5,
		Signal:      "logs",
		Size:        100,
		// No TraceIDs set
	})

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

	n := ctrl.DeprioritizeByTraceIDs([]string{"trace-abc"})
	if n != 0 {
		t.Errorf("deprioritized = %d, want 0 for entry with no trace IDs", n)
	}

	got, _ := meta.Get("file-no-traces")
	if got.AccessCount != 5 {
		t.Errorf("access count should be unchanged: got %d, want 5", got.AccessCount)
	}
}

func TestController_Metadata_ReturnsMetadataMap(t *testing.T) {
	meta := NewMetadataMap()
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

	if ctrl.Metadata() != meta {
		t.Error("expected Metadata() to return the same MetadataMap")
	}
}

func TestNewController_DefaultGracePeriod(t *testing.T) {
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
		// GracePeriod not set -- should default to 5m
	})

	if ctrl.gracePeriod != 5*time.Minute {
		t.Errorf("expected default grace period 5m, got %v", ctrl.gracePeriod)
	}
}

func TestStartSnapshotLoop_FinalSaveOnStop(t *testing.T) {
	tmpDir := t.TempDir()
	snapshotPath := filepath.Join(tmpDir, "final_snapshot.json")

	meta := NewMetadataMap()
	meta.Set("final-key", EntryMeta{
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		Signal:     "logs",
		Size:       50,
	})

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

	stop := make(chan struct{})
	// Use a very long interval so the ticker won't fire
	ctrl.StartSnapshotLoop(snapshotPath, 1*time.Hour, stop)

	// Close immediately - should trigger final save
	time.Sleep(20 * time.Millisecond) // let goroutine start
	close(stop)

	// Wait for final save
	time.Sleep(100 * time.Millisecond)

	if _, err := os.Stat(snapshotPath); os.IsNotExist(err) {
		t.Error("expected final snapshot to be saved on stop")
	}
}

func TestRecordTraceIDs_MissingKey_NoOp(t *testing.T) {
	meta := NewMetadataMap()
	// No entries in metadata

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

	// Should not panic when key doesn't exist
	ctrl.RecordTraceIDs("nonexistent-key", []string{"trace-1"})

	if meta.Len() != 0 {
		t.Error("expected no entries added for missing key")
	}
}
