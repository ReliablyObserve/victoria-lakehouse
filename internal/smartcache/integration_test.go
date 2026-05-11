package smartcache

import (
	"context"
	"testing"
	"time"
)

func TestIntegration_GetPinEvict(t *testing.T) {
	l1 := newMockL1()
	l2 := newMockL2()
	s3 := newMockS3()
	meta := NewMetadataMap()

	s3.data["file-a"] = []byte("data-a")
	s3.data["file-b"] = []byte("data-b")

	ctrl := NewController(ControllerConfig{
		L1:           l1,
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    s3,
		Metadata:     meta,
		MaxAge:       100 * time.Millisecond,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		GracePeriod:  10 * time.Minute,
		Signal:       "logs",
	})

	ctx := context.Background()

	// 1. Get file-a — downloads from S3
	data, err := ctrl.Get(ctx, "file-a", 6)
	if err != nil {
		t.Fatalf("Get file-a: %v", err)
	}
	if string(data) != "data-a" {
		t.Errorf("file-a data = %q, want %q", string(data), "data-a")
	}

	// 2. Pin file-a
	ctrl.Pin("file-a", "query-1")

	// 3. Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)

	// 4. Run eviction — file-a should survive because it's pinned
	evicted := ctrl.RunEvictionOnce()
	if contains(evicted, "file-a") {
		t.Error("file-a should not be evicted while pinned")
	}

	// 5. Unpin file-a
	ctrl.Unpin("file-a", "query-1")

	// 6. Run eviction again — file-a should now be evicted (TTL already expired)
	evicted = ctrl.RunEvictionOnce()
	if !contains(evicted, "file-a") {
		t.Error("file-a should be evicted after unpin + expired TTL")
	}

	// 7. Clear L1 to simulate bounded memory eviction, then re-Get from S3
	delete(l1.data, "file-a")
	data, err = ctrl.Get(ctx, "file-a", 6)
	if err != nil {
		t.Fatalf("re-Get file-a: %v", err)
	}
	if string(data) != "data-a" {
		t.Errorf("re-Get data = %q, want %q", string(data), "data-a")
	}
	if s3.calls.Load() != 2 {
		t.Errorf("S3 calls = %d, want 2 (original + re-download)", s3.calls.Load())
	}
}

func TestIntegration_TraceIDRecordAndDeprioritize(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("log-file", EntryMeta{
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		Signal:     "logs",
		Size:       100,
		TraceIDs:   []string{"trace-abc", "trace-def"},
	})
	meta.Set("unrelated-file", EntryMeta{
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		Signal:     "logs",
		Size:       200,
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
	if n != 1 {
		t.Errorf("deprioritized = %d, want 1", n)
	}

	got, _ := meta.Get("log-file")
	if got.AccessCount != 0 {
		t.Errorf("access count = %d, want 0", got.AccessCount)
	}
	if !got.LastAccess.IsZero() {
		t.Error("expected LastAccess to be zeroed")
	}

	unrelated, _ := meta.Get("unrelated-file")
	if unrelated.LastAccess.IsZero() {
		t.Error("unrelated file should not be affected")
	}
}

func TestIntegration_SizingCalculator(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	calc.RecordIngestion(2 * 1024 * 1024 * 1024)
	calc.SetIngestionInterval(1 * time.Hour)

	est := calc.RecommendedPerNode(0, 3)
	expectedPerNode := int64(2 * 1024 * 1024 * 1024 * 24 / 3)
	tolerance := int64(float64(expectedPerNode) * 0.01)
	if est < expectedPerNode-tolerance || est > expectedPerNode+tolerance {
		t.Errorf("per-node estimate = %d, want ~%d", est, expectedPerNode)
	}

	for i := 0; i < 100; i++ {
		calc.RecordQueryRead(int64(i), 10*1024*1024)
	}

	est12 := calc.RecommendedPerNode(12*time.Hour, 3)
	queryPerNode := int64(100 * 10 * 1024 * 1024 / 3)
	tolerance = int64(float64(queryPerNode) * 0.01)
	if est12 < queryPerNode-tolerance || est12 > queryPerNode+tolerance {
		t.Errorf("12h per-node estimate = %d, want ~%d", est12, queryPerNode)
	}
}

func TestController_ImplementsEvictionRouter(t *testing.T) {
	meta := NewMetadataMap()
	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	n := ctrl.DeprioritizeByTraceIDs([]string{"test"})
	_ = n
}
