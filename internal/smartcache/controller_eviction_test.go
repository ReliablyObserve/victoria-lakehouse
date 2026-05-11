package smartcache

import (
	"testing"
	"time"
)

func TestController_EvictionLoop_RemovesExpired(t *testing.T) {
	l1 := newMockL1()
	l2 := newMockL2()
	meta := NewMetadataMap()

	meta.Set("expired-key", EntryMeta{
		CreatedAt:         time.Now().Add(-2 * time.Hour),
		LastAccess:        time.Now().Add(-2 * time.Hour),
		AccessCount:       0,
		AccessWindowStart: time.Now().Add(-2 * time.Hour),
		Signal:            "logs",
		Size:              100,
	})
	l2.Put("expired-key", make([]byte, 100))

	meta.Set("fresh-key", EntryMeta{
		CreatedAt:         time.Now(),
		LastAccess:        time.Now(),
		AccessCount:       0,
		AccessWindowStart: time.Now(),
		Signal:            "logs",
		Size:              200,
	})
	l2.Put("fresh-key", make([]byte, 200))

	ctrl := NewController(ControllerConfig{
		L1:           l1,
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

	ctrl.RunEvictionOnce()

	if _, ok := meta.Get("expired-key"); ok {
		t.Error("expected expired entry to be evicted")
	}
	if _, ok := l2.Get("expired-key"); ok {
		t.Error("expected expired entry to be removed from L2")
	}
	if _, ok := meta.Get("fresh-key"); !ok {
		t.Error("expected fresh entry to survive eviction")
	}
}

func TestController_DeprioritizeByTraceIDs(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("log-file", EntryMeta{
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 5,
		Signal:      "logs",
		Size:        100,
		TraceIDs:    []string{"trace-abc", "trace-def"},
	})
	meta.Set("unrelated-file", EntryMeta{
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 3,
		Signal:      "logs",
		Size:        200,
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
		t.Errorf("access count = %d, want 0 after deprioritization", got.AccessCount)
	}
	if !got.LastAccess.IsZero() {
		t.Error("expected LastAccess to be zeroed after deprioritization")
	}

	unrelated, _ := meta.Get("unrelated-file")
	if unrelated.LastAccess.IsZero() {
		t.Error("unrelated file should not be affected")
	}
}

func TestController_DeprioritizeByTraceIDs_NoMatch(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("file-1", EntryMeta{
		CreatedAt:   time.Now(),
		LastAccess:  time.Now(),
		AccessCount: 5,
		Signal:      "logs",
		Size:        100,
		TraceIDs:    []string{"trace-xyz"},
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

	n := ctrl.DeprioritizeByTraceIDs([]string{"trace-nonexistent"})
	if n != 0 {
		t.Errorf("deprioritized = %d, want 0 for non-matching trace IDs", n)
	}

	got, _ := meta.Get("file-1")
	if got.AccessCount != 5 {
		t.Errorf("access count should be unchanged: got %d, want 5", got.AccessCount)
	}
}

func TestController_EvictionLoop_PinnedSurvives(t *testing.T) {
	l2 := newMockL2()
	meta := NewMetadataMap()

	meta.Set("pinned-key", EntryMeta{
		CreatedAt:         time.Now().Add(-2 * time.Hour),
		LastAccess:        time.Now().Add(-2 * time.Hour),
		AccessCount:       0,
		AccessWindowStart: time.Now().Add(-2 * time.Hour),
		PinnedBy:          map[string]time.Time{"query-1": time.Now().Add(10 * time.Minute)},
		Signal:            "logs",
		Size:              100,
	})
	l2.Put("pinned-key", make([]byte, 100))

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
	if contains(evicted, "pinned-key") {
		t.Error("pinned key should not be evicted")
	}
	if _, ok := meta.Get("pinned-key"); !ok {
		t.Error("pinned key should still exist in metadata")
	}
}

func contains(s []string, v string) bool {
	for _, item := range s {
		if item == v {
			return true
		}
	}
	return false
}
