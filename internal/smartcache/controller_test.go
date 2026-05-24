package smartcache

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type mockL1 struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newMockL1() *mockL1 { return &mockL1{data: make(map[string][]byte)} }

func (m *mockL1) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.data[key]
	return d, ok
}
func (m *mockL1) Put(key string, val []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = val
}

type mockL2 struct {
	mu   sync.RWMutex
	data map[string][]byte
}

func newMockL2() *mockL2 { return &mockL2{data: make(map[string][]byte)} }

func (m *mockL2) Get(key string) ([]byte, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	d, ok := m.data[key]
	return d, ok
}
func (m *mockL2) Put(key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.data[key] = data
	return nil
}
func (m *mockL2) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.data, key)
}
func (m *mockL2) Size() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var s int64
	for _, d := range m.data {
		s += int64(len(d))
	}
	return s
}

type mockPeerLookup struct {
	selfAddr string
}

func (m *mockPeerLookup) Lookup(key string) (string, bool) {
	return m.selfAddr, true
}
func (m *mockPeerLookup) Members() []string {
	return []string{m.selfAddr}
}
func (m *mockPeerLookup) MemberCount() int { return 1 }

type mockPeerFetcher struct{}

func (m *mockPeerFetcher) Fetch(ctx context.Context, peer, key string) ([]byte, bool, error) {
	return nil, false, nil
}

type mockS3Fetcher struct {
	data  map[string][]byte
	calls atomic.Int64
	delay time.Duration
}

func newMockS3() *mockS3Fetcher {
	return &mockS3Fetcher{data: make(map[string][]byte)}
}

func (m *mockS3Fetcher) Download(ctx context.Context, key string) ([]byte, error) {
	m.calls.Add(1)
	if m.delay > 0 {
		select {
		case <-time.After(m.delay):
		case <-ctx.Done():
			return nil, ctx.Err()
		}
	}
	d, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return d, nil
}

func TestController_GetFromL1(t *testing.T) {
	l1 := newMockL1()
	l1.Put("file1", []byte("hello"))

	ctrl := NewController(ControllerConfig{
		L1:           l1,
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

	data, err := ctrl.Get(context.Background(), "file1", 5)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("data = %q, want %q", string(data), "hello")
	}
}

func TestController_GetFromL2_OwnedKey(t *testing.T) {
	l1 := newMockL1()
	l2 := newMockL2()
	_ = l2.Put("file2", []byte("disk-data"))

	meta := NewMetadataMap()
	meta.Set("file2", EntryMeta{
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		Signal:     "logs",
		Size:       9,
	})

	ctrl := NewController(ControllerConfig{
		L1:           l1,
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

	data, err := ctrl.Get(context.Background(), "file2", 9)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(data) != "disk-data" {
		t.Errorf("data = %q, want %q", string(data), "disk-data")
	}

	// Should also be promoted to L1
	if _, ok := l1.Get("file2"); !ok {
		t.Error("expected file2 to be promoted to L1 after L2 hit")
	}
}

func TestController_GetFromS3_StoresInL2(t *testing.T) {
	l1 := newMockL1()
	l2 := newMockL2()
	s3 := newMockS3()
	s3.data["file3"] = []byte("s3-data")

	ctrl := NewController(ControllerConfig{
		L1:           l1,
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    s3,
		Metadata:     NewMetadataMap(),
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	data, err := ctrl.Get(context.Background(), "file3", 7)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(data) != "s3-data" {
		t.Errorf("data = %q, want %q", string(data), "s3-data")
	}

	if _, ok := l2.Get("file3"); !ok {
		t.Error("expected file3 to be stored in L2 after S3 download")
	}
	if _, ok := l1.Get("file3"); !ok {
		t.Error("expected file3 to be stored in L1 after S3 download")
	}
	if s3.calls.Load() != 1 {
		t.Errorf("S3 calls = %d, want 1", s3.calls.Load())
	}
}

func TestController_PinUnpin(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("file1", EntryMeta{Signal: "logs", Size: 100})

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

	ctrl.Pin("file1", "query-1")

	got, _ := meta.Get("file1")
	if len(got.PinnedBy) != 1 {
		t.Fatalf("expected 1 pin, got %d", len(got.PinnedBy))
	}

	ctrl.Unpin("file1", "query-1")

	got, _ = meta.Get("file1")
	if len(got.PinnedBy) != 0 {
		t.Errorf("expected 0 pins after unpin, got %d", len(got.PinnedBy))
	}
}

func TestController_RecordTraceIDs(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("file1", EntryMeta{Signal: "logs", Size: 100})

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

	ctrl.RecordTraceIDs("file1", []string{"trace-abc", "trace-def"})

	got, _ := meta.Get("file1")
	if len(got.TraceIDs) != 2 {
		t.Errorf("trace_ids len = %d, want 2", len(got.TraceIDs))
	}
}

// --- Additional coverage tests ---

func TestController_Singleflight_Dedup(t *testing.T) {
	s3 := newMockS3()
	s3.data["dedup-key"] = []byte("data")
	s3.delay = 50 * time.Millisecond

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    s3,
		Metadata:     NewMetadataMap(),
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	var wg sync.WaitGroup
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			data, err := ctrl.Get(context.Background(), "dedup-key", 4)
			if err != nil {
				t.Errorf("Get error: %v", err)
				return
			}
			if string(data) != "data" {
				t.Errorf("data = %q, want %q", string(data), "data")
			}
		}()
	}
	wg.Wait()

	if s3.calls.Load() != 1 {
		t.Errorf("S3 calls = %d, want 1 (singleflight dedup)", s3.calls.Load())
	}
}

func TestController_S3Error_Propagation(t *testing.T) {
	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(), // empty S3 → returns "not found"
		Metadata:     NewMetadataMap(),
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	_, err := ctrl.Get(context.Background(), "nonexistent", 0)
	if err == nil {
		t.Fatal("expected error for missing S3 key")
	}
}

func TestController_ContextCancellation(t *testing.T) {
	s3 := newMockS3()
	s3.data["slow-key"] = []byte("data")
	s3.delay = 5 * time.Second // very slow

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    s3,
		Metadata:     NewMetadataMap(),
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()

	_, err := ctrl.Get(ctx, "slow-key", 4)
	if err == nil {
		t.Fatal("expected error from cancelled context")
	}
}

func TestController_NonLocalKey_FetchFromPeer(t *testing.T) {
	peerLookup := &mockNonLocalPeerLookup{peerAddr: "peer1:9428"}
	peerFetcher := &mockSuccessfulPeerFetcher{data: []byte("peer-data")}

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   peerLookup,
		PeerFetcher:  peerFetcher,
		S3Fetcher:    newMockS3(),
		Metadata:     NewMetadataMap(),
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	data, err := ctrl.Get(context.Background(), "remote-file", 9)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(data) != "peer-data" {
		t.Errorf("data = %q, want %q", string(data), "peer-data")
	}
}

// Helper mocks for additional tests
type mockNonLocalPeerLookup struct {
	peerAddr string
}

func (m *mockNonLocalPeerLookup) Lookup(key string) (string, bool) {
	return m.peerAddr, false // not local
}
func (m *mockNonLocalPeerLookup) Members() []string {
	return []string{m.peerAddr}
}
func (m *mockNonLocalPeerLookup) MemberCount() int { return 1 }

type mockSuccessfulPeerFetcher struct {
	data []byte
}

func (m *mockSuccessfulPeerFetcher) Fetch(ctx context.Context, peer, key string) ([]byte, bool, error) {
	return m.data, true, nil
}

// --- Partition mode tests ---

type mockAZPeerLookup struct {
	selfAddr string
	azPeer   string
	azLocal  bool
	sameAZ   bool
}

func (m *mockAZPeerLookup) Lookup(key string) (string, bool) {
	return m.selfAddr, true // global ring always returns self
}
func (m *mockAZPeerLookup) Members() []string  { return []string{m.selfAddr} }
func (m *mockAZPeerLookup) MemberCount() int   { return 1 }
func (m *mockAZPeerLookup) LookupAZ(key string) (string, bool, bool) {
	return m.azPeer, m.azLocal, m.sameAZ
}

func TestController_PartitionMode_Global(t *testing.T) {
	c := NewController(ControllerConfig{
		L1:            newMockL1(),
		L2:            newMockL2(),
		PeerLookup:    &mockPeerLookup{selfAddr: "self:9428"},
		PartitionMode: "global",
		Metadata:      NewMetadataMap(),
	})
	// In global mode, lookupOwner uses Lookup (not LookupAZ)
	peer, isLocal := c.lookupOwner("test-key")
	if peer != "self:9428" || !isLocal {
		t.Fatalf("expected self:9428/local, got %s/%v", peer, isLocal)
	}
}

func TestController_PartitionMode_Distributed(t *testing.T) {
	c := NewController(ControllerConfig{
		L1:            newMockL1(),
		L2:            newMockL2(),
		PeerLookup:    &mockPeerLookup{selfAddr: "self:9428"},
		PartitionMode: "distributed",
		Metadata:      NewMetadataMap(),
	})
	peer, isLocal := c.lookupOwner("test-key")
	if peer != "self:9428" || !isLocal {
		t.Fatalf("expected self:9428/local, got %s/%v", peer, isLocal)
	}
}

func TestController_PartitionMode_AZLocal_WithAZLookup(t *testing.T) {
	azLookup := &mockAZPeerLookup{
		selfAddr: "self:9428",
		azPeer:   "az-peer:9428",
		azLocal:  false,
		sameAZ:   true,
	}
	c := NewController(ControllerConfig{
		L1:            newMockL1(),
		L2:            newMockL2(),
		PeerLookup:    azLookup,
		PartitionMode: "az-local",
		Metadata:      NewMetadataMap(),
	})
	// In az-local mode with AZPeerLookup, should use LookupAZ
	peer, isLocal := c.lookupOwner("test-key")
	if peer != "az-peer:9428" {
		t.Fatalf("expected az-peer:9428, got %s", peer)
	}
	if isLocal {
		t.Fatal("expected non-local from AZ lookup")
	}
}

func TestController_PartitionMode_AZLocal_FallbackToGlobal(t *testing.T) {
	// When PeerLookup doesn't implement AZPeerLookup, az-local falls back to global Lookup
	c := NewController(ControllerConfig{
		L1:            newMockL1(),
		L2:            newMockL2(),
		PeerLookup:    &mockPeerLookup{selfAddr: "self:9428"},
		PartitionMode: "az-local",
		Metadata:      NewMetadataMap(),
	})
	peer, isLocal := c.lookupOwner("test-key")
	if peer != "self:9428" || !isLocal {
		t.Fatalf("expected self:9428/local, got %s/%v", peer, isLocal)
	}
}

func TestController_PartitionMode_Default(t *testing.T) {
	// Empty PartitionMode should default to az-local
	c := NewController(ControllerConfig{
		L1:         newMockL1(),
		L2:         newMockL2(),
		PeerLookup: &mockPeerLookup{selfAddr: "self:9428"},
		Metadata:   NewMetadataMap(),
	})
	if c.partitionMode != "az-local" {
		t.Fatalf("expected default partition mode 'az-local', got %q", c.partitionMode)
	}
}

func TestController_PartitionMode_Global_IgnoresAZLookup(t *testing.T) {
	// In global mode, even if PeerLookup implements AZPeerLookup,
	// lookupOwner should use global Lookup, not LookupAZ
	azLookup := &mockAZPeerLookup{
		selfAddr: "self:9428",
		azPeer:   "az-peer:9428",
		azLocal:  false,
		sameAZ:   true,
	}
	c := NewController(ControllerConfig{
		L1:            newMockL1(),
		L2:            newMockL2(),
		PeerLookup:    azLookup,
		PartitionMode: "global",
		Metadata:      NewMetadataMap(),
	})
	peer, isLocal := c.lookupOwner("test-key")
	// Should use Lookup (returns self:9428, true), not LookupAZ (returns az-peer:9428, false)
	if peer != "self:9428" {
		t.Fatalf("expected self:9428 from global Lookup, got %s", peer)
	}
	if !isLocal {
		t.Fatal("expected local from global Lookup")
	}
}
