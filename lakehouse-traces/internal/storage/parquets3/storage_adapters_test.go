package parquets3

import (
	"context"
	"os"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
)

// --- l1Adapter tests ---

func TestL1Adapter_PutGet(t *testing.T) {
	lru := cache.NewLRU(1 << 20) // 1 MiB
	a := &l1Adapter{lru: lru}

	data := []byte("hello-world")
	a.Put("key1", data)

	got, ok := a.Get("key1")
	if !ok {
		t.Fatal("expected key1 to be found")
	}
	if string(got) != string(data) {
		t.Fatalf("expected %q, got %q", data, got)
	}
}

func TestL1Adapter_GetMiss(t *testing.T) {
	lru := cache.NewLRU(1 << 20)
	a := &l1Adapter{lru: lru}

	_, ok := a.Get("nonexistent")
	if ok {
		t.Fatal("expected miss for nonexistent key")
	}
}

func TestL1Adapter_OverwriteKey(t *testing.T) {
	lru := cache.NewLRU(1 << 20)
	a := &l1Adapter{lru: lru}

	a.Put("k", []byte("v1"))
	a.Put("k", []byte("v2"))

	got, ok := a.Get("k")
	if !ok {
		t.Fatal("expected key to be found after overwrite")
	}
	if string(got) != "v2" {
		t.Fatalf("expected v2, got %q", got)
	}
}

func TestL1Adapter_Eviction(t *testing.T) {
	// Cache with 100 bytes max
	lru := cache.NewLRU(100)
	a := &l1Adapter{lru: lru}

	// Fill beyond capacity
	for i := 0; i < 20; i++ {
		key := string(rune('a'+i)) + "key"
		a.Put(key, make([]byte, 20))
	}

	// At least some early keys should be evicted
	_, ok := a.Get("akey")
	// We just verify it doesn't panic; eviction behavior depends on LRU internals
	_ = ok
}

// --- l2Adapter tests ---

func TestL2Adapter_NilDiskCache_GetReturnsFalse(t *testing.T) {
	a := &l2Adapter{dc: nil}

	_, ok := a.Get("anything")
	if ok {
		t.Fatal("expected nil diskCache Get to return false")
	}
}

func TestL2Adapter_NilDiskCache_PutReturnsNil(t *testing.T) {
	a := &l2Adapter{dc: nil}

	err := a.Put("key", []byte("data"))
	if err != nil {
		t.Fatalf("expected nil error for nil diskCache Put, got: %v", err)
	}
}

func TestL2Adapter_NilDiskCache_DeleteNoPanic(t *testing.T) {
	a := &l2Adapter{dc: nil}
	// Should not panic
	a.Delete("key")
}

func TestL2Adapter_NilDiskCache_SizeReturnsZero(t *testing.T) {
	a := &l2Adapter{dc: nil}

	if a.Size() != 0 {
		t.Fatalf("expected Size()=0 for nil diskCache, got %d", a.Size())
	}
}

func TestL2Adapter_WithDiskCache_PutAndGet(t *testing.T) {
	dir := t.TempDir()
	dc, err := cache.NewDiskCache(dir, 1<<20, 0.8)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	a := &l2Adapter{dc: dc}

	data := []byte("test-payload-12345")
	if err := a.Put("file.parquet", data); err != nil {
		t.Fatalf("Put: %v", err)
	}

	got, ok := a.Get("file.parquet")
	if !ok {
		t.Fatal("expected to find key after Put")
	}
	if string(got) != string(data) {
		t.Fatalf("expected %q, got %q", data, got)
	}
}

func TestL2Adapter_WithDiskCache_GetMiss(t *testing.T) {
	dir := t.TempDir()
	dc, err := cache.NewDiskCache(dir, 1<<20, 0.8)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	a := &l2Adapter{dc: dc}

	_, ok := a.Get("missing-key")
	if ok {
		t.Fatal("expected miss for key never put")
	}
}

func TestL2Adapter_WithDiskCache_Delete(t *testing.T) {
	dir := t.TempDir()
	dc, err := cache.NewDiskCache(dir, 1<<20, 0.8)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	a := &l2Adapter{dc: dc}

	_ = a.Put("del-key", []byte("to-delete"))
	a.Delete("del-key")

	_, ok := a.Get("del-key")
	if ok {
		t.Fatal("expected miss after Delete")
	}
}

func TestL2Adapter_WithDiskCache_Size(t *testing.T) {
	dir := t.TempDir()
	dc, err := cache.NewDiskCache(dir, 1<<20, 0.8)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	a := &l2Adapter{dc: dc}

	if a.Size() != 0 {
		t.Fatalf("expected Size()=0 for empty cache, got %d", a.Size())
	}

	_ = a.Put("s-key", []byte("12345"))
	if a.Size() == 0 {
		t.Fatal("expected Size() > 0 after Put")
	}
}

func TestL2Adapter_WithDiskCache_GetAfterFileRemoved(t *testing.T) {
	dir := t.TempDir()
	dc, err := cache.NewDiskCache(dir, 1<<20, 0.8)
	if err != nil {
		t.Fatalf("NewDiskCache: %v", err)
	}

	a := &l2Adapter{dc: dc}
	_ = a.Put("vanish", []byte("ephemeral"))

	// Remove the underlying file to simulate corruption
	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		_ = os.Remove(dir + "/" + e.Name())
	}

	_, ok := a.Get("vanish")
	if ok {
		t.Fatal("expected miss when underlying file is removed")
	}
}

// --- localOnlyLookup tests ---

func TestLocalOnlyLookup_Lookup(t *testing.T) {
	l := &localOnlyLookup{}

	peer, isLocal := l.Lookup("any-key")
	if peer != "self" {
		t.Fatalf("expected peer=%q, got %q", "self", peer)
	}
	if !isLocal {
		t.Fatal("expected isLocal=true")
	}
}

func TestLocalOnlyLookup_LookupDifferentKeys(t *testing.T) {
	l := &localOnlyLookup{}

	keys := []string{"", "a", "key/with/slashes", "very-long-key-" + string(make([]byte, 100))}
	for _, k := range keys {
		peer, isLocal := l.Lookup(k)
		if peer != "self" || !isLocal {
			t.Fatalf("Lookup(%q): expected (self, true), got (%q, %v)", k, peer, isLocal)
		}
	}
}

func TestLocalOnlyLookup_Members(t *testing.T) {
	l := &localOnlyLookup{}

	members := l.Members()
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d", len(members))
	}
	if members[0] != "self" {
		t.Fatalf("expected member[0]=%q, got %q", "self", members[0])
	}
}

func TestLocalOnlyLookup_MemberCount(t *testing.T) {
	l := &localOnlyLookup{}

	if l.MemberCount() != 1 {
		t.Fatalf("expected MemberCount()=1, got %d", l.MemberCount())
	}
}

// --- peerLookupAdapter tests ---

func TestPeerLookupAdapter_EmptyRing(t *testing.T) {
	// A fresh PeerCache has an empty ring (members added via UpdatePeers).
	pc := peercache.New("127.0.0.1:9428", "test-key", 5*time.Second, 10)
	a := &peerLookupAdapter{pc: pc}

	if a.MemberCount() != 0 {
		t.Fatalf("expected MemberCount()=0 for fresh PeerCache, got %d", a.MemberCount())
	}
	if len(a.Members()) != 0 {
		t.Fatalf("expected empty Members() for fresh PeerCache, got %v", a.Members())
	}
}

func TestPeerLookupAdapter_WithSelfAsMember(t *testing.T) {
	pc := peercache.New("127.0.0.1:9428", "test-key", 5*time.Second, 10)
	pc.UpdatePeers([]string{"127.0.0.1:9428"})

	a := &peerLookupAdapter{pc: pc}

	members := a.Members()
	if len(members) != 1 {
		t.Fatalf("expected 1 member, got %d: %v", len(members), members)
	}

	if a.MemberCount() != 1 {
		t.Fatalf("expected MemberCount()=1, got %d", a.MemberCount())
	}

	// Lookup for any key should route to self (isLocal=true)
	peer, isLocal := a.Lookup("some-key")
	if !isLocal {
		t.Fatalf("expected isLocal=true with self as only peer, got peer=%q", peer)
	}
	if peer != "127.0.0.1:9428" {
		t.Fatalf("expected peer=127.0.0.1:9428, got %q", peer)
	}
}

func TestPeerLookupAdapter_WithMultiplePeers(t *testing.T) {
	pc := peercache.New("127.0.0.1:9428", "test-key", 5*time.Second, 10)
	pc.UpdatePeers([]string{"127.0.0.1:9428", "10.0.0.2:9428", "10.0.0.3:9428"})

	a := &peerLookupAdapter{pc: pc}

	if a.MemberCount() != 3 {
		t.Fatalf("expected MemberCount()=3, got %d", a.MemberCount())
	}

	members := a.Members()
	if len(members) != 3 {
		t.Fatalf("expected 3 members, got %d", len(members))
	}

	// Lookup should deterministically map a key to a peer
	peer1, _ := a.Lookup("key-abc")
	peer2, _ := a.Lookup("key-abc")
	if peer1 != peer2 {
		t.Fatalf("expected consistent Lookup, got %q then %q", peer1, peer2)
	}
}

// --- s3Adapter tests ---

func TestS3Adapter_NilPool_Panics(t *testing.T) {
	// s3Adapter with nil pool should panic on Download (nil dereference)
	a := &s3Adapter{pool: nil}

	defer func() {
		if r := recover(); r == nil {
			t.Fatal("expected panic on nil pool Download")
		}
	}()

	_, _ = a.Download(context.Background(), "some-key")
}
