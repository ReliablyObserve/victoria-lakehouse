package smartcache

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type remoteLookup struct{ peer string }

func (r remoteLookup) Lookup(string) (string, bool) { return r.peer, false }
func (r remoteLookup) Members() []string            { return []string{r.peer} }
func (r remoteLookup) MemberCount() int             { return 2 }

type stubPeer struct {
	data  []byte
	found bool
	err   error
	calls atomic.Int64
}

func (p *stubPeer) Fetch(context.Context, string, string) ([]byte, bool, error) {
	p.calls.Add(1)
	return p.data, p.found, p.err
}

func rangeController(l1 L1Cache, l2 L2Cache, lookup PeerLookup, peer PeerFetcher) *Controller {
	return NewController(ControllerConfig{
		L1: l1, L2: l2, PeerLookup: lookup, PeerFetcher: peer, S3Fetcher: newMockS3(),
		Metadata: NewMetadataMap(), MaxAge: time.Hour, HotThreshold: 3, HotWindow: time.Minute, Signal: "traces",
	})
}

// GetRange answers from the memory tier, then the disk tier (promoting to memory), then the owner
// over the peer ring (keeping what it got on its own disk), and only then fetches: one shared fetch
// per range, whose bytes every tier keeps.
func TestGetRange_TiersInOrder(t *testing.T) {
	ctx := context.Background()
	fetches := 0
	fetch := func(context.Context) ([]byte, error) { fetches++; return []byte("from-s3"), nil }

	l1, l2 := newMockL1(), newMockL2()
	c := rangeController(l1, l2, &mockPeerLookup{selfAddr: "self"}, &mockPeerFetcher{})

	if d, tier, err := c.GetRange(ctx, "k#0+7", fetch); err != nil || tier != "s3" || string(d) != "from-s3" || fetches != 1 {
		t.Fatalf("first read: %q %s %v (fetches %d)", d, tier, err, fetches)
	}
	if _, ok := l1.Get("k#0+7"); !ok {
		t.Error("the fetched range is not in memory")
	}
	if _, ok := l2.Get("k#0+7"); !ok {
		t.Error("the fetched range is not on disk")
	}
	if _, tier, _ := c.GetRange(ctx, "k#0+7", fetch); tier != "memory" || fetches != 1 {
		t.Errorf("second read came from %s (fetches %d), want memory", tier, fetches)
	}
	l1.mu.Lock()
	delete(l1.data, "k#0+7")
	l1.mu.Unlock()
	if _, tier, _ := c.GetRange(ctx, "k#0+7", fetch); tier != "disk" || fetches != 1 {
		t.Errorf("after the memory tier lost it: %s (fetches %d), want disk", tier, fetches)
	}
	if _, ok := l1.Get("k#0+7"); !ok {
		t.Error("a disk hit is not promoted to memory")
	}
}

func TestGetRange_PeerOwnerThenS3Fallback(t *testing.T) {
	ctx := context.Background()
	peer := &stubPeer{data: []byte("from-peer"), found: true}
	l1, l2 := newMockL1(), newMockL2()
	c := rangeController(l1, l2, remoteLookup{"owner"}, peer)
	fetch := func(context.Context) ([]byte, error) { t.Fatal("the owner had the range: no S3 read"); return nil, nil }
	d, tier, err := c.GetRange(ctx, "k#5+9", fetch)
	if err != nil || tier != "peer" || string(d) != "from-peer" {
		t.Fatalf("%q %s %v", d, tier, err)
	}
	if _, ok := l2.Get("k#5+9"); !ok {
		t.Error("a non-owner does not keep the range on its own disk")
	}

	// the owner does not have it, or fails: the object store answers
	for _, p := range []*stubPeer{{found: false}, {err: errors.New("peer down")}} {
		c := rangeController(newMockL1(), newMockL2(), remoteLookup{"owner"}, p)
		d, tier, err := c.GetRange(ctx, "k#0+2", func(context.Context) ([]byte, error) { return []byte("s3"), nil })
		if err != nil || tier != "s3" || string(d) != "s3" || p.calls.Load() != 1 {
			t.Errorf("fallback: %q %s %v (peer calls %d)", d, tier, err, p.calls.Load())
		}
	}
}

func TestGetRange_NoDiskTierAndFetchError(t *testing.T) {
	ctx := context.Background()
	c := rangeController(newMockL1(), nil, &mockPeerLookup{selfAddr: "self"}, &mockPeerFetcher{})
	if d, tier, err := c.GetRange(ctx, "k#0+1", func(context.Context) ([]byte, error) { return []byte("x"), nil }); err != nil || tier != "s3" || string(d) != "x" {
		t.Fatalf("%q %s %v", d, tier, err)
	}
	boom := errors.New("boom")
	if _, _, err := c.GetRange(ctx, "k#9+1", func(context.Context) ([]byte, error) { return nil, boom }); !errors.Is(err, boom) {
		t.Fatalf("error %v, want %v", err, boom)
	}
	if _, ok := c.l1.Get("k#9+1"); ok {
		t.Error("a failed fetch is cached")
	}
}

// Concurrent readers of one range share one fetch.
func TestGetRange_ConcurrentReadersShareOneFetch(t *testing.T) {
	ctx := context.Background()
	c := rangeController(newMockL1(), newMockL2(), &mockPeerLookup{selfAddr: "self"}, &mockPeerFetcher{})
	var fetches atomic.Int64
	release := make(chan struct{})
	fetch := func(context.Context) ([]byte, error) { fetches.Add(1); <-release; return []byte("shared"), nil }
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if d, _, err := c.GetRange(ctx, "k#0+6", fetch); err != nil || string(d) != "shared" {
				t.Errorf("%q %v", d, err)
			}
		}()
	}
	time.Sleep(50 * time.Millisecond)
	close(release)
	wg.Wait()
	if n := fetches.Load(); n != 1 {
		t.Errorf("fetches = %d, want 1", n)
	}
}

func TestBudgetedL1_PutNoCopyStoresTheBuffer(t *testing.T) {
	c := NewBudgetedL1(1<<20, nil)
	buf := []byte("abc")
	c.PutNoCopy("k", buf)
	got, ok := c.Get("k")
	if !ok || string(got) != "abc" {
		t.Fatalf("%q %v", got, ok)
	}
}
