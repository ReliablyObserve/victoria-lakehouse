package smartcache

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/selfaddr"
)

// countingPeer is an HTTP peer that answers every cache fetch with a miss and
// counts the fetches.
type countingPeer struct {
	srv  *httptest.Server
	hits atomic.Int64
}

func newCountingPeer() *countingPeer {
	p := &countingPeer{}
	p.srv = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		p.hits.Add(1)
		http.NotFound(w, r)
	}))
	return p
}

func (p *countingPeer) addr() string { return strings.TrimPrefix(p.srv.URL, "http://") }

type countingS3 struct{ gets atomic.Int64 }

func (s *countingS3) Download(_ context.Context, key string) ([]byte, error) {
	s.gets.Add(1)
	return []byte("data-" + key), nil
}

type fetchAdapter struct{ pc *peercache.PeerCache }

func (a fetchAdapter) Fetch(ctx context.Context, peer, key string) ([]byte, bool, error) {
	return a.pc.Fetch(ctx, peer, key)
}

type lookupAdapter struct{ pc *peercache.PeerCache }

func (a lookupAdapter) Lookup(key string) (string, bool) { return a.pc.Lookup(key) }
func (a lookupAdapter) Members() []string                { return a.pc.Members() }
func (a lookupAdapter) MemberCount() int                 { return len(a.pc.Members()) }

// A pod whose discovered ring lists it as "localhost:<port>" while it listens on
// ":<port>" reads the keys it owns from its own caches and from S3, with no HTTP
// request to itself, and fetches the other pod's keys from that pod over HTTP.
// A pod that does not find itself sends every key, its own included, over HTTP
// (the situation before the self-address fix, kept as the control).
func TestController_OwnKeysCauseNoSelfHTTP(t *testing.T) {
	for _, tc := range []struct {
		name         string
		hostname     string
		wantSelfHTTP bool
	}{
		{"pod finds itself", "localhost", false},
		{"pod does not find itself", "another-pod", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			self, other := newCountingPeer(), newCountingPeer()
			defer self.srv.Close()
			defer other.srv.Close()
			// Both peers are on 127.0.0.1; the pod is the one named localhost.
			selfPort := strings.Split(self.addr(), ":")[1]
			selfMember := "localhost:" + selfPort
			members := []string{selfMember, other.addr()}

			// The pod listens on the port its peers reach it on, on every interface.
			pc := peercache.NewWithStabilize(":"+selfPort, "", 2*time.Second, 4, time.Millisecond)
			pc.SetSelfEnv(selfaddr.Env{Hostnames: func() []string { return []string{tc.hostname} }})
			pc.UpdatePeers(members)

			s3 := &countingS3{}
			l2 := newMockL2()
			ctl := NewController(ControllerConfig{
				L1: newMockL1(), L2: l2, PeerLookup: lookupAdapter{pc}, PeerFetcher: fetchAdapter{pc},
				S3Fetcher: s3, Metadata: NewMetadataMap(), PartitionMode: "global",
			})

			var ownKeys, otherKeys []string
			for i := 0; len(ownKeys) < 20 || len(otherKeys) < 20; i++ {
				key := fmt.Sprintf("tenant/dt=2026-10-01/f%04d.parquet", i)
				peer, _ := pc.Lookup(key)
				switch {
				case peer == selfMember && len(ownKeys) < 20:
					ownKeys = append(ownKeys, key)
				case peer == other.addr() && len(otherKeys) < 20:
					otherKeys = append(otherKeys, key)
				}
			}

			for _, k := range ownKeys {
				if _, err := ctl.Get(context.Background(), k, 10); err != nil {
					t.Fatal(err)
				}
			}
			selfHTTP, otherHTTP := self.hits.Load(), other.hits.Load()
			if tc.wantSelfHTTP != (selfHTTP > 0) {
				t.Errorf("own keys caused %d HTTP requests to the pod itself, want >0=%v", selfHTTP, tc.wantSelfHTTP)
			}
			if otherHTTP != 0 {
				t.Errorf("own keys caused %d HTTP requests to the other pod, want 0", otherHTTP)
			}
			if got := s3.gets.Load(); got != int64(len(ownKeys)) {
				t.Errorf("S3 GETs for %d own keys = %d, want one each", len(ownKeys), got)
			}
			// A key a pod owns is kept on its disk cache; one it does not find itself for is not.
			if _, ok := l2.Get(ownKeys[0]); ok == tc.wantSelfHTTP {
				t.Errorf("own key cached on disk = %v, want %v", ok, !tc.wantSelfHTTP)
			}

			s3Before := s3.gets.Load()
			for _, k := range otherKeys {
				if _, err := ctl.Get(context.Background(), k, 10); err != nil {
					t.Fatal(err)
				}
			}
			if got := other.hits.Load() - otherHTTP; got != int64(len(otherKeys)) {
				t.Errorf("the other pod's keys caused %d HTTP requests to it, want %d (one each)", got, len(otherKeys))
			}
			if got := s3.gets.Load() - s3Before; got != int64(len(otherKeys)) {
				t.Errorf("S3 GETs for the other pod's keys = %d, want %d (its peer missed, so one each)", got, len(otherKeys))
			}
		})
	}
}
