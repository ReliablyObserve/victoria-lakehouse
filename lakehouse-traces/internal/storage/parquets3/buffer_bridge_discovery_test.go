package parquets3

import (
	"context"
	"encoding/json"
	"math"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// insertPod answers /internal/buffer/query like an insert pod with n buffered
// spans of tenant 0:0.
func insertPod(t *testing.T, n int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/buffer/query" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		w.Header().Set(buffer.SegmentsHeader, "65000000aaaabbbb")
		enc := json.NewEncoder(w)
		for i := 0; i < n; i++ {
			_ = enc.Encode(schema.TraceRow{TimestampUnixNano: int64(i + 1), TraceID: "t", SpanID: "s"})
		}
	}))
	t.Cleanup(srv.Close)
	return srv
}

// Discovery yields "host:port" (no scheme). The bridge must still reach the
// pod: before, the request URL "host:port/internal/buffer/query" did not parse,
// so every discovered peer's unflushed rows were missing from every answer.
func TestBufferBridge_ReachesDiscoveredHostPort(t *testing.T) {
	srv := insertPod(t, 3)
	b := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeTraces)
	b.SetEndpoints([]string{strings.TrimPrefix(srv.URL, "http://")})
	rows, nonces := b.QueryTraces(context.Background(), 0, math.MaxInt64, tenantScope{account: "0", project: "0"})
	if _, ok := nonces["65000000aaaabbbb"]; len(rows) != 3 || !ok {
		t.Fatalf("QueryTraces = %d rows, nonces %v; want the pod's 3 rows and its segment", len(rows), nonces)
	}
	// With AZ zones too.
	b.SetEndpointsWithZones(map[string]string{strings.TrimPrefix(srv.URL, "http://"): "az-a"}, "az-a")
	rows, nonces = b.QueryTraces(context.Background(), 0, math.MaxInt64, tenantScope{account: "0", project: "0"})
	if _, ok := nonces["65000000aaaabbbb"]; len(rows) != 3 || !ok {
		t.Fatalf("QueryTraces with zones = %d rows, nonces %v; want 3 and the segment", len(rows), nonces)
	}
}

func TestBridgeEndpoint_KeepsAnExplicitScheme(t *testing.T) {
	for in, want := range map[string]string{
		"10.0.0.7:10428":         "http://10.0.0.7:10428",
		"http://10.0.0.7:10428":  "http://10.0.0.7:10428",
		"https://insert-0:10428": "https://insert-0:10428",
		"[::1]:10428":            "http://[::1]:10428",
	} {
		if got := bridgeEndpoint(in); got != want {
			t.Errorf("bridgeEndpoint(%q) = %q, want %q", in, got, want)
		}
	}
}

// A select pod of a split deployment finds the insert pods through
// select.insert_headless_service (it was read by nothing before), and that,
// not the peer ring, is what the bridge queries.
func TestRefreshDiscovery_InsertHeadlessServiceFeedsTheBridge(t *testing.T) {
	s := testStorage()
	s.cfg.Select.BufferQueryEnabled = true
	s.cfg.Select.BufferQueryTimeout = 2 * time.Second
	s.cfg.Select.InsertHeadlessService = "lh-traces-insert-headless:10428"
	s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
	s.discovery = discovery.New("", nil, "", "lh-traces-select-headless", "10428", 5*time.Second,
		discovery.WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no srv"}
		}),
		discovery.WithLookupHost(func(_ context.Context, host string) ([]string, error) {
			switch host {
			case "lh-traces-insert-headless":
				return []string{"10.0.0.8", "10.0.0.7"}, nil
			case "lh-traces-select-headless":
				return []string{"10.9.9.9"}, nil
			}
			return nil, &net.DNSError{Err: "unknown " + host}
		}),
	)
	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.bufferBridge.mu.RLock()
	got := strings.Join(s.bufferBridge.endpoints, ",")
	s.bufferBridge.mu.RUnlock()
	if got != "http://10.0.0.7:10428,http://10.0.0.8:10428" {
		t.Errorf("bridge endpoints = %s; want the insert pods", got)
	}
	if !s.bufferBridge.HasPeers() {
		t.Error("the bridge reports no peers")
	}
}

// Without select.insert_headless_service the peer ring stays the bridge's
// source, as before.
func TestRefreshDiscovery_PeerRingFeedsTheBridgeWithoutInsertService(t *testing.T) {
	s := testStorage()
	s.cfg.Select.BufferQueryEnabled = true
	s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
	s.discovery = discovery.New("", nil, "", "lh-peers", "10428", 5*time.Second,
		discovery.WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no srv"}
		}),
		discovery.WithLookupHost(func(_ context.Context, _ string) ([]string, error) {
			return []string{"10.1.1.1"}, nil
		}),
	)
	if err := s.RefreshDiscovery(context.Background()); err != nil {
		t.Fatal(err)
	}
	s.bufferBridge.mu.RLock()
	got := strings.Join(s.bufferBridge.endpoints, ",")
	s.bufferBridge.mu.RUnlock()
	if got != "http://10.1.1.1:10428" {
		t.Errorf("bridge endpoints = %s; want the peer ring", got)
	}
}

// An insert service that does not resolve is an error of the refresh (it is
// retried on the next tick), and leaves the bridge as it was.
func TestRefreshDiscovery_InsertServiceErrorKeepsTheBridge(t *testing.T) {
	s := testStorage()
	s.cfg.Select.BufferQueryEnabled = true
	s.cfg.Select.InsertHeadlessService = "missing:10428"
	s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
	s.bufferBridge.SetEndpoints([]string{"10.2.2.2:10428"})
	s.discovery = discovery.New("", nil, "", "", "10428", 5*time.Second,
		discovery.WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no srv"}
		}),
		discovery.WithLookupHost(func(_ context.Context, _ string) ([]string, error) {
			return nil, &net.DNSError{Err: "no such host"}
		}),
	)
	if err := s.RefreshDiscovery(context.Background()); err == nil {
		t.Fatal("no error for an insert service that does not resolve")
	}
	s.bufferBridge.mu.RLock()
	defer s.bufferBridge.mu.RUnlock()
	if len(s.bufferBridge.endpoints) != 1 || s.bufferBridge.endpoints[0] != "http://10.2.2.2:10428" {
		t.Errorf("endpoints = %v; want them unchanged", s.bufferBridge.endpoints)
	}
}

// With a peer ring and an insert service that does not resolve, the bridge is
// not pointed at the peer ring (select pods hold no buffer): it keeps the
// insert pods it had.
func TestRefreshDiscovery_FailingInsertServiceNeverFallsBackToPeers(t *testing.T) {
	s := testStorage()
	s.cfg.Select.BufferQueryEnabled = true
	s.cfg.Select.InsertHeadlessService = "missing:10428"
	s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
	s.bufferBridge.SetEndpoints([]string{"10.2.2.2:10428"})
	s.discovery = discovery.New("", nil, "", "lh-peers", "10428", 5*time.Second,
		discovery.WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no srv"}
		}),
		discovery.WithLookupHost(func(_ context.Context, host string) ([]string, error) {
			if host == "lh-peers" {
				return []string{"10.1.1.1"}, nil
			}
			return nil, &net.DNSError{Err: "no such host"}
		}),
	)
	if err := s.RefreshDiscovery(context.Background()); err == nil {
		t.Fatal("no error for an insert service that does not resolve")
	}
	s.bufferBridge.mu.RLock()
	defer s.bufferBridge.mu.RUnlock()
	if len(s.bufferBridge.endpoints) != 1 || s.bufferBridge.endpoints[0] != "http://10.2.2.2:10428" {
		t.Errorf("endpoints = %v; want the insert pods it had", s.bufferBridge.endpoints)
	}
}
