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
// rows of tenant 0:0.
func insertPod(t *testing.T, n int) *httptest.Server {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/buffer/query" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set(buffer.TenantScopeHeader, "0:0")
		enc := json.NewEncoder(w)
		for i := 0; i < n; i++ {
			_ = enc.Encode(schema.LogRow{TimestampUnixNano: int64(i + 1), Body: "buffered"})
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
	b := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true, BufferQueryTimeout: 2 * time.Second}, config.ModeLogs)
	b.SetEndpoints([]string{strings.TrimPrefix(srv.URL, "http://")})
	rows, err := b.QueryLogs(context.Background(), 0, math.MaxInt64, tenantScope{account: "0", project: "0"})
	if err != nil || len(rows) != 3 {
		t.Fatalf("QueryLogs = %d rows, %v; want the pod's 3 rows", len(rows), err)
	}
	// With AZ zones too.
	b.SetEndpointsWithZones(map[string]string{strings.TrimPrefix(srv.URL, "http://"): "az-a"}, "az-a")
	rows, err = b.QueryLogs(context.Background(), 0, math.MaxInt64, tenantScope{account: "0", project: "0"})
	if err != nil || len(rows) != 3 {
		t.Fatalf("QueryLogs with zones = %d rows, %v; want 3", len(rows), err)
	}
}

func TestBridgeEndpoint_KeepsAnExplicitScheme(t *testing.T) {
	for in, want := range map[string]string{
		"10.0.0.7:9428":         "http://10.0.0.7:9428",
		"http://10.0.0.7:9428":  "http://10.0.0.7:9428",
		"https://insert-0:9428": "https://insert-0:9428",
		"[::1]:9428":            "http://[::1]:9428",
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
	s.cfg.Select.InsertHeadlessService = "lh-logs-insert-headless:9428"
	s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
	s.discovery = discovery.New("", nil, "", "lh-logs-select-headless", "9428", 5*time.Second,
		discovery.WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no srv"}
		}),
		discovery.WithLookupHost(func(_ context.Context, host string) ([]string, error) {
			switch host {
			case "lh-logs-insert-headless":
				return []string{"10.0.0.8", "10.0.0.7"}, nil
			case "lh-logs-select-headless":
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
	if got != "http://10.0.0.7:9428,http://10.0.0.8:9428" {
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
	s.discovery = discovery.New("", nil, "", "lh-peers", "9428", 5*time.Second,
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
	if got != "http://10.1.1.1:9428" {
		t.Errorf("bridge endpoints = %s; want the peer ring", got)
	}
}

// An insert service that does not resolve is an error of the refresh (it is
// retried on the next tick), and leaves the bridge as it was.
func TestRefreshDiscovery_InsertServiceErrorKeepsTheBridge(t *testing.T) {
	s := testStorage()
	s.cfg.Select.BufferQueryEnabled = true
	s.cfg.Select.InsertHeadlessService = "missing:9428"
	s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
	s.bufferBridge.SetEndpoints([]string{"10.2.2.2:9428"})
	s.discovery = discovery.New("", nil, "", "", "9428", 5*time.Second,
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
	if len(s.bufferBridge.endpoints) != 1 || s.bufferBridge.endpoints[0] != "http://10.2.2.2:9428" {
		t.Errorf("endpoints = %v; want them unchanged", s.bufferBridge.endpoints)
	}
}

// With a peer ring and an insert service that does not resolve, the bridge is
// not pointed at the peer ring (select pods hold no buffer): it keeps the
// insert pods it had.
func TestRefreshDiscovery_FailingInsertServiceNeverFallsBackToPeers(t *testing.T) {
	s := testStorage()
	s.cfg.Select.BufferQueryEnabled = true
	s.cfg.Select.InsertHeadlessService = "missing:9428"
	s.bufferBridge = NewBufferBridge(&s.cfg.Select, s.cfg.Mode)
	s.bufferBridge.SetEndpoints([]string{"10.2.2.2:9428"})
	s.discovery = discovery.New("", nil, "", "lh-peers", "9428", 5*time.Second,
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
	if len(s.bufferBridge.endpoints) != 1 || s.bufferBridge.endpoints[0] != "http://10.2.2.2:9428" {
		t.Errorf("endpoints = %v; want the insert pods it had", s.bufferBridge.endpoints)
	}
}
