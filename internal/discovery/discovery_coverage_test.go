package discovery

import (
	"context"
	"net"
	"net/http"
	"testing"
	"time"
)

// TestWithHTTPClient exercises the WithHTTPClient option.
func TestWithHTTPClient(t *testing.T) {
	customClient := &http.Client{Timeout: 10 * time.Second}

	d := New("", nil, "", "", "9428", 5*time.Second,
		WithHTTPClient(customClient),
	)

	if d.httpClient != customClient {
		t.Error("expected custom HTTP client to be set")
	}
}

// TestNew_DefaultPort verifies that an empty defaultPort defaults to "9428".
func TestNew_DefaultPort(t *testing.T) {
	d := New("svc.local", nil, "", "", "", 5*time.Second)
	if d.defaultPort != "9428" {
		t.Errorf("defaultPort = %q, want 9428", d.defaultPort)
	}
}

// TestDiscoverStorageNodes_HeadlessSRV_WithCustomPort verifies that when
// a SRV record is found and a custom port is specified, the custom port is used.
func TestDiscoverStorageNodes_HeadlessSRV_WithCustomPort(t *testing.T) {
	d := New("vlstorage.monitoring.svc.cluster.local:10428", nil, "", "", "9428", 5*time.Second,
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", []*net.SRV{
				{Target: "pod-0.monitoring.svc.cluster.local.", Port: 9428},
			}, nil
		}),
	)

	nodes, err := d.DiscoverStorageNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	// Custom port from headless service should override SRV port.
	if nodes[0] != "pod-0.monitoring.svc.cluster.local:10428" {
		t.Errorf("node = %q, want pod-0.monitoring.svc.cluster.local:10428", nodes[0])
	}
}

// TestDiscoverPeers_Error verifies error handling in DiscoverPeers.
func TestDiscoverPeers_Error(t *testing.T) {
	d := New("", nil, "", "peer-svc.local", "9428", 5*time.Second,
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no such host"}
		}),
		WithLookupHost(func(_ context.Context, _ string) ([]string, error) {
			return nil, &net.DNSError{Err: "lookup failed", Name: "peer-svc.local", IsNotFound: true}
		}),
	)

	_, err := d.DiscoverPeers(context.Background())
	if err == nil {
		t.Fatal("expected error from failed peer discovery")
	}
}

// TestGetPeers_Empty verifies GetPeers on a discovery with no peers.
func TestGetPeers_Empty(t *testing.T) {
	d := New("", nil, "", "", "9428", 5*time.Second)

	peers := d.GetPeers()
	if len(peers) != 0 {
		t.Errorf("expected 0 peers, got %d", len(peers))
	}
}

// TestSetHotBoundaryForTest verifies the test helper method.
func TestSetHotBoundaryForTest(t *testing.T) {
	d := New("", nil, "", "", "9428", 5*time.Second)

	b := &HotBoundary{
		MinDate: "20260501",
		MaxDate: "20260510",
		MinTime: time.Date(2026, 5, 1, 0, 0, 0, 0, time.UTC),
		MaxTime: time.Date(2026, 5, 10, 0, 0, 0, 0, time.UTC),
	}
	d.SetHotBoundaryForTest(b)

	got := d.GetHotBoundary()
	if got == nil {
		t.Fatal("expected non-nil boundary")
	}
	if got.MinDate != "20260501" {
		t.Errorf("MinDate = %q, want 20260501", got.MinDate)
	}
}

// TestDiscoverStorageNodes_DNSError verifies error propagation from DNS.
func TestDiscoverStorageNodes_DNSError(t *testing.T) {
	d := New("nonexistent.local", nil, "", "", "9428", 5*time.Second,
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no such host"}
		}),
		WithLookupHost(func(_ context.Context, _ string) ([]string, error) {
			return nil, &net.DNSError{Err: "dns lookup failed"}
		}),
	)

	_, err := d.DiscoverStorageNodes(context.Background())
	if err == nil {
		t.Fatal("expected error from DNS failure")
	}
}
