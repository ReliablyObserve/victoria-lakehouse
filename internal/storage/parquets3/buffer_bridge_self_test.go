package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// TestBufferBridge_SelfFallback_WhenNoPeers pins the single-node
// behaviour: when peer discovery yields zero endpoints (no headless
// service configured, or k8s DNS returned nothing yet) AND a self-
// endpoint is registered, getQueryEndpoints returns [selfEndpoint]
// so this pod's local buffer is still queried. Without the
// fallback, a single-node deployment never serves its own
// unflushed buffer and queries against the last <flush-interval>
// window return zero data until the next flush.
func TestBufferBridge_SelfFallback_WhenNoPeers(t *testing.T) {
	b := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true}, config.ModeLogs)
	b.SetSelfEndpoint("http://localhost:9428")

	got := b.getQueryEndpoints()
	if len(got) != 1 || got[0] != "http://localhost:9428" {
		t.Errorf("getQueryEndpoints() = %v, want [http://localhost:9428] (self fallback)", got)
	}
}

// TestBufferBridge_RealPeersOverrideSelf pins the cluster
// behaviour: once SetEndpoints populates real peer URLs, the
// self-fallback must silently step aside. Peer discovery is the
// source of truth in a cluster — querying self in addition to
// peers would emit duplicate rows from the same buffer.
func TestBufferBridge_RealPeersOverrideSelf(t *testing.T) {
	b := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true}, config.ModeLogs)
	b.SetSelfEndpoint("http://localhost:9428")
	b.SetEndpoints([]string{"http://peer-a:9428", "http://peer-b:9428"})

	got := b.getQueryEndpoints()
	if len(got) != 2 {
		t.Fatalf("getQueryEndpoints() = %v, want [peer-a, peer-b] (real peers)", got)
	}
	for _, g := range got {
		if g == "http://localhost:9428" {
			t.Errorf("self entry leaked into peer list: %v", got)
		}
	}
}

// TestBufferBridge_NoSelfNoPeers_EmptyEndpoints guards the case
// where neither SetSelfEndpoint nor SetEndpoints have been called:
// getQueryEndpoints must return empty (not nil-deref), and
// QueryLogs must early-return without panicking.
func TestBufferBridge_NoSelfNoPeers_EmptyEndpoints(t *testing.T) {
	b := NewBufferBridge(&config.SelectConfig{BufferQueryEnabled: true}, config.ModeLogs)

	got := b.getQueryEndpoints()
	if len(got) != 0 {
		t.Errorf("getQueryEndpoints() = %v, want empty (no self, no peers)", got)
	}
}
