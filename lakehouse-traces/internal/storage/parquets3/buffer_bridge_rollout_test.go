package parquets3

import (
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// The first step of a peer authentication rollout: the insert pod holds the
// peer key but does not enforce it (peer.auth_enabled false), as the binary
// mounts it (buffer.NewHandler(src, EnforcedKey()).WithPeerKey(AuthKey)).
// Select pods already send their key, so a global read works as soon as the
// key is rolled out, before enforcement is switched on.
func heldKeyInsertPod(t *testing.T, e *viewEnv, key string) *httptest.Server {
	t.Helper()
	mux := http.NewServeMux()
	mux.Handle(buffer.Path, buffer.Gate(buffer.Path, func() bool { return false },
		buffer.NewHandler(BridgeSource{Segments: e.segs}, "").WithPeerKey(key)))
	srv := httptest.NewServer(mux)
	t.Cleanup(srv.Close)
	return srv
}

func TestBridgeRollout_HeldKeyServesAGlobalReadToAKeyedSelectPod(t *testing.T) {
	e := newViewEnv(t)
	ingestTenantRows(e, tenantA, "A", 30)
	ingestTenantRows(e, tenantB, "B", 20)
	peer := heldKeyInsertPod(t, e, bridgeAuthKey)

	before := metrics.BufferBridgeErrors.Get("auth")
	got := msgCounts(t, authSelectPod(t, e, bridgeAuthKey, peer.URL), nil, true)
	if countPrefix(got, "A-") != 30 || countPrefix(got, "B-") != 20 {
		t.Fatalf("global read of a pod that holds the key without enforcing it: A=%d B=%d, want 30 and 20", countPrefix(got, "A-"), countPrefix(got, "B-"))
	}
	if metrics.BufferBridgeErrors.Get("auth") != before {
		t.Error("a keyed select pod was refused by a pod holding the same key")
	}
}

// A select pod that has no key yet (not rolled out) is still served the
// single-tenant reads it always was, and refused the global read as before.
func TestBridgeRollout_KeylessSelectPodKeepsSingleTenantReads(t *testing.T) {
	e := newViewEnv(t)
	ingestTenantRows(e, tenantA, "A", 30)
	ingestTenantRows(e, tenantB, "B", 20)
	peer := heldKeyInsertPod(t, e, bridgeAuthKey)

	got := msgCounts(t, authSelectPod(t, e, "", peer.URL), []logstorage.TenantID{tenantA}, false)
	if countPrefix(got, "A-") != 30 || countPrefix(got, "B-") != 0 {
		t.Errorf("single-tenant read without a key, not enforcing: A=%d B=%d, want 30 and 0", countPrefix(got, "A-"), countPrefix(got, "B-"))
	}

	before := metrics.BufferBridgeErrors.Get("auth")
	if got := msgCounts(t, authSelectPod(t, e, "", peer.URL), nil, true); len(got) != 0 {
		t.Errorf("a keyless select pod got %d rows from a global read", len(got))
	}
	if metrics.BufferBridgeErrors.Get("auth") == before {
		t.Error("the 403 of the global read was not counted as reason=auth")
	}
}
