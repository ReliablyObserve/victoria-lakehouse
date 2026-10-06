package main

import (
	"net"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// The admin parity endpoint asks the server THIS process listens on
// (-httpListenAddr), not the mode's default address: with Lakehouse on :19429
// and a hot VictoriaLogs/VictoriaTraces on the default port, the caller's
// global-read secret must reach Lakehouse only.
func TestMountParity_LoopbackGoesToTheListenAddrNotTheDefault(t *testing.T) {
	var lhHits atomic.Int32
	var lhSecret atomic.Value
	lh := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		lhHits.Add(1)
		lhSecret.Store(r.Header.Get("X-Global-Read"))
		_, _ = w.Write([]byte(`{"data":{"result":[{"value":[0,"0"]}]}}`))
	}))
	defer lh.Close()
	var decoyHits atomic.Int32
	decoy := httptest.NewServer(http.HandlerFunc(func(http.ResponseWriter, *http.Request) { decoyHits.Add(1) }))
	defer decoy.Close()

	cfg := &config.Config{}
	cfg.Tenant.GlobalReadHeader = "X-Global-Read"
	cfg.Tenant.GlobalReadValue = "s3cret"
	mux := http.NewServeMux()
	_, port, _ := net.SplitHostPort(lh.Listener.Addr().String())
	mountParity(mux, cfg, manifest.New("b", ""), nil, ":"+port)

	// Without the credential: refused, nothing asked anywhere.
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity", nil))
	if rr.Code != http.StatusForbidden || lhHits.Load() != 0 || decoyHits.Load() != 0 {
		t.Fatalf("no credential: status=%d lh=%d decoy=%d, want 403/0/0", rr.Code, lhHits.Load(), decoyHits.Load())
	}

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity", nil)
	req.Header.Set("X-Global-Read", "s3cret")
	rr = httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}
	if lhHits.Load() != 1 || lhSecret.Load() != "s3cret" {
		t.Fatalf("listen address got %d request(s), secret=%v; want 1 with the credential", lhHits.Load(), lhSecret.Load())
	}
	if decoyHits.Load() != 0 {
		t.Fatalf("another server got %d request(s): the secret must not leave the listen address", decoyHits.Load())
	}
}
