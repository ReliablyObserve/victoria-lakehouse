package stats

import (
	"bytes"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/peerauth"
)

// The stats sync body is capped at peer.max_request_body_bytes, and so is the
// size a zstd body expands to. The largest legitimate push (a delta carrying
// every tenant of a very large fleet) must pass with the default cap.

func syncDeltaBody(t *testing.T, tenants int) []byte {
	t.Helper()
	src := NewTenantRegistry("node-src")
	for i := 0; i < tenants; i++ {
		src.RecordWrite(fmt.Sprintf("%d:%d", i, i%7), 4096, 8192, 100, "STANDARD")
	}
	return marshalDeltaJSON(t, src.BuildDelta(0))
}

func postSync(h http.Handler, body []byte, zstd bool) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, "/internal/stats/sync", bytes.NewReader(body))
	if zstd {
		req.Header.Set("Content-Encoding", "zstd")
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	return w
}

func TestSyncHandler_LargestLegitimateBodyPassesDefaultCap(t *testing.T) {
	t.Cleanup(func() { peerauth.SetMaxBodyBytes(0) })
	peerauth.SetMaxBodyBytes(0)

	// 20 000 tenants is far above a real fleet (the registry tracks one entry
	// per account:project); measured below, it stays well under the cap.
	body := syncDeltaBody(t, 20000)
	t.Logf("stats sync body for 20000 tenants: %d bytes plain, %d bytes zstd (default cap %d)", len(body), len(compressZSTD(body)), peerauth.DefaultMaxBodyBytes)
	if int64(len(body))*4 > peerauth.DefaultMaxBodyBytes {
		t.Fatalf("the largest legitimate body (%d bytes) leaves no headroom under the default cap %d", len(body), peerauth.DefaultMaxBodyBytes)
	}

	for _, tc := range []struct {
		name string
		body []byte
		zstd bool
	}{{"plain", body, false}, {"zstd", compressZSTD(body), true}} {
		reg := NewTenantRegistry("node-recv")
		if w := postSync(NewSyncHandler(reg, ""), tc.body, tc.zstd); w.Code != http.StatusOK {
			t.Fatalf("%s: %d %q, want 200", tc.name, w.Code, w.Body.String())
		}
		if got := reg.TenantCount(); got != 20000 {
			t.Errorf("%s: %d tenants merged, want 20000", tc.name, got)
		}
	}
}

func TestSyncHandler_BodyJustOverTheCapIs413(t *testing.T) {
	t.Cleanup(func() { peerauth.SetMaxBodyBytes(0) })
	body := syncDeltaBody(t, 50)

	peerauth.SetMaxBodyBytes(int64(len(body)))
	reg := NewTenantRegistry("node-recv")
	if w := postSync(NewSyncHandler(reg, ""), body, false); w.Code != http.StatusOK {
		t.Fatalf("a body exactly at the cap: %d %q, want 200", w.Code, w.Body.String())
	}

	peerauth.SetMaxBodyBytes(int64(len(body)) - 1)
	reg = NewTenantRegistry("node-recv")
	w := postSync(NewSyncHandler(reg, ""), body, false)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("a body one byte over the cap: %d %q, want 413", w.Code, w.Body.String())
	}
	if reg.TenantCount() != 0 {
		t.Error("an oversized body was merged")
	}
}

// A small compressed body that expands past the cap is refused: the cap is on
// the decompressed size too.
func TestSyncHandler_ZstdBombRefused(t *testing.T) {
	t.Cleanup(func() { peerauth.SetMaxBodyBytes(0) })
	peerauth.SetMaxBodyBytes(1 << 20)
	bomb := compressZSTD(make([]byte, 64<<20))
	if int64(len(bomb)) >= peerauth.MaxBodyBytes() {
		t.Fatalf("the test bomb is not small: %d bytes", len(bomb))
	}
	reg := NewTenantRegistry("node-recv")
	w := postSync(NewSyncHandler(reg, ""), bomb, true)
	if w.Code != http.StatusBadRequest || !strings.HasPrefix(w.Body.String(), "decompress:") {
		t.Fatalf("a body expanding past the cap: %d %q, want 400 from the decompression", w.Code, w.Body.String())
	}
	if reg.TenantCount() != 0 {
		t.Error("a bomb was merged")
	}
}

// The cap is independent of the key: an enforcing handler caps the body of an
// authenticated peer too, and a refused request never reads its body.
func TestSyncHandler_CapAppliesWithAKey(t *testing.T) {
	t.Cleanup(func() { peerauth.SetMaxBodyBytes(0) })
	body := syncDeltaBody(t, 50)
	peerauth.SetMaxBodyBytes(int64(len(body)) - 1)
	h := NewSyncHandler(NewTenantRegistry("n"), "k")

	req := httptest.NewRequest(http.MethodPost, "/internal/stats/sync", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer k")
	w := httptest.NewRecorder()
	h.ServeHTTP(w, req)
	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("authenticated oversized body: %d, want 413", w.Code)
	}

	w = postSync(h, body, false)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("unauthenticated oversized body: %d, want 401 before anything is read", w.Code)
	}
}
