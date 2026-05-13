package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

// ---------------------------------------------------------------------------
// SyncHandler tests
// ---------------------------------------------------------------------------

func TestSyncHandlerReceiveDelta(t *testing.T) {
	reg := NewTenantRegistry("node-recv")
	handler := NewSyncHandler(reg, "")

	// Build a delta from another node.
	src := NewTenantRegistry("node-src")
	src.RecordWrite("acme:proj1", 1024, 2048, 10, "STANDARD")
	delta := src.BuildDelta(0)

	body := marshalDeltaJSON(t, delta)

	req := httptest.NewRequest(http.MethodPost, "/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", w.Code, w.Body.String())
	}

	ts := reg.Get("acme:proj1")
	if ts == nil {
		t.Fatal("expected tenant acme:proj1 after merge")
	}
	if ts.TotalBytes != 1024 {
		t.Errorf("TotalBytes = %d, want 1024", ts.TotalBytes)
	}
	if ts.TotalRows != 10 {
		t.Errorf("TotalRows = %d, want 10", ts.TotalRows)
	}
}

func TestSyncHandlerRejectsBadAuth(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	handler := NewSyncHandler(reg, "secret-key")

	req := httptest.NewRequest(http.MethodPost, "/sync", bytes.NewReader([]byte(`{}`)))
	req.Header.Set("Authorization", "Bearer wrong-key")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestSyncHandlerNoAuthWhenEmpty(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	handler := NewSyncHandler(reg, "") // no auth configured

	src := NewTenantRegistry("node-src")
	src.RecordWrite("t:1", 100, 100, 1, "STANDARD")
	delta := src.BuildDelta(0)
	body := marshalDeltaJSON(t, delta)

	// No Authorization header at all.
	req := httptest.NewRequest(http.MethodPost, "/sync", bytes.NewReader(body))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 (no auth required)", w.Code)
	}
	if reg.Get("t:1") == nil {
		t.Error("expected tenant t:1 after merge with no auth")
	}
}

func TestSyncHandlerZSTDCompression(t *testing.T) {
	reg := NewTenantRegistry("node-recv")
	handler := NewSyncHandler(reg, "")

	src := NewTenantRegistry("node-src")
	src.RecordWrite("z:1", 512, 1024, 5, "GLACIER")
	delta := src.BuildDelta(0)
	body := marshalDeltaJSON(t, delta)
	compressed := compressZSTD(body)

	req := httptest.NewRequest(http.MethodPost, "/sync", bytes.NewReader(compressed))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Content-Encoding", "zstd")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200; body = %s", w.Code, w.Body.String())
	}

	ts := reg.Get("z:1")
	if ts == nil {
		t.Fatal("expected tenant z:1 after ZSTD merge")
	}
	if ts.TotalBytes != 512 {
		t.Errorf("TotalBytes = %d, want 512", ts.TotalBytes)
	}
}

func TestSyncHandlerMethodNotAllowed(t *testing.T) {
	handler := NewSyncHandler(NewTenantRegistry("n"), "")

	req := httptest.NewRequest(http.MethodGet, "/sync", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestSyncHandlerBadJSON(t *testing.T) {
	handler := NewSyncHandler(NewTenantRegistry("n"), "")

	req := httptest.NewRequest(http.MethodPost, "/sync", bytes.NewReader([]byte(`{not json`)))
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", w.Code)
	}
}

// ---------------------------------------------------------------------------
// SyncPusher tests
// ---------------------------------------------------------------------------

func TestSyncPusherSendsDelta(t *testing.T) {
	var received []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		received, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	reg := NewTenantRegistry("node-push")
	reg.RecordWrite("acme:proj1", 256, 512, 3, "STANDARD")

	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg,
		GetPeers: func() []string { return []string{srv.URL} },
		SelfAddr: "http://self:8080",
	})

	if err := pusher.PushDelta(context.Background()); err != nil {
		t.Fatalf("PushDelta: %v", err)
	}

	if len(received) == 0 {
		t.Fatal("peer received no data")
	}

	// Verify the received payload is a valid delta.
	var dj tenantDeltaJSON
	if err := json.Unmarshal(received, &dj); err != nil {
		t.Fatalf("unmarshal received: %v", err)
	}
	if _, ok := dj.Tenants["acme:proj1"]; !ok {
		t.Error("received delta missing tenant acme:proj1")
	}
}

func TestSyncPusherSkipsSelf(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 100, 1, "STANDARD")

	// SelfAddr matches one of the peers.
	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg,
		GetPeers: func() []string { return []string{srv.URL, "http://self:9090"} },
		SelfAddr: "http://self:9090",
	})

	if err := pusher.PushDelta(context.Background()); err != nil {
		t.Fatalf("PushDelta: %v", err)
	}

	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("call count = %d, want 1 (self should be skipped)", callCount)
	}
}

func TestSyncPusherCompression(t *testing.T) {
	var receivedBody []byte
	var receivedEncoding string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		receivedEncoding = r.Header.Get("Content-Encoding")
		var err error
		receivedBody, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 100, 1, "STANDARD")

	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg,
		GetPeers: func() []string { return []string{srv.URL} },
		Compress: true,
	})

	if err := pusher.PushDelta(context.Background()); err != nil {
		t.Fatalf("PushDelta: %v", err)
	}

	if receivedEncoding != "zstd" {
		t.Errorf("Content-Encoding = %q, want %q", receivedEncoding, "zstd")
	}

	// Verify we can decompress the body back to valid JSON.
	decompressed, err := decompressZSTD(receivedBody)
	if err != nil {
		t.Fatalf("decompress: %v", err)
	}
	var dj tenantDeltaJSON
	if err := json.Unmarshal(decompressed, &dj); err != nil {
		t.Fatalf("unmarshal decompressed: %v", err)
	}
	if _, ok := dj.Tenants["a:1"]; !ok {
		t.Error("decompressed delta missing tenant a:1")
	}
}

func TestSyncPusherNoPeers(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 100, 1, "STANDARD")

	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg,
		GetPeers: func() []string { return nil },
	})

	if err := pusher.PushDelta(context.Background()); err != nil {
		t.Fatalf("PushDelta with no peers: %v", err)
	}
}

func TestSyncPusherNoDelta(t *testing.T) {
	var callCount int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&callCount, 1)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	reg := NewTenantRegistry("node-1")
	// Record a write and push it.
	reg.RecordWrite("a:1", 100, 100, 1, "STANDARD")

	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg,
		GetPeers: func() []string { return []string{srv.URL} },
	})

	// First push sends the delta.
	if err := pusher.PushDelta(context.Background()); err != nil {
		t.Fatalf("first PushDelta: %v", err)
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Fatalf("first push call count = %d, want 1", callCount)
	}

	// Second push with no new changes — no HTTP call.
	if err := pusher.PushDelta(context.Background()); err != nil {
		t.Fatalf("second PushDelta: %v", err)
	}
	if atomic.LoadInt32(&callCount) != 1 {
		t.Errorf("second push call count = %d, want 1 (no new delta)", callCount)
	}
}

func TestSyncPusherPushFull(t *testing.T) {
	var received []byte
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var err error
		received, err = io.ReadAll(r.Body)
		if err != nil {
			t.Errorf("read body: %v", err)
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("a:1", 100, 200, 1, "STANDARD")
	reg.RecordWrite("b:2", 300, 400, 3, "GLACIER")

	// Push delta first to advance lastPushGen.
	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg,
		GetPeers: func() []string { return []string{srv.URL} },
	})
	if err := pusher.PushDelta(context.Background()); err != nil {
		t.Fatalf("PushDelta: %v", err)
	}

	// PushFull should still send everything (from gen 0).
	if err := pusher.PushFull(context.Background()); err != nil {
		t.Fatalf("PushFull: %v", err)
	}

	var dj tenantDeltaJSON
	if err := json.Unmarshal(received, &dj); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if len(dj.Tenants) != 2 {
		t.Errorf("PushFull sent %d tenants, want 2", len(dj.Tenants))
	}
}

// ---------------------------------------------------------------------------
// S3 snapshot round-trip through sync layer
// ---------------------------------------------------------------------------

func TestSyncS3SnapshotRoundTrip(t *testing.T) {
	reg := NewTenantRegistry("node-1")
	reg.RecordWrite("acme:proj1", 1024, 2048, 10, "STANDARD")
	reg.RecordWrite("beta:proj2", 512, 1024, 5, "GLACIER")
	reg.RecordQuery("acme:proj1")

	data, err := reg.MarshalSnapshot()
	if err != nil {
		t.Fatalf("MarshalSnapshot: %v", err)
	}

	// Optionally compress like we would for S3.
	compressed := compressZSTD(data)
	decompressed, err := decompressZSTD(compressed)
	if err != nil {
		t.Fatalf("decompressZSTD: %v", err)
	}

	reg2 := NewTenantRegistry("node-2")
	if err := reg2.LoadSnapshot("node-1", decompressed); err != nil {
		t.Fatalf("LoadSnapshot: %v", err)
	}

	if reg2.TenantCount() != 2 {
		t.Errorf("TenantCount = %d, want 2", reg2.TenantCount())
	}

	ts := reg2.Get("acme:proj1")
	if ts == nil {
		t.Fatal("tenant acme:proj1 not found after round-trip")
	}
	if ts.TotalBytes != 1024 {
		t.Errorf("TotalBytes = %d, want 1024", ts.TotalBytes)
	}
	if ts.TotalRows != 10 {
		t.Errorf("TotalRows = %d, want 10", ts.TotalRows)
	}

	ts2 := reg2.Get("beta:proj2")
	if ts2 == nil {
		t.Fatal("tenant beta:proj2 not found after round-trip")
	}
	if ts2.TotalBytes != 512 {
		t.Errorf("TotalBytes = %d, want 512", ts2.TotalBytes)
	}
}

// ---------------------------------------------------------------------------
// Compression tests
// ---------------------------------------------------------------------------

func TestCompressDecompressZSTD(t *testing.T) {
	original := []byte(`{"node_id":"n1","generation":42,"tenants":{"a:1":{"total_bytes":100}}}`)
	compressed := compressZSTD(original)

	// Compressed should be different from original (and typically shorter for
	// non-trivial inputs, but we don't assert size).
	if bytes.Equal(compressed, original) {
		t.Error("compressed data equals original — expected transformation")
	}

	decompressed, err := decompressZSTD(compressed)
	if err != nil {
		t.Fatalf("decompressZSTD: %v", err)
	}
	if !bytes.Equal(decompressed, original) {
		t.Errorf("round-trip mismatch:\n  got:  %s\n  want: %s", decompressed, original)
	}
}

func TestDecompressZSTDInvalidData(t *testing.T) {
	_, err := decompressZSTD([]byte("this is not zstd data"))
	if err == nil {
		t.Error("expected error decompressing invalid data, got nil")
	}
}

// ---------------------------------------------------------------------------
// Helpers
// ---------------------------------------------------------------------------

// marshalDeltaJSON serialises a TenantDelta via its JSON mirror type.
func marshalDeltaJSON(t *testing.T, d *TenantDelta) []byte {
	t.Helper()
	dj := tenantDeltaToJSON(d)
	data, err := json.Marshal(dj)
	if err != nil {
		t.Fatalf("marshal delta: %v", err)
	}
	return data
}
