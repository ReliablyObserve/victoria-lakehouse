package peercache

import (
	"context"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestPeerCache_Fetch_Hit(t *testing.T) {
	handler := NewHandler("")
	handler.Put("test-key", []byte("test-data"))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	pc := New("self:9428", "", 5*time.Second, 10, testLogger())

	data, found, err := pc.Fetch(context.Background(), srv.Listener.Addr().String(), "test-key")
	if err != nil {
		t.Fatal(err)
	}
	if !found {
		t.Fatal("expected hit")
	}
	if string(data) != "test-data" {
		t.Errorf("data = %q, want test-data", data)
	}

	stats := pc.Stats()
	if stats.Hits != 1 {
		t.Errorf("hits = %d, want 1", stats.Hits)
	}
}

func TestPeerCache_Fetch_Miss(t *testing.T) {
	handler := NewHandler("")
	srv := httptest.NewServer(handler)
	defer srv.Close()

	pc := New("self:9428", "", 5*time.Second, 10, testLogger())

	_, found, err := pc.Fetch(context.Background(), srv.Listener.Addr().String(), "missing-key")
	if err != nil {
		t.Fatal(err)
	}
	if found {
		t.Error("expected miss")
	}

	stats := pc.Stats()
	if stats.Misses != 1 {
		t.Errorf("misses = %d, want 1", stats.Misses)
	}
}

func TestPeerCache_Fetch_WithAuth(t *testing.T) {
	handler := NewHandler("secret-key")
	handler.Put("k", []byte("v"))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	pc := New("self:9428", "secret-key", 5*time.Second, 10, testLogger())
	data, found, err := pc.Fetch(context.Background(), srv.Listener.Addr().String(), "k")
	if err != nil {
		t.Fatal(err)
	}
	if !found || string(data) != "v" {
		t.Errorf("found=%v data=%q", found, data)
	}
}

func TestPeerCache_Fetch_AuthRejected(t *testing.T) {
	handler := NewHandler("correct-key")
	handler.Put("k", []byte("v"))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	pc := New("self:9428", "wrong-key", 5*time.Second, 10, testLogger())
	_, _, err := pc.Fetch(context.Background(), srv.Listener.Addr().String(), "k")
	if err == nil {
		t.Error("expected error for wrong auth key")
	}
}

func TestPeerCache_Has(t *testing.T) {
	handler := NewHandler("")
	handler.Put("exists", []byte("data"))

	srv := httptest.NewServer(handler)
	defer srv.Close()

	pc := New("self:9428", "", 5*time.Second, 10, testLogger())

	ok, err := pc.Has(context.Background(), srv.Listener.Addr().String(), "exists")
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Error("expected Has=true for existing key")
	}

	ok, err = pc.Has(context.Background(), srv.Listener.Addr().String(), "missing")
	if err != nil {
		t.Fatal(err)
	}
	if ok {
		t.Error("expected Has=false for missing key")
	}
}

func TestPeerCache_UpdatePeers(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10, testLogger())
	pc.UpdatePeers([]string{"a:1", "b:1", "c:1"})

	stats := pc.Stats()
	if stats.Members != 3 {
		t.Errorf("members = %d, want 3", stats.Members)
	}

	members := pc.Members()
	if len(members) != 3 {
		t.Errorf("members list = %d, want 3", len(members))
	}
}

func TestPeerCache_Lookup(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10, testLogger())
	pc.UpdatePeers([]string{"self:9428", "peer-1:9428"})

	peer, isLocal := pc.Lookup("some-key")
	if peer == "" {
		t.Error("expected non-empty peer")
	}
	_ = isLocal
}

func TestHandler_MissingKey(t *testing.T) {
	handler := NewHandler("")
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/internal/cache/fetch")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusBadRequest {
		t.Errorf("status = %d, want 400", resp.StatusCode)
	}
}

func TestHandler_UnknownPath(t *testing.T) {
	handler := NewHandler("")
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/internal/cache/unknown?key=k")
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Errorf("status = %d, want 404", resp.StatusCode)
	}
}

func TestHandler_DataCopy(t *testing.T) {
	handler := NewHandler("")
	original := []byte("original-data")
	handler.Put("k", original)

	original[0] = 'X'

	data, ok := handler.Get("k")
	if !ok {
		t.Fatal("expected hit")
	}
	if string(data) != "original-data" {
		t.Errorf("data = %q, expected original-data (copy should be independent)", data)
	}
}
