package parquets3

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/peercache"
)

func TestStorage_SetSelfAZ_SetsField(t *testing.T) {
	s := &Storage{}
	s.SetSelfAZ("us-east-1a")

	if s.selfAZ != "us-east-1a" {
		t.Errorf("expected selfAZ=us-east-1a, got %q", s.selfAZ)
	}
}

func TestStorage_SelfAZ_ReturnsValue(t *testing.T) {
	s := &Storage{}

	if s.SelfAZ() != "" {
		t.Errorf("expected empty initially, got %q", s.SelfAZ())
	}

	s.selfAZ = "eu-west-1c"
	if s.SelfAZ() != "eu-west-1c" {
		t.Errorf("expected eu-west-1c, got %q", s.SelfAZ())
	}
}

func TestStorage_SetSelfAZ_PropagesToHandler(t *testing.T) {
	s := &Storage{}
	s.peerHandler = peercache.NewHandler("", "")

	s.SetSelfAZ("az-b")

	if s.selfAZ != "az-b" {
		t.Errorf("expected selfAZ=az-b on storage, got %q", s.selfAZ)
	}

	// Verify handler also got updated
	srv := httptest.NewServer(s.peerHandler)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/internal/cache/stats")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		AZ string `json:"az"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.AZ != "az-b" {
		t.Errorf("expected handler AZ=az-b, got %q", result.AZ)
	}
}

func TestStorage_SetSelfAZ_NilHandler(t *testing.T) {
	s := &Storage{}
	// Should not panic with nil peerHandler
	s.SetSelfAZ("az-a")
	if s.selfAZ != "az-a" {
		t.Errorf("expected selfAZ=az-a, got %q", s.selfAZ)
	}
}

func TestStorage_FetchPeerAZ_Success(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/cache/stats" {
			http.NotFound(w, r)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"az":"us-west-2a"}`))
	}))
	defer srv.Close()

	s := &Storage{cfg: testConfig()}
	az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())
	if az != "us-west-2a" {
		t.Errorf("expected us-west-2a, got %q", az)
	}
}

func TestStorage_FetchPeerAZ_WithAuth(t *testing.T) {
	var gotHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotHeader = r.Header.Get("X-Peer-Auth-Key")
		_, _ = w.Write([]byte(`{"az":"az-a"}`))
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.Peer.AuthKey = "test-secret"
	s := &Storage{cfg: cfg}
	s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())

	if gotHeader != "test-secret" {
		t.Errorf("expected X-Peer-Auth-Key=test-secret, got %q", gotHeader)
	}
}

func TestStorage_FetchPeerAZ_ServerDown(t *testing.T) {
	s := &Storage{cfg: testConfig()}
	az := s.fetchPeerAZ(context.Background(), "127.0.0.1:1")
	if az != "" {
		t.Errorf("expected empty for unreachable peer, got %q", az)
	}
}

func TestStorage_FetchPeerAZ_InvalidJSON(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte("not json"))
	}))
	defer srv.Close()

	s := &Storage{cfg: testConfig()}
	az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())
	if az != "" {
		t.Errorf("expected empty for invalid JSON, got %q", az)
	}
}

func TestStorage_FetchPeerAZ_EmptyAZ(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"az":""}`))
	}))
	defer srv.Close()

	s := &Storage{cfg: testConfig()}
	az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())
	if az != "" {
		t.Errorf("expected empty AZ, got %q", az)
	}
}

func TestStorage_QueryPeerAZs(t *testing.T) {
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"az":"az-a"}`))
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"az":"az-b"}`))
	}))
	defer srv2.Close()

	s := &Storage{cfg: testConfig()}
	peers := []string{srv1.Listener.Addr().String(), srv2.Listener.Addr().String()}
	zones := s.queryPeerAZs(context.Background(), peers)

	if len(zones) != 2 {
		t.Fatalf("expected 2 zones, got %d", len(zones))
	}
	if zones[srv1.Listener.Addr().String()] != "az-a" {
		t.Errorf("expected az-a for peer1, got %q", zones[srv1.Listener.Addr().String()])
	}
	if zones[srv2.Listener.Addr().String()] != "az-b" {
		t.Errorf("expected az-b for peer2, got %q", zones[srv2.Listener.Addr().String()])
	}
}

func TestStorage_QueryPeerAZs_MixedAvailability(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(`{"az":"az-a"}`))
	}))
	defer srv.Close()

	s := &Storage{cfg: testConfig()}
	peers := []string{srv.Listener.Addr().String(), "127.0.0.1:1"}
	zones := s.queryPeerAZs(context.Background(), peers)

	if len(zones) != 2 {
		t.Fatalf("expected 2 entries, got %d", len(zones))
	}
	if zones[srv.Listener.Addr().String()] != "az-a" {
		t.Errorf("reachable peer should have AZ, got %q", zones[srv.Listener.Addr().String()])
	}
	if zones["127.0.0.1:1"] != "" {
		t.Errorf("unreachable peer should have empty AZ, got %q", zones["127.0.0.1:1"])
	}
}
