package peercache

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestAZIntegration_FullFlow(t *testing.T) {
	handlerA := NewHandler("", "az-a")
	handlerA.Put("shared-key", []byte("data-from-az-a"))

	handlerB := NewHandler("", "az-b")
	handlerB.Put("shared-key", []byte("data-from-az-b"))

	serverA := httptest.NewServer(handlerA)
	defer serverA.Close()
	serverB := httptest.NewServer(handlerB)
	defer serverB.Close()

	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		serverA.Listener.Addr().String(): "az-a",
		serverB.Listener.Addr().String(): "az-b",
		"self:9428":                      "az-a",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	stats := pc.StatsAZ()
	if stats.SameAZMembers != 2 {
		t.Errorf("expected 2 same-AZ, got %d", stats.SameAZMembers)
	}
	if stats.CrossAZMembers != 1 {
		t.Errorf("expected 1 cross-AZ, got %d", stats.CrossAZMembers)
	}

	crossAZ := 0
	for i := 0; i < 200; i++ {
		_, _, isSameAZ := pc.LookupAZ(fmt.Sprintf("key-%d", i))
		if !isSameAZ {
			crossAZ++
		}
	}
	if crossAZ > 0 {
		t.Errorf("expected 0 cross-AZ lookups, got %d/200", crossAZ)
	}
}

func TestAZIntegration_StatsEndpoint(t *testing.T) {
	handler := NewHandler("", "us-east-1a")
	server := httptest.NewServer(handler)
	defer server.Close()

	resp, err := http.Get(server.URL + "/internal/cache/stats")
	if err != nil {
		t.Fatalf("stats request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	var result struct {
		AZ string `json:"az"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode failed: %v", err)
	}
	if result.AZ != "us-east-1a" {
		t.Errorf("expected az=us-east-1a, got %q", result.AZ)
	}
}

func TestAZIntegration_PeerAZDiscovery(t *testing.T) {
	handler1 := NewHandler("", "az-a")
	handler2 := NewHandler("", "az-b")
	srv1 := httptest.NewServer(handler1)
	defer srv1.Close()
	srv2 := httptest.NewServer(handler2)
	defer srv2.Close()

	peers := []string{srv1.Listener.Addr().String(), srv2.Listener.Addr().String()}
	peerZones := make(map[string]string)

	for _, peer := range peers {
		resp, err := http.Get(fmt.Sprintf("http://%s/internal/cache/stats", peer))
		if err != nil {
			t.Fatalf("query peer %s: %v", peer, err)
		}
		var result struct {
			AZ string `json:"az"`
		}
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			t.Fatalf("decode peer %s: %v", peer, err)
		}
		_ = resp.Body.Close()
		peerZones[peer] = result.AZ
	}

	if len(peerZones) != 2 {
		t.Fatalf("expected 2 peer zones, got %d", len(peerZones))
	}
	if peerZones[peers[0]] != "az-a" {
		t.Errorf("peer 0 should be az-a, got %q", peerZones[peers[0]])
	}
	if peerZones[peers[1]] != "az-b" {
		t.Errorf("peer 1 should be az-b, got %q", peerZones[peers[1]])
	}
}
