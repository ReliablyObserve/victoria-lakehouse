package peercache

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestPeerCache_UpdatePeersWithZones(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		"self:9428":   "az-a",
		"peer-a:9428": "az-a",
		"peer-b:9428": "az-b",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	if len(pc.Members()) != 3 {
		t.Errorf("expected 3 members, got %d", len(pc.Members()))
	}
	if pc.SelfAZ() != "az-a" {
		t.Errorf("expected selfAZ=az-a, got %q", pc.SelfAZ())
	}
}

func TestPeerCache_LookupAZ_RoutesSameZone(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	peerZones := map[string]string{
		"self:9428":    "az-a",
		"peer-a:9428":  "az-a",
		"peer-b1:9428": "az-b",
		"peer-b2:9428": "az-b",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	crossAZ := 0
	for i := 0; i < 500; i++ {
		_, _, isSameAZ := pc.LookupAZ(fmt.Sprintf("file-%d.parquet", i))
		if !isSameAZ {
			crossAZ++
		}
	}

	if crossAZ > 0 {
		t.Errorf("expected 0 cross-AZ lookups, got %d", crossAZ)
	}
}

func TestPeerCache_StatsAZ(t *testing.T) {
	pc := New("self:9428", "", 5*time.Second, 10)

	stats := pc.StatsAZ()
	if stats.SelfAZ != "" {
		t.Errorf("expected empty selfAZ before zone config, got %q", stats.SelfAZ)
	}

	peerZones := map[string]string{
		"self:9428": "az-a",
		"peer:9428": "az-b",
	}
	pc.UpdatePeersWithZones(peerZones, "az-a")

	stats = pc.StatsAZ()
	if stats.SelfAZ != "az-a" {
		t.Errorf("expected selfAZ=az-a, got %q", stats.SelfAZ)
	}
	if stats.SameAZMembers != 1 {
		t.Errorf("expected 1 same-AZ member, got %d", stats.SameAZMembers)
	}
	if stats.CrossAZMembers != 1 {
		t.Errorf("expected 1 cross-AZ member, got %d", stats.CrossAZMembers)
	}
}

func TestHandler_StatsEndpoint_IncludesAZ(t *testing.T) {
	h := NewHandler("", "us-east-1a")
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/internal/cache/stats")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp.StatusCode)
	}

	var result struct {
		AZ string `json:"az"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.AZ != "us-east-1a" {
		t.Errorf("expected az=us-east-1a, got %q", result.AZ)
	}
}

func TestHandler_StatsEndpoint_EmptyAZ(t *testing.T) {
	h := NewHandler("", "")
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/internal/cache/stats")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer resp.Body.Close()

	var result struct {
		AZ string `json:"az"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.AZ != "" {
		t.Errorf("expected empty az, got %q", result.AZ)
	}
}
