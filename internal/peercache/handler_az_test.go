package peercache

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandler_SetSelfAZ(t *testing.T) {
	h := NewHandler("", "")

	h.SetSelfAZ("us-west-2a")

	h.mu.RLock()
	got := h.selfAZ
	h.mu.RUnlock()

	if got != "us-west-2a" {
		t.Errorf("expected us-west-2a, got %q", got)
	}
}

func TestHandler_SetSelfAZ_UpdatesStatsEndpoint(t *testing.T) {
	h := NewHandler("", "")
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Initially empty
	az := fetchAZFromStats(t, srv.URL)
	if az != "" {
		t.Errorf("expected empty AZ initially, got %q", az)
	}

	// Update via SetSelfAZ
	h.SetSelfAZ("eu-central-1b")

	az = fetchAZFromStats(t, srv.URL)
	if az != "eu-central-1b" {
		t.Errorf("expected eu-central-1b after SetSelfAZ, got %q", az)
	}
}

func TestHandler_StatsEndpoint_WithAuth(t *testing.T) {
	h := NewHandler("secret-key", "az-a")
	srv := httptest.NewServer(h)
	defer srv.Close()

	// Without auth header — should fail
	resp, err := http.Get(srv.URL + "/internal/cache/stats")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 without auth, got %d", resp.StatusCode)
	}

	// With wrong auth header — should fail
	req, _ := http.NewRequest("GET", srv.URL+"/internal/cache/stats", nil)
	req.Header.Set("X-Peer-Auth-Key", "wrong-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Errorf("expected 401 with wrong auth, got %d", resp.StatusCode)
	}

	// With correct auth header — should succeed
	req, _ = http.NewRequest("GET", srv.URL+"/internal/cache/stats", nil)
	req.Header.Set("X-Peer-Auth-Key", "secret-key")
	resp, err = http.DefaultClient.Do(req)
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("expected 200 with correct auth, got %d", resp.StatusCode)
	}

	var result struct {
		AZ string `json:"az"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if result.AZ != "az-a" {
		t.Errorf("expected az=az-a, got %q", result.AZ)
	}
}

func TestHandler_StatsEndpoint_ContentType(t *testing.T) {
	h := NewHandler("", "az-a")
	srv := httptest.NewServer(h)
	defer srv.Close()

	resp, err := http.Get(srv.URL + "/internal/cache/stats")
	if err != nil {
		t.Fatalf("request failed: %v", err)
	}
	resp.Body.Close()

	ct := resp.Header.Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("expected Content-Type application/json, got %q", ct)
	}
}

func fetchAZFromStats(t *testing.T, baseURL string) string {
	t.Helper()
	resp, err := http.Get(baseURL + "/internal/cache/stats")
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
	return result.AZ
}
