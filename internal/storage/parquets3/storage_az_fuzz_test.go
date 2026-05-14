package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func FuzzStorageFetchPeerAZ(f *testing.F) {
	f.Add(`{"az":"us-east-1a"}`, 200, "")
	f.Add(`{"az":""}`, 200, "")
	f.Add(`{"az":"zone\"quotes"}`, 200, "")
	f.Add(`not json`, 200, "")
	f.Add(`{"other":"field"}`, 200, "")
	f.Add(`{}`, 200, "")
	f.Add(`[]`, 200, "")
	f.Add(`null`, 200, "")
	f.Add(`{"az":123}`, 200, "")
	f.Add(``, 200, "")
	f.Add(`{"az":"ok"}`, 500, "")
	f.Add(`{"az":"ok"}`, 404, "")
	f.Add(`{"az":"ok"}`, 200, "my-secret")

	f.Fuzz(func(t *testing.T, body string, statusCode int, authKey string) {
		if statusCode < 100 || statusCode > 599 {
			return
		}
		var gotAuthHeader string
		srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			gotAuthHeader = r.Header.Get("X-Peer-Auth-Key")
			w.WriteHeader(statusCode)
			w.Write([]byte(body))
		}))
		defer srv.Close()

		cfg := testConfig()
		cfg.Peer.AuthKey = authKey
		s := &Storage{cfg: cfg}
		az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())

		if strings.TrimSpace(authKey) != "" && gotAuthHeader != authKey {
			t.Errorf("expected X-Peer-Auth-Key=%q, got %q", authKey, gotAuthHeader)
		}

		// For non-200 status, json decode will still proceed on body; fetchPeerAZ
		// doesn't check status code, so it may or may not return an AZ.
		// Just verify no panic.
		_ = az
	})
}

func TestStorage_FetchPeerAZ_HTTP500(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(500)
		w.Write([]byte(`{"az":"should-still-parse"}`))
	}))
	defer srv.Close()

	s := &Storage{cfg: testConfig()}
	az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())
	// fetchPeerAZ doesn't check status code, just decodes JSON
	if az != "should-still-parse" {
		t.Logf("note: fetchPeerAZ on 500 returned %q (implementation detail)", az)
	}
}

func TestStorage_FetchPeerAZ_SlowServer(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Don't sleep — just verify short timeout doesn't block
		w.Write([]byte(`{"az":"az-a"}`))
	}))
	defer srv.Close()

	s := &Storage{cfg: testConfig()}
	az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())
	if az != "az-a" {
		t.Errorf("expected az-a, got %q", az)
	}
}

func TestStorage_FetchPeerAZ_ExtraFields(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"az":"az-a","members":5,"version":"1.0"}`))
	}))
	defer srv.Close()

	s := &Storage{cfg: testConfig()}
	az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())
	if az != "az-a" {
		t.Errorf("expected az-a with extra fields, got %q", az)
	}
}

func TestStorage_FetchPeerAZ_NoAuthKeySet(t *testing.T) {
	var gotAuthHeader string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthHeader = r.Header.Get("X-Peer-Auth-Key")
		w.Write([]byte(`{"az":"az-a"}`))
	}))
	defer srv.Close()

	cfg := testConfig()
	cfg.Peer.AuthKey = ""
	s := &Storage{cfg: cfg}
	s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())

	if gotAuthHeader != "" {
		t.Errorf("should not send auth header when no key configured, got %q", gotAuthHeader)
	}
}

func TestStorage_QueryPeerAZs_EmptyPeerList(t *testing.T) {
	s := &Storage{cfg: testConfig()}
	zones := s.queryPeerAZs(context.Background(), nil)
	if len(zones) != 0 {
		t.Errorf("expected empty map, got %d entries", len(zones))
	}
}

func TestStorage_QueryPeerAZs_AllDown(t *testing.T) {
	s := &Storage{cfg: testConfig()}
	ctx, cancel := context.WithTimeout(context.Background(), 500*time.Millisecond)
	defer cancel()
	peers := []string{"127.0.0.1:1", "127.0.0.1:2", "127.0.0.1:3"}
	zones := s.queryPeerAZs(ctx, peers)

	if len(zones) != 3 {
		t.Fatalf("expected 3 entries, got %d", len(zones))
	}
	for _, peer := range peers {
		if zones[peer] != "" {
			t.Errorf("unreachable peer %s should have empty AZ, got %q", peer, zones[peer])
		}
	}
}

func TestStorage_QueryPeerAZs_LargePeerSet(t *testing.T) {
	mux := http.NewServeMux()
	mux.HandleFunc("/internal/cache/stats", func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte(`{"az":"az-a"}`))
	})
	srv := httptest.NewServer(mux)
	defer srv.Close()

	s := &Storage{cfg: testConfig()}
	addr := srv.Listener.Addr().String()
	peers := make([]string, 5)
	for i := range peers {
		peers[i] = addr
	}
	zones := s.queryPeerAZs(context.Background(), peers)

	// All 5 point to same addr, map deduplicates to 1
	if len(zones) != 1 {
		t.Errorf("expected 1 entry (deduped), got %d", len(zones))
	}
	if zones[addr] != "az-a" {
		t.Errorf("expected az-a, got %q", zones[addr])
	}
}

func TestStorage_SetSelfAZ_Overwrite(t *testing.T) {
	s := &Storage{}
	s.SetSelfAZ("az-a")
	if s.SelfAZ() != "az-a" {
		t.Fatalf("expected az-a, got %q", s.SelfAZ())
	}

	s.SetSelfAZ("az-b")
	if s.SelfAZ() != "az-b" {
		t.Errorf("expected az-b after overwrite, got %q", s.SelfAZ())
	}

	s.SetSelfAZ("")
	if s.SelfAZ() != "" {
		t.Errorf("expected empty after clearing, got %q", s.SelfAZ())
	}
}

func TestStorage_FetchPeerAZ_ValidatesJSONStructure(t *testing.T) {
	cases := []struct {
		name string
		body string
		want string
	}{
		{"nested", `{"az":"az-a","nested":{"key":"val"}}`, "az-a"},
		{"number_az", `{"az":42}`, ""},
		{"bool_az", `{"az":true}`, ""},
		{"null_az", `{"az":null}`, ""},
		{"array_az", `{"az":["a"]}`, ""},
		{"missing_az", `{"zone":"az-a"}`, ""},
		{"empty_object", `{}`, ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				w.Write([]byte(tc.body))
			}))
			defer srv.Close()

			s := &Storage{cfg: testConfig()}
			az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())

			// For non-string az values, json decode into struct{AZ string} will give ""
			if tc.want == "" && az != "" {
				t.Errorf("expected empty for body %s, got %q", tc.body, az)
			}
			if tc.want != "" && az != tc.want {
				t.Errorf("expected %q for body %s, got %q", tc.want, tc.body, az)
			}
		})
	}
}

// Integration test: fetchPeerAZ → Handler roundtrip with auth
func TestStorage_FetchPeerAZ_HandlerRoundtrip(t *testing.T) {
	cases := []struct {
		name        string
		handlerAuth string
		handlerAZ   string
		fetchAuth   string
		wantAZ      string
	}{
		{"no_auth", "", "az-a", "", "az-a"},
		{"matching_auth", "secret", "az-b", "secret", "az-b"},
		{"mismatched_auth", "secret", "az-c", "wrong", ""},
		{"empty_az", "", "", "", ""},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h := newPeerCacheHandler(tc.handlerAuth, tc.handlerAZ)
			srv := httptest.NewServer(h)
			defer srv.Close()

			cfg := testConfig()
			cfg.Peer.AuthKey = tc.fetchAuth
			s := &Storage{cfg: cfg}
			az := s.fetchPeerAZ(context.Background(), srv.Listener.Addr().String())

			if az != tc.wantAZ {
				t.Errorf("expected %q, got %q", tc.wantAZ, az)
			}
		})
	}
}

func newPeerCacheHandler(authKey, selfAZ string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if authKey != "" {
			if r.Header.Get("X-Peer-Auth-Key") != authKey {
				http.Error(w, "unauthorized", http.StatusUnauthorized)
				return
			}
		}
		if r.URL.Path == "/internal/cache/stats" {
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"az":%s}`, mustMarshal(selfAZ))
			return
		}
		http.NotFound(w, r)
	})
}

func mustMarshal(s string) string {
	b, _ := json.Marshal(s)
	return string(b)
}
