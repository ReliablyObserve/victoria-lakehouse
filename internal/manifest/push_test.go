package manifest

import (
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestPusher_NotifiesAllPeers(t *testing.T) {
	var received atomic.Int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost {
			t.Errorf("expected POST, got %s", r.Method)
		}
		if r.URL.Path != "/internal/manifest/update" {
			t.Errorf("expected /internal/manifest/update, got %s", r.URL.Path)
		}
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	})

	srv1 := httptest.NewServer(handler)
	defer srv1.Close()
	srv2 := httptest.NewServer(handler)
	defer srv2.Close()

	peers := func() []string {
		return []string{
			srv1.Listener.Addr().String(),
			srv2.Listener.Addr().String(),
		}
	}

	p := NewPusher(PusherConfig{
		GetPeers:   peers,
		AuthSecret: "test-secret",
		SelfAddr:   "10.0.0.99:9428",
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	added := []FileInfo{{Key: "logs/dt=2026-05-02/hour=10/new.parquet", Size: 1000}}
	removed := []string{"logs/dt=2026-05-02/hour=10/old.parquet"}

	p.Notify(added, removed)
	time.Sleep(200 * time.Millisecond)

	if got := received.Load(); got != 2 {
		t.Fatalf("expected 2 peers notified, got %d", got)
	}
}

func TestPusher_SkipsSelf(t *testing.T) {
	var received atomic.Int32

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		received.Add(1)
		w.WriteHeader(http.StatusOK)
	})
	srv := httptest.NewServer(handler)
	defer srv.Close()

	selfAddr := srv.Listener.Addr().String()

	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{selfAddr} },
		AuthSecret: "",
		SelfAddr:   selfAddr,
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	p.Notify([]FileInfo{{Key: "test"}}, nil)
	time.Sleep(200 * time.Millisecond)

	if got := received.Load(); got != 0 {
		t.Fatalf("expected 0 (self skipped), got %d", got)
	}
}

func TestPusher_AuthHeader(t *testing.T) {
	var gotAuth string

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuth = r.Header.Get("Authorization")
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	p := NewPusher(PusherConfig{
		GetPeers:   func() []string { return []string{srv.Listener.Addr().String()} },
		AuthSecret: "my-secret",
		SelfAddr:   "10.0.0.99:9428",
		Logger:     slog.New(slog.NewTextHandler(io.Discard, nil)),
	})

	p.Notify([]FileInfo{{Key: "test"}}, nil)
	time.Sleep(200 * time.Millisecond)

	expected := "Bearer my-secret"
	if gotAuth != expected {
		t.Fatalf("expected auth %q, got %q", expected, gotAuth)
	}
}

func TestManifestUpdatePayload(t *testing.T) {
	payload := ManifestUpdate{
		Added:   []FileInfo{{Key: "a.parquet", Size: 100}},
		Removed: []string{"b.parquet"},
		Source:  "pod-0",
	}
	data, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	var decoded ManifestUpdate
	if err := json.Unmarshal(data, &decoded); err != nil {
		t.Fatal(err)
	}
	if len(decoded.Added) != 1 || decoded.Added[0].Key != "a.parquet" {
		t.Fatalf("unexpected decoded: %+v", decoded)
	}
	if len(decoded.Removed) != 1 || decoded.Removed[0] != "b.parquet" {
		t.Fatalf("unexpected removed: %v", decoded.Removed)
	}
}
