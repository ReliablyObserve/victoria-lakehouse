package discovery

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

func TestDiscoverStorageNodes_Static(t *testing.T) {
	d := New("", []string{"node1:9428", "node2:9428"}, "", "", 5*time.Second, testLogger())

	nodes, err := d.DiscoverStorageNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	if nodes[0] != "node1:9428" || nodes[1] != "node2:9428" {
		t.Errorf("nodes = %v", nodes)
	}
}

func TestDiscoverStorageNodes_NoConfig(t *testing.T) {
	d := New("", nil, "", "", 5*time.Second, testLogger())

	nodes, err := d.DiscoverStorageNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if nodes != nil {
		t.Errorf("expected nil nodes, got %v", nodes)
	}
}

func TestDiscoverStorageNodes_HeadlessSRV(t *testing.T) {
	d := New("vlstorage.monitoring.svc.cluster.local", nil, "", "", 5*time.Second, testLogger(),
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", []*net.SRV{
				{Target: "vlstorage-0.monitoring.svc.cluster.local.", Port: 9428},
				{Target: "vlstorage-1.monitoring.svc.cluster.local.", Port: 9428},
			}, nil
		}),
	)

	nodes, err := d.DiscoverStorageNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	if nodes[0] != "vlstorage-0.monitoring.svc.cluster.local:9428" {
		t.Errorf("node[0] = %q", nodes[0])
	}
}

func TestDiscoverStorageNodes_HeadlessHostLookup(t *testing.T) {
	d := New("vlstorage.monitoring.svc.cluster.local", nil, "", "", 5*time.Second, testLogger(),
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no such host"}
		}),
		WithLookupHost(func(_ context.Context, _ string) ([]string, error) {
			return []string{"10.0.0.1", "10.0.0.2"}, nil
		}),
	)

	nodes, err := d.DiscoverStorageNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	if nodes[0] != "10.0.0.1:9428" {
		t.Errorf("node[0] = %q", nodes[0])
	}
}

func TestDiscoverStorageNodes_HeadlessWithPort(t *testing.T) {
	d := New("vlstorage.monitoring.svc.cluster.local:10428", nil, "", "", 5*time.Second, testLogger(),
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", nil, &net.DNSError{Err: "no such host"}
		}),
		WithLookupHost(func(_ context.Context, _ string) ([]string, error) {
			return []string{"10.0.0.1"}, nil
		}),
	)

	nodes, err := d.DiscoverStorageNodes(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(nodes) != 1 {
		t.Fatalf("got %d nodes, want 1", len(nodes))
	}
	if nodes[0] != "10.0.0.1:10428" {
		t.Errorf("node = %q, want 10.0.0.1:10428", nodes[0])
	}
}

func TestDiscoverPeers(t *testing.T) {
	d := New("", nil, "", "lakehouse.monitoring.svc.cluster.local", 5*time.Second, testLogger(),
		WithLookupSRV(func(_ context.Context, _, _, _ string) (string, []*net.SRV, error) {
			return "", []*net.SRV{
				{Target: "lakehouse-0.monitoring.svc.cluster.local.", Port: 9428},
				{Target: "lakehouse-1.monitoring.svc.cluster.local.", Port: 9428},
				{Target: "lakehouse-2.monitoring.svc.cluster.local.", Port: 9428},
			}, nil
		}),
	)

	peers, err := d.DiscoverPeers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(peers) != 3 {
		t.Fatalf("got %d peers, want 3", len(peers))
	}
}

func TestDiscoverPeers_NoPeerService(t *testing.T) {
	d := New("", nil, "", "", 5*time.Second, testLogger())

	peers, err := d.DiscoverPeers(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if peers != nil {
		t.Errorf("expected nil peers, got %v", peers)
	}
}

func TestPollPartitionList(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/partition/list" {
			http.NotFound(w, r)
			return
		}
		if err := json.NewEncoder(w).Encode([]string{"20260426", "20260427", "20260428", "20260429", "20260430", "20260501", "20260502"}); err != nil {
			http.Error(w, err.Error(), 500)
		}
	}))
	defer srv.Close()

	d := New("", []string{srv.Listener.Addr().String()}, "", "", 5*time.Second, testLogger())

	if _, err := d.DiscoverStorageNodes(context.Background()); err != nil {
		t.Fatal(err)
	}

	boundary, err := d.PollPartitionList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if boundary == nil {
		t.Fatal("expected non-nil boundary")
	}
	if boundary.MinDate != "20260426" {
		t.Errorf("min date = %q, want 20260426", boundary.MinDate)
	}
	if boundary.MaxDate != "20260502" {
		t.Errorf("max date = %q, want 20260502", boundary.MaxDate)
	}

	expected := time.Date(2026, 4, 26, 0, 0, 0, 0, time.UTC)
	if !boundary.MinTime.Equal(expected) {
		t.Errorf("min time = %v, want %v", boundary.MinTime, expected)
	}
	expectedMax := time.Date(2026, 5, 3, 0, 0, 0, 0, time.UTC)
	if !boundary.MaxTime.Equal(expectedMax) {
		t.Errorf("max time = %v, want %v", boundary.MaxTime, expectedMax)
	}
}

func TestPollPartitionList_WithAuthKey(t *testing.T) {
	var gotAuthKey string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotAuthKey = r.URL.Query().Get("authKey")
		if err := json.NewEncoder(w).Encode([]string{"20260501"}); err != nil {
			http.Error(w, err.Error(), 500)
		}
	}))
	defer srv.Close()

	d := New("", []string{srv.Listener.Addr().String()}, "test-secret", "", 5*time.Second, testLogger())
	if _, err := d.DiscoverStorageNodes(context.Background()); err != nil {
		t.Fatal(err)
	}

	if _, err := d.PollPartitionList(context.Background()); err != nil {
		t.Fatal(err)
	}
	if gotAuthKey != "test-secret" {
		t.Errorf("auth key = %q, want test-secret", gotAuthKey)
	}
}

func TestPollPartitionList_NoNodes(t *testing.T) {
	d := New("", nil, "", "", 5*time.Second, testLogger())

	boundary, err := d.PollPartitionList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if boundary != nil {
		t.Errorf("expected nil boundary with no nodes")
	}
}

func TestPollPartitionList_NodeError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "internal error", 500)
	}))
	defer srv.Close()

	d := New("", []string{srv.Listener.Addr().String()}, "", "", 5*time.Second, testLogger())
	if _, err := d.DiscoverStorageNodes(context.Background()); err != nil {
		t.Fatal(err)
	}

	boundary, err := d.PollPartitionList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if boundary != nil {
		t.Errorf("expected nil boundary on error")
	}
}

func TestPollPartitionList_MultipleNodes(t *testing.T) {
	srv1 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode([]string{"20260501", "20260502"}); err != nil {
			http.Error(w, err.Error(), 500)
		}
	}))
	defer srv1.Close()

	srv2 := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		if err := json.NewEncoder(w).Encode([]string{"20260430", "20260501"}); err != nil {
			http.Error(w, err.Error(), 500)
		}
	}))
	defer srv2.Close()

	d := New("", []string{srv1.Listener.Addr().String(), srv2.Listener.Addr().String()}, "", "", 5*time.Second, testLogger())
	if _, err := d.DiscoverStorageNodes(context.Background()); err != nil {
		t.Fatal(err)
	}

	boundary, err := d.PollPartitionList(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if boundary == nil {
		t.Fatal("expected non-nil boundary")
	}
	if boundary.MinDate != "20260430" {
		t.Errorf("union min = %q, want 20260430", boundary.MinDate)
	}
	if boundary.MaxDate != "20260502" {
		t.Errorf("union max = %q, want 20260502", boundary.MaxDate)
	}
}

func TestGetHotBoundary(t *testing.T) {
	d := New("", nil, "", "", 5*time.Second, testLogger())
	if d.GetHotBoundary() != nil {
		t.Error("expected nil boundary initially")
	}
}

func TestGetStorageNodes(t *testing.T) {
	d := New("", []string{"a:1", "b:2"}, "", "", 5*time.Second, testLogger())
	if _, err := d.DiscoverStorageNodes(context.Background()); err != nil {
		t.Fatal(err)
	}

	nodes := d.GetStorageNodes()
	if len(nodes) != 2 {
		t.Fatalf("got %d nodes, want 2", len(nodes))
	}
	nodes[0] = "modified"
	if d.GetStorageNodes()[0] == "modified" {
		t.Error("GetStorageNodes should return a copy")
	}
}

func TestSplitHostPort(t *testing.T) {
	tests := []struct {
		input    string
		wantHost string
		wantPort string
	}{
		{"host:9428", "host", "9428"},
		{"host", "host", ""},
		{"host.svc.cluster.local:10428", "host.svc.cluster.local", "10428"},
		{"host.svc.cluster.local", "host.svc.cluster.local", ""},
	}
	for _, tt := range tests {
		h, p := splitHostPort(tt.input)
		if h != tt.wantHost || p != tt.wantPort {
			t.Errorf("splitHostPort(%q) = (%q, %q), want (%q, %q)", tt.input, h, p, tt.wantHost, tt.wantPort)
		}
	}
}
