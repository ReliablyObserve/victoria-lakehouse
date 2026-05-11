package crosssignal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestClient_SendHint(t *testing.T) {
	var mu sync.Mutex
	var received []PrefetchHint

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/prefetch/hint" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("X-Cross-Signal-Key") != "test-secret" {
			t.Errorf("missing or wrong auth key")
		}

		var hint PrefetchHint
		if err := json.NewDecoder(r.Body).Decode(&hint); err != nil {
			t.Errorf("decode error: %v", err)
		}
		mu.Lock()
		received = append(received, hint)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:      srv.URL,
		AuthKey:       "test-secret",
		Timeout:       2 * time.Second,
		MaxBatch:      100,
		BatchInterval: 50 * time.Millisecond,
	})
	defer client.Close()

	client.EnqueueHint([]string{"trace-1", "trace-2"}, 1000, 2000, "logs")

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(received))
	}
	if len(received[0].TraceIDs) != 2 {
		t.Errorf("trace_ids = %d, want 2", len(received[0].TraceIDs))
	}
	if received[0].SourceSignal != "logs" {
		t.Errorf("source_signal = %q, want %q", received[0].SourceSignal, "logs")
	}
}

func TestClient_BatchAccumulation(t *testing.T) {
	var mu sync.Mutex
	var received []PrefetchHint

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var hint PrefetchHint
		_ = json.NewDecoder(r.Body).Decode(&hint)
		mu.Lock()
		received = append(received, hint)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:      srv.URL,
		AuthKey:       "",
		Timeout:       2 * time.Second,
		MaxBatch:      100,
		BatchInterval: 100 * time.Millisecond,
	})
	defer client.Close()

	client.EnqueueHint([]string{"t1"}, 1000, 2000, "logs")
	client.EnqueueHint([]string{"t2"}, 1000, 2000, "logs")
	client.EnqueueHint([]string{"t3"}, 1000, 2000, "logs")

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 1 {
		t.Fatalf("expected 1 batched hint, got %d", len(received))
	}
	if len(received[0].TraceIDs) != 3 {
		t.Errorf("batched trace_ids = %d, want 3", len(received[0].TraceIDs))
	}
}

func TestClient_MaxBatchFlush(t *testing.T) {
	var mu sync.Mutex
	var received []PrefetchHint

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var hint PrefetchHint
		_ = json.NewDecoder(r.Body).Decode(&hint)
		mu.Lock()
		received = append(received, hint)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:      srv.URL,
		Timeout:       2 * time.Second,
		MaxBatch:      3,
		BatchInterval: 10 * time.Second,
	})
	defer client.Close()

	ids := []string{"t1", "t2", "t3", "t4", "t5"}
	client.EnqueueHint(ids, 1000, 2000, "logs")

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(received) < 1 {
		t.Fatal("expected at least 1 flush from max_batch")
	}
	if len(received[0].TraceIDs) != 3 {
		t.Errorf("first batch trace_ids = %d, want 3", len(received[0].TraceIDs))
	}
}

func TestClient_SendEvictionHint(t *testing.T) {
	var received EvictionHint

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/cache/evict-hint" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:      srv.URL,
		Timeout:       2 * time.Second,
		MaxBatch:      100,
		BatchInterval: 50 * time.Millisecond,
	})
	defer client.Close()

	client.SendEvictionHint([]string{"evict-1", "evict-2"}, "logs")

	time.Sleep(100 * time.Millisecond)

	if len(received.TraceIDs) != 2 {
		t.Errorf("eviction hint trace_ids = %d, want 2", len(received.TraceIDs))
	}
}

func TestClient_NilEndpoint_NoOp(t *testing.T) {
	client := NewClient(ClientConfig{
		Endpoint: "",
	})
	defer client.Close()

	client.EnqueueHint([]string{"t1"}, 1000, 2000, "logs")
	client.SendEvictionHint([]string{"t1"}, "logs")
}

func TestClient_CloseFlushesRemaining(t *testing.T) {
	var mu sync.Mutex
	var received []PrefetchHint

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path == "/internal/prefetch/hint" {
			var hint PrefetchHint
			_ = json.NewDecoder(r.Body).Decode(&hint)
			mu.Lock()
			received = append(received, hint)
			mu.Unlock()
		}
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:      srv.URL,
		Timeout:       2 * time.Second,
		MaxBatch:      100,
		BatchInterval: 10 * time.Second, // long interval
	})

	client.EnqueueHint([]string{"pending-1", "pending-2"}, 1000, 2000, "logs")
	client.Close() // should flush remaining

	mu.Lock()
	defer mu.Unlock()
	if len(received) < 1 {
		t.Fatal("Close() should have flushed pending hints")
	}
}

func TestClient_EmptyTraceIDs_NoOp(t *testing.T) {
	client := NewClient(ClientConfig{
		Endpoint:      "http://localhost:9999",
		Timeout:       100 * time.Millisecond,
		MaxBatch:      100,
		BatchInterval: 50 * time.Millisecond,
	})
	defer client.Close()

	// Should not send anything or panic
	client.EnqueueHint([]string{}, 1000, 2000, "logs")
	client.SendEvictionHint([]string{}, "logs")
}
