package crosssignal

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type mockPrefetchRouter struct {
	enqueued atomic.Int64
}

func (m *mockPrefetchRouter) EnqueueCrossSignal(keys []string) int {
	m.enqueued.Add(int64(len(keys)))
	return len(keys)
}

type mockEvictionHandler struct {
	deprioritized atomic.Int64
}

func (m *mockEvictionHandler) DeprioritizeByTraceIDs(traceIDs []string) int {
	n := len(traceIDs)
	m.deprioritized.Add(int64(n))
	return n
}

func TestPrefetchHintHandler_ValidRequest(t *testing.T) {
	prefetch := &mockPrefetchRouter{}
	h := NewHandler(HandlerConfig{
		AuthKey:        "secret",
		PrefetchRouter: prefetch,
	})

	hint := PrefetchHint{
		TraceIDs:     []string{"trace-1", "trace-2"},
		StartNs:      1000,
		EndNs:        2000,
		SourceSignal: "logs",
	}
	body, _ := json.Marshal(hint)

	req := httptest.NewRequest(http.MethodPost, "/internal/prefetch/hint", bytes.NewReader(body))
	req.Header.Set("X-Cross-Signal-Key", "secret")
	w := httptest.NewRecorder()

	h.HandlePrefetchHint(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if prefetch.enqueued.Load() != 2 {
		t.Errorf("enqueued = %d, want 2", prefetch.enqueued.Load())
	}
}

func TestPrefetchHintHandler_Unauthorized(t *testing.T) {
	h := NewHandler(HandlerConfig{
		AuthKey: "secret",
	})

	req := httptest.NewRequest(http.MethodPost, "/internal/prefetch/hint", nil)
	w := httptest.NewRecorder()

	h.HandlePrefetchHint(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestEvictHintHandler_ValidRequest(t *testing.T) {
	eviction := &mockEvictionHandler{}
	h := NewHandler(HandlerConfig{
		AuthKey:         "secret",
		EvictionHandler: eviction,
	})

	hint := EvictionHint{
		TraceIDs:     []string{"t1", "t2", "t3"},
		SourceSignal: "logs",
	}
	body, _ := json.Marshal(hint)

	req := httptest.NewRequest(http.MethodPost, "/internal/cache/evict-hint", bytes.NewReader(body))
	req.Header.Set("X-Cross-Signal-Key", "secret")
	w := httptest.NewRecorder()

	h.HandleEvictHint(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if eviction.deprioritized.Load() != 3 {
		t.Errorf("deprioritized = %d, want 3", eviction.deprioritized.Load())
	}
}

func TestEvictHintHandler_NoAuthKey_AllowAll(t *testing.T) {
	eviction := &mockEvictionHandler{}
	h := NewHandler(HandlerConfig{
		AuthKey:         "",
		EvictionHandler: eviction,
	})

	hint := EvictionHint{TraceIDs: []string{"t1"}, SourceSignal: "logs"}
	body, _ := json.Marshal(hint)

	req := httptest.NewRequest(http.MethodPost, "/internal/cache/evict-hint", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.HandleEvictHint(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 when no auth configured", w.Code)
	}
}

func TestPrefetchHintHandler_InvalidJSON(t *testing.T) {
	h := NewHandler(HandlerConfig{
		AuthKey: "",
	})

	req := httptest.NewRequest(http.MethodPost, "/internal/prefetch/hint", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()

	h.HandlePrefetchHint(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for invalid JSON", w.Code)
	}
}

func TestEvictHintHandler_InvalidJSON(t *testing.T) {
	h := NewHandler(HandlerConfig{
		AuthKey: "",
	})

	req := httptest.NewRequest(http.MethodPost, "/internal/cache/evict-hint", bytes.NewReader([]byte("not json")))
	w := httptest.NewRecorder()

	h.HandleEvictHint(w, req)

	if w.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want 400 for invalid JSON", w.Code)
	}
}

func TestPrefetchHintHandler_EmptyTraceIDs_StillOK(t *testing.T) {
	prefetch := &mockPrefetchRouter{}
	h := NewHandler(HandlerConfig{
		AuthKey:        "",
		PrefetchRouter: prefetch,
	})

	hint := PrefetchHint{TraceIDs: []string{}, SourceSignal: "logs"}
	body, _ := json.Marshal(hint)

	req := httptest.NewRequest(http.MethodPost, "/internal/prefetch/hint", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.HandlePrefetchHint(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if prefetch.enqueued.Load() != 0 {
		t.Errorf("should not enqueue empty trace IDs")
	}
}

func TestHandler_Register(t *testing.T) {
	h := NewHandler(HandlerConfig{})
	mux := http.NewServeMux()
	h.Register(mux)

	// Verify routes are registered by making requests
	req := httptest.NewRequest(http.MethodPost, "/internal/prefetch/hint", bytes.NewReader([]byte("{}")))
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code == http.StatusNotFound {
		t.Error("prefetch hint route not registered")
	}

	req = httptest.NewRequest(http.MethodPost, "/internal/cache/evict-hint", bytes.NewReader([]byte("{}")))
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code == http.StatusNotFound {
		t.Error("evict hint route not registered")
	}
}
