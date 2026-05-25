package lifecycle

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleDrain_PostOnly(t *testing.T) {
	orch := NewShutdownOrchestrator(testShutdownConfig(), ShutdownHooks{})
	h := HandleDrain(orch)

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/drain", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusMethodNotAllowed {
		t.Errorf("GET should be 405, got %d", rec.Code)
	}
}

func TestHandleDrain_Post(t *testing.T) {
	orch := NewShutdownOrchestrator(testShutdownConfig(), ShutdownHooks{})
	h := HandleDrain(orch)

	req := httptest.NewRequest(http.MethodPost, "/internal/lifecycle/drain", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("POST should be 200, got %d", rec.Code)
	}
	if rec.Header().Get("X-Lakehouse-Draining") != "true" {
		t.Error("missing X-Lakehouse-Draining header")
	}
}

func TestHandleDrain_NilOrchestrator(t *testing.T) {
	h := HandleDrain(nil)

	req := httptest.NewRequest(http.MethodPost, "/internal/lifecycle/drain", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusInternalServerError {
		t.Errorf("nil orchestrator should be 500, got %d", rec.Code)
	}
}

func TestHandleLifecycleReady_Ready(t *testing.T) {
	h := HandleLifecycleReady(LifecycleInfo{
		GetPhase:   func() string { return "ready" },
		IsReady:    func() bool { return true },
		IsDraining: func() bool { return false },
	})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/ready", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("ready should be 200, got %d", rec.Code)
	}

	var resp struct {
		Ready    bool   `json:"ready"`
		Phase    string `json:"phase"`
		Draining bool   `json:"draining"`
	}
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !resp.Ready {
		t.Error("expected ready=true")
	}
	if resp.Phase != "ready" {
		t.Errorf("phase = %q, want %q", resp.Phase, "ready")
	}
	if resp.Draining {
		t.Error("expected draining=false")
	}
}

func TestHandleLifecycleReady_NotReady(t *testing.T) {
	h := HandleLifecycleReady(LifecycleInfo{
		GetPhase:   func() string { return "init" },
		IsReady:    func() bool { return false },
		IsDraining: func() bool { return false },
	})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/ready", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("not ready should be 503, got %d", rec.Code)
	}
}

func TestHandleLifecycleReady_Draining(t *testing.T) {
	h := HandleLifecycleReady(LifecycleInfo{
		GetPhase:   func() string { return "drain" },
		IsReady:    func() bool { return true },
		IsDraining: func() bool { return true },
	})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/ready", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusServiceUnavailable {
		t.Errorf("draining should be 503, got %d", rec.Code)
	}
	if rec.Header().Get("X-Lakehouse-Draining") != "true" {
		t.Error("missing draining header")
	}
}

func TestHandleLifecycleRing(t *testing.T) {
	h := HandleLifecycleRing(LifecycleInfo{
		GetRingState: func() *RingState {
			return &RingState{
				Members:     []string{"pod-0:9428", "pod-1:9428"},
				MemberCount: 2,
				SelfAddr:    "pod-0:9428",
			}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/ring", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("ring should be 200, got %d", rec.Code)
	}

	var state RingState
	if err := json.NewDecoder(rec.Body).Decode(&state); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if state.MemberCount != 2 {
		t.Errorf("member count = %d, want 2", state.MemberCount)
	}
	if state.SelfAddr != "pod-0:9428" {
		t.Errorf("self_addr = %q, want %q", state.SelfAddr, "pod-0:9428")
	}
}

func TestHandleLifecycleRing_NilProvider(t *testing.T) {
	h := HandleLifecycleRing(LifecycleInfo{})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/ring", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("nil ring should be 200, got %d", rec.Code)
	}

	var state RingState
	if err := json.NewDecoder(rec.Body).Decode(&state); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if state.MemberCount != 0 {
		t.Errorf("member count = %d, want 0", state.MemberCount)
	}
}

func TestHandleLifecycleStale_NilProvider(t *testing.T) {
	h := HandleLifecycleStale(LifecycleInfo{})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/stale", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("stale should be 200, got %d", rec.Code)
	}
}

func TestHandleLifecycleStale_WithData(t *testing.T) {
	h := HandleLifecycleStale(LifecycleInfo{
		GetStaleness: func() *StalenessInfo {
			return &StalenessInfo{
				StaleDetected:    true,
				WALReconciled:    false,
				CacheRevalidated: true,
			}
		},
	})

	req := httptest.NewRequest(http.MethodGet, "/internal/lifecycle/stale", nil)
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("stale should be 200, got %d", rec.Code)
	}

	var info StalenessInfo
	if err := json.NewDecoder(rec.Body).Decode(&info); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if !info.StaleDetected {
		t.Error("expected stale_detected=true")
	}
	if info.WALReconciled {
		t.Error("expected wal_reconciled=false")
	}
	if !info.CacheRevalidated {
		t.Error("expected cache_revalidated=true")
	}
}
