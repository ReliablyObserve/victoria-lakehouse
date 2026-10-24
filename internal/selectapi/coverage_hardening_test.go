package selectapi

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// TestNewHandler_DefaultMaxConcurrent exercises the MaxConcurrent <= 0 default branch.
func TestNewHandler_DefaultMaxConcurrent(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 0, // should default to 32
		},
	}
	h := NewHandler(mockStore{}, cfg)
	if h == nil {
		t.Fatal("NewHandler returned nil")
	}
	if cap(h.sem) != 32 {
		t.Errorf("sem capacity = %d, want 32 (default)", cap(h.sem))
	}
}

// TestNewHandler_NegativeMaxConcurrent exercises negative MaxConcurrent branch.
func TestNewHandler_NegativeMaxConcurrent(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: -5, // should default to 32
		},
	}
	h := NewHandler(mockStore{}, cfg)
	if cap(h.sem) != 32 {
		t.Errorf("sem capacity = %d, want 32 (default for negative)", cap(h.sem))
	}
}

// TestWrapVL_SlowQueryLogging exercises the slow query logging branch.
func TestWrapVL_SlowQueryLogging(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 10,
			SlowThreshold: 1 * time.Nanosecond, // extremely low threshold to trigger slow query log
		},
	}
	h := NewHandler(mockStore{}, cfg)

	wrapped := h.wrapVL(func(_ context.Context, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/query?query=test", nil)
	wrapped(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}

// TestWrapVL_SlowQueryWithTenantHeaders exercises slow query path with tenant headers.
func TestWrapVL_SlowQueryWithTenantHeaders(t *testing.T) {
	cfg := &config.Config{
		Mode: config.ModeLogs,
		Query: config.QueryConfig{
			Timeout:       5 * time.Second,
			MaxConcurrent: 10,
			SlowThreshold: 1 * time.Nanosecond,
		},
	}
	h := NewHandler(mockStore{}, cfg)

	wrapped := h.wrapVL(func(_ context.Context, w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	rec := httptest.NewRecorder()
	req := httptest.NewRequest("GET", "/select/logsql/query?query=slow_query", nil)
	req.Header.Set("AccountID", "42")
	req.Header.Set("ProjectID", "7")
	wrapped(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("expected 200, got %d", rec.Code)
	}
}
