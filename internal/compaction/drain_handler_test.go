package compaction

import (
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// TestDrainHandler_HappyPath asserts a POST to /lakehouse/drain
// invokes Scheduler.Drain and returns 200 with the
// X-Lakehouse-Draining header set.
//
// Negative-control proof: removing the sched.Drain() call would
// leave IsDraining false after the request — this test would fail
// on the post-call IsDraining assertion.
func TestDrainHandler_HappyPath(t *testing.T) {
	sched := newDrainTestScheduler(t, 50*time.Millisecond)
	defer sched.Stop()

	handler := DrainHandler(sched)
	srv := httptest.NewServer(handler)
	defer srv.Close()

	resp, err := http.Post(srv.URL, "", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("status: got %d, want 200", resp.StatusCode)
	}
	if h := resp.Header.Get("X-Lakehouse-Draining"); h != "true" {
		t.Fatalf("X-Lakehouse-Draining: got %q, want true", h)
	}
	if !sched.IsDraining() {
		t.Fatal("scheduler not draining after handler call")
	}
}

// TestDrainHandler_NilScheduler returns 503 so operators can
// distinguish "compaction disabled" from "drain failure".
func TestDrainHandler_NilScheduler(t *testing.T) {
	srv := httptest.NewServer(DrainHandler(nil))
	defer srv.Close()
	resp, err := http.Post(srv.URL, "", nil)
	if err != nil {
		t.Fatalf("POST: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status: got %d, want 503", resp.StatusCode)
	}
}

// TestDrainHandler_RejectsGet covers the method-not-allowed branch.
func TestDrainHandler_RejectsGet(t *testing.T) {
	sched := newDrainTestScheduler(t, 50*time.Millisecond)
	defer sched.Stop()
	srv := httptest.NewServer(DrainHandler(sched))
	defer srv.Close()

	resp, err := http.Get(srv.URL)
	if err != nil {
		t.Fatalf("GET: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status: got %d, want 405", resp.StatusCode)
	}
}

// TestDrainHandler_Idempotent — second drain returns 200 immediately.
func TestDrainHandler_Idempotent(t *testing.T) {
	sched := newDrainTestScheduler(t, 50*time.Millisecond)
	defer sched.Stop()
	srv := httptest.NewServer(DrainHandler(sched))
	defer srv.Close()

	for i := 0; i < 3; i++ {
		resp, err := http.Post(srv.URL, "", nil)
		if err != nil {
			t.Fatalf("POST %d: %v", i, err)
		}
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("POST %d status: got %d, want 200", i, resp.StatusCode)
		}
	}
}

func newDrainTestScheduler(t *testing.T, drainTimeout time.Duration) *Scheduler {
	t.Helper()
	pool := newMockPool()
	m := manifest.New("test-bucket", "logs/")
	return NewScheduler(SchedulerConfig{
		Manifest:         m,
		Pool:             pool,
		Ownership:        soleOwnerResolver(),
		Policy:           NewLevelPolicy(10, 20, 0),
		Prefix:           "logs/",
		Mode:             config.ModeLogs,
		Interval:         time.Hour,
		MaxConcurrent:    1,
		RowGroupSize:     1000,
		CompressionLevel: 7,
		DrainTimeout:     drainTimeout,
	})
}
