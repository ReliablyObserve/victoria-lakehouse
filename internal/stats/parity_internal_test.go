package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// fakeCounter satisfies VTInternalCounter for tests.
type fakeCounter struct{ values map[string]uint64 }

func (f *fakeCounter) Get(kind string) uint64 { return f.values[kind] }

func TestParity_VTInternalDropped_AccountedInExpectedDrift(t *testing.T) {
	mf := manifest.New("b", "")
	// Manifest holds 1M real spans.
	mf.AddFile("dt=2026-06-04/hour=10", manifest.FileInfo{
		Key: "1/1/traces/dt=2026-06-04/hour=10/a.parquet", RowCount: 1_000_000, Size: 1,
	})

	api := NewAPI(APIConfig{Manifest: mf})
	mux := http.NewServeMux()
	// VL sees 1.9M rows total (1M real + 800K trace_id_idx + 100K service_graph).
	// The writer dropped exactly those 900K internal rows before they
	// reached the manifest — counter records the drop.
	api.RegisterParityWithInternal(mux,
		&fakeVL{rows: 1_900_000},
		nil, // open
		&fakeCounter{values: map[string]uint64{
			"trace_id_idx":  800_000,
			"service_graph": 100_000,
		}},
		[]string{"trace_id_idx", "service_graph"},
	)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Fatalf("status=%d body=%s", rr.Code, rr.Body.String())
	}

	var r ParityResponse
	if err := json.Unmarshal(rr.Body.Bytes(), &r); err != nil {
		t.Fatal(err)
	}

	if r.RowsDelta != 900_000 {
		t.Errorf("rows_delta=%d, want 900000 (VL − manifest)", r.RowsDelta)
	}
	if r.ExpectedDrift != 900_000 {
		t.Errorf("expected_drift=%d, want 900000 (sum of dropped kinds)", r.ExpectedDrift)
	}
	if r.VerifiedDrift != 0 {
		t.Errorf("verified_drift=%d, want 0 — dropped counter fully explains the gap", r.VerifiedDrift)
	}
	if got := r.VTInternalDropped["trace_id_idx"]; got != 800_000 {
		t.Errorf("trace_id_idx counter = %d, want 800000", got)
	}
}

func TestParity_NoInternalCounter_LegacyResponseShape(t *testing.T) {
	mf := manifest.New("b", "")
	mf.AddFile("dt=2026-06-04/hour=10", manifest.FileInfo{
		Key: "1/1/logs/dt=2026-06-04/hour=10/a.parquet", RowCount: 1000, Size: 1,
	})
	api := NewAPI(APIConfig{Manifest: mf})
	mux := http.NewServeMux()
	// RegisterParity (no internal counter) — logs side keeps the
	// pre-counter response shape.
	api.RegisterParity(mux, &fakeVL{rows: 1000}, nil)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	var r ParityResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &r)
	if r.ExpectedDrift != 0 {
		t.Errorf("expected_drift=%d, want 0 when no internal counter wired", r.ExpectedDrift)
	}
	if len(r.VTInternalDropped) != 0 {
		t.Errorf("vt_internal_dropped=%v, want empty when not wired", r.VTInternalDropped)
	}
}

// TestManifest_LiveAggregateWindow_FiltersByFileTime pins the
// windowed aggregate helper the parity check now uses so the
// manifest scope matches the VL query scope.
func TestManifest_LiveAggregateWindow_FiltersByFileTime(t *testing.T) {
	mf := manifest.New("b", "")
	mf.AddFile("dt=2026-06-04/hour=00", manifest.FileInfo{
		Key:       "1/1/logs/dt=2026-06-04/hour=00/old.parquet",
		RowCount:  500,
		Size:      1,
		MinTimeNs: 1_000_000_000, // 1s
		MaxTimeNs: 2_000_000_000, // 2s
	})
	mf.AddFile("dt=2026-06-04/hour=10", manifest.FileInfo{
		Key:       "1/1/logs/dt=2026-06-04/hour=10/new.parquet",
		RowCount:  1000,
		Size:      1,
		MinTimeNs: 100_000_000_000, // 100s
		MaxTimeNs: 200_000_000_000, // 200s
	})

	// Full window: both files.
	if got := mf.LiveAggregateWindow(0, 0); got.Rows != 1500 {
		t.Errorf("full window rows=%d, want 1500", got.Rows)
	}
	// Window covering only the new file.
	if got := mf.LiveAggregateWindow(50_000_000_000, 0); got.Rows != 1000 {
		t.Errorf("new-only window rows=%d, want 1000", got.Rows)
	}
	// Window covering only the old file.
	if got := mf.LiveAggregateWindow(0, 50_000_000_000); got.Rows != 500 {
		t.Errorf("old-only window rows=%d, want 500", got.Rows)
	}
}
