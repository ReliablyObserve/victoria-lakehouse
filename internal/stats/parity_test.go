package stats

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

type fakeVL struct {
	rows int64
	err  error
}

func (f *fakeVL) StatsCountAll(_ context.Context, _, _ int64) (int64, error) {
	return f.rows, f.err
}

func TestParity_AuthRequired(t *testing.T) {
	api := NewAPI(APIConfig{Manifest: manifest.New("b", "")})
	mux := http.NewServeMux()
	api.RegisterParity(mux, &fakeVL{rows: 100}, func(_ *http.Request) bool { return false })

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusForbidden {
		t.Errorf("status=%d, want 403 with denying auth", rr.Code)
	}
}

func TestParity_RowsAgree_ReportsZeroDelta(t *testing.T) {
	mf := manifest.New("b", "")
	mf.AddFile("dt=2026-06-04/hour=10", manifest.FileInfo{
		Key: "1/1/logs/dt=2026-06-04/hour=10/a.parquet", RowCount: 1000, Size: 1,
	})
	api := NewAPI(APIConfig{Manifest: mf})
	mux := http.NewServeMux()
	api.RegisterParity(mux, &fakeVL{rows: 1000}, nil)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	var r ParityResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &r)
	if r.VLRows != 1000 || r.ManifestRows != 1000 {
		t.Errorf("rows: vl=%d manifest=%d, want 1000/1000", r.VLRows, r.ManifestRows)
	}
	if r.RowsDelta != 0 || r.RowsDelta_ != 0 {
		t.Errorf("delta: %d (%.2f%%), want 0", r.RowsDelta, r.RowsDelta_)
	}
}

func TestParity_RowsDisagree_ReportsDelta(t *testing.T) {
	mf := manifest.New("b", "")
	mf.AddFile("dt=2026-06-04/hour=10", manifest.FileInfo{
		Key: "1/1/logs/dt=2026-06-04/hour=10/a.parquet", RowCount: 1000, Size: 1,
	})
	api := NewAPI(APIConfig{Manifest: mf})
	mux := http.NewServeMux()
	api.RegisterParity(mux, &fakeVL{rows: 1500}, nil)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	var r ParityResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &r)
	if r.RowsDelta != 500 {
		t.Errorf("delta=%d, want 500", r.RowsDelta)
	}
	if r.RowsDelta_ != 50 {
		t.Errorf("delta_pct=%.2f, want 50.00", r.RowsDelta_)
	}
}

func TestParity_VLError_ReturnsPartial(t *testing.T) {
	mf := manifest.New("b", "")
	mf.AddFile("dt=2026-06-04/hour=10", manifest.FileInfo{
		Key: "1/1/logs/dt=2026-06-04/hour=10/a.parquet", RowCount: 1000, Size: 1,
	})
	api := NewAPI(APIConfig{Manifest: mf})
	mux := http.NewServeMux()
	api.RegisterParity(mux, &fakeVL{err: errors.New("vl down")}, nil)

	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	if rr.Code != http.StatusOK {
		t.Errorf("status=%d, want 200 (partial)", rr.Code)
	}
	var body map[string]any
	_ = json.Unmarshal(rr.Body.Bytes(), &body)
	if _, ok := body["vl_error"]; !ok {
		t.Errorf("expected vl_error surfaced, got %+v", body)
	}
	if v, _ := body["manifest_rows"].(float64); v != 1000 {
		t.Errorf("manifest_rows=%v, want 1000 (LH side must still report)", v)
	}
}

func TestParity_WindowOverride(t *testing.T) {
	api := NewAPI(APIConfig{Manifest: manifest.New("b", "")})
	mux := http.NewServeMux()
	api.RegisterParity(mux, &fakeVL{}, nil)
	req := httptest.NewRequest("GET", "/lakehouse/api/v1/admin/parity?window=6h", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)
	var r ParityResponse
	_ = json.Unmarshal(rr.Body.Bytes(), &r)
	delta := r.EndUnixNano - r.StartUnixNano
	wantNs := int64(6 * 60 * 60 * 1e9)
	if delta < wantNs-1e9 || delta > wantNs+1e9 {
		t.Errorf("window delta ns = %d, want ~%d (6h)", delta, wantNs)
	}
}
