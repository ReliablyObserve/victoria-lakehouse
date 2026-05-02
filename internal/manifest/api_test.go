package manifest

import (
	"encoding/json"
	"log/slog"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestRangeHandler_Empty(t *testing.T) {
	m := New("bucket", "logs/", slog.New(slog.NewTextHandler(io.Discard, nil)))

	req := httptest.NewRequest(http.MethodGet, "/manifest/range", nil)
	w := httptest.NewRecorder()
	m.RangeHandler()(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}

	var resp RangeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.TotalFiles != 0 {
		t.Errorf("totalFiles = %d, want 0", resp.TotalFiles)
	}
}

func TestRangeHandler_WithData(t *testing.T) {
	m := New("bucket", "logs/", slog.New(slog.NewTextHandler(io.Discard, nil)))

	m.AddFile("dt=2026-04-01/hour=00", FileInfo{Key: "logs/dt=2026-04-01/hour=00/f.parquet", Size: 1000})
	m.AddFile("dt=2026-04-30/hour=23", FileInfo{Key: "logs/dt=2026-04-30/hour=23/g.parquet", Size: 2000})

	req := httptest.NewRequest(http.MethodGet, "/manifest/range", nil)
	w := httptest.NewRecorder()
	m.RangeHandler()(w, req)

	var resp RangeResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if resp.TotalFiles != 2 {
		t.Errorf("totalFiles = %d, want 2", resp.TotalFiles)
	}
	if resp.TotalBytes != 3000 {
		t.Errorf("totalBytes = %d, want 3000", resp.TotalBytes)
	}
	if resp.MinDate != "2026-04-01" {
		t.Errorf("minDate = %q, want 2026-04-01", resp.MinDate)
	}

	expectedMin := time.Date(2026, 4, 1, 0, 0, 0, 0, time.UTC)
	if resp.MinTime != expectedMin.UnixNano() {
		t.Errorf("minTime = %d, want %d", resp.MinTime, expectedMin.UnixNano())
	}

	ct := w.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("content-type = %q, want application/json", ct)
	}
}
