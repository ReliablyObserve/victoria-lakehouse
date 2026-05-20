package manifest

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestVerifyManifest_RangeHandler_JSONFormat(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=08", FileInfo{
		Key:       "logs/dt=2026-05-01/hour=08/file1.parquet",
		Size:      1024,
		MinTimeNs: 1746086400000000000,
		MaxTimeNs: 1746090000000000000,
	})
	m.AddFile("dt=2026-05-02/hour=14", FileInfo{
		Key:       "logs/dt=2026-05-02/hour=14/file2.parquet",
		Size:      2048,
		MinTimeNs: 1746172800000000000,
		MaxTimeNs: 1746176400000000000,
	})

	req := httptest.NewRequest(http.MethodGet, "/manifest/range", nil)
	rec := httptest.NewRecorder()
	m.RangeHandler()(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp RangeResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.TotalFiles != 2 {
		t.Errorf("TotalFiles = %d, want 2", resp.TotalFiles)
	}

	wantBytes := int64(1024 + 2048)
	if resp.TotalBytes != wantBytes {
		t.Errorf("TotalBytes = %d, want %d", resp.TotalBytes, wantBytes)
	}

	if resp.MinTime >= resp.MaxTime {
		t.Errorf("MinTime (%d) must be less than MaxTime (%d)", resp.MinTime, resp.MaxTime)
	}

	if resp.MinDate == "" {
		t.Error("MinDate is empty, want non-empty date string")
	}
	if resp.MaxDate == "" {
		t.Error("MaxDate is empty, want non-empty date string")
	}
}

func TestVerifyManifest_PartitionsHandler_JSONFormat(t *testing.T) {
	m := New("bucket", "logs/")

	m.AddFile("dt=2026-05-01/hour=08", FileInfo{
		Key:  "logs/dt=2026-05-01/hour=08/file1.parquet",
		Size: 512,
	})
	m.AddFile("dt=2026-05-01/hour=09", FileInfo{
		Key:  "logs/dt=2026-05-01/hour=09/file2.parquet",
		Size: 1024,
	})

	req := httptest.NewRequest(http.MethodGet, "/manifest/partitions", nil)
	rec := httptest.NewRecorder()
	m.PartitionsHandler()(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp PartitionsResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if len(resp.Partitions) < 1 {
		t.Errorf("partitions = %d, want at least 1", len(resp.Partitions))
	}
}

func TestVerifyManifest_RangeHandler_ContentType(t *testing.T) {
	m := New("bucket", "logs/")

	req := httptest.NewRequest(http.MethodGet, "/manifest/range", nil)
	rec := httptest.NewRecorder()
	m.RangeHandler()(rec, req)

	ct := rec.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want \"application/json\"", ct)
	}
}

func TestVerifyManifest_EmptyManifest(t *testing.T) {
	m := New("bucket", "logs/")

	req := httptest.NewRequest(http.MethodGet, "/manifest/range", nil)
	rec := httptest.NewRecorder()
	m.RangeHandler()(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", rec.Code)
	}

	var resp RangeResponse
	if err := json.NewDecoder(rec.Body).Decode(&resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.TotalFiles != 0 {
		t.Errorf("TotalFiles = %d, want 0", resp.TotalFiles)
	}
	if resp.TotalBytes != 0 {
		t.Errorf("TotalBytes = %d, want 0", resp.TotalBytes)
	}
}
