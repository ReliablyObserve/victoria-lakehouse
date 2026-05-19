package bloomindex

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestHandleBloomStatus_Basic(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	cache := NewBloomCache(1024*1024, nil)

	sp := &StatusProvider{
		Controller: bc,
		Cache:      cache,
		Mode:       "logs",
	}

	handler := HandleBloomStatus(sp)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom/status", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp BloomStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}

	if resp.Mode != "logs" {
		t.Errorf("mode = %q, want logs", resp.Mode)
	}
	if !resp.Enabled {
		t.Error("should be enabled")
	}
	if resp.AutoTuning == nil {
		t.Fatal("auto_tuning should not be nil")
	}
	if resp.AutoTuning.PartitionGranularity != "hour" {
		t.Errorf("granularity = %q, want hour", resp.AutoTuning.PartitionGranularity)
	}
	if len(resp.Tiers) != 4 {
		t.Errorf("tiers count = %d, want 4", len(resp.Tiers))
	}
}

func TestHandleBloomStatus_MethodNotAllowed(t *testing.T) {
	sp := &StatusProvider{Mode: "traces"}
	handler := HandleBloomStatus(sp)
	req := httptest.NewRequest(http.MethodPost, "/api/v1/bloom/status", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want 405", w.Code)
	}
}

func TestHandleBloomStatus_NilController(t *testing.T) {
	sp := &StatusProvider{Mode: "traces"}
	handler := HandleBloomStatus(sp)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom/status", nil)
	w := httptest.NewRecorder()

	handler(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("status = %d, want 200", w.Code)
	}

	var resp BloomStatusResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatal(err)
	}
	if resp.Enabled {
		t.Error("should not be enabled with nil controller")
	}
}

func TestHandleBloomStatus_WithCacheData(t *testing.T) {
	cache := NewBloomCache(1024*1024, nil)

	pi := NewPartitionedIndex(GranularityHour, 0.01)
	pi.AddFile("p1", "f1", map[string][]string{"trace_id": {"aaa"}})
	cache.Put("p1", pi.GetPartition("p1"))

	sp := &StatusProvider{
		Controller: NewBloomController(DefaultBloomControllerConfig()),
		Cache:      cache,
		Mode:       "traces",
	}

	handler := HandleBloomStatus(sp)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom/status", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	var resp BloomStatusResponse
	_ = json.NewDecoder(w.Body).Decode(&resp)

	if resp.Cache.Partitions != 1 {
		t.Errorf("cache partitions = %d, want 1", resp.Cache.Partitions)
	}
	if resp.Cache.MemoryBytesUsed == 0 {
		t.Error("cache memory bytes should be > 0")
	}
}

func TestHandleBloomStatus_WithAdjustments(t *testing.T) {
	bc := NewBloomController(DefaultBloomControllerConfig())
	bc.SetLeader(true)
	bc.Observe(context.Background(), Observation{FilesPerHour: 5000})

	sp := &StatusProvider{
		Controller: bc,
		Mode:       "traces",
	}

	handler := HandleBloomStatus(sp)
	req := httptest.NewRequest(http.MethodGet, "/api/v1/bloom/status", nil)
	w := httptest.NewRecorder()
	handler(w, req)

	var resp BloomStatusResponse
	_ = json.NewDecoder(w.Body).Decode(&resp)

	if len(resp.AutoTuning.RecentAdjustments) != 1 {
		t.Errorf("recent adjustments = %d, want 1", len(resp.AutoTuning.RecentAdjustments))
	}
}
