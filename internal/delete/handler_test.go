package delete

import (
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// mockManifest returns predictable FileInfo for testing.
type mockManifest struct {
	files []FileInfo
}

func (m *mockManifest) GetFilesForRange(startNs, endNs int64) []FileInfo {
	var result []FileInfo
	for _, f := range m.files {
		if f.MinTimeNs <= endNs && f.MaxTimeNs >= startNs {
			result = append(result, f)
		}
	}
	return result
}

func newTestHandler(cfg *config.DeleteConfig, files []FileInfo) *Handler {
	store := NewTombstoneStore()
	manifest := &mockManifest{files: files}
	detector := NewStorageClassDetector(nil) // no lifecycle rules
	return NewHandler(store, manifest, detector, cfg, "logs")
}

func defaultCfg() *config.DeleteConfig {
	return &config.DeleteConfig{
		Enabled:     true,
		DefaultMode: "auto",
	}
}

func testFiles() []FileInfo {
	return []FileInfo{
		{Key: "data/file1.parquet", Size: 1024 * 1024, MinTimeNs: 1000, MaxTimeNs: 5000},
		{Key: "data/file2.parquet", Size: 2048 * 1024, MinTimeNs: 3000, MaxTimeNs: 8000},
		{Key: "data/file3.parquet", Size: 512 * 1024, MinTimeNs: 9000, MaxTimeNs: 12000},
	}
}

func postForm(handler http.HandlerFunc, path string, params url.Values) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(params.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func getRequest(handler http.HandlerFunc, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func deleteRequest(handler http.HandlerFunc, path string) *httptest.ResponseRecorder {
	req := httptest.NewRequest(http.MethodDelete, path, nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)
	return w
}

func decodeJSON(t *testing.T, body io.Reader) map[string]any {
	t.Helper()
	var result map[string]any
	if err := json.NewDecoder(body).Decode(&result); err != nil {
		t.Fatalf("failed to decode JSON: %v", err)
	}
	return result
}

// --- handleDelete tests ---

func TestHandler_handleDelete_Valid(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"6000"},
	}
	w := postForm(h.handleDelete, "/delete/logsql/delete", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	result := decodeJSON(t, w.Body)
	if result["tombstone_id"] == "" {
		t.Error("expected non-empty tombstone_id")
	}
	if result["mode"] != "auto" {
		t.Errorf("expected mode=auto, got %v", result["mode"])
	}
	// files 1 and 2 overlap [1000, 6000]
	if int(result["affected_files"].(float64)) != 2 {
		t.Errorf("expected 2 affected files, got %v", result["affected_files"])
	}
	if result["message"] != "tombstone created successfully" {
		t.Errorf("unexpected message: %v", result["message"])
	}

	// Verify tombstone was stored
	if h.store.Count() != 1 {
		t.Errorf("expected 1 tombstone in store, got %d", h.store.Count())
	}
}

func TestHandler_handleDelete_MissingQuery(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"start": {"1000"},
		"end":   {"6000"},
	}
	w := postForm(h.handleDelete, "/delete/logsql/delete", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_handleDelete_Disabled(t *testing.T) {
	cfg := defaultCfg()
	cfg.Enabled = false
	h := newTestHandler(cfg, testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"6000"},
	}
	w := postForm(h.handleDelete, "/delete/logsql/delete", params)

	if w.Code != http.StatusForbidden {
		t.Fatalf("expected 403, got %d", w.Code)
	}
}

func TestHandler_handleDelete_ModeHide(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"6000"},
		"mode":  {"hide"},
	}
	w := postForm(h.handleDelete, "/delete/logsql/delete", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["mode"] != "hide" {
		t.Errorf("expected mode=hide, got %v", result["mode"])
	}
}

func TestHandler_handleDelete_ModePermanent(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"6000"},
		"mode":  {"permanent"},
	}
	w := postForm(h.handleDelete, "/delete/logsql/delete", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["mode"] != "permanent" {
		t.Errorf("expected mode=permanent, got %v", result["mode"])
	}
}

func TestHandler_handleDelete_ModeAuto(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"6000"},
		"mode":  {"auto"},
	}
	w := postForm(h.handleDelete, "/delete/logsql/delete", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["mode"] != "auto" {
		t.Errorf("expected mode=auto, got %v", result["mode"])
	}
}

func TestHandler_handleDelete_InvalidMode(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"6000"},
		"mode":  {"bogus"},
	}
	w := postForm(h.handleDelete, "/delete/logsql/delete", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_handleDelete_InvalidStart(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"not-a-number"},
		"end":   {"6000"},
	}
	w := postForm(h.handleDelete, "/delete/logsql/delete", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_handleDelete_InvalidEnd(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"not-a-number"},
	}
	w := postForm(h.handleDelete, "/delete/logsql/delete", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- handleEstimate tests ---

func TestHandler_handleEstimate_Valid(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"6000"},
	}
	w := postForm(h.handleEstimate, "/delete/logsql/estimate", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	result := decodeJSON(t, w.Body)
	if int(result["affected_files"].(float64)) != 2 {
		t.Errorf("expected 2 affected files, got %v", result["affected_files"])
	}
	classes, ok := result["storage_classes"].(map[string]any)
	if !ok {
		t.Fatal("expected storage_classes map")
	}
	if len(classes) == 0 {
		t.Error("expected non-empty storage_classes")
	}
	if result["recommended_mode"] == "" {
		t.Error("expected non-empty recommended_mode")
	}
	if result["auto_behavior"] == "" {
		t.Error("expected non-empty auto_behavior")
	}
}

func TestHandler_handleEstimate_MissingQuery(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"start": {"1000"},
		"end":   {"6000"},
	}
	w := postForm(h.handleEstimate, "/delete/logsql/estimate", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_handleEstimate_NoFiles(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"99000"},
		"end":   {"99999"},
	}
	w := postForm(h.handleEstimate, "/delete/logsql/estimate", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if int(result["affected_files"].(float64)) != 0 {
		t.Errorf("expected 0 affected files, got %v", result["affected_files"])
	}
	// With no files, recommended should be "hide"
	if result["recommended_mode"] != "hide" {
		t.Errorf("expected recommended_mode=hide, got %v", result["recommended_mode"])
	}
}

// --- handleListTombstones tests ---

func TestHandler_handleListTombstones_Empty(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	w := getRequest(h.handleListTombstones, "/delete/logsql/tombstones")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	result := decodeJSON(t, w.Body)
	if int(result["count"].(float64)) != 0 {
		t.Errorf("expected count=0, got %v", result["count"])
	}
}

func TestHandler_handleListTombstones_WithTombstones(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	// Add some tombstones
	h.store.Add(Tombstone{ID: "ts-1", Query: "error", StartNs: 1000, EndNs: 5000, Mode: "hide"})
	h.store.Add(Tombstone{ID: "ts-2", Query: "warn", StartNs: 2000, EndNs: 6000, Mode: "permanent"})

	w := getRequest(h.handleListTombstones, "/delete/logsql/tombstones")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}
	result := decodeJSON(t, w.Body)
	if int(result["count"].(float64)) != 2 {
		t.Errorf("expected count=2, got %v", result["count"])
	}
}

// --- handleTombstoneByID tests ---

func TestHandler_handleTombstoneByID_GetExisting(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())
	h.store.Add(Tombstone{ID: "ts-abc", Query: "error", StartNs: 1000, EndNs: 5000, Mode: "hide"})

	w := getRequest(h.handleTombstoneByID, "/delete/logsql/tombstone/ts-abc")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["ID"] != "ts-abc" {
		t.Errorf("expected ID=ts-abc, got %v", result["ID"])
	}
}

func TestHandler_handleTombstoneByID_GetMissing(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	w := getRequest(h.handleTombstoneByID, "/delete/logsql/tombstone/nonexistent")

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

func TestHandler_handleTombstoneByID_DeleteExisting(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())
	h.store.Add(Tombstone{ID: "ts-del", Query: "error", StartNs: 1000, EndNs: 5000, Mode: "hide"})

	w := deleteRequest(h.handleTombstoneByID, "/delete/logsql/tombstone/ts-del")

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["status"] != "removed" {
		t.Errorf("expected status=removed, got %v", result["status"])
	}
	if result["id"] != "ts-del" {
		t.Errorf("expected id=ts-del, got %v", result["id"])
	}

	// Verify it's gone
	if h.store.Count() != 0 {
		t.Errorf("expected 0 tombstones after delete, got %d", h.store.Count())
	}
}

func TestHandler_handleTombstoneByID_DeleteMissing(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	w := deleteRequest(h.handleTombstoneByID, "/delete/logsql/tombstone/nonexistent")

	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404, got %d", w.Code)
	}
}

// --- handleVerify tests ---

func TestHandler_handleVerify_WithMatchingTombstone(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())
	h.store.Add(Tombstone{ID: "ts-v1", Query: "error", StartNs: 1000, EndNs: 5000, Mode: "hide"})

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"5000"},
	}
	w := postForm(h.handleVerify, "/delete/logsql/verify", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["verified"] != true {
		t.Errorf("expected verified=true, got %v", result["verified"])
	}
	ids := result["tombstone_ids"].([]any)
	if len(ids) != 1 || ids[0] != "ts-v1" {
		t.Errorf("expected [ts-v1], got %v", ids)
	}
	if result["coverage"].(float64) != 1.0 {
		t.Errorf("expected coverage=1.0, got %v", result["coverage"])
	}
}

func TestHandler_handleVerify_WithoutMatchingTombstone(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"5000"},
	}
	w := postForm(h.handleVerify, "/delete/logsql/verify", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["verified"] != false {
		t.Errorf("expected verified=false, got %v", result["verified"])
	}
}

func TestHandler_handleVerify_PartialCoverage(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())
	// Tombstone covers only half the range
	h.store.Add(Tombstone{ID: "ts-partial", Query: "error", StartNs: 1000, EndNs: 3000, Mode: "hide"})

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"5000"},
	}
	w := postForm(h.handleVerify, "/delete/logsql/verify", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["verified"] != true {
		t.Errorf("expected verified=true (tombstone exists), got %v", result["verified"])
	}
	coverage := result["coverage"].(float64)
	if coverage != 0.5 {
		t.Errorf("expected coverage=0.5, got %v", coverage)
	}
}

func TestHandler_handleVerify_MissingQuery(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"start": {"1000"},
		"end":   {"5000"},
	}
	w := postForm(h.handleVerify, "/delete/logsql/verify", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- handleEstimate additional coverage ---

func TestHandler_handleEstimate_CachedStorageClass(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())
	// Pre-cache a storage class for file1
	h.detector.SetCache("data/file1.parquet", ClassGlacier)

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"5000"},
	}
	w := postForm(h.handleEstimate, "/delete/logsql/estimate", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	classes := result["storage_classes"].(map[string]any)
	// file1 should show GLACIER from cache
	if classes["GLACIER"] == nil {
		t.Error("expected GLACIER class from cached value")
	}
	// With a non-rewritable class, recommended should be "hide"
	if result["recommended_mode"] != "hide" {
		t.Errorf("expected recommended_mode=hide for glacier files, got %v", result["recommended_mode"])
	}
	if result["auto_behavior"] != "hide data at query time" {
		t.Errorf("expected hide behavior, got %v", result["auto_behavior"])
	}
}

func TestHandler_handleEstimate_InvalidStart(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"bad"},
		"end":   {"6000"},
	}
	w := postForm(h.handleEstimate, "/delete/logsql/estimate", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_handleEstimate_InvalidEnd(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"bad"},
	}
	w := postForm(h.handleEstimate, "/delete/logsql/estimate", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- handleVerify additional coverage ---

func TestHandler_handleVerify_ZeroRange(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())
	h.store.Add(Tombstone{ID: "ts-zero", Query: "error", StartNs: 1000, EndNs: 1000, Mode: "hide"})

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"1000"},
	}
	w := postForm(h.handleVerify, "/delete/logsql/verify", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["verified"] != true {
		t.Errorf("expected verified=true for zero-range match, got %v", result["verified"])
	}
	if result["coverage"].(float64) != 1.0 {
		t.Errorf("expected coverage=1.0 for zero range, got %v", result["coverage"])
	}
}

func TestHandler_handleVerify_InvalidStart(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"bad"},
		"end":   {"5000"},
	}
	w := postForm(h.handleVerify, "/delete/logsql/verify", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

func TestHandler_handleVerify_InvalidEnd(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{
		"query": {"error"},
		"start": {"1000"},
		"end":   {"bad"},
	}
	w := postForm(h.handleVerify, "/delete/logsql/verify", params)

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- handleTombstoneByID missing ID test ---

func TestHandler_handleTombstoneByID_MissingID(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	w := getRequest(h.handleTombstoneByID, "/delete/logsql/tombstone/")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("expected 400, got %d", w.Code)
	}
}

// --- Method validation tests ---

func TestHandler_handleDelete_WrongMethod(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	w := getRequest(h.handleDelete, "/delete/logsql/delete")

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandler_handleEstimate_WrongMethod(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	w := getRequest(h.handleEstimate, "/delete/logsql/estimate")

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandler_handleListTombstones_WrongMethod(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	params := url.Values{"query": {"x"}}
	w := postForm(h.handleListTombstones, "/delete/logsql/tombstones", params)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandler_handleTombstoneByID_WrongMethod(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	// Use PUT which is not allowed
	req := httptest.NewRequest(http.MethodPut, "/delete/logsql/tombstone/ts-1", nil)
	w := httptest.NewRecorder()
	h.handleTombstoneByID(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

func TestHandler_handleVerify_WrongMethod(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())

	w := getRequest(h.handleVerify, "/delete/logsql/verify")

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("expected 405, got %d", w.Code)
	}
}

// --- Register test ---

func TestHandler_Register(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())
	mux := http.NewServeMux()
	h.Register(mux)

	// Verify routes are registered by making requests to the mux.
	req := httptest.NewRequest(http.MethodGet, "/delete/logsql/tombstones", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("expected 200 for /delete/logsql/tombstones via mux, got %d", w.Code)
	}
}

// --- Wildcard tombstone verify test ---

func TestHandler_handleVerify_WildcardTombstone(t *testing.T) {
	h := newTestHandler(defaultCfg(), testFiles())
	h.store.Add(Tombstone{ID: "ts-wild", Query: "*", StartNs: 0, EndNs: 100000, Mode: "hide"})

	params := url.Values{
		"query": {"anything"},
		"start": {"1000"},
		"end":   {"5000"},
	}
	w := postForm(h.handleVerify, "/delete/logsql/verify", params)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	result := decodeJSON(t, w.Body)
	if result["verified"] != true {
		t.Errorf("expected verified=true for wildcard tombstone, got %v", result["verified"])
	}
}

// --- Trace mode handler tests ---

func TestHandler_TraceMode_Delete(t *testing.T) {
	store := NewTombstoneStore()
	manifest := &mockManifest{files: []FileInfo{
		{Key: "traces/dt=2026-05-02/hour=10/batch.parquet", Size: 1024, MinTimeNs: 1000, MaxTimeNs: 5000},
	}}
	detector := NewStorageClassDetector(nil)
	cfg := defaultCfg()

	h := NewHandler(store, manifest, detector, cfg, "traces")
	mux := http.NewServeMux()
	h.Register(mux)

	form := url.Values{}
	form.Set("query", `service.name:="order-svc"`)
	form.Set("start", "0")
	form.Set("end", "10000")
	form.Set("mode", "permanent")

	req := httptest.NewRequest("POST", "/delete/tracessql/delete", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if store.Count() != 1 {
		t.Fatalf("expected 1 tombstone, got %d", store.Count())
	}
}

func TestHandler_TraceMode_Estimate(t *testing.T) {
	store := NewTombstoneStore()
	manifest := &mockManifest{files: []FileInfo{
		{Key: "traces/dt=2026-05-02/hour=10/batch.parquet", Size: 2048, MinTimeNs: 1000, MaxTimeNs: 5000},
	}}
	detector := NewStorageClassDetector(nil)
	cfg := defaultCfg()

	h := NewHandler(store, manifest, detector, cfg, "traces")
	mux := http.NewServeMux()
	h.Register(mux)

	form := url.Values{}
	form.Set("query", `trace_id:="abc"`)
	form.Set("start", "0")
	form.Set("end", "10000")

	req := httptest.NewRequest("POST", "/delete/tracessql/estimate", strings.NewReader(form.Encode()))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandler_TraceMode_ListTombstones(t *testing.T) {
	store := NewTombstoneStore()
	manifest := &mockManifest{}
	detector := NewStorageClassDetector(nil)
	cfg := defaultCfg()

	h := NewHandler(store, manifest, detector, cfg, "traces")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/delete/tracessql/tombstones", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
}

func TestHandler_TraceMode_LogsEndpointReturns404(t *testing.T) {
	store := NewTombstoneStore()
	manifest := &mockManifest{}
	detector := NewStorageClassDetector(nil)
	cfg := defaultCfg()

	h := NewHandler(store, manifest, detector, cfg, "traces")
	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/delete/logsql/tombstones", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("logs endpoints should not be registered in traces mode, got %d", w.Code)
	}
}
