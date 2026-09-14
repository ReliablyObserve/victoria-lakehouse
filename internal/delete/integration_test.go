package delete

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	lhmanifest "github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// writeTestParquet generates a Parquet file from the given LogRows and returns the bytes.
func writeTestParquet(t *testing.T, rows []schema.LogRow) []byte {
	t.Helper()
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.LogRow](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatalf("write parquet rows: %v", err)
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("close parquet writer: %v", err)
	}
	return buf.Bytes()
}

// testSetup creates all the components needed for integration tests.
type testSetup struct {
	pool     *mockS3Pool
	store    *TombstoneStore
	manifest *mockManifest
	// files is the real manifest the rewrite scheduler publishes into. Without
	// one the scheduler refuses to rewrite at all, which is the safeguard that
	// keeps an unwired deployment from orphaning its replacements.
	files     *lhmanifest.Manifest
	detector  *StorageClassDetector
	rewriter  *Rewriter
	scheduler *RewriteScheduler
	handler   *Handler
	mux       *http.ServeMux
}

// setFiles registers the given files with BOTH the handler's query-side mock
// and the real manifest the rewrite scheduler publishes into, so a test cannot
// accidentally set up a state where the two already disagree.
func (ts *testSetup) setFiles(files []FileInfo) {
	ts.manifest.files = files
	objects := make([]lhmanifest.ListedObject, 0, len(files))
	for _, f := range files {
		ts.files.AddFile(extractPartition(f.Key), lhmanifest.FileInfo{
			Key:       f.Key,
			Size:      f.Size,
			MinTimeNs: f.MinTimeNs,
			MaxTimeNs: f.MaxTimeNs,
			RowCount:  1,
		})
		objects = append(objects, lhmanifest.ListedObject{Key: f.Key, Size: f.Size})
	}
	// A running node has listed its bucket; until a manifest has, the scheduler
	// refuses to read an absent key as a deleted object and never retires a
	// tombstone (see Manifest.Listed).
	ts.files.ApplyListing(objects, time.Now())
}

func newTestSetup(t *testing.T, lifecycleRules []LifecycleRule) *testSetup {
	t.Helper()

	pool := newMockS3Pool()
	store := NewTombstoneStore()
	manifest := &mockManifest{}
	detector := NewStorageClassDetector(lifecycleRules)
	rewriter := NewRewriter(pool, "logs/", 10000, "logs")

	cfg := &config.DeleteConfig{
		Enabled:     true,
		DefaultMode: "auto",
	}

	handler := NewHandler(store, manifest, detector, cfg, "logs")

	files := lhmanifest.New("test-bucket", "")

	scheduler := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       rewriter,
		Detector:       detector,
		RewriteDelay:   0, // no delay for tests
		AllowedClasses: []string{"STANDARD"},
		MaxConcurrent:  1,
		Manifest:       files,
	})

	mux := http.NewServeMux()
	handler.Register(mux)

	return &testSetup{
		pool:      pool,
		store:     store,
		manifest:  manifest,
		files:     files,
		detector:  detector,
		rewriter:  rewriter,
		scheduler: scheduler,
		handler:   handler,
		mux:       mux,
	}
}

func (s *testSetup) doPost(t *testing.T, path string, params url.Values) map[string]any {
	t.Helper()
	body := params.Encode()
	req := httptest.NewRequest(http.MethodPost, path, strings.NewReader(body))
	req.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("POST %s: expected 200, got %d: %s", path, w.Code, w.Body.String())
	}

	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal response from %s: %v", path, err)
	}
	return result
}

func (s *testSetup) doGet(t *testing.T, path string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("GET %s: expected 200, got %d: %s", path, w.Code, w.Body.String())
	}

	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal response from %s: %v", path, err)
	}
	return result
}

// doGetExpect asserts the status code of a GET without requiring a JSON body —
// used where the interesting outcome IS the status (a retired tombstone 404s).
func (s *testSetup) doGetExpect(t *testing.T, path string, want int) {
	t.Helper()
	req := httptest.NewRequest(http.MethodGet, path, nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)
	if w.Code != want {
		t.Fatalf("GET %s: expected %d, got %d: %s", path, want, w.Code, w.Body.String())
	}
}

func (s *testSetup) doDelete(t *testing.T, path string) map[string]any {
	t.Helper()
	req := httptest.NewRequest(http.MethodDelete, path, nil)
	w := httptest.NewRecorder()
	s.mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("DELETE %s: expected 200, got %d: %s", path, w.Code, w.Body.String())
	}

	var result map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
		t.Fatalf("unmarshal response from %s: %v", path, err)
	}
	return result
}

func TestIntegration_FullRoundTrip_Permanent(t *testing.T) {
	ts := newTestSetup(t, nil)

	// Generate test Parquet data: 5 rows, 2 of which will match the delete query.
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "all good", SeverityText: "info"},
		{TimestampUnixNano: 1500, Body: "error occurred", SeverityText: "error"},
		{TimestampUnixNano: 2000, Body: "normal log", SeverityText: "info"},
		{TimestampUnixNano: 2500, Body: "another error", SeverityText: "error"},
		{TimestampUnixNano: 3000, Body: "final log", SeverityText: "info"},
	}

	fileKey := "logs/dt=2026-01-01/hour=10/00001.parquet"
	parquetData := writeTestParquet(t, rows)
	if err := ts.pool.Upload(context.Background(), fileKey, parquetData); err != nil {
		t.Fatalf("upload test file: %v", err)
	}

	// Register file in manifest.
	ts.setFiles([]FileInfo{
		{Key: fileKey, Size: int64(len(parquetData)), MinTimeNs: 1000, MaxTimeNs: 3000},
	})

	// Step 1: POST /delete/logsql/estimate
	estimateResp := ts.doPost(t, "/delete/logsql/estimate", url.Values{
		"query": []string{`severity_text:="error"`},
		"start": []string{"1000"},
		"end":   []string{"3000"},
	})
	affectedFiles := int(estimateResp["affected_files"].(float64))
	if affectedFiles != 1 {
		t.Fatalf("estimate: expected 1 affected file, got %d", affectedFiles)
	}

	// Step 2: POST /delete/logsql/delete?mode=permanent
	deleteResp := ts.doPost(t, "/delete/logsql/delete", url.Values{
		"query": []string{`severity_text:="error"`},
		"start": []string{"1000"},
		"end":   []string{"3000"},
		"mode":  []string{"permanent"},
	})
	tombstoneID, ok := deleteResp["tombstone_id"].(string)
	if !ok || tombstoneID == "" {
		t.Fatalf("delete: expected tombstone_id, got %v", deleteResp)
	}
	if deleteResp["mode"] != "permanent" {
		t.Fatalf("delete: expected mode=permanent, got %v", deleteResp["mode"])
	}

	// Step 3: GET /delete/logsql/tombstones
	listResp := ts.doGet(t, "/delete/logsql/tombstones")
	count := int(listResp["count"].(float64))
	if count != 1 {
		t.Fatalf("list: expected 1 tombstone, got %d", count)
	}

	// Step 4: POST /delete/logsql/verify
	verifyResp := ts.doPost(t, "/delete/logsql/verify", url.Values{
		"query": []string{`severity_text:="error"`},
		"start": []string{"1000"},
		"end":   []string{"3000"},
	})
	if verifyResp["verified"] != true {
		t.Fatalf("verify: expected verified=true, got %v", verifyResp["verified"])
	}
	coverage := verifyResp["coverage"].(float64)
	if coverage != 1.0 {
		t.Fatalf("verify: expected coverage=1.0, got %f", coverage)
	}

	// Step 5: Run scheduler.RunOnce() to trigger rewrite.
	// Set the tombstone's CreatedAt to the past to bypass rewrite delay.
	storedTS, _ := ts.store.Get(tombstoneID)
	storedTS.CreatedAt = time.Now().Add(-1 * time.Hour)
	ts.store.Add(storedTS)

	rewritesBefore := metrics.DeleteRewriteTotal.Get()
	results := ts.scheduler.RunOnce(context.Background())
	rewritesAfter := metrics.DeleteRewriteTotal.Get()

	if rewritesAfter <= rewritesBefore {
		t.Fatal("expected rewrite metric to increment")
	}
	if len(results) == 0 {
		t.Fatal("expected at least one rewrite result")
	}

	result := results[0]
	if result.RowsRemoved != 2 {
		t.Fatalf("expected 2 rows removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 3 {
		t.Fatalf("expected 3 rows kept, got %d", result.RowsKept)
	}

	// Step 6: Verify old file is deleted, new file exists.
	oldExists := ts.pool.Has(fileKey)
	newExists := ts.pool.Has(result.NewKey)

	if oldExists {
		t.Fatal("expected old file to be deleted from pool")
	}
	if !newExists {
		t.Fatalf("expected new file %s to exist in pool", result.NewKey)
	}

	// Step 7: the manifest now serves the kept rows from the replacement and no
	// longer points at the object that was deleted in step 6. This is the whole
	// point of the rewrite: neither the kept rows nor the deleted ones may be
	// reachable through a stale entry.
	if ts.files.HasKey(fileKey) {
		t.Fatalf("manifest still points at the deleted object %s", fileKey)
	}
	if !ts.files.HasKey(result.NewKey) {
		t.Fatalf("manifest does not know the replacement %s", result.NewKey)
	}
	if fi, ok := ts.files.GetFileByKey(result.NewKey); !ok || fi.RowCount != result.RowsKept {
		t.Fatalf("replacement RowCount = %d, want RowsKept = %d", fi.RowCount, result.RowsKept)
	}

	// Step 8: the tombstone covered exactly one file, that file is rewritten,
	// so the tombstone is retired — GET must now 404 rather than return a
	// record that stays Active() forever.
	ts.doGetExpect(t, "/delete/logsql/tombstone/"+tombstoneID, http.StatusNotFound)
	if _, still := ts.store.Get(tombstoneID); still {
		t.Fatal("a fully reaped tombstone must be completed, not left active")
	}
}

func TestIntegration_ModeHide_NoRewrite(t *testing.T) {
	ts := newTestSetup(t, nil)

	// Generate test Parquet data.
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "error log", SeverityText: "error"},
		{TimestampUnixNano: 2000, Body: "info log", SeverityText: "info"},
	}

	fileKey := "logs/dt=2026-01-01/hour=11/00001.parquet"
	parquetData := writeTestParquet(t, rows)
	if err := ts.pool.Upload(context.Background(), fileKey, parquetData); err != nil {
		t.Fatalf("upload test file: %v", err)
	}

	ts.setFiles([]FileInfo{
		{Key: fileKey, Size: int64(len(parquetData)), MinTimeNs: 1000, MaxTimeNs: 2000},
	})

	// POST /delete/logsql/delete?mode=hide
	deleteResp := ts.doPost(t, "/delete/logsql/delete", url.Values{
		"query": []string{`severity_text:="error"`},
		"start": []string{"1000"},
		"end":   []string{"2000"},
		"mode":  []string{"hide"},
	})
	tombstoneID := deleteResp["tombstone_id"].(string)
	if tombstoneID == "" {
		t.Fatal("expected tombstone to be created")
	}

	// Set CreatedAt to past to bypass any delay.
	storedTS, _ := ts.store.Get(tombstoneID)
	storedTS.CreatedAt = time.Now().Add(-1 * time.Hour)
	ts.store.Add(storedTS)

	// Run scheduler - mode=hide should NOT trigger rewrite.
	rewritesBefore := metrics.DeleteRewriteTotal.Get()
	results := ts.scheduler.RunOnce(context.Background())
	rewritesAfter := metrics.DeleteRewriteTotal.Get()

	if rewritesAfter != rewritesBefore {
		t.Fatal("expected no rewrite for mode=hide")
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for mode=hide, got %d", len(results))
	}

	// Verify old file still exists.
	exists := ts.pool.Has(fileKey)

	if !exists {
		t.Fatal("expected file to still exist for mode=hide")
	}
}

func TestIntegration_Undelete(t *testing.T) {
	ts := newTestSetup(t, nil)

	fileKey := "logs/dt=2026-01-01/hour=12/00001.parquet"
	ts.setFiles([]FileInfo{
		{Key: fileKey, Size: 1024, MinTimeNs: 1000, MaxTimeNs: 3000},
	})

	// Create a tombstone.
	deleteResp := ts.doPost(t, "/delete/logsql/delete", url.Values{
		"query": []string{`severity_text:="error"`},
		"start": []string{"1000"},
		"end":   []string{"3000"},
		"mode":  []string{"permanent"},
	})
	tombstoneID := deleteResp["tombstone_id"].(string)

	// Verify tombstone exists.
	if ts.store.Count() != 1 {
		t.Fatalf("expected 1 tombstone, got %d", ts.store.Count())
	}

	// DELETE /delete/logsql/tombstone/{id} to un-delete.
	undeleteResp := ts.doDelete(t, "/delete/logsql/tombstone/"+tombstoneID)
	if undeleteResp["status"] != "removed" {
		t.Fatalf("expected status=removed, got %v", undeleteResp["status"])
	}

	// Verify store is empty.
	if ts.store.Count() != 0 {
		t.Fatalf("expected 0 tombstones after un-delete, got %d", ts.store.Count())
	}

	// Verify GET returns 404 now.
	req := httptest.NewRequest(http.MethodGet, "/delete/logsql/tombstone/"+tombstoneID, nil)
	w := httptest.NewRecorder()
	ts.mux.ServeHTTP(w, req)
	if w.Code != http.StatusNotFound {
		t.Fatalf("expected 404 for removed tombstone, got %d", w.Code)
	}
}

func TestIntegration_GlacierSkip(t *testing.T) {
	// Configure lifecycle rules: files older than 1 day go to GLACIER.
	rules := []LifecycleRule{
		{TransitionDays: 1, Class: ClassGlacier},
	}
	ts := newTestSetup(t, rules)

	// Generate test Parquet data.
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "error log", SeverityText: "error"},
		{TimestampUnixNano: 2000, Body: "info log", SeverityText: "info"},
	}

	fileKey := "logs/dt=2026-01-01/hour=13/00001.parquet"
	parquetData := writeTestParquet(t, rows)
	if err := ts.pool.Upload(context.Background(), fileKey, parquetData); err != nil {
		t.Fatalf("upload test file: %v", err)
	}

	ts.setFiles([]FileInfo{
		{Key: fileKey, Size: int64(len(parquetData)), MinTimeNs: 1000, MaxTimeNs: 2000},
	})

	// Create tombstone with mode=permanent.
	deleteResp := ts.doPost(t, "/delete/logsql/delete", url.Values{
		"query": []string{`severity_text:="error"`},
		"start": []string{"1000"},
		"end":   []string{"2000"},
		"mode":  []string{"permanent"},
	})
	tombstoneID := deleteResp["tombstone_id"].(string)

	// Set CreatedAt to 48 hours ago - this means the detector will see
	// fileAgeHours > 24 and predict GLACIER class.
	storedTS, _ := ts.store.Get(tombstoneID)
	storedTS.CreatedAt = time.Now().Add(-48 * time.Hour)
	ts.store.Add(storedTS)

	// Record metric before.
	glacierSkipBefore := metrics.DeleteRewriteSkippedGlacier.Get()
	rewritesBefore := metrics.DeleteRewriteTotal.Get()

	// Run scheduler - should skip due to GLACIER class.
	results := ts.scheduler.RunOnce(context.Background())

	glacierSkipAfter := metrics.DeleteRewriteSkippedGlacier.Get()
	rewritesAfter := metrics.DeleteRewriteTotal.Get()

	// No rewrite should have happened.
	if rewritesAfter != rewritesBefore {
		t.Fatal("expected no rewrite for glacier-class file")
	}
	if len(results) != 0 {
		t.Fatalf("expected 0 results for glacier skip, got %d", len(results))
	}

	// Glacier skip metric should have incremented.
	if glacierSkipAfter <= glacierSkipBefore {
		t.Fatal("expected DeleteRewriteSkippedGlacier metric to increment")
	}

	// Verify file still exists (not deleted).
	exists := ts.pool.Has(fileKey)

	if !exists {
		t.Fatal("expected file to still exist when glacier skip occurs")
	}

	// Verify tombstone key is NOT marked as reaped.
	storedTS, _ = ts.store.Get(tombstoneID)
	if storedTS.Reaped != nil && storedTS.Reaped[fileKey] {
		t.Fatal("expected file NOT to be marked as reaped for glacier skip")
	}
}

// TestIntegration_AllRowsRemoved verifies that when a rewrite removes all rows,
// the old file is deleted and no new file is uploaded.
func TestIntegration_AllRowsRemoved(t *testing.T) {
	ts := newTestSetup(t, nil)

	// All rows match the delete query.
	rows := []schema.LogRow{
		{TimestampUnixNano: 1000, Body: "error 1", SeverityText: "error"},
		{TimestampUnixNano: 2000, Body: "error 2", SeverityText: "error"},
	}

	fileKey := "logs/dt=2026-01-01/hour=14/00001.parquet"
	parquetData := writeTestParquet(t, rows)
	if err := ts.pool.Upload(context.Background(), fileKey, parquetData); err != nil {
		t.Fatalf("upload test file: %v", err)
	}

	ts.setFiles([]FileInfo{
		{Key: fileKey, Size: int64(len(parquetData)), MinTimeNs: 1000, MaxTimeNs: 2000},
	})

	// Create tombstone with wildcard query (matches all rows).
	deleteResp := ts.doPost(t, "/delete/logsql/delete", url.Values{
		"query": []string{"*"},
		"start": []string{"1000"},
		"end":   []string{"2000"},
		"mode":  []string{"permanent"},
	})
	tombstoneID := deleteResp["tombstone_id"].(string)

	storedTS, _ := ts.store.Get(tombstoneID)
	storedTS.CreatedAt = time.Now().Add(-1 * time.Hour)
	ts.store.Add(storedTS)

	results := ts.scheduler.RunOnce(context.Background())
	if len(results) == 0 {
		t.Fatal("expected a rewrite result")
	}

	result := results[0]
	if result.RowsRemoved != 2 {
		t.Fatalf("expected 2 rows removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 0 {
		t.Fatalf("expected 0 rows kept, got %d", result.RowsKept)
	}

	// Verify old file is deleted and no new file is uploaded.
	oldExists := ts.pool.Has(fileKey)

	if oldExists {
		t.Fatal("expected old file to be deleted")
	}
	// When all rows are removed, the rewriter deletes without uploading.
	// newKey should be empty string.
	if result.NewKey != "" {
		t.Fatalf("expected empty NewKey when all rows removed, got %s", result.NewKey)
	}
}

// --- Trace integration tests ---

func TestIntegration_TraceDelete_FullRoundTrip(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockS3Pool()

	// Upload trace Parquet file with mixed services
	rows := []schema.TraceRow{
		{TimestampUnixNano: 1000, TraceID: "t1", SpanID: "s1", SpanName: "GET /users", ServiceName: "user-svc"},
		{TimestampUnixNano: 2000, TraceID: "t1", SpanID: "s2", SpanName: "DB query", ServiceName: "user-svc"},
		{TimestampUnixNano: 3000, TraceID: "t2", SpanID: "s3", SpanName: "GET /orders", ServiceName: "order-svc"},
		{TimestampUnixNano: 4000, TraceID: "t2", SpanID: "s4", SpanName: "payment", ServiceName: "payment-svc"},
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.TraceRow](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	key := "traces/dt=2026-05-02/hour=10/batch.parquet"
	pool.Put(key, buf.Bytes())

	manifest := &mockManifest{files: []FileInfo{
		{Key: key, Size: int64(len(mustGet(t, pool, key))), MinTimeNs: 1000, MaxTimeNs: 4000},
	}}

	cfg := &config.DeleteConfig{
		Enabled:              true,
		DefaultMode:          "auto",
		AutoRewriteClasses:   []string{"STANDARD"},
		RewriteDelay:         0,
		RewriteBatchSize:     10,
		RewriteMaxConcurrent: 2,
	}

	detector := NewStorageClassDetector(nil)
	handler := NewHandler(store, manifest, detector, cfg, "traces")

	// Create tombstone via API
	mux := http.NewServeMux()
	handler.Register(mux)

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
		t.Fatalf("delete request failed: %d: %s", w.Code, w.Body.String())
	}

	var resp map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("unmarshal response: %v", err)
	}

	// Rewrite the file
	rewriter := NewRewriter(pool, "traces/", 10000, "traces")
	active := store.Active()
	if len(active) != 1 {
		t.Fatalf("expected 1 active tombstone, got %d", len(active))
	}

	result, err := rewriter.RewriteFile(context.Background(), key, active)
	if err != nil {
		t.Fatalf("rewrite error: %v", err)
	}

	if result.RowsRemoved != 1 {
		t.Fatalf("expected 1 row removed (order-svc), got %d", result.RowsRemoved)
	}
	if result.RowsKept != 3 {
		t.Fatalf("expected 3 rows kept, got %d", result.RowsKept)
	}

	// Verify new file contents
	newData := mustGet(t, pool, result.NewKey)
	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(newData))
	defer func() { _ = reader.Close() }()

	readRows := make([]schema.TraceRow, 10)
	n, _ := reader.Read(readRows)
	if n != 3 {
		t.Fatalf("expected 3 rows, got %d", n)
	}
	for i := 0; i < n; i++ {
		if readRows[i].ServiceName == "order-svc" {
			t.Fatalf("order-svc span should be deleted")
		}
	}
}

func TestIntegration_TraceDelete_ByTraceID(t *testing.T) {
	store := NewTombstoneStore()
	pool := newMockS3Pool()

	rows := []schema.TraceRow{
		{TimestampUnixNano: 1000, TraceID: "trace-aaa", SpanID: "s1", SpanName: "root", ServiceName: "svc"},
		{TimestampUnixNano: 2000, TraceID: "trace-aaa", SpanID: "s2", SpanName: "child", ServiceName: "svc"},
		{TimestampUnixNano: 3000, TraceID: "trace-bbb", SpanID: "s3", SpanName: "other", ServiceName: "svc"},
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.TraceRow](&buf)
	if _, err := writer.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	key := "traces/dt=2026-05-02/hour=10/batch2.parquet"
	pool.Put(key, buf.Bytes())

	// Create tombstone for specific trace ID
	ts := Tombstone{
		ID:           "del-trace",
		Query:        `trace_id:="trace-aaa"`,
		StartNs:      0,
		EndNs:        10000,
		AffectedKeys: []string{key},
		CreatedAt:    time.Now().Add(-2 * time.Hour),
		Mode:         "permanent",
		Reaped:       make(map[string]bool),
	}
	store.Add(ts)

	rewriter := NewRewriter(pool, "traces/", 10000, "traces")
	result, err := rewriter.RewriteFile(context.Background(), key, []Tombstone{ts})
	if err != nil {
		t.Fatalf("rewrite error: %v", err)
	}

	if result.RowsRemoved != 2 {
		t.Fatalf("expected 2 rows removed (both trace-aaa spans), got %d", result.RowsRemoved)
	}
	if result.RowsKept != 1 {
		t.Fatalf("expected 1 row kept (trace-bbb), got %d", result.RowsKept)
	}

	_ = metrics.DeleteRewriteTotal
}
