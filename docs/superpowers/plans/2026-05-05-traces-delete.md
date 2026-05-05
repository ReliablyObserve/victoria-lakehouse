# Traces Delete Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Extend cost-aware deletion to traces mode — same tombstone/rewrite/query-filtering infrastructure, different Parquet schema and API endpoints.

**Architecture:** The existing delete package is logs-only (hardcoded `schema.LogRow`, `logRowToMap`, `/delete/logsql/*` endpoints). This plan makes it mode-aware: adds `traceRowToMap`, a generic rewriter dispatcher, `/delete/tracessql/*` endpoints, and trace-aware query filtering. 80% of infrastructure is reused as-is.

**Tech Stack:** Go 1.26, parquet-go v0.29.0, existing `internal/delete/*` package, `schema.TraceRow`.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/delete/rewriter.go` | Modify: add TraceRewriter, traceRowToMap, mode dispatch |
| `internal/delete/rewriter_test.go` | Modify: add trace rewrite tests |
| `internal/delete/handler.go` | Modify: add `/delete/tracessql/*` endpoint registration |
| `internal/delete/handler_test.go` | Modify: add trace handler tests |
| `internal/delete/tombstone.go` | Modify: add MatchesTraceRow helper (body fallback → SpanName) |
| `internal/delete/tombstone_test.go` | Modify: add trace-specific matching tests |
| `internal/delete/integration_test.go` | Modify: add trace integration test |
| `internal/storage/parquets3/storage.go` | No change: filterTombstonedRows already works on DataBlock columns (mode-agnostic) |
| `cmd/lakehouse/main.go` | Modify: pass mode to Rewriter and Handler |
| `tests/e2e/delete_test.go` | Modify: add trace E2E if mode=traces |

---

### Task 1: traceRowToMap + Trace Tombstone Matching

**Files:**
- Modify: `internal/delete/rewriter.go`
- Modify: `internal/delete/tombstone.go`
- Test: `internal/delete/tombstone_test.go`

- [ ] **Step 1: Write the failing test for traceRowToMap**

In `internal/delete/tombstone_test.go`, add:

```go
func TestMatchesRow_TraceFields(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		Query:   `service.name:="order-service"`,
		StartNs: 0,
		EndNs:   100_000_000_000,
	}

	traceFields := map[string]string{
		"trace_id":     "abc123",
		"span_id":      "span1",
		"span.name":    "HTTP GET /api/orders",
		"service.name": "order-service",
		"status.code":  "2",
		"http.method":  "GET",
	}

	if !ts.MatchesRow(traceFields, 50_000_000_000) {
		t.Fatal("expected tombstone to match trace row by service.name")
	}

	// Wildcard query should match span.name as body fallback for traces
	tsWild := Tombstone{
		ID:      "t2",
		Query:   `*`,
		StartNs: 0,
		EndNs:   100_000_000_000,
	}
	if !tsWild.MatchesRow(traceFields, 50_000_000_000) {
		t.Fatal("wildcard should match trace row")
	}
}
```

- [ ] **Step 2: Run test to verify it passes (MatchesRow already works on any map)**

Run: `go test ./internal/delete/ -run TestMatchesRow_TraceFields -v`
Expected: PASS — MatchesRow is already field-map generic. The test validates this.

- [ ] **Step 3: Write traceRowToMap in rewriter.go**

Add below `logRowToMap`:

```go
// traceRowToMap converts a TraceRow into a map[string]string for tombstone matching.
func traceRowToMap(row *schema.TraceRow) map[string]string {
	m := map[string]string{
		"trace_id":            row.TraceID,
		"span_id":             row.SpanID,
		"parent_span_id":      row.ParentSpanID,
		"span.name":           row.SpanName,
		"service.name":        row.ServiceName,
		"status.message":      row.StatusMessage,
		"scope.name":          row.ScopeName,
		"deployment.environment": row.DeployEnv,
		"cloud.region":        row.CloudRegion,
		"host.name":           row.HostName,
		"k8s.namespace.name":  row.K8sNamespaceName,
		"k8s.deployment.name": row.K8sDeploymentName,
		"k8s.node.name":       row.K8sNodeName,
		"http.method":         row.HTTPMethod,
		"http.status_code":    row.HTTPStatusCode,
		"http.url":            row.HTTPUrl,
		"db.system":           row.DBSystem,
		"db.statement":        row.DBStatement,
		"body":                row.SpanName,
	}
	// Integer fields as strings for matching
	if row.SpanKind != 0 {
		m["span.kind"] = fmt.Sprintf("%d", row.SpanKind)
	}
	if row.StatusCode != 0 {
		m["status.code"] = fmt.Sprintf("%d", row.StatusCode)
	}
	if row.DurationNs != 0 {
		m["duration_ns"] = fmt.Sprintf("%d", row.DurationNs)
	}
	// Merge attribute maps
	for k, v := range row.ResourceAttributes {
		m[k] = v
	}
	for k, v := range row.SpanAttributes {
		m[k] = v
	}
	for k, v := range row.ScopeAttributes {
		m[k] = v
	}
	return m
}
```

- [ ] **Step 4: Write test for traceRowToMap**

In `internal/delete/rewriter_test.go`, add:

```go
func TestTraceRowToMap(t *testing.T) {
	row := schema.TraceRow{
		TraceID:      "trace123",
		SpanID:       "span456",
		SpanName:     "HTTP GET /users",
		ServiceName:  "user-service",
		StatusCode:   2,
		DurationNs:   50000000,
		HTTPMethod:   "GET",
		HTTPUrl:      "http://user-service:8080/users",
		DeployEnv:    "production",
		SpanAttributes: map[string]string{"custom.field": "value1"},
	}

	m := traceRowToMap(&row)

	if m["trace_id"] != "trace123" {
		t.Fatalf("expected trace123, got %s", m["trace_id"])
	}
	if m["span.name"] != "HTTP GET /users" {
		t.Fatalf("expected span name, got %s", m["span.name"])
	}
	if m["body"] != "HTTP GET /users" {
		t.Fatal("body should map to SpanName for traces")
	}
	if m["status.code"] != "2" {
		t.Fatalf("expected status.code=2, got %s", m["status.code"])
	}
	if m["custom.field"] != "value1" {
		t.Fatal("span attributes should be merged")
	}
}
```

- [ ] **Step 5: Run tests**

Run: `go test ./internal/delete/ -run TestTraceRowToMap -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/delete/rewriter.go internal/delete/tombstone_test.go internal/delete/rewriter_test.go
git commit -m "feat(delete): add traceRowToMap for trace tombstone matching"
```

---

### Task 2: Mode-Aware Rewriter

**Files:**
- Modify: `internal/delete/rewriter.go`
- Test: `internal/delete/rewriter_test.go`

- [ ] **Step 1: Add mode field to Rewriter**

Modify the `Rewriter` struct and `NewRewriter`:

```go
type Rewriter struct {
	pool         RewriterPool
	prefix       string
	rowGroupSize int
	mode         string // "logs" or "traces"
}

func NewRewriter(pool RewriterPool, prefix string, rowGroupSize int, mode string) *Rewriter {
	if rowGroupSize <= 0 {
		rowGroupSize = 10000
	}
	if mode == "" {
		mode = "logs"
	}
	return &Rewriter{
		pool:         pool,
		prefix:       prefix,
		rowGroupSize: rowGroupSize,
		mode:         mode,
	}
}
```

- [ ] **Step 2: Write failing test for trace rewrite**

In `internal/delete/rewriter_test.go`:

```go
func TestRewriteFile_Traces(t *testing.T) {
	pool := &mockRewriterPool{objects: make(map[string][]byte)}

	// Create test trace Parquet file with 5 spans
	rows := []schema.TraceRow{
		{TimestampUnixNano: 1000, TraceID: "t1", SpanID: "s1", SpanName: "GET /users", ServiceName: "user-svc", StatusCode: 0},
		{TimestampUnixNano: 2000, TraceID: "t1", SpanID: "s2", SpanName: "DB SELECT", ServiceName: "user-svc", StatusCode: 0},
		{TimestampUnixNano: 3000, TraceID: "t2", SpanID: "s3", SpanName: "GET /orders", ServiceName: "order-svc", StatusCode: 2},
		{TimestampUnixNano: 4000, TraceID: "t2", SpanID: "s4", SpanName: "DB INSERT", ServiceName: "order-svc", StatusCode: 0},
		{TimestampUnixNano: 5000, TraceID: "t3", SpanID: "s5", SpanName: "GET /health", ServiceName: "user-svc", StatusCode: 0},
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.TraceRow](&buf)
	_, err := writer.Write(rows)
	if err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}

	key := "traces/dt=2026-05-02/hour=10/batch-01.parquet"
	pool.objects[key] = buf.Bytes()

	// Tombstone: delete all order-svc spans
	tombstone := Tombstone{
		ID:      "ts1",
		Query:   `service.name:="order-svc"`,
		StartNs: 0,
		EndNs:   10000,
	}

	rw := NewRewriter(pool, "traces/", 1000, "traces")
	result, err := rw.RewriteFile(context.Background(), key, []Tombstone{tombstone})
	if err != nil {
		t.Fatalf("rewrite error: %v", err)
	}

	if result.RowsRemoved != 2 {
		t.Fatalf("expected 2 rows removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 3 {
		t.Fatalf("expected 3 rows kept, got %d", result.RowsKept)
	}

	// Verify new file has correct rows
	newData := pool.objects[result.NewKey]
	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(newData))
	defer func() { _ = reader.Close() }()

	readRows := make([]schema.TraceRow, 10)
	n, _ := reader.Read(readRows)
	if n != 3 {
		t.Fatalf("expected 3 rows in new file, got %d", n)
	}

	for i := 0; i < n; i++ {
		if readRows[i].ServiceName == "order-svc" {
			t.Fatalf("row %d should not be order-svc", i)
		}
	}
}
```

- [ ] **Step 3: Run test to verify it fails**

Run: `go test ./internal/delete/ -run TestRewriteFile_Traces -v`
Expected: FAIL — RewriteFile still uses LogRow

- [ ] **Step 4: Implement mode dispatch in RewriteFile**

Replace the body of `RewriteFile` with mode dispatch:

```go
func (r *Rewriter) RewriteFile(ctx context.Context, key string, tombstones []Tombstone) (*RewriteResult, error) {
	start := time.Now()

	data, err := r.pool.Download(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", key, err)
	}

	result := &RewriteResult{
		OldKey:      key,
		BytesBefore: int64(len(data)),
	}

	switch r.mode {
	case "traces":
		err = r.rewriteTraceRows(ctx, data, tombstones, result)
	default:
		err = r.rewriteLogRows(ctx, data, tombstones, result)
	}
	if err != nil {
		return nil, err
	}

	// If nothing was removed, leave the file untouched.
	if result.RowsRemoved == 0 {
		result.Duration = time.Since(start)
		return result, nil
	}

	// If all rows removed, delete old file.
	if result.RowsKept == 0 {
		if err := r.pool.Delete(ctx, key); err != nil {
			return nil, fmt.Errorf("delete empty file %s: %w", key, err)
		}
		result.BytesAfter = 0
		result.Duration = time.Since(start)
		return result, nil
	}

	// Upload new file, delete old.
	partition := extractPartition(key)
	short := uuid.New().String()[:8]
	newKey := fmt.Sprintf("%s%s/%s.parquet", r.prefix, partition, short)
	result.NewKey = newKey

	if err := r.pool.Upload(ctx, newKey, result.newData); err != nil {
		return nil, fmt.Errorf("upload %s: %w", newKey, err)
	}
	if err := r.pool.Delete(ctx, key); err != nil {
		return nil, fmt.Errorf("delete old file %s: %w", key, err)
	}

	result.Duration = time.Since(start)
	return result, nil
}
```

Add a `newData` field to RewriteResult (unexported, internal use):

```go
type RewriteResult struct {
	OldKey      string
	NewKey      string
	RowsKept    int64
	RowsRemoved int64
	BytesBefore int64
	BytesAfter  int64
	Duration    time.Duration
	newData     []byte // internal: holds written Parquet bytes for upload
}
```

Add `rewriteLogRows` (extracted from current code):

```go
func (r *Rewriter) rewriteLogRows(_ context.Context, data []byte, tombstones []Tombstone, result *RewriteResult) error {
	reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	n := int(reader.NumRows())
	rows := make([]schema.LogRow, n)
	total, err := reader.Read(rows)
	if err != nil && total == 0 {
		return fmt.Errorf("read parquet rows: %w", err)
	}
	rows = rows[:total]

	kept := make([]schema.LogRow, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		fields := logRowToMap(row)
		ts := row.TimestampUnixNano

		matched := false
		for j := range tombstones {
			if tombstones[j].MatchesRow(fields, ts) {
				matched = true
				break
			}
		}
		if !matched {
			kept = append(kept, *row)
		}
	}

	result.RowsRemoved = int64(len(rows)) - int64(len(kept))
	result.RowsKept = int64(len(kept))

	if result.RowsRemoved == 0 || len(kept) == 0 {
		return nil
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.MaxRowsPerRowGroup(int64(r.rowGroupSize)),
	)
	if _, err := writer.Write(kept); err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close parquet writer: %w", err)
	}

	result.newData = buf.Bytes()
	result.BytesAfter = int64(len(result.newData))
	return nil
}
```

Add `rewriteTraceRows`:

```go
func (r *Rewriter) rewriteTraceRows(_ context.Context, data []byte, tombstones []Tombstone, result *RewriteResult) error {
	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	n := int(reader.NumRows())
	rows := make([]schema.TraceRow, n)
	total, err := reader.Read(rows)
	if err != nil && total == 0 {
		return fmt.Errorf("read parquet rows: %w", err)
	}
	rows = rows[:total]

	kept := make([]schema.TraceRow, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		fields := traceRowToMap(row)
		ts := row.TimestampUnixNano

		matched := false
		for j := range tombstones {
			if tombstones[j].MatchesRow(fields, ts) {
				matched = true
				break
			}
		}
		if !matched {
			kept = append(kept, *row)
		}
	}

	result.RowsRemoved = int64(len(rows)) - int64(len(kept))
	result.RowsKept = int64(len(kept))

	if result.RowsRemoved == 0 || len(kept) == 0 {
		return nil
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.TraceRow](&buf,
		parquet.MaxRowsPerRowGroup(int64(r.rowGroupSize)),
	)
	if _, err := writer.Write(kept); err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return fmt.Errorf("close parquet writer: %w", err)
	}

	result.newData = buf.Bytes()
	result.BytesAfter = int64(len(result.newData))
	return nil
}
```

- [ ] **Step 5: Update all NewRewriter call sites to pass mode**

In `internal/delete/scheduler.go`, `internal/delete/rewriter_test.go`, `internal/delete/integration_test.go`, and `cmd/lakehouse/main.go` — add the mode parameter.

- [ ] **Step 6: Run all tests**

Run: `go test ./internal/delete/ -v`
Expected: ALL PASS

- [ ] **Step 7: Commit**

```bash
git add internal/delete/rewriter.go internal/delete/rewriter_test.go
git commit -m "feat(delete): mode-aware rewriter supporting both logs and traces"
```

---

### Task 3: Trace Delete Handler Endpoints

**Files:**
- Modify: `internal/delete/handler.go`
- Test: `internal/delete/handler_test.go`

- [ ] **Step 1: Write failing test for trace delete endpoint**

In `internal/delete/handler_test.go`, add:

```go
func TestHandler_TraceDelete(t *testing.T) {
	store := NewTombstoneStore()
	mf := &mockManifestQuerier{files: []FileInfo{
		{Key: "traces/dt=2026-05-02/hour=10/batch.parquet", Size: 1024, MinTimeNs: 1000, MaxTimeNs: 5000},
	}}
	detector := NewStorageClassDetector(nil)

	h := NewHandler(store, mf, detector, "traces")
	mux := http.NewServeMux()
	h.Register(mux)

	body := `query=service.name:="order-svc"&start=0&end=10000&mode=permanent`
	req := httptest.NewRequest("POST", "/delete/tracessql/delete", strings.NewReader(body))
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
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/delete/ -run TestHandler_TraceDelete -v`
Expected: FAIL — handler only registers `/delete/logsql/*`

- [ ] **Step 3: Add mode to Handler and register mode-appropriate routes**

Modify Handler struct:

```go
type Handler struct {
	store    *TombstoneStore
	manifest ManifestQuerier
	detector *StorageClassDetector
	mode     string // "logs" or "traces"
}

func NewHandler(store *TombstoneStore, manifest ManifestQuerier, detector *StorageClassDetector, mode string) *Handler {
	if mode == "" {
		mode = "logs"
	}
	return &Handler{store: store, manifest: manifest, detector: detector, mode: mode}
}
```

Modify `Register`:

```go
func (h *Handler) Register(mux *http.ServeMux) {
	var prefix string
	switch h.mode {
	case "traces":
		prefix = "/delete/tracessql"
	default:
		prefix = "/delete/logsql"
	}

	mux.HandleFunc(prefix+"/delete", h.handleDelete)
	mux.HandleFunc(prefix+"/estimate", h.handleEstimate)
	mux.HandleFunc(prefix+"/verify", h.handleVerify)
	mux.HandleFunc(prefix+"/tombstones", h.handleListTombstones)
	mux.HandleFunc(prefix+"/tombstone/", h.handleTombstoneByID)
}
```

- [ ] **Step 4: Update all NewHandler call sites**

In `cmd/lakehouse/main.go` and test files, pass the mode string.

- [ ] **Step 5: Run all tests**

Run: `go test ./internal/delete/ -v`
Expected: ALL PASS

- [ ] **Step 6: Commit**

```bash
git add internal/delete/handler.go internal/delete/handler_test.go cmd/lakehouse/main.go
git commit -m "feat(delete): register /delete/tracessql/* endpoints in traces mode"
```

---

### Task 4: Integration Test for Traces Delete

**Files:**
- Modify: `internal/delete/integration_test.go`

- [ ] **Step 1: Write trace integration test**

```go
func TestIntegration_TraceDelete_FullRoundTrip(t *testing.T) {
	store := NewTombstoneStore()
	pool := &mockIntegrationPool{objects: make(map[string][]byte)}

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
	pool.objects[key] = buf.Bytes()

	// Create tombstone for order-svc
	ts := Tombstone{
		ID:          "del-1",
		Query:       `service.name:="order-svc"`,
		StartNs:     0,
		EndNs:       10000,
		AffectedKeys: []string{key},
		CreatedAt:   time.Now().Add(-2 * time.Hour),
		Mode:        "permanent",
		Reaped:      make(map[string]bool),
	}
	store.Add(ts)

	// Create rewriter in traces mode
	rw := NewRewriter(pool, "traces/", 1000, "traces")

	// Rewrite the file
	result, err := rw.RewriteFile(context.Background(), key, []Tombstone{ts})
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
	newData := pool.objects[result.NewKey]
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
```

- [ ] **Step 2: Run test**

Run: `go test ./internal/delete/ -run TestIntegration_TraceDelete -v`
Expected: PASS (after Task 2 is complete)

- [ ] **Step 3: Commit**

```bash
git add internal/delete/integration_test.go
git commit -m "test(delete): add trace delete integration test"
```

---

### Task 5: Wire Mode into main.go and Scheduler

**Files:**
- Modify: `cmd/lakehouse/main.go`
- Modify: `internal/delete/scheduler.go`

- [ ] **Step 1: Pass mode to scheduler and rewriter in main.go**

In `cmd/lakehouse/main.go`, the Rewriter and Handler creation should use `cfg.Mode`:

```go
mode := string(cfg.Mode) // "logs" or "traces"
rewriter := delete.NewRewriter(s3PoolAdapter, cfg.S3.Prefix, cfg.Insert.RowGroupSize, mode)
handler := delete.NewHandler(tombstoneStore, manifestAdapter, detector, mode)
```

- [ ] **Step 2: Update scheduler to pass mode through**

If the scheduler creates a Rewriter internally, pass mode. If it receives a Rewriter, no change needed (already mode-aware).

- [ ] **Step 3: Run full test suite**

Run: `go test ./... 2>&1 | grep -E "FAIL|ok"`
Expected: ALL PASS

- [ ] **Step 4: Commit**

```bash
git add cmd/lakehouse/main.go internal/delete/scheduler.go
git commit -m "feat(delete): wire mode into main binary for traces delete support"
```

---

### Task 6: E2E Test for Traces Delete

**Files:**
- Modify: `tests/e2e/delete_test.go`

- [ ] **Step 1: Add trace delete E2E test**

```go
func TestDelete_Traces_TombstoneAndQuery(t *testing.T) {
	if os.Getenv("LAKEHOUSE_MODE") != "traces" {
		t.Skip("skipping trace delete E2E: LAKEHOUSE_MODE != traces")
	}

	endpoint := os.Getenv("LAKEHOUSE_ENDPOINT")
	if endpoint == "" {
		endpoint = "http://localhost:10428"
	}

	// Create tombstone via trace delete API
	form := url.Values{}
	form.Set("query", `service.name:="order-svc"`)
	form.Set("start", "0")
	form.Set("end", fmt.Sprintf("%d", time.Now().UnixNano()))
	form.Set("mode", "permanent")

	resp, err := http.PostForm(endpoint+"/delete/tracessql/delete", form)
	if err != nil {
		t.Fatalf("delete request failed: %v", err)
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("expected 200, got %d: %s", resp.StatusCode, string(body))
	}

	// Verify tombstone is listed
	resp2, err := http.Get(endpoint + "/delete/tracessql/tombstones")
	if err != nil {
		t.Fatalf("list tombstones failed: %v", err)
	}
	defer func() { _ = resp2.Body.Close() }()

	if resp2.StatusCode != http.StatusOK {
		t.Fatalf("expected 200, got %d", resp2.StatusCode)
	}
}
```

- [ ] **Step 2: Run E2E (if environment available)**

Run: `LAKEHOUSE_MODE=traces go test -tags=e2e ./tests/e2e/ -run TestDelete_Traces -v`
Expected: PASS when running against a traces-mode lakehouse instance

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/delete_test.go
git commit -m "test(delete): add traces delete E2E test"
```

---

### Task 7: Helm Chart Update for Traces Delete

**Files:**
- Modify: `charts/victoria-lakehouse/values.yaml` (no change needed — delete config is mode-agnostic)
- Verify: existing `lakehouseConfig.delete` applies to both modes

- [ ] **Step 1: Verify Helm config is mode-agnostic**

The existing `lakehouseConfig.delete` section in `values.yaml` contains:
- `enabled`, `default_mode`, `auto_rewrite_classes`, `rewrite_delay`, etc.

These are all mode-independent — they apply regardless of whether `lakehouseConfig.mode` is `logs` or `traces`. The endpoint prefix (`/delete/logsql/*` vs `/delete/tracessql/*`) is determined at runtime by the mode setting.

Run: `helm lint charts/victoria-lakehouse/`
Expected: PASS

- [ ] **Step 2: Commit (if any documentation update needed)**

No code changes needed — the existing chart already supports traces delete via mode selection.

---

## Verification

1. `go test ./internal/delete/ -v` — all unit + integration tests pass (logs AND traces)
2. `go build ./cmd/lakehouse/` — binary compiles with mode-aware delete
3. `helm lint charts/victoria-lakehouse/` — chart validates
4. Full test suite: `go test ./...` — no regressions
5. Mode=traces binary: `./lakehouse --lakehouse.mode=traces` registers `/delete/tracessql/*` endpoints

## Notes

- `filterTombstonedRows` in `storage.go` already works for traces — it operates on DataBlock columns (map-based), not on typed rows. No changes needed there.
- The tombstone store, persistence (disk + S3), storage class detection, and scheduler are all mode-agnostic — they work on file keys and time ranges.
- This plan adds ~200-300 LOC of new code while reusing 80%+ of existing infrastructure.
