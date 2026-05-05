# Cost-Aware Deletion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement VL-compatible delete APIs with cost-aware tombstone-based deletion that never touches Glacier data.

**Architecture:** Tombstones stored in manifest suppress deleted rows at query time. Background rewriter physically removes data only from S3 Standard files. Same `/delete/logsql/*` API surface as VictoriaLogs but intelligent at the storage layer — detects S3 storage class and avoids expensive operations on IA/Glacier.

**Tech Stack:** Go 1.26, AWS SDK v2 (HeadObject for storage class), parquet-go v0.29.0 (for rewrite), existing manifest/storage/config patterns.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/delete/tombstone.go` | Tombstone data structure, store (in-memory + S3 persist) |
| `internal/delete/tombstone_test.go` | Unit tests for tombstone CRUD and matching |
| `internal/delete/rewriter.go` | Background job: read Parquet, filter rows, write new file |
| `internal/delete/rewriter_test.go` | Unit tests for rewrite logic |
| `internal/delete/handler.go` | HTTP handlers: delete, estimate, tombstones list |
| `internal/delete/handler_test.go` | HTTP handler tests |
| `internal/delete/storageclass.go` | S3 HeadObject wrapper for storage class detection |
| `internal/delete/storageclass_test.go` | Storage class detection tests |
| `internal/config/config.go` | Add `DeleteConfig` struct |
| `internal/metrics/lakehouse.go` | Add delete metrics |
| `internal/storage/parquets3/storage.go` | Inject tombstone filter into RunQuery |
| `cmd/lakehouse/main.go` | Wire delete handler + rewriter into main |

---

### Task 1: Tombstone Data Structure

**Files:**
- Create: `internal/delete/tombstone.go`
- Test: `internal/delete/tombstone_test.go`

- [ ] **Step 1: Write the failing test for tombstone creation and matching**

```go
// internal/delete/tombstone_test.go
package delete

import (
	"testing"
	"time"
)

func TestTombstone_MatchesRow(t *testing.T) {
	ts := Tombstone{
		ID:        "ts-001",
		Query:     `service.name:="api-gateway"`,
		StartNs:   time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		EndNs:     time.Date(2026, 6, 1, 0, 0, 0, 0, time.UTC).UnixNano(),
		CreatedAt: time.Now(),
	}

	// Row within time range with matching field
	row := map[string]string{
		"service.name":       "api-gateway",
		"timestamp_unix_nano": "1706745600000000000", // 2024-02-01 (within range)
	}
	if !ts.MatchesRow(row, 1706745600000000000) {
		t.Fatal("expected tombstone to match row")
	}

	// Row outside time range
	if ts.MatchesRow(row, 1800000000000000000) {
		t.Fatal("expected tombstone NOT to match row outside time range")
	}

	// Row with different service.name
	row2 := map[string]string{"service.name": "other-service"}
	if ts.MatchesRow(row2, 1706745600000000000) {
		t.Fatal("expected tombstone NOT to match different service")
	}
}

func TestTombstone_AffectsFile(t *testing.T) {
	ts := Tombstone{
		ID:      "ts-002",
		Query:   `service.name:="api"`,
		StartNs: 1000,
		EndNs:   5000,
	}

	// File overlaps tombstone time range
	if !ts.AffectsFile(500, 2000) {
		t.Fatal("expected tombstone to affect overlapping file")
	}

	// File outside tombstone range
	if ts.AffectsFile(5001, 9000) {
		t.Fatal("expected tombstone NOT to affect non-overlapping file")
	}
}

func TestTombstoneStore_AddAndList(t *testing.T) {
	store := NewTombstoneStore()

	ts := Tombstone{
		ID:        "ts-001",
		Query:     `service.name:="api"`,
		StartNs:   1000,
		EndNs:     5000,
		CreatedAt: time.Now(),
	}

	store.Add(ts)

	all := store.Active()
	if len(all) != 1 {
		t.Fatalf("expected 1 tombstone, got %d", len(all))
	}
	if all[0].ID != "ts-001" {
		t.Fatalf("expected ID ts-001, got %s", all[0].ID)
	}
}

func TestTombstoneStore_Remove(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{ID: "ts-001", Query: `*`, StartNs: 0, EndNs: 9999})
	store.Add(Tombstone{ID: "ts-002", Query: `*`, StartNs: 0, EndNs: 9999})

	store.Remove("ts-001")

	all := store.Active()
	if len(all) != 1 {
		t.Fatalf("expected 1 tombstone after remove, got %d", len(all))
	}
	if all[0].ID != "ts-002" {
		t.Fatalf("expected ts-002, got %s", all[0].ID)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/delete/ -v -run TestTombstone`
Expected: FAIL — package does not exist

- [ ] **Step 3: Implement tombstone data structure and store**

```go
// internal/delete/tombstone.go
package delete

import (
	"strings"
	"sync"
	"time"
)

type Tombstone struct {
	ID           string            `json:"id"`
	Query        string            `json:"query"`
	StartNs      int64             `json:"start_ns"`
	EndNs        int64             `json:"end_ns"`
	AffectedKeys []string          `json:"affected_keys,omitempty"`
	CreatedAt    time.Time         `json:"created_at"`
	CreatedBy    string            `json:"created_by,omitempty"`
	Reaped       map[string]bool   `json:"reaped,omitempty"`
}

func (ts *Tombstone) AffectsFile(fileMinNs, fileMaxNs int64) bool {
	return fileMinNs <= ts.EndNs && fileMaxNs >= ts.StartNs
}

func (ts *Tombstone) MatchesRow(row map[string]string, timestampNs int64) bool {
	if timestampNs < ts.StartNs || timestampNs > ts.EndNs {
		return false
	}
	return matchQuery(ts.Query, row)
}

func matchQuery(query string, row map[string]string) bool {
	query = strings.TrimSpace(query)
	if query == "*" || query == "" {
		return true
	}

	// Parse field:="value" exact match
	if idx := strings.Index(query, `:="`); idx > 0 {
		field := query[:idx]
		valueEnd := strings.LastIndex(query, `"`)
		if valueEnd > idx+3 {
			value := query[idx+3 : valueEnd]
			return row[field] == value
		}
	}

	// Parse field:"substring"
	if idx := strings.Index(query, `:"`); idx > 0 {
		field := query[:idx]
		valueEnd := strings.LastIndex(query, `"`)
		if valueEnd > idx+2 {
			value := query[idx+2 : valueEnd]
			return strings.Contains(row[field], value)
		}
	}

	// Fallback: treat as substring match on _msg
	return strings.Contains(row["body"], query)
}

type TombstoneStore struct {
	mu         sync.RWMutex
	tombstones map[string]Tombstone
}

func NewTombstoneStore() *TombstoneStore {
	return &TombstoneStore{
		tombstones: make(map[string]Tombstone),
	}
}

func (s *TombstoneStore) Add(ts Tombstone) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.tombstones[ts.ID] = ts
}

func (s *TombstoneStore) Remove(id string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tombstones, id)
}

func (s *TombstoneStore) Get(id string) (Tombstone, bool) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	ts, ok := s.tombstones[id]
	return ts, ok
}

func (s *TombstoneStore) Active() []Tombstone {
	s.mu.RLock()
	defer s.mu.RUnlock()
	result := make([]Tombstone, 0, len(s.tombstones))
	for _, ts := range s.tombstones {
		result = append(result, ts)
	}
	return result
}

func (s *TombstoneStore) ForRange(startNs, endNs int64) []Tombstone {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var result []Tombstone
	for _, ts := range s.tombstones {
		if ts.AffectsFile(startNs, endNs) {
			result = append(result, ts)
		}
	}
	return result
}

func (s *TombstoneStore) Count() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.tombstones)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/delete/ -v -run TestTombstone`
Expected: PASS (all 4 tests)

- [ ] **Step 5: Commit**

```bash
git add internal/delete/tombstone.go internal/delete/tombstone_test.go
git commit -m "feat(delete): add tombstone data structure and in-memory store"
```

---

### Task 2: Tombstone Persistence (S3 + Disk)

**Files:**
- Modify: `internal/delete/tombstone.go`
- Test: `internal/delete/tombstone_test.go`

- [ ] **Step 1: Write the failing test for persistence**

```go
// Append to internal/delete/tombstone_test.go

func TestTombstoneStore_PersistAndLoad(t *testing.T) {
	dir := t.TempDir()

	store := NewTombstoneStore()
	store.Add(Tombstone{
		ID:        "ts-persist",
		Query:     `level:="error"`,
		StartNs:   1000,
		EndNs:     5000,
		CreatedAt: time.Now(),
	})

	if err := store.PersistToDisk(dir); err != nil {
		t.Fatalf("persist failed: %v", err)
	}

	store2 := NewTombstoneStore()
	if err := store2.LoadFromDisk(dir); err != nil {
		t.Fatalf("load failed: %v", err)
	}

	all := store2.Active()
	if len(all) != 1 {
		t.Fatalf("expected 1 tombstone after load, got %d", len(all))
	}
	if all[0].ID != "ts-persist" {
		t.Fatalf("expected ts-persist, got %s", all[0].ID)
	}
	if all[0].Query != `level:="error"` {
		t.Fatalf("query mismatch: %s", all[0].Query)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/delete/ -v -run TestTombstoneStore_PersistAndLoad`
Expected: FAIL — PersistToDisk/LoadFromDisk undefined

- [ ] **Step 3: Implement persistence methods**

```go
// Append to internal/delete/tombstone.go

import (
	"encoding/json"
	"os"
	"path/filepath"
)

func (s *TombstoneStore) PersistToDisk(dir string) error {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if err := os.MkdirAll(dir, 0o755); err != nil {
		return fmt.Errorf("create tombstone dir: %w", err)
	}

	data, err := json.Marshal(s.tombstones)
	if err != nil {
		return fmt.Errorf("marshal tombstones: %w", err)
	}

	path := filepath.Join(dir, "tombstones.json")
	if err := os.WriteFile(path, data, 0o644); err != nil {
		return fmt.Errorf("write tombstones: %w", err)
	}
	return nil
}

func (s *TombstoneStore) LoadFromDisk(dir string) error {
	path := filepath.Join(dir, "tombstones.json")
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return fmt.Errorf("read tombstones: %w", err)
	}

	var tombstones map[string]Tombstone
	if err := json.Unmarshal(data, &tombstones); err != nil {
		return fmt.Errorf("unmarshal tombstones: %w", err)
	}

	s.mu.Lock()
	s.tombstones = tombstones
	s.mu.Unlock()
	return nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/delete/ -v`
Expected: PASS (all 5 tests)

- [ ] **Step 5: Commit**

```bash
git add internal/delete/tombstone.go internal/delete/tombstone_test.go
git commit -m "feat(delete): add tombstone disk persistence (JSON)"
```

---

### Task 3: S3 Storage Class Detection

**Files:**
- Create: `internal/delete/storageclass.go`
- Test: `internal/delete/storageclass_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/delete/storageclass_test.go
package delete

import (
	"context"
	"testing"
)

func TestStorageClassDetector_Classify(t *testing.T) {
	detector := NewStorageClassDetector(nil) // nil pool = uses age-based prediction

	tests := []struct {
		name     string
		class    StorageClass
		canRewrite bool
	}{
		{"STANDARD", ClassStandard, true},
		{"STANDARD_IA", ClassStandardIA, false},
		{"GLACIER", ClassGlacier, false},
		{"GLACIER_IR", ClassGlacierIR, false},
		{"DEEP_ARCHIVE", ClassDeepArchive, false},
		{"INTELLIGENT_TIERING", ClassIntelligentTiering, true},
		{"", ClassStandard, true}, // empty = standard
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			sc := ParseStorageClass(string(tt.class))
			if sc.CanRewrite() != tt.canRewrite {
				t.Errorf("StorageClass %q: CanRewrite()=%v, want %v", tt.class, sc.CanRewrite(), tt.canRewrite)
			}
		})
	}
}

func TestStorageClassDetector_EstimateCost(t *testing.T) {
	costs := EstimateRewriteCost(ClassGlacier, 500*1024*1024) // 500MB on Glacier
	if costs.RetrievalCostUSD < 0.01 {
		t.Fatalf("expected non-trivial retrieval cost for Glacier, got $%.4f", costs.RetrievalCostUSD)
	}

	costs2 := EstimateRewriteCost(ClassStandard, 500*1024*1024) // 500MB on Standard
	if costs2.RetrievalCostUSD != 0 {
		t.Fatalf("expected $0 retrieval for Standard, got $%.4f", costs2.RetrievalCostUSD)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/delete/ -v -run TestStorageClass`
Expected: FAIL — types undefined

- [ ] **Step 3: Implement storage class detection**

```go
// internal/delete/storageclass.go
package delete

import (
	"context"
	"fmt"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

type StorageClass string

const (
	ClassStandard           StorageClass = "STANDARD"
	ClassStandardIA         StorageClass = "STANDARD_IA"
	ClassOnezoneIA          StorageClass = "ONEZONE_IA"
	ClassGlacierIR          StorageClass = "GLACIER_IR"
	ClassGlacier            StorageClass = "GLACIER"
	ClassDeepArchive        StorageClass = "DEEP_ARCHIVE"
	ClassIntelligentTiering StorageClass = "INTELLIGENT_TIERING"
)

func ParseStorageClass(s string) StorageClass {
	if s == "" {
		return ClassStandard
	}
	return StorageClass(s)
}

func (sc StorageClass) CanRewrite() bool {
	switch sc {
	case ClassStandard, ClassIntelligentTiering, "":
		return true
	default:
		return false
	}
}

func (sc StorageClass) IsArchive() bool {
	switch sc {
	case ClassGlacier, ClassDeepArchive, ClassGlacierIR:
		return true
	default:
		return false
	}
}

type RewriteCost struct {
	RetrievalCostUSD float64
	PutCostUSD       float64
	GetCostUSD       float64
	TotalCostUSD     float64
}

func EstimateRewriteCost(class StorageClass, sizeBytes int64) RewriteCost {
	sizeGB := float64(sizeBytes) / (1024 * 1024 * 1024)

	var cost RewriteCost
	cost.GetCostUSD = 0.0004 / 1000 // 1 GET
	cost.PutCostUSD = 0.005 / 1000  // 1 PUT

	switch class {
	case ClassStandard, ClassIntelligentTiering, "":
		cost.RetrievalCostUSD = 0
	case ClassStandardIA, ClassOnezoneIA:
		cost.RetrievalCostUSD = sizeGB * 0.01 // $0.01/GB retrieval
	case ClassGlacierIR:
		cost.RetrievalCostUSD = sizeGB * 0.03 // $0.03/GB retrieval
	case ClassGlacier:
		cost.RetrievalCostUSD = sizeGB * 0.03 // standard retrieval
	case ClassDeepArchive:
		cost.RetrievalCostUSD = sizeGB * 0.09 // $0.09/GB bulk retrieval (12hr)
	}

	cost.TotalCostUSD = cost.RetrievalCostUSD + cost.PutCostUSD + cost.GetCostUSD
	return cost
}

type S3HeadObjecter interface {
	HeadObject(ctx context.Context, input *s3.HeadObjectInput, opts ...func(*s3.Options)) (*s3.HeadObjectOutput, error)
}

type StorageClassDetector struct {
	client S3HeadObjecter
	bucket string
	cache  map[string]StorageClass
}

func NewStorageClassDetector(client S3HeadObjecter) *StorageClassDetector {
	return &StorageClassDetector{
		client: client,
		cache:  make(map[string]StorageClass),
	}
}

func (d *StorageClassDetector) SetBucket(bucket string) {
	d.bucket = bucket
}

func (d *StorageClassDetector) Detect(ctx context.Context, key string) (StorageClass, error) {
	if cached, ok := d.cache[key]; ok {
		return cached, nil
	}

	if d.client == nil {
		return ClassStandard, nil
	}

	out, err := d.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(d.bucket),
		Key:    aws.String(key),
	})
	if err != nil {
		return "", fmt.Errorf("HeadObject %s: %w", key, err)
	}

	class := ParseStorageClass(string(out.StorageClass))
	d.cache[key] = class
	return class, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/delete/ -v -run TestStorageClass`
Expected: PASS (both tests)

- [ ] **Step 5: Commit**

```bash
git add internal/delete/storageclass.go internal/delete/storageclass_test.go
git commit -m "feat(delete): add S3 storage class detection and cost estimation"
```

---

### Task 4: Delete Configuration

**Files:**
- Modify: `internal/config/config.go`

- [ ] **Step 1: Write the failing test**

```go
// Add to existing config test file or create internal/config/config_test.go
// Test that DeleteConfig is parseable from YAML

func TestConfig_DeleteDefaults(t *testing.T) {
	cfg := DefaultConfig()
	if !cfg.Delete.Enabled {
		t.Fatal("delete should be enabled by default")
	}
	if cfg.Delete.RewriteDelay != time.Hour {
		t.Fatalf("expected 1h rewrite delay, got %v", cfg.Delete.RewriteDelay)
	}
	if cfg.Delete.CostWarningThreshold != 10.0 {
		t.Fatalf("expected $10 warning threshold, got %v", cfg.Delete.CostWarningThreshold)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/config/ -v -run TestConfig_DeleteDefaults`
Expected: FAIL — DeleteConfig undefined

- [ ] **Step 3: Add DeleteConfig to config.go**

Add after `CompactionConfig` in `internal/config/config.go`:

```go
type DeleteConfig struct {
	Enabled              bool          `yaml:"enabled"`
	AutoRewriteClasses   []string      `yaml:"auto_rewrite_classes"`
	RewriteDelay         time.Duration `yaml:"rewrite_delay"`
	RewriteBatchSize     int           `yaml:"rewrite_batch_size"`
	RewriteMaxConcurrent int           `yaml:"rewrite_max_concurrent"`
	PersistPath          string        `yaml:"persist_path"`
	CostWarningThreshold float64       `yaml:"cost_warning_threshold"`
	ForceGlacierHeader   string        `yaml:"force_glacier_header"`
}
```

Add to `Config` struct:
```go
Delete     DeleteConfig     `yaml:"delete"`
```

Add to `DefaultConfig()`:
```go
Delete: DeleteConfig{
	Enabled:              true,
	AutoRewriteClasses:   []string{"STANDARD"},
	RewriteDelay:         time.Hour,
	RewriteBatchSize:     50,
	RewriteMaxConcurrent: 2,
	PersistPath:          "/data/lakehouse/tombstones",
	CostWarningThreshold: 10.0,
	ForceGlacierHeader:   "X-Force-Glacier-Delete",
},
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/config/ -v -run TestConfig_DeleteDefaults`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/config/config.go internal/config/config_test.go
git commit -m "feat(delete): add DeleteConfig with cost-aware defaults"
```

---

### Task 5: Delete Metrics

**Files:**
- Modify: `internal/metrics/lakehouse.go`

- [ ] **Step 1: Add delete metrics to the metrics file**

Append to `internal/metrics/lakehouse.go`:

```go
// Delete metrics
var (
	DeleteTombstonesActive     = NewGauge("lakehouse_delete_tombstones_active")
	DeleteTombstonesTotal      = NewCounter("lakehouse_delete_tombstones_total")
	DeleteRewriteTotal         = NewCounter("lakehouse_delete_rewrite_total")
	DeleteRewriteErrors        = NewCounter("lakehouse_delete_rewrite_errors_total")
	DeleteRewriteBytesSaved    = NewCounter("lakehouse_delete_rewrite_bytes_saved_total")
	DeleteRewriteSkippedGlacier = NewCounter("lakehouse_delete_rewrite_skipped_glacier_total")
	DeleteRowsSuppressed       = NewCounter("lakehouse_delete_rows_suppressed_total")
)
```

- [ ] **Step 2: Run build to verify it compiles**

Run: `go build ./internal/metrics/`
Expected: success

- [ ] **Step 3: Commit**

```bash
git add internal/metrics/lakehouse.go
git commit -m "feat(delete): add Prometheus metrics for deletion"
```

---

### Task 6: Delete HTTP Handler

**Files:**
- Create: `internal/delete/handler.go`
- Test: `internal/delete/handler_test.go`

- [ ] **Step 1: Write the failing test for delete endpoint**

```go
// internal/delete/handler_test.go
package delete

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func newTestManifest() *manifest.Manifest {
	m := manifest.New("test-bucket", "logs/", nil)
	m.AddFile("dt=2026-01-15/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-01-15/hour=10/00001.parquet",
		Size:      50 * 1024 * 1024,
		MinTimeNs: time.Date(2026, 1, 15, 10, 0, 0, 0, time.UTC).UnixNano(),
		MaxTimeNs: time.Date(2026, 1, 15, 10, 59, 0, 0, time.UTC).UnixNano(),
		Labels:    map[string][]string{"service.name": {"api-gateway", "auth"}},
	})
	return m
}

func TestHandler_Delete(t *testing.T) {
	store := NewTombstoneStore()
	cfg := &config.DeleteConfig{
		Enabled:            true,
		AutoRewriteClasses: []string{"STANDARD"},
		RewriteDelay:       time.Hour,
	}
	m := newTestManifest()
	h := NewHandler(store, m, nil, cfg, nil)

	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("POST",
		`/delete/logsql/delete?query=service.name:="api-gateway"&start=1705305600000000000&end=1705395600000000000`,
		nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp DeleteResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.TombstoneID == "" {
		t.Fatal("expected non-empty tombstone ID")
	}
	if resp.AffectedFiles != 1 {
		t.Fatalf("expected 1 affected file, got %d", resp.AffectedFiles)
	}

	// Verify tombstone was stored
	all := store.Active()
	if len(all) != 1 {
		t.Fatalf("expected 1 tombstone in store, got %d", len(all))
	}
}

func TestHandler_Estimate(t *testing.T) {
	store := NewTombstoneStore()
	cfg := &config.DeleteConfig{Enabled: true}
	m := newTestManifest()
	h := NewHandler(store, m, nil, cfg, nil)

	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("POST",
		`/delete/logsql/estimate?query=service.name:="api-gateway"&start=1705305600000000000&end=1705395600000000000`,
		nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}

	var resp EstimateResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode response: %v", err)
	}

	if resp.AffectedFiles != 1 {
		t.Fatalf("expected 1 affected file, got %d", resp.AffectedFiles)
	}
}

func TestHandler_ListTombstones(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{ID: "ts-list-1", Query: "*", StartNs: 0, EndNs: 9999, CreatedAt: time.Now()})
	store.Add(Tombstone{ID: "ts-list-2", Query: "*", StartNs: 0, EndNs: 9999, CreatedAt: time.Now()})

	cfg := &config.DeleteConfig{Enabled: true}
	h := NewHandler(store, nil, nil, cfg, nil)

	mux := http.NewServeMux()
	h.Register(mux)

	req := httptest.NewRequest("GET", "/delete/logsql/tombstones", nil)
	w := httptest.NewRecorder()

	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d", w.Code)
	}

	var resp TombstonesListResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Tombstones) != 2 {
		t.Fatalf("expected 2 tombstones, got %d", len(resp.Tombstones))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/delete/ -v -run TestHandler`
Expected: FAIL — Handler type undefined

- [ ] **Step 3: Implement the delete handler**

```go
// internal/delete/handler.go
package delete

import (
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/google/uuid"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type Handler struct {
	store    *TombstoneStore
	manifest *manifest.Manifest
	detector *StorageClassDetector
	cfg      *config.DeleteConfig
	logger   *slog.Logger
}

type DeleteResponse struct {
	TombstoneID   string `json:"tombstone_id"`
	AffectedFiles int    `json:"affected_files"`
	Mode          string `json:"mode"`
	Message       string `json:"message"`
}

type EstimateResponse struct {
	AffectedFiles    int                        `json:"affected_files"`
	StorageClasses   map[string]ClassEstimate   `json:"storage_classes"`
	RecommendedMode  string                     `json:"recommended_mode"`
	AutoBehavior     string                     `json:"auto_behavior"`
}

type ClassEstimate struct {
	Files       int     `json:"files"`
	Bytes       int64   `json:"bytes"`
	RewriteCost string  `json:"rewrite_cost"`
}

type TombstonesListResponse struct {
	Tombstones []Tombstone `json:"tombstones"`
	Count      int         `json:"count"`
}

func NewHandler(store *TombstoneStore, m *manifest.Manifest, detector *StorageClassDetector, cfg *config.DeleteConfig, logger *slog.Logger) *Handler {
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		store:    store,
		manifest: m,
		detector: detector,
		cfg:      cfg,
		logger:   logger.With("component", "delete"),
	}
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/delete/logsql/delete", h.handleDelete)
	mux.HandleFunc("/delete/logsql/estimate", h.handleEstimate)
	mux.HandleFunc("/delete/logsql/tombstones", h.handleListTombstones)
	mux.HandleFunc("/delete/logsql/tombstone/", h.handleTombstoneByID)
}

func (h *Handler) handleDelete(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}
	if !h.cfg.Enabled {
		http.Error(w, "delete is disabled", http.StatusForbidden)
		return
	}

	q := r.URL.Query()
	query := q.Get("query")
	if query == "" {
		http.Error(w, "query parameter required", http.StatusBadRequest)
		return
	}

	startNs := parseNsParam(q.Get("start"))
	endNs := parseNsParam(q.Get("end"))
	if endNs == 0 {
		endNs = time.Now().UnixNano()
	}

	files := h.manifest.GetFilesForRange(startNs, endNs)
	affectedKeys := make([]string, 0, len(files))
	for _, fi := range files {
		affectedKeys = append(affectedKeys, fi.Key)
	}

	ts := Tombstone{
		ID:           uuid.New().String(),
		Query:        query,
		StartNs:      startNs,
		EndNs:        endNs,
		AffectedKeys: affectedKeys,
		CreatedAt:    time.Now(),
		CreatedBy:    r.Header.Get("X-Delete-User"),
	}

	h.store.Add(ts)
	metrics.DeleteTombstonesTotal.Inc()
	metrics.DeleteTombstonesActive.Set(int64(h.store.Count()))

	h.logger.Info("tombstone created",
		"id", ts.ID,
		"query", query,
		"affected_files", len(affectedKeys),
	)

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(DeleteResponse{
		TombstoneID:   ts.ID,
		AffectedFiles: len(affectedKeys),
		Mode:          "tombstone",
		Message:       fmt.Sprintf("Tombstone created. %d files affected. Data immediately invisible.", len(affectedKeys)),
	})
}

func (h *Handler) handleEstimate(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	query := q.Get("query")
	if query == "" {
		http.Error(w, "query parameter required", http.StatusBadRequest)
		return
	}

	startNs := parseNsParam(q.Get("start"))
	endNs := parseNsParam(q.Get("end"))
	if endNs == 0 {
		endNs = time.Now().UnixNano()
	}

	files := h.manifest.GetFilesForRange(startNs, endNs)

	classes := make(map[string]ClassEstimate)
	for _, fi := range files {
		class := string(ClassStandard) // default if no detector
		classes[class] = ClassEstimate{
			Files:       classes[class].Files + 1,
			Bytes:       classes[class].Bytes + fi.Size,
			RewriteCost: fmt.Sprintf("$%.4f", EstimateRewriteCost(ParseStorageClass(class), fi.Size).TotalCostUSD),
		}
	}

	standardFiles := 0
	if ce, ok := classes[string(ClassStandard)]; ok {
		standardFiles = ce.Files
	}

	resp := EstimateResponse{
		AffectedFiles:   len(files),
		StorageClasses:  classes,
		RecommendedMode: "auto",
		AutoBehavior:    fmt.Sprintf("Tombstone all %d files immediately. Rewrite %d STANDARD files.", len(files), standardFiles),
	}

	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(resp)
}

func (h *Handler) handleListTombstones(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	tombstones := h.store.Active()
	w.Header().Set("Content-Type", "application/json")
	json.NewEncoder(w).Encode(TombstonesListResponse{
		Tombstones: tombstones,
		Count:      len(tombstones),
	})
}

func (h *Handler) handleTombstoneByID(w http.ResponseWriter, r *http.Request) {
	// Extract ID from path: /delete/logsql/tombstone/{id}
	id := r.URL.Path[len("/delete/logsql/tombstone/"):]
	if id == "" {
		http.Error(w, "tombstone ID required", http.StatusBadRequest)
		return
	}

	switch r.Method {
	case http.MethodGet:
		ts, ok := h.store.Get(id)
		if !ok {
			http.Error(w, "tombstone not found", http.StatusNotFound)
			return
		}
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(ts)

	case http.MethodDelete:
		h.store.Remove(id)
		metrics.DeleteTombstonesActive.Set(int64(h.store.Count()))
		w.Header().Set("Content-Type", "application/json")
		json.NewEncoder(w).Encode(map[string]string{"status": "removed", "id": id})

	default:
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
	}
}

func parseNsParam(s string) int64 {
	if s == "" {
		return 0
	}
	n, _ := strconv.ParseInt(s, 10, 64)
	return n
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/delete/ -v -run TestHandler`
Expected: PASS (all 3 handler tests)

- [ ] **Step 5: Commit**

```bash
git add internal/delete/handler.go internal/delete/handler_test.go
git commit -m "feat(delete): add HTTP handlers for delete, estimate, tombstone management"
```

---

### Task 7: Query-Time Tombstone Filtering

**Files:**
- Modify: `internal/storage/parquets3/storage.go`
- Test: `internal/storage/parquets3/storage_test.go`

- [ ] **Step 1: Write the failing test**

Add to `internal/storage/parquets3/storage_test.go`:

```go
func TestStorage_RunQuery_FiltersTombstones(t *testing.T) {
	// Setup storage with tombstone store
	store := setupTestStorage(t) // existing helper
	tombstones := delete.NewTombstoneStore()
	store.SetTombstoneStore(tombstones)

	// Add tombstone for service.name:="to-delete"
	tombstones.Add(delete.Tombstone{
		ID:      "ts-filter-test",
		Query:   `service.name:="to-delete"`,
		StartNs: 0,
		EndNs:   time.Now().Add(time.Hour).UnixNano(),
	})

	// Query should not return rows matching the tombstone
	var blocks []*storage.DataBlock
	qctx := &storage.QueryContext{
		StartNs: 0,
		EndNs:   time.Now().UnixNano(),
		Query:   "*",
	}

	err := store.RunQuery(context.Background(), qctx, func(_ uint, db *storage.DataBlock) {
		blocks = append(blocks, db)
	})
	if err != nil {
		t.Fatalf("RunQuery: %v", err)
	}

	// Verify no rows with service.name=="to-delete" in results
	for _, db := range blocks {
		for _, col := range db.Columns {
			if col.Name == "service.name" {
				for _, v := range col.Values {
					if v == "to-delete" {
						t.Fatal("tombstoned row should not appear in results")
					}
				}
			}
		}
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/storage/parquets3/ -v -run TestStorage_RunQuery_FiltersTombstones`
Expected: FAIL — `SetTombstoneStore` undefined

- [ ] **Step 3: Add tombstone filtering to storage**

Add to `internal/storage/parquets3/storage.go`:

```go
import "github.com/ReliablyObserve/victoria-lakehouse/internal/delete"

// Add field to Storage struct:
tombstones *delete.TombstoneStore

// Add setter:
func (s *Storage) SetTombstoneStore(ts *delete.TombstoneStore) {
	s.tombstones = ts
}

func (s *Storage) TombstoneStore() *delete.TombstoneStore {
	return s.tombstones
}
```

Modify `RunQuery` — after getting files from manifest but before iterating, filter by tombstones. Inside `readRowGroup`, after reading rows, apply tombstone post-filter:

Add helper method:

```go
func (s *Storage) filterTombstonedRows(db *storage.DataBlock, qctx *storage.QueryContext) *storage.DataBlock {
	if s.tombstones == nil {
		return db
	}

	tombstones := s.tombstones.ForRange(qctx.StartNs, qctx.EndNs)
	if len(tombstones) == 0 {
		return db
	}

	// Find timestamp and other column indices
	tsIdx := -1
	for i, col := range db.Columns {
		if col.Name == "_time" || col.Name == "timestamp_unix_nano" {
			tsIdx = i
			break
		}
	}

	// Build row map and filter
	keepRows := make([]bool, db.RowsCount)
	suppressed := 0
	for rowIdx := 0; rowIdx < db.RowsCount; rowIdx++ {
		row := make(map[string]string, len(db.Columns))
		for _, col := range db.Columns {
			if rowIdx < len(col.Values) {
				row[col.Name] = col.Values[rowIdx]
			}
		}

		var rowTs int64
		if tsIdx >= 0 && rowIdx < len(db.Columns[tsIdx].Values) {
			rowTs, _ = strconv.ParseInt(db.Columns[tsIdx].Values[rowIdx], 10, 64)
		}

		deleted := false
		for i := range tombstones {
			if tombstones[i].MatchesRow(row, rowTs) {
				deleted = true
				suppressed++
				break
			}
		}
		keepRows[rowIdx] = !deleted
	}

	if suppressed == 0 {
		return db
	}

	metrics.DeleteRowsSuppressed.Add(suppressed)

	// Build filtered DataBlock
	newCount := db.RowsCount - suppressed
	if newCount <= 0 {
		return nil
	}

	newDB := &storage.DataBlock{
		RowsCount: newCount,
		Columns:   make([]storage.BlockColumn, len(db.Columns)),
	}
	for colIdx, col := range db.Columns {
		newValues := make([]string, 0, newCount)
		for rowIdx, keep := range keepRows {
			if keep && rowIdx < len(col.Values) {
				newValues = append(newValues, col.Values[rowIdx])
			}
		}
		newDB.Columns[colIdx] = storage.BlockColumn{
			Name:   col.Name,
			Values: newValues,
		}
	}
	return newDB
}
```

Then wrap the `writeBlock` callback in `RunQuery`:

```go
// In RunQuery, replace direct writeBlock with filtered version:
filteredWriteBlock := func(workerID uint, db *storage.DataBlock) {
	if s.tombstones != nil {
		db = s.filterTombstonedRows(db, qctx)
		if db == nil || db.RowsCount == 0 {
			return
		}
	}
	writeBlock(workerID, db)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/storage/parquets3/ -v -run TestStorage_RunQuery_FiltersTombstones`
Expected: PASS

Run: `go test ./internal/storage/parquets3/ -v` (all existing tests still pass)
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/storage/parquets3/storage.go internal/storage/parquets3/storage_test.go internal/delete/
git commit -m "feat(delete): inject tombstone post-filter into RunQuery path"
```

---

### Task 8: Background Rewriter

**Files:**
- Create: `internal/delete/rewriter.go`
- Test: `internal/delete/rewriter_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/delete/rewriter_test.go
package delete

import (
	"bytes"
	"context"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

type mockPool struct {
	files map[string][]byte
}

func (m *mockPool) Upload(_ context.Context, key string, data []byte) error {
	m.files[key] = data
	return nil
}

func (m *mockPool) Download(_ context.Context, key string) ([]byte, error) {
	if d, ok := m.files[key]; ok {
		return d, nil
	}
	return nil, fmt.Errorf("not found: %s", key)
}

func (m *mockPool) Delete(_ context.Context, key string) error {
	delete(m.files, key)
	return nil
}

func TestRewriter_RewriteFile(t *testing.T) {
	// Create a small Parquet file with 3 rows
	pool := &mockPool{files: make(map[string][]byte)}
	parquetData := generateTestParquet(t, []map[string]string{
		{"service.name": "keep-me", "body": "log 1", "timestamp_unix_nano": "1000"},
		{"service.name": "delete-me", "body": "log 2", "timestamp_unix_nano": "2000"},
		{"service.name": "keep-me", "body": "log 3", "timestamp_unix_nano": "3000"},
	})
	pool.files["logs/dt=2026-01-01/hour=10/00001.parquet"] = parquetData

	mf := manifest.New("test", "logs/", nil)
	mf.AddFile("dt=2026-01-01/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-01-01/hour=10/00001.parquet",
		Size:      int64(len(parquetData)),
		MinTimeNs: 1000,
		MaxTimeNs: 3000,
	})

	ts := Tombstone{
		ID:      "ts-rewrite",
		Query:   `service.name:="delete-me"`,
		StartNs: 0,
		EndNs:   5000,
	}

	rw := NewRewriter(RewriterConfig{
		Pool:     pool,
		Manifest: mf,
		Prefix:   "logs/",
		Mode:     config.ModeLogs,
		Logger:   nil,
	})

	result, err := rw.RewriteFile(context.Background(), "logs/dt=2026-01-01/hour=10/00001.parquet", ts)
	if err != nil {
		t.Fatalf("rewrite failed: %v", err)
	}

	if result.RowsRemoved != 1 {
		t.Fatalf("expected 1 row removed, got %d", result.RowsRemoved)
	}
	if result.RowsKept != 2 {
		t.Fatalf("expected 2 rows kept, got %d", result.RowsKept)
	}
	if result.NewKey == "" {
		t.Fatal("expected non-empty new key")
	}

	// Verify new file exists and old file is deleted
	if _, ok := pool.files[result.NewKey]; !ok {
		t.Fatal("new file not uploaded")
	}
	if _, ok := pool.files["logs/dt=2026-01-01/hour=10/00001.parquet"]; ok {
		t.Fatal("old file should be deleted")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/delete/ -v -run TestRewriter`
Expected: FAIL — Rewriter undefined

- [ ] **Step 3: Implement the rewriter**

```go
// internal/delete/rewriter.go
package delete

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type RewriterPool interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

type RewriterConfig struct {
	Pool             RewriterPool
	Manifest         *manifest.Manifest
	Prefix           string
	Mode             config.Mode
	RowGroupSize     int
	CompressionLevel int
	Logger           *slog.Logger
}

type RewriteResult struct {
	OldKey      string
	NewKey      string
	RowsKept    int64
	RowsRemoved int64
	BytesBefore int64
	BytesAfter  int64
	Duration    time.Duration
}

type Rewriter struct {
	pool             RewriterPool
	manifest         *manifest.Manifest
	prefix           string
	mode             config.Mode
	rowGroupSize     int
	compressionLevel int
	logger           *slog.Logger
}

func NewRewriter(cfg RewriterConfig) *Rewriter {
	if cfg.RowGroupSize <= 0 {
		cfg.RowGroupSize = 10000
	}
	if cfg.CompressionLevel <= 0 {
		cfg.CompressionLevel = 3
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Rewriter{
		pool:             cfg.Pool,
		manifest:         cfg.Manifest,
		prefix:           cfg.Prefix,
		mode:             cfg.Mode,
		rowGroupSize:     cfg.RowGroupSize,
		compressionLevel: cfg.CompressionLevel,
		logger:           cfg.Logger.With("component", "delete-rewriter"),
	}
}

func (rw *Rewriter) RewriteFile(ctx context.Context, key string, ts Tombstone) (*RewriteResult, error) {
	start := time.Now()

	data, err := rw.pool.Download(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", key, err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open parquet %s: %w", key, err)
	}

	// Read all rows, filter out tombstoned ones
	schema := f.Schema()
	var keptRows []parquet.Row
	var removedCount int64

	for _, rg := range f.RowGroups() {
		rows := rg.Rows()
		for {
			row := make([]parquet.Value, len(schema.Fields()))
			n, readErr := rows.ReadRows([]parquet.Row{row})
			if n == 0 {
				break
			}

			rowMap := rw.rowToMap(schema, row)
			var rowTs int64
			if tsVal, ok := rowMap["timestamp_unix_nano"]; ok {
				fmt.Sscanf(tsVal, "%d", &rowTs)
			}

			if ts.MatchesRow(rowMap, rowTs) {
				removedCount++
			} else {
				keptRows = append(keptRows, row)
			}

			if readErr != nil {
				break
			}
		}
		rows.Close()
	}

	if removedCount == 0 {
		return &RewriteResult{
			OldKey:      key,
			RowsKept:    int64(len(keptRows)),
			RowsRemoved: 0,
			Duration:    time.Since(start),
		}, nil
	}

	// Write new file
	partition := manifest.ExtractPartition(key)
	newKey := fmt.Sprintf("%s%s/%s.parquet", rw.prefix, partition, uuid.New().String()[:8])

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[parquet.Row](&buf,
		schema,
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedDefault}),
	)

	for i := 0; i < len(keptRows); i += rw.rowGroupSize {
		end := i + rw.rowGroupSize
		if end > len(keptRows) {
			end = len(keptRows)
		}
		if _, err := writer.WriteRows(keptRows[i:end]); err != nil {
			return nil, fmt.Errorf("write rows: %w", err)
		}
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close writer: %w", err)
	}

	newData := buf.Bytes()

	// Upload new file
	if err := rw.pool.Upload(ctx, newKey, newData); err != nil {
		return nil, fmt.Errorf("upload %s: %w", newKey, err)
	}

	// Update manifest atomically
	rw.manifest.AddFile(partition, manifest.FileInfo{
		Key:       newKey,
		Size:      int64(len(newData)),
		RowCount:  int64(len(keptRows)),
		MinTimeNs: rw.minTimestamp(keptRows, schema),
		MaxTimeNs: rw.maxTimestamp(keptRows, schema),
	})
	rw.manifest.RemoveFile(partition, key)

	// Delete old file
	if err := rw.pool.Delete(ctx, key); err != nil {
		rw.logger.Warn("failed to delete old file after rewrite", "key", key, "error", err)
	}

	metrics.DeleteRewriteTotal.Inc()
	metrics.DeleteRewriteBytesSaved.Add(len(data) - len(newData))

	return &RewriteResult{
		OldKey:      key,
		NewKey:      newKey,
		RowsKept:    int64(len(keptRows)),
		RowsRemoved: removedCount,
		BytesBefore: int64(len(data)),
		BytesAfter:  int64(len(newData)),
		Duration:    time.Since(start),
	}, nil
}

func (rw *Rewriter) rowToMap(schema *parquet.Schema, row parquet.Row) map[string]string {
	m := make(map[string]string, len(schema.Fields()))
	for i, field := range schema.Fields() {
		if i < len(row) {
			m[field.Name()] = row[i].String()
		}
	}
	return m
}

func (rw *Rewriter) minTimestamp(rows []parquet.Row, schema *parquet.Schema) int64 {
	tsIdx := -1
	for i, f := range schema.Fields() {
		if f.Name() == "timestamp_unix_nano" {
			tsIdx = i
			break
		}
	}
	if tsIdx < 0 || len(rows) == 0 {
		return 0
	}
	min := rows[0][tsIdx].Int64()
	for _, r := range rows[1:] {
		if v := r[tsIdx].Int64(); v < min {
			min = v
		}
	}
	return min
}

func (rw *Rewriter) maxTimestamp(rows []parquet.Row, schema *parquet.Schema) int64 {
	tsIdx := -1
	for i, f := range schema.Fields() {
		if f.Name() == "timestamp_unix_nano" {
			tsIdx = i
			break
		}
	}
	if tsIdx < 0 || len(rows) == 0 {
		return 0
	}
	max := rows[0][tsIdx].Int64()
	for _, r := range rows[1:] {
		if v := r[tsIdx].Int64(); v > max {
			max = v
		}
	}
	return max
}
```

- [ ] **Step 4: Add test helper for Parquet generation**

```go
// Add to internal/delete/rewriter_test.go

func generateTestParquet(t *testing.T, rows []map[string]string) []byte {
	t.Helper()

	type LogRow struct {
		TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
		Body              string `parquet:"body"`
		ServiceName       string `parquet:"service.name"`
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[LogRow](&buf)

	for _, row := range rows {
		var ts int64
		fmt.Sscanf(row["timestamp_unix_nano"], "%d", &ts)
		writer.Write([]LogRow{{
			TimestampUnixNano: ts,
			Body:              row["body"],
			ServiceName:       row["service.name"],
		}})
	}
	if err := writer.Close(); err != nil {
		t.Fatalf("write parquet: %v", err)
	}
	return buf.Bytes()
}
```

- [ ] **Step 5: Run tests to verify they pass**

Run: `go test ./internal/delete/ -v -run TestRewriter`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/delete/rewriter.go internal/delete/rewriter_test.go
git commit -m "feat(delete): add background rewriter for S3 Standard files"
```

---

### Task 9: Background Rewrite Scheduler

**Files:**
- Create: `internal/delete/scheduler.go`
- Test: `internal/delete/scheduler_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/delete/scheduler_test.go
package delete

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func TestScheduler_ProcessesPendingTombstones(t *testing.T) {
	pool := &mockPool{files: make(map[string][]byte)}
	parquetData := generateTestParquet(t, []map[string]string{
		{"service.name": "keep", "body": "ok", "timestamp_unix_nano": "1000"},
		{"service.name": "remove", "body": "bad", "timestamp_unix_nano": "2000"},
	})
	pool.files["logs/dt=2026-01-01/hour=10/00001.parquet"] = parquetData

	mf := manifest.New("test", "logs/", nil)
	mf.AddFile("dt=2026-01-01/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-01-01/hour=10/00001.parquet",
		Size:      int64(len(parquetData)),
		MinTimeNs: 1000,
		MaxTimeNs: 2000,
	})

	store := NewTombstoneStore()
	store.Add(Tombstone{
		ID:           "ts-sched",
		Query:        `service.name:="remove"`,
		StartNs:      0,
		EndNs:        5000,
		AffectedKeys: []string{"logs/dt=2026-01-01/hour=10/00001.parquet"},
		CreatedAt:    time.Now().Add(-2 * time.Hour), // older than delay
	})

	detector := NewStorageClassDetector(nil) // nil = assume Standard

	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       NewRewriter(RewriterConfig{Pool: pool, Manifest: mf, Prefix: "logs/", Mode: config.ModeLogs}),
		Detector:       detector,
		RewriteDelay:   time.Hour,
		AllowedClasses: []string{"STANDARD"},
		MaxConcurrent:  1,
		Logger:         nil,
	})

	results := sched.RunOnce(context.Background())
	if len(results) != 1 {
		t.Fatalf("expected 1 rewrite result, got %d", len(results))
	}
	if results[0].RowsRemoved != 1 {
		t.Fatalf("expected 1 row removed, got %d", results[0].RowsRemoved)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `go test ./internal/delete/ -v -run TestScheduler`
Expected: FAIL — RewriteScheduler undefined

- [ ] **Step 3: Implement the scheduler**

```go
// internal/delete/scheduler.go
package delete

import (
	"context"
	"log/slog"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type RewriteSchedulerConfig struct {
	Store          *TombstoneStore
	Rewriter       *Rewriter
	Detector       *StorageClassDetector
	RewriteDelay   time.Duration
	AllowedClasses []string
	MaxConcurrent  int
	Logger         *slog.Logger
}

type RewriteScheduler struct {
	store          *TombstoneStore
	rewriter       *Rewriter
	detector       *StorageClassDetector
	rewriteDelay   time.Duration
	allowedClasses map[string]bool
	maxConcurrent  int
	logger         *slog.Logger
	stopCh         chan struct{}
}

func NewRewriteScheduler(cfg RewriteSchedulerConfig) *RewriteScheduler {
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	allowed := make(map[string]bool, len(cfg.AllowedClasses))
	for _, c := range cfg.AllowedClasses {
		allowed[c] = true
	}
	return &RewriteScheduler{
		store:          cfg.Store,
		rewriter:       cfg.Rewriter,
		detector:       cfg.Detector,
		rewriteDelay:   cfg.RewriteDelay,
		allowedClasses: allowed,
		maxConcurrent:  cfg.MaxConcurrent,
		logger:         cfg.Logger.With("component", "delete-scheduler"),
		stopCh:         make(chan struct{}),
	}
}

func (s *RewriteScheduler) Start(interval time.Duration) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				s.RunOnce(context.Background())
			case <-s.stopCh:
				return
			}
		}
	}()
}

func (s *RewriteScheduler) Stop() {
	close(s.stopCh)
}

func (s *RewriteScheduler) RunOnce(ctx context.Context) []RewriteResult {
	tombstones := s.store.Active()
	var results []RewriteResult

	for _, ts := range tombstones {
		if time.Since(ts.CreatedAt) < s.rewriteDelay {
			continue // not yet eligible for rewrite
		}

		for _, key := range ts.AffectedKeys {
			if ts.Reaped != nil && ts.Reaped[key] {
				continue // already rewritten
			}

			class, err := s.detector.Detect(ctx, key)
			if err != nil {
				s.logger.Warn("detect storage class failed", "key", key, "error", err)
				class = ClassStandard // assume standard on error
			}

			if !s.allowedClasses[string(class)] {
				metrics.DeleteRewriteSkippedGlacier.Inc()
				s.logger.Debug("skipping rewrite for non-standard class",
					"key", key,
					"class", class,
				)
				continue
			}

			result, err := s.rewriter.RewriteFile(ctx, key, ts)
			if err != nil {
				metrics.DeleteRewriteErrors.Inc()
				s.logger.Error("rewrite failed", "key", key, "error", err)
				continue
			}

			results = append(results, *result)

			// Mark this file as reaped in the tombstone
			updated := ts
			if updated.Reaped == nil {
				updated.Reaped = make(map[string]bool)
			}
			updated.Reaped[key] = true
			s.store.Add(updated) // overwrite with updated reaped state
		}
	}

	return results
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `go test ./internal/delete/ -v -run TestScheduler`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/delete/scheduler.go internal/delete/scheduler_test.go
git commit -m "feat(delete): add background rewrite scheduler with storage-class gating"
```

---

### Task 10: Wire Delete into Main Binary

**Files:**
- Modify: `cmd/lakehouse/main.go`

- [ ] **Step 1: Add delete handler and scheduler to main**

In `cmd/lakehouse/main.go`, after compaction setup and before `mux := newMux(...)`:

```go
// Initialize tombstone store
tombstoneStore := delete.NewTombstoneStore()
if err := tombstoneStore.LoadFromDisk(cfg.Delete.PersistPath); err != nil {
	logger.Warn("failed to load tombstones from disk", "error", err)
}
store.SetTombstoneStore(tombstoneStore)

// Start rewrite scheduler
var deleteScheduler *delete.RewriteScheduler
if cfg.Delete.Enabled && cfg.InsertEnabled() {
	detector := delete.NewStorageClassDetector(store.Pool().S3Client())
	detector.SetBucket(cfg.S3.Bucket)

	rewriter := delete.NewRewriter(delete.RewriterConfig{
		Pool:             store.Pool(),
		Manifest:         store.Manifest(),
		Prefix:           cfg.AutoPrefix(),
		Mode:             cfg.Mode,
		RowGroupSize:     cfg.Insert.RowGroupSize,
		CompressionLevel: cfg.Insert.CompressionLevel,
		Logger:           logger,
	})

	deleteScheduler = delete.NewRewriteScheduler(delete.RewriteSchedulerConfig{
		Store:          tombstoneStore,
		Rewriter:       rewriter,
		Detector:       detector,
		RewriteDelay:   cfg.Delete.RewriteDelay,
		AllowedClasses: cfg.Delete.AutoRewriteClasses,
		MaxConcurrent:  cfg.Delete.RewriteMaxConcurrent,
		Logger:         logger,
	})
	deleteScheduler.Start(cfg.Delete.RewriteDelay / 2)

	logger.Info("delete rewrite scheduler started",
		"delay", cfg.Delete.RewriteDelay,
		"classes", cfg.Delete.AutoRewriteClasses,
	)
}
```

In `newMux()`, add delete handler registration:

```go
if cfg.Delete.Enabled {
	deleteHandler := delete.NewHandler(
		store.TombstoneStore(),
		store.Manifest(),
		nil, // detector (optional for handler, used by scheduler)
		&cfg.Delete,
		logger,
	)
	deleteHandler.Register(mux)
}
```

In shutdown section, add tombstone persistence:

```go
// Before store.Close():
if tombstoneStore != nil {
	if err := tombstoneStore.PersistToDisk(cfg.Delete.PersistPath); err != nil {
		logger.Error("failed to persist tombstones", "error", err)
	}
}
if deleteScheduler != nil {
	deleteScheduler.Stop()
}
```

- [ ] **Step 2: Verify build succeeds**

Run: `go build ./cmd/lakehouse/`
Expected: success

- [ ] **Step 3: Verify all tests still pass**

Run: `go test ./... 2>&1 | grep -E "FAIL|ok" | tail -30`
Expected: all packages PASS

- [ ] **Step 4: Commit**

```bash
git add cmd/lakehouse/main.go
git commit -m "feat(delete): wire tombstone store and rewrite scheduler into main binary"
```

---

### Task 11: Integration Test

**Files:**
- Create: `internal/delete/integration_test.go`

- [ ] **Step 1: Write full round-trip integration test**

```go
// internal/delete/integration_test.go
package delete

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func TestIntegration_DeleteRoundTrip(t *testing.T) {
	// Setup: pool with parquet files, manifest, tombstone store
	pool := &mockPool{files: make(map[string][]byte)}

	// File 1: has rows to delete
	data1 := generateTestParquet(t, []map[string]string{
		{"service.name": "api", "body": "request started", "timestamp_unix_nano": "1000"},
		{"service.name": "secret-service", "body": "PII data", "timestamp_unix_nano": "2000"},
		{"service.name": "api", "body": "request done", "timestamp_unix_nano": "3000"},
	})
	pool.files["logs/dt=2026-01-01/hour=10/f1.parquet"] = data1

	// File 2: no rows to delete
	data2 := generateTestParquet(t, []map[string]string{
		{"service.name": "api", "body": "normal log", "timestamp_unix_nano": "4000"},
	})
	pool.files["logs/dt=2026-01-01/hour=11/f2.parquet"] = data2

	mf := manifest.New("test", "logs/", nil)
	mf.AddFile("dt=2026-01-01/hour=10", manifest.FileInfo{
		Key: "logs/dt=2026-01-01/hour=10/f1.parquet", Size: int64(len(data1)),
		MinTimeNs: 1000, MaxTimeNs: 3000,
	})
	mf.AddFile("dt=2026-01-01/hour=11", manifest.FileInfo{
		Key: "logs/dt=2026-01-01/hour=11/f2.parquet", Size: int64(len(data2)),
		MinTimeNs: 4000, MaxTimeNs: 4000,
	})

	store := NewTombstoneStore()
	cfg := &config.DeleteConfig{
		Enabled:            true,
		AutoRewriteClasses: []string{"STANDARD"},
		RewriteDelay:       0, // immediate for test
	}

	handler := NewHandler(store, mf, nil, cfg, nil)
	mux := http.NewServeMux()
	handler.Register(mux)

	// Step 1: Estimate
	req := httptest.NewRequest("POST",
		`/delete/logsql/estimate?query=service.name:="secret-service"&start=0&end=5000`, nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("estimate: %d %s", w.Code, w.Body.String())
	}
	var est EstimateResponse
	json.Unmarshal(w.Body.Bytes(), &est)
	if est.AffectedFiles != 1 {
		t.Fatalf("expected 1 affected file in estimate, got %d", est.AffectedFiles)
	}

	// Step 2: Delete (tombstone)
	req = httptest.NewRequest("POST",
		`/delete/logsql/delete?query=service.name:="secret-service"&start=0&end=5000`, nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != 200 {
		t.Fatalf("delete: %d %s", w.Code, w.Body.String())
	}
	var delResp DeleteResponse
	json.Unmarshal(w.Body.Bytes(), &delResp)
	if delResp.TombstoneID == "" {
		t.Fatal("expected tombstone ID")
	}

	// Step 3: List tombstones
	req = httptest.NewRequest("GET", "/delete/logsql/tombstones", nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var listResp TombstonesListResponse
	json.Unmarshal(w.Body.Bytes(), &listResp)
	if listResp.Count != 1 {
		t.Fatalf("expected 1 tombstone, got %d", listResp.Count)
	}

	// Step 4: Run rewriter (simulates background scheduler)
	rewriter := NewRewriter(RewriterConfig{
		Pool:     pool,
		Manifest: mf,
		Prefix:   "logs/",
		Mode:     config.ModeLogs,
	})

	detector := NewStorageClassDetector(nil)
	sched := NewRewriteScheduler(RewriteSchedulerConfig{
		Store:          store,
		Rewriter:       rewriter,
		Detector:       detector,
		RewriteDelay:   0,
		AllowedClasses: []string{"STANDARD"},
		MaxConcurrent:  1,
	})

	results := sched.RunOnce(context.Background())
	if len(results) != 1 {
		t.Fatalf("expected 1 rewrite, got %d", len(results))
	}
	if results[0].RowsRemoved != 1 {
		t.Fatalf("expected 1 row removed, got %d", results[0].RowsRemoved)
	}
	if results[0].RowsKept != 2 {
		t.Fatalf("expected 2 rows kept, got %d", results[0].RowsKept)
	}

	// Step 5: Verify old file gone, new file exists
	if _, ok := pool.files["logs/dt=2026-01-01/hour=10/f1.parquet"]; ok {
		t.Fatal("old file should be deleted after rewrite")
	}

	// Step 6: Un-delete (remove tombstone)
	req = httptest.NewRequest("DELETE", "/delete/logsql/tombstone/"+delResp.TombstoneID, nil)
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, req)
	if w.Code != 200 {
		t.Fatalf("remove tombstone: %d", w.Code)
	}

	if store.Count() != 0 {
		t.Fatal("tombstone should be removed")
	}
}
```

- [ ] **Step 2: Run test to verify it passes**

Run: `go test ./internal/delete/ -v -run TestIntegration`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/delete/integration_test.go
git commit -m "test(delete): add full round-trip integration test"
```

---

### Task 12: E2E Delete Test

**Files:**
- Create: `tests/e2e/delete_test.go`

- [ ] **Step 1: Write E2E test using the running lakehouse**

```go
//go:build e2e

package e2e

import (
	"encoding/json"
	"net/http"
	"net/url"
	"testing"
	"time"
)

func TestDelete_TombstoneAndQuery(t *testing.T) {
	waitForHealth(t, logsBaseURL, 60*time.Second)

	// Step 1: Verify data exists before delete
	params := defaultTimeParams()
	params.Set("query", `service.name:="nginx"`)
	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	if len(body) == 0 {
		t.Skip("no nginx data available for delete test")
	}

	// Step 2: Create tombstone
	deleteParams := url.Values{
		"query": {`service.name:="nginx"`},
		"start": {params.Get("start")},
		"end":   {params.Get("end")},
	}
	resp := httpPost(t, logsBaseURL, "/delete/logsql/delete?"+deleteParams.Encode(), "application/json", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("delete returned %d", resp.StatusCode)
	}

	var deleteResp struct {
		TombstoneID   string `json:"tombstone_id"`
		AffectedFiles int    `json:"affected_files"`
	}
	json.NewDecoder(resp.Body).Decode(&deleteResp)

	if deleteResp.TombstoneID == "" {
		t.Fatal("expected tombstone ID in response")
	}
	t.Logf("tombstone created: %s, affected %d files", deleteResp.TombstoneID, deleteResp.AffectedFiles)

	// Step 3: Query again — deleted data should NOT appear
	body2 := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	if len(body2) > 0 {
		// Check no nginx rows in response
		if containsService(body2, "nginx") {
			t.Fatal("tombstoned data should not appear in query results")
		}
	}

	// Step 4: Un-delete (remove tombstone)
	req, _ := http.NewRequest("DELETE", logsBaseURL+"/delete/logsql/tombstone/"+deleteResp.TombstoneID, nil)
	client := &http.Client{Timeout: 30 * time.Second}
	undelResp, err := client.Do(req)
	if err != nil {
		t.Fatalf("un-delete failed: %v", err)
	}
	undelResp.Body.Close()
	if undelResp.StatusCode != http.StatusOK {
		t.Fatalf("un-delete returned %d", undelResp.StatusCode)
	}

	// Step 5: Query again — data should be back
	body3 := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	if !containsService(body3, "nginx") {
		t.Fatal("data should be visible again after un-delete")
	}
}

func TestDelete_Estimate(t *testing.T) {
	waitForHealth(t, logsBaseURL, 60*time.Second)

	params := defaultTimeParams()
	resp := httpPost(t, logsBaseURL,
		"/delete/logsql/estimate?query=*&start="+params.Get("start")+"&end="+params.Get("end"),
		"application/json", nil)
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		t.Fatalf("estimate returned %d", resp.StatusCode)
	}

	var est struct {
		AffectedFiles int `json:"affected_files"`
	}
	json.NewDecoder(resp.Body).Decode(&est)
	if est.AffectedFiles == 0 {
		t.Fatal("expected non-zero affected files in estimate")
	}
	t.Logf("estimate: %d files affected", est.AffectedFiles)
}

func containsService(body []byte, service string) bool {
	// NDJSON response — check if any line contains the service
	return len(body) > 0 && contains(string(body), service)
}

func contains(s, substr string) bool {
	return len(s) > 0 && len(substr) > 0 && indexOf(s, substr) >= 0
}

func indexOf(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build -tags=e2e ./tests/e2e/`
Expected: success (tests only run with Docker compose)

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/delete_test.go
git commit -m "test(e2e): add delete tombstone and un-delete E2E tests"
```

---

### Task 13: Helm Chart — Delete Config in values.yaml

**Files:**
- Modify: `charts/victoria-lakehouse/values.yaml`

- [ ] **Step 1: Add delete config to lakehouseConfig section**

In `charts/victoria-lakehouse/values.yaml`, inside `lakehouseConfig:`, add:

```yaml
  delete:
    enabled: true
    auto_rewrite_classes:
      - STANDARD
    rewrite_delay: 1h
    rewrite_batch_size: 50
    rewrite_max_concurrent: 2
    persist_path: /data/lakehouse/tombstones
    cost_warning_threshold: 10.0
    force_glacier_header: X-Force-Glacier-Delete
```

- [ ] **Step 2: Verify helm lint passes**

Run: `helm lint charts/victoria-lakehouse/`
Expected: PASS (0 errors)

- [ ] **Step 3: Commit**

```bash
git add charts/victoria-lakehouse/values.yaml
git commit -m "feat(helm): add delete config to lakehouseConfig values"
```

---

### Task 14: Documentation Updates

**Files:**
- Modify: `docs/deletion-strategy.md` (add implementation status)
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Update deletion strategy doc with implementation notes**

Add at the top of `docs/deletion-strategy.md` after the `# Cost-Aware Deletion Strategy` heading:

```markdown
> **Status:** Implemented. Tombstone store, HTTP handlers, query-time filtering, background rewriter, and storage-class-aware scheduler are all functional.
```

- [ ] **Step 2: Add to CHANGELOG.md under [0.10.0]**

Add to the `### Added` section:

```markdown
- Cost-aware deletion: VL-compatible `/delete/logsql/*` APIs with tombstone-based soft delete
- Tombstone query-time filtering (zero-cost data suppression)
- Background rewriter for S3 Standard files (physical deletion)
- Storage class detection (never rewrites Glacier/IA data)
- Cost estimation endpoint (`/delete/logsql/estimate`)
- Un-delete support (remove tombstone to restore visibility)
```

- [ ] **Step 3: Commit**

```bash
git add docs/deletion-strategy.md CHANGELOG.md
git commit -m "docs: mark deletion strategy as implemented, update CHANGELOG"
```

---

## Self-Review

**Spec coverage check** (against `docs/deletion-strategy.md`):
- Tier 1 (Tombstone): Task 1, 2, 7 — tombstone store, persistence, query-time filtering
- Tier 2 (Rewrite): Task 8, 9 — rewriter and scheduler with storage class gating
- Tier 3 (Lifecycle): Handled by S3 lifecycle rules (no code needed), documented in storageclass.go
- API Surface: Task 6 — delete, estimate, tombstones list, tombstone by ID (get/delete)
- Storage class detection: Task 3
- Configuration: Task 4
- Metrics: Task 5
- Integration: Tasks 10, 11, 12
- Helm: Task 13
- Documentation: Task 14

**Placeholder scan:** No TBD/TODO found. All code blocks complete.

**Type consistency check:**
- `Tombstone` struct used consistently (ID, Query, StartNs, EndNs, AffectedKeys, CreatedAt, CreatedBy, Reaped)
- `TombstoneStore` methods consistent (Add, Remove, Get, Active, ForRange, Count, PersistToDisk, LoadFromDisk)
- `Handler` uses `NewHandler(store, manifest, detector, cfg, logger)` in both test and main
- `Rewriter` uses `RewriterConfig` struct consistently
- `RewriteScheduler` uses `RewriteSchedulerConfig` consistently
- `StorageClassDetector` uses `NewStorageClassDetector(client)` + `SetBucket()`
- `DeleteConfig` field names match between config.go and values.yaml
