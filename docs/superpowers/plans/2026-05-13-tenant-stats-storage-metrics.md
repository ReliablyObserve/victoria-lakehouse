# Tenant Stats & Storage Metrics — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Implement the full runtime for per-tenant storage statistics, S3 storage class tracking, cost estimation, JSON API, cardinality limiter, Lakehouse Explorer UI, and VMUI tab injection — all 12 remaining spec components from the tenant-stats-storage-metrics design.

**Architecture:** A distributed TenantRegistry (CRDT peer-synced, S3-durable) feeds JSON API endpoints and Prometheus metrics. A standalone Preact+uPlot UI at `/lakehouse/ui/` plus VMUI tab injection provide visualization. Storage class tracking uses a 4-layer detection strategy (write-tag → lifecycle prediction → HeadObject sampling → S3 Inventory).

**Tech Stack:** Go 1.26, `github.com/valyala/gozstd` (ZSTD), `github.com/VictoriaMetrics/metrics` (Prometheus), AWS SDK v2 (S3 HeadObject), Preact+uPlot+HTM (UI, CDN, no build step)

**Spec:** `docs/superpowers/specs/2026-05-13-tenant-stats-storage-metrics-design.md`

**Build constraint:** `GOWORK=off` mandatory for all `go build`/`go test` commands.

---

## File Structure

| File | Responsibility |
|---|---|
| `internal/manifest/manifest.go` | Extend `FileInfo` with StorageClass fields |
| `internal/cache/persist.go` | Extend `LabelInfo` with per-tenant cardinality |
| `internal/stats/cardinality_limiter.go` | Prometheus tenant label cardinality cap |
| `internal/stats/cardinality_limiter_test.go` | Limiter unit tests |
| `internal/stats/storageclass.go` | 4-layer storage class detection |
| `internal/stats/storageclass_test.go` | Storage class tests |
| `internal/stats/cost.go` | Per-class pricing, lifecycle projections |
| `internal/stats/cost_test.go` | Cost calculation tests |
| `internal/stats/registry.go` | TenantRegistry: CRDT merge, feed points |
| `internal/stats/registry_test.go` | Registry CRUD, merge, concurrency tests |
| `internal/stats/sync.go` | Peer broadcast, S3 snapshots |
| `internal/stats/sync_test.go` | Sync protocol tests |
| `internal/stats/api.go` | 7 JSON API handlers |
| `internal/stats/api_test.go` | API endpoint tests |
| `internal/ui/ui.go` | HTTP handler serving static UI |
| `internal/ui/static/index.html` | Preact+uPlot single-file app (3 tabs) |
| `internal/ui/static/vmui-tab.js` | VMUI nav injection script |
| `internal/ui/vmui_inject.go` | VMUI response wrapper middleware |
| `internal/ui/vmui_inject_test.go` | Injection tests |
| `cmd/lakehouse-logs/main.go` | Wire registry, API, UI, sync, metrics |
| `lakehouse-traces/main.go` | Mirror wiring for traces module |
| `internal/manifest/manifest_test.go` | Regression tests for FileInfo extension |
| `internal/cache/persist_test.go` | Regression tests for LabelIndex extension |

---

### Task 1: Manifest FileInfo Extension

**Files:**
- Modify: `internal/manifest/manifest.go:22-32`
- Test: `internal/manifest/manifest_test.go` (add regression tests)

This is the foundation — storage class tracking, cost estimation, and the registry all depend on these new fields.

- [ ] **Step 1: Write regression tests for existing FileInfo behavior**

Add tests that verify existing FileInfo serialization, AddFile, RemoveFile, and GetFilesForRange still work after we add fields. These pin the current behavior.

```go
// Add to internal/manifest/manifest_test.go

func TestFileInfoJSONBackwardCompat(t *testing.T) {
	// Existing FileInfo without new fields must still deserialize
	raw := `{"key":"logs/dt=2026-05-01/hour=00/batch1.parquet","size":1024,"row_count":100,"min_time_ns":1000,"max_time_ns":2000}`
	var fi FileInfo
	if err := json.Unmarshal([]byte(raw), &fi); err != nil {
		t.Fatalf("unmarshal old format: %v", err)
	}
	if fi.Key != "logs/dt=2026-05-01/hour=00/batch1.parquet" {
		t.Fatalf("key mismatch: %s", fi.Key)
	}
	if fi.Size != 1024 {
		t.Fatalf("size mismatch: %d", fi.Size)
	}
	// New fields should be zero-valued
	if fi.StorageClass != "" {
		t.Fatalf("storage class should be empty for old format: %s", fi.StorageClass)
	}
	if !fi.CreatedAt.IsZero() {
		t.Fatalf("created_at should be zero for old format")
	}
}

func TestFileInfoJSONRoundTrip(t *testing.T) {
	now := time.Now().Truncate(time.Second)
	fi := FileInfo{
		Key:            "logs/dt=2026-05-01/hour=10/batch.parquet",
		Size:           2048,
		RowCount:       500,
		MinTimeNs:      1000,
		MaxTimeNs:      9000,
		RawBytes:       4096,
		StorageClass:   "STANDARD_IA",
		ClassCheckedAt: now,
		ClassSource:    "lifecycle",
		CreatedAt:      now.Add(-24 * time.Hour),
	}
	data, err := json.Marshal(fi)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}
	var fi2 FileInfo
	if err := json.Unmarshal(data, &fi2); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}
	if fi2.StorageClass != "STANDARD_IA" {
		t.Fatalf("storage class mismatch: %s", fi2.StorageClass)
	}
	if fi2.ClassSource != "lifecycle" {
		t.Fatalf("class source mismatch: %s", fi2.ClassSource)
	}
	if !fi2.CreatedAt.Equal(fi.CreatedAt) {
		t.Fatalf("created_at mismatch: %v vs %v", fi2.CreatedAt, fi.CreatedAt)
	}
}

func TestAddFilePreservesNewFields(t *testing.T) {
	m := New("test-bucket", "logs/")
	fi := FileInfo{
		Key:          "logs/dt=2026-05-01/hour=10/batch.parquet",
		Size:         2048,
		MinTimeNs:    time.Date(2026, 5, 1, 10, 0, 0, 0, time.UTC).UnixNano(),
		MaxTimeNs:    time.Date(2026, 5, 1, 10, 59, 0, 0, time.UTC).UnixNano(),
		StorageClass: "STANDARD",
		ClassSource:  "write",
		CreatedAt:    time.Now(),
	}
	m.AddFile("dt=2026-05-01/hour=10", fi)

	files := m.GetFilesForRange(fi.MinTimeNs, fi.MaxTimeNs)
	if len(files) != 1 {
		t.Fatalf("expected 1 file, got %d", len(files))
	}
	if files[0].StorageClass != "STANDARD" {
		t.Fatalf("storage class lost: %s", files[0].StorageClass)
	}
	if files[0].ClassSource != "write" {
		t.Fatalf("class source lost: %s", files[0].ClassSource)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail (new fields don't exist yet)**

Run: `GOWORK=off go test ./internal/manifest/ -run 'TestFileInfoJSON|TestAddFilePreserves' -v`
Expected: Compilation error — `fi.StorageClass` undefined.

- [ ] **Step 3: Add new fields to FileInfo**

In `internal/manifest/manifest.go`, extend the FileInfo struct:

```go
type FileInfo struct {
	Key               string              `json:"key"`
	Size              int64               `json:"size"`
	RowCount          int64               `json:"row_count,omitempty"`
	MinTimeNs         int64               `json:"min_time_ns,omitempty"`
	MaxTimeNs         int64               `json:"max_time_ns,omitempty"`
	RawBytes          int64               `json:"raw_bytes,omitempty"`
	SchemaFingerprint string              `json:"schema_fp,omitempty"`
	CompactionLevel   int                 `json:"compaction_level,omitempty"`
	Labels            map[string][]string `json:"labels,omitempty"`
	StorageClass      string              `json:"storage_class,omitempty"`
	ClassCheckedAt    time.Time           `json:"class_checked_at,omitempty"`
	ClassSource       string              `json:"class_source,omitempty"`
	CreatedAt         time.Time           `json:"created_at,omitempty"`
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOWORK=off go test ./internal/manifest/ -run 'TestFileInfoJSON|TestAddFilePreserves' -v`
Expected: PASS

- [ ] **Step 5: Run full manifest test suite for regressions**

Run: `GOWORK=off go test ./internal/manifest/ -v -count=1`
Expected: All existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add internal/manifest/manifest.go internal/manifest/manifest_test.go
git commit -m "feat(manifest): add StorageClass, ClassSource, CreatedAt fields to FileInfo"
```

---

### Task 2: LabelIndex Per-Tenant Extension

**Files:**
- Modify: `internal/cache/persist.go:12-17` (LabelInfo struct), `internal/cache/persist.go:28-57` (Add method)
- Test: `internal/cache/persist_test.go` (add regression + new tests)

- [ ] **Step 1: Write regression tests for existing LabelIndex behavior + new per-tenant tests**

```go
// Add to internal/cache/persist_test.go (or create if not exists)

func TestLabelIndexExistingBehavior(t *testing.T) {
	idx := NewLabelIndex()
	idx.Add("service.name", []string{"api", "web", "worker"})
	idx.Add("service.name", []string{"web", "cron"})

	names := idx.GetFieldNames()
	if len(names) != 1 {
		t.Fatalf("expected 1 field, got %d", len(names))
	}
	vals := idx.GetFieldValues("service.name", 0)
	if len(vals) != 4 { // api, web, worker, cron (web deduped)
		t.Fatalf("expected 4 values, got %d: %v", len(vals), vals)
	}
	if idx.Len() != 1 {
		t.Fatalf("expected Len()=1, got %d", idx.Len())
	}
}

func TestLabelInfoPerTenantCardinality(t *testing.T) {
	idx := NewLabelIndex()
	idx.AddWithTenant("service.name", []string{"api", "web"}, "100/1")
	idx.AddWithTenant("service.name", []string{"worker", "cron"}, "200/5")
	idx.AddWithTenant("service.name", []string{"api"}, "100/1")

	li := idx.GetLabelInfo("service.name")
	if li == nil {
		t.Fatal("label info not found")
	}
	if li.Cardinality != 4 {
		t.Fatalf("global cardinality: want 4, got %d", li.Cardinality)
	}
	if li.PerTenant == nil {
		t.Fatal("per-tenant map is nil")
	}
	if li.PerTenant["100/1"] != 2 {
		t.Fatalf("tenant 100/1 cardinality: want 2, got %d", li.PerTenant["100/1"])
	}
	if li.PerTenant["200/5"] != 2 {
		t.Fatalf("tenant 200/5 cardinality: want 2, got %d", li.PerTenant["200/5"])
	}
}

func TestLabelIndexAddWithTenantBackwardCompat(t *testing.T) {
	// Old-style Add (no tenant) still works
	idx := NewLabelIndex()
	idx.Add("k8s.pod.name", []string{"pod-1", "pod-2"})

	li := idx.GetLabelInfo("k8s.pod.name")
	if li == nil {
		t.Fatal("label info not found")
	}
	if li.PerTenant != nil && len(li.PerTenant) > 0 {
		t.Fatalf("per-tenant should be empty for plain Add: %v", li.PerTenant)
	}
	if li.Cardinality != 2 {
		t.Fatalf("cardinality: want 2, got %d", li.Cardinality)
	}
}

func TestLabelIndexPersistRoundTripWithPerTenant(t *testing.T) {
	dir := t.TempDir()
	p, err := NewPersister(dir)
	if err != nil {
		t.Fatal(err)
	}

	idx := NewLabelIndex()
	idx.AddWithTenant("trace_id", []string{"abc", "def"}, "100/1")
	idx.AddWithTenant("trace_id", []string{"ghi"}, "200/5")

	if err := p.SaveLabelIndex(idx); err != nil {
		t.Fatalf("save: %v", err)
	}

	loaded, err := p.LoadLabelIndex()
	if err != nil {
		t.Fatalf("load: %v", err)
	}

	li := loaded.GetLabelInfo("trace_id")
	if li == nil {
		t.Fatal("trace_id not found after load")
	}
	if li.Cardinality != 3 {
		t.Fatalf("cardinality after load: want 3, got %d", li.Cardinality)
	}
	if li.PerTenant["100/1"] != 2 {
		t.Fatalf("tenant 100/1 after load: want 2, got %d", li.PerTenant["100/1"])
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test ./internal/cache/ -run 'TestLabelInfo|TestLabelIndex' -v`
Expected: Compilation errors — `AddWithTenant`, `GetLabelInfo`, `PerTenant` undefined.

- [ ] **Step 3: Extend LabelInfo and LabelIndex**

In `internal/cache/persist.go`:

```go
type LabelInfo struct {
	Name        string         `json:"name"`
	Cardinality int            `json:"cardinality"`
	Values      []string       `json:"values,omitempty"`
	SeenInFiles int            `json:"seen_in_files"`
	PerTenant   map[string]int `json:"per_tenant,omitempty"`
}
```

Add new methods to LabelIndex:

```go
func (idx *LabelIndex) AddWithTenant(name string, values []string, tenant string) {
	idx.mu.Lock()
	defer idx.mu.Unlock()

	li, ok := idx.labels[name]
	if !ok {
		li = &LabelInfo{Name: name}
		idx.labels[name] = li
	}
	li.SeenInFiles++

	existing := make(map[string]bool, len(li.Values))
	for _, v := range li.Values {
		existing[v] = true
	}
	for _, v := range values {
		if !existing[v] && len(li.Values) < 10000 {
			li.Values = append(li.Values, v)
			existing[v] = true
			li.Cardinality++
		}
	}

	if tenant != "" {
		if li.PerTenant == nil {
			li.PerTenant = make(map[string]int)
		}
		tenantExisting := li.PerTenant[tenant]
		newCount := len(values)
		if newCount > tenantExisting {
			li.PerTenant[tenant] = newCount
		}
	}
}

func (idx *LabelIndex) GetLabelInfo(name string) *LabelInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	return idx.labels[name]
}

func (idx *LabelIndex) GetAllLabelInfo() []*LabelInfo {
	idx.mu.RLock()
	defer idx.mu.RUnlock()
	result := make([]*LabelInfo, 0, len(idx.labels))
	for _, li := range idx.labels {
		result = append(result, li)
	}
	return result
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOWORK=off go test ./internal/cache/ -run 'TestLabelInfo|TestLabelIndex' -v`
Expected: PASS

- [ ] **Step 5: Run full cache test suite for regressions**

Run: `GOWORK=off go test ./internal/cache/ -v -count=1`
Expected: All existing tests still pass.

- [ ] **Step 6: Commit**

```bash
git add internal/cache/persist.go internal/cache/persist_test.go
git commit -m "feat(cache): add per-tenant cardinality tracking to LabelIndex"
```

---

### Task 3: Cardinality Limiter

**Files:**
- Create: `internal/stats/cardinality_limiter.go`
- Create: `internal/stats/cardinality_limiter_test.go`

Small, standalone component — no dependencies on other new code.

- [ ] **Step 1: Write tests**

```go
package stats

import (
	"sync"
	"testing"
)

func TestCardinalityLimiterAllow(t *testing.T) {
	cl := NewCardinalityLimiter(3)

	if !cl.Allow("100/1") {
		t.Fatal("first tenant should be allowed")
	}
	if !cl.Allow("200/5") {
		t.Fatal("second tenant should be allowed")
	}
	if !cl.Allow("300/1") {
		t.Fatal("third tenant should be allowed (at cap)")
	}
	if cl.Allow("400/1") {
		t.Fatal("fourth tenant should be rejected (over cap)")
	}
	// Existing tenant still allowed
	if !cl.Allow("100/1") {
		t.Fatal("existing tenant should still be allowed")
	}

	if cl.TrackedCount() != 3 {
		t.Fatalf("tracked: want 3, got %d", cl.TrackedCount())
	}
	if cl.OverflowCount() != 1 {
		t.Fatalf("overflow: want 1, got %d", cl.OverflowCount())
	}
}

func TestCardinalityLimiterZeroDisables(t *testing.T) {
	cl := NewCardinalityLimiter(0)

	// 0 means disable per-tenant metrics entirely
	if cl.Allow("100/1") {
		t.Fatal("should reject all when limit is 0")
	}
}

func TestCardinalityLimiterNegativeUnlimited(t *testing.T) {
	cl := NewCardinalityLimiter(-1)

	for i := 0; i < 1000; i++ {
		if !cl.Allow(fmt.Sprintf("tenant/%d", i)) {
			t.Fatalf("should allow unlimited tenants, blocked at %d", i)
		}
	}
}

func TestCardinalityLimiterConcurrent(t *testing.T) {
	cl := NewCardinalityLimiter(100)
	var wg sync.WaitGroup
	for i := 0; i < 200; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			cl.Allow(fmt.Sprintf("tenant/%d", id))
		}(i)
	}
	wg.Wait()

	if cl.TrackedCount() != 100 {
		t.Fatalf("tracked: want 100, got %d", cl.TrackedCount())
	}
	if cl.OverflowCount() != 100 {
		t.Fatalf("overflow: want 100, got %d", cl.OverflowCount())
	}
}

func TestCardinalityLimiterReset(t *testing.T) {
	cl := NewCardinalityLimiter(2)
	cl.Allow("a/1")
	cl.Allow("b/2")
	cl.Allow("c/3") // overflow

	cl.Reset()
	if cl.TrackedCount() != 0 {
		t.Fatalf("tracked after reset: want 0, got %d", cl.TrackedCount())
	}
	// overflow counter is NOT reset (it's cumulative)
	if cl.OverflowCount() != 1 {
		t.Fatalf("overflow should persist after reset: want 1, got %d", cl.OverflowCount())
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test ./internal/stats/ -run TestCardinalityLimiter -v`
Expected: Compilation error — package doesn't exist yet.

- [ ] **Step 3: Implement CardinalityLimiter**

```go
package stats

import (
	"sync"
	"sync/atomic"
)

type CardinalityLimiter struct {
	mu         sync.RWMutex
	maxTenants int
	tracked    map[string]bool
	overflow   atomic.Int64
}

func NewCardinalityLimiter(maxTenants int) *CardinalityLimiter {
	return &CardinalityLimiter{
		maxTenants: maxTenants,
		tracked:    make(map[string]bool),
	}
}

func (cl *CardinalityLimiter) Allow(tenant string) bool {
	if cl.maxTenants == 0 {
		return false
	}
	if cl.maxTenants < 0 {
		cl.mu.Lock()
		cl.tracked[tenant] = true
		cl.mu.Unlock()
		return true
	}

	cl.mu.RLock()
	if cl.tracked[tenant] {
		cl.mu.RUnlock()
		return true
	}
	cl.mu.RUnlock()

	cl.mu.Lock()
	defer cl.mu.Unlock()
	if cl.tracked[tenant] {
		return true
	}
	if len(cl.tracked) >= cl.maxTenants {
		cl.overflow.Add(1)
		return false
	}
	cl.tracked[tenant] = true
	return true
}

func (cl *CardinalityLimiter) TrackedCount() int {
	cl.mu.RLock()
	defer cl.mu.RUnlock()
	return len(cl.tracked)
}

func (cl *CardinalityLimiter) OverflowCount() int64 {
	return cl.overflow.Load()
}

func (cl *CardinalityLimiter) Reset() {
	cl.mu.Lock()
	defer cl.mu.Unlock()
	cl.tracked = make(map[string]bool)
}
```

- [ ] **Step 4: Run tests to verify they pass**

Run: `GOWORK=off go test ./internal/stats/ -run TestCardinalityLimiter -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/stats/cardinality_limiter.go internal/stats/cardinality_limiter_test.go
git commit -m "feat(stats): add CardinalityLimiter for Prometheus tenant label cap"
```

---

### Task 4: Storage Class Tracker

**Files:**
- Create: `internal/stats/storageclass.go`
- Create: `internal/stats/storageclass_test.go`

Depends on: Task 1 (FileInfo extension), config.LifecycleRuleConfig.

- [ ] **Step 1: Write tests**

```go
package stats

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func TestLifecyclePrediction(t *testing.T) {
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
		{TransitionDays: 90, StorageClass: "GLACIER"},
		{TransitionDays: 365, StorageClass: "DEEP_ARCHIVE"},
	}
	tracker := NewStorageClassTracker(rules, nil)

	tests := []struct {
		name     string
		age      time.Duration
		expected string
	}{
		{"fresh file", 1 * 24 * time.Hour, "STANDARD"},
		{"29 days", 29 * 24 * time.Hour, "STANDARD"},
		{"30 days", 30 * 24 * time.Hour, "STANDARD_IA"},
		{"89 days", 89 * 24 * time.Hour, "STANDARD_IA"},
		{"90 days", 90 * 24 * time.Hour, "GLACIER"},
		{"364 days", 364 * 24 * time.Hour, "GLACIER"},
		{"365 days", 365 * 24 * time.Hour, "DEEP_ARCHIVE"},
		{"2 years", 730 * 24 * time.Hour, "DEEP_ARCHIVE"},
	}

	now := time.Now()
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			createdAt := now.Add(-tt.age)
			class := tracker.PredictClass(createdAt, now)
			if class != tt.expected {
				t.Fatalf("age %v: want %s, got %s", tt.age, tt.expected, class)
			}
		})
	}
}

func TestLifecyclePredictionNoRules(t *testing.T) {
	tracker := NewStorageClassTracker(nil, nil)
	class := tracker.PredictClass(time.Now().Add(-365*24*time.Hour), time.Now())
	if class != "STANDARD" {
		t.Fatalf("no rules should return STANDARD, got %s", class)
	}
}

func TestNearTransitionBoundary(t *testing.T) {
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
	}
	tracker := NewStorageClassTracker(rules, nil)

	now := time.Now()
	// File is 28 days old — within 2 days of 30-day boundary
	createdAt := now.Add(-28 * 24 * time.Hour)
	if !tracker.NearBoundary(createdAt, now) {
		t.Fatal("28-day file should be near 30-day boundary")
	}

	// File is 10 days old — not near any boundary
	createdAt = now.Add(-10 * 24 * time.Hour)
	if tracker.NearBoundary(createdAt, now) {
		t.Fatal("10-day file should not be near any boundary")
	}

	// File is 32 days old — past boundary, not near next
	createdAt = now.Add(-32 * 24 * time.Hour)
	if tracker.NearBoundary(createdAt, now) {
		t.Fatal("32-day file past only boundary should not be near anything")
	}
}

func TestPerTenantRuleOverride(t *testing.T) {
	defaultRules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
	}
	tenantRules := map[string][]config.LifecycleRuleConfig{
		"100/1": {
			{TransitionDays: 14, StorageClass: "STANDARD_IA"},
			{TransitionDays: 60, StorageClass: "GLACIER"},
		},
	}
	tracker := NewStorageClassTracker(defaultRules, tenantRules)

	now := time.Now()
	createdAt := now.Add(-20 * 24 * time.Hour)

	// Default rules: 20 days = STANDARD
	class := tracker.PredictClass(createdAt, now)
	if class != "STANDARD" {
		t.Fatalf("default rules, 20 days: want STANDARD, got %s", class)
	}

	// Tenant 100/1 rules: 20 days = STANDARD_IA (14-day transition)
	class = tracker.PredictClassForTenant(createdAt, now, "100/1")
	if class != "STANDARD_IA" {
		t.Fatalf("tenant 100/1 rules, 20 days: want STANDARD_IA, got %s", class)
	}

	// Tenant 200/5 (no override): uses defaults
	class = tracker.PredictClassForTenant(createdAt, now, "200/5")
	if class != "STANDARD" {
		t.Fatalf("tenant 200/5 (default), 20 days: want STANDARD, got %s", class)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test ./internal/stats/ -run TestLifecycle -v`
Expected: Compilation error.

- [ ] **Step 3: Implement StorageClassTracker**

```go
package stats

import (
	"sort"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

type StorageClassTracker struct {
	defaultRules []config.LifecycleRuleConfig
	tenantRules  map[string][]config.LifecycleRuleConfig
}

func NewStorageClassTracker(defaultRules []config.LifecycleRuleConfig, tenantRules map[string][]config.LifecycleRuleConfig) *StorageClassTracker {
	sorted := make([]config.LifecycleRuleConfig, len(defaultRules))
	copy(sorted, defaultRules)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TransitionDays > sorted[j].TransitionDays
	})
	return &StorageClassTracker{
		defaultRules: sorted,
		tenantRules:  tenantRules,
	}
}

func (sct *StorageClassTracker) PredictClass(createdAt, now time.Time) string {
	return predictWithRules(sct.defaultRules, createdAt, now)
}

func (sct *StorageClassTracker) PredictClassForTenant(createdAt, now time.Time, tenant string) string {
	if rules, ok := sct.tenantRules[tenant]; ok {
		return predictWithRules(rules, createdAt, now)
	}
	return predictWithRules(sct.defaultRules, createdAt, now)
}

func predictWithRules(rules []config.LifecycleRuleConfig, createdAt, now time.Time) string {
	if len(rules) == 0 {
		return "STANDARD"
	}
	ageDays := int(now.Sub(createdAt).Hours() / 24)
	sorted := make([]config.LifecycleRuleConfig, len(rules))
	copy(sorted, rules)
	sort.Slice(sorted, func(i, j int) bool {
		return sorted[i].TransitionDays > sorted[j].TransitionDays
	})
	for _, rule := range sorted {
		if ageDays >= rule.TransitionDays {
			return rule.StorageClass
		}
	}
	return "STANDARD"
}

func (sct *StorageClassTracker) NearBoundary(createdAt, now time.Time) bool {
	return nearBoundaryWithRules(sct.defaultRules, createdAt, now)
}

func nearBoundaryWithRules(rules []config.LifecycleRuleConfig, createdAt, now time.Time) bool {
	ageDays := int(now.Sub(createdAt).Hours() / 24)
	const boundaryWindow = 2
	for _, rule := range rules {
		diff := rule.TransitionDays - ageDays
		if diff > 0 && diff <= boundaryWindow {
			return true
		}
	}
	return false
}
```

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/stats/ -run 'TestLifecycle|TestNear|TestPerTenant' -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/stats/storageclass.go internal/stats/storageclass_test.go
git commit -m "feat(stats): add StorageClassTracker with lifecycle prediction and per-tenant rules"
```

---

### Task 5: Cost Calculator

**Files:**
- Create: `internal/stats/cost.go`
- Create: `internal/stats/cost_test.go`

Depends on: config.StatsConfig (S3PricePerGB, S3RequestPrices).

- [ ] **Step 1: Write tests**

```go
package stats

import (
	"math"
	"testing"
)

func TestCostCalculatorStorageCost(t *testing.T) {
	prices := map[string]float64{
		"STANDARD":     0.023,
		"STANDARD_IA":  0.0125,
		"GLACIER":      0.0036,
		"DEEP_ARCHIVE": 0.00099,
	}
	calc := NewCostCalculator(prices, nil)

	tests := []struct {
		class string
		bytes int64
		want  float64
	}{
		{"STANDARD", 1 << 30, 0.023},       // 1 GB
		{"STANDARD_IA", 10 << 30, 0.125},    // 10 GB
		{"GLACIER", 100 << 30, 0.36},        // 100 GB
		{"DEEP_ARCHIVE", 1000 << 30, 0.99},  // 1 TB
		{"UNKNOWN_CLASS", 1 << 30, 0},        // unknown class = $0
	}
	for _, tt := range tests {
		t.Run(tt.class, func(t *testing.T) {
			got := calc.MonthlyStorageCost(tt.class, tt.bytes)
			if math.Abs(got-tt.want) > 0.001 {
				t.Fatalf("want $%.4f, got $%.4f", tt.want, got)
			}
		})
	}
}

func TestCostCalculatorTotalCost(t *testing.T) {
	prices := map[string]float64{
		"STANDARD":    0.023,
		"STANDARD_IA": 0.0125,
	}
	calc := NewCostCalculator(prices, nil)

	byClass := map[string]int64{
		"STANDARD":    100 << 30, // 100 GB
		"STANDARD_IA": 50 << 30,  // 50 GB
	}
	total := calc.TotalMonthlyCost(byClass)
	expected := 0.023*100 + 0.0125*50 // 2.3 + 0.625 = 2.925
	if math.Abs(total-expected) > 0.001 {
		t.Fatalf("want $%.4f, got $%.4f", expected, total)
	}
}

func TestCostCalculatorRequestCost(t *testing.T) {
	reqPrices := map[string]float64{
		"PUT":  0.005,  // per 1000
		"GET":  0.0004, // per 1000
		"LIST": 0.005,  // per 1000
	}
	calc := NewCostCalculator(nil, reqPrices)

	got := calc.RequestCost("PUT", 3600)
	expected := 0.005 * 3.6 // 3600/1000 * 0.005 = 0.018
	if math.Abs(got-expected) > 0.0001 {
		t.Fatalf("PUT cost: want $%.4f, got $%.4f", expected, got)
	}
}

func TestCostCalculatorLifecycleSavings(t *testing.T) {
	prices := map[string]float64{
		"STANDARD":    0.023,
		"STANDARD_IA": 0.0125,
		"GLACIER":     0.0036,
	}
	calc := NewCostCalculator(prices, nil)

	byClass := map[string]int64{
		"STANDARD":    100 << 30,
		"STANDARD_IA": 50 << 30,
		"GLACIER":     200 << 30,
	}
	savings := calc.LifecycleSavings(byClass)

	totalBytes := int64(350) << 30
	allStandard := 0.023 * float64(totalBytes) / (1 << 30)
	actual := calc.TotalMonthlyCost(byClass)
	expected := allStandard - actual

	if math.Abs(savings-expected) > 0.01 {
		t.Fatalf("savings: want $%.2f, got $%.2f", expected, savings)
	}
}

func TestCostCalculatorGrowthProjection(t *testing.T) {
	calc := NewCostCalculator(map[string]float64{"STANDARD": 0.023}, nil)

	dailyBytes := []int64{
		10 << 30, 10 << 30, 10 << 30, 10 << 30, 10 << 30,
		10 << 30, 10 << 30,
	}
	projected30d := calc.ProjectCost30d(dailyBytes, "STANDARD")
	// 10 GB/day * 30 days = 300 GB cumulative avg ~ 150 GB avg over period
	// More precisely: sum(day_i * (30-i) for 0..29) / 30 days storage
	// Simplified: just checks it's positive and reasonable
	if projected30d <= 0 {
		t.Fatalf("30d projection should be positive, got $%.4f", projected30d)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test ./internal/stats/ -run TestCostCalculator -v`
Expected: Compilation error.

- [ ] **Step 3: Implement CostCalculator**

```go
package stats

type CostCalculator struct {
	storagePrices map[string]float64
	requestPrices map[string]float64
}

func NewCostCalculator(storagePrices, requestPrices map[string]float64) *CostCalculator {
	if storagePrices == nil {
		storagePrices = make(map[string]float64)
	}
	if requestPrices == nil {
		requestPrices = make(map[string]float64)
	}
	return &CostCalculator{
		storagePrices: storagePrices,
		requestPrices: requestPrices,
	}
}

func (cc *CostCalculator) MonthlyStorageCost(class string, bytes int64) float64 {
	price, ok := cc.storagePrices[class]
	if !ok {
		return 0
	}
	gb := float64(bytes) / (1 << 30)
	return price * gb
}

func (cc *CostCalculator) TotalMonthlyCost(byClass map[string]int64) float64 {
	var total float64
	for class, bytes := range byClass {
		total += cc.MonthlyStorageCost(class, bytes)
	}
	return total
}

func (cc *CostCalculator) RequestCost(operation string, count int64) float64 {
	price, ok := cc.requestPrices[operation]
	if !ok {
		return 0
	}
	return price * float64(count) / 1000
}

func (cc *CostCalculator) LifecycleSavings(byClass map[string]int64) float64 {
	var totalBytes int64
	for _, b := range byClass {
		totalBytes += b
	}
	allStandard := cc.MonthlyStorageCost("STANDARD", totalBytes)
	actual := cc.TotalMonthlyCost(byClass)
	savings := allStandard - actual
	if savings < 0 {
		return 0
	}
	return savings
}

func (cc *CostCalculator) ProjectCost30d(dailyBytes []int64, defaultClass string) float64 {
	if len(dailyBytes) == 0 {
		return 0
	}
	var sum int64
	for _, b := range dailyBytes {
		sum += b
	}
	avgDaily := float64(sum) / float64(len(dailyBytes))
	totalAfter30d := avgDaily * 30
	avgStored := totalAfter30d / 2
	return cc.MonthlyStorageCost(defaultClass, int64(avgStored))
}
```

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/stats/ -run TestCostCalculator -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/stats/cost.go internal/stats/cost_test.go
git commit -m "feat(stats): add CostCalculator with per-class pricing and lifecycle savings"
```

---

### Task 6: TenantRegistry

**Files:**
- Create: `internal/stats/registry.go`
- Create: `internal/stats/registry_test.go`

Depends on: Task 1 (FileInfo), Task 4 (StorageClassTracker), Task 5 (CostCalculator).

- [ ] **Step 1: Write tests**

```go
package stats

import (
	"fmt"
	"sync"
	"testing"
	"time"
)

func TestRegistryRecordWrite(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")

	ts := reg.Get("100/1")
	if ts == nil {
		t.Fatal("tenant not found")
	}
	if ts.TotalFiles != 1 {
		t.Fatalf("files: want 1, got %d", ts.TotalFiles)
	}
	if ts.TotalBytes != 1024 {
		t.Fatalf("bytes: want 1024, got %d", ts.TotalBytes)
	}
	if ts.RawBytes != 2048 {
		t.Fatalf("raw bytes: want 2048, got %d", ts.RawBytes)
	}
	if ts.TotalRows != 500 {
		t.Fatalf("rows: want 500, got %d", ts.TotalRows)
	}
	if ts.BytesByClass["STANDARD"] != 1024 {
		t.Fatalf("bytes by class: want 1024, got %d", ts.BytesByClass["STANDARD"])
	}
	if ts.FilesByClass["STANDARD"] != 1 {
		t.Fatalf("files by class: want 1, got %d", ts.FilesByClass["STANDARD"])
	}
	if ts.LastWriteAt.IsZero() {
		t.Fatal("last write should be set")
	}
}

func TestRegistryRecordQuery(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	reg.RecordQuery("100/1")

	ts := reg.Get("100/1")
	if ts.LastQueryAt.IsZero() {
		t.Fatal("last query should be set")
	}
}

func TestRegistryMultipleWrites(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	reg.RecordWrite("100/1", 2048, 4096, 1000, "STANDARD_IA")

	ts := reg.Get("100/1")
	if ts.TotalFiles != 2 {
		t.Fatalf("files: want 2, got %d", ts.TotalFiles)
	}
	if ts.TotalBytes != 3072 {
		t.Fatalf("bytes: want 3072, got %d", ts.TotalBytes)
	}
	if ts.BytesByClass["STANDARD"] != 1024 {
		t.Fatalf("STANDARD bytes: want 1024, got %d", ts.BytesByClass["STANDARD"])
	}
	if ts.BytesByClass["STANDARD_IA"] != 2048 {
		t.Fatalf("STANDARD_IA bytes: want 2048, got %d", ts.BytesByClass["STANDARD_IA"])
	}
}

func TestRegistryListAll(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 100, "STANDARD")
	reg.RecordWrite("200/5", 512, 1024, 50, "STANDARD")

	all := reg.All()
	if len(all) != 2 {
		t.Fatalf("want 2 tenants, got %d", len(all))
	}
}

func TestRegistryCRDTMerge(t *testing.T) {
	reg1 := NewTenantRegistry("node-1", nil, nil)
	reg2 := NewTenantRegistry("node-2", nil, nil)

	reg1.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	reg2.RecordWrite("100/1", 2048, 4096, 1000, "STANDARD")

	delta := reg2.BuildDelta(0)
	reg1.Merge(delta)

	ts := reg1.Get("100/1")
	// Per-node sums: node-1=1024 + node-2=2048 = 3072
	if ts.TotalBytes != 3072 {
		t.Fatalf("merged bytes: want 3072, got %d", ts.TotalBytes)
	}
	if ts.TotalRows != 1500 {
		t.Fatalf("merged rows: want 1500, got %d", ts.TotalRows)
	}
}

func TestRegistryCRDTMergeTimestampExtrema(t *testing.T) {
	reg1 := NewTenantRegistry("node-1", nil, nil)
	reg2 := NewTenantRegistry("node-2", nil, nil)

	reg1.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	time.Sleep(10 * time.Millisecond)
	reg2.RecordWrite("100/1", 2048, 4096, 1000, "STANDARD")

	ts1Before := reg1.Get("100/1").LastWriteAt
	delta := reg2.BuildDelta(0)
	reg1.Merge(delta)

	ts := reg1.Get("100/1")
	if !ts.LastWriteAt.After(ts1Before) {
		t.Fatal("merged LastWriteAt should be max(local, remote)")
	}
}

func TestRegistryCRDTMergeIdempotent(t *testing.T) {
	reg1 := NewTenantRegistry("node-1", nil, nil)
	reg2 := NewTenantRegistry("node-2", nil, nil)

	reg2.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	delta := reg2.BuildDelta(0)

	reg1.Merge(delta)
	bytesAfterFirst := reg1.Get("100/1").TotalBytes

	reg1.Merge(delta)
	bytesAfterSecond := reg1.Get("100/1").TotalBytes

	if bytesAfterFirst != bytesAfterSecond {
		t.Fatalf("merge should be idempotent: %d vs %d", bytesAfterFirst, bytesAfterSecond)
	}
}

func TestRegistryGeneration(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	gen0 := reg.Generation()

	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	gen1 := reg.Generation()
	if gen1 <= gen0 {
		t.Fatalf("generation should increase: %d -> %d", gen0, gen1)
	}

	reg.RecordQuery("100/1")
	gen2 := reg.Generation()
	if gen2 <= gen1 {
		t.Fatalf("generation should increase on query: %d -> %d", gen1, gen2)
	}
}

func TestRegistryConcurrent(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	var wg sync.WaitGroup
	for i := 0; i < 100; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			tenant := fmt.Sprintf("%d/1", id%10)
			reg.RecordWrite(tenant, 100, 200, 10, "STANDARD")
			reg.RecordQuery(tenant)
			_ = reg.All()
			_ = reg.Get(tenant)
		}(i)
	}
	wg.Wait()

	all := reg.All()
	if len(all) != 10 {
		t.Fatalf("want 10 tenants, got %d", len(all))
	}
}

func TestRegistryBuildDeltaSinceGeneration(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	gen := reg.Generation()

	reg.RecordWrite("200/5", 512, 1024, 50, "STANDARD")

	delta := reg.BuildDelta(gen)
	if delta.NodeID != "node-1" {
		t.Fatalf("delta nodeID: want node-1, got %s", delta.NodeID)
	}
	// Only tenant 200/5 changed since gen
	if len(delta.Tenants) != 1 {
		t.Fatalf("delta should have 1 tenant, got %d", len(delta.Tenants))
	}
	if _, ok := delta.Tenants["200/5"]; !ok {
		t.Fatal("delta should contain 200/5")
	}
}

func TestRegistryGlobalAggregates(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	reg.RecordWrite("200/5", 2048, 4096, 1000, "GLACIER")

	agg := reg.GlobalAggregates()
	if agg.TotalFiles != 2 {
		t.Fatalf("total files: want 2, got %d", agg.TotalFiles)
	}
	if agg.TotalBytes != 3072 {
		t.Fatalf("total bytes: want 3072, got %d", agg.TotalBytes)
	}
	if agg.TenantCount != 2 {
		t.Fatalf("tenant count: want 2, got %d", agg.TenantCount)
	}
	if agg.BytesByClass["STANDARD"] != 1024 {
		t.Fatalf("STANDARD bytes: want 1024, got %d", agg.BytesByClass["STANDARD"])
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test ./internal/stats/ -run TestRegistry -v`
Expected: Compilation error.

- [ ] **Step 3: Implement TenantRegistry**

Create `internal/stats/registry.go` with:

- `TenantStats` struct (all fields from spec: AccountID, ProjectID, TotalFiles, TotalBytes, RawBytes, TotalRows, Partitions, MinTimeNs, MaxTimeNs, LastWriteAt, LastQueryAt, Labels, BytesByClass, FilesByClass, NodeContribs)
- `TenantRegistry` struct with `mu sync.RWMutex`, `tenants map[string]*TenantStats`, `nodeID string`, `generation uint64`, `lastPushGen uint64`, `tenantGeneration map[string]uint64` (tracks per-tenant last-change generation)
- `NewTenantRegistry(nodeID, lifecycle, pricing)` constructor
- `RecordWrite(tenant, bytes, rawBytes, rows, storageClass)` — updates stats, increments generation
- `RecordQuery(tenant)` — updates LastQueryAt, increments generation
- `Get(tenant) *TenantStats` — returns copy
- `All() []*TenantStats` — returns all copies sorted by bytes desc
- `GlobalAggregates() *GlobalStats` — returns summed stats
- `BuildDelta(sinceGeneration) *TenantDelta` — only tenants changed since sinceGeneration
- `Merge(delta *TenantDelta)` — CRDT merge: per-node counters sum, timestamps extrema, labels max
- `Generation() uint64` — current generation
- `TenantCount() int` — number of tracked tenants

`GlobalStats` struct: TotalFiles, TotalBytes, RawBytes, TotalRows, TenantCount, BytesByClass, FilesByClass.

`TenantDelta` struct: NodeID, Generation, Tenants map[string]*TenantStats, Timestamp.

CRDT merge rules:
- Counters (files, bytes, rows): `NodeContribs[remoteNodeID] = remote.NodeContribs[remoteNodeID]`, then `total = sum(NodeContribs)`
- Timestamps: `MinTimeNs = min(local, remote)`, `MaxTimeNs = max(local, remote)`, `LastWriteAt = max(local, remote)`, `LastQueryAt = max(local, remote)`
- BytesByClass/FilesByClass: per-node tracking similar to counters

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/stats/ -run TestRegistry -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/stats/registry.go internal/stats/registry_test.go
git commit -m "feat(stats): add TenantRegistry with CRDT merge and per-node counter tracking"
```

---

### Task 7: Peer Sync

**Files:**
- Create: `internal/stats/sync.go`
- Create: `internal/stats/sync_test.go`

Depends on: Task 6 (TenantRegistry). Uses `github.com/valyala/gozstd` for compression.

- [ ] **Step 1: Write tests**

```go
package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestSyncHandlerReceiveDelta(t *testing.T) {
	reg := NewTenantRegistry("node-2", nil, nil)
	handler := NewSyncHandler(reg, "secret-key")

	delta := &TenantDelta{
		NodeID:     "node-1",
		Generation: 5,
		Tenants: map[string]*TenantStats{
			"100/1": {
				AccountID:    "100",
				ProjectID:    "1",
				TotalFiles:   10,
				TotalBytes:   1048576,
				RawBytes:     2097152,
				TotalRows:    5000,
				NodeContribs: map[string]int64{"node-1": 1048576},
				BytesByClass: map[string]int64{"STANDARD": 1048576},
				FilesByClass: map[string]int64{"STANDARD": 10},
			},
		},
		Timestamp: time.Now(),
	}

	body, _ := json.Marshal(delta)
	req := httptest.NewRequest(http.MethodPost, "/internal/stats/sync", bytes.NewReader(body))
	req.Header.Set("Authorization", "Bearer secret-key")
	req.Header.Set("Content-Type", "application/json")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d: %s", w.Code, w.Body.String())
	}

	ts := reg.Get("100/1")
	if ts == nil {
		t.Fatal("tenant not merged")
	}
	if ts.TotalBytes != 1048576 {
		t.Fatalf("bytes: want 1048576, got %d", ts.TotalBytes)
	}
}

func TestSyncHandlerRejectsBadAuth(t *testing.T) {
	reg := NewTenantRegistry("node-2", nil, nil)
	handler := NewSyncHandler(reg, "correct-key")

	req := httptest.NewRequest(http.MethodPost, "/internal/stats/sync", bytes.NewReader([]byte("{}")))
	req.Header.Set("Authorization", "Bearer wrong-key")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	if w.Code != http.StatusUnauthorized {
		t.Fatalf("want 401, got %d", w.Code)
	}
}

func TestSyncHandlerNoAuthWhenEmpty(t *testing.T) {
	reg := NewTenantRegistry("node-2", nil, nil)
	handler := NewSyncHandler(reg, "")

	body, _ := json.Marshal(&TenantDelta{NodeID: "node-1", Tenants: map[string]*TenantStats{}})
	req := httptest.NewRequest(http.MethodPost, "/internal/stats/sync", bytes.NewReader(body))
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200 with no auth, got %d", w.Code)
	}
}

func TestSyncHandlerZSTDCompression(t *testing.T) {
	reg := NewTenantRegistry("node-2", nil, nil)
	handler := NewSyncHandler(reg, "")

	delta := &TenantDelta{
		NodeID: "node-1",
		Tenants: map[string]*TenantStats{
			"100/1": {
				AccountID:    "100",
				ProjectID:    "1",
				TotalFiles:   5,
				TotalBytes:   512,
				NodeContribs: map[string]int64{"node-1": 512},
			},
		},
	}

	body, _ := json.Marshal(delta)
	compressed := compressZSTD(body)

	req := httptest.NewRequest(http.MethodPost, "/internal/stats/sync", bytes.NewReader(compressed))
	req.Header.Set("Content-Encoding", "zstd")
	w := httptest.NewRecorder()

	handler.ServeHTTP(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("want 200 with zstd, got %d: %s", w.Code, w.Body.String())
	}

	ts := reg.Get("100/1")
	if ts == nil {
		t.Fatal("tenant not merged from zstd body")
	}
}

func TestSyncPusherSendsDelta(t *testing.T) {
	var received TenantDelta
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		data, _ := io.ReadAll(r.Body)
		_ = json.Unmarshal(data, &received)
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()

	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")

	pusher := NewSyncPusher(SyncPusherConfig{
		Registry: reg,
		GetPeers: func() []string { return []string{server.URL} },
		AuthKey:  "",
		SelfAddr: "self:9428",
	})

	err := pusher.PushDelta(context.Background())
	if err != nil {
		t.Fatalf("push: %v", err)
	}

	if received.NodeID != "node-1" {
		t.Fatalf("nodeID: want node-1, got %s", received.NodeID)
	}
	if _, ok := received.Tenants["100/1"]; !ok {
		t.Fatal("tenant 100/1 not in delta")
	}
}

func TestSyncS3SnapshotRoundTrip(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	reg.RecordWrite("200/5", 2048, 4096, 1000, "GLACIER")

	// Snapshot to bytes
	data, err := reg.MarshalSnapshot()
	if err != nil {
		t.Fatalf("marshal snapshot: %v", err)
	}

	// Load into fresh registry
	reg2 := NewTenantRegistry("node-2", nil, nil)
	if err := reg2.LoadSnapshot("node-1", data); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	ts := reg2.Get("100/1")
	if ts == nil {
		t.Fatal("tenant 100/1 not loaded")
	}
	if ts.TotalBytes != 1024 {
		t.Fatalf("bytes after snapshot: want 1024, got %d", ts.TotalBytes)
	}

	all := reg2.All()
	if len(all) != 2 {
		t.Fatalf("want 2 tenants, got %d", len(all))
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test ./internal/stats/ -run TestSync -v`
Expected: Compilation error.

- [ ] **Step 3: Implement Sync components**

Create `internal/stats/sync.go` with:

- `SyncHandler` struct: HTTP handler for `POST /internal/stats/sync`. Receives JSON or ZSTD-compressed TenantDelta, calls `registry.Merge()`. Bearer auth check if configured.
- `SyncPusher` struct: periodic pusher. `SyncPusherConfig{Registry, GetPeers func()[]string, AuthKey, SelfAddr, Compress bool}`. `PushDelta(ctx)` builds delta since last push, marshals to JSON, optionally ZSTD compresses, POSTs to each peer (excluding self). `PushFull(ctx)` sends full registry (after max_delta_count).
- `compressZSTD(data []byte) []byte` / `decompressZSTD(data []byte) ([]byte, error)` — using `github.com/valyala/gozstd`
- `MarshalSnapshot() ([]byte, error)` on TenantRegistry — marshals full state
- `LoadSnapshot(sourceNodeID string, data []byte) error` on TenantRegistry — loads and merges

Also add to TenantRegistry: `SetLastPushGen(gen uint64)`, `LastPushGen() uint64`.

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/stats/ -run TestSync -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/stats/sync.go internal/stats/sync_test.go
git commit -m "feat(stats): add peer sync with ZSTD compression and S3 snapshot support"
```

---

### Task 8: JSON API Handlers

**Files:**
- Create: `internal/stats/api.go`
- Create: `internal/stats/api_test.go`

Depends on: Task 6 (TenantRegistry), Task 5 (CostCalculator), Task 4 (StorageClassTracker).

- [ ] **Step 1: Write tests for all 7 endpoints**

```go
package stats

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func setupTestAPI(t *testing.T) (*API, *TenantRegistry, *manifest.Manifest) {
	t.Helper()

	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 50<<30, 100<<30, 5000000, "STANDARD")
	reg.RecordWrite("200/5", 10<<30, 20<<30, 1000000, "STANDARD_IA")
	reg.RecordQuery("100/1")

	m := manifest.New("obs-archive", "logs/")
	m.AddFile("dt=2026-05-13/hour=10", manifest.FileInfo{
		Key:       "logs/dt=2026-05-13/hour=10/batch1.parquet",
		Size:      1 << 20,
		MinTimeNs: time.Date(2026, 5, 13, 10, 0, 0, 0, time.UTC).UnixNano(),
		MaxTimeNs: time.Date(2026, 5, 13, 10, 59, 0, 0, time.UTC).UnixNano(),
		RowCount:  1000,
	})

	prices := map[string]float64{
		"STANDARD":    0.023,
		"STANDARD_IA": 0.0125,
	}
	calc := NewCostCalculator(prices, nil)
	tracker := NewStorageClassTracker(nil, nil)
	labelIdx := cache.NewLabelIndex()
	labelIdx.Add("service.name", []string{"api", "web"})

	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     m,
		CostCalc:     calc,
		ClassTracker: tracker,
		LabelIndex:   labelIdx,
		Mode:         "logs",
		Bucket:       "obs-archive",
	})
	return api, reg, m
}

func TestAPITenantsEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/tenants", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	var resp TenantsResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalTenants != 2 {
		t.Fatalf("want 2 tenants, got %d", resp.TotalTenants)
	}
	if len(resp.Tenants) != 2 {
		t.Fatalf("want 2 tenant entries, got %d", len(resp.Tenants))
	}
}

func TestAPITenantsSort(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/tenants?sort=bytes", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	var resp TenantsResponse
	_ = json.NewDecoder(w.Body).Decode(&resp)

	if len(resp.Tenants) < 2 {
		t.Fatal("need at least 2 tenants")
	}
	if resp.Tenants[0].TotalBytes < resp.Tenants[1].TotalBytes {
		t.Fatal("should be sorted by bytes descending")
	}
}

func TestAPIOverviewEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/stats/overview", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	var resp OverviewResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.Bucket != "obs-archive" {
		t.Fatalf("bucket: want obs-archive, got %s", resp.Bucket)
	}
	if resp.Mode != "logs" {
		t.Fatalf("mode: want logs, got %s", resp.Mode)
	}
	if resp.TotalFiles != 2 {
		t.Fatalf("total files: want 2, got %d", resp.TotalFiles)
	}
	if resp.TenantCount != 2 {
		t.Fatalf("tenant count: want 2, got %d", resp.TenantCount)
	}
}

func TestAPICostEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/stats/cost", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if _, ok := resp["total_monthly_usd"]; !ok {
		t.Fatal("missing total_monthly_usd")
	}
}

func TestAPICardinalityEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/cardinality/fields", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	var resp CardinalityResponse
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp.TotalFields != 1 {
		t.Fatalf("total fields: want 1, got %d", resp.TotalFields)
	}
}

func TestAPITenantDetailEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/tenants/100/1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}

	var resp map[string]any
	if err := json.NewDecoder(w.Body).Decode(&resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if resp["account_id"] != "100" {
		t.Fatalf("account_id: want 100, got %v", resp["account_id"])
	}
}

func TestAPITenantDetailNotFound(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/tenants/999/999", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404, got %d", w.Code)
	}
}

func TestAPIIngestionEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/stats/ingestion?period=day&range=7d", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
}

func TestAPICompressionEndpoint(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/stats/compression?period=day", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test ./internal/stats/ -run TestAPI -v`
Expected: Compilation error.

- [ ] **Step 3: Implement JSON API**

Create `internal/stats/api.go` with:

- Response types: `TenantsResponse`, `TenantEntry`, `OverviewResponse`, `CostResponse`, `CardinalityResponse`, `FieldEntry`, `IngestionResponse`, `CompressionResponse`
- `APIConfig` struct: Registry, Manifest, CostCalc, ClassTracker, LabelIndex, Mode, Bucket
- `API` struct holding config
- `NewAPI(cfg APIConfig) *API`
- `Register(mux *http.ServeMux)` — registers all 7 endpoint handlers:
  - `GET /lakehouse/api/v1/tenants` — list all tenants with stats, sort param
  - `GET /lakehouse/api/v1/tenants/{accountID}/{projectID}` — tenant drill-down
  - `GET /lakehouse/api/v1/stats/overview` — global stats
  - `GET /lakehouse/api/v1/stats/ingestion` — temporal ingestion, period/range/tenant params
  - `GET /lakehouse/api/v1/stats/cost` — cost breakdown with projections
  - `GET /lakehouse/api/v1/stats/compression` — compression trends
  - `GET /lakehouse/api/v1/cardinality/fields` — field cardinality, tenant/sort/limit params

Each handler:
1. Parse query params
2. Read from registry/manifest/labelIndex
3. Compute derived values (compression ratio, cost, etc.)
4. Marshal JSON response

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/stats/ -run TestAPI -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/stats/api.go internal/stats/api_test.go
git commit -m "feat(stats): add 7 JSON API endpoints for tenant stats, cost, cardinality"
```

---

### Task 9: VMUI Tab Injection

**Files:**
- Create: `internal/ui/vmui_inject.go`
- Create: `internal/ui/vmui_inject_test.go`

Standalone — no dependency on other new packages.

- [ ] **Step 1: Write tests**

```go
package ui

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestInjectLakehouseTabHTML(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html; charset=utf-8")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<!DOCTYPE html><html><head><title>VMUI</title></head><body><div id="root"></div></body></html>`))
	})

	handler := InjectLakehouseTab(upstream)
	req := httptest.NewRequest(http.MethodGet, "/vmui/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	if !strings.Contains(body, "vmui-tab.js") {
		t.Fatal("script tag not injected")
	}
	if !strings.Contains(body, `</body>`) {
		t.Fatal("closing body tag missing")
	}
	// Script should be before </body>
	scriptIdx := strings.Index(body, "vmui-tab.js")
	bodyIdx := strings.Index(body, "</body>")
	if scriptIdx > bodyIdx {
		t.Fatal("script should be injected before </body>")
	}
}

func TestInjectLakehouseTabPassthroughNonHTML(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/javascript")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`console.log("hello");`))
	})

	handler := InjectLakehouseTab(upstream)
	req := httptest.NewRequest(http.MethodGet, "/vmui/static/js/main.js", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	if strings.Contains(body, "vmui-tab.js") {
		t.Fatal("should not inject into non-HTML responses")
	}
	if body != `console.log("hello");` {
		t.Fatalf("JS content modified: %s", body)
	}
}

func TestInjectLakehouseTabPreservesStatusCode(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusNotFound)
		_, _ = w.Write([]byte(`<html><body>Not Found</body></html>`))
	})

	handler := InjectLakehouseTab(upstream)
	req := httptest.NewRequest(http.MethodGet, "/vmui/missing", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("status code: want 404, got %d", w.Code)
	}
	if !strings.Contains(w.Body.String(), "vmui-tab.js") {
		t.Fatal("should still inject into HTML 404")
	}
}

func TestInjectLakehouseTabNoBody(t *testing.T) {
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(`<html><head></head></html>`))
	})

	handler := InjectLakehouseTab(upstream)
	req := httptest.NewRequest(http.MethodGet, "/vmui/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	// No </body> to inject before — should pass through unchanged
	if strings.Contains(body, "vmui-tab.js") {
		t.Fatal("should not inject when no </body> tag")
	}
}

func TestInjectLakehouseTabContentLengthUpdated(t *testing.T) {
	html := `<!DOCTYPE html><html><body></body></html>`
	upstream := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/html")
		w.Header().Set("Content-Length", "40")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte(html))
	})

	handler := InjectLakehouseTab(upstream)
	req := httptest.NewRequest(http.MethodGet, "/vmui/", nil)
	w := httptest.NewRecorder()
	handler.ServeHTTP(w, req)

	body := w.Body.String()
	if len(body) == 40 {
		t.Fatal("body length should have increased after injection")
	}
	// Content-Length should be removed (let Go's http set it) or updated
	cl := w.Header().Get("Content-Length")
	if cl != "" && cl == "40" {
		t.Fatal("Content-Length was not updated")
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test ./internal/ui/ -run TestInject -v`
Expected: Compilation error.

- [ ] **Step 3: Implement VMUI injection middleware**

```go
package ui

import (
	"bytes"
	"net/http"
	"strings"
)

const injectScript = `<script src="/lakehouse/ui/vmui-tab.js"></script>`

func InjectLakehouseTab(upstream http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		rec := &responseRecorder{
			ResponseWriter: w,
			body:           &bytes.Buffer{},
		}
		upstream.ServeHTTP(rec, r)

		ct := rec.Header().Get("Content-Type")
		if !strings.Contains(ct, "text/html") {
			w.WriteHeader(rec.statusCode)
			_, _ = w.Write(rec.body.Bytes())
			return
		}

		body := rec.body.String()
		idx := strings.LastIndex(body, "</body>")
		if idx < 0 {
			w.WriteHeader(rec.statusCode)
			_, _ = w.Write(rec.body.Bytes())
			return
		}

		injected := body[:idx] + injectScript + "\n" + body[idx:]
		rec.Header().Del("Content-Length")
		w.WriteHeader(rec.statusCode)
		_, _ = io.WriteString(w, injected)
	})
}

type responseRecorder struct {
	http.ResponseWriter
	statusCode int
	body       *bytes.Buffer
}

func (rr *responseRecorder) WriteHeader(code int) {
	rr.statusCode = code
}

func (rr *responseRecorder) Write(b []byte) (int, error) {
	return rr.body.Write(b)
}
```

- [ ] **Step 4: Run tests**

Run: `GOWORK=off go test ./internal/ui/ -run TestInject -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add internal/ui/vmui_inject.go internal/ui/vmui_inject_test.go
git commit -m "feat(ui): add VMUI tab injection middleware"
```

---

### Task 10: UI Static Handler + Assets

**Files:**
- Create: `internal/ui/ui.go`
- Create: `internal/ui/static/index.html`
- Create: `internal/ui/static/vmui-tab.js`
- Create: `internal/ui/ui_test.go`

- [ ] **Step 1: Write tests**

```go
package ui

import (
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestUIHandlerServesIndex(t *testing.T) {
	handler := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	handler.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/ui/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	ct := w.Header().Get("Content-Type")
	if !strings.Contains(ct, "text/html") {
		t.Fatalf("content type: want text/html, got %s", ct)
	}
	body := w.Body.String()
	if !strings.Contains(body, "Lakehouse Explorer") {
		t.Fatal("index.html should contain 'Lakehouse Explorer'")
	}
}

func TestUIHandlerServesVMUITabJS(t *testing.T) {
	handler := NewHandler(HandlerConfig{Enabled: true})
	mux := http.NewServeMux()
	handler.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/ui/vmui-tab.js", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
	body := w.Body.String()
	if !strings.Contains(body, "Lakehouse") {
		t.Fatal("vmui-tab.js should contain 'Lakehouse'")
	}
}

func TestUIHandlerDisabled(t *testing.T) {
	handler := NewHandler(HandlerConfig{Enabled: false})
	mux := http.NewServeMux()
	handler.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/ui/", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusNotFound {
		t.Fatalf("want 404 when disabled, got %d", w.Code)
	}
}
```

- [ ] **Step 2: Run tests to verify they fail**

Run: `GOWORK=off go test ./internal/ui/ -run TestUIHandler -v`
Expected: Compilation error.

- [ ] **Step 3: Create static assets**

Create `internal/ui/static/vmui-tab.js` (~50 lines):
```javascript
(function() {
  'use strict';
  var LAKEHOUSE_URL = '/lakehouse/ui/';
  var TAB_LABEL = 'Lakehouse';

  function injectTab() {
    var nav = document.querySelector('.vm-header-nav, [class*="headerNav"], nav');
    if (!nav) return false;
    if (document.getElementById('lakehouse-tab')) return true;

    var items = nav.querySelectorAll('a, button, [role="tab"]');
    if (items.length === 0) return false;

    var sample = items[items.length - 1];
    var tab = sample.cloneNode(true);
    tab.id = 'lakehouse-tab';
    tab.textContent = TAB_LABEL;
    tab.href = '#lakehouse';
    tab.classList.remove('active', 'vm-header-nav-item_active');

    tab.addEventListener('click', function(e) {
      e.preventDefault();
      items.forEach(function(it) {
        it.classList.remove('active', 'vm-header-nav-item_active');
      });
      tab.classList.add('active', 'vm-header-nav-item_active');

      var main = document.querySelector('.vm-container, [class*="content"], main, #root > div > div:last-child');
      if (main) {
        main.innerHTML = '<iframe src="' + LAKEHOUSE_URL + '" style="width:100%;height:calc(100vh - 60px);border:none;"></iframe>';
      }
    });

    nav.appendChild(tab);
    return true;
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function() { injectTab(); });
  } else {
    injectTab();
  }

  var observer = new MutationObserver(function() { injectTab(); });
  observer.observe(document.body, { childList: true, subtree: true });
})();
```

Create `internal/ui/static/index.html` — full Preact+uPlot+HTM single-file app with 3 tabs (Storage Overview, Tenants, Cardinality Explorer). Uses CDN imports:

```html
<!DOCTYPE html>
<html lang="en">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Lakehouse Explorer</title>
  <script type="module" crossorigin src="https://esm.sh/preact@10.19.3"></script>
  <script type="module" crossorigin src="https://esm.sh/preact@10.19.3/hooks"></script>
  <script type="module" crossorigin src="https://esm.sh/htm@3.1.1/preact"></script>
  <link rel="stylesheet" href="https://cdn.jsdelivr.net/npm/uplot@1.6.30/dist/uPlot.min.css">
  <script src="https://cdn.jsdelivr.net/npm/uplot@1.6.30/dist/uPlot.iife.min.js"></script>
  <style>
    /* ... minimal CSS styling matching VMUI dark/light themes ... */
    :root { --bg: #fff; --fg: #1a1a2e; --card: #f8f9fa; --border: #dee2e6; --accent: #4361ee; }
    @media (prefers-color-scheme: dark) { :root { --bg: #1a1a2e; --fg: #e8e8e8; --card: #16213e; --border: #333; --accent: #4361ee; } }
    [data-theme="dark"] { --bg: #1a1a2e; --fg: #e8e8e8; --card: #16213e; --border: #333; }
    [data-theme="light"] { --bg: #fff; --fg: #1a1a2e; --card: #f8f9fa; --border: #dee2e6; }
    * { margin: 0; padding: 0; box-sizing: border-box; }
    body { font-family: -apple-system, BlinkMacSystemFont, sans-serif; background: var(--bg); color: var(--fg); }
    .container { max-width: 1400px; margin: 0 auto; padding: 16px; }
    .tabs { display: flex; gap: 8px; margin-bottom: 16px; border-bottom: 2px solid var(--border); }
    .tab { padding: 8px 16px; cursor: pointer; border: none; background: none; color: var(--fg); font-size: 14px; }
    .tab.active { border-bottom: 2px solid var(--accent); color: var(--accent); font-weight: 600; }
    .cards { display: grid; grid-template-columns: repeat(auto-fit, minmax(200px, 1fr)); gap: 12px; margin-bottom: 16px; }
    .card { background: var(--card); border: 1px solid var(--border); border-radius: 8px; padding: 16px; }
    .card h3 { font-size: 12px; text-transform: uppercase; opacity: 0.6; margin-bottom: 4px; }
    .card .value { font-size: 24px; font-weight: 700; }
    table { width: 100%; border-collapse: collapse; }
    th, td { text-align: left; padding: 8px 12px; border-bottom: 1px solid var(--border); }
    th { font-size: 12px; text-transform: uppercase; opacity: 0.6; cursor: pointer; }
    .badge { display: inline-block; padding: 2px 6px; border-radius: 4px; font-size: 11px; }
    .badge-warn { background: #ff6b35; color: white; }
    .badge-ok { background: #4caf50; color: white; }
    .refresh-bar { display: flex; align-items: center; gap: 8px; margin-bottom: 12px; font-size: 13px; }
  </style>
</head>
<body>
  <div id="app"></div>
  <script type="module">
    import { h, render } from 'https://esm.sh/preact@10.19.3';
    import { useState, useEffect, useCallback } from 'https://esm.sh/preact@10.19.3/hooks';
    import { html } from 'https://esm.sh/htm@3.1.1/preact';

    const API = '/lakehouse/api/v1';
    const fmtBytes = (b) => {
      if (b >= 1e12) return (b/1e12).toFixed(1) + ' TB';
      if (b >= 1e9) return (b/1e9).toFixed(1) + ' GB';
      if (b >= 1e6) return (b/1e6).toFixed(1) + ' MB';
      if (b >= 1e3) return (b/1e3).toFixed(1) + ' KB';
      return b + ' B';
    };
    const fmtNum = (n) => n != null ? n.toLocaleString() : '—';
    const fmtUSD = (n) => n != null ? '$' + n.toFixed(2) : '—';

    function App() {
      const [tab, setTab] = useState('overview');
      const [refreshInterval, setRefreshInterval] = useState(0);

      return html`
        <div class="container">
          <h1 style="margin-bottom:8px">Lakehouse Explorer</h1>
          <div class="refresh-bar">
            <span>Auto-refresh:</span>
            ${[0,10,30,60].map(s => html`
              <button class=${refreshInterval===s?'tab active':'tab'} onClick=${()=>setRefreshInterval(s)}>
                ${s===0?'Off':s+'s'}
              </button>
            `)}
          </div>
          <div class="tabs">
            <button class=${tab==='overview'?'tab active':'tab'} onClick=${()=>setTab('overview')}>Storage Overview</button>
            <button class=${tab==='tenants'?'tab active':'tab'} onClick=${()=>setTab('tenants')}>Tenants</button>
            <button class=${tab==='cardinality'?'tab active':'tab'} onClick=${()=>setTab('cardinality')}>Cardinality Explorer</button>
          </div>
          ${tab==='overview' && html`<${OverviewTab} refresh=${refreshInterval} />`}
          ${tab==='tenants' && html`<${TenantsTab} refresh=${refreshInterval} />`}
          ${tab==='cardinality' && html`<${CardinalityTab} refresh=${refreshInterval} />`}
        </div>
      `;
    }

    function OverviewTab({refresh}) {
      const [data, setData] = useState(null);
      const load = useCallback(() => fetch(API+'/stats/overview').then(r=>r.json()).then(setData), []);
      useEffect(() => { load(); if(refresh>0){const id=setInterval(load,refresh*1000);return()=>clearInterval(id);} }, [refresh]);
      if (!data) return html`<p>Loading...</p>`;
      return html`
        <div class="cards">
          <div class="card"><h3>Total Files</h3><div class="value">${fmtNum(data.total_files)}</div></div>
          <div class="card"><h3>Total Size</h3><div class="value">${fmtBytes(data.total_bytes)}</div></div>
          <div class="card"><h3>Compression</h3><div class="value">${data.avg_compression_ratio?.toFixed(1)}x</div></div>
          <div class="card"><h3>Rows</h3><div class="value">${fmtNum(data.total_rows)}</div></div>
          <div class="card"><h3>Tenants</h3><div class="value">${fmtNum(data.tenant_count)}</div></div>
          <div class="card"><h3>Partitions</h3><div class="value">${fmtNum(data.partition_count)}</div></div>
        </div>
      `;
    }

    function TenantsTab({refresh}) {
      const [data, setData] = useState(null);
      const [sort, setSort] = useState('bytes');
      const load = useCallback(() => fetch(API+'/tenants?sort='+sort).then(r=>r.json()).then(setData), [sort]);
      useEffect(() => { load(); if(refresh>0){const id=setInterval(load,refresh*1000);return()=>clearInterval(id);} }, [refresh,sort]);
      if (!data) return html`<p>Loading...</p>`;
      return html`
        <table>
          <thead><tr>
            <th onClick=${()=>setSort('tenant')}>Tenant</th>
            <th onClick=${()=>setSort('files')}>Files</th>
            <th onClick=${()=>setSort('bytes')}>Size</th>
            <th onClick=${()=>setSort('rows')}>Rows</th>
            <th>Compression</th>
            <th onClick=${()=>setSort('cost')}>Monthly Cost</th>
            <th>Last Write</th>
          </tr></thead>
          <tbody>
            ${(data.tenants||[]).map(t => html`
              <tr>
                <td>${t.account_id}/${t.project_id}</td>
                <td>${fmtNum(t.total_files)}</td>
                <td>${fmtBytes(t.total_bytes)}</td>
                <td>${fmtNum(t.total_rows)}</td>
                <td>${t.compression_ratio?.toFixed(1)}x</td>
                <td>${fmtUSD(t.monthly_cost_usd)}</td>
                <td>${t.last_write_at ? new Date(t.last_write_at).toLocaleString() : '—'}</td>
              </tr>
            `)}
          </tbody>
        </table>
      `;
    }

    function CardinalityTab({refresh}) {
      const [data, setData] = useState(null);
      const load = useCallback(() => fetch(API+'/cardinality/fields?sort=cardinality').then(r=>r.json()).then(setData), []);
      useEffect(() => { load(); if(refresh>0){const id=setInterval(load,refresh*1000);return()=>clearInterval(id);} }, [refresh]);
      if (!data) return html`<p>Loading...</p>`;
      return html`
        <div style="margin-bottom:8px">
          <strong>${data.total_fields}</strong> fields (<strong>${data.total_promoted}</strong> promoted, <strong>${data.total_map}</strong> map)
        </div>
        <table>
          <thead><tr>
            <th>Field</th>
            <th>Cardinality</th>
            <th>Type</th>
            <th>Bloom</th>
          </tr></thead>
          <tbody>
            ${(data.fields||[]).map(f => html`
              <tr>
                <td>${f.name} ${f.cardinality > (data.cardinality_threshold||10000) ? html`<span class="badge badge-warn">HIGH</span>` : ''}</td>
                <td>${fmtNum(f.cardinality)}</td>
                <td><span class="badge badge-ok">${f.type}</span></td>
                <td>${f.has_bloom ? '✓' : '—'}</td>
              </tr>
            `)}
          </tbody>
        </table>
      `;
    }

    render(html`<${App} />`, document.getElementById('app'));
  </script>
</body>
</html>
```

- [ ] **Step 4: Implement UI handler**

```go
package ui

import (
	"embed"
	"io/fs"
	"net/http"
)

//go:embed static
var staticFiles embed.FS

type HandlerConfig struct {
	Enabled bool
}

type Handler struct {
	cfg HandlerConfig
}

func NewHandler(cfg HandlerConfig) *Handler {
	return &Handler{cfg: cfg}
}

func (h *Handler) Register(mux *http.ServeMux) {
	if !h.cfg.Enabled {
		mux.HandleFunc("/lakehouse/ui/", func(w http.ResponseWriter, r *http.Request) {
			http.NotFound(w, r)
		})
		return
	}

	sub, _ := fs.Sub(staticFiles, "static")
	fileServer := http.FileServer(http.FS(sub))
	mux.Handle("/lakehouse/ui/", http.StripPrefix("/lakehouse/ui/", fileServer))
}
```

- [ ] **Step 5: Run tests**

Run: `GOWORK=off go test ./internal/ui/ -v`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
git add internal/ui/ui.go internal/ui/ui_test.go internal/ui/vmui_inject.go internal/ui/vmui_inject_test.go internal/ui/static/index.html internal/ui/static/vmui-tab.js
git commit -m "feat(ui): add Lakehouse Explorer UI with VMUI tab injection"
```

---

### Task 11: Wire Everything into Main Binary (Logs)

**Files:**
- Modify: `cmd/lakehouse-logs/main.go`
- Modify: `internal/storage/parquets3/storage.go` (add SetTenantRegistry, expose LabelIndex)

Depends on: All previous tasks.

- [ ] **Step 1: Add registry accessor to Storage**

In `internal/storage/parquets3/storage.go`, add:

```go
func (s *Storage) LabelIndex() *cache.LabelIndex {
	return s.labelIndex
}
```

- [ ] **Step 2: Wire TenantRegistry, API, UI, Sync into main.go**

In `cmd/lakehouse-logs/main.go`, in the `run()` function (after tombstone loading, before `newMux`):

```go
// --- Tenant Stats ---
var tenantRegistry *stats.TenantRegistry
var cardLimiter *stats.CardinalityLimiter
var syncPusher *stats.SyncPusher

if cfg.Stats.Enabled {
	tenantRules := make(map[string][]config.LifecycleRuleConfig)
	for _, kt := range cfg.Tenant.KnownTenants {
		key := kt.AccountID + "/" + kt.ProjectID
		if len(kt.LifecycleRules) > 0 {
			tenantRules[key] = kt.LifecycleRules
		}
	}
	classTracker := stats.NewStorageClassTracker(cfg.Stats.S3LifecycleRules, tenantRules)
	costCalc := stats.NewCostCalculator(cfg.Stats.S3PricePerGB, cfg.Stats.S3RequestPrices)

	tenantRegistry = stats.NewTenantRegistry(hostname(), cfg.Stats.S3LifecycleRules, cfg.Stats.S3PricePerGB)
	cardLimiter = stats.NewCardinalityLimiter(cfg.Stats.MetricsCardinalityLimit)

	// Load S3 snapshot on startup
	snapshotKey := cfg.Stats.SnapshotPrefix + "/" + hostname() + ".json.zst"
	if data, err := store.Pool().Download(context.Background(), snapshotKey); err == nil {
		if err := tenantRegistry.LoadSnapshot(hostname(), data); err != nil {
			logger.Warnf("failed to load stats snapshot: %s", err)
		} else {
			logger.Infof("loaded tenant stats from S3 snapshot; tenants=%d", tenantRegistry.TenantCount())
		}
	}

	// Peer sync pusher
	if cfg.Discovery.PeerHeadlessService != "" {
		disc := store.Discovery()
		syncPusher = stats.NewSyncPusher(stats.SyncPusherConfig{
			Registry: tenantRegistry,
			GetPeers: func() []string { return disc.GetPeers() },
			AuthKey:  cfg.Peer.AuthKey,
			SelfAddr: addr,
			Compress: cfg.Stats.PushCompression,
		})
	}

	logger.Infof("tenant stats enabled; cardinality_limit=%d, push_interval=%v",
		cfg.Stats.MetricsCardinalityLimit, cfg.Stats.PushInterval)
}
```

In `newMux()`, add handler registration (after existing handlers):

```go
// Tenant stats API + UI
if cfg.Stats.Enabled && tenantRegistry != nil {
	classTracker := stats.NewStorageClassTracker(cfg.Stats.S3LifecycleRules, nil)
	costCalc := stats.NewCostCalculator(cfg.Stats.S3PricePerGB, cfg.Stats.S3RequestPrices)

	statsAPI := stats.NewAPI(stats.APIConfig{
		Registry:     tenantRegistry,
		Manifest:     store.Manifest(),
		CostCalc:     costCalc,
		ClassTracker: classTracker,
		LabelIndex:   store.LabelIndex(),
		Mode:         "logs",
		Bucket:       cfg.S3.Bucket,
	})
	statsAPI.Register(mux)

	syncHandler := stats.NewSyncHandler(tenantRegistry, cfg.Peer.AuthKey)
	mux.Handle("/internal/stats/sync", syncHandler)
}

if cfg.UI.Enabled {
	uiHandler := ui.NewHandler(ui.HandlerConfig{Enabled: true})
	uiHandler.Register(mux)
}
```

Update `newMux` signature to accept tenantRegistry, cardLimiter:

```go
func newMux(cfg *config.Config, store *parquets3.Storage, sm *startup.Manager,
	tombstoneStore *delete.TombstoneStore, detector *delete.StorageClassDetector,
	tenantRegistry *stats.TenantRegistry, cardLimiter *stats.CardinalityLimiter) *http.ServeMux {
```

Add background loops in `run()` (after mux creation):

```go
// Stats background loops
if cfg.Stats.Enabled && tenantRegistry != nil {
	// Peer push loop
	if syncPusher != nil {
		go func() {
			ticker := time.NewTicker(cfg.Stats.PushInterval)
			defer ticker.Stop()
			pushCount := 0
			for {
				select {
				case <-ticker.C:
					pushCount++
					var err error
					if pushCount >= cfg.Stats.MaxDeltaCount {
						err = syncPusher.PushFull(context.Background())
						pushCount = 0
					} else {
						err = syncPusher.PushDelta(context.Background())
					}
					if err != nil {
						logger.Warnf("stats push error: %s", err)
					}
				case <-stopCh:
					return
				}
			}
		}()
	}

	// S3 snapshot loop
	go func() {
		ticker := time.NewTicker(cfg.Stats.SnapshotInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				data, err := tenantRegistry.MarshalSnapshot()
				if err != nil {
					logger.Warnf("stats snapshot marshal error: %s", err)
					continue
				}
				snapshotKey := cfg.Stats.SnapshotPrefix + "/" + hostname() + ".json.zst"
				if err := store.Pool().Upload(context.Background(), snapshotKey, data); err != nil {
					logger.Warnf("stats snapshot upload error: %s", err)
				}
			case <-stopCh:
				return
			}
		}
	}()

	// Prometheus metrics update loop
	go func() {
		ticker := time.NewTicker(15 * time.Second)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				updatePrometheusMetrics(tenantRegistry, cardLimiter)
			case <-stopCh:
				return
			}
		}
	}()
}
```

Add helper function:

```go
func updatePrometheusMetrics(reg *stats.TenantRegistry, limiter *stats.CardinalityLimiter) {
	agg := reg.GlobalAggregates()
	metrics.StorageFilesTotal.Set(agg.TotalFiles)
	metrics.StorageBytesTotal.Set(agg.TotalBytes)
	metrics.StorageRawBytesTotal.Set(agg.RawBytes)
	metrics.StorageRowsTotal.Set(agg.TotalRows)
	metrics.StorageTenantsTotal.Set(int64(agg.TenantCount))

	if agg.TotalBytes > 0 && agg.RawBytes > 0 {
		metrics.StorageCompressionRatio.Set(float64(agg.RawBytes) / float64(agg.TotalBytes))
	}

	for class, bytes := range agg.BytesByClass {
		metrics.StorageBytesByClass.Set(class, bytes)
	}
	for class, files := range agg.FilesByClass {
		metrics.StorageFilesByClass.Set(class, files)
	}

	if limiter != nil {
		metrics.MetricsCardinalityLimit.Set(int64(limiter.TrackedCount()))
		metrics.MetricsCardinalityTracked.Set(int64(limiter.TrackedCount()))
	}

	for _, ts := range reg.All() {
		tenant := ts.AccountID + "/" + ts.ProjectID
		if limiter != nil && !limiter.Allow(tenant) {
			continue
		}
		metrics.TenantFiles.Set(tenant, ts.TotalFiles)
		metrics.TenantBytes.Set(tenant, ts.TotalBytes)
		metrics.TenantRawBytes.Set(tenant, ts.RawBytes)
		if !ts.LastWriteAt.IsZero() {
			metrics.TenantLastWriteTimestamp.Set(tenant, ts.LastWriteAt.Unix())
		}
		if !ts.LastQueryAt.IsZero() {
			metrics.TenantLastQueryTimestamp.Set(tenant, ts.LastQueryAt.Unix())
		}
	}
}
```

- [ ] **Step 3: Add imports**

Add to imports in `cmd/lakehouse-logs/main.go`:

```go
"github.com/ReliablyObserve/victoria-lakehouse/internal/stats"
"github.com/ReliablyObserve/victoria-lakehouse/internal/ui"
```

- [ ] **Step 4: Verify compilation**

Run: `GOWORK=off go build ./cmd/lakehouse-logs/`
Expected: Build succeeds.

- [ ] **Step 5: Run all tests for regressions**

Run: `GOWORK=off go test ./internal/... -count=1`
Expected: All tests pass.

- [ ] **Step 6: Commit**

```bash
git add cmd/lakehouse-logs/main.go internal/storage/parquets3/storage.go
git commit -m "feat: wire tenant stats, API, UI, sync into lakehouse-logs main"
```

---

### Task 12: Mirror to Traces Module

**Files:**
- Modify: `lakehouse-traces/main.go`

The traces module uses `replace` directive to import root module's `internal/*` packages. So `internal/stats/` and `internal/ui/` are automatically available. Only the `main.go` wiring needs mirroring.

- [ ] **Step 1: Add the same wiring to lakehouse-traces/main.go**

Mirror the exact same registry init, API handler registration, sync setup, UI handler, background loops, and shutdown logic from Task 11, but with:
- Mode: `"traces"` instead of `"logs"`
- Port: `10428` instead of `9428`
- Info gauge label: `"mode": "traces"`

- [ ] **Step 2: Verify compilation**

Run: `GOWORK=off go build ./lakehouse-traces/`
Expected: Build succeeds.

- [ ] **Step 3: Run traces module tests**

Run: `cd lakehouse-traces && GOWORK=off go test ./... -count=1`
Expected: All tests pass.

- [ ] **Step 4: Commit**

```bash
git add lakehouse-traces/main.go
git commit -m "feat: wire tenant stats, API, UI into lakehouse-traces main"
```

---

### Task 13: Integration Tests

**Files:**
- Create: `internal/stats/integration_test.go`

Full round-trip tests that verify all components work together.

- [ ] **Step 1: Write integration tests**

```go
package stats

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func TestIntegrationFullRoundTrip(t *testing.T) {
	// 1. Create registry, tracker, cost calc, limiter
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
	}
	prices := map[string]float64{
		"STANDARD":    0.023,
		"STANDARD_IA": 0.0125,
	}
	reg := NewTenantRegistry("node-1", rules, prices)
	limiter := NewCardinalityLimiter(100)
	tracker := NewStorageClassTracker(rules, nil)
	calc := NewCostCalculator(prices, nil)
	labelIdx := cache.NewLabelIndex()
	labelIdx.Add("service.name", []string{"api", "web"})

	m := manifest.New("obs-archive", "logs/")

	// 2. Simulate writes
	reg.RecordWrite("100/1", 50<<20, 100<<20, 50000, "STANDARD")
	reg.RecordWrite("100/1", 30<<20, 60<<20, 30000, "STANDARD")
	reg.RecordWrite("200/5", 10<<20, 20<<20, 10000, "STANDARD_IA")
	reg.RecordQuery("100/1")

	// 3. Setup API
	api := NewAPI(APIConfig{
		Registry:     reg,
		Manifest:     m,
		CostCalc:     calc,
		ClassTracker: tracker,
		LabelIndex:   labelIdx,
		Mode:         "logs",
		Bucket:       "obs-archive",
	})
	mux := http.NewServeMux()
	api.Register(mux)

	// 4. Test tenants endpoint
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/lakehouse/api/v1/tenants", nil))
	if w.Code != 200 {
		t.Fatalf("tenants: %d", w.Code)
	}
	var tenantsResp TenantsResponse
	json.NewDecoder(w.Body).Decode(&tenantsResp)
	if tenantsResp.TotalTenants != 2 {
		t.Fatalf("want 2 tenants, got %d", tenantsResp.TotalTenants)
	}

	// 5. Test overview endpoint
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/overview", nil))
	if w.Code != 200 {
		t.Fatalf("overview: %d", w.Code)
	}

	// 6. Test cost endpoint
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/lakehouse/api/v1/stats/cost", nil))
	if w.Code != 200 {
		t.Fatalf("cost: %d", w.Code)
	}

	// 7. Test cardinality
	w = httptest.NewRecorder()
	mux.ServeHTTP(w, httptest.NewRequest("GET", "/lakehouse/api/v1/cardinality/fields", nil))
	if w.Code != 200 {
		t.Fatalf("cardinality: %d", w.Code)
	}

	// 8. Verify limiter works
	if !limiter.Allow("100/1") {
		t.Fatal("limiter should allow tracked tenant")
	}
	if limiter.TrackedCount() != 1 {
		t.Fatalf("tracked: want 1, got %d", limiter.TrackedCount())
	}
}

func TestIntegrationCRDTPeerConvergence(t *testing.T) {
	// Simulate 3-node fleet convergence
	reg1 := NewTenantRegistry("node-1", nil, nil)
	reg2 := NewTenantRegistry("node-2", nil, nil)
	reg3 := NewTenantRegistry("node-3", nil, nil)

	// Each node writes different tenants
	reg1.RecordWrite("100/1", 1024, 2048, 100, "STANDARD")
	reg2.RecordWrite("200/5", 2048, 4096, 200, "STANDARD")
	reg3.RecordWrite("100/1", 512, 1024, 50, "STANDARD") // same tenant, different node

	// Pairwise sync
	d1 := reg1.BuildDelta(0)
	d2 := reg2.BuildDelta(0)
	d3 := reg3.BuildDelta(0)

	reg1.Merge(d2)
	reg1.Merge(d3)
	reg2.Merge(d1)
	reg2.Merge(d3)
	reg3.Merge(d1)
	reg3.Merge(d2)

	// All registries should converge
	for i, reg := range []*TenantRegistry{reg1, reg2, reg3} {
		all := reg.All()
		if len(all) != 2 {
			t.Fatalf("node %d: want 2 tenants, got %d", i+1, len(all))
		}
		ts := reg.Get("100/1")
		if ts == nil {
			t.Fatalf("node %d: tenant 100/1 missing", i+1)
		}
		// node-1 contributed 1024, node-3 contributed 512
		if ts.TotalBytes != 1536 {
			t.Fatalf("node %d: 100/1 bytes want 1536, got %d", i+1, ts.TotalBytes)
		}
	}
}

func TestIntegrationSnapshotRecovery(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	reg.RecordWrite("200/5", 2048, 4096, 1000, "GLACIER")

	// Snapshot
	snap, err := reg.MarshalSnapshot()
	if err != nil {
		t.Fatal(err)
	}

	// Simulate restart: fresh registry loads snapshot
	reg2 := NewTenantRegistry("node-1", nil, nil)
	if err := reg2.LoadSnapshot("node-1", snap); err != nil {
		t.Fatal(err)
	}

	// New writes on top of recovered state
	reg2.RecordWrite("300/1", 512, 1024, 100, "STANDARD")

	all := reg2.All()
	if len(all) != 3 {
		t.Fatalf("want 3 tenants, got %d", len(all))
	}
}

func TestIntegrationStorageClassWithCost(t *testing.T) {
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
		{TransitionDays: 90, StorageClass: "GLACIER"},
	}
	prices := map[string]float64{
		"STANDARD":    0.023,
		"STANDARD_IA": 0.0125,
		"GLACIER":     0.0036,
	}
	tracker := NewStorageClassTracker(rules, nil)
	calc := NewCostCalculator(prices, nil)

	now := time.Now()
	byClass := map[string]int64{
		tracker.PredictClass(now.Add(-10*24*time.Hour), now): 50 << 30,  // 10 days = STANDARD
		tracker.PredictClass(now.Add(-60*24*time.Hour), now): 30 << 30,  // 60 days = STANDARD_IA
		tracker.PredictClass(now.Add(-120*24*time.Hour), now): 20 << 30, // 120 days = GLACIER
	}

	total := calc.TotalMonthlyCost(byClass)
	if total <= 0 {
		t.Fatalf("total cost should be positive: $%.4f", total)
	}

	savings := calc.LifecycleSavings(byClass)
	if savings <= 0 {
		t.Fatalf("lifecycle savings should be positive: $%.4f", savings)
	}
}
```

- [ ] **Step 2: Run integration tests**

Run: `GOWORK=off go test ./internal/stats/ -run TestIntegration -v`
Expected: PASS

- [ ] **Step 3: Commit**

```bash
git add internal/stats/integration_test.go
git commit -m "test(stats): add integration tests for full round-trip, CRDT convergence, recovery"
```

---

### Task 14: Comprehensive Regression & Coverage Tests

**Files:**
- Modify: `internal/stats/registry_test.go` (add edge cases)
- Modify: `internal/stats/api_test.go` (add error paths)
- Modify: `internal/ui/vmui_inject_test.go` (add edge cases)
- Create: `internal/stats/storageclass_bench_test.go`

- [ ] **Step 1: Add registry edge case tests**

```go
// Add to internal/stats/registry_test.go

func TestRegistryGetNonExistent(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	ts := reg.Get("nonexistent/tenant")
	if ts != nil {
		t.Fatal("should return nil for unknown tenant")
	}
}

func TestRegistryEmptyDelta(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	delta := reg.BuildDelta(0)
	if len(delta.Tenants) != 0 {
		t.Fatalf("empty registry should produce empty delta")
	}
}

func TestRegistryMergeEmptyDelta(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")
	bytesBefore := reg.Get("100/1").TotalBytes

	reg.Merge(&TenantDelta{NodeID: "node-2", Tenants: map[string]*TenantStats{}})

	bytesAfter := reg.Get("100/1").TotalBytes
	if bytesBefore != bytesAfter {
		t.Fatalf("merge empty delta shouldn't change stats: %d vs %d", bytesBefore, bytesAfter)
	}
}

func TestRegistryRecordWriteZeroValues(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 0, 0, 0, "STANDARD")

	ts := reg.Get("100/1")
	if ts == nil {
		t.Fatal("should create tenant even with zero values")
	}
	if ts.TotalFiles != 1 {
		t.Fatalf("files: want 1, got %d", ts.TotalFiles)
	}
}

func TestRegistryMergeSameNodeIdempotent(t *testing.T) {
	reg := NewTenantRegistry("node-1", nil, nil)
	reg.RecordWrite("100/1", 1024, 2048, 500, "STANDARD")

	delta := reg.BuildDelta(0)
	reg.Merge(delta) // merge own delta

	ts := reg.Get("100/1")
	if ts.TotalBytes != 1024 {
		t.Fatalf("self-merge should be idempotent: want 1024, got %d", ts.TotalBytes)
	}
}
```

- [ ] **Step 2: Add API error path tests**

```go
// Add to internal/stats/api_test.go

func TestAPIMethodNotAllowed(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodPost, "/lakehouse/api/v1/tenants", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusMethodNotAllowed {
		t.Fatalf("want 405, got %d", w.Code)
	}
}

func TestAPITenantsInvalidSort(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/tenants?sort=invalid", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// Should still return 200 with default sort
	if w.Code != http.StatusOK {
		t.Fatalf("invalid sort should fall back to default, got %d", w.Code)
	}
}

func TestAPICardinalityWithTenantFilter(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/cardinality/fields?tenant=100/1", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	if w.Code != http.StatusOK {
		t.Fatalf("want 200, got %d", w.Code)
	}
}

func TestAPIIngestionInvalidPeriod(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	req := httptest.NewRequest(http.MethodGet, "/lakehouse/api/v1/stats/ingestion?period=invalid", nil)
	w := httptest.NewRecorder()
	mux.ServeHTTP(w, req)

	// Should fall back to default period
	if w.Code != http.StatusOK {
		t.Fatalf("invalid period should fall back, got %d", w.Code)
	}
}

func TestAPIResponseContentType(t *testing.T) {
	api, _, _ := setupTestAPI(t)
	mux := http.NewServeMux()
	api.Register(mux)

	endpoints := []string{
		"/lakehouse/api/v1/tenants",
		"/lakehouse/api/v1/stats/overview",
		"/lakehouse/api/v1/stats/cost",
		"/lakehouse/api/v1/stats/ingestion",
		"/lakehouse/api/v1/stats/compression",
		"/lakehouse/api/v1/cardinality/fields",
	}
	for _, ep := range endpoints {
		w := httptest.NewRecorder()
		mux.ServeHTTP(w, httptest.NewRequest("GET", ep, nil))
		ct := w.Header().Get("Content-Type")
		if ct != "application/json" {
			t.Errorf("%s: content-type want application/json, got %s", ep, ct)
		}
	}
}
```

- [ ] **Step 3: Add storage class benchmark**

```go
// Create internal/stats/storageclass_bench_test.go

package stats

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

func BenchmarkPredictClass(b *testing.B) {
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
		{TransitionDays: 90, StorageClass: "GLACIER"},
		{TransitionDays: 365, StorageClass: "DEEP_ARCHIVE"},
	}
	tracker := NewStorageClassTracker(rules, nil)
	now := time.Now()
	created := now.Add(-60 * 24 * time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.PredictClass(created, now)
	}
}

func BenchmarkNearBoundary(b *testing.B) {
	rules := []config.LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "STANDARD_IA"},
		{TransitionDays: 90, StorageClass: "GLACIER"},
	}
	tracker := NewStorageClassTracker(rules, nil)
	now := time.Now()
	created := now.Add(-28 * 24 * time.Hour)

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		tracker.NearBoundary(created, now)
	}
}
```

- [ ] **Step 4: Run all tests + check coverage**

Run: `GOWORK=off go test ./internal/stats/ -v -count=1 -coverprofile=coverage.out && go tool cover -func=coverage.out`
Expected: All pass, coverage > 80%.

Run: `GOWORK=off go test ./internal/ui/ -v -count=1`
Expected: All pass.

Run: `GOWORK=off go test ./internal/cache/ -v -count=1`
Expected: All pass (including new per-tenant tests).

Run: `GOWORK=off go test ./internal/manifest/ -v -count=1`
Expected: All pass (including FileInfo backward compat tests).

- [ ] **Step 5: Commit**

```bash
git add internal/stats/registry_test.go internal/stats/api_test.go internal/stats/storageclass_bench_test.go internal/ui/vmui_inject_test.go
git commit -m "test: add comprehensive regression, edge case, and benchmark tests"
```

---

### Task 15: Final Verification & Build Check

- [ ] **Step 1: Full test suite**

Run: `GOWORK=off go test ./internal/... -count=1`
Expected: All tests pass.

- [ ] **Step 2: Build both binaries**

Run: `GOWORK=off go build ./cmd/lakehouse-logs/ && GOWORK=off go build ./lakehouse-traces/`
Expected: Both build successfully.

- [ ] **Step 3: Helm lint**

Run: `/opt/homebrew/bin/helm lint charts/victoria-lakehouse/`
Expected: 0 failures.

- [ ] **Step 4: Full test suite including traces module**

Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && GOWORK=off go test ./... -count=1`
Expected: All pass.

- [ ] **Step 5: Coverage report**

Run: `GOWORK=off go test ./internal/stats/ -coverprofile=/tmp/stats-coverage.out && go tool cover -func=/tmp/stats-coverage.out | tail -1`
Expected: Total coverage > 80%.

- [ ] **Step 6: Commit and push**

```bash
git push origin feat/tenant-stats-storage-metrics
```

---

## Verification Checklist

| Check | Command | Expected |
|---|---|---|
| Stats package tests | `GOWORK=off go test ./internal/stats/ -v` | All pass |
| UI package tests | `GOWORK=off go test ./internal/ui/ -v` | All pass |
| Manifest regression | `GOWORK=off go test ./internal/manifest/ -v` | All pass |
| Cache regression | `GOWORK=off go test ./internal/cache/ -v` | All pass |
| Logs binary builds | `GOWORK=off go build ./cmd/lakehouse-logs/` | OK |
| Traces binary builds | `GOWORK=off go build ./lakehouse-traces/` | OK |
| Helm lint | `helm lint charts/victoria-lakehouse/` | 0 failures |
| Stats coverage | `go test ./internal/stats/ -coverprofile` | > 80% |
| Full regression | `GOWORK=off go test ./internal/... -count=1` | All pass |
