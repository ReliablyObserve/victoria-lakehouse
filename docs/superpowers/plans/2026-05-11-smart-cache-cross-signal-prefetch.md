# Smart Cache & Cross-Signal Prefetch — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Add intelligent caching with TTL, cross-signal prefetch between lakehouse-logs and lakehouse-traces, parallel query execution, hash-routed cache ownership, and cache sizing metrics — all behind the Storage interface, invisible to VL/VT.

**Architecture:** SmartCacheController wraps existing L1/L2/L3 caches with unified TTL, hot detection, pin tracking, and hash-routed ownership. CrossSignalClient enables bidirectional prefetch hints between separate logs and traces deployments. RunQuery gains parallel file workers and trace_id extraction for prefetch. Two Go modules (root for logs, lakehouse-traces/ for traces) both need identical changes.

**Tech Stack:** Go 1.26+, parquet-go/parquet-go, VictoriaMetrics/metrics, existing L1 LRU (`internal/cache/lru.go`), L2 disk cache (`internal/cache/disk.go`), L3 peer cache (`internal/peercache/`), prefetch engine (`internal/prefetch/prefetch.go`)

**Spec:** `docs/superpowers/specs/2026-05-11-smart-cache-cross-signal-prefetch-design.md`

**Two-module note:** Victoria Lakehouse has two Go modules. The root module (`go.mod`) builds `cmd/lakehouse-logs/main.go`. The `lakehouse-traces/` directory has its own `go.mod` and builds `lakehouse-traces/main.go`. Both share `internal/` packages from the root module. The traces module has its OWN copy of `internal/storage/parquets3/` at `lakehouse-traces/internal/storage/parquets3/`. Changes to shared packages (`internal/smartcache/`, `internal/crosssignal/`, `internal/config/`, `internal/metrics/`, `internal/prefetch/`) only need to be made once in the root module — both binaries import them. Changes to `internal/storage/parquets3/` must be made in BOTH locations.

---

## File Structure

| File | Responsibility |
|------|---------------|
| **Create:** `internal/smartcache/metadata.go` | `EntryMeta` struct, `MetadataMap` (thread-safe in-memory map), JSON snapshot persistence |
| **Create:** `internal/smartcache/metadata_test.go` | Metadata CRUD, snapshot save/load, reconciliation tests |
| **Create:** `internal/smartcache/eviction.go` | TTL enforcement goroutine, hot detection, eviction priority logic |
| **Create:** `internal/smartcache/eviction_test.go` | Eviction ordering, hot detection, pin protection tests |
| **Create:** `internal/smartcache/controller.go` | `SmartCacheController`: wraps L1/L2/L3, hash routing, pin/unpin, Get/Put |
| **Create:** `internal/smartcache/controller_test.go` | Controller Get/Put with hash routing, pin/unpin, L1/L2/L3 fallthrough |
| **Create:** `internal/smartcache/sizing.go` | Cache sizing calculator: ingestion + query blend, auto-sizing |
| **Create:** `internal/smartcache/sizing_test.go` | Sizing calculation, blend weight, auto-sizing tests |
| **Create:** `internal/crosssignal/client.go` | `CrossSignalClient`: discovery, hint batching, HTTP send/receive |
| **Create:** `internal/crosssignal/client_test.go` | Client batch/send tests with mock HTTP |
| **Create:** `internal/crosssignal/handler.go` | HTTP handlers: `/internal/prefetch/hint`, `/internal/cache/evict-hint` |
| **Create:** `internal/crosssignal/handler_test.go` | Handler auth, parsing, routing tests |
| **Modify:** `internal/config/config.go:55-57,174-182,203-208,222-227,276-399` | Add `SmartCacheConfig`, `CrossSignalConfig`, update `CacheConfig`, `PrefetchConfig`, `QueryConfig`, defaults |
| **Modify:** `internal/metrics/lakehouse.go:23-30,79-84` | Add ~15 smart cache + cross-signal metrics |
| **Modify:** `internal/prefetch/prefetch.go:14-18,62-71,99-107` | Add `TypeCrossSignal`, S3 semaphore, priority dequeue |
| **Modify:** `internal/storage/parquets3/storage.go:26-42,176-244` | Add `smartCache` field, replace `getFileData` to route through controller |
| **Modify:** `internal/storage/parquets3/storage_query.go:21-97,99-140,142-167` | Parallel file workers, trace_id extraction, prefetch wiring |
| **Modify:** `lakehouse-traces/internal/storage/parquets3/storage.go` | Same changes as root module storage.go |
| **Modify:** `lakehouse-traces/internal/storage/parquets3/storage_query.go` | Same changes as root module storage_query.go |
| **Modify:** `cmd/lakehouse-logs/main.go:104-253` | Wire SmartCacheController + CrossSignalClient + handlers |
| **Modify:** `lakehouse-traces/main.go:105-255` | Same wiring for traces binary |

---

### Task 1: Entry Metadata — Data Structure & Persistence

**Files:**
- Create: `internal/smartcache/metadata.go`
- Create: `internal/smartcache/metadata_test.go`

- [ ] **Step 1: Write the failing test for EntryMeta and MetadataMap**

```go
// internal/smartcache/metadata_test.go
package smartcache

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestMetadataMap_SetGet(t *testing.T) {
	m := NewMetadataMap()

	now := time.Now()
	meta := EntryMeta{
		CreatedAt:         now,
		LastAccess:        now,
		AccessCount:       0,
		AccessWindowStart: now,
		Signal:            "logs",
		Size:              1024,
	}

	m.Set("file1.parquet", meta)

	got, ok := m.Get("file1.parquet")
	if !ok {
		t.Fatal("expected to find entry")
	}
	if got.Signal != "logs" {
		t.Errorf("signal = %q, want %q", got.Signal, "logs")
	}
	if got.Size != 1024 {
		t.Errorf("size = %d, want 1024", got.Size)
	}
}

func TestMetadataMap_Delete(t *testing.T) {
	m := NewMetadataMap()
	m.Set("key1", EntryMeta{Signal: "logs", Size: 100})
	m.Delete("key1")

	_, ok := m.Get("key1")
	if ok {
		t.Fatal("expected entry to be deleted")
	}
}

func TestMetadataMap_RecordAccess(t *testing.T) {
	m := NewMetadataMap()
	now := time.Now()
	m.Set("key1", EntryMeta{
		CreatedAt:         now.Add(-time.Hour),
		LastAccess:        now.Add(-time.Hour),
		AccessCount:       0,
		AccessWindowStart: now.Add(-time.Hour),
		Signal:            "logs",
		Size:              100,
	})

	m.RecordAccess("key1")

	got, ok := m.Get("key1")
	if !ok {
		t.Fatal("expected to find entry")
	}
	if got.AccessCount != 1 {
		t.Errorf("access count = %d, want 1", got.AccessCount)
	}
	if got.LastAccess.Before(now) {
		t.Error("expected LastAccess to be updated to now or later")
	}
}

func TestMetadataMap_Len(t *testing.T) {
	m := NewMetadataMap()
	m.Set("a", EntryMeta{Size: 10})
	m.Set("b", EntryMeta{Size: 20})
	m.Set("c", EntryMeta{Size: 30})

	if m.Len() != 3 {
		t.Errorf("len = %d, want 3", m.Len())
	}
}

func TestMetadataMap_TotalSize(t *testing.T) {
	m := NewMetadataMap()
	m.Set("a", EntryMeta{Size: 100})
	m.Set("b", EntryMeta{Size: 200})

	if m.TotalSize() != 300 {
		t.Errorf("total size = %d, want 300", m.TotalSize())
	}
}

func TestMetadataMap_PinUnpin(t *testing.T) {
	m := NewMetadataMap()
	m.Set("key1", EntryMeta{Signal: "logs", Size: 100})

	m.Pin("key1", "query-1", 5*time.Minute)

	got, _ := m.Get("key1")
	if len(got.PinnedBy) != 1 {
		t.Fatalf("expected 1 pin, got %d", len(got.PinnedBy))
	}
	if _, ok := got.PinnedBy["query-1"]; !ok {
		t.Error("expected pin by query-1")
	}

	m.Unpin("key1", "query-1")

	got, _ = m.Get("key1")
	if len(got.PinnedBy) != 0 {
		t.Errorf("expected 0 pins after unpin, got %d", len(got.PinnedBy))
	}
}

func TestSnapshot_SaveLoad(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "smartcache.meta.json")

	m := NewMetadataMap()
	now := time.Now().Truncate(time.Millisecond) // JSON loses sub-ms precision
	m.Set("file1", EntryMeta{
		CreatedAt:   now,
		LastAccess:  now,
		AccessCount: 5,
		Signal:      "logs",
		Size:        1024,
		TraceIDs:    []string{"abc", "def"},
	})
	m.Set("file2", EntryMeta{
		CreatedAt:  now.Add(-time.Hour),
		LastAccess: now.Add(-30 * time.Minute),
		Signal:     "traces",
		Size:       2048,
	})

	if err := m.SaveSnapshot(path); err != nil {
		t.Fatalf("save snapshot: %v", err)
	}

	loaded := NewMetadataMap()
	if err := loaded.LoadSnapshot(path); err != nil {
		t.Fatalf("load snapshot: %v", err)
	}

	if loaded.Len() != 2 {
		t.Fatalf("loaded len = %d, want 2", loaded.Len())
	}

	got, ok := loaded.Get("file1")
	if !ok {
		t.Fatal("expected file1 in loaded snapshot")
	}
	if got.AccessCount != 5 {
		t.Errorf("access count = %d, want 5", got.AccessCount)
	}
	if got.Signal != "logs" {
		t.Errorf("signal = %q, want %q", got.Signal, "logs")
	}
	if len(got.TraceIDs) != 2 {
		t.Errorf("trace_ids len = %d, want 2", len(got.TraceIDs))
	}
}

func TestSnapshot_LoadMissing(t *testing.T) {
	m := NewMetadataMap()
	err := m.LoadSnapshot("/nonexistent/path/smartcache.meta.json")
	if err != nil {
		t.Fatalf("load missing snapshot should return nil, got: %v", err)
	}
	if m.Len() != 0 {
		t.Errorf("expected empty map after loading missing file")
	}
}

func TestMetadataMap_Reconcile(t *testing.T) {
	m := NewMetadataMap()
	m.Set("exists", EntryMeta{Signal: "logs", Size: 100})
	m.Set("orphan", EntryMeta{Signal: "logs", Size: 200})

	// Simulate disk files: "exists" is present, "orphan" is not, "untracked" is new
	diskFiles := map[string]int64{
		"exists":    100,
		"untracked": 300,
	}

	m.Reconcile(diskFiles)

	if _, ok := m.Get("orphan"); ok {
		t.Error("expected orphan to be removed during reconciliation")
	}

	got, ok := m.Get("exists")
	if !ok {
		t.Fatal("expected exists to survive reconciliation")
	}
	if got.Size != 100 {
		t.Errorf("existing entry size = %d, want 100", got.Size)
	}

	got, ok = m.Get("untracked")
	if !ok {
		t.Fatal("expected untracked to be added during reconciliation")
	}
	if got.Size != 300 {
		t.Errorf("untracked size = %d, want 300", got.Size)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestMetadata|TestSnapshot'`
Expected: FAIL — package does not exist

- [ ] **Step 3: Write minimal implementation**

```go
// internal/smartcache/metadata.go
package smartcache

import (
	"encoding/json"
	"os"
	"sync"
	"time"
)

type EntryMeta struct {
	CreatedAt         time.Time            `json:"created_at"`
	LastAccess        time.Time            `json:"last_access"`
	AccessCount       int                  `json:"access_count"`
	AccessWindowStart time.Time            `json:"access_window_start"`
	PinnedBy          map[string]time.Time `json:"pinned_by,omitempty"`
	Signal            string               `json:"signal"`
	TraceIDs          []string             `json:"trace_ids,omitempty"`
	Size              int64                `json:"size"`
}

func (e *EntryMeta) IsPinned() bool {
	now := time.Now()
	for _, expiry := range e.PinnedBy {
		if now.Before(expiry) {
			return true
		}
	}
	return false
}

type MetadataMap struct {
	mu    sync.RWMutex
	items map[string]EntryMeta
}

func NewMetadataMap() *MetadataMap {
	return &MetadataMap{
		items: make(map[string]EntryMeta),
	}
}

func (m *MetadataMap) Set(key string, meta EntryMeta) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[key] = meta
}

func (m *MetadataMap) Get(key string) (EntryMeta, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	meta, ok := m.items[key]
	return meta, ok
}

func (m *MetadataMap) Delete(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, key)
}

func (m *MetadataMap) RecordAccess(key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, ok := m.items[key]
	if !ok {
		return
	}
	meta.LastAccess = time.Now()
	meta.AccessCount++
	m.items[key] = meta
}

func (m *MetadataMap) Pin(key, queryID string, grace time.Duration) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, ok := m.items[key]
	if !ok {
		return
	}
	if meta.PinnedBy == nil {
		meta.PinnedBy = make(map[string]time.Time)
	}
	meta.PinnedBy[queryID] = time.Now().Add(grace)
	m.items[key] = meta
}

func (m *MetadataMap) Unpin(key, queryID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	meta, ok := m.items[key]
	if !ok {
		return
	}
	delete(meta.PinnedBy, queryID)
	m.items[key] = meta
}

func (m *MetadataMap) Len() int {
	m.mu.RLock()
	defer m.mu.RUnlock()
	return len(m.items)
}

func (m *MetadataMap) TotalSize() int64 {
	m.mu.RLock()
	defer m.mu.RUnlock()
	var total int64
	for _, meta := range m.items {
		total += meta.Size
	}
	return total
}

func (m *MetadataMap) All() map[string]EntryMeta {
	m.mu.RLock()
	defer m.mu.RUnlock()
	cp := make(map[string]EntryMeta, len(m.items))
	for k, v := range m.items {
		cp[k] = v
	}
	return cp
}

func (m *MetadataMap) Reconcile(diskFiles map[string]int64) {
	m.mu.Lock()
	defer m.mu.Unlock()

	for key := range m.items {
		if _, exists := diskFiles[key]; !exists {
			delete(m.items, key)
		}
	}

	now := time.Now()
	for key, size := range diskFiles {
		if _, exists := m.items[key]; !exists {
			m.items[key] = EntryMeta{
				CreatedAt:         now,
				LastAccess:        now,
				AccessWindowStart: now,
				Size:              size,
			}
		}
	}
}

func (m *MetadataMap) SaveSnapshot(path string) error {
	m.mu.RLock()
	cp := make(map[string]EntryMeta, len(m.items))
	for k, v := range m.items {
		cp[k] = v
	}
	m.mu.RUnlock()

	data, err := json.Marshal(cp)
	if err != nil {
		return err
	}

	tmpPath := path + ".tmp"
	if err := os.WriteFile(tmpPath, data, 0644); err != nil {
		return err
	}
	return os.Rename(tmpPath, path)
}

func (m *MetadataMap) LoadSnapshot(path string) error {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil
		}
		return err
	}

	var items map[string]EntryMeta
	if err := json.Unmarshal(data, &items); err != nil {
		return err
	}

	m.mu.Lock()
	defer m.mu.Unlock()
	m.items = items
	return nil
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestMetadata|TestSnapshot'`
Expected: PASS — all 8 tests

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/smartcache/metadata.go internal/smartcache/metadata_test.go
git commit -m "feat(smartcache): add EntryMeta struct and MetadataMap with snapshot persistence"
```

---

### Task 2: Eviction Logic — TTL, Hot Detection, Pin Protection

**Files:**
- Create: `internal/smartcache/eviction.go`
- Create: `internal/smartcache/eviction_test.go`

- [ ] **Step 1: Write the failing tests**

```go
// internal/smartcache/eviction_test.go
package smartcache

import (
	"testing"
	"time"
)

func TestIsHot(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name      string
		meta      EntryMeta
		threshold int
		window    time.Duration
		want      bool
	}{
		{
			name: "cold entry below threshold",
			meta: EntryMeta{
				AccessCount:       1,
				AccessWindowStart: now.Add(-5 * time.Minute),
			},
			threshold: 3,
			window:    10 * time.Minute,
			want:      false,
		},
		{
			name: "hot entry above threshold within window",
			meta: EntryMeta{
				AccessCount:       5,
				AccessWindowStart: now.Add(-5 * time.Minute),
			},
			threshold: 3,
			window:    10 * time.Minute,
			want:      true,
		},
		{
			name: "stale window resets",
			meta: EntryMeta{
				AccessCount:       5,
				AccessWindowStart: now.Add(-15 * time.Minute),
			},
			threshold: 3,
			window:    10 * time.Minute,
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsHot(tt.meta, tt.threshold, tt.window)
			if got != tt.want {
				t.Errorf("IsHot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvictionPriority(t *testing.T) {
	now := time.Now()
	maxAge := 1 * time.Hour
	hotThreshold := 3
	hotWindow := 10 * time.Minute

	entries := map[string]EntryMeta{
		"expired_cold": {
			CreatedAt:         now.Add(-2 * time.Hour),
			LastAccess:        now.Add(-90 * time.Minute),
			AccessCount:       1,
			AccessWindowStart: now.Add(-90 * time.Minute),
			Size:              100,
		},
		"cold_recent": {
			CreatedAt:         now.Add(-30 * time.Minute),
			LastAccess:        now.Add(-20 * time.Minute),
			AccessCount:       1,
			AccessWindowStart: now.Add(-20 * time.Minute),
			Size:              200,
		},
		"hot_entry": {
			CreatedAt:         now.Add(-30 * time.Minute),
			LastAccess:        now.Add(-1 * time.Minute),
			AccessCount:       10,
			AccessWindowStart: now.Add(-5 * time.Minute),
			Size:              300,
		},
		"pinned_entry": {
			CreatedAt:  now.Add(-2 * time.Hour),
			LastAccess: now.Add(-2 * time.Hour),
			PinnedBy:   map[string]time.Time{"q1": now.Add(5 * time.Minute)},
			Size:       400,
		},
	}

	order := SortByEvictionPriority(entries, maxAge, hotThreshold, hotWindow)

	if len(order) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(order))
	}

	// expired_cold should be first (highest eviction priority)
	if order[0] != "expired_cold" {
		t.Errorf("first eviction = %q, want %q", order[0], "expired_cold")
	}
	// pinned_entry should be last (lowest eviction priority)
	if order[3] != "pinned_entry" {
		t.Errorf("last eviction = %q, want %q", order[3], "pinned_entry")
	}
}

func TestCollectExpired(t *testing.T) {
	now := time.Now()
	maxAge := 1 * time.Hour
	hotThreshold := 3
	hotWindow := 10 * time.Minute

	m := NewMetadataMap()
	m.Set("expired", EntryMeta{
		CreatedAt:         now.Add(-2 * time.Hour),
		LastAccess:        now.Add(-2 * time.Hour),
		AccessCount:       0,
		AccessWindowStart: now.Add(-2 * time.Hour),
		Size:              100,
	})
	m.Set("fresh", EntryMeta{
		CreatedAt:         now.Add(-10 * time.Minute),
		LastAccess:        now.Add(-5 * time.Minute),
		AccessCount:       0,
		AccessWindowStart: now.Add(-5 * time.Minute),
		Size:              200,
	})
	m.Set("hot_expired_created_but_accessed_recently", EntryMeta{
		CreatedAt:         now.Add(-2 * time.Hour),
		LastAccess:        now.Add(-1 * time.Minute),
		AccessCount:       5,
		AccessWindowStart: now.Add(-5 * time.Minute),
		Size:              300,
	})
	m.Set("pinned_expired", EntryMeta{
		CreatedAt:  now.Add(-2 * time.Hour),
		LastAccess: now.Add(-2 * time.Hour),
		PinnedBy:   map[string]time.Time{"q1": now.Add(5 * time.Minute)},
		Size:       400,
	})

	expired := CollectExpired(m, maxAge, hotThreshold, hotWindow)

	if len(expired) != 1 {
		t.Fatalf("expected 1 expired entry, got %d: %v", len(expired), expired)
	}
	if expired[0] != "expired" {
		t.Errorf("expired entry = %q, want %q", expired[0], "expired")
	}
}

func TestCollectLRU(t *testing.T) {
	now := time.Now()
	hotThreshold := 3
	hotWindow := 10 * time.Minute

	m := NewMetadataMap()
	m.Set("oldest", EntryMeta{
		LastAccess:        now.Add(-1 * time.Hour),
		AccessCount:       1,
		AccessWindowStart: now.Add(-1 * time.Hour),
		Size:              100,
	})
	m.Set("middle", EntryMeta{
		LastAccess:        now.Add(-30 * time.Minute),
		AccessCount:       1,
		AccessWindowStart: now.Add(-30 * time.Minute),
		Size:              200,
	})
	m.Set("newest", EntryMeta{
		LastAccess:        now.Add(-1 * time.Minute),
		AccessCount:       1,
		AccessWindowStart: now.Add(-1 * time.Minute),
		Size:              300,
	})

	// Need to free 250 bytes → should evict "oldest" (100) then "middle" (200)
	toEvict := CollectLRU(m, 250, hotThreshold, hotWindow)

	if len(toEvict) != 2 {
		t.Fatalf("expected 2 entries to evict, got %d: %v", len(toEvict), toEvict)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestIsHot|TestEviction|TestCollect'`
Expected: FAIL — undefined functions

- [ ] **Step 3: Write minimal implementation**

```go
// internal/smartcache/eviction.go
package smartcache

import (
	"sort"
	"time"
)

func IsHot(meta EntryMeta, threshold int, window time.Duration) bool {
	if time.Since(meta.AccessWindowStart) > window {
		return false
	}
	return meta.AccessCount >= threshold
}

type evictionEntry struct {
	key      string
	priority int // lower = evict first
	lastAccess time.Time
}

func SortByEvictionPriority(entries map[string]EntryMeta, maxAge time.Duration, hotThreshold int, hotWindow time.Duration) []string {
	now := time.Now()
	sorted := make([]evictionEntry, 0, len(entries))

	for key, meta := range entries {
		p := classifyPriority(meta, now, maxAge, hotThreshold, hotWindow)
		sorted = append(sorted, evictionEntry{key: key, priority: p, lastAccess: meta.LastAccess})
	}

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].priority != sorted[j].priority {
			return sorted[i].priority < sorted[j].priority
		}
		return sorted[i].lastAccess.Before(sorted[j].lastAccess)
	})

	result := make([]string, len(sorted))
	for i, e := range sorted {
		result[i] = e.key
	}
	return result
}

func classifyPriority(meta EntryMeta, now time.Time, maxAge time.Duration, hotThreshold int, hotWindow time.Duration) int {
	pinned := meta.IsPinned()
	hot := IsHot(meta, hotThreshold, hotWindow)

	isExpired := false
	if hot {
		isExpired = now.Sub(meta.LastAccess) > maxAge
	} else {
		isExpired = now.Sub(meta.CreatedAt) > maxAge
	}

	if pinned {
		return 4 // never evict
	}
	if isExpired && !hot {
		return 1 // evict first
	}
	if !hot {
		return 2 // cold, not expired — LRU
	}
	return 3 // hot — evict last
}

func CollectExpired(m *MetadataMap, maxAge time.Duration, hotThreshold int, hotWindow time.Duration) []string {
	now := time.Now()
	all := m.All()
	var expired []string

	for key, meta := range all {
		if meta.IsPinned() {
			continue
		}
		hot := IsHot(meta, hotThreshold, hotWindow)
		if hot {
			if now.Sub(meta.LastAccess) > maxAge {
				expired = append(expired, key)
			}
		} else {
			if now.Sub(meta.CreatedAt) > maxAge {
				expired = append(expired, key)
			}
		}
	}
	return expired
}

func CollectLRU(m *MetadataMap, bytesNeeded int64, hotThreshold int, hotWindow time.Duration) []string {
	all := m.All()

	type candidate struct {
		key        string
		lastAccess time.Time
		size       int64
		hot        bool
		pinned     bool
	}

	candidates := make([]candidate, 0, len(all))
	for key, meta := range all {
		candidates = append(candidates, candidate{
			key:        key,
			lastAccess: meta.LastAccess,
			size:       meta.Size,
			hot:        IsHot(meta, hotThreshold, hotWindow),
			pinned:     meta.IsPinned(),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		pi, pj := 0, 0
		if candidates[i].pinned {
			pi = 2
		} else if candidates[i].hot {
			pi = 1
		}
		if candidates[j].pinned {
			pj = 2
		} else if candidates[j].hot {
			pj = 1
		}
		if pi != pj {
			return pi < pj
		}
		return candidates[i].lastAccess.Before(candidates[j].lastAccess)
	})

	var toEvict []string
	var freed int64
	for _, c := range candidates {
		if freed >= bytesNeeded {
			break
		}
		if c.pinned {
			break
		}
		toEvict = append(toEvict, c.key)
		freed += c.size
	}
	return toEvict
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestIsHot|TestEviction|TestCollect'`
Expected: PASS — all 5 tests

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/smartcache/eviction.go internal/smartcache/eviction_test.go
git commit -m "feat(smartcache): add eviction logic with TTL, hot detection, pin protection"
```

---

### Task 3: Config — SmartCacheConfig and CrossSignalConfig

**Files:**
- Modify: `internal/config/config.go`

- [ ] **Step 1: Write the failing test**

```go
// Add to internal/config/config_test.go (or create if not exists)
// internal/config/config_smartcache_test.go
package config

import (
	"testing"
	"time"
)

func TestSmartCacheConfig_Defaults(t *testing.T) {
	cfg := Default()

	if cfg.SmartCache.MaxAge != 24*time.Hour {
		t.Errorf("max_age = %v, want 24h", cfg.SmartCache.MaxAge)
	}
	if cfg.SmartCache.SnapshotInterval != 60*time.Second {
		t.Errorf("snapshot_interval = %v, want 60s", cfg.SmartCache.SnapshotInterval)
	}
	if cfg.SmartCache.QueryGracePeriod != 5*time.Minute {
		t.Errorf("query_grace_period = %v, want 5m", cfg.SmartCache.QueryGracePeriod)
	}
	if cfg.SmartCache.HotAccessThreshold != 3 {
		t.Errorf("hot_access_threshold = %d, want 3", cfg.SmartCache.HotAccessThreshold)
	}
	if cfg.SmartCache.HotWindow != 10*time.Minute {
		t.Errorf("hot_window = %v, want 10m", cfg.SmartCache.HotWindow)
	}
	if cfg.SmartCache.TargetHours != 24 {
		t.Errorf("target_hours = %d, want 24", cfg.SmartCache.TargetHours)
	}
	if cfg.SmartCache.DiskLimitMax != "100GB" {
		t.Errorf("disk_limit_max = %q, want %q", cfg.SmartCache.DiskLimitMax, "100GB")
	}
}

func TestCrossSignalConfig_Defaults(t *testing.T) {
	cfg := Default()

	if cfg.CrossSignal.Enabled != false {
		t.Errorf("cross_signal.enabled = %v, want false", cfg.CrossSignal.Enabled)
	}
	if cfg.CrossSignal.Timeout != 2*time.Second {
		t.Errorf("timeout = %v, want 2s", cfg.CrossSignal.Timeout)
	}
	if cfg.CrossSignal.MaxBatch != 100 {
		t.Errorf("max_batch = %d, want 100", cfg.CrossSignal.MaxBatch)
	}
	if cfg.CrossSignal.BatchInterval != 500*time.Millisecond {
		t.Errorf("batch_interval = %v, want 500ms", cfg.CrossSignal.BatchInterval)
	}
}

func TestQueryConfig_FileWorkers(t *testing.T) {
	cfg := Default()

	if cfg.Query.FileWorkers != 8 {
		t.Errorf("file_workers = %d, want 8", cfg.Query.FileWorkers)
	}
}

func TestS3Config_MaxConcurrentDownloads(t *testing.T) {
	cfg := Default()

	if cfg.S3.MaxConcurrentDownloads != 16 {
		t.Errorf("max_concurrent_downloads = %d, want 16", cfg.S3.MaxConcurrentDownloads)
	}
}

func TestPrefetchConfig_UpdatedDefaults(t *testing.T) {
	cfg := Default()

	if cfg.Prefetch.MaxConcurrent != 8 {
		t.Errorf("prefetch.max_concurrent = %d, want 8", cfg.Prefetch.MaxConcurrent)
	}
	if cfg.Prefetch.MaxQueue != 128 {
		t.Errorf("prefetch.max_queue = %d, want 128", cfg.Prefetch.MaxQueue)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/config/ -v -run 'TestSmartCache|TestCrossSignal|TestQueryConfig_FileWorkers|TestS3Config_MaxConcurrent|TestPrefetchConfig_Updated'`
Expected: FAIL — `SmartCache` field undefined

- [ ] **Step 3: Add config structs and defaults**

Add these new structs to `internal/config/config.go` after the `DeleteConfig` struct (around line 259):

```go
type SmartCacheConfig struct {
	MaxAge             time.Duration `yaml:"max_age"`
	SnapshotInterval   time.Duration `yaml:"snapshot_interval"`
	QueryGracePeriod   time.Duration `yaml:"query_grace_period"`
	HotAccessThreshold int           `yaml:"hot_access_threshold"`
	HotWindow          time.Duration `yaml:"hot_window"`
	TargetHours        int           `yaml:"target_hours"`
	DiskLimitMax       string        `yaml:"disk_limit_max"`
	IngestionRateHint  string        `yaml:"ingestion_rate_hint"`
}

type CrossSignalConfig struct {
	Enabled         bool          `yaml:"enabled"`
	Endpoint        string        `yaml:"endpoint"`
	HeadlessService string        `yaml:"headless_service"`
	AuthKey         string        `yaml:"auth_key"`
	Timeout         time.Duration `yaml:"timeout"`
	MaxBatch        int           `yaml:"max_batch"`
	BatchInterval   time.Duration `yaml:"batch_interval"`
}
```

Add fields to Config struct (after `Delete DeleteConfig`):

```go
	SmartCache  SmartCacheConfig  `yaml:"smart_cache"`
	CrossSignal CrossSignalConfig `yaml:"cross_signal"`
```

Add `FileWorkers` to `QueryConfig`:

```go
type QueryConfig struct {
	MaxConcurrent int           `yaml:"max_concurrent"`
	FileWorkers   int           `yaml:"file_workers"`
	Timeout       time.Duration `yaml:"timeout"`
	MaxRows       int64         `yaml:"max_rows"`
	SlowThreshold time.Duration `yaml:"slow_threshold"`
}
```

Add `MaxConcurrentDownloads` to `S3Config`:

```go
	MaxConcurrentDownloads int `yaml:"max_concurrent_downloads"`
```

Update defaults in `Default()`:

```go
	SmartCache: SmartCacheConfig{
		MaxAge:             24 * time.Hour,
		SnapshotInterval:   60 * time.Second,
		QueryGracePeriod:   5 * time.Minute,
		HotAccessThreshold: 3,
		HotWindow:          10 * time.Minute,
		TargetHours:        24,
		DiskLimitMax:       "100GB",
	},

	CrossSignal: CrossSignalConfig{
		Enabled:       false,
		Timeout:       2 * time.Second,
		MaxBatch:      100,
		BatchInterval: 500 * time.Millisecond,
	},
```

Update existing defaults:
- `Query.FileWorkers: 8`
- `S3.MaxConcurrentDownloads: 16`
- `Prefetch.MaxConcurrent: 8` (was 4)
- `Prefetch.MaxQueue: 128` (was 64)

Add merge logic for new fields in `mergeConfig()`.

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/config/ -v -run 'TestSmartCache|TestCrossSignal|TestQueryConfig_FileWorkers|TestS3Config_MaxConcurrent|TestPrefetchConfig_Updated'`
Expected: PASS

- [ ] **Step 5: Run full config test suite for regressions**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/config/ -v`
Expected: PASS — no regressions

- [ ] **Step 6: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/config/config.go internal/config/config_smartcache_test.go
git commit -m "feat(config): add SmartCacheConfig, CrossSignalConfig, query.file_workers, s3.max_concurrent_downloads"
```

---

### Task 4: Metrics — Smart Cache and Cross-Signal Metrics

**Files:**
- Modify: `internal/metrics/lakehouse.go`

- [ ] **Step 1: Add smart cache and cross-signal metrics**

Add after the existing "Cache metrics" block (line 30) and replace the "Prefetch metrics" block:

```go
// Smart cache metrics
var (
	SmartCacheHitRatio         = NewFloatGauge("lakehouse_cache_hit_ratio")
	SmartCacheEntriesTotal     = NewGauge("lakehouse_cache_entries_total")
	SmartCacheBytesUsed        = NewGauge("lakehouse_cache_bytes_used")
	SmartCacheBytesLimit       = NewGauge("lakehouse_cache_bytes_limit")
	SmartCacheEvictionsTotal   = NewCounterVec("lakehouse_cache_evictions_total", "reason")
	SmartCacheHotEntries       = NewGauge("lakehouse_cache_hot_entries")
	SmartCachePinnedEntries    = NewGauge("lakehouse_cache_pinned_entries")
	SmartCacheRecommendedBytes = NewCounterVec("lakehouse_cache_recommended_bytes", "method")
	SmartCacheCoverageHours    = NewFloatGauge("lakehouse_cache_coverage_hours")
	SmartCachePrefetchHitRatio = NewFloatGauge("lakehouse_cache_prefetch_hit_ratio")
	SmartCacheOwnedEntries     = NewGauge("lakehouse_cache_owned_entries")
	SmartCacheOwnedBytes       = NewGauge("lakehouse_cache_owned_bytes")
	SmartCachePeerServedTotal  = NewCounter("lakehouse_cache_peer_served_total")
	SmartCacheEffectiveBytes   = NewGauge("lakehouse_cache_effective_bytes")
)

// Cross-signal eviction metrics
var (
	CrossEvictionSent     = NewCounter("lakehouse_cache_cross_eviction_sent_total")
	CrossEvictionReceived = NewCounter("lakehouse_cache_cross_eviction_received_total")
	CrossEvictionPending  = NewGauge("lakehouse_cache_cross_eviction_pending")
	CrossEvictionApplied  = NewCounter("lakehouse_cache_cross_eviction_applied_total")
	CrossPrefetchSent     = NewCounter("lakehouse_cache_cross_prefetch_sent_total")
	CrossPrefetchReceived = NewCounter("lakehouse_cache_cross_prefetch_received_total")
)
```

- [ ] **Step 2: Verify compilation**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go build ./internal/metrics/`
Expected: Success

- [ ] **Step 3: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/metrics/lakehouse.go
git commit -m "feat(metrics): add smart cache, cross-signal, and cache sizing metrics"
```

---

### Task 5: Prefetch Engine — Add TypeCrossSignal and Priority Dequeue

**Files:**
- Modify: `internal/prefetch/prefetch.go`
- Create: `internal/prefetch/prefetch_crosssignal_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/prefetch/prefetch_crosssignal_test.go
package prefetch

import (
	"context"
	"sync/atomic"
	"testing"
	"time"
)

func TestTypeCrossSignal_String(t *testing.T) {
	if TypeCrossSignal.String() != "cross_signal" {
		t.Errorf("TypeCrossSignal.String() = %q, want %q", TypeCrossSignal.String(), "cross_signal")
	}
}

func TestEnqueueCrossSignal(t *testing.T) {
	var fetched atomic.Int64
	engine := NewEngine(2, 64, func(ctx context.Context, key string) error {
		fetched.Add(1)
		return nil
	})
	defer engine.Close()

	n := engine.EnqueueCrossSignal([]string{"trace-file-1", "trace-file-2"})
	if n != 2 {
		t.Errorf("enqueued = %d, want 2", n)
	}

	// Wait for processing
	time.Sleep(100 * time.Millisecond)

	if fetched.Load() != 2 {
		t.Errorf("fetched = %d, want 2", fetched.Load())
	}
}

func TestPriorityDequeue_CrossSignalBeforeReadAhead(t *testing.T) {
	var order []string
	orderCh := make(chan string, 10)

	engine := NewEngine(1, 64, func(ctx context.Context, key string) error {
		orderCh <- key
		time.Sleep(10 * time.Millisecond)
		return nil
	})
	defer engine.Close()

	// Enqueue read_ahead first, then cross_signal
	engine.Enqueue(Task{Key: "readahead-1", Type: TypeReadAhead, Priority: 2})
	engine.Enqueue(Task{Key: "readahead-2", Type: TypeReadAhead, Priority: 2})
	engine.Enqueue(Task{Key: "cross-1", Type: TypeCrossSignal, Priority: 1})
	engine.Enqueue(Task{Key: "cross-2", Type: TypeCrossSignal, Priority: 1})

	// Collect results
	timeout := time.After(2 * time.Second)
	for i := 0; i < 4; i++ {
		select {
		case key := <-orderCh:
			order = append(order, key)
		case <-timeout:
			t.Fatalf("timeout waiting for task %d, got %v", i, order)
		}
	}

	// Cross-signal tasks (priority 1) should come before read-ahead (priority 2)
	// The first task is already dequeued before we add higher-priority ones,
	// but among the remaining queued items, priority should be respected.
	// With 1 worker, first dequeue happens immediately. The remaining 3 are queued.
	// After the first completes, the highest-priority queued item should be next.
	crossCount := 0
	for i := 0; i < 3; i++ {
		if order[i] == "cross-1" || order[i] == "cross-2" {
			crossCount++
		}
	}
	// At least the 2 cross-signal tasks should complete in the first 3
	if crossCount < 2 {
		t.Errorf("expected cross-signal tasks to be prioritized, order was: %v", order)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/prefetch/ -v -run 'TestTypeCross|TestEnqueueCross|TestPriority'`
Expected: FAIL — `TypeCrossSignal` undefined

- [ ] **Step 3: Modify prefetch.go**

Add `TypeCrossSignal` to the type constants (after line 14):

```go
const (
	TypeCrossSignal Type = iota
	TypeCorrelated
	TypeReadAhead
	TypeWarmup
)
```

Add to `String()`:

```go
case TypeCrossSignal:
	return "cross_signal"
```

Add `EnqueueCrossSignal` method (after `EnqueueWarmup`):

```go
func (e *Engine) EnqueueCrossSignal(keys []string) int {
	enqueued := 0
	for _, key := range keys {
		if e.Enqueue(Task{Key: key, Type: TypeCrossSignal, Priority: 1}) {
			enqueued++
		}
	}
	return enqueued
}
```

Change `dequeue()` to use priority-based dequeue:

```go
func (e *Engine) dequeue() (Task, bool) {
	if len(e.queue) == 0 {
		return Task{}, false
	}
	bestIdx := 0
	for i := 1; i < len(e.queue); i++ {
		if e.queue[i].Priority < e.queue[bestIdx].Priority {
			bestIdx = i
		}
	}
	task := e.queue[bestIdx]
	e.queue = append(e.queue[:bestIdx], e.queue[bestIdx+1:]...)
	return task, true
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/prefetch/ -v`
Expected: PASS — all tests including new ones

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/prefetch/prefetch.go internal/prefetch/prefetch_crosssignal_test.go
git commit -m "feat(prefetch): add TypeCrossSignal with priority-based dequeue"
```

---

### Task 6: Smart Cache Controller — Core Get/Put with Hash Routing

**Files:**
- Create: `internal/smartcache/controller.go`
- Create: `internal/smartcache/controller_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/smartcache/controller_test.go
package smartcache

import (
	"context"
	"fmt"
	"sync/atomic"
	"testing"
	"time"
)

type mockL1 struct {
	data map[string][]byte
}

func newMockL1() *mockL1 { return &mockL1{data: make(map[string][]byte)} }

func (m *mockL1) Get(key string) ([]byte, bool) {
	d, ok := m.data[key]
	return d, ok
}
func (m *mockL1) Put(key string, val []byte) { m.data[key] = val }

type mockL2 struct {
	data map[string][]byte
}

func newMockL2() *mockL2 { return &mockL2{data: make(map[string][]byte)} }

func (m *mockL2) Get(key string) ([]byte, bool) {
	d, ok := m.data[key]
	return d, ok
}
func (m *mockL2) Put(key string, data []byte) error {
	m.data[key] = data
	return nil
}
func (m *mockL2) Delete(key string) { delete(m.data, key) }
func (m *mockL2) Size() int64 {
	var s int64
	for _, d := range m.data {
		s += int64(len(d))
	}
	return s
}

type mockPeerLookup struct {
	selfAddr string
}

func (m *mockPeerLookup) Lookup(key string) (string, bool) {
	return m.selfAddr, true
}
func (m *mockPeerLookup) Members() []string {
	return []string{m.selfAddr}
}
func (m *mockPeerLookup) MemberCount() int { return 1 }

type mockPeerFetcher struct{}

func (m *mockPeerFetcher) Fetch(ctx context.Context, peer, key string) ([]byte, bool, error) {
	return nil, false, nil
}

type mockS3Fetcher struct {
	data  map[string][]byte
	calls atomic.Int64
}

func newMockS3() *mockS3Fetcher {
	return &mockS3Fetcher{data: make(map[string][]byte)}
}

func (m *mockS3Fetcher) Download(ctx context.Context, key string) ([]byte, error) {
	m.calls.Add(1)
	d, ok := m.data[key]
	if !ok {
		return nil, fmt.Errorf("not found: %s", key)
	}
	return d, nil
}

func TestController_GetFromL1(t *testing.T) {
	l1 := newMockL1()
	l1.Put("file1", []byte("hello"))

	ctrl := NewController(ControllerConfig{
		L1:            l1,
		L2:            newMockL2(),
		PeerLookup:    &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:   &mockPeerFetcher{},
		S3Fetcher:     newMockS3(),
		Metadata:      NewMetadataMap(),
		MaxAge:        24 * time.Hour,
		HotThreshold:  3,
		HotWindow:     10 * time.Minute,
		Signal:        "logs",
	})

	data, err := ctrl.Get(context.Background(), "file1", 5)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(data) != "hello" {
		t.Errorf("data = %q, want %q", string(data), "hello")
	}
}

func TestController_GetFromL2_OwnedKey(t *testing.T) {
	l1 := newMockL1()
	l2 := newMockL2()
	l2.Put("file2", []byte("disk-data"))

	meta := NewMetadataMap()
	meta.Set("file2", EntryMeta{
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		Signal:     "logs",
		Size:       9,
	})

	ctrl := NewController(ControllerConfig{
		L1:            l1,
		L2:            l2,
		PeerLookup:    &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:   &mockPeerFetcher{},
		S3Fetcher:     newMockS3(),
		Metadata:      meta,
		MaxAge:        24 * time.Hour,
		HotThreshold:  3,
		HotWindow:     10 * time.Minute,
		Signal:        "logs",
	})

	data, err := ctrl.Get(context.Background(), "file2", 9)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(data) != "disk-data" {
		t.Errorf("data = %q, want %q", string(data), "disk-data")
	}

	// Should also be promoted to L1
	if _, ok := l1.Get("file2"); !ok {
		t.Error("expected file2 to be promoted to L1 after L2 hit")
	}
}

func TestController_GetFromS3_StoresInL2(t *testing.T) {
	l1 := newMockL1()
	l2 := newMockL2()
	s3 := newMockS3()
	s3.data["file3"] = []byte("s3-data")

	ctrl := NewController(ControllerConfig{
		L1:            l1,
		L2:            l2,
		PeerLookup:    &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:   &mockPeerFetcher{},
		S3Fetcher:     s3,
		Metadata:      NewMetadataMap(),
		MaxAge:        24 * time.Hour,
		HotThreshold:  3,
		HotWindow:     10 * time.Minute,
		Signal:        "logs",
	})

	data, err := ctrl.Get(context.Background(), "file3", 7)
	if err != nil {
		t.Fatalf("Get error: %v", err)
	}
	if string(data) != "s3-data" {
		t.Errorf("data = %q, want %q", string(data), "s3-data")
	}

	if _, ok := l2.Get("file3"); !ok {
		t.Error("expected file3 to be stored in L2 after S3 download")
	}
	if _, ok := l1.Get("file3"); !ok {
		t.Error("expected file3 to be stored in L1 after S3 download")
	}
	if s3.calls.Load() != 1 {
		t.Errorf("S3 calls = %d, want 1", s3.calls.Load())
	}
}

func TestController_PinUnpin(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("file1", EntryMeta{Signal: "logs", Size: 100})

	ctrl := NewController(ControllerConfig{
		L1:            newMockL1(),
		L2:            newMockL2(),
		PeerLookup:    &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:   &mockPeerFetcher{},
		S3Fetcher:     newMockS3(),
		Metadata:      meta,
		MaxAge:        24 * time.Hour,
		HotThreshold:  3,
		HotWindow:     10 * time.Minute,
		GracePeriod:   5 * time.Minute,
		Signal:        "logs",
	})

	ctrl.Pin("file1", "query-1")

	got, _ := meta.Get("file1")
	if len(got.PinnedBy) != 1 {
		t.Fatalf("expected 1 pin, got %d", len(got.PinnedBy))
	}

	ctrl.Unpin("file1", "query-1")

	got, _ = meta.Get("file1")
	if len(got.PinnedBy) != 0 {
		t.Errorf("expected 0 pins after unpin, got %d", len(got.PinnedBy))
	}
}

func TestController_RecordTraceIDs(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("file1", EntryMeta{Signal: "logs", Size: 100})

	ctrl := NewController(ControllerConfig{
		L1:            newMockL1(),
		L2:            newMockL2(),
		PeerLookup:    &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:   &mockPeerFetcher{},
		S3Fetcher:     newMockS3(),
		Metadata:      meta,
		MaxAge:        24 * time.Hour,
		HotThreshold:  3,
		HotWindow:     10 * time.Minute,
		Signal:        "logs",
	})

	ctrl.RecordTraceIDs("file1", []string{"trace-abc", "trace-def"})

	got, _ := meta.Get("file1")
	if len(got.TraceIDs) != 2 {
		t.Errorf("trace_ids len = %d, want 2", len(got.TraceIDs))
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestController'`
Expected: FAIL — `NewController` undefined

- [ ] **Step 3: Write the controller implementation**

```go
// internal/smartcache/controller.go
package smartcache

import (
	"context"
	"sync"
	"time"
)

type L1Cache interface {
	Get(key string) ([]byte, bool)
	Put(key string, val []byte)
}

type L2Cache interface {
	Get(key string) ([]byte, bool)
	Put(key string, data []byte) error
	Delete(key string)
	Size() int64
}

type PeerLookup interface {
	Lookup(key string) (peer string, isLocal bool)
	Members() []string
	MemberCount() int
}

type PeerFetcher interface {
	Fetch(ctx context.Context, peer, key string) ([]byte, bool, error)
}

type S3Fetcher interface {
	Download(ctx context.Context, key string) ([]byte, error)
}

type ControllerConfig struct {
	L1            L1Cache
	L2            L2Cache
	PeerLookup    PeerLookup
	PeerFetcher   PeerFetcher
	S3Fetcher     S3Fetcher
	Metadata      *MetadataMap
	MaxAge        time.Duration
	HotThreshold  int
	HotWindow     time.Duration
	GracePeriod   time.Duration
	Signal        string
}

type Controller struct {
	l1          L1Cache
	l2          L2Cache
	peerLookup  PeerLookup
	peerFetcher PeerFetcher
	s3Fetcher   S3Fetcher
	metadata    *MetadataMap
	maxAge      time.Duration
	hotThreshold int
	hotWindow   time.Duration
	gracePeriod time.Duration
	signal      string

	sfMu       sync.Mutex
	sfInFlight map[string]*sfCall
}

type sfCall struct {
	wg  sync.WaitGroup
	val []byte
	err error
}

func NewController(cfg ControllerConfig) *Controller {
	if cfg.GracePeriod == 0 {
		cfg.GracePeriod = 5 * time.Minute
	}
	return &Controller{
		l1:           cfg.L1,
		l2:           cfg.L2,
		peerLookup:   cfg.PeerLookup,
		peerFetcher:  cfg.PeerFetcher,
		s3Fetcher:    cfg.S3Fetcher,
		metadata:     cfg.Metadata,
		maxAge:       cfg.MaxAge,
		hotThreshold: cfg.HotThreshold,
		hotWindow:    cfg.HotWindow,
		gracePeriod:  cfg.GracePeriod,
		signal:       cfg.Signal,
		sfInFlight:   make(map[string]*sfCall),
	}
}

func (c *Controller) Get(ctx context.Context, key string, size int64) ([]byte, error) {
	if data, ok := c.l1.Get(key); ok {
		c.metadata.RecordAccess(key)
		return data, nil
	}

	peer, isLocal := c.peerLookup.Lookup(key)

	if isLocal {
		if data, ok := c.l2.Get(key); ok {
			c.metadata.RecordAccess(key)
			c.l1.Put(key, data)
			return data, nil
		}
	} else if c.peerFetcher != nil {
		data, found, err := c.peerFetcher.Fetch(ctx, peer, key)
		if err == nil && found {
			c.l1.Put(key, data)
			return data, nil
		}
	}

	data, err := c.singleflightDownload(ctx, key, size)
	if err != nil {
		return nil, err
	}

	c.l1.Put(key, data)

	if _, isLocal := c.peerLookup.Lookup(key); isLocal && c.l2 != nil {
		_ = c.l2.Put(key, data)
		now := time.Now()
		c.metadata.Set(key, EntryMeta{
			CreatedAt:         now,
			LastAccess:        now,
			AccessCount:       1,
			AccessWindowStart: now,
			Signal:            c.signal,
			Size:              int64(len(data)),
		})
	}

	return data, nil
}

func (c *Controller) singleflightDownload(ctx context.Context, key string, size int64) ([]byte, error) {
	c.sfMu.Lock()
	if call, ok := c.sfInFlight[key]; ok {
		c.sfMu.Unlock()
		call.wg.Wait()
		return call.val, call.err
	}
	call := &sfCall{}
	call.wg.Add(1)
	c.sfInFlight[key] = call
	c.sfMu.Unlock()

	call.val, call.err = c.s3Fetcher.Download(ctx, key)
	call.wg.Done()

	c.sfMu.Lock()
	delete(c.sfInFlight, key)
	c.sfMu.Unlock()

	return call.val, call.err
}

func (c *Controller) Pin(key, queryID string) {
	c.metadata.Pin(key, queryID, c.gracePeriod)
}

func (c *Controller) Unpin(key, queryID string) {
	c.metadata.Unpin(key, queryID)
}

func (c *Controller) RecordTraceIDs(key string, traceIDs []string) {
	meta, ok := c.metadata.Get(key)
	if !ok {
		return
	}
	meta.TraceIDs = traceIDs
	c.metadata.Set(key, meta)
}

func (c *Controller) Metadata() *MetadataMap {
	return c.metadata
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestController'`
Expected: PASS — all 5 tests

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/smartcache/controller.go internal/smartcache/controller_test.go
git commit -m "feat(smartcache): add Controller with hash-routed Get, singleflight, pin/unpin"
```

---

### Task 7: Cache Sizing Calculator

**Files:**
- Create: `internal/smartcache/sizing.go`
- Create: `internal/smartcache/sizing_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/smartcache/sizing_test.go
package smartcache

import (
	"testing"
	"time"
)

func TestSizingCalculator_IngestionBased(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	calc.RecordIngestion(1024 * 1024 * 1024) // 1GB in this interval
	calc.SetIngestionInterval(1 * time.Hour)

	est := calc.IngestionEstimate()
	// 1GB/hour * 24h = 24GB
	expected := int64(24 * 1024 * 1024 * 1024)
	tolerance := int64(float64(expected) * 0.01)
	if est < expected-tolerance || est > expected+tolerance {
		t.Errorf("ingestion estimate = %d, want ~%d", est, expected)
	}
}

func TestSizingCalculator_QueryBased(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	// Record 10 unique file reads at 100MB each
	for i := 0; i < 10; i++ {
		calc.RecordQueryRead(int64(i), 100*1024*1024)
	}

	est := calc.QueryEstimate()
	// 10 unique files * 100MB = 1GB
	expected := int64(10 * 100 * 1024 * 1024)
	if est != expected {
		t.Errorf("query estimate = %d, want %d", est, expected)
	}
}

func TestSizingCalculator_QueryBased_Deduplicates(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	// Same file queried 5 times — should count once
	for i := 0; i < 5; i++ {
		calc.RecordQueryRead(42, 100*1024*1024)
	}

	est := calc.QueryEstimate()
	expected := int64(100 * 1024 * 1024)
	if est != expected {
		t.Errorf("query estimate = %d, want %d (should deduplicate)", est, expected)
	}
}

func TestSizingCalculator_BlendedEstimate(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	calc.RecordIngestion(2 * 1024 * 1024 * 1024) // 2GB
	calc.SetIngestionInterval(1 * time.Hour)

	for i := 0; i < 5; i++ {
		calc.RecordQueryRead(int64(i), 200*1024*1024) // 1GB total query reads
	}

	// At hour 0: 100% ingestion
	est0 := calc.BlendedEstimate(0)
	ingEst := calc.IngestionEstimate()
	if est0 != ingEst {
		t.Errorf("blended at hour 0 = %d, want ingestion estimate %d", est0, ingEst)
	}

	// At hour 12+: 100% query
	est12 := calc.BlendedEstimate(12 * time.Hour)
	qEst := calc.QueryEstimate()
	if est12 != qEst {
		t.Errorf("blended at hour 12 = %d, want query estimate %d", est12, qEst)
	}

	// At hour 6: ~50/50 blend
	est6 := calc.BlendedEstimate(6 * time.Hour)
	if est6 <= qEst || est6 >= ingEst {
		t.Errorf("blended at hour 6 = %d, expected between %d and %d", est6, qEst, ingEst)
	}
}

func TestSizingCalculator_FleetDivision(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	calc.RecordIngestion(3 * 1024 * 1024 * 1024) // 3GB/hour
	calc.SetIngestionInterval(1 * time.Hour)

	// Single node: full estimate
	single := calc.RecommendedPerNode(0, 1)
	// 3 nodes: each gets 1/3
	perNode := calc.RecommendedPerNode(0, 3)

	expected := single / 3
	tolerance := int64(float64(expected) * 0.01)
	if perNode < expected-tolerance || perNode > expected+tolerance {
		t.Errorf("per-node estimate = %d, want ~%d", perNode, expected)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestSizing'`
Expected: FAIL — `NewSizingCalculator` undefined

- [ ] **Step 3: Write the implementation**

```go
// internal/smartcache/sizing.go
package smartcache

import (
	"sync"
	"time"
)

type SizingConfig struct {
	TargetHours int
}

type SizingCalculator struct {
	mu               sync.RWMutex
	targetHours      int
	ingestionBytes   int64
	ingestionInterval time.Duration
	queryReads       map[int64]int64 // fileID → bytes
}

func NewSizingCalculator(cfg SizingConfig) *SizingCalculator {
	if cfg.TargetHours <= 0 {
		cfg.TargetHours = 24
	}
	return &SizingCalculator{
		targetHours: cfg.TargetHours,
		queryReads:  make(map[int64]int64),
	}
}

func (s *SizingCalculator) RecordIngestion(bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingestionBytes += bytes
}

func (s *SizingCalculator) SetIngestionInterval(d time.Duration) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.ingestionInterval = d
}

func (s *SizingCalculator) RecordQueryRead(fileID int64, bytes int64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.queryReads[fileID] = bytes
}

func (s *SizingCalculator) IngestionEstimate() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	if s.ingestionInterval <= 0 || s.ingestionBytes <= 0 {
		return 0
	}

	bytesPerHour := float64(s.ingestionBytes) / s.ingestionInterval.Hours()
	return int64(bytesPerHour * float64(s.targetHours))
}

func (s *SizingCalculator) QueryEstimate() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()

	var total int64
	for _, bytes := range s.queryReads {
		total += bytes
	}
	return total
}

func (s *SizingCalculator) BlendedEstimate(uptime time.Duration) int64 {
	ingEst := s.IngestionEstimate()
	qEst := s.QueryEstimate()

	if ingEst == 0 {
		return qEst
	}
	if qEst == 0 {
		return ingEst
	}

	hours := uptime.Hours()
	weight := hours / 12.0
	if weight > 1.0 {
		weight = 1.0
	}
	if weight < 0 {
		weight = 0
	}

	return int64(float64(1-weight)*float64(ingEst) + weight*float64(qEst))
}

func (s *SizingCalculator) RecommendedPerNode(uptime time.Duration, fleetSize int) int64 {
	total := s.BlendedEstimate(uptime)
	if fleetSize <= 1 {
		return total
	}
	return total / int64(fleetSize)
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestSizing'`
Expected: PASS — all 5 tests

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/smartcache/sizing.go internal/smartcache/sizing_test.go
git commit -m "feat(smartcache): add cache sizing calculator with ingestion/query blend"
```

---

### Task 8: Cross-Signal Client — Hint Batching and HTTP Send

**Files:**
- Create: `internal/crosssignal/client.go`
- Create: `internal/crosssignal/client_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/crosssignal/client_test.go
package crosssignal

import (
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

func TestClient_SendHint(t *testing.T) {
	var mu sync.Mutex
	var received []PrefetchHint

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/prefetch/hint" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		if r.Header.Get("X-Cross-Signal-Key") != "test-secret" {
			t.Errorf("missing or wrong auth key")
		}

		var hint PrefetchHint
		if err := json.NewDecoder(r.Body).Decode(&hint); err != nil {
			t.Errorf("decode error: %v", err)
		}
		mu.Lock()
		received = append(received, hint)
		mu.Unlock()

		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:      srv.URL,
		AuthKey:       "test-secret",
		Timeout:       2 * time.Second,
		MaxBatch:      100,
		BatchInterval: 50 * time.Millisecond,
	})
	defer client.Close()

	client.EnqueueHint([]string{"trace-1", "trace-2"}, 1000, 2000, "logs")

	// Wait for batch flush
	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 1 {
		t.Fatalf("expected 1 hint, got %d", len(received))
	}
	if len(received[0].TraceIDs) != 2 {
		t.Errorf("trace_ids = %d, want 2", len(received[0].TraceIDs))
	}
	if received[0].SourceSignal != "logs" {
		t.Errorf("source_signal = %q, want %q", received[0].SourceSignal, "logs")
	}
}

func TestClient_BatchAccumulation(t *testing.T) {
	var mu sync.Mutex
	var received []PrefetchHint

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var hint PrefetchHint
		_ = json.NewDecoder(r.Body).Decode(&hint)
		mu.Lock()
		received = append(received, hint)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:      srv.URL,
		AuthKey:       "",
		Timeout:       2 * time.Second,
		MaxBatch:      100,
		BatchInterval: 100 * time.Millisecond,
	})
	defer client.Close()

	// Add hints in rapid succession — should be batched together
	client.EnqueueHint([]string{"t1"}, 1000, 2000, "logs")
	client.EnqueueHint([]string{"t2"}, 1000, 2000, "logs")
	client.EnqueueHint([]string{"t3"}, 1000, 2000, "logs")

	time.Sleep(300 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(received) != 1 {
		t.Fatalf("expected 1 batched hint, got %d", len(received))
	}
	if len(received[0].TraceIDs) != 3 {
		t.Errorf("batched trace_ids = %d, want 3", len(received[0].TraceIDs))
	}
}

func TestClient_MaxBatchFlush(t *testing.T) {
	var mu sync.Mutex
	var received []PrefetchHint

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var hint PrefetchHint
		_ = json.NewDecoder(r.Body).Decode(&hint)
		mu.Lock()
		received = append(received, hint)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:      srv.URL,
		Timeout:       2 * time.Second,
		MaxBatch:      3,
		BatchInterval: 10 * time.Second, // long interval to ensure max_batch triggers flush
	})
	defer client.Close()

	// Enqueue 5 trace_ids — should flush after 3
	ids := []string{"t1", "t2", "t3", "t4", "t5"}
	client.EnqueueHint(ids, 1000, 2000, "logs")

	time.Sleep(200 * time.Millisecond)

	mu.Lock()
	defer mu.Unlock()

	if len(received) < 1 {
		t.Fatal("expected at least 1 flush from max_batch")
	}
	// First batch should have exactly MaxBatch (3) trace_ids
	if len(received[0].TraceIDs) != 3 {
		t.Errorf("first batch trace_ids = %d, want 3", len(received[0].TraceIDs))
	}
}

func TestClient_SendEvictionHint(t *testing.T) {
	var received EvictionHint

	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/internal/cache/evict-hint" {
			t.Errorf("unexpected path: %s", r.URL.Path)
		}
		_ = json.NewDecoder(r.Body).Decode(&received)
		w.WriteHeader(http.StatusOK)
	}))
	defer srv.Close()

	client := NewClient(ClientConfig{
		Endpoint:      srv.URL,
		Timeout:       2 * time.Second,
		MaxBatch:      100,
		BatchInterval: 50 * time.Millisecond,
	})
	defer client.Close()

	client.SendEvictionHint([]string{"evict-1", "evict-2"}, "logs")

	time.Sleep(100 * time.Millisecond)

	if len(received.TraceIDs) != 2 {
		t.Errorf("eviction hint trace_ids = %d, want 2", len(received.TraceIDs))
	}
}

func TestClient_NilEndpoint_NoOp(t *testing.T) {
	client := NewClient(ClientConfig{
		Endpoint: "", // no endpoint configured
	})
	defer client.Close()

	// Should not panic
	client.EnqueueHint([]string{"t1"}, 1000, 2000, "logs")
	client.SendEvictionHint([]string{"t1"}, "logs")
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/crosssignal/ -v`
Expected: FAIL — package does not exist

- [ ] **Step 3: Write the client implementation**

```go
// internal/crosssignal/client.go
package crosssignal

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type PrefetchHint struct {
	TraceIDs     []string `json:"trace_ids"`
	StartNs      int64    `json:"start_ns"`
	EndNs        int64    `json:"end_ns"`
	SourceSignal string   `json:"source_signal"`
}

type EvictionHint struct {
	TraceIDs     []string `json:"trace_ids"`
	SourceSignal string   `json:"source_signal"`
}

type ClientConfig struct {
	Endpoint      string
	AuthKey       string
	Timeout       time.Duration
	MaxBatch      int
	BatchInterval time.Duration
}

type Client struct {
	endpoint   string
	authKey    string
	httpClient *http.Client
	maxBatch   int

	mu            sync.Mutex
	pendingIDs    []string
	pendingStart  int64
	pendingEnd    int64
	pendingSignal string

	closed chan struct{}
	wg     sync.WaitGroup
}

func NewClient(cfg ClientConfig) *Client {
	if cfg.Timeout <= 0 {
		cfg.Timeout = 2 * time.Second
	}
	if cfg.MaxBatch <= 0 {
		cfg.MaxBatch = 100
	}
	if cfg.BatchInterval <= 0 {
		cfg.BatchInterval = 500 * time.Millisecond
	}

	c := &Client{
		endpoint: cfg.Endpoint,
		authKey:  cfg.AuthKey,
		httpClient: &http.Client{
			Timeout: cfg.Timeout,
		},
		maxBatch: cfg.MaxBatch,
		closed:   make(chan struct{}),
	}

	if cfg.Endpoint != "" {
		c.wg.Add(1)
		go c.batchLoop(cfg.BatchInterval)
	}

	return c
}

func (c *Client) EnqueueHint(traceIDs []string, startNs, endNs int64, sourceSignal string) {
	if c.endpoint == "" || len(traceIDs) == 0 {
		return
	}

	c.mu.Lock()
	c.pendingIDs = append(c.pendingIDs, traceIDs...)
	c.pendingStart = startNs
	c.pendingEnd = endNs
	c.pendingSignal = sourceSignal

	if len(c.pendingIDs) >= c.maxBatch {
		ids := c.pendingIDs[:c.maxBatch]
		c.pendingIDs = c.pendingIDs[c.maxBatch:]
		start, end, signal := c.pendingStart, c.pendingEnd, c.pendingSignal
		c.mu.Unlock()

		go c.sendPrefetchHint(PrefetchHint{
			TraceIDs:     ids,
			StartNs:      start,
			EndNs:        end,
			SourceSignal: signal,
		})
		return
	}
	c.mu.Unlock()
}

func (c *Client) SendEvictionHint(traceIDs []string, sourceSignal string) {
	if c.endpoint == "" || len(traceIDs) == 0 {
		return
	}

	go c.sendEvictionHint(EvictionHint{
		TraceIDs:     traceIDs,
		SourceSignal: sourceSignal,
	})
}

func (c *Client) batchLoop(interval time.Duration) {
	defer c.wg.Done()
	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-c.closed:
			c.flush()
			return
		case <-ticker.C:
			c.flush()
		}
	}
}

func (c *Client) flush() {
	c.mu.Lock()
	if len(c.pendingIDs) == 0 {
		c.mu.Unlock()
		return
	}
	ids := c.pendingIDs
	start, end, signal := c.pendingStart, c.pendingEnd, c.pendingSignal
	c.pendingIDs = nil
	c.mu.Unlock()

	c.sendPrefetchHint(PrefetchHint{
		TraceIDs:     ids,
		StartNs:      start,
		EndNs:        end,
		SourceSignal: signal,
	})
}

func (c *Client) sendPrefetchHint(hint PrefetchHint) {
	metrics.CrossPrefetchSent.Inc()

	body, err := json.Marshal(hint)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.httpClient.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/internal/prefetch/hint", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if c.authKey != "" {
		req.Header.Set("X-Cross-Signal-Key", c.authKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Infof("cross-signal prefetch hint failed: %s", err)
		return
	}
	_ = resp.Body.Close()
}

func (c *Client) sendEvictionHint(hint EvictionHint) {
	metrics.CrossEvictionSent.Inc()

	body, err := json.Marshal(hint)
	if err != nil {
		return
	}

	ctx, cancel := context.WithTimeout(context.Background(), c.httpClient.Timeout)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.endpoint+"/internal/cache/evict-hint", bytes.NewReader(body))
	if err != nil {
		return
	}
	req.Header.Set("Content-Type", "application/json")
	if c.authKey != "" {
		req.Header.Set("X-Cross-Signal-Key", c.authKey)
	}

	resp, err := c.httpClient.Do(req)
	if err != nil {
		logger.Infof("cross-signal eviction hint failed: %s", err)
		return
	}
	_ = resp.Body.Close()
}

func (c *Client) Close() {
	select {
	case <-c.closed:
	default:
		close(c.closed)
	}
	c.wg.Wait()
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/crosssignal/ -v`
Expected: PASS — all 5 tests

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/crosssignal/client.go internal/crosssignal/client_test.go
git commit -m "feat(crosssignal): add Client with hint batching and HTTP send"
```

---

### Task 9: Cross-Signal Handler — HTTP Endpoints

**Files:**
- Create: `internal/crosssignal/handler.go`
- Create: `internal/crosssignal/handler_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/crosssignal/handler_test.go
package crosssignal

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

type mockPrefetchRouter struct {
	enqueued atomic.Int64
}

func (m *mockPrefetchRouter) EnqueueCrossSignal(keys []string) int {
	m.enqueued.Add(int64(len(keys)))
	return len(keys)
}

type mockEvictionHandler struct {
	deprioritized atomic.Int64
}

func (m *mockEvictionHandler) DeprioritizeByTraceIDs(traceIDs []string) int {
	n := len(traceIDs)
	m.deprioritized.Add(int64(n))
	return n
}

func TestPrefetchHintHandler_ValidRequest(t *testing.T) {
	prefetch := &mockPrefetchRouter{}
	h := NewHandler(HandlerConfig{
		AuthKey:        "secret",
		PrefetchRouter: prefetch,
	})

	hint := PrefetchHint{
		TraceIDs:     []string{"trace-1", "trace-2"},
		StartNs:      1000,
		EndNs:        2000,
		SourceSignal: "logs",
	}
	body, _ := json.Marshal(hint)

	req := httptest.NewRequest(http.MethodPost, "/internal/prefetch/hint", bytes.NewReader(body))
	req.Header.Set("X-Cross-Signal-Key", "secret")
	w := httptest.NewRecorder()

	h.HandlePrefetchHint(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if prefetch.enqueued.Load() != 2 {
		t.Errorf("enqueued = %d, want 2", prefetch.enqueued.Load())
	}
}

func TestPrefetchHintHandler_Unauthorized(t *testing.T) {
	h := NewHandler(HandlerConfig{
		AuthKey: "secret",
	})

	req := httptest.NewRequest(http.MethodPost, "/internal/prefetch/hint", nil)
	// No auth header
	w := httptest.NewRecorder()

	h.HandlePrefetchHint(w, req)

	if w.Code != http.StatusUnauthorized {
		t.Errorf("status = %d, want 401", w.Code)
	}
}

func TestEvictHintHandler_ValidRequest(t *testing.T) {
	eviction := &mockEvictionHandler{}
	h := NewHandler(HandlerConfig{
		AuthKey:          "secret",
		EvictionHandler:  eviction,
	})

	hint := EvictionHint{
		TraceIDs:     []string{"t1", "t2", "t3"},
		SourceSignal: "logs",
	}
	body, _ := json.Marshal(hint)

	req := httptest.NewRequest(http.MethodPost, "/internal/cache/evict-hint", bytes.NewReader(body))
	req.Header.Set("X-Cross-Signal-Key", "secret")
	w := httptest.NewRecorder()

	h.HandleEvictHint(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", w.Code)
	}
	if eviction.deprioritized.Load() != 3 {
		t.Errorf("deprioritized = %d, want 3", eviction.deprioritized.Load())
	}
}

func TestEvictHintHandler_NoAuthKey_AllowAll(t *testing.T) {
	eviction := &mockEvictionHandler{}
	h := NewHandler(HandlerConfig{
		AuthKey:         "", // no auth configured
		EvictionHandler: eviction,
	})

	hint := EvictionHint{TraceIDs: []string{"t1"}, SourceSignal: "logs"}
	body, _ := json.Marshal(hint)

	req := httptest.NewRequest(http.MethodPost, "/internal/cache/evict-hint", bytes.NewReader(body))
	w := httptest.NewRecorder()

	h.HandleEvictHint(w, req)

	if w.Code != http.StatusOK {
		t.Errorf("status = %d, want 200 when no auth configured", w.Code)
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/crosssignal/ -v -run 'TestPrefetchHint|TestEvictHint'`
Expected: FAIL — `NewHandler` undefined

- [ ] **Step 3: Write the handler implementation**

```go
// internal/crosssignal/handler.go
package crosssignal

import (
	"encoding/json"
	"net/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

type PrefetchRouter interface {
	EnqueueCrossSignal(keys []string) int
}

type EvictionRouter interface {
	DeprioritizeByTraceIDs(traceIDs []string) int
}

type HandlerConfig struct {
	AuthKey         string
	PrefetchRouter  PrefetchRouter
	EvictionHandler EvictionRouter
}

type Handler struct {
	authKey         string
	prefetchRouter  PrefetchRouter
	evictionHandler EvictionRouter
}

func NewHandler(cfg HandlerConfig) *Handler {
	return &Handler{
		authKey:         cfg.AuthKey,
		prefetchRouter:  cfg.PrefetchRouter,
		evictionHandler: cfg.EvictionHandler,
	}
}

func (h *Handler) HandlePrefetchHint(w http.ResponseWriter, r *http.Request) {
	if !h.checkAuth(w, r) {
		return
	}

	var hint PrefetchHint
	if err := json.NewDecoder(r.Body).Decode(&hint); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	metrics.CrossPrefetchReceived.Inc()

	if h.prefetchRouter != nil && len(hint.TraceIDs) > 0 {
		h.prefetchRouter.EnqueueCrossSignal(hint.TraceIDs)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) HandleEvictHint(w http.ResponseWriter, r *http.Request) {
	if !h.checkAuth(w, r) {
		return
	}

	var hint EvictionHint
	if err := json.NewDecoder(r.Body).Decode(&hint); err != nil {
		http.Error(w, "bad request", http.StatusBadRequest)
		return
	}

	metrics.CrossEvictionReceived.Inc()

	if h.evictionHandler != nil && len(hint.TraceIDs) > 0 {
		n := h.evictionHandler.DeprioritizeByTraceIDs(hint.TraceIDs)
		metrics.CrossEvictionApplied.Add(n)
	}

	w.WriteHeader(http.StatusOK)
}

func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/internal/prefetch/hint", h.HandlePrefetchHint)
	mux.HandleFunc("/internal/cache/evict-hint", h.HandleEvictHint)
}

func (h *Handler) checkAuth(w http.ResponseWriter, r *http.Request) bool {
	if h.authKey == "" {
		return true
	}
	if r.Header.Get("X-Cross-Signal-Key") != h.authKey {
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return false
	}
	return true
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/crosssignal/ -v`
Expected: PASS — all tests

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/crosssignal/handler.go internal/crosssignal/handler_test.go
git commit -m "feat(crosssignal): add HTTP handlers for prefetch and eviction hints"
```

---

### Task 10: Storage Integration — Replace getFileData with SmartCacheController

**Files:**
- Modify: `internal/storage/parquets3/storage.go`

This task reroutes `getFileData` through the SmartCacheController. The existing L1→L2→L3→S3 chain is replaced by `Controller.Get()` which handles hash routing, metadata tracking, and pin management.

- [ ] **Step 1: Add smartCache field to Storage struct**

In `internal/storage/parquets3/storage.go`, add import and field:

```go
// Add to imports:
"github.com/ReliablyObserve/victoria-lakehouse/internal/smartcache"

// Add to Storage struct (after tombstones field):
smartCache *smartcache.Controller
```

- [ ] **Step 2: Initialize SmartCacheController in New()**

After the `peerCache` initialization (around line 108), add SmartCacheController creation:

```go
	var sc *smartcache.Controller
	if cfg.SelectEnabled() {
		metaMap := smartcache.NewMetadataMap()

		snapshotPath := ""
		if cfg.Cache.DiskPath != "" {
			snapshotPath = cfg.Cache.DiskPath + "/smartcache.meta.json"
			if err := metaMap.LoadSnapshot(snapshotPath); err != nil {
				logger.Warnf("failed to load cache metadata snapshot: %s", err)
			}
		}

		var peerLookupImpl smartcache.PeerLookup
		var peerFetchImpl smartcache.PeerFetcher
		if pc != nil {
			peerLookupImpl = &peerLookupAdapter{pc: pc}
			peerFetchImpl = &peerFetchAdapter{pc: pc}
		} else {
			peerLookupImpl = &localOnlyLookup{}
			peerFetchImpl = nil
		}

		sc = smartcache.NewController(smartcache.ControllerConfig{
			L1:           &l1Adapter{lru: memCache},
			L2:           &l2Adapter{dc: diskCacheInst},
			PeerLookup:   peerLookupImpl,
			PeerFetcher:  peerFetchImpl,
			S3Fetcher:    &s3Adapter{pool: pool},
			Metadata:     metaMap,
			MaxAge:       cfg.SmartCache.MaxAge,
			HotThreshold: cfg.SmartCache.HotAccessThreshold,
			HotWindow:    cfg.SmartCache.HotWindow,
			GracePeriod:  cfg.SmartCache.QueryGracePeriod,
			Signal:       string(cfg.Mode),
		})
	}
```

Add the adapter types at the end of storage.go:

```go
type l1Adapter struct{ lru *cache.LRU }
func (a *l1Adapter) Get(key string) ([]byte, bool) { return a.lru.Get(key) }
func (a *l1Adapter) Put(key string, val []byte)     { a.lru.Put(key, val) }

type l2Adapter struct{ dc *cache.DiskCache }
func (a *l2Adapter) Get(key string) ([]byte, bool) {
	if a.dc == nil { return nil, false }
	path, ok := a.dc.Get(key)
	if !ok { return nil, false }
	data, err := os.ReadFile(path)
	if err != nil { a.dc.Delete(key); return nil, false }
	return data, true
}
func (a *l2Adapter) Put(key string, data []byte) error {
	if a.dc == nil { return nil }
	_, err := a.dc.Put(key, data)
	return err
}
func (a *l2Adapter) Delete(key string) {
	if a.dc != nil { a.dc.Delete(key) }
}
func (a *l2Adapter) Size() int64 {
	if a.dc == nil { return 0 }
	return a.dc.Size()
}

type peerLookupAdapter struct{ pc *peercache.PeerCache }
func (a *peerLookupAdapter) Lookup(key string) (string, bool) { return a.pc.Lookup(key) }
func (a *peerLookupAdapter) Members() []string { return a.pc.Members() }
func (a *peerLookupAdapter) MemberCount() int { return len(a.pc.Members()) }

type peerFetchAdapter struct{ pc *peercache.PeerCache }
func (a *peerFetchAdapter) Fetch(ctx context.Context, peer, key string) ([]byte, bool, error) {
	return a.pc.Fetch(ctx, peer, key)
}

type localOnlyLookup struct{}
func (l *localOnlyLookup) Lookup(key string) (string, bool) { return "self", true }
func (l *localOnlyLookup) Members() []string { return []string{"self"} }
func (l *localOnlyLookup) MemberCount() int { return 1 }

type s3Adapter struct{ pool *s3reader.ClientPool }
func (a *s3Adapter) Download(ctx context.Context, key string) ([]byte, error) {
	return a.pool.Download(ctx, key)
}
```

- [ ] **Step 3: Replace getFileData to use SmartCacheController**

Replace the existing `getFileData` method (lines 176-244):

```go
func (s *Storage) getFileData(ctx context.Context, key string, size int64) ([]byte, error) {
	if s.smartCache != nil {
		return s.smartCache.Get(ctx, key, size)
	}

	// Fallback: original L1→L2→L3→S3 chain (for insert-only nodes)
	if data, ok := s.memCache.Get(key); ok {
		metrics.CacheHitsTotal.Inc("L1")
		return data, nil
	}
	metrics.CacheMissesTotal.Inc("L1")

	if s.diskCache != nil {
		if path, ok := s.diskCache.Get(key); ok {
			data, err := os.ReadFile(path)
			if err == nil {
				metrics.CacheHitsTotal.Inc("L2")
				s.memCache.Put(key, data)
				return data, nil
			}
			s.diskCache.Delete(key)
		}
		metrics.CacheMissesTotal.Inc("L2")
	}

	if s.peerCache != nil {
		peer, isLocal := s.peerCache.Lookup(key)
		if !isLocal {
			metrics.PeerRequestsTotal.Inc("fetch")
			peerData, found, peerErr := s.peerCache.Fetch(ctx, peer, key)
			if peerErr == nil && found {
				metrics.CacheHitsTotal.Inc("L3")
				metrics.PeerHitsTotal.Inc()
				metrics.PeerBytesTransferred.Add("rx", len(peerData))
				s.memCache.Put(key, peerData)
				return peerData, nil
			}
			metrics.CacheMissesTotal.Inc("L3")
		}
	}

	data, err, shared := s.sfGroup.Do(key, func() ([]byte, error) {
		s3Start := time.Now()
		metrics.S3RequestsTotal.Inc("GET")
		d, dlErr := s.pool.Download(ctx, key)
		metrics.S3RequestDuration.Observe(time.Since(s3Start).Seconds())
		if dlErr != nil {
			metrics.S3ErrorsTotal.Inc("GET")
			return nil, dlErr
		}
		metrics.S3BytesReadTotal.Add(len(d))

		if s.diskCache != nil {
			if _, putErr := s.diskCache.Put(key, d); putErr != nil {
				logger.Warnf("disk cache put failed: %s; key=%s", putErr, key)
			}
		}

		if s.peerHandler != nil {
			s.peerHandler.Put(key, d)
		}

		return d, nil
	})
	if err != nil {
		return nil, err
	}
	if shared {
		metrics.CacheSingleflightDedup.Inc()
	}

	s.memCache.Put(key, data)
	return data, nil
}
```

Add `smartCache` to the Storage struct init return and add accessor:

```go
// In the return statement of New():
smartCache: sc,

// New accessor method:
func (s *Storage) SmartCache() *smartcache.Controller {
	return s.smartCache
}
```

- [ ] **Step 4: Verify compilation**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go build ./internal/storage/parquets3/`
Expected: Success

- [ ] **Step 5: Run existing tests for regressions**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/storage/parquets3/ -v -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/storage/parquets3/storage.go
git commit -m "feat(storage): route getFileData through SmartCacheController with hash routing"
```

---

### Task 11: Parallel File Workers in RunQuery

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go`

This task replaces the sequential file processing loop in `RunQuery` with a bounded worker pool.

- [ ] **Step 1: Replace sequential loop with parallel workers**

In `internal/storage/parquets3/storage_query.go`, replace the sequential for loop (lines 65-73) with parallel workers:

```go
	// Replace lines 65-73 with:

	fileWorkers := s.cfg.Query.FileWorkers
	if fileWorkers <= 0 {
		fileWorkers = 8
	}
	if fileWorkers > len(files) {
		fileWorkers = len(files)
	}

	queryID := fmt.Sprintf("q-%d", queryStart.UnixNano())

	// Pin files in smart cache for the duration of this query
	if s.smartCache != nil {
		for _, fi := range files {
			s.smartCache.Pin(fi.Key, queryID)
		}
		defer func() {
			for _, fi := range files {
				s.smartCache.Unpin(fi.Key, queryID)
			}
		}()
	}

	type fileTask struct {
		fi manifest.FileInfo
	}

	taskCh := make(chan fileTask, len(files))
	for _, fi := range files {
		taskCh <- fileTask{fi: fi}
	}
	close(taskCh)

	var wg sync.WaitGroup
	var queryErr atomic.Value

	// Use a mutex to serialize writeBlock calls from workers
	var writeMu sync.Mutex
	safeWriteBlock := func(workerID uint, db *logstorage.DataBlock) {
		writeMu.Lock()
		defer writeMu.Unlock()
		filteredWriteBlock(workerID, db)
	}

	for w := 0; w < fileWorkers; w++ {
		wg.Add(1)
		workerID := uint(w)
		go func() {
			defer wg.Done()
			for task := range taskCh {
				if err := ctx.Err(); err != nil {
					queryErr.Store(err)
					return
				}
				if err := s.queryFile(ctx, task.fi, startNs, endNs, queryStr, safeWriteBlock); err != nil {
					logger.Warnf("query file error: %s; key=%s", err, task.fi.Key)
					continue
				}
				_ = workerID
			}
		}()
	}

	wg.Wait()

	if errVal := queryErr.Load(); errVal != nil {
		if err, ok := errVal.(error); ok && err != nil {
			return err
		}
	}
```

Add required imports at the top:

```go
"sync"
"sync/atomic"
```

- [ ] **Step 2: Verify compilation**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go build ./internal/storage/parquets3/`
Expected: Success

- [ ] **Step 3: Run existing tests**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/storage/parquets3/ -v -count=1`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/storage/parquets3/storage_query.go
git commit -m "feat(query): parallel file workers with configurable concurrency and pin tracking"
```

---

### Task 12: Trace ID Extraction in queryFile

**Files:**
- Modify: `internal/storage/parquets3/storage_query.go`

This task adds trace_id extraction from query results to feed the prefetch engine and cross-signal hints.

- [ ] **Step 1: Add trace_id extraction to readRowGroup**

In `readRowGroup` (line 142), add trace_id collection. Modify the method signature to accept a trace_id collector:

```go
func (s *Storage) readRowGroup(f *parquet.File, rg parquet.RowGroup, startNs, endNs int64, writeBlock logstorage.WriteDataBlockFunc, traceIDs *[]string) error {
```

After the `writeBlock` call, extract trace_ids from the DataBlock:

```go
		if db != nil && db.RowsCount() > 0 {
			writeBlock(0, db)
			if traceIDs != nil {
				extractTraceIDs(db, traceIDs)
			}
		}
```

Add the extraction function:

```go
func extractTraceIDs(db *logstorage.DataBlock, dest *[]string) {
	cols := db.GetColumns(false)
	for _, col := range cols {
		if col.Name != "trace_id" {
			continue
		}
		seen := make(map[string]bool)
		for _, v := range col.Values {
			if v != "" && !seen[v] && len(*dest) < 200 {
				seen[v] = true
				*dest = append(*dest, v)
			}
		}
		return
	}
}
```

- [ ] **Step 2: Update queryFile to collect and forward trace_ids**

Modify `queryFile` to collect trace_ids and pass them back:

```go
func (s *Storage) queryFile(ctx context.Context, fi manifest.FileInfo, startNs, endNs int64, queryStr string, writeBlock logstorage.WriteDataBlockFunc) error {
	// ... existing file open code unchanged through bloom filter check ...

	var collectedTraceIDs []string

	for _, rg := range f.RowGroups() {
		if err := ctx.Err(); err != nil {
			return err
		}
		if tsIdx >= 0 && !rowGroupMatchesTimeRange(rg, tsIdx, startNs, endNs) {
			metrics.ParquetRowGroupsSkipped.Inc("stats")
			continue
		}
		if s.bloomFilterSkip(f, rg, bloomChecks) {
			metrics.ParquetRowGroupsSkipped.Inc("bloom")
			continue
		}
		metrics.ParquetRowGroupsScanned.Inc()
		if err := s.readRowGroup(f, rg, startNs, endNs, writeBlock, &collectedTraceIDs); err != nil {
			return err
		}
	}

	// Record trace_ids in cache metadata for connected eviction
	if s.smartCache != nil && len(collectedTraceIDs) > 0 {
		s.smartCache.RecordTraceIDs(fi.Key, collectedTraceIDs)
	}

	return nil
}
```

- [ ] **Step 3: Verify compilation and tests**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/storage/parquets3/ -v -count=1`
Expected: PASS

- [ ] **Step 4: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/storage/parquets3/storage_query.go
git commit -m "feat(query): extract trace_ids from query results for prefetch and cross-signal hints"
```

---

### Task 13: Eviction Background Goroutine on Controller

**Files:**
- Modify: `internal/smartcache/controller.go`
- Create: `internal/smartcache/controller_eviction_test.go`

- [ ] **Step 1: Write the failing test**

```go
// internal/smartcache/controller_eviction_test.go
package smartcache

import (
	"testing"
	"time"
)

func TestController_EvictionLoop_RemovesExpired(t *testing.T) {
	l1 := newMockL1()
	l2 := newMockL2()
	meta := NewMetadataMap()

	// Add an expired entry
	meta.Set("expired-key", EntryMeta{
		CreatedAt:         time.Now().Add(-2 * time.Hour),
		LastAccess:        time.Now().Add(-2 * time.Hour),
		AccessCount:       0,
		AccessWindowStart: time.Now().Add(-2 * time.Hour),
		Signal:            "logs",
		Size:              100,
	})
	l2.Put("expired-key", make([]byte, 100))

	// Add a fresh entry
	meta.Set("fresh-key", EntryMeta{
		CreatedAt:         time.Now(),
		LastAccess:        time.Now(),
		AccessCount:       0,
		AccessWindowStart: time.Now(),
		Signal:            "logs",
		Size:              200,
	})
	l2.Put("fresh-key", make([]byte, 200))

	ctrl := NewController(ControllerConfig{
		L1:           l1,
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       1 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	ctrl.RunEvictionOnce()

	if _, ok := meta.Get("expired-key"); ok {
		t.Error("expected expired entry to be evicted")
	}
	if _, ok := l2.Get("expired-key"); ok {
		t.Error("expected expired entry to be removed from L2")
	}
	if _, ok := meta.Get("fresh-key"); !ok {
		t.Error("expected fresh entry to survive eviction")
	}
}
```

- [ ] **Step 2: Run test to verify it fails**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestController_Eviction'`
Expected: FAIL — `RunEvictionOnce` undefined

- [ ] **Step 3: Add eviction methods to Controller**

Add to `internal/smartcache/controller.go`:

```go
func (c *Controller) RunEvictionOnce() []string {
	expired := CollectExpired(c.metadata, c.maxAge, c.hotThreshold, c.hotWindow)
	for _, key := range expired {
		meta, ok := c.metadata.Get(key)
		if !ok {
			continue
		}
		c.l2.Delete(key)
		c.metadata.Delete(key)
		_ = meta // will be used for cross-signal eviction hints later
	}
	return expired
}

func (c *Controller) StartEvictionLoop(interval time.Duration, stop <-chan struct{}) {
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				return
			case <-ticker.C:
				c.RunEvictionOnce()
			}
		}
	}()
}

func (c *Controller) StartSnapshotLoop(path string, interval time.Duration, stop <-chan struct{}) {
	if path == "" {
		return
	}
	go func() {
		ticker := time.NewTicker(interval)
		defer ticker.Stop()
		for {
			select {
			case <-stop:
				_ = c.metadata.SaveSnapshot(path)
				return
			case <-ticker.C:
				if err := c.metadata.SaveSnapshot(path); err != nil {
					// logged by caller's context
				}
			}
		}
	}()
}

func (c *Controller) DeprioritizeByTraceIDs(traceIDs []string) int {
	traceSet := make(map[string]bool, len(traceIDs))
	for _, id := range traceIDs {
		traceSet[id] = true
	}

	all := c.metadata.All()
	deprioritized := 0
	for key, meta := range all {
		for _, tid := range meta.TraceIDs {
			if traceSet[tid] {
				meta.LastAccess = time.Time{} // move to back of LRU
				meta.AccessCount = 0
				c.metadata.Set(key, meta)
				deprioritized++
				break
			}
		}
	}
	return deprioritized
}
```

- [ ] **Step 4: Run test to verify it passes**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestController_Eviction'`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/smartcache/controller.go internal/smartcache/controller_eviction_test.go
git commit -m "feat(smartcache): add eviction loop, snapshot persistence, and cross-signal deprioritization"
```

---

### Task 14: Wire SmartCacheController into lakehouse-logs main.go

**Files:**
- Modify: `cmd/lakehouse-logs/main.go`

- [ ] **Step 1: Add cross-signal handler registration to mux**

In `newMux()`, after the peer handler registration (around line 355), add:

```go
	// Cross-signal handlers
	if cfg.CrossSignal.Enabled && cfg.SelectEnabled() {
		var prefetchRouter crosssignal.PrefetchRouter
		// Will be wired when prefetch engine is integrated
		var evictionRouter crosssignal.EvictionRouter
		if sc := store.SmartCache(); sc != nil {
			evictionRouter = sc
		}

		csHandler := crosssignal.NewHandler(crosssignal.HandlerConfig{
			AuthKey:         cfg.CrossSignal.AuthKey,
			PrefetchRouter:  prefetchRouter,
			EvictionHandler: evictionRouter,
		})
		csHandler.Register(mux)
	}
```

Add imports:

```go
"github.com/ReliablyObserve/victoria-lakehouse/internal/crosssignal"
```

- [ ] **Step 2: Start eviction and snapshot loops in run()**

After `store.SetTombstoneStore(tombstoneStore)` (around line 193), add:

```go
	stopCh := make(chan struct{})
	defer close(stopCh)

	if sc := store.SmartCache(); sc != nil {
		sc.StartEvictionLoop(30*time.Second, stopCh)
		snapshotPath := ""
		if cfg.Cache.DiskPath != "" {
			snapshotPath = cfg.Cache.DiskPath + "/smartcache.meta.json"
		}
		sc.StartSnapshotLoop(snapshotPath, cfg.SmartCache.SnapshotInterval, stopCh)
		logger.Infof("smart cache started; max_age=%v, snapshot_interval=%v",
			cfg.SmartCache.MaxAge, cfg.SmartCache.SnapshotInterval)
	}
```

- [ ] **Step 3: Verify compilation**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go build ./cmd/lakehouse-logs/`
Expected: Success

- [ ] **Step 4: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add cmd/lakehouse-logs/main.go
git commit -m "feat(logs): wire SmartCacheController, eviction loop, and cross-signal handlers"
```

---

### Task 15: Mirror Storage Changes to lakehouse-traces Module

**Files:**
- Modify: `lakehouse-traces/internal/storage/parquets3/storage.go`
- Modify: `lakehouse-traces/internal/storage/parquets3/storage_query.go`
- Modify: `lakehouse-traces/main.go`

The traces module has its own copy of the storage package. Apply the same changes from Tasks 10, 11, 12, and 14.

- [ ] **Step 1: Apply storage.go changes to traces module**

Copy the same changes from Task 10 to `lakehouse-traces/internal/storage/parquets3/storage.go`:
- Add `smartcache` import
- Add `smartCache *smartcache.Controller` field
- Add adapter types (`l1Adapter`, `l2Adapter`, `peerLookupAdapter`, etc.)
- Initialize SmartCacheController in `New()`
- Replace `getFileData` to use SmartCacheController with fallback

- [ ] **Step 2: Apply storage_query.go changes to traces module**

Copy the same changes from Tasks 11 and 12 to `lakehouse-traces/internal/storage/parquets3/storage_query.go`:
- Parallel file workers in `RunQuery`
- Trace_id extraction in `readRowGroup` and `queryFile`
- `extractTraceIDs` helper function

- [ ] **Step 3: Apply main.go changes to traces module**

Copy the same changes from Task 14 to `lakehouse-traces/main.go`:
- Add cross-signal handler registration
- Start eviction and snapshot loops
- Add `crosssignal` import

- [ ] **Step 4: Verify traces module compilation**

Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && go build .`
Expected: Success

- [ ] **Step 5: Run traces module tests**

Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && go test ./internal/storage/parquets3/ -v -count=1`
Expected: PASS

- [ ] **Step 6: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add lakehouse-traces/internal/storage/parquets3/storage.go \
        lakehouse-traces/internal/storage/parquets3/storage_query.go \
        lakehouse-traces/main.go
git commit -m "feat(traces): mirror smart cache, parallel query, and cross-signal changes"
```

---

### Task 16: Integration Test — Full Round-Trip

**Files:**
- Create: `internal/smartcache/integration_test.go`

- [ ] **Step 1: Write integration test**

```go
// internal/smartcache/integration_test.go
package smartcache

import (
	"context"
	"fmt"
	"testing"
	"time"
)

func TestIntegration_GetPinEvict(t *testing.T) {
	l1 := newMockL1()
	l2 := newMockL2()
	s3 := newMockS3()
	meta := NewMetadataMap()

	// Pre-populate S3
	s3.data["file-a"] = []byte("data-a")
	s3.data["file-b"] = []byte("data-b")

	ctrl := NewController(ControllerConfig{
		L1:           l1,
		L2:           l2,
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    s3,
		Metadata:     meta,
		MaxAge:       100 * time.Millisecond, // very short TTL for test
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		GracePeriod:  50 * time.Millisecond,
		Signal:       "logs",
	})

	ctx := context.Background()

	// 1. Get file-a — downloads from S3
	data, err := ctrl.Get(ctx, "file-a", 6)
	if err != nil {
		t.Fatalf("Get file-a: %v", err)
	}
	if string(data) != "data-a" {
		t.Errorf("file-a data = %q, want %q", string(data), "data-a")
	}

	// 2. Pin file-a
	ctrl.Pin("file-a", "query-1")

	// 3. Wait for TTL to expire
	time.Sleep(150 * time.Millisecond)

	// 4. Run eviction — file-a should survive because it's pinned
	evicted := ctrl.RunEvictionOnce()
	if contains(evicted, "file-a") {
		t.Error("file-a should not be evicted while pinned")
	}

	// 5. Unpin file-a
	ctrl.Unpin("file-a", "query-1")

	// 6. Wait for grace period to expire
	time.Sleep(100 * time.Millisecond)

	// 7. Run eviction again — file-a should now be evicted
	evicted = ctrl.RunEvictionOnce()
	if !contains(evicted, "file-a") {
		t.Error("file-a should be evicted after unpin + grace period")
	}

	// 8. Get file-a again — should re-download from S3
	data, err = ctrl.Get(ctx, "file-a", 6)
	if err != nil {
		t.Fatalf("re-Get file-a: %v", err)
	}
	if string(data) != "data-a" {
		t.Errorf("re-Get data = %q, want %q", string(data), "data-a")
	}
	if s3.calls.Load() != 2 {
		t.Errorf("S3 calls = %d, want 2 (original + re-download)", s3.calls.Load())
	}
}

func TestIntegration_TraceIDRecordAndDeprioritize(t *testing.T) {
	meta := NewMetadataMap()
	meta.Set("log-file", EntryMeta{
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		Signal:     "logs",
		Size:       100,
		TraceIDs:   []string{"trace-abc", "trace-def"},
	})
	meta.Set("unrelated-file", EntryMeta{
		CreatedAt:  time.Now(),
		LastAccess: time.Now(),
		Signal:     "logs",
		Size:       200,
	})

	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       24 * time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	n := ctrl.DeprioritizeByTraceIDs([]string{"trace-abc"})
	if n != 1 {
		t.Errorf("deprioritized = %d, want 1", n)
	}

	got, _ := meta.Get("log-file")
	if got.AccessCount != 0 {
		t.Errorf("access count = %d, want 0 after deprioritization", got.AccessCount)
	}
	if !got.LastAccess.IsZero() {
		t.Error("expected LastAccess to be zeroed after deprioritization")
	}

	// Unrelated file should be unchanged
	unrelated, _ := meta.Get("unrelated-file")
	if unrelated.LastAccess.IsZero() {
		t.Error("unrelated file should not be affected")
	}
}

func TestIntegration_SizingCalculator(t *testing.T) {
	calc := NewSizingCalculator(SizingConfig{TargetHours: 24})

	// Simulate 2GB/hour ingestion
	calc.RecordIngestion(2 * 1024 * 1024 * 1024)
	calc.SetIngestionInterval(1 * time.Hour)

	// At startup: should recommend based on ingestion
	est := calc.RecommendedPerNode(0, 3)
	expectedPerNode := int64(2 * 1024 * 1024 * 1024 * 24 / 3) // 2GB/h * 24h / 3 nodes
	tolerance := int64(float64(expectedPerNode) * 0.01)
	if est < expectedPerNode-tolerance || est > expectedPerNode+tolerance {
		t.Errorf("per-node estimate = %d, want ~%d", est, expectedPerNode)
	}

	// Simulate query data (less than ingestion)
	for i := 0; i < 100; i++ {
		calc.RecordQueryRead(int64(i), 10*1024*1024) // 100 files * 10MB = 1GB
	}

	// After 12h: should be entirely query-based
	est12 := calc.RecommendedPerNode(12*time.Hour, 3)
	queryPerNode := int64(100 * 10 * 1024 * 1024 / 3)
	tolerance = int64(float64(queryPerNode) * 0.01)
	if est12 < queryPerNode-tolerance || est12 > queryPerNode+tolerance {
		t.Errorf("12h per-node estimate = %d, want ~%d", est12, queryPerNode)
	}
}

func contains(s []string, v string) bool {
	for _, item := range s {
		if item == v {
			return true
		}
	}
	return false
}

// Ensure mockS3Fetcher implements S3Fetcher
var _ S3Fetcher = (*mockS3Fetcher)(nil)

// Ensure mockL1 implements L1Cache
var _ L1Cache = (*mockL1)(nil)

// Ensure mockL2 implements L2Cache
var _ L2Cache = (*mockL2)(nil)

// Compile-time interface check: Controller implements EvictionRouter
func TestController_ImplementsEvictionRouter(t *testing.T) {
	meta := NewMetadataMap()
	ctrl := NewController(ControllerConfig{
		L1:           newMockL1(),
		L2:           newMockL2(),
		PeerLookup:   &mockPeerLookup{selfAddr: "self:9428"},
		PeerFetcher:  &mockPeerFetcher{},
		S3Fetcher:    newMockS3(),
		Metadata:     meta,
		MaxAge:       time.Hour,
		HotThreshold: 3,
		HotWindow:    10 * time.Minute,
		Signal:       "logs",
	})

	// This compiles only if Controller has DeprioritizeByTraceIDs([]string) int
	var n int = ctrl.DeprioritizeByTraceIDs([]string{"test"})
	_ = n
	_ = fmt.Sprintf("compiles")
}
```

- [ ] **Step 2: Run integration tests**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/smartcache/ -v -run 'TestIntegration'`
Expected: PASS — all 4 tests

- [ ] **Step 3: Run full test suite**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./internal/... -count=1`
Expected: PASS — no regressions across all internal packages

- [ ] **Step 4: Commit**

```bash
cd /private/tmp/victoria-lakehouse-fresh
git add internal/smartcache/integration_test.go
git commit -m "test(smartcache): add integration tests for get/pin/evict, trace_id deprioritization, sizing"
```

---

### Task 17: Build Verification — Both Binaries Compile

**Files:** None (verification only)

- [ ] **Step 1: Build lakehouse-logs**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go build ./cmd/lakehouse-logs/`
Expected: Success

- [ ] **Step 2: Build lakehouse-traces**

Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && go build .`
Expected: Success

- [ ] **Step 3: Run full test suites for both modules**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go test ./... -count=1`
Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && go test ./... -count=1`
Expected: PASS for both

- [ ] **Step 4: Verify no linting issues**

Run: `cd /private/tmp/victoria-lakehouse-fresh && go vet ./...`
Run: `cd /private/tmp/victoria-lakehouse-fresh/lakehouse-traces && go vet ./...`
Expected: No errors

---

## Verification Checklist

1. `go test ./internal/smartcache/ -v` — all metadata, eviction, controller, sizing, integration tests pass
2. `go test ./internal/crosssignal/ -v` — all client and handler tests pass
3. `go test ./internal/prefetch/ -v` — TypeCrossSignal + priority dequeue tests pass
4. `go test ./internal/config/ -v` — new config structs + defaults tests pass
5. `go build ./cmd/lakehouse-logs/` — logs binary compiles
6. `cd lakehouse-traces && go build .` — traces binary compiles
7. `go test ./... -count=1` — full root module test suite
8. `cd lakehouse-traces && go test ./... -count=1` — full traces module test suite
