# VT v0.9.0 Upgrade & Upstream Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Upgrade VictoriaTraces from v0.8.2 to v0.9.0, replace ~629 lines of custom Jaeger handlers with VT's upstream implementation, add Tempo API + Drilldown support, and fix shared code drift between logs/traces modules.

**Architecture:** Fork VT v0.9.0 into `lakehouse-traces/deps/VictoriaTraces`, add a thin `ExternalStorage` interface to `vtstorage` that intercepts all 8 dispatch functions, then import VT's Jaeger and Tempo `RequestHandler` functions unmodified. Lakehouse-traces plugs its S3/Parquet backend via `vtstorage.SetExternalStorage()`.

**Tech Stack:** Go 1.26, VictoriaTraces v0.9.0, VictoriaLogs (forked at commit a408207c2242), S3/Parquet storage

**Branch:** `fix/lifecycle-endpoint-wiring` (PR #93)

**Build requirement:** `GOWORK=off` mandatory for all builds and tests.

---

## File Structure

### New Files
| File | Purpose |
|------|---------|
| `lakehouse-traces/deps/VictoriaTraces/` | VT v0.9.0 fork (cloned from upstream) |
| `lakehouse-traces/deps/VictoriaTraces/app/vtstorage/external.go` | ExternalStorage interface + SetExternalStorage() |
| `lakehouse-traces/internal/vtstorage_adapter/adapter.go` | Bridge storage.Storage → VT ExternalStorage |
| `lakehouse-traces/internal/vtstorage_adapter/adapter_test.go` | Adapter delegation tests |

### Modified Files
| File | Change |
|------|--------|
| `lakehouse-traces/deps/VictoriaTraces/app/vtstorage/main.go` | 8 guard clauses for externalStorage |
| `lakehouse-traces/deps/VictoriaTraces/go.mod` | Add VL replace directive |
| `lakehouse-traces/go.mod` | Add VT replace directive, bump version |
| `lakehouse-traces/main.go` | Initialize vtstorage adapter, import VT handlers |
| `lakehouse-traces/internal/selectapi/handler.go` | Replace custom Jaeger routes with VT RequestHandler |
| `lakehouse-traces/internal/vlstorage/vlstorage.go` | Add missing `filter` param to 4 methods |
| `deployment/docker/docker-compose-e2e.yml` | Bump VT image v0.8.2 → v0.9.0 |

### Deleted Files
| File | Lines | Reason |
|------|-------|--------|
| `lakehouse-traces/internal/selectapi/jaeger.go` | 629 | Replaced by VT upstream jaeger.RequestHandler |
| `lakehouse-traces/internal/selectapi/jaeger_test.go` | ~750 | Tests for deleted custom handlers |
| `lakehouse-traces/internal/selectapi/jaeger_fuzz_test.go` | ~50 | Fuzz tests for deleted handlers |
| `lakehouse-traces/internal/selectapi/jaeger_verify_test.go` | ~200 | Verify tests for deleted handlers |
| `lakehouse-traces/internal/storage/interface.go` | 31 | Deduplicated — import from root module |

---

### Task 1: Clone VT v0.9.0 into deps/VictoriaTraces

**Files:**
- Create: `lakehouse-traces/deps/VictoriaTraces/` (clone from `/tmp/vt-0.9.0/`)
- Modify: `lakehouse-traces/deps/VictoriaTraces/go.mod`
- Modify: `lakehouse-traces/go.mod`

- [ ] **Step 1: Copy VT v0.9.0 source into deps**

```bash
cp -r /tmp/vt-0.9.0 /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces/deps/VictoriaTraces
```

- [ ] **Step 2: Add VL replace directive to VT fork's go.mod**

The VT fork must use the same VL as the rest of lakehouse-traces. VT v0.9.0 depends on VL at commit `a408207c2242` — which is exactly the commit in `deps/VictoriaLogs`. Add a replace directive so VT resolves VL to the shared fork.

In `lakehouse-traces/deps/VictoriaTraces/go.mod`, add after the `module` line:

```go
replace github.com/VictoriaMetrics/VictoriaLogs => ../VictoriaLogs
```

The relative path `../VictoriaLogs` resolves from `lakehouse-traces/deps/VictoriaTraces/` to `lakehouse-traces/deps/VictoriaLogs/`.

- [ ] **Step 3: Add VT replace directive to lakehouse-traces go.mod**

In `lakehouse-traces/go.mod`, add a replace directive and update the VT version:

```go
replace github.com/VictoriaMetrics/VictoriaTraces => ./deps/VictoriaTraces
```

Change the require line from:
```go
github.com/VictoriaMetrics/VictoriaTraces v0.8.2
```
to:
```go
github.com/VictoriaMetrics/VictoriaTraces v0.9.0
```

- [ ] **Step 4: Run go mod tidy to resolve dependencies**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go mod tidy
```

Expected: completes without errors. May add/update indirect dependencies.

- [ ] **Step 5: Verify build compiles**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go build ./...
```

Expected: BUILD SUCCESS. If there are type conflicts, check that both replace directives point to the correct VL fork.

- [ ] **Step 6: Commit**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse
git add lakehouse-traces/deps/VictoriaTraces/ lakehouse-traces/go.mod lakehouse-traces/go.sum
git commit -m "deps: fork VT v0.9.0 into lakehouse-traces/deps/VictoriaTraces

Clone VictoriaTraces v0.9.0 source and configure replace directives
so VT resolves VictoriaLogs to the shared fork at deps/VictoriaLogs.
This enables importing VT's Jaeger and Tempo handlers directly."
```

---

### Task 2: Add ExternalStorage Interface to VT Fork

**Files:**
- Create: `lakehouse-traces/deps/VictoriaTraces/app/vtstorage/external.go`
- Modify: `lakehouse-traces/deps/VictoriaTraces/app/vtstorage/main.go`

This mirrors the existing pattern in `lakehouse-traces/deps/VictoriaLogs/app/vlstorage/external.go`.

- [ ] **Step 1: Create external.go**

Create `lakehouse-traces/deps/VictoriaTraces/app/vtstorage/external.go`:

```go
package vtstorage

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// ExternalStorage allows an external backend (e.g., S3/Parquet) to handle
// all storage dispatch functions. When set via SetExternalStorage, the
// dispatch functions in main.go route to this interface instead of
// localStorage or netstorageSelect.
type ExternalStorage interface {
	RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error
	GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error)
	GetFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error)
	GetStreamFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error)
	GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error)
	GetStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error)
	GetStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error)
	GetTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error)
}

var externalStorage ExternalStorage

// SetExternalStorage configures an external storage backend.
// All dispatch functions will route to it when set.
func SetExternalStorage(s ExternalStorage) {
	externalStorage = s
}
```

- [ ] **Step 2: Add guard clauses to vtstorage/main.go**

Add `externalStorage` guard at the top of each dispatch function. The guard must appear BEFORE the `localStorage` check so external storage takes priority.

In `lakehouse-traces/deps/VictoriaTraces/app/vtstorage/main.go`, modify these 8 functions:

**RunQuery** (around line 565):
```go
func RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	if externalStorage != nil {
		return externalStorage.RunQuery(qctx, writeBlock)
	}
	// ... existing code unchanged
```

**GetFieldNames** (around line 583):
```go
func GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	if externalStorage != nil {
		return externalStorage.GetFieldNames(qctx)
	}
	// ... existing code unchanged
```

**GetFieldValues** (around line 593):
```go
func GetFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	if externalStorage != nil {
		return externalStorage.GetFieldValues(qctx, fieldName, limit)
	}
	// ... existing code unchanged
```

**GetStreamFieldNames** (around line 601):
```go
func GetStreamFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	if externalStorage != nil {
		return externalStorage.GetStreamFieldNames(qctx)
	}
	// ... existing code unchanged
```

**GetStreamFieldValues** (around line 611):
```go
func GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	if externalStorage != nil {
		return externalStorage.GetStreamFieldValues(qctx, fieldName, limit)
	}
	// ... existing code unchanged
```

**GetStreams** (around line 621):
```go
func GetStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	if externalStorage != nil {
		return externalStorage.GetStreams(qctx, limit)
	}
	// ... existing code unchanged
```

**GetStreamIDs** (around line 631):
```go
func GetStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	if externalStorage != nil {
		return externalStorage.GetStreamIDs(qctx, limit)
	}
	// ... existing code unchanged
```

**GetTenantIDs** (around line 675):
```go
func GetTenantIDs(ctx context.Context, start, end int64) ([]logstorage.TenantID, error) {
	if externalStorage != nil {
		return externalStorage.GetTenantIDs(ctx, start, end)
	}
	// ... existing code unchanged
```

- [ ] **Step 3: Verify VT fork compiles**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces/deps/VictoriaTraces && GOWORK=off go build ./app/vtstorage/...
```

Expected: BUILD SUCCESS

- [ ] **Step 4: Verify lakehouse-traces still compiles**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go build ./...
```

Expected: BUILD SUCCESS

- [ ] **Step 5: Commit**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse
git add lakehouse-traces/deps/VictoriaTraces/app/vtstorage/external.go lakehouse-traces/deps/VictoriaTraces/app/vtstorage/main.go
git commit -m "feat(vt-fork): add ExternalStorage interface to vtstorage dispatch

Add external.go with ExternalStorage interface (8 methods) and
guard clauses in main.go. When SetExternalStorage() is called,
all dispatch functions route to the external backend instead of
localStorage or netstorageSelect. Mirrors the VL fork pattern."
```

---

### Task 3: Create VT Storage Adapter

**Files:**
- Create: `lakehouse-traces/internal/vtstorage_adapter/adapter.go`
- Create: `lakehouse-traces/internal/vtstorage_adapter/adapter_test.go`

This adapter bridges lakehouse-traces' `storage.Storage` to VT's `vtstorage.ExternalStorage` interface. Same pattern as the existing `vlstorage` adapter but targeting VT's dispatch.

- [ ] **Step 1: Write the adapter test**

Create `lakehouse-traces/internal/vtstorage_adapter/adapter_test.go`:

```go
package vtstorageadapter

import (
	"context"
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type mockStorage struct {
	runQueryCalled           bool
	getFieldNamesCalled      bool
	getFieldValuesCalled     bool
	getStreamFieldNamesCalled  bool
	getStreamFieldValuesCalled bool
	getStreamsCalled         bool
	getStreamIDsCalled       bool
	hasDataForRangeCalled    bool
	lastFieldName            string
	lastLimit                uint64
}

func (m *mockStorage) RunQuery(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, _ logstorage.WriteDataBlockFunc) error {
	m.runQueryCalled = true
	return nil
}

func (m *mockStorage) GetFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	m.getFieldNamesCalled = true
	return []logstorage.ValueWithHits{{Value: "test_field"}}, nil
}

func (m *mockStorage) GetFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getFieldValuesCalled = true
	m.lastFieldName = fieldName
	m.lastLimit = limit
	return nil, nil
}

func (m *mockStorage) GetStreamFieldNames(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	m.getStreamFieldNamesCalled = true
	return nil, nil
}

func (m *mockStorage) GetStreamFieldValues(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamFieldValuesCalled = true
	m.lastFieldName = fieldName
	m.lastLimit = limit
	return nil, nil
}

func (m *mockStorage) GetStreams(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamsCalled = true
	m.lastLimit = limit
	return nil, nil
}

func (m *mockStorage) GetStreamIDs(_ context.Context, _ []logstorage.TenantID, _ *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	m.getStreamIDsCalled = true
	m.lastLimit = limit
	return nil, nil
}

func (m *mockStorage) HasDataForRange(_, _ int64) bool {
	m.hasDataForRangeCalled = true
	return true
}

func (m *mockStorage) Close() error { return nil }

func TestAdapterDelegatesRunQuery(t *testing.T) {
	mock := &mockStorage{}
	a := &Adapter{store: mock}

	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatal(err)
	}
	qctx := &logstorage.QueryContext{
		Context:   context.Background(),
		TenantIDs: nil,
		Query:     q,
	}

	err = a.RunQuery(qctx, func(_ uint, _ *logstorage.DataBlock) {})
	if err != nil {
		t.Fatalf("RunQuery error: %v", err)
	}
	if !mock.runQueryCalled {
		t.Error("expected RunQuery to delegate to store")
	}
}

func TestAdapterDelegatesGetFieldValues(t *testing.T) {
	mock := &mockStorage{}
	a := &Adapter{store: mock}

	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatal(err)
	}
	qctx := &logstorage.QueryContext{
		Context:   context.Background(),
		TenantIDs: nil,
		Query:     q,
	}

	_, err = a.GetFieldValues(qctx, "service.name", 100)
	if err != nil {
		t.Fatalf("GetFieldValues error: %v", err)
	}
	if !mock.getFieldValuesCalled {
		t.Error("expected GetFieldValues to delegate to store")
	}
	if mock.lastFieldName != "service.name" {
		t.Errorf("expected fieldName=service.name, got %s", mock.lastFieldName)
	}
	if mock.lastLimit != 100 {
		t.Errorf("expected limit=100, got %d", mock.lastLimit)
	}
}

func TestAdapterDelegatesAllMethods(t *testing.T) {
	mock := &mockStorage{}
	a := &Adapter{store: mock}

	q, err := logstorage.ParseQuery("*")
	if err != nil {
		t.Fatal(err)
	}
	qctx := &logstorage.QueryContext{
		Context:   context.Background(),
		TenantIDs: nil,
		Query:     q,
	}

	_, _ = a.GetFieldNames(qctx)
	_, _ = a.GetStreamFieldNames(qctx)
	_, _ = a.GetStreamFieldValues(qctx, "host", 50)
	_, _ = a.GetStreams(qctx, 10)
	_, _ = a.GetStreamIDs(qctx, 20)
	_, _ = a.GetTenantIDs(context.Background(), 0, 1000)

	if !mock.getFieldNamesCalled {
		t.Error("GetFieldNames not delegated")
	}
	if !mock.getStreamFieldNamesCalled {
		t.Error("GetStreamFieldNames not delegated")
	}
	if !mock.getStreamFieldValuesCalled {
		t.Error("GetStreamFieldValues not delegated")
	}
	if !mock.getStreamsCalled {
		t.Error("GetStreams not delegated")
	}
	if !mock.getStreamIDsCalled {
		t.Error("GetStreamIDs not delegated")
	}
	if !mock.hasDataForRangeCalled {
		t.Error("HasDataForRange not called via GetTenantIDs")
	}
}

func TestAdapterGetTenantIDsNoData(t *testing.T) {
	mock := &mockStorage{}
	mock.hasDataForRangeCalled = false
	a := &Adapter{store: &noDataStorage{}}

	ids, err := a.GetTenantIDs(context.Background(), 0, 1000)
	if err != nil {
		t.Fatal(err)
	}
	if len(ids) != 0 {
		t.Errorf("expected empty tenant IDs when no data, got %d", len(ids))
	}
}

type noDataStorage struct{ mockStorage }

func (n *noDataStorage) HasDataForRange(_, _ int64) bool { return false }
```

- [ ] **Step 2: Run test to verify it fails**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go test ./internal/vtstorage_adapter/ -v -count=1 2>&1 | tail -20
```

Expected: FAIL — `Adapter` type not defined.

- [ ] **Step 3: Write the adapter implementation**

Create `lakehouse-traces/internal/vtstorage_adapter/adapter.go`:

```go
package vtstorageadapter

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"
)

// Adapter bridges lakehouse-traces' storage.Storage to VT's
// vtstorage.ExternalStorage interface.
type Adapter struct {
	store storage.Storage
}

// Init registers the given storage backend as VT's external storage.
// After this call, all VT query dispatch functions (RunQuery, GetFieldNames, etc.)
// route through the adapter to the S3/Parquet backend.
func Init(store storage.Storage) {
	a := &Adapter{store: store}
	vtstorage.SetExternalStorage(a)
}

func (a *Adapter) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	return a.store.RunQuery(qctx.Context, qctx.TenantIDs, qctx.Query, writeBlock)
}

func (a *Adapter) GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	return a.store.GetFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
}

func (a *Adapter) GetFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}

func (a *Adapter) GetStreamFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
}

func (a *Adapter) GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}

func (a *Adapter) GetStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreams(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func (a *Adapter) GetStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamIDs(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func (a *Adapter) GetTenantIDs(_ context.Context, start, end int64) ([]logstorage.TenantID, error) {
	if !a.store.HasDataForRange(start, end) {
		return nil, nil
	}
	return []logstorage.TenantID{{AccountID: 0, ProjectID: 0}}, nil
}
```

- [ ] **Step 4: Run tests to verify they pass**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go test ./internal/vtstorage_adapter/ -v -count=1 2>&1 | tail -30
```

Expected: all tests PASS.

- [ ] **Step 5: Verify interface compliance at compile time**

Add a compile-time assertion to adapter.go (at top, after imports):

```go
var _ vtstorage.ExternalStorage = (*Adapter)(nil)
```

Rebuild:
```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go build ./internal/vtstorage_adapter/
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Commit**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse
git add lakehouse-traces/internal/vtstorage_adapter/
git commit -m "feat: add vtstorage adapter bridging S3/Parquet to VT dispatch

Implements vtstorage.ExternalStorage interface by delegating all 8
methods to storage.Storage. Same pattern as the existing vlstorage
adapter. When Init() is called, VT's Jaeger and Tempo handlers
automatically route queries through the S3/Parquet backend."
```

---

### Task 4: Fix Traces vlstorage Adapter — Missing Filter Params

**Files:**
- Modify: `lakehouse-traces/internal/vlstorage/vlstorage.go`

The traces vlstorage adapter is behind the logs adapter on a VL API change. Four methods need the `filter` parameter added to match the `ExternalStorage` interface.

- [ ] **Step 1: Check the ExternalStorage interface for current signatures**

Read `lakehouse-traces/deps/VictoriaLogs/app/vlstorage/external.go` and verify which methods have `filter string` params. The methods that need updating are:
- `GetFieldNames(qctx, filter)`
- `GetFieldValues(qctx, fieldName, filter, limit)`
- `GetStreamFieldNames(qctx, filter)`
- `GetStreamFieldValues(qctx, fieldName, filter, limit)`

- [ ] **Step 2: Update GetFieldNames signature**

In `lakehouse-traces/internal/vlstorage/vlstorage.go`, change:

```go
func (a *adapter) GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	return filterHiddenValues(results, qctx.HiddenFieldsFilters), nil
}
```

to:

```go
func (a *adapter) GetFieldNames(qctx *logstorage.QueryContext, filter string) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	results = filterHiddenValues(results, qctx.HiddenFieldsFilters)
	return filterValuesBySubstring(results, filter), nil
}
```

- [ ] **Step 3: Update GetFieldValues signature**

Change:

```go
func (a *adapter) GetFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}
```

to:

```go
func (a *adapter) GetFieldValues(qctx *logstorage.QueryContext, fieldName, filter string, limit uint64) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
	if err != nil {
		return nil, err
	}
	return filterValuesBySubstring(results, filter), nil
}
```

- [ ] **Step 4: Update GetStreamFieldNames signature**

Change:

```go
func (a *adapter) GetStreamFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetStreamFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	return filterHiddenValues(results, qctx.HiddenFieldsFilters), nil
}
```

to:

```go
func (a *adapter) GetStreamFieldNames(qctx *logstorage.QueryContext, filter string) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetStreamFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	results = filterHiddenValues(results, qctx.HiddenFieldsFilters)
	return filterValuesBySubstring(results, filter), nil
}
```

- [ ] **Step 5: Update GetStreamFieldValues signature**

Change:

```go
func (a *adapter) GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}
```

to:

```go
func (a *adapter) GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName, filter string, limit uint64) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetStreamFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
	if err != nil {
		return nil, err
	}
	return filterValuesBySubstring(results, filter), nil
}
```

- [ ] **Step 6: Add filterValuesBySubstring helper**

Add at the bottom of `vlstorage.go` (this matches the root module's implementation):

```go
func filterValuesBySubstring(results []logstorage.ValueWithHits, filter string) []logstorage.ValueWithHits {
	if filter == "" {
		return results
	}
	filtered := make([]logstorage.ValueWithHits, 0, len(results))
	for _, v := range results {
		if strings.Contains(v.Value, filter) {
			filtered = append(filtered, v)
		}
	}
	return filtered
}
```

Also add `"strings"` to the imports if not already present.

- [ ] **Step 7: Verify build**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go build ./internal/vlstorage/...
```

Expected: BUILD SUCCESS

- [ ] **Step 8: Run existing vlstorage tests**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go test ./internal/vlstorage/ -v -count=1 -run TestVLStorage 2>&1 | tail -30
```

Expected: all tests PASS

- [ ] **Step 9: Commit**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse
git add lakehouse-traces/internal/vlstorage/vlstorage.go
git commit -m "fix: add missing filter param to traces vlstorage adapter

Align traces vlstorage adapter with root module's implementation.
Four methods now accept a filter string parameter and apply
filterValuesBySubstring to results, matching the VL ExternalStorage
interface contract."
```

---

### Task 5: Deduplicate Storage Interface

**Files:**
- Delete: `lakehouse-traces/internal/storage/interface.go`
- Modify: all files in `lakehouse-traces/` that import `lakehouse-traces/internal/storage` for the `Storage` type

The traces module's `internal/storage/interface.go` is identical to the root module's version (minus `CountOnlyHint`). Since traces already imports root module packages via the `replace` directive, we can use the root module's interface directly.

- [ ] **Step 1: Find all files importing the traces storage interface**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && grep -r '"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"' --include='*.go' -l
```

Note every file path — these all need their import changed from:
```go
"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"
```
to:
```go
"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
```

- [ ] **Step 2: Update all import paths**

For each file found in step 1, change the import path. Use sed or manual editing.

**Key files likely affected:**
- `lakehouse-traces/internal/selectapi/handler.go` (line 18)
- `lakehouse-traces/internal/vlstorage/vlstorage.go` (line 13)
- `lakehouse-traces/internal/vtstorage_adapter/adapter.go` (line 9)
- `lakehouse-traces/internal/storage/parquets3/*.go` (multiple files)
- `lakehouse-traces/main.go`

For `handler.go`, the import alias may need updating since both root and traces handlers are in `package selectapi`:
```go
// Before:
"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"

// After:
"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
```

- [ ] **Step 3: Delete the duplicate interface file**

```bash
rm /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces/internal/storage/interface.go
```

- [ ] **Step 4: Verify build**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go build ./...
```

Expected: BUILD SUCCESS. If there are type mismatches, ensure the root module's `Storage` interface matches (it does — both have identical method signatures).

- [ ] **Step 5: Run full test suite**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go test ./... -count=1 2>&1 | tail -30
```

Expected: all tests PASS

- [ ] **Step 6: Commit**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse
git add -A lakehouse-traces/internal/storage/interface.go
git add -u lakehouse-traces/
git commit -m "refactor: deduplicate storage.Storage interface

Delete traces module's local copy of storage.Storage interface and
import from root module. Both interfaces are identical. Traces
module already depends on root via replace directive."
```

---

### Task 6: Replace Custom Jaeger Handlers with VT Upstream

**Files:**
- Delete: `lakehouse-traces/internal/selectapi/jaeger.go`
- Delete: `lakehouse-traces/internal/selectapi/jaeger_test.go`
- Delete: `lakehouse-traces/internal/selectapi/jaeger_fuzz_test.go`
- Delete: `lakehouse-traces/internal/selectapi/jaeger_verify_test.go`
- Modify: `lakehouse-traces/internal/selectapi/handler.go`

This is the core win: replace 629 lines of custom Jaeger handlers with VT's exported `jaeger.RequestHandler` and add Tempo API via `tempo.RequestHandler`.

- [ ] **Step 1: Update handler.go imports**

In `lakehouse-traces/internal/selectapi/handler.go`, add VT handler imports:

```go
import (
	"context"
	"net/http"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/logsql"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/jaeger"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/tempo"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)
```

Note: the `storage` import changes to root module per Task 5.

- [ ] **Step 2: Replace Jaeger route registration with VT handlers**

In `handler.go`, replace the traces-mode block (lines 67-78):

**Before:**
```go
if h.cfg.Mode == config.ModeTraces {
	mux.HandleFunc("/select/jaeger/api/traces/", h.handleJaegerTrace)
	mux.HandleFunc("/select/jaeger/api/traces", h.handleJaegerSearch)
	mux.HandleFunc("/select/jaeger/api/services", h.handleJaegerServices)
	mux.HandleFunc("/select/jaeger/api/services/", h.handleJaegerOperations)
	mux.HandleFunc("/select/jaeger/api/dependencies", h.handleJaegerDependencies)
	mux.HandleFunc("/api/traces/", h.handleJaegerTrace)
	mux.HandleFunc("/api/traces", h.handleJaegerSearch)
	mux.HandleFunc("/api/services", h.handleJaegerServices)
	mux.HandleFunc("/api/services/", h.handleJaegerOperations)
	mux.HandleFunc("/api/dependencies", h.handleJaegerDependencies)
}
```

**After:**
```go
if h.cfg.Mode == config.ModeTraces {
	mux.HandleFunc("/select/jaeger/", func(w http.ResponseWriter, r *http.Request) {
		jaeger.RequestHandler(r.Context(), w, r)
	})
	mux.HandleFunc("/select/tempo/", func(w http.ResponseWriter, r *http.Request) {
		tempo.RequestHandler(r.Context(), w, r)
	})
}
```

VT's `jaeger.RequestHandler` handles all Jaeger sub-paths internally (services, operations, traces, dependencies). VT's `tempo.RequestHandler` handles all Tempo sub-paths (search, tags, tag values, traces v2, metrics query range for Drilldown).

- [ ] **Step 3: Remove the Handler.store field if no longer needed**

After deleting jaeger.go, check if the `store` field is still used in handler.go. The `wrapVL` function doesn't use `h.store` — it delegates to VL's handlers which route through vlstorage. The Jaeger/Tempo handlers route through vtstorage. So `store` may no longer be needed in the Handler struct.

However, keep `store` for now if any other methods still reference it. Check with:

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && grep -n 'h\.store' internal/selectapi/handler.go
```

If no matches after removing jaeger.go, remove the `store` field from Handler struct and the `store` parameter from `NewHandler`. Update `main.go` accordingly. If it IS still used, keep it.

- [ ] **Step 4: Delete custom Jaeger handler files**

```bash
rm /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces/internal/selectapi/jaeger.go
rm /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces/internal/selectapi/jaeger_test.go
rm /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces/internal/selectapi/jaeger_fuzz_test.go
rm /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces/internal/selectapi/jaeger_verify_test.go
```

- [ ] **Step 5: Verify build**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go build ./...
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Run remaining handler tests**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go test ./internal/selectapi/ -v -count=1 2>&1 | tail -30
```

Expected: remaining handler_test.go tests PASS (or need minor updates to remove references to deleted handlers).

- [ ] **Step 7: Commit**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse
git add -A lakehouse-traces/internal/selectapi/
git commit -m "feat: replace custom Jaeger handlers with VT upstream + add Tempo API

Delete 629 lines of custom Jaeger handler code. Register VT's
jaeger.RequestHandler and tempo.RequestHandler which provide:
- Full Jaeger API (events, links, references, dependencies)
- Trace-ID index optimization
- Full Tempo API (search, trace-by-ID protobuf, tag names/values)
- Tempo Drilldown (rate, count_over_time, histogram_over_time,
  quantile_over_time, compare — all TraceQL metrics functions)"
```

---

### Task 7: Wire VT Storage Adapter in main.go

**Files:**
- Modify: `lakehouse-traces/main.go`

- [ ] **Step 1: Add vtstorage adapter import**

In `lakehouse-traces/main.go`, add the import:

```go
vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
```

- [ ] **Step 2: Initialize vtstorage adapter alongside vlstorage**

Find the section where `internalvlstorage.SetStorage(store, tombstoneStore)` is called (around line 601). Add the vtstorage adapter initialization right after it:

```go
if cfg.SelectEnabled() {
	// ... existing vlstorage setup ...
	internalvlstorage.SetStorage(store, tombstoneStore)
	
	// Also set VT's external storage so Jaeger/Tempo handlers route through S3/Parquet
	vtstorageadapter.Init(store)
	
	internalselect.Init()
	// ... rest of handler setup ...
}
```

The vtstorage adapter must be initialized BEFORE `internalselect.Init()` and before any handler that uses VT's dispatch functions.

- [ ] **Step 3: Verify build**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go build ./...
```

Expected: BUILD SUCCESS

- [ ] **Step 4: Run go mod tidy**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go mod tidy
```

Expected: dependencies updated cleanly

- [ ] **Step 5: Commit**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse
git add lakehouse-traces/main.go lakehouse-traces/go.mod lakehouse-traces/go.sum
git commit -m "feat: wire vtstorage adapter in traces main.go

Initialize VT storage adapter at startup so Jaeger and Tempo handlers
route queries through the S3/Parquet backend. Both VL (vlstorage) and
VT (vtstorage) adapters now point to the same underlying storage."
```

---

### Task 8: Update Docker Compose — Bump VT to v0.9.0

**Files:**
- Modify: `deployment/docker/docker-compose-e2e.yml`

- [ ] **Step 1: Bump VT image version**

In `deployment/docker/docker-compose-e2e.yml`, change both VT service images:

**victoriatraces service:**
```yaml
victoriatraces:
  image: victoriametrics/victoria-traces:v0.9.0
```

**vtselect service:**
```yaml
vtselect:
  image: victoriametrics/victoria-traces:v0.9.0
```

- [ ] **Step 2: Verify compose config is valid**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && docker compose -f deployment/docker/docker-compose-e2e.yml config --quiet
```

Expected: no errors

- [ ] **Step 3: Commit**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse
git add deployment/docker/docker-compose-e2e.yml
git commit -m "infra: bump VictoriaTraces image to v0.9.0

Update both victoriatraces and vtselect services to v0.9.0.
This matches the VT version in the deps/VictoriaTraces fork."
```

---

### Task 9: Update Grafana Datasources — Add Tempo Tiers

**Files:**
- Modify: `deployment/docker/grafana/provisioning/datasources/datasources.yaml`

The Grafana datasources file already has the global Tempo datasource (`tempo-global`). Add hot-tier and cold-tier Tempo datasources so each tier can be tested independently.

- [ ] **Step 1: Add Tempo VT Hot datasource**

After the existing `VictoriaTraces Hot (Disk 24h)` Jaeger datasource, add:

```yaml
  - name: "Tempo VT Hot (Disk 24h)"
    type: tempo
    uid: tempo-vt-hot
    access: proxy
    url: http://victoriatraces:10428/select/tempo
    jsonData:
      tracesToLogsV2:
        datasourceUid: victorialogs-hot
        spanStartTimeShift: "-1h"
        spanEndTimeShift: "1h"
        filterByTraceID: true
        filterBySpanID: false
```

- [ ] **Step 2: Add Tempo LH Cold datasource**

After the existing `Lakehouse Traces Cold (S3 Jaeger)` datasource, add:

```yaml
  - name: "Tempo LH Cold (S3 Parquet)"
    type: tempo
    uid: tempo-lh-cold
    access: proxy
    url: http://lakehouse-traces:10428/select/tempo
    jsonData:
      tracesToLogsV2:
        datasourceUid: victoria-lakehouse-cold
        spanStartTimeShift: "-1h"
        spanEndTimeShift: "1h"
        filterByTraceID: true
        filterBySpanID: false
```

- [ ] **Step 3: Update Loki proxy datasources for logs→traces linking**

Update the Loki proxy datasources to link to `tempo-global` for trace correlation:

In the `Loki via Proxy (Hot+Cold)` datasource, change:
```yaml
derivedFields:
  - datasourceUid: tempo-global
    matcherRegex: 'trace_id=(\w+)'
    name: traceID
    url: "$${__value.raw}"
```

In the `Loki via Proxy (Cold Only)` datasource, change:
```yaml
derivedFields:
  - datasourceUid: tempo-global
    matcherRegex: 'trace_id=(\w+)'
    name: traceID
    url: "$${__value.raw}"
```

- [ ] **Step 4: Commit**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse
git add deployment/docker/grafana/provisioning/datasources/datasources.yaml
git commit -m "infra: add Tempo datasources for hot and cold tiers

Add tempo-vt-hot (disk 24h) and tempo-lh-cold (S3 Parquet) Tempo
datasources. Update Loki proxy datasources to link to tempo-global
for logs-to-traces correlation via Tempo v2 API."
```

---

### Task 10: Build + Integration Verification

**Files:** None (verification only)

- [ ] **Step 1: Full build — both modules**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go build ./...
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go build ./...
```

Expected: both BUILD SUCCESS

- [ ] **Step 2: Run full test suite — root module**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && GOWORK=off go test ./... -count=1 2>&1 | tail -30
```

Expected: all tests PASS (root module is unchanged)

- [ ] **Step 3: Run full test suite — traces module**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go test ./... -count=1 -timeout=5m 2>&1 | tail -50
```

Expected: all tests PASS. Any failures need investigation.

- [ ] **Step 4: Run go vet on traces module**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse/lakehouse-traces && GOWORK=off go vet ./...
```

Expected: no errors

- [ ] **Step 5: Build Docker image**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && docker build -f lakehouse-traces/Dockerfile -t lakehouse-traces:vt-upgrade .
```

Expected: BUILD SUCCESS

- [ ] **Step 6: Verify compose stack starts**

```bash
cd /Users/slawomirskowron/github/victoria-lakehouse && docker compose -f deployment/docker/docker-compose-e2e.yml up -d victoriatraces vtselect lakehouse-traces
```

Wait for health checks to pass:
```bash
docker compose -f deployment/docker/docker-compose-e2e.yml ps
```

Expected: all three services healthy

- [ ] **Step 7: Verify Jaeger API on lakehouse-traces**

```bash
curl -s http://localhost:10428/select/jaeger/api/services | head -5
```

Expected: JSON response with `"data"` array (may be empty if no traces ingested)

- [ ] **Step 8: Verify Tempo API on lakehouse-traces**

```bash
curl -s http://localhost:10428/select/tempo/api/echo
```

Expected: `echo`

```bash
curl -s http://localhost:10428/select/tempo/api/v2/search/tags | head -5
```

Expected: JSON response with tag scopes

- [ ] **Step 9: Verify existing LogsQL endpoints still work**

```bash
curl -s "http://localhost:10428/select/logsql/field_names?query=*&start=-1h" | head -5
```

Expected: JSON response (may be empty)

---

## Post-Integration Checklist

After all tasks are complete, verify:

- [ ] `lakehouse-traces` builds with `GOWORK=off go build ./...`
- [ ] Root module builds with `GOWORK=off go build ./...`
- [ ] All traces tests pass with `GOWORK=off go test ./... -count=1`
- [ ] All root module tests pass
- [ ] Docker image builds
- [ ] Compose stack starts with VT v0.9.0
- [ ] Jaeger API responds via lakehouse-traces
- [ ] Tempo API responds via lakehouse-traces
- [ ] LogsQL endpoints unchanged
- [ ] Grafana shows all datasources

## Out of Scope (Follow-up)

- **filter.go dedup**: `parseFilterFromQuery` is unexported in both modules' `parquets3` packages. Deduplication requires extracting to a shared utility package. Low priority since both copies are identical and tested independently.
- **VL dependency upgrade**: VL stays at v1.50.0 from `deps/VictoriaLogs`
- **TraceQL query language**: Beyond what VT v0.9.0 provides
- **Jaeger gRPC**: Only HTTP Jaeger API
- **Multi-level vtselect changes**: Separate VT binary
- **handler_test.go updates**: Tests for the deleted Jaeger handlers. VT's own handler tests cover this functionality now.
