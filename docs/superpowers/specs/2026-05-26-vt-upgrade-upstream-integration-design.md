# VT v0.9.0 Upgrade & Upstream Integration

## Goal

Upgrade VictoriaTraces dependency from v0.8.2 to v0.9.0, replace ~1,200 lines of custom Jaeger handlers with VT's upstream implementation, add Tempo API + Drilldown support, and deduplicate shared code between logs/traces modules. End state: lakehouse-traces exposes the full VT Jaeger + Tempo API surface backed by S3/Parquet storage, with zero custom query handler code.

## Architecture

Fork VT v0.9.0 into `lakehouse-traces/deps/VictoriaTraces` (same pattern as the existing VL fork in `deps/VictoriaLogs`). Add a thin `ExternalStorage` interface to VT's `app/vtstorage` package that intercepts all storage dispatch functions. Lakehouse-traces sets its S3/Parquet backend via `vtstorage.SetExternalStorage()`, then imports VT's Jaeger and Tempo handlers unmodified.

When VT upstream eventually adds pluggable storage support, delete the fork and import VT as a regular Go module.

## Components

### 1. VT Fork — `lakehouse-traces/deps/VictoriaTraces`

**What:** Clone VT v0.9.0, add `app/vtstorage/external.go` (~30 lines), modify 8 dispatch functions in `app/vtstorage/main.go` with `externalStorage` guard clauses.

**Files to create:**

`app/vtstorage/external.go`:
```go
package vtstorage

import (
    "context"
    "github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

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

func SetExternalStorage(s ExternalStorage) {
    externalStorage = s
}
```

**Modifications to `app/vtstorage/main.go`:** Add guard clause to each dispatch function:

```go
func RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
    if externalStorage != nil {
        return externalStorage.RunQuery(qctx, writeBlock)
    }
    // ... existing code unchanged
}
```

Same pattern for `GetFieldNames`, `GetFieldValues`, `GetStreamFieldNames`, `GetStreamFieldValues`, `GetStreams`, `GetStreamIDs`, `GetTenantIDs`.

**`go.mod` change in `lakehouse-traces/`:**
```
replace github.com/VictoriaMetrics/VictoriaTraces => ./deps/VictoriaTraces
```

**VL dependency alignment:** VT v0.9.0 depends on a specific VL commit. The forked VT in `deps/VictoriaTraces` must use the same VL as `deps/VictoriaLogs` (commit `a408207c2242`). Add a `replace` directive inside the VT fork's `go.mod` to point `github.com/VictoriaMetrics/VictoriaLogs` to `../../deps/VictoriaLogs` (the shared forked VL). This avoids duplicate VL types at compile time.

**Delta from upstream:** 1 new file (`external.go`), 8 one-line insertions in `main.go`, 1 `replace` in `go.mod`. No upstream code is modified — only additive changes. Rebasing to future VT versions is trivial.

### 2. VT Storage Adapter — `lakehouse-traces/internal/vtstorage_adapter`

**What:** Bridge lakehouse-traces' `storage.Storage` to VT's `vtstorage.ExternalStorage` interface. Similar to the existing `vlstorage` adapter but targeting VT's dispatch.

**Why separate from vlstorage?** VL and VT have separate storage dispatch packages (`vlstorage` vs `vtstorage`). Both need to be initialized — VL for LogsQL endpoints, VT for Jaeger/Tempo endpoints. The adapter implements VT's `ExternalStorage` interface and delegates to the same underlying `storage.Storage`.

```go
package vtstorageadapter

import (
    "github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"
)

type Adapter struct {
    store storage.Storage
}

func Init(store storage.Storage) {
    a := &Adapter{store: store}
    vtstorage.SetExternalStorage(a)
}

func (a *Adapter) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
    return a.store.RunQuery(qctx.GetContext(), qctx.GetTenantIDs(), qctx.Query, writeBlock)
}
// ... remaining methods delegate similarly
```

### 3. Handler Replacement — `lakehouse-traces/internal/selectapi/handler.go`

**What:** Delete custom `jaeger.go` (~629 lines). Register VT's Jaeger and Tempo handlers in the mux.

**Before (current):**
```go
if h.cfg.Mode == config.ModeTraces {
    mux.HandleFunc("/select/jaeger/api/traces/", h.handleJaegerTrace)  // custom
    mux.HandleFunc("/select/jaeger/api/traces", h.handleJaegerSearch)   // custom
    // ... 8 more custom handlers
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

**Files deleted:** `lakehouse-traces/internal/selectapi/jaeger.go` (629 lines)

**What we gain:**
- Full Jaeger API with events, links, references, dependency computation
- Trace-ID index optimization (VT uses `trace_id_idx` streams for fast lookups)
- Full Tempo API: search, trace-by-ID (protobuf), tag names/values
- Tempo Drilldown: `rate()`, `count_over_time()`, `histogram_over_time()`, `quantile_over_time()`, `compare()` — all TraceQL metrics functions
- Exemplar support (Grafana links metric data points to specific traces)

### 4. Shared Code Deduplication

**4a. `internal/storage/interface.go`** — Already in root module. Traces module imports via `replace` directive. Delete `lakehouse-traces/internal/storage/interface.go`. Update traces imports.

**4b. `internal/storage/parquets3/filter.go`** — Identical between modules. Delete traces copy. Import from root module.

**4c. `internal/selectapi/handler.go` base logic** — Extract shared `wrapVL()`, `normalizeTimeParams()`, concurrency semaphore into root module's `internal/selectapi/middleware.go`. Both modules import it.

**4d. vlstorage adapter `filter` param** — Traces vlstorage adapter is behind on VL API change (missing `filter` param in `GetFieldNames`). Fix by matching the logs adapter's signature.

### 5. Grafana Datasource Configuration

Add/update datasources in `deployment/docker/grafana/provisioning/datasources/datasources.yaml`:

```yaml
# Tempo VT Hot (disk-based, 24h) — for manual reference checking
- name: "Tempo VT Hot (Disk 24h)"
  type: tempo
  uid: tempo-vt-hot
  access: proxy
  url: http://victoriatraces:10428/select/tempo

# Tempo LH Cold (S3 Parquet) — for Loki proxy trace correlation
- name: "Tempo LH Cold (S3 Parquet)"
  type: tempo
  uid: tempo-lh-cold
  access: proxy
  url: http://lakehouse-traces:10428/select/tempo

# Tempo Global (Hot+Cold via vtselect)
- name: "Tempo via VictoriaTraces (Hot+Cold)"
  type: tempo
  uid: tempo-global
  access: proxy
  url: http://vtselect:10428/select/tempo
```

Update Loki proxy datasources to link to `tempo-global` for logs-to-traces:
```yaml
derivedFields:
  - datasourceUid: tempo-global
    matcherRegex: 'trace_id=(\w+)'
    name: traceID
    url: "$${__value.raw}"
```

### 6. Docker Compose Updates

Bump VT image to v0.9.0 in `deployment/docker/docker-compose-e2e.yml`:
```yaml
victoriatraces:
  image: victoriametrics/victoria-traces:v0.9.0
vtselect:
  image: victoriametrics/victoria-traces:v0.9.0
```

Rebuild lakehouse-traces image with new deps.

## Testing

### Unit Tests
- Verify `vtstorage.ExternalStorage` adapter correctly delegates all 8 methods
- Verify handler registration routes to VT's `jaeger.RequestHandler` and `tempo.RequestHandler`

### Integration Tests (compose stack)
- Jaeger API parity: `/select/jaeger/api/services`, `/api/traces/{id}`, `/api/traces?service=X`
- Tempo API: `/select/tempo/api/search`, `/select/tempo/api/v2/traces/{id}`, `/select/tempo/api/v2/search/tags`
- Tempo Drilldown: `/select/tempo/api/metrics/query_range` with `rate()`, `histogram_over_time()`
- Verify trace lookup works from Grafana: Loki logs → click traceID → Tempo trace view

### Regression Tests
- All existing LogsQL endpoint tests pass unchanged
- All parity tests pass unchanged
- Benchmark regression: query latency should not increase

### Parity Tests
- Jaeger API: LH responses match VT responses for the same trace data
- Tempo API: LH responses match VT responses
- Verify Tempo Drilldown metrics match between LH cold and VT hot for same data

## Migration Path

This is a non-breaking change. The VT module upgrade and handler replacement are internal — no external API contracts change. The Jaeger API becomes more complete (events, links, dependencies now work). The Tempo API is a new capability.

Rollback: revert the `go.mod` replace directive and restore the custom `jaeger.go` file.

## Out of Scope

- VL dependency upgrade (VL stays at v1.50.0 from deps/VictoriaLogs)
- TraceQL query language support beyond what VT v0.9.0 provides
- Jaeger gRPC (only HTTP Jaeger API)
- Multi-level select (vtselect) changes — that's a separate VT binary

## File Inventory

### New Files
| File | Lines | Purpose |
|------|-------|---------|
| `lakehouse-traces/deps/VictoriaTraces/app/vtstorage/external.go` | ~30 | ExternalStorage interface |
| `lakehouse-traces/internal/vtstorage_adapter/adapter.go` | ~120 | Bridge storage.Storage → VT ExternalStorage |
| `internal/selectapi/middleware.go` | ~80 | Shared handler middleware (wrapVL, normalizeTimeParams) |

### Modified Files
| File | Change |
|------|--------|
| `lakehouse-traces/deps/VictoriaTraces/app/vtstorage/main.go` | 8 one-line guard clauses |
| `lakehouse-traces/go.mod` | Add VT replace directive, bump VT version |
| `lakehouse-traces/internal/selectapi/handler.go` | Replace custom handlers with VT's RequestHandler |
| `lakehouse-traces/internal/vlstorage/vlstorage.go` | Fix missing filter param |
| `lakehouse-traces/main.go` | Initialize vtstorage adapter |
| `deployment/docker/docker-compose-e2e.yml` | Bump VT image to v0.9.0 |
| `deployment/docker/grafana/provisioning/datasources/datasources.yaml` | Add Tempo datasources |

### Deleted Files
| File | Lines Removed | Reason |
|------|---------------|--------|
| `lakehouse-traces/internal/selectapi/jaeger.go` | 629 | Replaced by VT upstream |
| `lakehouse-traces/internal/storage/interface.go` | 31 | Import from root module |
| `lakehouse-traces/internal/storage/parquets3/filter.go` | 153 | Import from root module |

**Net change: ~230 lines added, ~813 lines deleted = ~583 lines net reduction.**
