# Upstream API Coverage Matrix & Reuse Safeguards

> **HARD RULE**: Never implement custom handlers for APIs that exist in upstream VL/VT.
> Always check upstream source code FIRST. Only add Lakehouse-specific logic when
> no upstream equivalent exists.

## Safeguard Checklist (MANDATORY before any handler work)

Before implementing ANY insert or select API handler:

1. **Check upstream VL/VT source** for existing handler functions:
   - VL select: `deps/VictoriaLogs/app/vlselect/logsql/` — all LogSQL handlers
   - VL insert: `deps/VictoriaLogs/app/vlinsert/` — all insert format handlers
   - VT select: `deps/VictoriaTraces/app/vtselect/jaeger/` — Jaeger API handlers
   - VT select: `deps/VictoriaTraces/app/vtselect/tempo/` — Tempo API handlers
   - VT insert: `deps/VictoriaTraces/app/vtinsert/` — all trace insert handlers

2. **If upstream has a handler**: Use it directly via `RequestHandler(ctx, w, r)`.
   Wire it in handler.go. Do NOT rewrite the logic.

3. **If upstream does NOT have a handler**: Document why in a comment, then implement.

4. **ExternalStorage interface**: The ONLY code we add to upstream VL/VT is:
   - `external.go` — ExternalStorage interface + SetExternalStorage()
   - `dispatch.patch` — guard clauses routing to externalStorage before localStorage
   - Never modify function signatures, add params, or change upstream behavior.

## Coverage Matrix

### Logs Module (Root)

| Endpoint | Handler Source | Status |
|----------|---------------|--------|
| `/select/logsql/query` | `logsql.ProcessQueryRequest` | UPSTREAM |
| `/select/logsql/query_time_range` | `logsql.ProcessQueryTimeRangeRequest` | UPSTREAM |
| `/select/logsql/facets` | `logsql.ProcessFacetsRequest` | UPSTREAM |
| `/select/logsql/field_names` | `logsql.ProcessFieldNamesRequest` | UPSTREAM |
| `/select/logsql/field_values` | `logsql.ProcessFieldValuesRequest` | UPSTREAM |
| `/select/logsql/stream_field_names` | `logsql.ProcessStreamFieldNamesRequest` | UPSTREAM |
| `/select/logsql/stream_field_values` | `logsql.ProcessStreamFieldValuesRequest` | UPSTREAM |
| `/select/logsql/streams` | `logsql.ProcessStreamsRequest` | UPSTREAM |
| `/select/logsql/stream_ids` | `logsql.ProcessStreamIDsRequest` | UPSTREAM |
| `/select/logsql/hits` | `logsql.ProcessHitsRequest` | UPSTREAM |
| `/select/logsql/stats_query` | `logsql.ProcessStatsQueryRequest` | UPSTREAM |
| `/select/logsql/stats_query_range` | `logsql.ProcessStatsQueryRangeRequest` | UPSTREAM |
| `/select/logsql/tail` | `handleTailNoop` | LH_ONLY (cold storage) |
| `/select/tenant_ids` | `logsql.ProcessTenantIDsRequest` | UPSTREAM |
| `/insert/*` | `vlinsert.RequestHandler` | UPSTREAM |
| `/internal/select/*` | `internalselect.RequestHandler` | UPSTREAM |

### Traces Module (lakehouse-traces)

| Endpoint | Handler Source | Status | Action Needed |
|----------|---------------|--------|---------------|
| `/select/logsql/*` (all 12) | `logsql.Process*` | UPSTREAM | None |
| `/select/jaeger/api/services` | `selectapi.handleJaegerServices` | **CUSTOM_REIMPL** | Replace with VT `jaeger.RequestHandler` |
| `/select/jaeger/api/services/*/operations` | `selectapi.handleJaegerOperations` | **CUSTOM_REIMPL** | Replace with VT `jaeger.RequestHandler` |
| `/select/jaeger/api/traces/*` | `selectapi.handleJaegerTrace` | **CUSTOM_REIMPL** | Replace with VT `jaeger.RequestHandler` |
| `/select/jaeger/api/traces` | `selectapi.handleJaegerSearch` | **CUSTOM_REIMPL** | Replace with VT `jaeger.RequestHandler` |
| `/api/services` (alt paths) | `selectapi.handleJaeger*` | **CUSTOM_REIMPL** | Replace with VT `jaeger.RequestHandler` |
| `/api/traces` (alt paths) | `selectapi.handleJaeger*` | **CUSTOM_REIMPL** | Replace with VT `jaeger.RequestHandler` |
| Tempo API (search, tags, traces) | Not wired | **MISSING** | Add VT `tempo.RequestHandler` |
| `/insert/*` | `vtinsert.RequestHandler` | UPSTREAM | None |
| `/internal/select/*` | `internalselect.RequestHandler` | UPSTREAM | None |

### Lakehouse-Only Endpoints (No upstream equivalent)

These are legitimate Lakehouse features with no VL/VT equivalent:

| Endpoint | Purpose |
|----------|---------|
| `/health`, `/ready` | K8s probes |
| `/lakehouse/info` | Build/config info |
| `/manifest/*` | S3 manifest tracking |
| `/internal/cache/*` | Distributed cache |
| `/internal/lifecycle/*` | K8s lifecycle hooks |
| `/internal/stats/sync` | Fleet statistics |
| `/internal/tenant/*` | Tenant alias system |
| `/internal/crosssignal/*` | Cross-signal cache |
| `/internal/buffer/query` | Write buffer querying |
| `/api/v1/bloom/status` | Bloom filter state |
| `/api/v1/stats/*` | Detailed stats API |
| `/delete/logsql` | Tombstone-based delete |
| `/ui/*`, `/vmui/*` | Web dashboards |

## Storage Layer Pattern

The ONLY modification to upstream VL/VT:

```
┌─────────────────────────────────┐
│ VL/VT upstream code (unmodified)│
│                                 │
│ Dispatch functions check:       │
│  if externalStorage != nil {    │
│    return externalStorage.X()   │  ← Our patch adds THIS guard
│  }                              │
│  if localStorage != nil { ... } │  ← Original VL/VT code
│  return netstorageSelect.X()    │
└─────────────────────────────────┘
         │
         ▼
┌─────────────────────────────────┐
│ Lakehouse Adapter               │
│ (vtstorage_adapter/adapter.go   │
│  or vlstorage/vlstorage.go)     │
│                                 │
│ Translates ExternalStorage      │
│ interface → storage.Storage     │
│ (S3/Parquet)                    │
└─────────────────────────────────┘
```

## Version-Specific Notes

| Component | Version | Filter Param | Notes |
|-----------|---------|-------------|-------|
| Root VL | v1.50.0 | YES | ExternalStorage has filter params |
| Traces VL | commit 77df0c04d532 | YES | v0.9.2-compatible, filter in interface |
| VT | v0.9.2 | YES | Filter param added in v0.9.2 |

When upgrading traces VL to a version with filter, update:
1. `patches/vl-traces/external.go.src` — add filter params
2. `patches/vl-traces/vlstorage-dispatch.patch` — pass filter through
3. `lakehouse-traces/internal/vlstorage/vlstorage.go` — add filter to methods
