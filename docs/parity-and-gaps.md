# Parity & Cold-Tier Gaps

Track what the cold tier (Lakehouse-stored Parquet) does, doesn't, and only-approximately does relative to upstream VictoriaLogs / VictoriaTraces. Two related concerns:

1. **Parity** — when both VL/VT and Lakehouse claim to know the same fact, do they agree?
2. **Gaps** — what features does upstream support that the cold tier silently degrades or omits?

## Parity endpoint

`GET /lakehouse/api/v1/admin/parity[?window=24h]` runs the embedded VL stats path (`* | stats count() as n` with an embedded `_time:[start, end]` filter) and the manifest's `LiveAggregateWindow` for the same window. Both are answering "how many rows do we hold over this window?" from different code paths.

Response shape:

```json
{
  "start_unix_nano": ...,
  "end_unix_nano": ...,
  "vl_rows": <int>,
  "manifest_rows": <int>,
  "manifest_bytes": <int>,
  "manifest_files": <int>,
  "rows_delta": <vl - manifest>,
  "rows_delta_pct": <%>,
  "vt_internal_dropped": {"trace_id_idx": <n>, "service_graph": <n>},
  "expected_drift": <int>,
  "verified_drift": <rows_delta - expected_drift>,
  "verified_drift_pct": <%>,
  "per_tenant_supported": false,
  "per_tenant_note": "..."
}
```

`verified_drift` is the drift after accounting for VT-internal index rows the writer drops at insert time (`metrics.VTInternalRowsDropped`). Trace-mode drift is dominated by these dropped rows; subtracting them gives the operationally meaningful residual.

Auth-gated by `X-Lakehouse-Global-Read` (same surface as `/admin/tenant/migrate`).

### Expected drift behavior

| Signal | Typical `rows_delta_pct` | Typical `verified_drift_pct` | What dominates the residual |
|---|---|---|---|
| Logs | 0–2% | 0–2% | Manifest-window includes whole files that straddle the boundary; VL filters precisely. |
| Traces | 90–300% (raw) | 5–30% (after subtracting dropped) | Spans cluster within a trace duration; trace files span wider [Min, Max] than the window. |

If `verified_drift_pct` jumps significantly above these bands, investigate — most likely:
- writer stopped dropping VT-internal rows (regression)
- manifest's RefreshFromS3 missed a prefix (tenant-isolation routing bug)
- compaction wrote outputs to a different prefix than its inputs

## Cold-tier feature gaps register

What hot VT/VL gives users that the cold tier silently doesn't, with rough effort estimates so each gap is decision-ready.

### Traces

| Feature | Status | Severity | Notes / effort to close |
|---|---|---|---|
| **Service Graph** (`/api/v2/service-graph`) | **Closed** | UX-degradation | Reuses VT upstream's `servicegraph` background task — see Closed section below. |
| **Per-tenant stats group-by** (`* \| stats by(account_id, project_id) count()`) | Not supported via VL stats path | Metric-only | `account_id`/`project_id` are plain Parquet columns, not VL stream tags, so VL stats can't group on them. Workaround: read `/api/v1/tenants`. Closing requires promoting these to stream-id components or extending stats path. |
| **TraceQL non-trivial aggregations** | Partial | Functional-degradation | Simple traceQL works via vtselect → vlselect overlay. Complex `count_over_time()` / `histogram_over_time()` paths may not have been exercised end-to-end on cold tier. |
| **Live tail** (`/api/v2/search/tail`) | Returns 501 | Expected | Cold storage is write-once-read-many; live tail makes no sense post-flush. Handled gracefully. |
| **Span metrics auto-derive** | Not implemented | UX-degradation | Tempo emits derived RED metrics from spans; cold tier doesn't synthesize these. Workaround: use VT hot tier metrics over its retention period. |
| **VT-internal `trace_id_idx_stream` index rows** | Dropped at insert | Expected | Replaced by our `_trace_idx` Parquet footer KV — see `internal/traceindex`. The parity check counts dropped rows so the discrepancy is visible, not invisible. |
| **VT-internal `service_graph` stream rows** | **Kept** (changed from drop) | Resolved | Service-graph edges are LOW-cardinality aggregates VT's upstream `servicegraph` task emits; we persist them so the upstream `/select/jaeger/api/dependencies` reader works unchanged. See Closed section below. |

### Logs

| Feature | Status | Severity | Notes |
|---|---|---|---|
| **`pipe top`, `pipe unique`, `pipe unroll`** | Untested at scale | Risk-only | The vlselect dispatch overlay forwards these to our cold-tier reader; correctness assumed but not exhaustively tested. |
| **Sub-second `_time` precision on aggregations** | Hour-bucket precision in cold | Metric-only | Cold partitions are hour-granular; `_time:[<sec1>, <sec2>]` falls back to hour-bucket overlap so sub-hour windowed counts include some adjacent-hour rows. Drives the small parity residual on logs (~2%). |

### Cross-cutting

| Concern | Status | Severity | Notes |
|---|---|---|---|
| **Per-tenant bucket migration with concurrent writes** | Synchronous, full-window | Risk-only | `/admin/tenant/migrate` copies → flips manifest → deletes. New writes mid-migration land in the OLD bucket and become orphans needing a second migrate pass. Acceptable for the admin-only path; a "pause writes" knob would tighten this. |
| **Cross-tenant aggregations** | Gated by global-read header | Expected | Same gate hot VT/VL exposes; behaves identically. |
| **Stats snapshot vs manifest divergence** | Reconciled at API layer | Resolved | `/api/v1/tenants` now overlays manifest truth on registry entries; `LiveAggregateWindow` is the single source for time-bounded totals. |

## Versioning gap-register

This file is the source of truth for "what cold tier doesn't do yet". When closing a gap, move its row to a closed section at the bottom with the PR number and date so reviewers can see the trajectory.

### Closed (history)

**Service Graph** — PR #121

The Lakehouse cold tier now serves Grafana's Service Graph view via
the exact same code path the upstream hot tier uses. No reimplementation,
no new aggregation engine, no new storage format.

How it works:

1. VT's upstream `app/victoria-traces/servicegraph` package runs a
   background task. We enable it via `-servicegraph.enableTask=true`
   on `lakehouse-traces`.
2. Per tick (default 1m, set to 2m in our e2e compose), the task:
   - Calls `vtstorage.GetTenantIDs(start, end)` — we adapt this via
     `Adapter.WithTenantLister` to return every tenant the manifest
     holds, windowed to the tick's lookbehind.
   - For each tenant, runs `vtselect.GetServiceGraphTimeRange` which
     issues a JOIN+aggregation LogsQL query against spans —
     `vtstorage.RunQuery` is our adapter, so the query reads from
     cold-tier Parquet.
   - Calls `vtinsert.PersistServiceGraph` which writes the resulting
     edge rows tagged `{trace_service_graph_stream="-"}` back through
     `vtinsertutil.SetLogRowsStorage` (our writer adapter) — they
     land in cold-tier Parquet like any other row.
3. Grafana → Tempo datasource → Jaeger Dependencies API
   (`/select/jaeger/api/dependencies`) → `query.GetServiceGraphList`
   runs `{trace_service_graph_stream="-"} | fields parent, child,
   callCount | stats by (parent, child) sum(callCount)` and returns
   edges from cold storage.

The lakehouse-side delta to make this work was ~50 LOC:

- `vtInternalRowKind` split returns `(kind, drop)`: drop trace_id_idx
  (replaced by `_trace_idx` footer KV — too high-cardinality to
  persist), KEEP service_graph (low-cardinality aggregates the reader
  expects to find).
- `Adapter.WithTenantLister` option threads a real tenant list into
  upstream `GetTenantIDs`, replacing the legacy single-`{0,0}` answer.
- `Manifest.TenantSummariesInWindow(start, end)` exposes the per-tenant
  list filtered by file time overlap so the task only iterates
  tenants with relevant data.
- `servicegraph.Init() + defer servicegraph.Stop()` mounted at startup.

Compose enables `-servicegraph.enableTask=true -servicegraph.taskInterval=2m -servicegraph.taskLookbehind=5m`
to give the writer's 120s flush time to publish before the task scans.

Verification: `tests/e2e/service_graph_test.go::TestServiceGraph_ColdTierGeneratesEdges`
pushes a 3-span chain (service-a → service-b → service-c), waits up
to 5min for one task tick + flush, then asserts
`/select/jaeger/api/dependencies` returns the expected (parent, child)
edge.
