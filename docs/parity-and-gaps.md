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

## Intentional differences

Places where cold deliberately does something hot does not, and why. These are
decisions, not gaps — a change here needs a reason, not a fix.

- **Traces: the bare Parquet-name alias for a promoted column.** A promoted
  traces column surfaces under VictoriaTraces' own name
  (`resource_attr:service.name`), and additionally under its raw Parquet spelling
  (`service.name`) **only when the query itself spells that name** — in a filter
  term, or in a column-selecting pipe (`fields`, `stats`, `uniq`, `top`).
  Operators routinely type the Parquet spelling because that is what a Parquet
  tool shows them, and VictoriaLogs' filters match on the names the DataBlock
  actually carries, so without the alias `service.name:="X"` matches zero rows
  while `_stream:{resource_attr:service.name="X"}` finds every one of them
  (a5576bf). Field-enumerating pipes do NOT trigger it: `field_names`,
  `field_values` and `facets` answer from the schema, not from an alias column,
  so their output carries VT's names alone. Hot VT has no `service.name` field at
  all, so on every query that does not spell it — a Grafana span list above all —
  cold returns exactly what hot returns.

## Cold-tier feature gaps register

What hot VT/VL gives users that the cold tier silently doesn't, with rough effort estimates so each gap is decision-ready.

### Traces

| Feature | Status | Severity | Notes / effort to close |
|---|---|---|---|
| **Service Graph — Jaeger Dependencies view** | **Closed** | UX-degradation | Cold-tier `/select/jaeger/api/dependencies` serves edges. See Closed section below. |
| **Service Graph — Tempo plugin view in Grafana** | Partial | UX-degradation | The Tempo plugin requires `serviceMap.datasourceUid` → Prometheus with `traces_service_graph_*` metrics; VT has no metrics_generator and our cold tier persists edges as LogsQL rows, not Prometheus metrics. Datasource UID wired so the plugin no longer errors, but the view renders empty. Closing fully needs a Prometheus-API shim on lakehouse-traces that converts `{trace_service_graph_stream="-"}` rows into `traces_service_graph_request_total{client=…, server=…}` series. ~200-400 LOC. |
| **Per-tenant stats group-by** (`* \| stats by(account_id, project_id) count()`) | Not supported via VL stats path | Metric-only | `account_id`/`project_id` are plain Parquet columns, not VL stream tags, so VL stats can't group on them. Workaround: read `/api/v1/tenants`. Closing requires promoting these to stream-id components or extending stats path. |
| **TraceQL non-trivial aggregations** | Partial | Functional-degradation | Simple traceQL works via vtselect → vlselect overlay. Complex `count_over_time()` / `histogram_over_time()` paths may not have been exercised end-to-end on cold tier. |
| **TraceQL search `/select/tempo/api/search?q=...`** | **Resolved** | UX-degradation | Was broken before the registry registration of cold-tier columns + full compose rebuild. Cold now matches hot on `{}`, `{.service.name="X"}` and other TraceQL filters. Pinned by `TestServiceGraphParity_TraceQLSearch`. |
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
| **Cold row field set** | **Resolved** | UX-degradation | Cold rows used to carry every Parquet leaf column — unset ones as the literal `"<null>"` — plus the tenant columns, the unmapped spare slots, and (on traces) a duplicate of every promoted attribute under its raw Parquet name and the service-graph edge columns. A cold row now carries exactly its ingested fields, under the same names hot returns. See the Closed section below. |

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

**Cold row field set** — 2026-09

A cold row now carries exactly the fields that were ingested for it, under the
same names hot VictoriaLogs / VictoriaTraces uses. Before, every row coming back
from cold storage carried:

- every Parquet leaf column of the file, with unset cells rendered as the literal
  string `"<null>"` (`"telemetry.sdk.name":"<null>"`, `"ded_s04":"<null>"`, ...);
- the tenant bookkeeping columns `account_id` and `project_id`;
- the unmapped Tier-2 spare slots `ded_s01`..`ded_s08`;
- on traces, each promoted attribute twice — under VictoriaTraces' name and under
  its raw Parquet spelling (`service.name` next to `resource_attr:service.name`,
  `span.name` next to `name`, `timestamp_unix_nano` next to `_time`) — plus the
  service-graph edge columns `parent` / `child` / `callCount` on plain spans.

Two causes, both in the scan path:

1. `parquetValueToInterface` was the only one of the four Parquet value
   converters without an `IsNull()` guard, so a NULL cell fell through to
   `parquet.Value.String()`, which renders it as `"<null>"`. VictoriaLogs treats
   an empty value as a non-existing field, so a NULL cell must map to `""`.
2. The two scan implementations — the columnar fast path (`readRowGroupColumnar`)
   and the row-oriented slow path (`projectedFieldsToDataBlock`) — each resolved
   field names on their own, and neither applied the column classification the
   typed reader applies (`logRowToFields` / `remapSlotFields`), which is why the
   typed path was correct and the scan path was not.

Both paths now resolve names through one shared `queryFieldName`:

- columns classified `ColumnInternal` (the tenant ids) never surface;
- a `ColumnSlot` surfaces under the attribute name the file's footer KV binds it
  to, and not at all when the slot is unbound — the same rule `remapSlotFields`
  applies on the typed path;
- everything else resolves through the schema registry, which supplies VT's
  `resource_attr:` / `span_attr:` prefixes;
- a key inside a MAP attribute column takes its column's prefix and then passes
  the same check (`mapAttrFieldName`), so there is one naming rule for every
  emitted field, not one per scan path and one per column shape;
- a column that is empty on every row of a row group is not emitted at all.

The raw Parquet spelling of a promoted traces column is still emitted alongside
the VT name, but only when the query actually spells it (a filter term or a
column-selecting pipe), so `service.name:="X"` keeps matching the same rows as
`_stream:{resource_attr:service.name="X"}` while a wildcard span list carries VT's
field names alone.

The classification lives in `internal/schema/columns.go` and is guarded by
`TestClassifyColumn_EveryRowColumnIsClassified`, which enumerates every top-level
Parquet column of `LogRow` and `TraceRow` and fails on one nobody classified — so
a column added later cannot leak into query results unnoticed.

One rule, not two: the same `emittableFieldName` check every top-level column
passes is also applied to the name a MAP attribute key would be emitted under, so
a key inside `log.attributes` / `span.attributes` cannot introduce a field name a
column is forbidden to produce. The check runs on the name that is actually
emitted, which is what makes one rule sufficient: a span attribute `account_id`
surfaces as `span_attr:account_id` and is an ordinary user attribute, while the
same key in a map whose attributes surface unprefixed would be the reserved name
itself and is dropped.

Verification: `internal/storage/parquets3/cold_row_fields_test.go` and
`lakehouse-traces/internal/storage/parquets3/cold_row_fields_test.go` (row-group
readers and the full query path, both the columnar and the row-oriented scan;
reserved-looking MAP keys asserted absent in both modules). For logs the typed
reader is the naming oracle and the scan path is asserted equal to it row by row.
For traces it is NOT: `traceRowToFields` spells MAP attributes and the Tier-1
dedicated columns bare (`custom.res`, `url.full`) where the scan path spells them
as VT does (`resource_attr:custom.res`, `span_attr:url.full`) — which is what hot
VT returns — so both spellings are pinned literally by
`TestColdSpanFields_TypedPathNamesAreExplicit`, with the values asserted equal
across the two. That split is a twin divergence worth closing separately; it is
not introduced here.

Other verification:
`tests/parity/cold_row_fields_test.go` (`TestParity_ColdRowFields`,
`TestParity_Traces_ColdSpanFields`), and the shared `assertNoInternalFields`
helper now also asserted in `TestParity_Response/jsonl_structure`. Registry rows:
`lh.rows.field_set_logs`, `lh.rows.field_set_traces`.

Performance: dropping ~30 columns per row from the wildcard scan makes it
cheaper, measured with `BenchmarkReadRowGroupColumnar_Wildcard100k` (one 100k-row
row group, no projection, Apple M5 Pro, `-benchtime 5x -count 5`, median):

| Module | ns/op before | ns/op after | allocs/op before | allocs/op after | B/op before | B/op after |
|---|---|---|---|---|---|---|
| logs | 155,244,183 | 79,132,592 (-49%) | 4,502,981 | 2,202,717 (-51%) | 237,613,003 | 183,950,430 (-23%) |
| traces | 143,373,533 | 120,211,033 (-16%) | 5,303,660 | 2,603,376 (-51%) | 279,252,492 | 219,069,683 (-22%) |

Routing MAP attribute keys through the same rule adds no allocations and about
3 ns per attribute: `BenchmarkMapAttrFieldName` measures the rule at 12.2 ns/op
(logs) and 16.0 ns/op (traces) against 9.0 and 13.3 ns/op for the bare
concatenate-and-intern loop it replaced, 0 allocs/op in all four cases — about
0.5% of the 100k-row wildcard scan above. An interleaved A/B of the wildcard
benchmark with and without the MAP-side check (10 alternating runs per module)
shows allocs/op and B/op unchanged and a fastest-run delta of +1.0% (logs) and
+0.75% (traces); the median deltas (+3.8% / +0.65%) were taken with host load
above 5 and are dominated by that noise.
