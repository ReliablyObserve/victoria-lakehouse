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

## Known divergences under investigation

Failures the hot-vs-cold parity suite (`tests/parity`, build tag `parity`)
reproduces on every run. Each is a real cold-tier divergence, not a harness
defect, and each is listed in `tests/parity/known_failures.txt` so CI fails
the moment a *new* one appears or one of these starts passing. The ids are
the handle the allowlist and the fixes refer to.

| Id | Divergence | Surfaces as |
|---|---|---|
| **B1** | Cold query rows carry columns hot does not: `<null>` placeholders for unset map attributes, the `account_id` / `project_id` tenant columns, the `ded_s01`…`ded_s08` dedicated slot columns, and unprefixed duplicates of the traces attributes (`service.name` next to `resource_attr:service.name`). | Every `rows_match` comparison, `facets` value sets, `traces_trace_id_lookup`, `traces_field_names_resource_attr_prefix`. |
| **B2** | `/select/logsql/field_names` is built from the Parquet columns present in the scanned files only, so it reports a fraction of the fields hot VL/VT lists (22 of 63 on the seeded traces corpus) and omits `_time`, `_msg`, `_stream`, the VT metadata fields and every map-stored attribute. | `field_names*` on both signals, `traces_field_names_vt_metadata`, `traces_field_names_span_attr_prefix`, `traces_field_names_completeness`. |
| **B3** | A filter combined with any pipe returns 0 rows on cold while hot returns the full match set. Seen for every filter on a non-promoted map attribute (with a pipe present, the cold column projection in `internal/storage/parquets3/projection.go` reads no map column), for an exact `_msg` literal that contains `:` (the same projection only reads the body for filters that look like free text, and a `:` inside the literal defeats that check), and for a `_msg` regexp containing an escaped double quote. | `range_numeric`, `ipv4_filter`, `exact_msg`, `field_exists_multi`, `negated_exists_combined`, the numeric range/comparison filters, `filter_with_double_quote`, `stats_by_format`. |
| **B4** | The `rename`, `format`, `len`, `math`, `extract` and `unpack_json` pipes drop their input columns on cold, so the output row is missing the fields the pipe read from. | `TestParity_PipesExtended/*`, `TestParity_PipesGapfill/string_functions`, `TestParity_PipesGapfill/chained_pipes_3plus`. |
| **B5** | `/select/logsql/hits` at sub-hour `step` returns evenly spaced synthetic buckets — the totals match hot but the per-bucket distribution is flat, because cold partitions are hour-granular and the sub-hour buckets are interpolated rather than counted. | `hits_small_step`, `hits_bucket_keys`. |
| **B6** | A tenant-scoped read on the cold tier answers with every tenant's rows rather than only the requesting tenant's: the logs query path in `internal/storage/parquets3/storage_query.go` selects files with `GetFilesForRange` instead of `GetFilesForRangeTenant` and never consults the request's tenant ids, and on both binaries `field_names`, `field_values` and `streams`, the pmeta catalog, the label index and the buffer bridge are unscoped; the traces Jaeger path passes `tenantIDs=nil`. | `TestTenantIsolation_Logs_PerTenantCounts/LH/*`, `TestTenantIsolation_Traces_PerTenantParity/*/field_values_hits`. |
| **B7** | A row whose timestamp is exactly the last nanosecond of the query window is dropped on cold. The HTTP `end` bound is exclusive and the upstream handler turns it into an inclusive bound by subtracting 1 ns; cold row-group pruning (`rowGroupMatchesTimeRange` in `storage_query.go`, both binaries) then compares that inclusive bound exclusively (`rgMin < endNs`), so a row group whose smallest timestamp sits on the bound is skipped. The file-level and row-level checks are inclusive and agree with hot. | `TestParity_TimeRange/boundary_ns_start_inclusive`. |

Each is fixed in its own PR; none of them is a test-harness problem, so the
suite records them rather than hiding them.

## Intentional differences

Behaviors where hot and cold deliberately disagree. These are **not** gaps —
a test that "fixes" one of them would be wrong.

| Behavior | Hot | Cold | Why |
|---|---|---|---|
| Bare `service.name` on traces LogsQL | No such field; matches nothing | Answers from a promoted alias column | The Lakehouse traces schema promotes `service.name` alongside VT's `resource_attr:service.name` so Grafana/Tempo-shaped queries work without the prefix. Pinned by `TestParity_Traces_LogsQL/traces_service_name_is_lh_only_alias`, which asserts hot returns 0 and cold returns rows. |
| Live tail (`/api/v2/search/tail`, `/select/logsql/tail`) | Streams | `501 Not Implemented` | Cold storage is write-once-read-many; there is nothing to tail after the flush. |

## Running the parity suite

The suite lives in `tests/parity` behind the `parity` build tag and runs
inside its own compose stack (`tests/parity/docker-compose.yml`), which
publishes no host ports — every service is addressed by container DNS from
the `parity-tests` service, so it can run alongside other local stacks.

```sh
docker compose -f tests/parity/docker-compose.yml build
docker compose -f tests/parity/docker-compose.yml up -d
# Wait until datagen-seed and datagen-seed-tenant2 have exited 0, then until
# each cold tier agrees with its hot counterpart on a positive row count — the
# logs corpus, and span_id:* for traces tenants 0 and 1 — unchanged across
# four checks 5s apart. The "Wait for LH to flush and settle" step of
# .github/workflows/parity.yaml is that poll.
docker compose -f tests/parity/docker-compose.yml --profile test run --rm --no-deps -T \
  parity-tests go test -tags=parity -json -count=1 -timeout=15m ./... \
  > parity-results.json
python scripts/ci/parity_ratchet.py --results parity-results.json
docker compose -f tests/parity/docker-compose.yml down -v
```

`--no-deps` is not optional. Without it `compose run` starts the
`parity-tests` dependencies again, and for the one-shot datagen services that
means seeding a second copy of the corpus — after the cold tier was checked
and while the suite is already reading it.

Five properties the harness has to keep, because breaking any of them turns
a comparison into a silent no-op or a result that depends on timing:

- **Quote field names containing `:`.** `resource_attr:service.name` unquoted
  parses as field `resource_attr` with a bucket, matches nothing, and the
  comparison then holds vacuously. Write `` `resource_attr:service.name` ``.
- **Ask for the seeded window.** `cmd/datagen` backfills at
  `now - rand[1..hours-back]h`; a short relative window like `_time:10m` is
  empty on both tiers. Use `seedWindowParams()` / `seedWindowFilter()` from
  `tests/parity/helpers.go`. A relative filter inside the query is evaluated
  at the request's `end`, which `seedWindowParams()` puts an hour after the
  newest row, so a case testing `_time:1h` also sets `end` to
  `seedWindowMidpoint()`.
- **Never compare against an empty reference.** `requireNonEmptyReference`
  fails every comparison — set, row, bucket, structure and count — whose
  reference side produced nothing; for counts that includes a `NaN` or empty
  aggregate value. If it fires, fix the query or the seed — relaxing the
  guard restores the vacuous pass it exists to catch. A case built to match
  nothing sets `ExpectEmpty`, and then both tiers must answer 0.
- **Look values up instead of guessing them.** The seed randomizes message
  text and timestamps, so a case that needs an exact message or a row's exact
  `_time` reads it from the reference tier with `referenceRow()` /
  `referenceRowTime()`, and a narrow window is anchored on a row that exists
  rather than on a fixed offset that is empty in some seeds.
- **Assert on cold rows only once they have settled.** For rows written
  seconds ago the cold tier's answer depends on where they are: the local
  buffer answers first, the first flushed file hides the other still-buffered
  rows behind its time watermark, and a manifest refresh racing the flush can
  drop the new file for one refresh interval. A tenant-scoped read that leaks
  on flushed data looks correctly scoped while the rows are buffered. So the
  settle step requires its counts to stay unchanged for three manifest refresh
  intervals, and a test that writes its own rows
  (`TestTenantIsolation_Logs_PerTenantCounts`) waits until the cold manifest
  lists them and the cold answers have stopped changing for as long — judged
  from evidence other than the answers it asserts on.

### The known-failure ratchet

`tests/parity/known_failures.txt` lists every test allowed to fail, one per
line: the test path, then whitespace, `#`, whitespace and a reason naming a
divergence id above (the whitespace is what lets a path such as `sub#01`
contain a `#`). One `# min-pass: N` directive records how many tests passed
when the list was last updated. `scripts/ci/parity_ratchet.py` reads
`go test -json` output, keyed by package and test, and fails the Parity Tests
job when:

- a failing test is not on the list (new divergence or harness regression),
- a test started and never finished, the test binary panicked, or a package
  failed without a failing test — a timeout or crash, which `go test -json`
  otherwise reports only as a package-level `fail` and which no list entry
  can cover,
- a listed test passes, skips, or no longer exists (stale entry — delete it),
- the pass count drops below `min-pass` (coverage went backwards, typically
  a test that started skipping on missing data).

A parent test that fails only because a listed subtest failed is accepted
without its own entry. `go test -json` does not say whether the parent's own
body failed as well, so such a parent is excused either way — keep
assertions out of parents whose subtests are listed. The list only ever
shrinks.

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
