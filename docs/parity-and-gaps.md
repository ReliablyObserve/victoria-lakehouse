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
| **Tenant read scoping** | Resolved | — | Cold-tier reads answer from exactly one tenant — the request's `AccountID`/`ProjectID`, or `0:0` without headers — on both binaries and for every read class (row scan, timestamp-only and `count()` metadata fast paths, field/stream enumeration, pmeta catalog answers, Jaeger handlers, the traces trace-by-id index lookup, the multi-pod buffer bridge), in both the shared-bucket prefix layout and the bucket-per-tenant layout. Pinned by the invariant suites in `internal/storage/parquets3/tenant_scope_test.go` (and the traces twin), exact per-tenant counts in `tests/e2e/multitenancy_test.go`, and `tests/parity/tenant_scope_parity_test.go`. See [multi-tenancy — Read Scoping](multi-tenancy.md#read-scoping-which-data-a-request-sees). |
| **Cross-tenant aggregations** | Gated by the global-read credential | Expected | The cold tier widens a read to every tenant only for a request carrying the configured global-read header or bearer token. Hot VL/VT have no such credential — a cross-tenant read there goes through a per-tenant fan-out (`/select/tenant_ids`) — so this is a Lakehouse addition, not a parity surface. |
| **Bucket-per-tenant manifest refresh** | Resolved | — | The periodic refresh listed only the default bucket, so objects that lived solely in a dedicated tenant bucket left the manifest at the next refresh. It now lists each override bucket under its tenant prefix as well. |
| **`isolation: bucket` + `bucket_template`** | Open | Functional-degradation | Validated at startup but not wired to bucket routing; per-tenant `overrides[].s3.bucket` is the working form. |
| **Stats snapshot vs manifest divergence** | Reconciled at API layer | Resolved | `/api/v1/tenants` now overlays manifest truth on registry entries; `LiveAggregateWindow` is the single source for time-bounded totals. |
| **`field_values` from the pmeta catalog** | Open | UX-degradation | When every partition in range is catalogued, low-card and complete, `field_values` answers from the catalog in RAM. That answer differs from hot VL/VT in three ways: every value carries `hits` 1 (hot returns the number of matching rows, and returns 0 for every value once the result exceeds `limit`); with a `limit`, the catalog returns the first `limit` values in sort order, where hot returns whichever values its search met first; and the value set is hour-granular — a window that cuts a partition hour lists the values of the whole hour. Requests the catalog cannot answer exactly go to the row scan, which counts hits per row inside the window. |
| **Cold row field set** | **Resolved** | UX-degradation | Cold rows used to carry every Parquet leaf column — unset ones as the literal `"<null>"` — plus the tenant columns, the unmapped spare slots, and (on traces) a duplicate of every promoted attribute under its raw Parquet name and the service-graph edge columns. A cold row now carries exactly its ingested fields, under the same names hot returns. See the Closed section below. |

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

**Cold `field_values` listed a subset of the values in range** — intermittent in
the parity suite (`field_values_level`, `field_values_jsonl`: cold answered 3 of
hot's 4 levels, once 1 of 4, while every row count agreed). The pmeta catalog is
keyed by Parquet column (`severity_text`) and was looked up with the request's
name (`level`), so every aliased field missed it — `level` on logs; `name`,
`status_message`, `resource_attr:*`, `span_attr:*` on traces. The miss fell
through to the in-memory label index, whose values are a sample of the first
rows of whichever file the process's first query opened, and that sample was
returned as the whole answer; which file came first depended on query order.

Fixed together:

- the catalog lookup resolves the name through the schema registry
  (`catalogFieldKey`);
- `field_values` never answers from the label index;
- the catalog answers only when it holds the complete value set of every
  partition in range. A partition without a catalog, a partition where the field
  is high-card, or a file whose labels never reached the catalog sends the
  request to the row scan; unioning the remaining partitions returned a subset;
- the row scan behind `field_values`, `streams` and `stream_ids` applies the
  query window per row, so a file straddling a window edge no longer contributes
  values or hits from rows outside it;
- `field_names` over a window holding no objects is empty (both binaries).

Not part of this fix: the `field_names` gap the same parity run logged (cold 35
fields, hot 42) is B2 — cold field names come from Parquet columns only and map
keys are not expanded. The catalog path's remaining differences from upstream are
recorded in the cross-cutting table above.

Regression tests (both modules): `TestFieldValues_AliasedField_ServedFromCatalog`,
`TestFieldValues_SampledLabelIndexIsNeverTheAnswer`,
`TestFieldValues_AliasedField_CatalogStaysTenantScoped`,
`TestFieldValues_CatalogUnionWithAHighCardPartitionScans`,
`TestFieldValues_CatalogMissingAFileScans`, `TestFieldValues_ScanIsConfinedToTheWindow`,
`TestStreams_ScanIsConfinedToTheWindow`, `TestFieldNames_EmptyWindowIsEmpty`,
`TestCatalogFieldKey`; `internal/pmeta`: `TestFieldValuesExact_DistinguishesAbsentFromHighCard`,
`TestCatalogCoversFile`. Cost: see `BenchmarkFieldValues_Level` below.

`BenchmarkFieldValues_Level` (`internal/storage/parquets3`, 24 hourly partitions
× 400 rows, 4 levels, Apple M5 Pro, `-benchtime 2000x`; `window=cut` starts and
ends mid-hour so two files straddle it). Against `origin/main`, pmeta on went
from 7.5 µs to 10.0 µs per request: the request is now a catalog answer instead
of a catalog miss plus a label-index read. pmeta off went from 74 ns (the
sampled index) to 3.0 ms (a projected scan of 24 files from the local mock S3).
The completeness checks and the per-row window, measured interleaved against the
first commit of the fix (2 × 5 runs each, medians, host load average 12–22, so
treat single-digit percentages as noise):

| Case | first commit | with completeness + window |
|---|---|---|
| pmeta on, whole window | 18.5 µs | 20.6 µs |
| pmeta on, cut window | 18.4 µs | 19.8 µs |
| pmeta off, whole window | 5.9 ms | 6.3 ms |
| pmeta off, cut window | 6.8 ms | 5.3 ms |

The catalog path pays one file-meta lookup per file in range (about 7–12% here);
the scan's per-row window check applies only to the files that straddle the
window and is within the noise of this host.

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
