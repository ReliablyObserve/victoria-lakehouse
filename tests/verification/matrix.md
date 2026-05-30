# Victoria Lakehouse — Per-Component Verification Matrix

Living artifact tracking every exposed HTTP surface (logs + traces + admin
+ UI + Grafana datasources). Each row records: canonical request, expected
result, VL/VT parity reference, current LH response, and the test layer
that covers it. Sweep-style "all tests pass" reports don't substitute —
each row needs its own verdict.

**Maintenance rules:**
1. After any change that could affect a row, re-run that row's check and
   update `verified` date + `last_state`.
2. When adding a new endpoint, add a row BEFORE shipping the endpoint.
3. Rows with `state=UNVERIFIED` are known gaps — track them like bugs.
4. Each row's `vl_vt_ref` must point to the upstream baseline (VL or VT
   running the same query against the same data). LH-only surfaces with
   no upstream (e.g. lifecycle endpoints) use `spec` instead.

Related rules (memories): `feedback_per_component_verification`,
`feedback_layered_test_strategy`, `feedback_vl_vt_upstream`,
`feedback_no_silent_regressions`.

---

## Legend

- `state`: `PASS` (verified equal/equivalent to baseline) · `DIFFER` (known
  divergence, see notes) · `FAIL` (broken now, must fix) · `UNVERIFIED` (no
  test covers this — gap)
- `layer`: `unit` · `integration` · `parity` · `e2e` · `manual` — the
  authoritative test source for this row
- `vl_vt_ref`: file/test or curl command against `victorialogs:9428` /
  `victoriatraces:10428` that establishes the reference behavior
- `verified`: ISO date of last manual or CI verification

---

## Logs query surface (`lakehouse-logs:9428`)

| # | Endpoint | Query shape | state | layer | vl_vt_ref | last_state | verified |
|---|----------|-------------|-------|-------|-----------|-----------|----------|
| L1 | `/select/logsql/query` | wildcard `*` | PASS | parity + memory-regression | `tests/parity/logs_*_test.go` + `internal/storage/parquets3/query_memory_budget_test.go` (`TestRunQuery_ProductionShape_WildcardScalesUnderMemoryBudget` — 200 files × 5000 rows × wildcard projection × 16 file workers) + `tests/verification/probe_logs_24h_wildcard.sh` + `tests/verification/probe_logs_Nday_wildcard.sh` (2-day AND 7-day windows against live container) | row count + content match VL; container survives 24h AND 2-day AND 7-day wildcards on 2 GiB mem_limit (peak heap bounded by chunked DataBlock emission + row-group decoder semaphore + PutNoCopy cache wiring). Negative-control: disable all three fixes → 2-day probe restarts container. | 2026-05-30 |
| L2 | `/select/logsql/query` | exact `level:="ERROR"` | PASS | parity | same | identical results | 2026-05-29 |
| L3 | `/select/logsql/query` | OR `level:="ERROR" OR level:="WARN"` | PASS | parity | same | identical | 2026-05-29 |
| L4 | `/select/logsql/query_time_range` | bucketing | PASS | parity | same | identical | 2026-05-29 |
| L5 | `/select/logsql/facets` | `query=*` | PASS | parity | same | identical | 2026-05-29 |
| L6 | `/select/logsql/field_names` | `query=*` | PASS | parity + regression-test | `tests/parity/logs_*` + `storage_fields_footer_test.go` | footer-only, hits=non-null-count | 2026-05-29 |
| L7 | `/select/logsql/field_values` | `field=level&query=*` | PASS | parity + regression-test | same | column-projected | 2026-05-29 |
| L8 | `/select/logsql/field_values` | filter `level:=INFO`, target `service.name` | PASS | unit + parity | `TestGetFieldValues_UsesColumnProjectedRead` | column-projected, <50% bytes | 2026-05-29 |
| L9 | `/select/logsql/stream_field_names` | wildcard | PASS | parity | same | identical | 2026-05-29 |
| L10 | `/select/logsql/stream_field_values` | wildcard | PASS | parity | same | identical | 2026-05-29 |
| L11 | `/select/logsql/streams` | wildcard | PASS | parity + unit | column-projected refactor | identical | 2026-05-29 |
| L12 | `/select/logsql/stream_ids` | wildcard | PASS | unit (`TestComputeStreamID_*`) + insert wiring | `internal/vlstorage/stream_id.go` + `insert.go:75` (logs); `lakehouse-traces/internal/vlstorage/stream_id.go` + `insert.go:81` (traces) | LH now populates `_stream_id` at insert time using VL's exact hash algorithm (xxhash64 + `"magic!"` suffix, 48-char lowercase hex). Mirrored to both modules. | 2026-05-29 |
| L13 | `/select/logsql/hits` | wildcard buckets | PASS | parity | same | identical | 2026-05-29 |
| L14 | `/select/logsql/hits` | 18-OR drilldown query | PASS | unit + integration | `TestBloomFilterFilesByOrBranches_Integration` | OR-branch bloom evaluation | 2026-05-29 |
| L15 | `/select/logsql/stats_query` | `* \| stats count() rows` | PASS | parity | same | identical | 2026-05-29 |
| L16 | `/select/logsql/stats_query` | `\| stats by(level) count()` | PASS | parity | same | identical | 2026-05-29 |
| L17 | `/select/logsql/stats_query_range` | bucketed stats | PASS | parity | same | identical | 2026-05-29 |
| L18 | `/select/logsql/tail` | noop | PASS | unit | spec-only (no VL ref needed) | returns empty stream | 2026-05-29 |
| L19 | `/select/tenant_ids` | tenant enum | PASS | parity | same | identical | 2026-05-29 |

## Logs admin (LH-specific; spec compliance only)

| # | Endpoint | state | layer | spec | last_state | verified |
|---|----------|-------|-------|------|-----------|----------|
| LA1 | `/lakehouse/info` | PASS | manual | docs/architecture.md | 200, `{"mode":"logs","phase":"ready",...,"vl_compat":"1.50.0"}` | 2026-05-29 |
| LA2 | `/internal/lifecycle/drain` | PASS | unit | K8s scaling safety spec | returns 202+metric | 2026-05-25 |
| LA3 | `/internal/lifecycle/ready` | PASS | unit | same | returns 200/503 per state | 2026-05-25 |
| LA4 | `/internal/lifecycle/ring` | PASS | unit | same | returns member list JSON | 2026-05-25 |
| LA5 | `/internal/lifecycle/stale` | PASS | unit | same | returns staleness signal | 2026-05-25 |
| LA6 | `/api/v1/bloom/status` | PASS | manual | docs/bloom-index.md | 200, valid tiered status JSON | 2026-05-29 |
| LA7 | `/internal/cache/stats` | PASS | manual | docs/cache-architecture.md | 200, `{"az":"az-a","l1_entries":2,...}` | 2026-05-29 |
| LA8 | `/internal/cache/clear` | UNVERIFIED | manual | same | needs POST + before/after check | — |
| LA9 | `/manifest/range` | PASS | manual | docs/manifest-system.md | correct path (no `/internal/` prefix); matrix path was wrong | 2026-05-29 |
| LA10 | `/manifest/partitions` | PASS | manual | same | correct path (no `/internal/` prefix); matrix path was wrong | 2026-05-29 |

## Logs insert

| # | Endpoint | state | layer | last_state | verified |
|---|----------|-------|-------|-----------|----------|
| LI1 | `/insert/jsonline` | PASS | e2e | datagen pushes succeed | 2026-05-29 |
| LI2 | `/insert/loki/api/v1/push` (JSON) | UNVERIFIED | manual | — | — |
| LI3 | `/insert/loki/api/v1/push` (protobuf) | UNVERIFIED | manual | — | — |
| LI4 | `/insert/elasticsearch/_bulk` | UNVERIFIED | manual | — | — |
| LI5 | `/insert/opentelemetry/v1/logs` | UNVERIFIED | manual | — | — |
| LI6 | `/insert/datadog/api/v2/logs` | UNVERIFIED | manual | — | — |
| LI7 | `/insert/journald` | UNVERIFIED | manual | — | — |
| LI8 | `/insert/splunk/services/collector` | UNVERIFIED | manual | — | — |

## Traces query surface (`lakehouse-traces:10428`)

| # | Endpoint | Query shape | state | layer | vt_ref | last_state | verified |
|---|----------|-------------|-------|-------|--------|-----------|----------|
| T1 | `/select/logsql/query` | wildcard | PASS | parity | `tests/parity/traces_*` | identical | 2026-05-29 |
| T2 | `/select/logsql/query` | `trace_id:="..."` | PASS | parity | same | identical | 2026-05-29 |
| T3 | `/select/logsql/field_names` | wildcard | PASS | unit + parity | footer-only, single-file pattern | 2026-05-29 |
| T4 | `/select/logsql/field_values` | filtered | PASS | unit + parity | column-projected | 2026-05-29 |
| T5 | `/select/logsql/stats_query` | `\| stats by(service.name)` | PASS | parity | same | identical | 2026-05-29 |
| T6 | `/select/logsql/hits` | bucketed | PASS | parity | same | identical | 2026-05-29 |
| T7 | `/select/jaeger/api/services` | list services | PASS | unit + manual | `victoriatraces:10428/select/jaeger/api/services` | service-name truncation fixed — see commit dropping parquet column-index extraction in extractDistinctFromStats | 2026-05-29 |
| T8 | `/select/jaeger/api/traces` | search by service | PASS | manual | same | returns trace data with span sets; covered by `tests/verification/probe_jaeger_search_24h.sh` | 2026-05-30 |
| T8a | `/select/jaeger/api/traces` | search by service + tag | PASS | unit + manual | same | regression: adapter no longer pipe-strips before storage; covered by `tests/verification/probe_jaeger_search_24h_with_tag.sh` and `TestRunQuery_PreservesPipesToStorage` | 2026-05-30 |
| T9 | `/select/jaeger/api/traces/{id}` | trace lookup | PASS | manual | same | curl returned span set | 2026-05-29 |
| T10 | `/select/jaeger/api/services/{svc}/operations` | operations | UNVERIFIED | manual | same | — | — |
| T11 | `/select/jaeger/api/dependencies` | dep graph | UNVERIFIED | manual | same | — | — |
| T12 | `/select/tempo/api/search` | `q={}` | PASS | manual | same | curl returns trace list | 2026-05-29 |
| T13 | `/select/tempo/api/search/tags` | tag enum | DIFFER | manual | same | returns 200 with empty body — VT returns same shape; needs deeper check vs VT reference for non-empty case | 2026-05-29 |
| T14 | `/select/tempo/api/search/tag/{key}/values` | tag values | UNVERIFIED | manual | same | — | — |
| T15 | `/select/tempo/api/traces/{id}` | trace lookup | PASS | manual | same | trace_id resolved + returned | 2026-05-29 |
| T16 | `/select/tempo/api/metrics/query_range` | TraceQL `count_over_time() by(...)` | PASS | manual | same | curl returns series | 2026-05-29 |
| T17 | `/select/tempo/api/metrics/instant` | instant TraceQL | UNVERIFIED | manual | same | — | — |

## Traces insert

| # | Endpoint | state | layer | last_state | verified |
|---|----------|-------|-------|-----------|----------|
| TI1 | `/insert/jsonline` | PASS | e2e | datagen succeeds | 2026-05-29 |
| TI2 | `/insert/zipkin/api/v2/spans` | UNVERIFIED | manual | — | — |
| TI3 | `/insert/opentelemetry/v1/traces` | UNVERIFIED | manual | — | — |

## Grafana datasources (e2e compose; smoke query each)

| # | Datasource UID | Type | state | last_state | verified |
|---|----------------|------|-------|-----------|----------|
| G1 | victorialogs-hot | VictoriaLogs | PASS | basic query returns | 2026-05-29 |
| G2 | victorialogs-global | VictoriaLogs (vlselect) | PASS | hot+cold merge | 2026-05-29 |
| G3 | victoria-lakehouse-cold | VictoriaLogs (LH) | PASS | returns logs | 2026-05-29 |
| G4 | loki-vl-proxy | Loki | PASS | drilldown works | 2026-05-29 |
| G5 | loki-vl-proxy-cold | Loki cold-only | PASS | container healthy after restarting lakehouse-logs (had been OOM-stopped before the field-API fixes) | 2026-05-29 |
| G6 | victoriatraces-hot | Jaeger (VT) | PASS | services list returns | 2026-05-29 |
| G7 | victoriatraces-global | Jaeger (vtselect) | PASS | merged services list | 2026-05-29 |
| G8 | victoria-lakehouse-traces | Jaeger (LH) | PASS | service-name truncation fixed; see T7 | 2026-05-29 |
| G9 | tempo-vt-hot | Tempo (VT) | PASS | metrics_query_range returns | 2026-05-29 |
| G10 | tempo-global | Tempo (vtselect) | PASS | merged metrics | 2026-05-29 |
| G11 | tempo-lh-cold | Tempo (LH) | PASS | metrics_query_range + search return | 2026-05-29 |
| G12 | clickhouse-logs | ClickHouse | UNVERIFIED | — | — |
| G13 | clickhouse-traces | ClickHouse | UNVERIFIED | — | — |
| G14 | clickhouse-otel | ClickHouse | UNVERIFIED | — | — |
| G15 | clickhouse-analytics | ClickHouse | UNVERIFIED | — | — |
| G16 | victoriametrics-metrics | Prometheus | UNVERIFIED | — | — |

## UIs

| # | URL | state | last_state | verified |
|---|-----|-------|-----------|----------|
| U1 | `http://localhost:3003/` (Grafana home) | PASS | login + dashboards load | 2026-05-29 |
| U2 | `http://localhost:29428/select/vmui/` (LH logs VMUI) | PASS | health 200, UI loads | 2026-05-29 |
| U3 | `http://localhost:20428/select/vmui/` (LH traces VMUI) | PASS | health 200, UI loads | 2026-05-29 |
| U4 | Logs Drilldown (Grafana app) | UNVERIFIED | needs browser smoke | — |
| U5 | Traces Drilldown (Grafana app) | DIFFER | metrics queries return data; user reported empty panels — likely fixed after OOM fixes; needs re-verification | 2026-05-29 |

## Open bugs / known gaps

1. ~~**T7 / G8** — Jaeger service-name truncation.~~ **FIXED** by
   removing the parquet column-index seed in
   `extractDistinctFromStats`; data-page scan is now the only source.
2. **L12** — `/select/logsql/stream_ids` returns empty for LH because
   the external insert path never populates `_stream_id`. **Design
   decision below.**
3. ~~**G5** — `loki-vl-proxy-cold` unhealthy.~~ **FIXED** by starting
   the `lakehouse-logs` container (it had been OOM-stopped before the
   field-API memory fixes landed).
4. Roughly half of LA*, LI*, T8-T17, TI*, G12-G16 rows are
   `UNVERIFIED` — gaps in the manual/e2e layer. Build them out one
   row at a time as bugs surface or before each release.

### L12 — `_stream_id` must be populated (100% VL API compat)

**Status**: FAIL. Real bug, not "expected divergence".

The 100% VL/VT API compatibility rule is non-negotiable
(`feedback_vl_vt_upstream`). Every field VL itself returns from
`/select/logsql/stream_ids` MUST be returned by LH. "Document the
divergence as expected" is **not an acceptable resolution** — that
was an earlier wrong call now corrected.

**Background**: VL computes `_stream_id` as a uint64 hash over the
`_stream` labels. The hash is part of VL's on-disk format and is
present on every row VL returns to a client. Lakehouse's external
insert path accepts already-flattened rows from `vlinsert` and writes
to Parquet without setting `_stream_id` — so cold rows have an empty
value.

**Fix direction**: replicate VL's hash at insert time, store it in
the Parquet `_stream_id` column. Re-deriving at query time is a
fallback if insert-time computation isn't viable; either way the
output MUST match what VL produces for the same `_stream` labels.

**Sub-tasks**:
1. Locate VL's stream-ID hash function (`deps/VictoriaLogs/lib/logstorage/stream_id.go` or equivalent) and verify it's pure-fn of the `_stream` label string.
2. Call it from LH insert path (`internal/vlstorage/insert.go`) when `_stream_id` field is empty on the incoming row.
3. Add a parity test that asserts LH's `_stream_id` for a known `_stream` label matches VL's for the same label (use VL's own implementation as the oracle).
4. Backfill existing cold rows: optional; for new data the fix takes effect immediately.

**Owner**: not assigned. Tracked as L12 FAIL.

## Process for filling gaps

For each `UNVERIFIED` row:

1. Bring up `tests/parity/docker-compose.yml` or
   `deployment/docker/docker-compose-e2e.yml` per the surface.
2. Run the canonical request via `curl` against both LH and the
   `vl_vt_ref` reference.
3. Diff the responses (jq, json-diff).
4. If equivalent, flip `state=PASS`, set `verified=<date>`.
5. If different, document under "Open bugs / known gaps" with
   reproduction + suspected cause.
6. Add a regression test in the appropriate layer (`tests/parity/`
   for query surfaces, `internal/.../*_test.go` for unit/integration).

## How fixes update this file

The `feedback_no_silent_regressions` rule: after every commit that
touches a surface, find the row, re-run the check, update `last_state`
and `verified`. If the change is meant to flip `state` (e.g. fixing a
DIFFER to PASS), the commit message must say so.
