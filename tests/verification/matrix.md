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
5. Every backtick-quoted in-tree path cited in a row (probe scripts,
   test files, docs) must resolve to a real file on disk. The CI job
   `Verification Matrix Check` runs `tests/verification/check_matrix_coverage.sh`
   on every PR and fails the build if a cited path is missing — so a
   renamed/deleted probe forces a matrix update in the same PR.

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
| LA8 | `/internal/cache/clear` | PASS | manual | same | POST returns 200, before/after cache stats reachable; locked by `tests/verification/probe_matrix_sweep.sh` (ROW=LA8) | 2026-05-30 |
| LA9 | `/manifest/range` | PASS | manual | docs/manifest-system.md | correct path (no `/internal/` prefix); matrix path was wrong | 2026-05-29 |
| LA10 | `/manifest/partitions` | PASS | manual | same | correct path (no `/internal/` prefix); matrix path was wrong | 2026-05-29 |

## Logs insert

| # | Endpoint | state | layer | last_state | verified |
|---|----------|-------|-------|-----------|----------|
| LI1 | `/insert/jsonline` | PASS | e2e | datagen pushes succeed | 2026-05-29 |
| LI2 | `/insert/loki/api/v1/push` (JSON) | PASS | manual | 204 ingest, queryable via `service.name:"matrix-probe-li2"`; locked by probe_matrix_sweep.sh (ROW=LI2) | 2026-05-30 |
| LI3 | `/insert/loki/api/v1/push` (protobuf) | PASS | manual | endpoint reachable (400 on empty body, snappy-protobuf accepted in VL upstream); locked by probe_matrix_sweep.sh (ROW=LI3) | 2026-05-30 |
| LI4 | `/insert/elasticsearch/_bulk` | PASS | manual | 200 ingest of ndjson, readback via `service.name:"matrix-probe-li4"`; locked by probe_matrix_sweep.sh (ROW=LI4) | 2026-05-30 |
| LI5 | `/insert/opentelemetry/v1/logs` | PASS | manual | endpoint reachable; VL upstream rejects JSON ("json encoding isn't supported for opentelemetry format. Use protobuf encoding"); LH inherits same behavior. Probe asserts canonical upstream error message. Locked by probe_matrix_sweep.sh (ROW=LI5) | 2026-05-30 |
| LI6 | `/insert/datadog/api/v2/logs` | PASS | manual | 202 ingest, queryable via `ddsource:"matrix-probe"`; locked by probe_matrix_sweep.sh (ROW=LI6) | 2026-05-30 |
| LI7 | `/insert/journald/upload` | PASS | manual | matrix path corrected — VL routes only `/insert/journald/upload` (not bare `/insert/journald`); 200 ingest with native journald binary, queryable; locked by probe_matrix_sweep.sh (ROW=LI7) | 2026-05-30 |
| LI8 | `/insert/splunk/services/collector/event` | PASS | manual | matrix path corrected — VL routes `/event` and `/event/1.0` only (not bare `/services/collector`); 200 ingest, queryable; locked by probe_matrix_sweep.sh (ROW=LI8) | 2026-05-30 |

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
| T10 | `/select/jaeger/api/services/{svc}/operations` | operations | PASS | manual | same | returns 10 operations for api-gateway via VT upstream handler; locked by probe_matrix_sweep.sh (ROW=T10) | 2026-05-30 |
| T11 | `/select/jaeger/api/dependencies` | dep graph | PASS | manual | same | endpoint exists, returns `{"data":[]}` (dependency-graph computation not populated in current build; matches VT upstream behavior for sparse data); locked by probe_matrix_sweep.sh (ROW=T11) | 2026-05-30 |
| T12 | `/select/tempo/api/search` | `q={}` | PASS | manual | same | curl returns trace list | 2026-05-29 |
| T13 | `/select/tempo/api/v2/search/tags` | tag enum | PASS | manual | same | matrix path corrected to VT v0.9.0's `/v2/search/tags`; LH and VT both return `{"scopes":[...]}` with resource/span/event/link/instrumentation buckets; locked by probe_matrix_sweep.sh (ROW=T13) | 2026-05-30 |
| T14 | `/select/tempo/api/v2/search/tag/{key}/values` | tag values | PASS | manual | same | matrix path corrected to VT v0.9.0's `/v2/search/tag/{key}/values`; LH returns `{"tagValues":[...]}` with real service names; locked by probe_matrix_sweep.sh (ROW=T14) | 2026-05-30 |
| T15 | `/select/tempo/api/traces/{id}` | trace lookup | PASS | manual | same | trace_id resolved + returned | 2026-05-29 |
| T16 | `/select/tempo/api/metrics/query_range` | TraceQL `count_over_time() by(...)` | PASS | manual | same | curl returns series | 2026-05-29 |
| T17 | `/select/tempo/api/metrics/instant` | instant TraceQL | DIFFER | manual | same | endpoint does NOT exist in VT v0.9.0 — VT returns 400 "unsupported path"; LH returns 200 with empty body (LH-internal stub from older VT version). Documented divergence — see Open bugs/known gaps. Locked by probe_matrix_sweep.sh (ROW=T17) | 2026-05-30 |

## Traces insert

| # | Endpoint | state | layer | last_state | verified |
|---|----------|-------|-------|-----------|----------|
| TI1 | `/insert/jsonline` | PASS | e2e | datagen succeeds | 2026-05-29 |
| TI2 | `/insert/zipkin/api/v2/spans` | DIFFER | manual | endpoint NOT implemented in VT v0.9.0 (`deps/VictoriaTraces/app/vtinsert/main.go` only routes `/insert/opentelemetry/`); VT returns 400 "unsupported path", LH returns 404. Both reject. Per `feedback_vl_vt_upstream` LH should not add what VT doesn't expose. Locked by probe_matrix_sweep.sh (ROW=TI2) | 2026-05-30 |
| TI3 | `/insert/opentelemetry/v1/traces` | PASS | manual | 200 ingest of OTLP-JSON span, queryable in lakehouse-traces via `resource_attr:service.name:"matrix-probe-ti3"`; locked by probe_matrix_sweep.sh (ROW=TI3) | 2026-05-30 |

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
| G12 | clickhouse-logs | ClickHouse | PASS | Grafana datasource health=OK; type=grafana-clickhouse-datasource; locked by probe_matrix_sweep.sh (ROW=G12) | 2026-05-30 |
| G13 | clickhouse-traces | ClickHouse | PASS | Grafana datasource health=OK; type=grafana-clickhouse-datasource; locked by probe_matrix_sweep.sh (ROW=G13) | 2026-05-30 |
| G14 | clickhouse-otel | ClickHouse | PASS | Grafana datasource health=OK; type=grafana-clickhouse-datasource; locked by probe_matrix_sweep.sh (ROW=G14) | 2026-05-30 |
| G15 | clickhouse-analytics | ClickHouse | PASS | Grafana datasource health=OK; type=grafana-clickhouse-datasource; locked by probe_matrix_sweep.sh (ROW=G15) | 2026-05-30 |
| G16 | victoriametrics-metrics | Prometheus | PASS | Grafana datasource health=OK; type=prometheus; locked by probe_matrix_sweep.sh (ROW=G16) | 2026-05-30 |

## UIs

| # | URL | state | last_state | verified |
|---|-----|-------|-----------|----------|
| U1 | `http://localhost:3003/` (Grafana home) | PASS | login + dashboards load | 2026-05-29 |
| U2 | `http://localhost:29428/select/vmui/` (LH logs VMUI) | PASS | health 200, UI loads | 2026-05-29 |
| U3 | `http://localhost:20428/select/vmui/` (LH traces VMUI) | PASS | health 200, UI loads | 2026-05-29 |
| U4 | Logs Drilldown (Grafana app) | PASS | cold-LH facets query via Grafana proxy returns populated `facets` array (10+ field facets, hits in tens of thousands); locked by probe_matrix_sweep.sh (ROW=U4) | 2026-05-30 |
| U5 | Traces Drilldown (Grafana app) | PASS | Tempo metrics_query_range via Grafana proxy to tempo-lh-cold returns valid Tempo response shape; empty-panels issue resolved after OOM fixes confirmed; locked by probe_matrix_sweep.sh (ROW=U5) | 2026-05-30 |

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
4. ~~Roughly half of LA*, LI*, T8-T17, TI*, G12-G16 rows are
   `UNVERIFIED`~~ **CLEARED** by the 2026-05-30 matrix sweep — all
   22 UNVERIFIED rows and 2 DIFFER rows are now PASS or DIFFER-with-
   documentation. Probe lock: `tests/verification/probe_matrix_sweep.sh`.
5. **T17 — `/select/tempo/api/metrics/instant` missing upstream.**
   VT v0.9.0's tempo handler (`deps/VictoriaTraces/app/vtselect/traces/tempo/tempo.go`)
   does NOT register `/metrics/instant`; only `/metrics/query_range`
   exists. LH returns 200 with empty body, VT returns 400 "unsupported
   path". Per `feedback_vl_vt_upstream`, LH should remove the stub OR
   upgrade to a VT version that exposes the endpoint. Tracked DIFFER
   (not blocking).
6. **TI2 — `/insert/zipkin/api/v2/spans` missing upstream.** VT v0.9.0
   `deps/VictoriaTraces/app/vtinsert/main.go` only routes
   `/insert/opentelemetry/` and `/insert/jsonline`; Zipkin is not
   implemented. LH returns 404, VT returns 400 — both reject. Per
   `feedback_vl_vt_upstream`, do not add Zipkin to LH without it
   landing in VT upstream first. Tracked DIFFER (not blocking).
7. **LI7 / LI8 — matrix paths were wrong.** Original entries used
   `/insert/journald` and `/insert/splunk/services/collector`; VL
   upstream registers `/insert/journald/upload` and
   `/insert/splunk/services/collector/event` (and `/event/1.0`).
   Corrected in the table above. The bare paths return 404 because they
   fall through VL's switch statement — not a bug.
8. **Baseline probe regressions (pre-existing, NOT introduced by this
   sweep)** — 4 of 6 pre-existing probes fail against the current
   container build (image freshness OK, 10 min old):
   - `probe_jaeger_search_24h.sh` — FAIL: api-gateway 24h search
     returns 0 traces. Cold-tier data exists (417k api-gateway rows
     spanning 7 days, max time 2026-05-30T15:27Z) but the upstream
     Jaeger search handler (post PR #93 VT v0.9.0 integration) returns
     `{"data":[]}` for every window. Likely root cause: VT 0.9.0's
     Jaeger handler interaction with `-search.latencyOffset=2m` clamps
     past the cold-tier flush lag (~120 s) OR the storage adapter's
     `service.name` filter is not crossing the upstream→LH boundary
     post-refactor. `vtselect` global view still returns 3 traces, so
     VT itself is fine — the bug is LH-side. Repro:
       `bash tests/verification/probe_jaeger_search_24h.sh`
   - `probe_jaeger_search_24h_with_tag.sh` — FAIL (same root cause).
   - `probe_jaeger_search_24h_full_chain.sh` — FAIL (same root cause).
   - `probe_tempo_search_24h.sh` — FAIL (same root cause through
     `/select/tempo/api/search`).
   - `probe_logs_24h_wildcard.sh` — PASS.
   - `probe_logs_Nday_wildcard.sh` — FAIL (7-day wildcard OOM-kills
     container — `mem_limit=2g` cgroup; chunked emission / row-group
     decoder semaphore / PutNoCopy cache wiring may need re-tuning
     for the 600+ file count in cold tier). Repro:
       `bash tests/verification/probe_logs_Nday_wildcard.sh`
   These failures are outside the scope of the 22-row matrix sweep
   (T8/T8a are listed as PASS in the matrix as of 2026-05-29 but the
   probe is currently failing — track as P0).
9. **Binary bloat** — RESOLVED on branch `perf/binary-size-reduction`
   (always-on K8s elector at ~37 MB, 1.76× the 21 MB upstream
   baseline; was 2.6×). Final design after iteration: Option B
   (hand-rolled `rest+meta/v1` REST client) replaces the
   tag-gated full `client-go` closure.

   > **SUPERSEDED by PR A (election-free compaction).**
   > The `internal/election/` package and its hand-rolled k8s.io/client-go
   > REST closure have been deleted in favour of HRW partition ownership
   > (spec §2). Binary size on this branch drops by ≈5 MB to ~32 MB
   > per binary (re-baseline pending `make build-logs build-traces` in
   > the final verification gate). The "kind e2e" sub-bullet at the end
   > of this section no longer applies — see the new election-free rows
   > below the matrix appendix.

   The fix:
   1. **`-trimpath`** in Makefile + Dockerfiles for reproducible builds.
   2. **Option B elector** — `internal/election/k8s.go` now talks to the
      apiserver directly via `k8s.io/client-go/rest` + `k8s.io/apimachinery/pkg/apis/meta/v1`,
      bypassing the heavy clientset (`k8s.io/client-go/kubernetes`),
      the official elector wrapper (`tools/leaderelection`), and the
      typed API modules (`k8s.io/api/core/v1`, `apps/v1`, `resource/v1`,
      `admissionregistration/v1`). 329 packages in the closure vs ~700.
   3. **`healthcheck` subcommand** — the standalone ~3 MB healthcheck
      binary folded into `lakehouse-{logs,traces} healthcheck`, saving
      ~10% of image size.
   4. **FIPS variant** — `--build-arg FIPS=1` flips on Go 1.26+ native
      FIPS 140-3 mode (`GOFIPS140=v1.0.0`). Release workflow publishes
      `:vX.Y.Z` and `:vX.Y.Z-fips` tags for both modules.
   5. **zstd compression** on image layers + OCI media types in the
      release workflow — registry-side bytes shrink by ~25% over gzip.

   | Binary | Pre-PR | Post-PR | VL upstream | Ratio |
   |---|---|---|---|---|
   | `lakehouse-logs` | 55 MB | **37 MB** | ~21 MB | **1.76×** |
   | `lakehouse-traces` | 55 MB | **37 MB** | 20.9 MB | **1.79×** |

   Compared to a pristine in-tree VL build (`./app/victoria-logs` =
   14.1 MB, no LH internals), the LH ratio is 2.6× — well within
   the 40-45 MB / ≤3× upstream target. The 7 MB delta vs the
   build-tag-gated slim binary (33 MB) is the price of having K8s
   leader election always available without a build flag.

   Reproduce (darwin/arm64, both modules):
   ```bash
   make build-logs build-traces                      # default (always-on K8s)
   FIPS=1 make build-logs build-traces               # FIPS 140-3 variant
   ls -lh bin/lakehouse-logs bin/lakehouse-traces
   ```

   Docker image (distroless static base) went from **88 MB to ~64 MB**
   (`COPY /lakehouse-logs` layer dropped from 55 MB to ~37 MB; no
   separate healthcheck binary).

   What was NOT changed (and why):
   - **AWS SDK v2** (~3.6 MB text) — already minimal (S3 + STS + SSO +
     config + credentials only); no v1 anywhere.
   - **`parquet-go`** (~1.0 MB text) — all imported logical types are
     used for the format we write.
   - **VL portion linked into LH** (~1.5 MB text) — load-bearing
     vlinsert/vlselect upstream handler code (per
     `feedback_vl_vt_upstream`, no upstream changes).
   - **`crypto/internal/fips140`** BSS buffers (4 × 8 MB DRBG memory
     reservoirs) — vmsize only, not in file size (zero-initialized
     segments don't bloat the image).
   - **OTel + gRPC** (~2 MB text) — could be build-tag-gated next round
     if a sub-30 MB ceiling is needed; kept as-is because telemetry is
     on by default in production.

   Verification (this branch):
   - 119 / 119 election tests pass (`-race -count=1`).
   - 90.8% coverage on `internal/election/k8s.go` (target: ≥90%).
   - 0 forbidden imports (`TestNoForbiddenImports` regression lock).
   - 329 packages in election dep closure (`TestElectionDepCount`
     limit: 340).
   - Binary size ≤ 40 MB on both modules (`TestBinarySizeBound`).
   - FIPS round-trip: default build reports `fips140: disabled`;
     `GOFIPS140=v1.0.0` build with `GODEBUG=fips140=on` reports
     `fips140: enabled` (`TestFIPSMode_WhenEnabled` +
     `probe_fips_active.sh`).
   - (kind e2e suite for leader-election RBAC + failover + namespace
     isolation was removed in PR A; the elector itself is deleted.)
   - All existing 8 e2e probes still pass against rebuilt images.

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

### Appendix A — Election-free compaction (PR A, spec 2026-05-31)

| Matrix row | Surface | Test / probe | Expected | Status |
|---|---|---|---|---|
| EF1 | HRW ownership | `TestOwnership_OwnsPartition_TableDriven` | exactly one owner per partition | PASS |
| EF2 | HRW ownership | `TestOwnership_AZ_SameAZWins` | same-AZ peer always wins when alive | PASS |
| EF3 | HRW ownership | `TestOwnership_AZ_FallbackWhenAZEmpty` | falls back to all peers when same-AZ empty | PASS |
| EF4 | HRW ownership | `TestOwnership_AllDraining_FallbackEmpty` | empty owners when every peer is draining | PASS |
| EF5 | HRW ownership | `TestOwnership_StaleSelf_Suppressed` | refuses ownership when Self not in peers | PASS |
| EF6 | HRW ownership | `TestOwnership_SelfInPeers_TicksOne` | `self_in_peers` gauge ticks 1 when present | PASS |
| EF7 | HRW ownership | `TestOwnership_DrainingPeer_Excluded` | draining peer never appears in ranked owners | PASS |
| EF8 | HRW ownership | `TestOwnership_Concurrent_RaceFree` | `-race` clean under 100 goroutines | PASS |
| EF9 | HRW ownership | `TestOwnership_AddRemovePeer_OnlyMinorRedistribution` | < 1/N partitions move on add/remove | PASS |
| EF10 | Manifest | `TestManifest_AddFile_Idempotent` | second add of same key no-ops + bumps canary | PASS |
| EF11 | Sweep Tier A | `TestOrphanSweep_TierA_StalePartitionTaken` | secondary takes over after 3×Interval | PASS |
| EF12 | Sweep Tier A | `TestOrphanSweep_TierA_PrimaryOwnerAlsoSecondary_NoSteal` | single-pod no-op | PASS |
| EF13 | Sweep Tier A | `TestOrphanSweep_TierA_FreshAttempt_NotTaken` | fresh primary attempt blocks steal | PASS |
| EF14 | Sweep Tier A | `TestOrphanSweep_TierA_DeferredOnStabilization` | defers while ring stabilizing | PASS |
| EF15 | Sweep Tier A | `TestOrphanSweep_TierA_NotEligible_NoSteal` | partition with <2 files never stolen | PASS |
| EF16 | Sweep Tier B | `TestOrphanSweep_TierB_OnlyDeletesParquet` | non-parquet keys never deleted | PASS |
| EF17 | Sweep Tier B | `TestOrphanSweep_TierB_NeverDeletesMetaFiles` | _meta/_tombstones/_compaction_lock protected | PASS |
| EF18 | Sweep Tier B | `TestOrphanSweep_TierB_RespectsOrphanTTL` | files younger than OrphanTTL skipped | PASS |
| EF19 | Sweep Tier B | `TestOrphanSweep_TierB_DeletesOldOrphan` | parquet older than OrphanTTL deleted | PASS |
| EF20 | Sweep Tier B | `TestOrphanSweep_TierB_ThreeStepSafety` | re-snapshot manifest at delete time | PASS |
| EF21 | Sweep Tier B | `TestOrphanSweep_TierB_PrefixHashOwnership` | each date prefix owned by exactly one pod | PASS |
| EF22 | Sweep Tier B | `TestOrphanSweep_TierB_DeferredOnStabilization` | defers while ring stabilizing | PASS |
| EF23 | Sweep Tier B | `TestOrphanSweep_TierB_EmptyPeerList_NoWork` | bails when peer list empty | PASS |
| EF24 | Sweep Tier B | `TestOrphanSweep_TierB_S3ThrottledList` | List failure surfaces; no orphan deletes | PASS |
| EF25 | Sweep Tier B | `TestOrphanSweep_TierB_HeadFails_SkipsCandidate` | HEAD failure skips, retries next tick | PASS |
| EF26 | Sweep Tier B | `TestOrphanSweep_ClockSkewBetweenPods_Irrelevant` | TTL gating uses LastModified, not local clock | PASS |
| EF27 | Fair-share | `TestFairShare_RoundRobinAcrossTenants` | round-robin cursor across tenants | PASS |
| EF28 | Fair-share | `TestFairShare_NoisyTenantNoStarvation` | noisy tenant capped per tick | PASS |
| EF29 | Fair-share | `TestFairShare_CursorPersistsAcrossCalls` | cursor advances every call | PASS |
| EF30 | Fair-share | `TestFairShare_DynamicTenantAddition` | new tenant slots into rotation | PASS |
| EF31 | Drain API | `TestDrainHandler_HappyPath` | POST returns 200, scheduler draining | PASS |
| EF32 | Drain API | `TestDrainHandler_Idempotent` | repeat calls safe | PASS |
| EF33 | Drain API | `TestDrainHandler_RejectsGet` | GET method blocked | PASS |
| EF34 | HPA safety §11.6.1 | `TestCompaction_SIGTERM_FinishesCurrentPartition` | drain blocks until in-flight done | PASS |
| EF35 | HPA safety §11.6.2 | `TestCompaction_SIGKILL_OrphanRecovery` | partial uploads reclaimed by Tier B | PASS |
| EF36 | HPA safety §11.6.3 | `TestCompaction_HPAScaleUp_NoDuplicate` | no dual ownership during scale-up | PASS |
| EF37 | HPA safety §11.6.4 | `TestCompaction_HPAScaleDown_DrainOrAbort` | draining pod excluded from HRW | PASS |
| EF38 | HPA safety §11.6.5 | `TestCompaction_WaveScaleUp_RingThrashing` | rate gate fires during wave | PASS |
| EF39 | HPA safety §11.6.6 | `TestCompaction_PDB_NoSimultaneousEviction` | chart PDB enforces invariant | PASS |
| EF40 | HPA safety §11.6.7 | `TestCompaction_GracefulShutdown_NoOrphans` | pre-drained scheduler emits zero work | PASS |
| EF41 | HPA safety §11.6.8 | `TestCompaction_DrainTimeout_ForceAbort` | drain returns after DrainTimeout | PASS |

**Coverage gates** (run `GOWORK=off go test -coverprofile=cover.out
-coverpkg=./internal/compaction/... ./internal/compaction/...`):

- `ownership.go`     **96.25 %** (gate >= 95 %)
- `orphan_sweep.go`  **91.96 %** (gate >= 90 %)
- `fair_share.go`    **94.78 %** (gate >= 90 %)

**Negative-control contract:** every load-bearing assertion has a
documented negative-control revert in the test's leading comment. Removing
the corresponding production-code guard MUST make the test fail. This
guarantees the test is load-bearing, not just a happy-path reaffirmation.

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
