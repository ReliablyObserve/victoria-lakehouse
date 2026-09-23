# Changelog

All notable changes to Victoria Lakehouse will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.142.10] - 2026-09-23

### Security

- **`/internal/delete/*` is gated like upstream.**
  The cluster delete protocol is now served only with `-internaldelete.enable` (default
  `false`), through upstream's own code: the logs binary mounts VictoriaLogs'
  `vlselect.RequestHandler` for `/internal/delete/*`, the traces binary a verbatim copy of
  VictoriaTraces' gate that a test checks against the vendored source. With the flag on,
  `delete.enabled: true` is also required. Both binaries used to serve the protocol
  unconditionally.

  `run_task` is refused even when enabled, because lakehouse tombstones are instance-wide and
  cannot be limited to the request's `tenant_ids`; `stop_task` and `active_tasks` work. The logs
  binary's `-help` now also lists VictoriaLogs' select flags (`-search.maxQueryDuration`,
  `-select.disable`, …); the lakehouse `/select/*` path does not read them yet, so setting one
  makes lakehouse-logs refuse to start rather than ignore it.

## [0.142.9] - 2026-09-15

### Added

- **`print-default-config` and a config drift gate: the code defaults are the one source of truth.** Both binaries gain a `print-default-config` subcommand (also accepted as `-print-default-config`) that prints the whole configuration surface as JSON: all 237 config keys with their default and how a config-file value is merged, every profile as the keys it overrides, and every flag (77 `-lakehouse.*` flags on `lakehouse-logs`, 79 on `lakehouse-traces`) with the key it writes and its effect — found by probing each binary's own flag handling, not a hand-kept mapping. Golden tests pin both outputs, and every config key and section now carries a doc comment. `docs/configuration.md` gains a reference generated from the code (type, default, config-file merge rule, flags, profile overrides and description of every key), a generated flag table, every profile as its explicit override set and a "Profile flag gaps" table; the profile summary in `README.md` and `docs/getting-started.md` and the key tables on topic pages are generated too, and keys that no binary reads are marked. The `helm-config-drift` CI job now also fails when a chart value, schema default or template fallback differs from the code default without a recorded `override` line (with a reason, in `scripts/ci/helm-drift-allowlist.txt`, stale as soon as either side changes), when docs name a flag or key that does not exist, quote a wrong default or set a key the binaries ignore without saying so, when a flag usage string claims a wrong default, when a deployment config is malformed, or when a generated block is stale; every documented `lakehouse:` example must load strictly. Run it locally with `make config-drift`; regenerate with `make config-docs`. Tooling and docs; no runtime or perf change.

### Changed

- **Helm chart defaults follow the code defaults.** Two values change what chart deployments run:

- **Flag help states the real defaults.** Eleven `-lakehouse.*` usage strings claimed defaults the code does not use — among them `-lakehouse.query.file-workers` (said 8, code 64), `-lakehouse.cache.memory-mb` (256, code 512), `-lakehouse.cache.disk-max-mb` (1024, code 51200), `-lakehouse.query.max-files-per-query` (500, code unlimited) and `-lakehouse.tenant.header-account` (AccountID, code X-Scope-AccountID). `-lakehouse.compaction.enabled` and `-lakehouse.traces.jaeger-enabled` now say that `false` does not turn the feature off, and `-lakehouse.profile` says it only fills keys left at zero.

- **Configuration docs describe what the binaries actually do.** The hand-written tables in `docs/configuration.md` documented 110 flags that do not exist and contradicted each other (compaction both on and off by default; the `balanced` profile with compaction off and zstd 7); the generated reference replaces them. Across the docs: compaction is on by default in both binaries and partitions are assigned by HRW ownership (the leader-election flags, Lease RBAC and troubleshooting steps no longer exist); `insert.ack_mode` and 45 other keys are accepted but not read by either binary, so the `flush-sync` zero-loss acknowledgement described in `docs/cross-az-optimization.md`, `docs/write-path.md`, `docs/durability.md`, `docs/cost-comparison.md` and `README.md` is not in effect — use the `logstore` buffer engine for crash durability; `--lakehouse.profile` (which the chart uses) applies only the keys the loaded config leaves at zero, so for example `max-cost-savings` selected that way keeps compaction on; `startup.min_manifest_files`, `startup.serve_while_warming`, `cache.footer_max_items`, the `cache.warmup_*` keys, `pmeta.*` and `promoted_attributes` are not read from the config file, and the examples that set them now say so.

### Fixed

- **Chart deployments following the documented `--set lakehouseConfig.mode=...` no longer fail to start.** The config map already writes `mode` for each signal, so the extra value rendered a duplicate `mode` key that the binaries reject; the chart now drops a user-set `lakehouseConfig.mode`, and `docs/kubernetes-deployment.md` installs the traces release with `logs.enabled=false` / `traces.enabled=true`.

- **Documented config examples that did not load.** Per-tenant retention overrides in `README.md` and `docs/multi-tenancy.md` were written as `retention: 720h`; the type is `retention: {keep: 720h}`, so a copied example stopped the binary at startup. `docs/deletion-strategy.md`, `docs/write-path.md`, `docs/operations.md`, `docs/bloom-index.md` and `docs/architecture/field-value-catalog.md` used config keys that do not exist. `deployment/docker/lakehouse-benchmark-config.yml`, which nothing mounted and whose keys both binaries ignored (no `lakehouse:` root), is removed.

## [0.142.8] - 2026-09-15

### Changed

- **loki-vl-proxy 1.58.0 → 1.76.0 in the e2e and benchmark composes.**

  `deployment/docker/Dockerfile.loki-vl-proxy` pins the binary all four proxy services run (e2e
  hot + cold, benchmark lh + vl-lh). The releases in between are largely Loki-parity corrections
  on the paths Grafana and Drilldown drive: invalid queries answer Loki's `400 bad_data` instead
  of `502`; sliding and bare-parser range metrics return the samples Loki returns, and
  `query_range` rejects more than 11,000 points; `| json` / `| logfmt` parsed labels survive
  windowed and `-stream-response` log queries; and `/labels` and `/label/{name}/values` return
  every name and value with data in the requested range, with versioned cache keys and a marker
  on answers served stale after a backend failure.

  Two releases change behaviour, and neither is behaviour this repository depends on — checked
  before the bump rather than assumed: 1.75.0 makes a multi-tenant request fail as a whole when
  any tenant fails, as Loki does, dropping the `X-Multi-Tenant-Partial-Failures` header and the
  partial `warnings` (nothing here reads either); and 1.76.0 reports `/loki/api/v1/index/volume`
  and `/index/volume_range` in BYTES rather than line counts, with Loki's bucket stamping (no
  dashboard, alert, test or script here reads those endpoints, but anything added later that
  graphs volume must be written against bytes).

  Verified by building the image and checking the baked binary's SHA256 against the published
  release asset, and by probing all fourteen flags the composes pass: every one is still defined
  at 1.76.0.

## [0.142.7] - 2026-09-15

Cut from #220, a test-only change, so this release carries no user-facing change of its own.
A release run for #223 started while this one was still running and published 0.142.8 twelve
minutes later: the loki-vl-proxy bump is in THAT tag's tree, not this one.

## [0.142.6] - 2026-09-15

### Fixed

- **The hot/cold parity suite was measuring two broken tests and a stale allowlist.**
  With the settle probe fixed (#216) the suite runs again, and its first honest result was two
  failures that were defects in the TESTS, not cold-tier divergences.
  `TestServiceGraphParity_JoinPipeWorksOnCold` built `| NOT parent:eq_field(child)`, but LogsQL
  has no bare `NOT` pipe — upstream writes `| filter NOT <parent>:eq_field(<child>)` — so BOTH
  tiers answered 400 and the case failed on a malformed query; with `filter` restored the join
  works on cold, and the case now compares the edge sets of both tiers instead of only checking
  that cold returned well-shaped rows (20 edges, every callCount equal).
  `TestParity_TracesExtended/traces_filter_resource_region` hard-coded
  `cloud.region:="us-east-1"`, but `cmd/datagen` draws each service's region from three
  candidates with a clock-seeded rng, so with five services that value is absent from roughly
  one seed in eight — the case had been comparing two empty answers at that rate since it was
  written, passing vacuously until the empty-reference guard called it what it is; the region is
  now discovered from the seed. 27 allowlist entries are deleted because the work that fixed
  them has shipped: two B5 sub-hour `/hits` interpolation entries, eight B6
  cold-tier-ignores-the-tenant entries, and seventeen pipe/sort/limit/facet/jsonl entries,
  taking the allowlist from 60 to 33 — what remains is B2 and B3. `min-pass` rises 386 → 428 so
  none of it can quietly regress. Verified on a local parity stack across four consecutive full
  runs: 428 passed, 51 failed, 0 aborted, every failure allowlisted and every allowlist entry
  still live.
## [0.142.5] - 2026-09-15

### Changed

- **Long changelog entries are broken into paragraphs instead of one wall of prose.**

  Wrapping the file at 96 columns fixed the line length but not the shape: an entry was still
  a single paragraph of up to three thousand characters, which is no easier to read at 96
  columns than at 4,773. The 75 entries over 700 characters are now split at sentence
  boundaries into paragraphs of roughly 420 characters, with the bold lead-in alone on the
  first line — so an entry reads as title, symptom, cause, scope, verification rather than as
  one block.

  No word was changed: the split is mechanical and asserted by requiring the file's text to be
  identical once all whitespace is collapsed. `check_changelog_pr.py` learned the matching
  rule — a blank line inside a list item is a paragraph break, not the end of the bullet, so a
  restructured entry is still the same bullet and the release gate does not read it as new.
  Only a blank line followed by UNINDENTED text ends an item.

- **The changelog is wrapped, and the gates that read it now understand a wrapped entry.**

  Every entry was written on one physical line — median 465 characters, longest 4,773 — so the
  file was unreadable in an editor and a one-word correction showed up in a diff as the whole
  entry rewritten. Bodies are now wrapped at 96 columns and bold lead-ins are kept whole on
  their own line; markdown renders a soft-wrapped paragraph identically, so the published
  changelog is unchanged, and the reflow was asserted by requiring the file's text to be
  identical once all whitespace is collapsed.

  Two gates keyed bullets by physical line and had to learn otherwise: `check_changelog_pr.py`
  now treats a bullet as its `- ` marker plus the wrapped remainder, whitespace-normalised, so
  re-wrapping a released entry is not read as adding a new one; and `check_registry_touch.sh`
  now joins a lead-in that wraps before using it as a feature key — it was silently truncating
  one at the first line break, which is how a bullet already split in `main` had been keyed by a
  fragment.

## [0.142.4] - 2026-09-15

### Fixed

- **Cold query rows carry only the ingested fields — no `<null>`, tenant, spare-slot or service-graph columns; traces keep their attribute prefixes.**

  Every cold log line in Grafana came back decorated with roughly thirty fields the same line
  served hot never shows: every Parquet leaf column of the file, with unset cells rendered as
  the literal string `"<null>"` (e.g. `"telemetry.sdk.name":"<null>"`), the tenant bookkeeping
  columns `account_id` / `project_id`, and the unmapped Tier-2 spare slots `ded_s01`..`ded_s08`.
  On traces it was worse: each promoted attribute came back twice — once under VictoriaTraces'
  own name and once under its raw Parquet spelling (`service.name` next to
  `resource_attr:service.name`, `span.name` next to `name`, `timestamp_unix_nano` next to
  `_time`) — and the service-graph edge columns `parent` / `child` / `callCount` leaked onto
  plain spans.

  Root cause: the scan path's value converter (`parquetValueToInterface`) was the only one of
  four siblings without an `IsNull()` guard, so a NULL cell fell through to
  `parquet.Value.String()`; and the two scan implementations (the columnar fast path and the
  row-oriented slow path) each resolved field names on their own, neither applying the column
  classification the typed reader applies. Both paths now go through one shared naming rule —
  storage-internal columns never surface, a spare slot surfaces under the attribute name the
  file's footer KV binds it to (and not at all when unbound), and the rest resolve through the
  schema registry (which supplies VT's `resource_attr:` / `span_attr:` prefixes).

  The same check is applied to the name a MAP attribute key would be emitted under, so a key
  inside `log.attributes` / `span.attributes` cannot introduce a field name a column is
  forbidden to produce. The raw Parquet spelling of a promoted traces column is still emitted,
  but only when the query actually spells it — in a filter term or a
  `fields`/`stats`/`uniq`/`top` pipe, never for `field_names`/`field_values`/`facets` — so
  `service.name:="X"` keeps matching while a wildcard span list carries VT's field names alone.

  A new `internal/schema` column classification plus a guard test enumerating every row-struct
  column keeps a future column from leaking silently. Both modules; regression tests at the
  row-group readers and through the full query path, and new parity subtests comparing the cold
  field vocabulary against hot. Reading ~30 fewer columns per row also makes the wildcard scan
  measurably cheaper: logs 155.2 → 79.1 ms/op (-49%) and 4.50M → 2.20M allocs/op (-51%) on a
  100k-row row group; traces 143.4 → 120.2 ms/op (-16%) and 5.30M → 2.60M allocs/op (-51%)
  (`BenchmarkReadRowGroupColumnar_Wildcard100k`, medians of 5).

## [0.142.3] - 2026-09-14

### Added

- **Per-tenant read-scope parity coverage (`tests/parity/tenant_isolation_parity_test.go`).**

  A read carrying one tenant's headers must answer with that tenant's rows and no others; the
  suite had no test for it, so a cold tier answering every tenant's query with the union of all
  tenants passed unnoticed. The logs side seeds its own two-tenant corpus (disjoint
  `service.name` sets, 40 and 25 rows, written 40 h back so it sits outside every other test's
  window and cannot perturb another comparison, for two tenants that own no other data) and
  asserts exact per-tenant counts for `/select/logsql/query`, `/select/logsql/hits` and
  `/select/logsql/field_values` once the corpus has settled on the cold tier — the cold manifest
  lists both tenants and the cold answers have not changed for three manifest refresh intervals.

  Asserting any sooner measured the flush, not the scoping: while the rows are buffered the cold
  reads answer each tenant correctly, and mid-flush they answered both tenants with one tenant's
  rows, which a readiness check summing the two reads accepted; the traces side asserts the same
  against hot VictoriaTraces as the reference. Both cold tiers currently answer with the union —
  recorded as B6 in `docs/parity-and-gaps.md` and allowlisted, with the fix tracked separately.

  Exact counts, not directional checks: the difference between correct and leaking is the
  presence of another tenant's rows, which a `>=` assertion cannot see. The per-tenant tests in
  `tests/parity/tenant_scope_parity_test.go` hold to the same rule: the Jaeger dependencies
  graph is compared with hot VictoriaTraces call count by call count for each seeded tenant,
  pinned to one service-graph snapshot both tiers stamp on the same minute (a longer lookback
  sums every snapshot in it and scales each count by the number of task ticks), and a Tempo
  search is compared as the exact trace-id set per tenant; the unknown-tenant read runs on both
  tiers with a seeded-tenant control, and every reader fails on a request error instead of
  returning 0.

  Before this they could not fail — one compared each tenant's count with the sum of all
  tenants' counts, one checked that the dependencies API answered the same twice, one accepted
  any HTTP 200 over a ten-minute window — and the first is removed in favor of the exact
  `TestTenantIsolation_Traces_PerTenantParity/*/query_rows`. The traces subtests of
  `TestTenantIsolation_Traces_PerTenantParity` also fail on a hot reference of 0 or on a hot
  request error, which used to compare equal to the same outcome on cold.

### Fixed

- **The hot-vs-cold parity suite now measures parity instead of reporting a constant.**

  `tests/parity` had produced the same 18 FAIL / 19 PASS / 10 SKIP on every run since June, and
  most of it was the harness, not the cold tier. Fixed: LogsQL field names containing `:` are
  backtick-quoted everywhere (`` `resource_attr:service.name` ``, `` `span_attr:http.method` ``)
  — unquoted, VictoriaLogs parses `resource_attr` as the field and the rest as a bucket, so the
  hot side returned 0 rows and the comparison held vacuously (this closes the "Known issues"
  note in 0.121.0, and the same fix lands in `scripts/comparative-benchmark-traces.sh`, whose
  VictoriaTraces column was timing empty result sets); the three `TestParity_Traces_LogsQL`
  subtests that queried the bare `service.name` spelling now query VictoriaTraces' native field
  on both tiers, with one new subtest pinning the Lakehouse-only `service.name` alias as an
  expected difference (hot 0, cold > 0); the service-graph join test asks for the seeded window
  instead of `_time:10m`, which is empty by the time the suite runs;
  `-servicegraph.enableTask=true` plus a tick interval and lookbehind sized to the seed are set
  on BOTH `victoriatraces` and `lakehouse-traces` in the parity compose, so the two
  service-graph tests that used to burn 120 s and 420 s timing out now pass in seconds and the
  three that skipped for want of edges run (the dependencies and stats-by tests wait up to three
  minutes for the cold task's first snapshot, which lands a minute after the binary starts,
  instead of failing or skipping when the suite reaches them sooner); a second tenant is seeded
  into the traces tier for the per-tenant tests (`cmd/datagen` now accepts a traces-only seed —
  it used to insist on a logs endpoint, which would have forced the second tenant into the logs
  corpus every other test counts); and `count_same_across_time_formats` builds its nanosecond
  and second windows from one truncated `time.Now()`.

  The comparators gained a hard guard: `set_equal`, `set_superset`, `rows_match`,
  `bucket_match`, `structure_match`, `count_equal` and `count_tolerance` now FAIL when the
  reference tier returned nothing (for a count: 0, `NaN` or an empty aggregate value), because
  every one of those comparisons is vacuously true against an empty reference, and a case built
  to match nothing declares `ExpectEmpty` and must then read 0 on both tiers.

  The count guard exposed 30 count comparisons that had been passing on nothing. Eleven were
  genuine negative-path cases and are now `ExpectEmpty`; `boundary_ns` among them is pinned on
  the oldest seeded row, because its old one-second window at the edge of the data held a row in
  roughly one run in ten, and a new `boundary_ns_start_inclusive` pins the other side of the
  same nanosecond. The other nineteen queried nothing and now query seeded data: log aggregates
  and `math` pipes over a `duration` field log rows do not carry (now `severity_number`), a
  `_msg:~"\btimeout\b"` whose single backslash LogsQL unquotes into a backspace, case-sensitive
  `seq("connection", "refused")` and `_msg:Error` filters the corpus never matches, `ipv4_range`
  over whole messages (it only matches a value that is an address, so it now reads `client_ip`),
  a negated regexp every body matched through its `trace_id=` suffix, span kind 1 which the seed
  never writes, `_msg:="specific log message"` (now an exact message looked up from the
  reference tier), and `_time:1h` / `_time:5m`, which are evaluated at the request's `end` — an
  hour past the data — and now evaluate in the middle of the seeded day; three of the aggregates
  had passed as `1 == 1` because the count reader fell back to counting the one-line stats
  envelope on each side, which it no longer does.

  `narrow_1min` is anchored on a row that exists instead of a fixed minute that is empty in a
  few percent of seeds. Three of the repaired comparisons now fail on the cold tier and are
  recorded rather than hidden: `exact_msg` and `ipv4_filter` under B3 (a filter plus a pipe
  returns 0 rows — the cold column projection reads no map column, and never reads the body for
  an exact `_msg` literal containing `:`), and `boundary_ns_start_inclusive`, which exposed B7:
  cold row-group pruning compares the query's inclusive end bound exclusively, so a row sitting
  exactly on the window's last nanosecond is dropped.

  Value extraction learned `| uniq by(x)` rows (which carry the named column, not a `value`
  key), `/select/logsql/facets`, and top-level JSON arrays (`/select/tenant_ids`), and
  `structure_match` compares the top-level key set for flat responses like `query_time_range`
  rather than logging "missing data field" and passing. The blanket `t.Skip` on
  `TestParity_Traces_Jaeger` is gone; its sibling Jaeger tests pass on the same stack.

- **Parity CI is now a ratchet instead of `continue-on-error`.**

  The Parity Tests job no longer swallows its own result. `tests/parity/known_failures.txt`
  lists the seven remaining cold-tier divergences (B1–B7, documented in
  `docs/parity-and-gaps.md`) with a reason per line and a recorded minimum pass count, and
  `scripts/ci/parity_ratchet.py` parses `go test -json`, keyed by package and test, and fails
  the job when a failing test is not on the list, when a test started and never finished or the
  test binary panicked or a package failed outside any test (a `-timeout` or a crash leaves only
  a package-level `fail` behind, which the gate used to pass), when a listed test starts passing
  or skipping or disappears (stale entry must be deleted), or when the pass count drops.

  A parent test that fails only because a listed subtest failed needs no entry of its own. List
  entries separate the test path from the reason with whitespace around the `#`, so a repeated
  subtest such as `name#01` can be listed, and a second `min-pass` directive is an error rather
  than silently winning. The ratchet's own 67 unit tests
  (`scripts/ci/tests/test_parity_ratchet.py`, including event streams recorded from a timed-out,
  a panicking and an exiting test binary) run before the stack is built, so a broken gate cannot
  wave a broken suite through.

  The stack steps fail instead of carrying on: the health and datagen waits exit 1 when they run
  out of attempts (and on a datagen exit code other than 0), and the cold-tier settle step fails
  when the tiers never agree instead of falling out of its poll loop and exiting 0. Settling now
  needs a positive, equal row count for the logs corpus and for both seeded traces tenants,
  unchanged across four checks 5 s apart — an unreachable probe used to print an empty string on
  both sides, and two empty strings compared as settled, and a single agreeing check can land
  while the cold tier still answers from its local buffer.

  The suite also runs `docker compose run --no-deps`: without it, starting `parity-tests` re-ran
  the one-shot datagen services after the settle step, which doubled the corpus and had the
  suite reading a cold tier that was still flushing the second copy.

## [0.142.2] - 2026-09-14

### Changed

- **The manifest fast path costs O(files), not O(rows).**

  It emitted one formatted timestamp string per row — for a compacted 128 MiB file that is
  millions of `FormatField` calls and allocations before a single row is counted. It now emits a
  CONSTANT `_time` column: VictoriaLogs stores a constant column once
  (`blockResult.addResultColumn` → `addResultColumnConst`) and `pipeStats` answers `count()` and
  `count() by (_time:<bucket>)` from the block's row count without walking the rows, so cost per
  file is one `FormatField` call and a fixed number of allocations whatever the row count.

  A new minimal upstream patch (`patches/vl-*/vl-const-timestamps-parse.patch`, identical in
  both VL pins) makes VL's `tryParseTimestamps` parse such a column once instead of once per row
  — a pure optimisation with no semantic change. Measured on an Apple M5 Pro, Go 1.26
  (`BenchmarkManifestFastPath_Emit`, `BenchmarkManifestFastPath_CountQuery`): 124 files x 2 000
  rows 12.09 ms -> 0.83 ms to emit and 16.2 ms -> 0.93 ms end to end, 496 498 -> 738
  allocations; 4 files x 2 000 000 rows 155.3 ms -> 0.087 ms to emit and 248.0 ms -> 11.2 ms end
  to end, 8 001 326 -> 136 allocations and 226.0 MB -> 1.1 MB — while the "before" figures stop
  at 1 M rows per file and the "after" ones count every row.

  `TestStreamConstTimeBlocks_AllocationCeiling` keeps allocations independent of `RowCount`. The
  "zero S3 requests warm" property for fully-covered files is unchanged. Exactness has a
  measured price when a histogram step is finer than the files' time spans, because every
  straddling file is then read (`BenchmarkManifestFastPath_StraddlingFallback`, every response
  validated, medians of 6 runs): 8 files x 50 000 rows under `_time:5m` cost 8.4 ms / 64 S3 GETs
  / 3.13 MB warm (13.8 ms / 92 GETs / 4.49 MB cold) against 0.27 ms and zero S3 under
  `_time:1d`, and the same `_time:5m` query run without the timestamp-only hint — a plain scan —
  costs 8.2 ms / 64 GETs / 3.13 MB, so the gate adds nothing measurable beyond the read itself.

  The scan path's row-group shortcut (`syntheticTimestampBlock`) still formats one timestamp per
  row of a qualifying row group — correct and uncapped, but O(rows); it only runs after a footer
  read. Docs: `docs/performance.md`, `docs/query-performance-optimization.md`,
  `docs/performance-machinery.md`, `patches/README.md`.

### Fixed

- **`count()` over a file with more than 1 000 000 rows was under-reported.**

  The manifest fast path answers count-class queries (`* | stats count()`,
  `/select/logsql/hits`, `| stats by (_time:<step>) count()`) from manifest metadata for files
  whose whole time span sits inside the query window — zero S3 requests. It materialised one row
  per file row and stopped at `maxSyntheticRows = 1_000_000`, so a file above that contributed 1
  000 000 instead of its real `RowCount`: a 1.5 M-row file would have lost 500 000 rows from the
  answer, with no error, and after compaction into target-sized files every file is above the
  cap.

  The cap is gone; a fully-covered file now contributes exactly its `RowCount`
  (`TestStreamConstTimeBlocks_NoRowCap`, `TestManifestFastPath_ExactAboveOneMillion`). In its
  place stand safeguards that cannot truncate an answer: a manifest entry whose row count is
  implausible (above 2^40) is READ rather than served from metadata; emission stops when the
  query's own max-rows / live-bytes budget cancels the context, and the fast path now RETURNS
  that cancellation the way the scan branch does instead of handing back the short count as if
  it were complete (`TestManifestFastPath_ExactAtMaxRowsBudget`; in the traces module a max-rows
  cancellation stays a documented truncation, matching that module's own scan branch, while any
  other cancellation propagates).

  The traces module carried the same row cap at 50 000 000 with no chunking (a single 800 MB
  `[]string` allocation at the limit); it is fixed the same way.

  **This was latent, not live.** No route has set the timestamp-only hint since commit
  `7718942`: the stats endpoints lost it in `a5a6f54` (merged as #83) and `7718942` removed the
  last one, `/select/logsql/hits`, in both modules (merged as #86) — plausibly because of the
  fabricated-distribution defect below. `wrapVLTimestampOnly` has been defined-but-unregistered
  since. No deployed query took this path, so no stored or reported result was affected; what
  this release changes is that the path is now exact, which is the precondition for re-wiring
  the hint in a follow-up.

- **Time-bucketed counts no longer depend on a fabricated row distribution.**

  The fast path spread synthetic timestamps evenly across a file's `[MinTimeNs, MaxTimeNs]`, so
  `| stats by (_time:<step>) count()` over a file spanning several buckets would have reported a
  uniform split that the real rows do not have. A file (and, in the scan path, a row group) is
  now served from metadata only when every `_time` bucket the query groups by contains its WHOLE
  time span — then every row provably belongs to that one bucket and the count is exact.

  Anything else is read for real. `lakehouse_metadata_only_fallback_files_total` counts files
  skipped this way, so a histogram step finer than the files' time spans is visible instead of
  silent.

- **Metadata-only blocks would have been served to queries that need real row values.**

  Eligibility was decided by the endpoint hint alone, which would also have served a retrieval,
  a `sort by (_time)`, an ungrouped `by (_time)`, a `by (<field>)` grouping, `sum(x)` /
  `count(x)`, or a per-function `if (...)` filter with fabricated rows. A query classifier
  (`logstorage.GetQueryTimeBucketing`, added to the Lakehouse-owned `external_query.go` in
  `patches/vl-logs` and `patches/vl-traces`) now requires the first pipe to be a `stats` pipe
  grouping only by a bucketed `_time` with aggregates that read no column; every other shape
  reads the files. Each branch of the decision table has a test
  (`TestPlanMetadataOnly_DecisionTable`).

## [0.142.1] - 2026-09-14

### Changed

- **Docs: the petabyte-scale audit page is replaced by "Scale limits and roadmap" (`docs/petabyte-scale-audit.md`, same URL).**

  The old page was stale: four of its five must-fix items are fixed — per-key manifest index,
  incremental tenant summaries and tenant-scoped refresh (all v0.39.0) and the label-index
  startup reads (warmup samples 10 files, index persisted) — the fifth (`KeysUnderPrefix`) is
  fixed for its only production caller, and the should-fix binary snapshot also shipped
  (v0.39.0; streaming decode v0.49.0); one of its numbers was wrong (query fan-out defaults to
  64 workers, not 8).

  The new page lists, per component, what scales with file count, partitions × tenants and
  replicas, with the code path, the current limit, the failure mode past it and the planned
  change, plus what is not measured. Limits it documents that were not on the old page: every
  replica re-lists every key on each manifest refresh under a hard 2-minute timeout (startup: 5
  minutes) with at most 8 tenant prefixes in parallel; resident pmeta bundles are never evicted
  for live partitions and scale with partitions × tenants; bundles are persisted with an
  unconditional PUT, so two writers for one partition overwrite each other; compaction
  selection, retention, the size-stats recompute, trace-id lookup and three stats API handlers
  each walk a full copy of the manifest; compaction defaults to one partition per 5-minute tick;
  flush compares *uncompressed* bytes to the 128 MiB target; and the logs binary ignores
  `cache.footer_max_items` (footer cache fixed at 10 000 entries — traces honours it).

  `docs/operations/sizing.md` now marks its PB figures as estimates, adds the pmeta term,
  corrects the footer-cache snapshot (a key list, already implemented), the LIST scaling row and
  the query-memory default (32 × 512 MiB); README, `performance-machinery.md`,
  `metadata-and-s3-optimization.md` and `pb-scale-resources-pmeta.md` drop the over-claims and
  link the new page. Docs only; no code or perf delta.

### Fixed

- **A bucket listing that began before a compaction or a permanent delete put the deleted objects back into the manifest.**

  The manifest keeps a retired key out of the periodic refresh, and in 0.132.1 that record was
  dropped the moment the object's delete returned success — which is before the listing already
  in flight is applied. `ListObjectsV2` answers from the bucket as it was when it ran, so that
  listing still names the deleted sources, and the refresh applying it re-admitted them next to
  the merged output or the filtered replacement that already held their rows: phantom entries
  with no row count, no time bounds and no aggregates, inflating
  `lakehouse_manifest_total_files`/`_bytes`, failing every download of them
  (`lakehouse_query_file_not_found_total`) and failing compaction of that partition until the
  next refresh healed it — and on the delete path, serving back the rows a delete request had
  removed.

  The window is one manifest refresh interval (5 minutes by default) after every compaction and
  every permanent delete, which is to say the normal case: deletes usually succeed. A landed
  delete now settles the debt instead of forgetting the key — `ConfirmDeleted` clears `Reclaim`,
  so nothing retries it, and marks the record `Deleted` — and the record is dropped by the first
  *accepted* listing that began after the retirement and came back without the key, which no
  in-flight listing can contradict.

  `lakehouse_manifest_retired_delete_landed` reports that part of the set and
  `lakehouse_manifest_retired_settled_total` counts the guards each accepted listing releases —
  the drain signal, since on a node compacting faster than it refreshes the gauge never reaches
  zero even while draining perfectly; `..._retired_evicted_total{reason="cap_delete_landed"}`
  counts a guard lost to the size cap. This restores the guarantee 0.132.0 shipped as
  `Manifest.MarkSuperseded`, which 0.132.1 replaced with the retired set on the incorrect
  assumption that the retired set already covered it; the two covered opposite halves of the
  same race.

  Pinned at all three layers — the manifest API, a real `Compact` against a listing that began
  before it, and the rewrite path — for both outcomes of the delete, plus the rejected-listing
  case.

## [0.142.0] - 2026-09-14

### Changed

- **Upstream bump: VictoriaLogs v1.52.0 (logs) and VictoriaTraces v0.11.0 with VictoriaLogs v1.51.0 (traces, VictoriaTraces' own pin).**

  Both Go modules move to the newest upstream releases while keeping the deliberate two-pin
  model: the logs binary embeds the newest VictoriaLogs (`VL_VERSION_LOGS = v1.52.0`), the
  traces binary embeds the exact VictoriaLogs commit VictoriaTraces itself requires
  (`VL_COMMIT_TRACES = 6ae2da3c11f3`, i.e. v1.51.0, read off `VictoriaTraces v0.11.0`'s own
  `go.mod`) — a derived pin, never chosen, now enforced by
  `TestVLCommitTracesPinIsDerivedFromVT`.

  The old equality guard between the two vendored VictoriaLogs copies is replaced by a
  containment guard (`TestVLSurface_LogsPinSupersetOfTracesPin`): the logs pin must expose
  everything the traces pin does, so the extracted upstream inventory never under-reports the
  traces surface, and the logs-only delta (here: the `json_array_concat` pipe, new in v1.52.0)
  is reported rather than failed. The multi-level select protocol moves from `v4` to `v5` on
  both sides; the internal delete protocol is `v2` at the logs pin and `v1` at the traces pin,
  which is why the conformance rows carry `{{proto.internal_select}}` /
  `{{proto.internal_delete}}` placeholders resolved per module instead of a hard-coded version.

  Upstream inventory grows from 423 to 437 items (routes 135 → 140, pipes 48 → 50, flags 174 →
  181); the drift gate reports 0 unmapped, 0 stale and **0 pending-bump** (was 12 — every row
  previously waiting on this bump is now live), with 155 soft flag-coverage notes (was 156; the
  bump added six flags and three of them are now rowed, see the flag entry below). Patch
  handling: `vl-logs/vlstorage-dispatch.patch`, `vl-logs/vl-export-streamtags-get.patch` and
  both `vl-traces/` counterparts are regenerated against the new trees (same hunks, same
  exported symbols — only upstream context moved), and `patches/vt-traces/go-mod-replace.patch`
  is deleted in favour of a `go mod edit -replace` line in the Makefile's `deps-vt` target,
  which cannot rot on context.

  Because the two VictoriaLogs pins now legitimately differ, `scripts/ci/check_patches_equal.sh`
  (new, self-tested, wired into the `lint-logs` CI job) enforces byte-equality between
  `patches/vl-logs/` and `patches/vl-traces/` except for files declared with a reason in
  `patches/vl-traces/DIVERGENCE.md`, and fails on a stale declaration so the exception list
  empties itself once the pins converge. The daily upstream check that used to bump a version
  manifest and one Compose tag is now a sync probe (`scripts/ci/upstream_sync_probe.sh`,
  `upstream-check.yaml`) that performs the mechanical half of the next bump on a throwaway clone
  — pins, deps trees with `git apply --check` per patch, `go mod tidy`, build and vet in both
  modules, inventory regeneration and the conformance gates — and opens one pull request per
  version pair carrying the result matrix, authenticated with a repository-scoped
  `UPSTREAM_SYNC_TOKEN` rather than `GITHUB_TOKEN` (`docs/upstream-sync.md` has the whole
  procedure and the token setup). `parquet-go` moves separately, see the next entry.
- **`github.com/parquet-go/parquet-go` v0.30.1 -> v0.32.0 in both modules, with nothing changed on disk.**

  No source change was needed: the v0.32.0 `format` rework (`LogicalType`/`TimeUnit` modelled as
  sum types with a single `Value` field, `FileMetaData.KeyValueMetadata` moving from
  `[]KeyValue` to `thrift.Slice[KeyValue]`) reaches no call site here -- every
  `KeyValueMetadata` use is a `range` or a `len`, both unchanged on the named slice type, and
  the `ColumnChunk.ColumnIndex()`/`OffsetIndex()` signatures are untouched, so all 12 call sites
  compile as-is.

  The same rows written by both versions with the same writer options produce files that are
  byte-identical apart from the footer's `created_by` provenance string (2 of 237,050 bytes on a
  5,000-row logs file: `version 0.30.1(build )` -> `version 0.32.0(build )`); a field-by-field
  pyarrow footer comparison across every row group and column chunk (encodings, codec, value
  counts, dictionary-page presence, null counts, min/max statistics -- 2,084 lines) differs in
  that one line and nowhere else.

  The pyarrow + DuckDB readback gate passes unchanged. New structural readback goldens
  (`TestReadbackGolden`, both modules) capture row counts, row-group boundaries, per-column
  encodings and codecs, bloom presence, bloom hit/miss on fixed known-present and known-absent
  key samples, page-index presence, per-row-group page counts, footer key-value keys and the
  time column's min/max for three production writer shapes -- 31,234-row logs,
  200,000-distinct-value high-cardinality logs, 20,777-row traces -- and are identical on both
  versions and between the two modules.

  The three upstream writer fixes we inherit (row groups no longer overwriting each other's page
  counts, blooms correctly sized when a row group is closed by the row limit, dictionary
  fallback keeping every value) are protective rather than corrective here: all three properties
  already held at v0.30.1 through the LH writers, which build files via `Write(rows)` and size
  filters at flush time -- measured 12,512-byte blooms for a 10,000-row group at 10 bits per
  value, 1.03-1.27% false positives, no false negatives, and a 200,000-distinct-value dictionary
  column read back complete and in order.

  Four regression tests per module now pin all of it. `Writer.WriteRowGroup`'s new verbatim
  column-chunk copy is benchmarked but deliberately not adopted (see below). Fuzzing on the new
  library for 120 s per target with `-parallel=4`: `FuzzParseFooterBytes` 2,964,976 execs,
  `FuzzFooterLength` 2,053,671, `FuzzLogRowToDataBlock` 2,544,586, `FuzzTraceRowToDataBlock`
  2,201,991, `FuzzTraceRowWithMapAttributes` 1,867,508 -- no crashers, and the footer-hardening
  regressions (64 MiB declared-length cap, buffer-bound pre-check, decoder-panic recovery) pass
  unchanged.
- **Compaction keeps merging through decoded rows; the parquet-go copy path is measured and rejected for now.**

  `BenchmarkMergeInterleaved` / `BenchmarkMergeDisjoint` (`internal/compaction`, 3 files x
  100,000 rows, production writer options) compare the merge the compactor runs -- decode into
  `[]schema.LogRow`, sort, write -- against `parquet.MergeRowGroups` + `Writer.WriteRowGroup`,
  which lets v0.32.0 copy whole column chunks verbatim where it can. Interleaved inputs: 745
  ms/op -> 596 ms/op ns/op, 1,850 MB/op -> 385 MB/op B/op, 6,687,946 -> 644,798 allocs/op.

  Disjoint inputs (where a merge is an in-order concatenation and the copy path fully engages):
  671 ms/op -> 465 ms/op ns/op, 1,844 MB/op -> 159 MB/op B/op, 6,687,909 -> 155,640 allocs/op.
  The allocation win is large and real, but the copy path is not adoptable as the compactor
  stands: the merge is not a pure row union -- it drops trace-shaped rows, backfills
  `SeverityText`, re-promotes dedicated columns and Tier-2 slots, re-sorts globally, and feeds
  the decoded rows to `schema.LogRowTimeBounds`, `schema.ExtractLogLabelAggregates` and
  `schema.ExtractLogBloomValues` for the manifest time range, the label aggregates and the pmeta
  bloom.

  A verbatim copy skips exactly the decode those five consumers need, so adopting it means
  building a second metadata path out of footer statistics and giving up the healing passes.
  `TestMergePathsAgree` asserts the two paths produce the same rows, row count and time bounds,
  so the benchmark compares identical work and the option stays open. Measured on both library
  versions, v0.32.0's new copy fast paths do not help this workload: the row-group path runs 601
  -> 596 ms/op interleaved and 451 -> 465 ms/op disjoint, and its interleaved allocations rise
  from 174 MB / 167,035 to 385 MB / 644,798 per op (the disjoint case improves slightly, 167,493
  -> 155,640 allocs).

  The win in the numbers above is the row-group path itself, not the version. One constraint
  found while measuring, recorded in the benchmark: a schema read back from a file is not
  `EqualNodes` to the one derived from the Go struct tags, so the copy path must build its
  writer on the merged row group's schema or `WriteRowGroup` rejects it outright.
- **`format.FooterDecoder` (v0.32.0's zero-allocation footer decoder) is not adopted in `footer_cache.go`.**

  It decodes thrift bytes straight into a reusable `*format.FileMetaData`, but
  `ParseFooterFromBytes` has to return a `*parquet.File` -- the read path calls `Root()`,
  `RowGroups()`, `ColumnChunks()`, `ColumnIndex()`, `OffsetIndex()` and `BloomFilter()` on it --
  and parquet-go exposes no way to build a `File` from a decoded `FileMetaData`. Using the
  decoder would mean keeping `parquet.OpenFile` anyway (decoding twice) or reimplementing `File`
  locally, and the decoder's own contract (returned metadata aliases a buffer reused on the next
  `Decode`) sits badly with a footer cache that hands parsed footers to concurrent readers. The
  64 MiB declared-length cap, the buffer-bound pre-check and the decoder-panic recovery stay
  exactly as they are.
- **The embedded VictoriaLogs web UI is re-synced from v1.52.0 and the copy is now reproducible and gated.**

  `internal/ui/vmui/` holds VictoriaLogs' own vmui build output, served at `/select/vmui/`
  through `go:embed` with the Lakehouse tab injected into `index.html`. Until now the copy
  existed only inline in `Dockerfile.logs` and `Dockerfile.traces`, so a local `make build-logs`
  embedded the previous version's `index.html` with no assets behind it, and nothing noticed
  when an upstream bump rebuilt vmui. New `make sync-vmui` / `make sync-vmui-traces` targets
  copy the bundle from the logs pin and the traces pin respectively (wiping the target first, so
  a hashed asset from an older VictoriaLogs cannot survive), print the pin they synced from, and
  are prerequisites of `build-logs` / `build-traces`.

  Re-synced from v1.52.0: `index.html`'s four content-hashed asset references move (index,
  rolldown-runtime and vendor JS, index CSS); the document structure is unchanged, so the
  Lakehouse-tab injector still finds `</body>` and needed no change. Three tests in
  `internal/ui` turn the sync into a drift gate — the embedded `index.html` must be
  byte-identical to the vendored tree's, every `./assets/*` reference must exist there (and
  match byte-for-byte when the working copy is synced), and the embedded tree may not carry a
  file the vendored tree no longer ships. A missing `deps/` tree skips only outside CI; CI and
  `CONFORMANCE_REQUIRE_DEPS=1` fail instead.
- **The internal peer-protocol versions are extracted per module and recorded in the generated inventory.**

  The registry expressed `/internal/select/*` and `/internal/delete/*` protocol versions as
  `{{proto.internal_select}}` / `{{proto.internal_delete}}` placeholders, but nothing said which
  vendored tree resolves them and nothing recorded the resolved values — so a bump moving either
  version would have surfaced as a runtime "unexpected protocol version" rejection between peers
  rather than as drift. `inventory.ExtractProtocols` now reads
  `app/vlstorage/netselect/netselect.go` from BOTH VictoriaLogs checkouts and records the pair
  per module under `protocol:` in `inventory.generated.yaml`: logs pin (v1.52.0) `select: v5`,
  `delete: v2`; traces pin (v1.51.0) `select: v5`, `delete: v1`.

  Every select-side constant in a tree must agree, and so must every delete-side one — a split
  family is a hard error, because a single placeholder could no longer stand for it.
  `tests/conformance/README.md` and the vocabulary header in `rows/vl/select.yaml` state the
  per-module rule and name the tree each surface reads (the traces binary mounts VictoriaLogs'
  `internalselect` package, so it follows its own VictoriaLogs pin, not VictoriaTraces and not
  the logs pin).
- **Registry rows for the flags the bump adds, and a gate on the traces flag-dedup patches.**

  Rows added for the two linked flags the bump leaves unrowed: `vt: nativeinsert.maxRequestSize`
  (new in VictoriaTraces v0.11.0, caps the bodies of the `/insert/native` routes the same bump
  adds, and `lakehouse-traces` mounts `vtinsert.RequestHandler` wholesale so it is enforced) and
  `vt: vmalert.proxyURL` (mirrors the VictoriaLogs row; neither dispatcher registers a vmalert
  proxy route). `vl: nativeinsert.maxRequestSize` is rowed alongside its twin so the
  native-ingest admission cap is covered on both surfaces.

  Sixteen rows carried notes written against the OLD pins ("pinned is 0.9.2", "re-check after
  the version bump") and are rewritten to what is true now — the feature is live at the pinned
  version, and what remains unverified is cold-tier behaviour; no `expect` value is changed,
  since the rows are still declarative. `tests/conformance/README.md` gains the triage rule
  (which flags get a row, and why ingest-format, storage-node/TLS, local-disk and
  internal-transport knobs stay soft warnings), the per-flag table for this bump, and the two
  VictoriaTraces flags that now no-op (`search.traceMaxServiceNameList`,
  `search.traceMaxSpanNameList`, superseded by `search.maxTags`).

  `TestVTFlagDedupCoversEveryCollision` recomputes the VictoriaLogs/VictoriaTraces
  flag-collision set from the two vendored trees — deriving "linked" from `go list -deps` on the
  traces module, which is finer-grained than the inventory's package list — and requires the
  dedup list to match it exactly in both directions: an unguarded collision panics the binary at
  startup with "flag redefined" while still compiling and passing every other test, and a guard
  with nothing behind it makes VictoriaTraces silently skip its own registration. 34 collisions
  at VictoriaLogs v1.51.0 / VictoriaTraces v0.11.0, all 34 guarded — the
  `patches/vt-traces/*-flag-dedup*` patches need no regeneration.
- **Every LogsQL literal in the repository is parsed against the vendored VictoriaLogs parser.**

  VictoriaLogs 1.51.0 stopped treating a bare word after a pipe as a filter: `_time:5m | error`
  is rejected with "probably, 'filter' is missing in front of". Quoted tokens, non-word tokens
  (`!foo`, `{host="x"}`, `>5`) and `not` were rejected too in 1.51.0 and restored in 1.52.0, so
  which forms survive a pipe is now pinned by `TestLogsQLPipeGrammarContract` (nine accepted,
  three rejected forms, asserted straight against the parser) rather than by prose.

  `TestRepoLogsQLLiteralsParse` extends the registry's existing row check to every other LogsQL
  literal — Go sources and tests, benchmark and operational shell scripts, docs, dashboards,
  alerts, workflows, this changelog: 530 literals parse across 884 candidate strings. The sweep
  found exactly one casualty, `docs/architecture/metadata-and-s3-optimization.md`, which used
  `_time:30d | service.name=foo | count()` — doubly wrong, since the filter must leave the pipe
  position and `=` is not a LogsQL comparison — now `_time:30d service.name:=foo | count()`.

  Separately, `TestRows_QueriesParseWithUpstream` no longer skips every row carrying a `since`,
  only rows whose `since` is newer than the Makefile pin; that brought two previously-unparsed
  rows into the parser and caught `vl.pipe.json_array_concat.basic` using
  `json_array_concat(tags, more_tags) as all_tags`, when the pipe joins ONE array field's
  elements with a delimiter and does not merge two arrays. 145 row queries now parse against
  v1.52.0.
- **Every upstream pin outside the Makefile moved with it, and is now gated.**

  `Dockerfile.logs`, `Dockerfile.traces` and `Dockerfile.datagen` clone VictoriaLogs and
  VictoriaTraces from their own `ARG` defaults, which still named v1.50.0 / 77df0c04d532 /
  v0.9.2 — and because the patches are generated against the new trees, the image builds failed
  with `patch failed: app/vlstorage/main.go:569`, a version problem wearing a patch problem's
  error message. `Dockerfile.traces` also still applied the deleted
  `patches/vt-traces/go-mod-replace.patch` and now runs the same `go mod edit -replace` the
  Makefile does.

  Compose hot-tier images (e2e, parity, benchmark and the gp3 override) and the `auto-release`
  workflow env moved to v1.52.0 / v0.11.0, and the Makefile `docker-*` targets plus the CI and
  release image builds pass the pins explicitly as `--build-arg`. VictoriaLogs v1.51.0+ and
  VictoriaTraces v0.10.0+ publish **distroless** images — no `/bin/sh`, no `wget`, only the
  product binary — so the composes' `test: ["CMD", "wget", ...]` healthchecks could never run,
  leaving every `condition: service_healthy` dependant waiting until its job timed out;
  `deployment/docker/Dockerfile.upstream-probe` wraps the unmodified upstream image with one
  static busybox at `/probe/busybox` (entrypoint, binary and behaviour untouched, so the hot
  tier under test is still the real release) and the healthchecks probe through it.

  Gates in `tests/conformance/pins_test.go`: Dockerfile `ARG` defaults, Compose image tags and
  workflow `env` pins must all equal the Makefile pins; every patch a Dockerfile applies must
  exist; a service built from the probe image may not probe with bare `wget`; and the probe
  image's own busybox base is pinned by tag and digest, so a re-pushed tag cannot change the
  binary every healthcheck runs.
- **The versions the binaries report are the versions they embed.**

  `vlCompat` moves from `1.50.0` to `1.52.0` and `vtCompat` from `0.8.2` to `0.11.0`; both are
  served on `/lakehouse/info` and logged at startup, so a stale value silently told every client
  the node spoke an older upstream than it did. Each module now has a test comparing the
  constant to its own `go.mod` requirement. `.upstream-versions.json` held
  `v1.20.0-victorialogs` / `v1.5.0-victoriatraces`, which are not real tags at all, so the daily
  upstream-release check compared against nothing; nothing else read it, so it is removed and
  the check reads the Makefile.

  Dated measurement records are deliberately NOT rewritten — `docs/performance.md`'s Phase 4
  traces run and `docs/vl-comparison.md`'s test setup keep the versions they were measured
  against and gain a line naming the current pin.
- **Parquet write reproducibility is pinned, and a golden mismatch now names the field that moved.**

  `diffGolden` walks the marshalled readback golden and reports one line per changed leaf
  (`columns[3].encodings[0]`, `bloom_probes[1].present_hits`, `rows_per_row_group[0]`) instead
  of printing two multi-thousand-line JSON documents; `TestGoldenDiffNamesTamperedFields` proves
  that sensitivity by mutating a deep copy of a real golden — one encoding, one bloom probe
  count, one row-group boundary — and requiring each to be named.

  `TestLogsParquetWriteIsByteReproducible` writes the same rows twice in-process and requires
  identical bytes, which is the precondition for a cross-version byte comparison meaning
  anything; it passes on parquet-go v0.32.0. Traces are excluded for a Lakehouse-side reason now
  recorded rather than waved at: the `_trace_idx` footer key-value entry is serialised by
  iterating a Go map, so its byte order varies run to run within one library version (readers
  are unaffected — the index is self-describing and the read path sorts it), and
  `TestTracesParquetIsStructurallyStableButNotByteStable` asserts the property that does hold,
  that two writes describe the same file.

  `footerMetadataDiff` reports which footer fields differ between two files and is re-runnable
  against another library version through `PARQUET_FOOTER_BASELINE`, so the next bump records
  the exact field list instead of asserting from memory; at v0.30.1 -> v0.32.0 that list was
  `created_by` alone.

### Security

- **`github.com/VictoriaMetrics/VictoriaMetrics` library advisory
  [GHSA-8q3c-rjr9-xxrp](https://github.com/advisories/GHSA-8q3c-rjr9-xxrp) closed.** The library
  was held at `v1.140.1-0.20260414051809-8a20ccf21db7` in both modules because `v1.146.0`'s
  `lib/mergeset.MustOpenTable` signature change did not compile against the old vendored
  VictoriaLogs/VictoriaTraces trees. The upstream bump above resolves it through the vendored
  trees' own requirements: the logs module now builds against
  `v1.146.1-0.20260630165203-c82127b6d4d1` and the traces module against
  `v1.149.1-0.20260811205936-d4a40004ef28` (`go list -m`), both past the `v1.146.0` fix.

## [0.132.1] - 2026-09-14

### Added

- **Delete leftovers API — what an instance still owes.**

  `GET /delete/logsql/leftovers` (traces: `/delete/tracessql/leftovers`) lists, read-only, the
  keys the manifest retired while their objects await deletion (with `delete_owed` marking the
  ones this process superseded and `replaced_by` naming the file that took over), the uploads
  claimed but not published (`held` marking a replacement whose swap is not durable yet), and
  the durable records of unfinished rewrites with their state and both object keys — plus
  whether the tombstone store's write-through is armed, how many records are owed to S3, and
  whether its S3 restore is still pending.

  Every delete alert asks an operator to act on specific objects (“delete the listed leftovers”)
  and until now nothing could name them; the alert texts now point here. Instance-wide, not
  tenant-scoped (`"scope": "instance"` in the payload, like the tombstone listing beside it),
  and bounded: the lists cap at `limit` entries (default 1000, maximum 10000, `truncated` set)
  while the counts are always the full totals.
  `lakehouse_delete_tombstone_removed_markers_evicted_total`,
  `lakehouse_delete_tombstone_restore_pending`, `lakehouse_delete_rewrites_unfinished`,
  `lakehouse_delete_rewrite_deferred_total{reason}`,
  `lakehouse_delete_rewrite_key_collisions_total`, `lakehouse_manifest_retired_delete_owed`,
  `lakehouse_manifest_held_keys` and `lakehouse_manifest_key_claim_rejected_total{reason}` now
  have dashboard panels, and a test fails the build if any `lakehouse_delete_*` metric is on no
  panel and in no alert.

### Fixed

- **A permanent delete could make the rows it was supposed to KEEP disappear — or make the rows it deleted reappear — and moved another tenant's kept rows into the default tenant.**

  The background rewriter wrote a filtered replacement Parquet object and deleted the original,
  but nothing ever told the manifest. Until the next manifest refresh re-listed the bucket, the
  manifest pointed at the deleted key, which failed in two opposite directions depending on
  whether the tombstone survived: with the tombstone present, the query path's 404 recovery
  skipped the missing key and the kept rows vanished from every result; with the tombstone lost,
  the same path synthesised `RowCount` blocks from the stale entry and the deleted rows came
  back in counts and hit totals.

  The refresh then adopted the replacement with nothing but its size — no row count, time
  bounds, labels or aggregates — and wherever it did not run before `orphan_ttl` the orphan
  sweep deleted the replacement, taking the kept rows with it permanently. The replacement key
  was also built from the deployment prefix, which under the `{AccountID}/{ProjectID}/<signal>/`
  layout is the default tenant's: rewriting tenant 1002/0's file put its kept rows under
  `0/0/logs/`, served to and billed to the wrong tenant; it now stays in the source object's own
  directory.

  Every rewrite step is now recorded on the tombstone before the step that depends on it — the
  replacement key before the upload, the publish after the atomic, conditional manifest swap,
  and the superseded object's delete, retried every pass until it lands — so a restart finishes
  or undoes an interrupted rewrite whatever the manifest snapshot it comes back with, and an
  un-delete in the middle of a rewrite is refused with `409`.

  The swap is conditional on the original still being registered, as is compaction's publish, so
  a rewrite and a compaction racing on the same file can no longer both land and store its rows
  twice. The replacement is written with the compactor's writer (SBBF blooms, slot binding,
  `_trace_idx` for traces) and its row count, time bounds, sizes, label sets and label
  aggregates are recomputed from the kept rows, so manifest-answered queries (`stats count() by
  (field)`) stop counting deleted rows and the file stays as prunable for LH and external
  Parquet readers as the one it replaced.

  The rewrite is handed to pmeta and to peers the way a compaction output is, and the pmeta
  field catalog is rebuilt so a deleted value does not reappear in `field_values`. A tombstone
  retires only after every file currently overlapping its range is handled — including
  compaction outputs that carried its rows forward, which are recorded clean only for the
  tombstones the merge actually applied — and compaction no longer physically removes rows of
  `hide` tombstones or of tombstones still inside `rewrite_delay`, restoring the un-delete
  guarantee.

  The rewriter refuses to run without a manifest rather than orphan its output. Pinned by a
  crash matrix that kills the rewrite at every step and restarts from a snapshot taken at the
  crash, a snapshot older than the rewrite, or a lost disk, with a manifest refresh before
  anything else runs; deterministic rewrite-vs-compaction race tests; a property suite over
  random delete/un-delete/compaction/restart/refresh sequences with failing deletes (each
  safeguard mutation-checked to fail its test when removed); and a reusable
  `internal/testutil/storageinvariants` package.

  Read-path perf unchanged; `field_values`/`streams`/`stream_ids` fall back to a
  column-projected scan only while a tombstone overlaps the files they scan.

- **The periodic manifest refresh brought back objects the manifest had let go of, and dropped files published while it was listing.**

  The refresh rebuilds the manifest from a bucket listing every `manifest.refresh_interval`, and
  a listing cannot tell a live file from a leftover: a compaction source whose delete failed was
  adopted again next to the compacted output (every row served twice, rows a delete had removed
  back once the tombstone retired, and the object invisible to the orphan sweep for good), an
  output uploaded but not yet published was adopted next to its sources (and its publish then
  dropped as a duplicate key, losing its row counts and labels), and a file published after the
  listing started vanished until the next refresh.

  The manifest now remembers retired keys (replaced by a publish, or abandoned, with the delete
  outstanding; persisted with the snapshot, forgotten once a later listing no longer contains
  them, bounded at 7 days / 100,000 keys) and pending uploads, keeps both out of the refresh,
  keeps files registered during the listing, and the compaction scheduler retries the
  outstanding deletes every scan. New `lakehouse_manifest_retired_keys`,
  `lakehouse_manifest_refresh_skipped_total{reason}`,
  `lakehouse_manifest_retired_evicted_total{reason}` and
  `lakehouse_manifest_retired_reclaimed_total` / `..._reclaim_errors_total`. Files the manifest
  never knew — flushed by a peer, or after the snapshot a node restarted from — are still
  adopted.

- **A hide-mode delete was undone by any ungraceful restart, never applied to field pickers, buffered aggregates or compaction, and could not be un-deleted by id on the traces binary.**

  Tombstones reached local disk only from `runShutdown` and `SyncToS3` had no caller, so the
  startup `LoadFromS3` never found anything — a `SIGKILL`, OOM kill or node failure silently
  un-deleted everything an operator had hidden (the S3 keys, had they been written, would also
  have carried a doubled slash, `logs//_tombstones/`, not the documented layout). Every
  tombstone change is now written through before the API returns (disk synchronously, S3 in the
  same call with a retry queue), startup restores the union of both copies without walking back
  rewrite progress, a removal leaves a marker so a crash before its S3 delete lands cannot bring
  an un-deleted or retired tombstone back, tombstone records are updated atomically so
  concurrent rewrite and compaction bookkeeping cannot overwrite each other, and a boot-time
  self-check counts every disagreement with the manifest.

  Tombstones were also applied only on the row-fetch path: `field_values`, `streams` and
  `stream_ids` still enumerated a deleted value (they now apply every tombstone overlapping the
  files they scan, not just the query window, and skip the time-blind label index while any
  tombstone is active), `field_names` reported hit counts that included deleted rows, a `stats`
  over a window only the co-located buffer covers counted deleted buffered rows (the pure-buffer
  path is now skipped while a tombstone overlaps), and compaction copied tombstoned rows
  forward; all now honour them (`field_names` reports unknown rather than wrong counts).

  The delete API rejects a tombstone whose query does not parse or that the durable encoding
  cannot carry (invalid UTF-8) instead of accepting a delete that silently hides nothing, `GET
  /delete/tracessql/tombstone/{id}` and its `DELETE` (un-delete) no longer 404, and the
  tombstone listing reports persistence state. New metrics, a *Deletes* dashboard row and a
  `lakehouse-deletes` alert group cover the lifecycle. Both modules.

  The LogsQL filter behind a tombstone is parsed once and cached instead of once per row, which
  makes the row filter itself substantially cheaper.

- **A rewrite could delete the only copy of the rows it was keeping when the record authorising the delete never reached durable storage, and a scheduler pass before the first manifest listing could retire a tombstone over files that still held its rows.**

  Writing a rewrite's `published` record into the tombstone store only queued the durable
  copies: a disk error was logged and an S3 failure retried in the background, and the rewrite
  committed anyway — deleting the superseded object while the copy a restart reads still said
  `prepared`. The restart then undid a finished rewrite: it retired and deleted the replacement
  (the only remaining copy of the kept rows) and restored a source that no longer existed.

  The same hole reopened at boot, where a restored record is a MERGE of the disk and S3 copies
  and so is held by neither: with the merge indistinguishable from a durable record, a disk copy
  that had outrun S3 authorised a delete S3 knew nothing about. Now every step that deletes an
  object first waits for the record authorising it to be acknowledged by the target a restore
  would read (S3 when configured, else the local disk) — `prepared` before the replacement is
  uploaded, `published` before peers, pmeta and the source's delete, `discarded` before an
  abandoned replacement is deleted — a restored record counts as durable nowhere until it has
  been written back, and a rewrite whose record cannot be confirmed is deferred with everything
  left exactly as it is and the replacement HELD, so no compaction can merge it while an undo is
  still possible (`ReplaceFile`/`ReplaceFiles`/`RemoveFileIfPresent` refuse a held key,
  compaction skips them in selection).

  Separately, the rewrite scheduler read "this key is not in the manifest" as "the object is
  gone" — but it runs from the first tick, before the process has ever listed the bucket, so a
  node that restarted with a lost disk or an old snapshot marked every one of a tombstone's keys
  reaped and retired it, un-hiding every row it covered. Absence is now only meaningful once a
  bucket listing has been applied in this process, and one key at a time after that: an undone
  rewrite's source is the live copy of its rows again but its entry only returns with the next
  refresh, so it is exempt until a listing that began after the undo has been applied.

  A key the manifest still serves is put back on the tombstone's work list rather than left
  recorded as gone. Pinned by the crash matrix run twice more — once with every tombstone record
  after `prepared` rejected by S3 (every step × the three restart modes; no object may be
  deleted anywhere in that window), once with the scheduler ticking BEFORE the first refresh —
  plus live-process and restart tests for each hole.

- **A failing tombstone restore from S3 was a warning, and the store could be read back from the wrong place after a rollback.**

  A node whose startup `LIST` of `_tombstones/` failed logged one line and carried on serving:
  it enforced only the deletes its local disk happened to hold (nothing at all, on a new pod),
  and it resolved no interrupted rewrite. The restore is now retried with backoff at startup,
  counted as `lakehouse_delete_startup_inconsistencies_total{kind="s3_restore_failed"}` with
  `lakehouse_delete_tombstone_restore_pending` staying at 1 and a critical alert on it, reported
  by the boot self-check, and retried every minute in the background (both binaries) as well as
  on every rewrite pass.

  While it is pending nothing is resolved, finished or retired, so a node with an incomplete
  store cannot make an irreversible decision on it. Rolling back is documented as
  one-directional — this release reads the previous release's files, the previous release cannot
  read this one's disk envelope and drops the rewrite records from the S3 copies it can read —
  with the procedure (drain `lakehouse_delete_rewrites_unfinished` and
  `lakehouse_delete_tombstone_persist_pending` to zero first) in `docs/operations.md` and
  `docs/durability.md`, and tests that pin each direction.

- **Replacement and compaction output keys could collide with a live file, and the retired set could forget the objects it owed deletes for.**

  Both key kinds are 8 hex characters with no check: drawing one that a manifest entry already
  used would have uploaded over a live object and then deleted it as the rewrite's own leftover.
  Keys are now claimed before anything is written (rejected when the key is registered, retired
  or already claimed), redrawn on a collision and counted, and a publish onto a key the manifest
  already serves is refused rather than silently overwriting the entry.

  The retired set's size cap evicted oldest-first regardless of whether the object's delete was
  still owed — forgetting exactly the keys whose objects the next refresh would then re-adopt,
  serving their rows twice and bringing deleted rows back; it now evicts the keys nobody owes a
  delete for first (and its TTL never drops an owed one), with `reason="cap_delete_owed"`
  counted separately and alerted as critical.

- **The delete API's documented examples were rejected by the server.** `docs/operations.md` and
  `docs/deletion-strategy.md` showed `start=2025-01-01&end=2025-06-01` and
  `mode=tombstone|rewrite|auto`, while the handler parses `start`/`end` as UNIX nanoseconds and
  accepts `hide|permanent|auto` — every documented command returned `400`. The examples now
  carry nanosecond timestamps (with the `date` incantation that produces them) and the real mode
  names, the tombstone-record example matches the record that is actually stored, and a test
  replays every delete URL in the docs through the handler so a documented example cannot go
  stale again.

## [0.132.0] - 2026-09-14

### Fixed

- **Cold-tier reads are scoped to the request tenant on both binaries.**

  A select request is now answered from exactly one tenant — the tenant in its
  `AccountID`/`ProjectID` (or `X-Scope-*`) headers, or `0:0` when there are none — and a request
  on VL's internal select protocol from exactly the tenants it lists, matching upstream
  VictoriaLogs/VictoriaTraces; only a request carrying the configured global-read header or
  bearer token reads across tenants. Previously the logs binary resolved cold-tier objects for a
  time range without consulting the tenant, and on both binaries field and stream enumeration,
  pmeta catalog answers, the in-memory label index, the Jaeger handlers (which passed no
  tenant), the traces trace-by-id index lookup and the multi-pod buffer bridge returned data of
  every tenant, so unscoped, scoped and unknown-tenant requests could see other tenants' rows
  and values.

  Affected endpoints: `/select/logsql/query`, `hits`, `stats_query`, `stats_query_range`,
  `facets`, `field_names`, `field_values`, `stream_field_values`, `streams`, `stream_ids`,
  `/select/tenant_ids` (reported a fixed `0:0`), the Jaeger and Tempo trace-by-id APIs, and
  `/internal/buffer/query`. Details:
  - One object selector (`filesForTenants`) backs the row scan, the timestamp-only and `count()`
    metadata fast paths, field/stream enumeration and the pmeta catalog answers; every selected
    object key is re-checked against the tenant before it is opened, and anything else is
    dropped and counted in the new `lakehouse_tenant_scope_violations_total{site}`.
  - Objects written under a static prefix with no tenant segment (the pre-template layout)
    belong to `0:0`: `0:0` still reads them and no other tenant does.
  - The in-memory label index is not tenant-keyed, so it now answers only a request for the
    single tenant the manifest holds objects for — legacy untenanted objects count as `0:0`, so
    a default-tenant deployment part-way through adopting the prefix template keeps the fast
    path. Every other request (another tenant, an unknown tenant, `0:0` where the only tenant is
    someone else, a tenant list) uses the tenant-partitioned pmeta catalog or a scan. Asking
    only "does the manifest hold one tenant?" had let a request that owns no objects read that
    tenant's field names and values through the index, on both binaries, including through
    Jaeger `/api/services` and Tempo tags.
  - A tenant's object selection walks the partitions inside the query window (a binary search
    into the manifest's partition index) instead of every partition the tenant ever wrote, so
    its cost follows the window: a one-hour query on a year of hourly partitions costs 0.32 µs
    and 7 allocations instead of 2.3 ms and 43,807, on every `hits`, `field_values` and
    `streams` request. See
    [docs/multi-tenancy.md](docs/multi-tenancy.md#read-scoping-which-data-a-request-sees).
  - The Parquet footer cache keeps a copy of each object's metadata tail (page index + footer)
    instead of a handle over the downloaded object. The cache is bounded by entry count
    (10,000), so entries that referenced object bodies pinned gigabytes of heap after
    whole-object reads, cache warm-up or small-file enrichment — on both binaries.
  - `--lakehouse.tenant.prefix-template` is validated at startup: it must contain both
    `{AccountID}` and `{ProjectID}` and no other placeholder. A template such as `{OrgID}/` was
    accepted and then never expanded, so every tenant's objects landed under one static prefix —
    untenanted objects, which belong to `0:0` and are invisible to every other tenant.
  - A dedicated tenant bucket that cannot be listed still fails the whole manifest refresh (the
    previous manifest is kept rather than dropping that tenant's objects), but it is now
    visible: `lakehouse_manifest_tenant_bucket_list_errors_total{bucket}` counts each failure
    (the series of every registered bucket is exported at zero), the
    `LakehouseTenantBucketListFailing` alert fires after 10 minutes, a panel on the tenants
    dashboard shows it, and
    [docs/multi-tenancy.md](docs/multi-tenancy.md#enterprise-bucket-per-tenant-isolation) says
    what to check. The dedicated buckets are listed with bounded concurrency and merged in
    registered order.
  - `/internal/buffer/query` requires `account_id`+`project_id` (or `all_tenants=true` for a
    global read) plus `tenant_scope=v1`, filters rows by tenant, and echoes the tenant in
    `X-Lakehouse-Tenant-Scope`; the select side drops the answer of a peer that does not echo it
    (an older build during a rolling upgrade loses that pod's unflushed window until it flushes,
    instead of merging it unscoped).
  - The global-read credential is now actually consulted on the select path (it was only used by
    the admin endpoints), with one shared constant-time validator; a validated global read also
    covers every tenant's unflushed rows, each tenant merged against its own flush watermark so
    a row is neither counted twice nor hidden behind another tenant's newer flush. New
    `lakehouse_global_read_queries_total`.
  - Bucket-per-tenant deployments: the client pool derives the bucket from the object key, so
    scoping the object list also means a scoped request only reaches its own bucket and an
    unscoped request only the default bucket. The periodic manifest refresh also lists each
    tenant's dedicated bucket; it previously listed only the default bucket, so objects stored
    solely in a dedicated bucket dropped out of the manifest at the next refresh.
  - Traces, found by the exact per-tenant counts: a query that reads every column (a bare
    filter, `* | limit N`, or the `limit` argument) returned no rows from any Parquet object of
    128 KiB or more whose footer the same query had just prefetched, because the scan decoded
    rows through the cached footer-only handle, whose column data reads as zeros. The
    whole-object read now always decodes the downloaded bytes, as the logs binary already did;
    projected queries (`| stats`, `| fields`) were not affected.
  - Logs, found by the same counts: the field-enumeration endpoints (`field_names`,
    `field_values`, `streams`, `stream_field_values`) dropped any object whose time range fell
    inside a higher-compaction-level neighbour's before scanning. On a partition that is both
    compacted and still being written — a live tenant whose history was backfilled across the
    hour — that hid the newest flush from every enumeration, permanently, while `query` and
    `hits` kept returning its rows. Redundancy is now settled where it is known rather than
    guessed: the compactor marks the sources it merged and deleted (`Manifest.MarkSuperseded`),
    and the manifest refresh ignores them, so a `ListObjectsV2` that started before those
    deletes can no longer put their rows back next to the merged output — which is the double
    counting the read-path guess was there to prevent. The marks are dropped as soon as a
    listing stops returning the key and expire after 10 minutes. See
    [docs/manifest-system.md](docs/manifest-system.md#compaction-integration).
  - Tests: regression tests for each leaking class; an invariant matrix over every query class ×
    request shape (default, scoped, unknown, global) × tenant layout (shared-bucket prefix,
    bucket-per-tenant with a bucket-recording S3 double); property, `-race` concurrency and
    fault-injection (drifted manifest template) tests; buffer-handler fuzzing; exact per-tenant
    e2e counts on both binaries in both layouts (the e2e stack gains a dedicated-bucket tenant);
    the tenant parity test compares exact hot-vs-cold counts; 48 conformance rows. See
    [docs/multi-tenancy.md](docs/multi-tenancy.md#read-scoping-which-data-a-request-sees).

## [0.122.1] - 2026-09-14

### Fixed

- **A single cold query could hang a core or restart the pod.**

  The cold read path's filter extraction (`parseFilterFromQuery`, both the logs and traces
  binaries) had two process-affecting defects. It decided whether a query was "time-only" by
  scanning the query text for `_time:[…]` and cutting to the matching `]`; a half-open range
  (`_time:[a, b)`, and the open `(a, b)`) has no `]`, so the scan never advanced and the request
  pinned one CPU core until the process was restarted. It also obtained the row filter by
  cloning the query, which renders it to text and parses it back — for an input VictoriaLogs
  accepts but cannot print back in parseable form (for example `>'-'`, rendered as `>-`) the
  clone panics, and because the VictoriaMetrics HTTP server turns a handler panic into
  `os.Exit(1)`, one such request restarted the whole instance.

  The filter is now taken directly from the parsed query (`logstorage.QueryFilter`, a new export
  in the VictoriaLogs patch), and the time-only decision walks the parsed filter's node types
  instead of scanning text, so every time-range form VictoriaLogs accepts is recognized and no
  query text is re-parsed. A side effect of the old re-parse — a relative range (`_time:5m`)
  being re-anchored to the moment the filter was parsed rather than the query's own timestamp —
  is fixed by the same change.

  Covered by a table test over every range shape run under a deadline, a fuzz target that
  asserts termination and that a "match all" verdict never rejects a row inside the range, and
  cold-read tests in both modules that check exact boundary inclusion and that every read entry
  point answers the un-printable query without panicking.
- **Releases no longer race or over-bump.**

  Two release runs that started together read the same latest tag and published v0.121.1 and
  v0.122.0 from one base; `auto-release.yaml` now serializes runs (`concurrency: auto-release`,
  queued not cancelled). An explicit `bugfix`/`performance`/`release:patch` label now wins over
  the size rule, which previously promoted any labeled patch PR over 5,000 changed lines or 50
  files to a +10 minor. Merges that change only `CONTRIBUTING.md`, `SECURITY.md` or
  `CODE_OF_CONDUCT.md` no longer cut a release.

  The changelog gate accepts a release-metadata PR that adds a version section out of order. The
  bump logic is now tested by running the workflow's own script
  (`scripts/ci/tests/test_auto_release_bump.py`).

## [0.122.0] - 2026-09-14

### Added

- **Lakehouse feature catalog — every feature linked to its rows, tests and docs, with generated highlights.**

  `tests/conformance/registry/features/` declares every Lakehouse capability (138 entries across
  13 areas) with its status, the changelog bullets that announced it, the conformance rows and
  regression tests that verify it, the benchmark scenarios that measure it, and the documents
  that describe it. The release a feature shipped in is read from `CHANGELOG.md` when the
  documents are generated, never recorded in the catalog: the release workflow moves the
  `[Unreleased]` bullets under a new version heading after every release without touching the
  catalog, so a recorded "unreleased" would go stale.

  An entry still unreleased is rendered as "the release after v*N*", which stays true after that
  move, and `confgen -check` accepts it as well as the exact version, so the release PR stays
  green without regenerating and the next regeneration names the version (replayed end to end
  against a clone: a merged unreleased feature, the release workflow's own changelog step, a
  second release on top). The loader (`registry/features.go`) decodes strictly, rejects unknown
  keys and duplicate ids, and checks that every referenced test, test function and documentation
  anchor actually exists on disk — a link that has rotted fails the build instead of quietly
  claiming verification the repo does not have — and that every `tests:` entry is a test: a Go
  test file, a test script, or a script under `tests/` or `scripts/**/tests/`, never the CI
  workflow, checker or implementation it covers (a feature's notes say when a linked test runs
  in no CI job).

  The gate (`tests/conformance/features.go`, also run by `confgen -check`) fails a PR when a
  Lakehouse registry row belongs to no feature, when a shipped feature cites neither a row nor a
  test, when a feature cites a row that does not exist, when an in-progress or planned feature
  cites a row that already executes, when a `### Added` changelog bullet maps to no feature —
  matched on the bullet's exact bold lead-in, so a release note can never describe something the
  catalog does not know about (88 bold lead-ins across 22 versions, all mapped) — when a feature
  claims a lead-in no bullet carries, or when a feature's optional `since:`/`changelog:`
  contradicts the changelog.

  `docs/features.md` is generated from the catalog with a per-area summary table and a
  coverage-gap list (✅ means a regression test is linked — the catalog checks that it exists,
  not that CI runs it), and README's "Key Features" bullets are now generated between `<!--
  features:begin -->` / `<!-- features:end -->` markers from the same source, so what ships and
  what is documented cannot drift apart. `scripts/ci/check_registry_touch.sh` classifies a PR as
  a feature PR when, compared with its merge base, it adds a lead-in to a changelog `### Added`
  section, adds or removes a route, handler or `lakehouse.*` flag registration in non-test Go,
  adds a YAML config key under `internal/config/`, or adds a Lakehouse registry row, and fails
  it unless the catalog was updated and the generated documents are current.

  Lead-ins, registrations and config keys are compared as sets rather than grepped from the
  diff, so a bold `### Fixed` or `### Changed` bullet, the release workflow moving
  `[Unreleased]` under a version heading, and a registration or config field that only moved
  within its file are not feature signals (a moved registration no longer counts as a route
  change for the registry-row rule either); replayed against the 62 release-metadata commits in
  this repository's history, the checker passes all of them (the diff-grep version failed two).

  The checker now has its own test suite (`scripts/ci/tests/test_check_registry_touch.sh`, 26
  cases against synthetic repositories and two against a clone of this repository). Also adds
  structural tests for the shipped Grafana dashboards and Prometheus alert rules, which
  previously had no regression coverage at all, and tests for the Parquet readback gate's
  generator, which read the generated files back and recompute the truth pyarrow and DuckDB are
  held to (run first in the `parquet-readback` CI job). Test/tooling only — no runtime code
  paths changed, no perf delta.

## [0.121.1] - 2026-09-14

Published from the commit after v0.122.0 by a release run that started at the same time as the
v0.122.0 run, so it carries a lower version number while containing everything in 0.122.0 plus
the change below. Release runs are now serialized, so this cannot recur.

### Changed

- **Contributing: main rules and the feature-PR process.** `CONTRIBUTING.md` now states the four
  non-negotiable rules (full VictoriaLogs/VictoriaTraces compatibility with upstream-first
  reuse; verified-not-assumed through the conformance machine; performance measured with
  validated three-way benchmarks; fastest Lakehouse storage on fully open Parquet) and the
  process for adding or extending a feature (feature catalog entry, registry rows and regression
  tests, regenerated docs, CHANGELOG bullet) enforced by the required `conformance-inventory`
  check; the PR template carries the matching checklist.

## [0.121.0] - 2026-09-13

### Changed

- **Performance baseline recorded before the upstream upgrade.**
  `bench-results/baseline-2026-09/` holds the consolidated LH vs VL/VT vs ClickHouse run (both
  signals, S3 latency 0 and 100 ms); `docs/benchmarks/full-scope-s3.md` carries the tables.
  Every later performance-affecting PR is compared against it. The full-scope S3-ops benchmark
  (e2e compose) has not run yet — its host port is held by another project's stack — and is
  tracked as pending in the docs, not fabricated. No code changes; no perf delta.
- **Benchmark harness now validates every timed response before counting its latency, including membership-validated truncated scans.**

  Previously the harness timed every request but fetched the comparable result (row/scalar
  count) only once, outside the loop — a system answering fast with an error, an empty result,
  or the wrong rows still earned a latency sample (this was exactly the VT-empty-cells failure
  mode found while recording the baseline above). `scripts/bench/run.sh` now captures and
  validates every iteration's response — HTTP status (curl no longer suppresses real 4xx/5xx
  into "000"), parseability (a body with even one unparseable line is now rejected, not just a
  body where every line fails), non-empty (unless the query is a documented miss scenario), and
  identical to the cell's first valid result (no flapping) — and drops invalid iterations from
  p50/p90/p95/p99 instead of counting them; a `scan`'s content is compared by a sha256 hash of
  its sorted row-key set (`_msg` for logs, `trace_id:span_id` for traces), not just row count; a
  group-by result (`count_by_service`, `high_card`) is hashed by its sorted (group, count) pairs
  (plus their sum, carried alongside for an independently checkable claim), not reduced to
  `sum(n)`, so a same-total-different-groups divergence is caught.

  VictoriaLogs documents that `limit N` without `sort` returns rows in arbitrary order once more
  than N rows match, so identity can never hold for a truncated scan — validity there is
  membership (every returned row is a member of a per-cell reference window, one untimed
  unlimited request per system) plus cardinality (exactly `limit` rows), not identity;
  ClickHouse's `scan` and group-by kinds are now requested as `FORMAT JSONEachRow` (columns
  aliased to `_msg`/`trace_id`/`span_id`/`n`), so ClickHouse goes through the exact same
  extractor as VL/VT/LH instead of a separate TSV path, and its `scan` gets a real comparable
  key instead of being validated by row count alone.

  Every request in one (signal, query, range, latency) cell shares ONE set of nanosecond window
  bounds, from a single `time.time()` call, used by every system INCLUDING ClickHouse (whose SQL
  now compares against `fromUnixTimestamp64Nano(...)`, matching LogsQL's inclusive-both-ends
  nanosecond window instead of a coarser, exclusive-end, second-granularity one) — with bounds
  shared and the seed a static one-time backfill (no live ingest), `report.py` requires EXACT
  equality between baseline and each engine, not a 5% tolerance (which stays in the pre-flight
  `parity_gate` as a sanity check on the sweep's starting conditions).

  The perf gate compares LH's own p90+p50 against a run, not raw p95 (literally the single max
  sample at `--iterations 20`, one outlier away from a false "regression") or the LH/baseline
  ratio; `report.py` only counts a ClickHouse speedup when the paired LH cell is itself valid,
  and the per-cell log line always shows `valid=<k>/<N>` next to `p95=`. Re-baselined both
  signals as `run-baseline-v3.3.{json,md,log}`: 64/64 cells valid on baseline, Lakehouse, AND
  ClickHouse (0 invalid on any system) — the one cell an earlier pass in this same round had
  flagged as a ClickHouse-specific finding (`logs/high_card/24h/lat100ms`) converges exactly
  once ClickHouse's window bounds are genuinely the same nanosecond, inclusive-both-ends bounds
  LogsQL uses; that finding is retracted, it was this harness's own bug.

  See `docs/benchmarks/full-scope-s3.md` and `bench-results/baseline-2026-09/README.md` for the
  current numbers and validity. New/extended tests:
  `scripts/bench/tests/test_report_validity.py`, `extract_result_test.sh`,
  `scan_membership_test.sh`, `measure_query_test.sh` (124 checks total).

### Fixed

- **Benchmark harness fixes found while recording the 2026-09 baseline.** The harness queried
  VictoriaTraces with Lakehouse-only field names (`service.name`, `span.name`, `duration_ns`; VT
  stores `resource_attr:service.name`, `name`, `duration`), so 12 of 28 traces cells were
  silently empty; the parity gate only printed and never failed; the sample trace id for point
  lookups was outside the measured windows; the ClickHouse `scan` result parser read a duration
  instead of a row count; MinIO images were unpinned `latest` tags no longer pullable from
  Docker Hub (now pinned to quay.io).

### Known issues

- Several parity tests and `scripts/comparative-benchmark-traces.sh` still use the unquoted
  `resource_attr:service.name` form, which VictoriaTraces' parser rejects or mismatches, so
  those checks silently skip; tracked for the conformance registry work.

## [0.111.0] - 2026-09-12

### Added

- **Conformance registry + upstream inventory (verification machine, first slice).**

  `tests/conformance/registry/` declares one row per endpoint and feature — native
  VictoriaLogs/VictoriaTraces surfaces first (135 routes, 48 pipes, 33 filters, 24 stats
  functions, 9 TraceQL metric functions — each tracked per surface, vl or vt, since the two
  binaries register some of the same route/flag names independently and are never deduped across
  surfaces — and the flags that matter for compatibility out of 174 upstream flags across both
  binaries, 20 of which aren't even wired into Lakehouse), then Lakehouse additions (`lh-shim`,
  `lh-addition`) — 423 upstream items, of which 267 are covered by registry rows (all 249
  non-flag items, plus 18 of 174 flags), each carrying an expected status (`pass`, documented
  `differ`, `absent`, `unsupported`).

  `tests/conformance/inventory/` extracts the real upstream surface directly from the vendored
  VictoriaLogs/VictoriaTraces sources, and every LogsQL row is validated by round-tripping it
  through VictoriaLogs' own `ParseQuery`. The drift check (`tests/conformance/drift.go`) is a
  hard gate on unmapped upstream items and on rows that cite something that no longer exists
  upstream — 0 of each on the committed inventory — with a soft warning for 12
  pending-upstream-bump rows and 156 flag-coverage notes.

  `UPSTREAM_COVERAGE.md` is now generated (`make conformance-gen`, byte-stable across reruns)
  with a reuse-first preamble and a status legend, replacing the old hand-written file. A new
  `Conformance` CI job requires `make conformance-check` (drift + generated-files-current) to be
  green on every PR, and separately fails a PR that changes routes/handlers/patches without a
  matching registry update. Every row is `pending: true` — a declared expectation, not yet
  executed; the runner that exercises them against hot and cold storage is future work. No perf
  delta expected (test/tooling only).

## [0.101.3] - 2026-09-12

### Fixed

- **`FuzzParseFooterBytes` nightly hang (`internal/storage/parquets3`).**

  The nightly fuzz job for `ParseFooterFromBytes` hung/died on every `main` run since at least
  2026-09-08: a footer whose declared length outran the actual buffer made `parquet-go`'s
  `OpenFile` issue one read for the whole (attacker-controlled, up to ~4 GiB) declared length,
  and our `footerReaderAt.ReadAt` zero-filled the resulting gap byte-by-byte — a multi-second,
  multi-GiB-touching loop once `fileSize` is large, not a bare allocation.

  `ParseFooterFromBytes` now rejects a declared length that exceeds the buffer it actually holds
  (the real guard) or a 64 MiB policy cap (sized for the largest legitimate trace-index footers,
  logged/counted on hit via `lakehouse_footer_parse_rejected_total`) before ever calling into
  `parquet-go`, and recovers a separate `parquet-go` panic (negative
  `SchemaElement.NumChildren`) the same fuzz run surfaced. Fixed in both the logs and traces
  modules; regression seeds and unit tests pin both bugs, and `FuzzParseFooterBytes` runs clean
  for 120s post-fix (~6.4M execs, exec/sec never below ~10K/s).
- **Retrieval queries that filter on a dedicated column no longer return blank rows (empty `_msg`, missing fields).**

  The manifest count-pushdown fast path (`countByPushdownField`) fired for ANY
  single-field-filtered query and served synthetic `{field,_time}` rows from the per-partition
  label aggregate — sound for an aggregation that reduces to that field (`| stats count()`, `|
  uniq`, `| top`, `| fields`), but WRONG for a full-row retrieval (`service.name:X`,
  `deployment.environment:Y`, `… | limit N`), which needs every column. The result was rows with
  only the filtered dedicated column populated and an empty body — e.g.
  `{"_msg":"","deployment.environment":"production"}` in Drilldown/Explore and via the
  loki-vl-proxy.

  The path was latent until the dedicated-columns work gave promoted columns their own label
  aggregates (which the synthetic path serves from). The fast path now engages only when the
  query carries a column-selecting pipe; retrievals fall through to the real scan and return
  full rows. Both logs and traces modules; regression cases added.
- **CI: fuzz jobs no longer fail on the fuzz-engine deadline race.**

  `fuzz-logs` / `fuzz-traces` (`fuzz-stress-memleak.yaml`) intermittently failed individual
  targets with `--- FAIL: FuzzX (30.02s) context deadline exceeded` and no crasher. Root cause:
  the stock Go fuzz coordinator has a narrow teardown race at the exact `-fuzztime` deadline —
  an in-flight worker RPC read can return `context deadline exceeded` instead of a clean stop;
  reproduced locally on `internal/schema:FuzzResolveToParquet` and
  `lakehouse-traces/internal/storage/parquets3:FuzzTraceRowWithMapAttributes` (~1-in-4 runs at
  default worker fan-out).

  The nightly budget (`-fuzztime=600s`) additionally sat exactly on `go test`'s default
  `-timeout` (10m), leaving no margin at all. Both fuzz steps now pass an explicit `-timeout`
  (3× fuzztime + 60s margin) and `-parallel=4`, and retry up to once on a crasher-less `context
  deadline exceeded` failure only — any run that actually writes a new corpus/crasher file under
  `testdata/fuzz/<Fn>/` fails immediately, no retry.
- **CI: E2E, parity and nightly load-test stacks pull MinIO from quay.io with pinned releases.**
  Docker Hub now denies anonymous pulls of `minio/minio` and `minio/mc` (`pull access denied`),
  which failed every `E2E Tests` and `Parity Tests` run on `main` at stack start. All compose
  files, the nightly load-test service container and the docs now use
  `quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z` /
  `quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z`.

## [0.101.2] - 2026-09-12

### Security

- **`google.golang.org/grpc` bumped `v1.80.0` → `v1.83.2`**

  in both `go.mod` (direct) and `lakehouse-traces/go.mod` (indirect), fixing four open
  Dependabot advisories:
  [GHSA-hrxh-6v49-42gf](https://github.com/advisories/GHSA-hrxh-6v49-42gf),
  [GHSA-2v4p-qf9q-27wj](https://github.com/advisories/GHSA-2v4p-qf9q-27wj) (needs `1.83.2` — its
  second affected range is `>=1.83.0, <1.83.2`, so `1.83.1` alone left it open),
  [GHSA-vp52-pcj8-j9qc](https://github.com/advisories/GHSA-vp52-pcj8-j9qc),
  [GHSA-qc2q-p7wx-3px3](https://github.com/advisories/GHSA-qc2q-p7wx-3px3).

  The `github.com/VictoriaMetrics/VictoriaMetrics` lib advisory
  [GHSA-8q3c-rjr9-xxrp](https://github.com/advisories/GHSA-8q3c-rjr9-xxrp) (fixed upstream in
  `v1.146.0`) is **not** bumped here — `v1.146.0`'s `lib/mergeset.MustOpenTable` gained a
  `flushCallbackInterval time.Duration` parameter and does not compile against the vendored
  VictoriaLogs v1.50.0 / VictoriaTraces v0.9.2 trees (both modules pin
  `v1.140.1-0.20260414051809-8a20ccf21db7`); that fix lands with the planned upstream VL/VT
  version bump instead.
- Go toolchain `1.26.4` → `1.26.8` (clears the Go-stdlib advisories `govulncheck` flagged:
  net/url, html/template, crypto/tls, net/http, encoding/xml, encoding/asn1, x/net/idna).
- `github.com/klauspost/compress` bumped `v1.18.6` → `v1.18.7` in both modules, closing the
  import-only `GO-2026-5841` (OOB read in `compress/s2`).
- `govulncheck ./...` reports **0 findings** in both modules after these three bumps.

## [0.101.1] - 2026-06-12

### Added

- **Compaction re-promotes map-stored attributes into dedicated columns (heals old files forward).**

  Files written before a key was promoted kept that attribute in the `attributes` map —
  promotion ran only at ingest (`mapFieldToRow`), and neither the writer nor the compactor
  re-applied it, so pre-promotion data never gained the dedicated-column compression (the −9.5%
  / −8.0% win missed the backlog) and its dedicated-column cardinality read `0`. `mergeLogFiles`
  / `mergeTraceFiles` now re-derive dedicated columns on every compaction pass via
  `vlstorage.Repromote{Log,Trace}Row`, re-routing each map entry through the **same ingest
  mappers** (Tier-1 OTel + Tier-2 custom slots, with and without bloom) — so old files heal
  forward as they roll up to higher levels.

  Traces promotion lives in the traces module (out of the root compactor's import reach) and is
  injected via `compaction.SetTraceRepromote`; a no-op in the logs binary and for
  already-promoted v2 rows. Guarded by unit + Tier-2-slot + empty-key + idempotency tests and
  `FuzzRepromote{Log,Trace}Row` (no panic, output keys ⊆ input, second pass is a no-op — 1M+
  execs clean).

## [0.101.0] - 2026-06-12

### Added

- **Compaction hints + manual recompaction trigger.**

  `GET /lakehouse/api/v1/stats/compaction` returns a manifest-derived efficiency picture —
  per-level file/byte counts with each level's compression ratio and the configured zstd,
  compacted-vs-pending bytes, stale-schema footprint, fragmented-partition count, and a
  **prioritized work-list of recompaction candidates** (each with estimated savings,
  before/after bytes, and the next output level + its zstd). A partition is flagged when it is
  `stale_schema` (older fingerprint, predates the current dedicated-column layout) and/or
  `fragmented` (≥2 files stuck at the top level the level policy never re-picks).

  The scheduler now **consumes these hints automatically** — recompacting stale/fragmented
  partitions it owns even when the L0/L1 level policy would not. `POST
  /lakehouse/compaction/recompact {partition, level?}` forces recompaction of one partition on
  demand through the same merge path, **HRW-ownership gated** (403 if the instance isn't the
  owner) so two pods never both rewrite a partition. Manifest-only (no file reads); full unit +
  e2e (real Parquet through the real compactor) + regression coverage. See `docs/operations.md`
  → Compaction.

- **Compaction retains the combined bloom of all merged parquets + exposes bloom footprint in stats.**

  Previously a compacted file got no pmeta partition-bloom entry, so a `trace_id` lookup could
  no longer file-skip it. The compactor now extracts the bloom-column values (trace_id +
  service.name) from the merged rows — the **union across every input** — and feeds that
  combined bloom into the pmeta bloom facet keyed by the output, so compacted files stay
  file-level bloom-prunable across everything they absorbed (no extra S3 read; the rows are
  already in hand).

  `/stats/compaction` also surfaces the **footer-bloom footprint** — `total_bloom_bytes` +
  per-level `bloom_bytes` (captured at write time, file-read-free) — plus `bloom_columns` and
  `bloom_fp_rate`. The shared `schema.Extract{Log,Trace}BloomValues` keeps the flush and
  compaction bloom sets identical.

## [0.100.1] - 2026-06-12

### Fixed

- **Exact-match queries on bloom columns no longer return 0 under a reduced projection (silent cold-tier data loss).**

  A query that pairs an equality filter on a bloomed column (`trace_id`, `span_id`,
  `service.name`, and the high-cardinality dedicated columns) with a column-selecting pipe —
  `trace_id:=X | stats count()`, `… | fields trace_id`, or any aggregation — reduces the column
  projection, which routes the file open through the projected **range-read** path (only the
  projected column-chunk byte ranges are fetched). The per-row-group footer-bloom skip
  (`bloomFilterSkip`) then called `ColumnChunk.BloomFilter()`, whose read is **lazy**: it pulled
  the bloom from byte offsets that were never fetched and got back a non-empty but all-zero
  filter whose `Check()` false-negatives every value — so **every in-range row group was wrongly
  skipped and the query returned 0**, while the full-projection retrieval of the same trace (the
  Jaeger/Tempo span-fetch path, which full-downloads the file) returned the rows correctly.

  The footer-bloom row-group skip is now gated on bloom residency (it runs only when the whole
  file body is resident); the file-level pmeta bloom already prunes files, so the per-row-group
  skip is a pure optimization and disabling it on the range-read path costs only extra in-range
  row-group reads, never correctness. The latency-critical full-projection trace-retrieval path
  is unaffected. Fixed in both the logs and traces modules; guarded by a planned-range-read
  regression test that reproduced the 0-vs-N gap end-to-end.

## [0.100.0] - 2026-06-12

### Added

- **Accurate Cardinality Explorer backed by durable pmeta state.** The Stats Cardinality
  Explorer now reads distinct counts from the pmeta catalog facet / persisted high-card HLL
  (write-fed, compaction-safe) instead of transient in-memory taps, so reported cardinality is
  accurate and survives restart and compaction. `has_bloom` is reported from the written schema
  bloom set.
- **vmui Lakehouse tab and sub-tab persistence.** The active Lakehouse Explorer tab is persisted
  in the URL hash (survives refresh, bookmarkable) and the selected sub-tab (Cardinality, etc.)
  is remembered across reload.
- **Searchable, extensible Storage Breakdown** with richer defaults.
- **Promoted id columns get real, durable cardinality (`container.id`, `service.instance.id`).**
  They're sketched by default (HLL) through the persisted per-partition catalog — the same
  durable path as `trace_id`/`span_id` — so the Cardinality Explorer reports an accurate
  distinct count that survives restart instead of `0`. Unioned into the effective sketch set in
  code (`schema.DefaultSketchIDColumns`) so an operator's `always_sketch_fields` YAML can't
  accidentally drop them; wired for both logs and traces.
- **Cardinality Explorer distinguishes "not indexed" from zero.** A field outside the tracked
  set (a map-only attribute that's never sketched) renders `—` instead of a misleading `0`, so
  "not counted" is no longer conflated with "zero distinct values". The `/cardinality/fields`
  API exposes an `indexed` flag per field.
- **Storage Breakdown selection is remembered and fully editable.** The chosen label set
  persists in `localStorage`, seeds from the server defaults on first visit, and every block —
  default or added — is removable; the `+` picker offers any field and flags non-indexed ones.
- **Per-field storage/metadata size stats — foundation.** The Parquet writer records per-column
  compressed bytes into `FileInfo.ColumnBytes` (both modules) and the manifest gains a
  `SetChangeObserver` add/remove hook. These back a cluster-wide, S3-cached, compaction-aware
  stats aggregate (per-field storage + metadata sizes; memory/disk/S3 tiers) under construction
  — design in
  [`docs/architecture/stats-aggregate-cache.md`](docs/architecture/stats-aggregate-cache.md).
- **Enriched Storage Overview tiles** — Avg File Size, Avg Rows/File, Saved (raw−comp), Storage
  Classes count, Fleet Nodes, Bucket, Registry generation — and a per-key value-count hint in
  the Storage Breakdown picker so usable break-down keys are obvious.
- **Per-field on-S3 storage size in the Cardinality Explorer (Phase A).**

  A new `StatsAggregate` cache materialises per-field/per-tenant storage bytes from
  `FileInfo.ColumnBytes`, maintained by manifest add/remove diffs (flush + compaction), seeded
  from the manifest on warm-load and reconciled on each refresh — so the stats API reads sizes
  in O(1) instead of rescanning the manifest. `/cardinality/fields` now returns `storage_bytes`
  per field, shown as a human-readable **Storage** column (dedicated columns show real bytes;
  map attributes show `—`).

  Wired for **both logs and traces** — the traces writer captures per-column footer bytes and
  the traces binary seeds/reconciles its own `StatsAggregate`, so `:20428`'s Cardinality
  Explorer populates the Storage column too.

- **Storage Overview metadata footprint (Phase B).** A new **Metadata footprint** section shows
  where metadata lives: pmeta RAM (this node), disk cache (this node), and the cluster-wide
  on-S3 metadata total. `/stats/overview` exposes `meta_resident_bytes` / `meta_disk_bytes` /
  `meta_s3_bytes`. The on-S3 figure is tracked **incrementally — never by scanning S3**: each
  pmeta bundle records its encoded byte size on persist / warm-load / compaction
  (`Bundle.PersistedSize` → `Store.PersistedBytes`), so the footprint is a live sum that needs
  no `ListObjects` sweep. Both modules.

- **Tenant-isolated pmeta metadata + exact per-tenant size (Phase E).**

  pmeta bundles are now keyed by a **tenant-scoped partition**
  (`<account>/<project>/<signal>/dt=/hour=`) instead of a global `dt=/hour=`, so each tenant's
  catalog/bloom/file-meta is physically separate under its own S3 prefix — metadata isolation
  that mirrors the data path. `/tenants` exposes **exact** per-tenant `metadata_bytes` (summed
  from that tenant's own bundles via `Store.PersistedBytesByTenant`, tracked incrementally — no
  scan), surfaced as a new **Metadata** column in the Tenants view.

  The flush, compaction, warm-from-manifest, warm-from-S3, cold-start enrichment, bloom reads
  and retention paths all derive the pmeta partition from the file key
  (`manifest.ExtractTenantPartition`), while the manifest's partition count + retention keep the
  pure `dt=/hour=` key. Existing global bundles self-heal-rebuild into the tenant-isolated
  layout on first warm, and a one-time, marker-gated startup migration
  (`CleanupLegacyGlobalBundles`, manifest-driven — no S3 scan) deletes the orphaned global
  `dt=/hour=` bundles. Both logs and traces.

- **Storage Details → per-field storage + metadata table (Phase C).** The Storage Details tab is
  now a sortable per-field table — **Field · Storage · Metadata · Cardinality · Bloom** — backed
  by a new `/stats/fields` endpoint. Per-field metadata is **exact**: the bloom + field-catalog
  facets expose per-field bytes (`BytesByField` — per-column bloom bitset + per-field
  value-set/HLL), summed cluster-wide by `Store.MetadataBytesByField` (incremental, no scan).
  Storage reuses the Cardinality Explorer's covered→total scaled bytes so magnitudes match. The
  tenant-breakdown facet is dropped from this view (tenants have their own tab). Both logs and
  traces.

- **Per-instance fleet metadata breakdown (Phase D).** Each node gossips its metadata footprint
  (pmeta RAM + disk cache) through the existing tenant-stats CRDT via a new **orthogonal**
  per-node `NodeMeta` channel (LWW by per-node generation, preserved in the S3 registry
  snapshot) — the tenant CRDT's convergence guarantees are untouched (the
  idempotent/commutative/associative/monotonic guard suite passes unchanged). `/stats/instances`
  returns the fleet's per-node footprint; the Storage Overview gains a **Fleet instances** table
  (Node · pmeta RAM · Disk cache · S3 metadata), self-row marked. Both logs and traces.

- **Storage Overview surfaces retention.** The overview shows the configured/applied retention —
  default keep-duration + override-rule count, or `off` — next to the data range (which moved
  from a tile into the info row). `/stats/overview` now exposes `retention_enabled` /
  `retention_default` / `retention_rules` from the global `RetentionConfig`.

- **Tenant aliases are config-mapped, reconstructed, and persisted.**

  Tenant `org-id ↔ account:project` aliases can be declared statically via
  `-lakehouse.tenant.alias=orgid:account:project[,...]` — a config baseline re-applied on every
  startup, so friendly tenant names reconstruct deterministically even if the S3 snapshot is
  lost. Runtime auto-registered (`X-Scope-OrgID`) aliases are now persisted by a periodic loop
  (`startTenantAliasPersist`, on the alias-sync interval) that writes the full resolver alias
  set to the S3 sidecar (`_meta/tenant-aliases.json`) and reloads it on startup — so
  auto-registered tenant names survive restarts/redeploys instead of vanishing. Wired for both
  logs and traces.

### Changed (UI internals)

- **The Lakehouse UI is one shared module.** `internal/ui/static/lakehouse-ui.js`
  (`LakehouseUI.mount`) is the single render core, used by BOTH the standalone `/lakehouse/ui/`
  page and the VMUI-embedded Lakehouse tab (`vmui-tab.js` is now only the VMUI integration;
  `index.html` is a thin host that defines VMUI's theme variables). This eliminates the two
  duplicate UI implementations that had drifted, so a UI change is made once.

### Changed

- **Bloom-value extraction is now schema-driven.** Value extraction covers every `HasBloom`
  column from the per-signal schema set (logs and traces) rather than a hardcoded subset, and
  dedicated columns are indexed for stats.
- **Stats Storage Breakdown is served from manifest `LabelAggregates`** (real and durable), with
  a fallback to manifest aggregates for dedicated columns, so breakdown shares no longer depend
  on in-memory state.
- **The Lakehouse UI and stats JSON responses are sent `no-store`**, so deploys and browsers
  never surface stale cached stats.
- **Default Storage Breakdown labels** are now `deployment.environment, service.name,
  k8s.namespace.name, k8s.cluster.name, k8s.deployment.name` (dropped `cloud.region`).

### Fixed

- **Multi-instance gossip now actually runs.**

  A multi-node deployment silently never gossiped — each node only ever saw itself. Several
  gaps, all fixed: (1) `Storage.RefreshDiscovery` (the only path that resolves the peer ring
  into the stats + tenant-alias `SyncPusher`s and the Phase D fleet-metadata gossip) had zero
  runtime callers — a `startPeerDiscovery` loop now calls it once at startup + every
  `discovery.peer_refresh_interval` (30s) when a peer headless service is configured; (2)
  `RefreshDiscovery` only ran `DiscoverPeers` when a peer cache existed — it now runs whenever a
  peer service is configured (`Discovery.HasPeerService`), and storage-node/partition-list
  discovery is non-fatal so it can't starve peering; (3) the stats `SyncPusher` skipped pushes
  with no tenant changes, dropping the piggy-backed Phase D `NodeMeta` — it now pushes whenever
  node metadata is present.

  Dead instances (node_id = ephemeral container hostname) accumulated forever in the gossiped
  snapshot; `NodeMeta` now carries a `LastUpdated` stamp and
  `NodeMetaAll`/`/stats/instances`/the overview sum drop peers older than `stats.node_meta_ttl`
  (default 90s). Single-instance behavior unchanged. Both modules. Found by standing up a 3-node
  cluster (`deployment/docker/docker-compose-cluster.override.yml`).
- **High-card id cardinality (trace_id / span_id) persists across restart.** The always-sketch
  id path (ids deliberately not bloomed) is persisted via the pmeta catalog facet, so distinct
  counts survive restart instead of resetting to zero.
- **Overview partition count is tenant-scoped** and reconciles with the tenant view; tenant
  partitions are scoped to the tenant.
- **vmui Lakehouse tab restore survives the React re-render race.**
- **Storage Classes panel reconciles with the headline totals.** Per-class files/bytes are
  derived from the live manifest file set (class age-predicted per file, as the Cost view
  already does) instead of the registry's cumulative class counters — which are never
  decremented on compaction and so over-reported (e.g. 1,815 files vs the live 1,425). The class
  split now sums exactly to Files / Compressed.
- **Storage Breakdown estimates scale to the live total.** The proportional `estimated_bytes`
  base uses `LiveAggregate()` — the same total the overview headline shows — instead of the
  drift-prone cached counters.
- **Cardinality Explorer Storage column shows real-magnitude bytes.** Per-field storage was
  badly undercounting (KB vs the GB on-S3 total) because files written before per-column
  accounting carry no `ColumnBytes` and only newly-flushed files were summed. The column now
  scales the covered per-field bytes up to the manifest's full on-S3 total
  (`StatsAggregate.TotalStorage`/`CoveredStorage`), so it reflects real storage immediately and
  converges to exact as compaction + new flushes backfill `ColumnBytes`.

### Performance

- **Pure-buffer query fast path:** unflushed in-memory windows are aggregated natively, avoiding
  unnecessary work for stats/count queries over not-yet-flushed data.
- **StatsAggregate S3 sidecar cache.** The per-field/per-tenant size aggregate is persisted to
  an S3 sidecar (`_meta/stats-aggregate.json`) after each reconcile (warm + refresh) and loaded
  on startup, so a fresh/restarted instance serves size stats from the cache immediately instead
  of waiting on the warm-load manifest rescan (a subsequent `Recompute` corrects any staleness).
  Wired for both logs and traces.

## [0.90.0] - 2026-06-11

### Added

- **Dedicated columns (Tier 1): promote hot OTel attributes out of the maps into typed Parquet columns.**

  15 log + 17 trace OpenTelemetry semantic-convention attributes (container.id,
  service.instance.id, k8s.cluster.name, telemetry.sdk.*, cloud.*, url.full, client.address,
  server.address, db.*, rpc.method, exception.type, …) are lifted from the resource/log/span
  attribute maps into first-class columns — dict-encoded for low-cardinality descriptors (the
  compression win) and plain+bloom for high-cardinality id-like keys (selective row-group
  skipping). **Measured net size −9.5% (logs) / −8.0% (traces)** on real L2 data — the promotion
  compression win absorbs the expanded blooms and then some.

  VL/VT-compatible by construction: read paths emit each column under its exact field name (logs
  bare, traces VT-prefixed), identical to the existing promoted-column mechanism; dual-read safe
  across schema versions (old files keep map storage, new files use columns, queries see the
  same fields). Pure-Parquet portability verified (pyarrow + DuckDB readback gate).

- **Dedicated columns (Tier 2): operator-configurable custom-attribute promotion.** Deployments
  declare non-OTel attributes via `logs.config.promoted_attributes` /
  `traces.config.promoted_attributes` (`{name, bloom}`, up to 8/signal); each is lifted into a
  spare slot column with an optional bloom. The name→slot binding is written to every file's
  Parquet footer (flush AND compaction), so files stay self-describing and read-back remaps
  slots by each file's own footer — correct even if the config later changes. Full-stack e2e
  (both signals) + dual-read equivalence proofs included.

### Changed

- **Bloom set re-aligned to measured cardinality.** Stopped blooming low-cardinality columns
  where a bloom never skips (k8s.namespace.name, k8s.deployment.name, deployment.environment),
  added high-cardinality ones (span_id, k8s.node.name) — plus the Tier-1 high/medium-card
  promotions. Logs 10 / traces 16 bloom columns, all equality-queried high/medium-cardinality.
  Writer + compactor now derive the bloom set from the strict per-signal sets in internal/schema
  (no more hardcoded service.name+trace_id).

## [0.89.0] - 2026-06-11

### Removed

- **The dead `schema.extra_promoted` dynamic-promotion feature (BREAKING for that config key only).**

  Strict-schema direction: which Parquet columns are promoted is owned by the compiled static
  profile (`registry.PromotedColumns()` = `profile.Promoted`), giving deliberate control over
  every column for write/read optimization — not a per-deployment dynamic config. The
  `extra_promoted` path was non-functional anyway (the registry was built without it and the
  struct-typed writers could not emit such columns; the docs promised behavior the code never
  delivered).

  Removed the config struct + `schema:` section, registry plumbing, chart block,
  schema/allowlist entries, docs, and tests. The live `PromotedColumns()` mechanism (the static
  profile) is unchanged and is what the upcoming strict dedicated columns extend.

## [0.88.1] - 2026-06-11

### Changed

- **parquet-go upgraded v0.29.0 → v0.30.1 (both modules).**

  Verified API-compatible for our usage (zero breaking changes in any API we call), full
  write/read/compaction suites green, and the multi-engine readback gate still PASSES (files
  remain bit-identically readable by pyarrow + DuckDB). Hygiene bump that keeps us current and
  unlocks v0.30.1 writer options for future use. Measured and deliberately NOT enabled:
  `BloomFilterCompression` (GZip bloom storage, #502) — re-encoding 5 real L2 files with it
  showed **−0.033% to +0.002% size change (noise)**, because split-block bloom filters are
  near-incompressible random bit patterns; revisit only if a future schema carries many bloom
  columns. Also surveyed and skipped with reasons: `PrefetchBloomFilters` (conflicts with the
  zero-GET-open design; we prune via pmeta), `MergeRowGroups` (no zero-copy benefit — writing a
  merged view still re-encodes; row-group sizing is already controlled by
  `RowGroupSizeByOutputLevel`), parallel column writes (not write-bound), low-level page reads
  (OffsetIndex page-granular measured 0% on our footers).


## [0.88.0] - 2026-06-11

### Added

- **Planned-fetch v2 slices 0+1: per-signal footer prefetch, span concurrency k=16, per-SPAN
  cap, gap discipline, and the S\* whole-file ladder (both modules; every slice-1 lever is
  opt-in-planned-path only — the `window` default is untouched).** From the approved v2 research
  (`docs/architecture/planned-fetch-v2-research.md`), attacking the live verdict's root causes
  (13–15 spans/file drained 4-at-a-time; the per-plan cap punishing exactly the coalescing that
  cuts GETs; 2 serial open RTTs per cold file):
  - **Slice 0a — `s3.footer_prefetch_bytes` (per-signal default: logs 128 KB, traces 640 KB; file-size-clamped at max(64 KB, size/8)).**
    Fixes the traces-L2-footers-always-full-download bug-class: every live traces compacted-L2
    footer measures 467–519 KB (trace index in footer KV) and could never fit the old shared 64
    KB constant, so footer prefetch hit `too_big` and traces L2 projected reads ALWAYS fell back
    to full downloads (`fallback{reason="no-footer"}`). Logs' 128 KB also covers the page-index
    stripe (ends 91–97 KB from EOF) in the same tail GET. Regression test (both twins): a footer
    over 64 KB but under the per-signal default prefetches + caches in one pass and the planned
    open takes the warm route with the no-footer counter NOT ticking (absent-value).
  - **Slice 0b — per-scenario spans-per-plan attribution in the bench capture.**
    `scripts/bench/full-scope-s3-bench.sh` S3-ops summary gains `plans/q` and `spans/plan`
    headline columns (derived from the existing planned counters, with the same per-family
    `reset` monotonicity discipline as gets/open) plus the new gap-choice/strategy families —
    the instrumentation the live verdict demanded before trusting the next planned run.
  - **Slice 1a — span concurrency: `s3.planned_fetch_max_inflight` (default 16, was a hard-coded 4).**
    Fetch dispatches min(k, spans) concurrent span GETs: a typical per-file plan now rides ONE
    RTT wave instead of ~4 (8 file workers × 16 = the HTTP/1.1 `MaxIdleConnsPerHost=128`
    ceiling).
  - **Slice 1b — cap re-scope: per-PLAN → per-SPAN.**
    New `s3.planned_fetch_span_cap_bytes` (default 16 MB — ClickHouse's `bytes_per_read_task`
    scope): coalesced spans above the cap are SPLIT into cap-sized concurrent GETs, never
    rejected. The old `s3.projected_fetch_max_bytes` per-plan cap is **retired** (key kept
    parsed, deprecated): plan admission rides the existing memory ledger (span bytes charge the
    same `fileBudget` the decode admission reads; the worker already holds an fi.Size admission
    that subsumes any plan) with fi.Size as the defensive absolute ceiling —
    `fallback{reason="cap"}` no longer fires for plans whose total exceeds 16 MB but whose spans
    all fit (pinned by regression: a 1-byte legacy cap arms plans with the cap counter NOT
    moving).
  - **Slice 1c — gap discipline.**
    `armProjectedPlan` prices each plan at candidate gaps {64 KB, 256 KB, 1 MB} — pure in-memory
    math over the already-parsed ranges, `cost = ceil(spans/k)·RTT + bytes/BW` with the
    simulator's constants RTT=100 ms, BW=50 MB/s/conn (the RTT term counts the k-wide WAVES the
    fetch actually pays) — and fetches with the cheapest
    (`lakehouse_s3_planned_gap_choice_total{gap}`). The 5 MB "RTT-aware" flat gap stays excluded
    (the simulator's proven geometry trap). Unit tests pin synthetic range sets where EACH
    candidate strictly wins.
  - **Slice 1d — S\* + footer-cache-gated strategy ladder**
    (planned path only): warm footer ⇒ plan immediately (metadata-free); cold footer + size <
    `s3.whole_file_threshold_bytes` (per-signal default: logs 5 MB, traces 8 MB — the cost
    model's whole-file breakeven on live size distributions) ⇒ ONE whole-file GET through the
    smart-cache path whose download IS the footer-cache warmup (`ParseFooterFromData`); cold
    footer + large file ⇒ footer fetch (two-phase capable — the traces module gains this rung,
    retiring its "no inline footer fetch ⇒ always full download" behavior) then plan exact
    spans. `lakehouse_s3_planned_strategy_total{strategy}` attributes the routing per scenario;
    the 3 legacy size cutoffs (64 KB/128 KB/4 MB) remain and are noted for unification in Slice
    3. Routing pinned with absent-value asserts on all three fallback reasons.

  New per-signal config (flags in both binaries + chart values/schema, helmdrift-gated):
  `s3.footer_prefetch_bytes`, `s3.planned_fetch_max_inflight`,
  `s3.planned_fetch_span_cap_bytes`, `s3.whole_file_threshold_bytes` (0 = per-signal default).
  Tests: planned↔window equivalence (RunQuery + GetFieldValues, both twins), k-respected
  concurrency bounds, span-cap splitting correctness, gap-choice wins, S\* routing,
  oversize-footer prefetch — all under `-race`; config and s3reader hold their 90% coverage
  gates (90.7% / 91.6%).

  **Unit-measured** (committed sims, `internal/s3reader/planned_fetch_test.go`): the
  count_24h-geometry cost-model sim (40 files / 8 workers, 13×64 KB spans/file at the real ~1.9
  MB L2 stride, RTT=100 ms, BW=50 MB/s) goes **v1 (k=4) 3.08 s → v2 (k=16 + span cap + gap
  discipline) 1.58 s → 0.58 s with slice-0a-warm footers, at 600→520 GETs** — inside the
  simulator's predicted 0.8–1.6 s band vs planned-v1's live 17.7 s and window's 2.45 s; the
  filtered_count access-pattern sim is unchanged (**58.7 → 9.2 MB on wire, −84.4% at equal
  GETs** — the standing ≥75% gate). The planned-by-default decision still waits on the LIVE
  re-entry gate (run `scripts/bench/with-s3-latency.sh 100 30
  scripts/bench/full-scope-s3-bench.sh` with `projected_fetch_mode: planned` post-merge; the new
  plans/q + spans/plan columns make the verdict attributable per scenario).

## [0.87.2] - 2026-06-10

### Fixed

- **`s3.projected_fetch_mode` default flipped `planned` → `window`: the Tier-2 plan-then-fetch default did not survive the live benchmark.**

  The unit sims showed −84% bytes-on-wire at equal GET count for the sparse filtered pattern,
  and the machinery works exactly as designed live (zero out-of-plan reads, zero fallbacks,
  waste = 0) — but on dense multi-row-group scans the per-RG exact plans fragment I/O into
  hundreds of small GETs (count_24h: 175 → 1185 GETs/q) and at 100 ms S3 RTT that inverts the
  trade catastrophically (count_24h 2.4 s → 17.7 s; confirmed on a quiet machine, not load
  noise).

  Planned mode stays fully available as an opt-in; the documented re-entry conditions
  (plan-density gate, per-FILE cross-RG batching, wider span concurrency — each
  live-bench-gated) are in .

## [0.87.1] - 2026-06-10

## [0.87.0] - 2026-06-10

### Added

- **S3 Tier-2 plan-then-fetch: column-projected reads now fetch EXACT coalesced column-chunk ranges — no speculative window (both modules).**

  The post-batch-2 live state showed `filtered_count` still wasting ~46 MB/query: every
  projected file open starts a fresh 2 MB base window, abandons it after ~300 KB of column
  chunks, and closes before the adaptive shrink can learn (per-reader-instance state —
  `readahead_shrink_total` stayed 0). The fix is the CH Prefetcher / arrow-rs
  vectored-per-row-group pattern (research doc Tier-2 items 8/9): after row-group pruning, the
  new `s3reader.PlannedFetchReaderAt` derives every matched row group's projected column-chunk
  byte ranges from the already-parsed footer (**dictionary pages included** via
  `DictionaryPageOffset`, plus the lazy ColumnIndex/OffsetIndex sections), coalesces them with
  the file-size-clamped `s3.coalesce_gap_bytes`, downloads the spans **concurrently** (min(4,
  spans) in flight per file) through the pool's ranged GETs, and serves all decode reads from
  the fetched spans.

  Out-of-plan reads fall through to exact-range GETs (never an error;
  `lakehouse_s3_planned_out_of_plan_reads_total`). Span bytes are charged to the same
  `fileBudget` ledger the decode path's admission control reads and released on view close.
  Wired into BOTH modules' `queryFile` and `scanProjectedFieldValues`; full scans keep the
  adaptive-window stack untouched. New per-signal config (flags in both binaries + chart
  values/schema): `s3.projected_fetch_mode` (`planned` default | `window` = full rollback) and
  `s3.projected_fetch_max_bytes` (per-file plan cap, default 16 MB; over-cap plans fall back to
  the window path).

  New metrics: `lakehouse_s3_planned_fetches_total` / `_planned_fetch_spans_total` /
  `_planned_fetch_bytes_total`,
  `lakehouse_s3_projected_fetch_fallback_total{reason=cap|no-footer|error}`,
  `lakehouse_s3_planned_out_of_plan_reads_total` — all added to the bench S3-ops capture.
  Regression tests pin: only planned byte ranges on the wire (mock-S3 request log, both
  modules), planned↔window result equivalence (RunQuery + GetFieldValues), cap fallback
  correctness, coalescing/gap clamps, concurrent span fetch, out-of-plan fallthrough, and budget
  release on close/error. **Unit-measured** (deterministic filtered_count access-pattern sim, 28
  files × 24 MB with ~300 KB of projected chunks each, production 2 MB/8 MB windows vs planned):
  bytes-on-wire **58.7 MB → 9.2 MB (−84.4%, 2.10 → 0.33 MB/file)** at equal GET count (28 vs
  28); the ≥75% reduction is the standing regression gate
  (`TestPlannedFetch_FilteredCountAccessPatternMeasurement`). Live before/after runs post-merge
  via `scripts/bench/with-s3-latency.sh 100 30 scripts/bench/full-scope-s3-bench.sh`.

### Fixed

- **Bench S3-ops capture: counter snapshot pairs are now validated monotonic — no more NEGATIVE
  deltas.** The post-batch-2 measured round produced a negative fulltext S3-ops row: a counter
  that goes BACKWARDS between a scenario's before/after `/metrics` snapshots (engine restart
  resets counters to 0; scrape race) used to emit a negative delta that silently poisoned the
  per-query table. `scripts/bench/full-scope-s3-bench.sh` now records such cells as the literal
  `reset` (per counter, per scenario) and the summary renders `reset` for any headline cell
  whose inputs were invalidated — a poisoned scenario is visible instead of plausible-but-wrong.
  Documented in the script header.

- **Filtered counts are now metadata-served: `service.name:api-gateway | stats count()` answers from manifest `LabelAggregates` with zero S3 reads (both modules).**

  The count-pushdown fast path previously required NO row filter. The new strict AST gate
  (`countPushdownFilterFields` — every node type explicitly recognized, unknown ⇒ refuse,
  `filterEqField`-style two-field nodes report both fields) proves when a filter references
  nothing but the one aggregated field (any shape over it: word match, exact, prefix, IN,
  negation, ORs — plus the `_time` filter, which is sound because containment runs against the
  EFFECTIVE `q.GetFilterTimeRange()` and synthetic timestamps interpolate within the contained
  file's bounds).

  The synthetic distribution rows then flow through `preFilter`, which applies the REAL filter
  downstream — exactness by construction, pinned by integration tests (filtered-equals-scan with
  the fast path FIRING, cross-field filters still skipping, the old skip-test inverted
  deliberately). This targets the last structural benchmark laggard: filtered_count at 100 ms S3
  latency was 3.0× CH via a ~50 GETs/q scan; for aggregated+contained files it becomes a ~0-GET
  metadata answer like unfiltered counts (which run at 0.7× CH).

## [0.86.0] - 2026-06-10

## [0.85.0] - 2026-06-10

### Added

- **Compression step 4 — 2× row groups on L2+ rollups: `compaction.row_group_size_by_output_level` (both modules).**

  New per-output-level Parquet row-group size schedule mirroring the progressive compression
  schedule exactly: slot N = max rows per row group for compacted output files at level N,
  saturating at the last slot for deeper rollups; an empty list falls back to the static
  `insert.row_group_size` (pre-schedule behaviour, pinned by regression test). Default `[10000,
  10000, 20000]` — L0/L1 outputs keep the historical 10k rows/group, L2+ rollups double to 20k
  (cold rollups are scan-heavy and rarely pruned at row-group granularity).

  Threaded through the compactor per output level; flag
  `-lakehouse.compaction.row-group-size-by-output-level` in BOTH binaries; chart `values.yaml` +
  `values.schema.json` now document and validate the **entire `compaction.*` section** (8
  grandfathered keys burned out of the helm-drift allowlist). Bug fixed en route: the YAML
  `compaction.compression_level_by_output_level` (and the new key) was **silently dropped by the
  config merge** — file values never reached the compactor; both schedules now merge and carry a
  round-trip regression test. **Measured** (the `scripts/bench/compression_ab` methodology: 4
  largest real compacted-L2 log files from the live e2e MinIO, 490,187 rows, identical rows
  re-encoded at zstd-best, only the row-group size varied): **size −0.15%** (94,972,408 →
  94,834,273 bytes), **row groups −46%** (52 → 28), **total pages −18%** (3,103 → 2,542; pages
  per column chunk 2.4 → 3.6).

  Honest read: the byte win is small on this body-dominated corpus — the structural win is
  metadata-side (half the row-group footers/dictionaries, 18% fewer page headers, half the
  manifest/footer entries per file) and it compounds with the upcoming (stream_id, timestamp)
  sort, where bigger groups give dictionaries and RLE runs 2× the room. Numbers + methodology in
  (step-4 measured section).
- **Multi-engine parquet readback CI gate (`parquet-readback` job): every parquet encoding change now ships behind a pyarrow + duckdb readback proof.**

  `scripts/ci/parquet-readback/gen` writes synthetic logs + traces files (5k rows, every column
  populated incl. the three attribute maps, deterministic seed) using the **REAL production
  schemas** (`internal/schema.LogRow`/`TraceRow` — the delta/dict tags ride along) and the
  **REAL writer options** (zstd `SpeedBestCompression`, `MaxRowsPerRowGroup`, split-block blooms
  on `service.name`+`trace_id`, the `_trace_idx` KV footer), and emits a writer-truth manifest
  (row count, exact big-int sums of integer columns — 5k nanosecond timestamps overflow int64,
  so truth is exact arithmetic — distinct counts of low-card strings).

  `verify.py` then proves, for BOTH engines independently: aggregates == writer truth;
  pyarrow↔duckdb row-level equality via `EXCEPT ALL` in both directions (0 rows);
  `DELTA_BINARY_PACKED` on every delta-tagged column and `RLE_DICTIONARY` on every dict-tagged
  column (expectations derived from the live struct tags via reflection, so schema changes
  auto-propagate); and PageIndex (ColumnIndex + OffsetIndex) present on 100% of column chunks.
  86 checks, all green locally and wired as a CI job. This is the standing gate promised in .

- **S3 batch 2a — waste-feedback read-ahead: the adaptive window now SHRINKS when it fetches bytes nobody reads (both modules).**

  The Tier-1 grow/reset state machine was blind to waste: the combined benchmark measured **46
  MB/query fetched-but-never-read on filtered counts (56% buffer hit rate) and 17 MB/query on
  fulltext** — sparse forward hops classify as "forward-sequential", so every abandoned window
  VOTED GROW for the next one. Now each window eviction computes the evicted window's never-read
  ratio (same high-water accounting as `lakehouse_s3_buffer_wasted_bytes_total`,
  allocation-free); above the threshold the next window is **halved (floored at
  `read_ahead_bytes`)** and the growth credit resets — growth resumes only after 2+ consecutive
  efficient windows.

  Fully-consumed sequential scans still grow to `read_ahead_max_bytes` and stay there (pinned by
  regression tests). New per-signal config: `s3.read_ahead_waste_threshold`
  (`-lakehouse.s3.read-ahead-waste-threshold`, default 0.5, `>=1` disables; + chart
  values/schema) and a `lakehouse_s3_readahead_shrink_total` counter. The `openRangedParquet`
  file-size clamps are untouched. **Unit-measured** (deterministic page-probe sim: 256 KB page
  per 3 MB stride over a 64 MB file at production 2 MB base / 8 MB max windows): bytes-on-wire
  **57.7 MB → 45.1 MB (−21.8%)**; steady-state never-read bytes per abandoned window drop from
  up-to-8 MB (grown max) to the 2 MB base (4× at defaults).

  Trade-off stated: the smaller window costs more GETs on sparse patterns (9 → 22 in the sim) —
  the live before/after (waste B/q on filtered/fulltext, p50s) runs post-merge via
  `scripts/bench/with-s3-latency.sh 100 30 scripts/bench/full-scope-s3-bench.sh`.

### Fixed

- **S3 batch 2b — compaction now HEALS missing `LabelAggregates` instead of propagating the wipe (both modules).**

  The compactor built the output `FileInfo` with `mergeFileLabelAggregates(g.Files)` — a merge
  of the INPUT files' aggregate maps, which are empty for every file written before the #138
  refresh-wipe fix, so compaction could never repair pre-fix data and `count() by (field)`
  fast-paths kept missing compacted files forever. The compactor holds the actual merged ROWS,
  so it now extracts aggregates from them via the **single shared implementation** the flush
  writers use — `schema.ExtractLogLabelAggregates` / `schema.ExtractTraceLabelAggregates`, moved
  to `internal/schema/label_aggregates.go` with the shared `MaxLabelAggregateValues` cap (no
  duplicate field lists to drift; both modules' writers and the compactor import the same code).

  Every compaction cycle now monotonically heals old files. Regression tests pin: (a)
  **healing** — inputs with nil aggregates produce outputs with correct per-(field,value) counts
  (logs + traces modes); (b) **equivalence** — when inputs DO carry aggregates, row extraction
  equals the old input-map merge for identical data; (c) **absent-value** — a field over the
  per-field value cap is ABSENT from compacted output exactly like flush.

  Expected live effect (verified post-merge over compaction cycles): `count_24h`-class metadata
  answers stop degrading as partitions compact.

## [0.84.1] - 2026-06-10

### Changed

- **Compression roadmap: the (stream_id, timestamp) sort is REJECTED on real-data evidence; the A/B harness gains the tooling that proved it.**

  The roadmap's projected-biggest item (30–50% smaller files from stream-clustered rows) was
  implemented, passed both modules' full suites including flush↔export parity — and then failed
  the mandatory real-data measurement: **+17.7% (zstd-default) / +13.1% (zstd-best) LARGER
  files** across 10 real compacted-L2 files (identical rows, only the order changed). Per-column
  attribution shows why this corpus punishes stream-clustering: `body` +2.07 MB per 24 MB file
  (similar log lines arrive in cross-stream time bursts that zstd exploits via adjacency),
  `trace_id` +0.86 MB (spans of one trace are time-adjacent → shared prefixes), `timestamp`
  +0.38 MB (globally monotonic deltas compress to almost nothing; stream-sawtooth doesn't).

  The sort code is reverted (preserved in branch history); the step-2 trap fixes stand on their
  own as correctness wins. Shipped instead: `scripts/bench/compression_ab` gains a
  `tagged+sorted` third variant, and the new `scripts/bench/compression_percol` attributes any
  size delta to individual columns — every future layout experiment gets judged by the same
  evidence, per the per-PR benchmark protocol. Re-entry condition documented in : a
  corpus-dependent sort toggle, only if production-shaped data (heavy per-stream template
  redundancy) measurably differs.

## [0.83.1] - 2026-06-10

### Fixed

- **The 30-second manifest refresh silently wiped `LabelAggregates` — the count-pushdown fast path (PERF-2) was dead in production.**

  `RefreshFromS3`'s preserve-enrichment merge copied ten `FileInfo` fields one by one;
  `LabelAggregates` (added by PERF-2 after that list was written) was not among them, so every
  file's aggregates vanished within one refresh interval and `* | stats by (field) count()`
  queries scanned ~100 files (1.7 s at 100 ms S3 latency) instead of being answered from the
  manifest in milliseconds. Root-caused from the new per-scenario S3-op telemetry.

  Fix: the merge now preserves the **entire tracked entry** on key match (S3 objects are
  immutable — a LIST carries no newer information), via `mergeRefreshedFilesLocked`.
  Anti-recurrence: `TestRefresh_PreservesEveryEnrichmentField` walks `FileInfo` by
  **reflection**, fills every exported field, and asserts each survives the merge — adding a
  field without preserving it now fails CI instead of silently regressing.

## [0.83.0] - 2026-06-10

### Added

- **S3 read-path Tier 1 (batch 1) — parquet open hygiene, adaptive read-ahead, BDP coalescing, singleflight, S3-op observability (both modules).**

  From the approved ClickHouse-first research : every ranged `parquet.OpenFile` now passes
  `SkipPageIndex/SkipBloomFilters(true)` (we prune via manifest/pmeta; internal index/bloom
  reads stay lazily available), `OptimisticRead(true)` (footer suffix+body in one tail GET),
  `FileReadMode(ReadModeAsync)` (library-native page read-ahead, bounded ~1 page in flight per
  column reader; `-lakehouse.s3.parquet-read-mode=sync` is the rollback) and a 1 MB read buffer
  (library default was 4 KB).

  The coalescing gap default goes 64 KB→1 MB (breakeven at real S3 RTT is megabytes — AnyBlob,
  VLDB 2023), read-ahead adapts 2→8 MB on sequential patterns, and the parquet magic read no
  longer pulls a wasted ~2 MB head window — **all clamped by file size** so small files keep
  precise column-projected reads. Footer/`.bloom`/pmeta-bundle GETs are singleflight-deduped
  (`context.WithoutCancel` so a cancelled query can't poison waiters). 8 new metric families
  (GETs by phase, per-open GET histogram, window waste, over-fetch, grow/reset, head-bypass,
  dedup) and the full-scope benchmark now records **per-scenario S3-op deltas**.

  New per-signal config: `s3.read_ahead_max_bytes`, `s3.read_buffer_size`,
  `s3.parquet_read_mode` (+flags +chart values/schema). **Measured on the live stack at 100 ms
  injected S3 latency: count_1h 1478→878 ms (−41%), count_24h 4133→2475 ms (−40%), `gets/open`
  4–6→2.0**; plain count_24h now beats hot VL (0.9×).

## [0.82.0] - 2026-06-10

### Changed

- **pmeta is now the metadata layer, period: legacy sidecar WRITERS deleted, `pmeta.enabled` default ON.**

  The hard cleanup after the consolidation baked: `Manifest.WritePartitionSidecar` (the
  `_file_metadata.json` S3 writer), the logs `storageBloomObserver` write side (per-file
  `.bloom` + partition `_bloom.bin` writers) + the `BloomObserver` interface, and the traces
  `FlushHook` bloom feed, `PersistBloomIndex`, and `BackfillBloomIndex` are **removed** — there
  is no code path that writes the legacy sidecars anymore. The
  `-lakehouse.pmeta.retire-sidecar-writes` flag (and its config key + validation) is gone with
  them; `-lakehouse.pmeta.enabled` now defaults to **true** and is the explicit opt-out into a
  degraded mode (no catalog/bloom for new files; cold restarts warm from footers only).
  **Read-fallbacks for pre-pmeta data are kept one more release**:
  `LoadSidecars`/`LoadSidecarsForPartitions`, the `.bloom`/`bloomCache` readers, and the traces
  `bloomIdx` load. 58 tests of the deleted machinery were removed; tests that used it as setup
  now seed the read paths directly (`MarshalFileMetaSidecar` + mock S3 / `bloomS3Loader`).

  Post-switch benchmark recorded in : cold LH at parity with hot VL on every scan scenario and
  2.7–10× faster on metadata queries; CH-over-S3 trails 30–40×.


## [0.81.0] - 2026-06-10

### Added

- **pmeta `retire-sidecar-writes` — stop writing ALL legacy sidecars the facets replace: `_file_metadata.json`, per-file `.bloom`, and partition `_bloom.bin` (`-lakehouse.pmeta.retire-sidecar-writes`, off by default, both modules).**

  The back half of the consolidation. File-meta: `WarmMetadata` serves from the bundle-warmed
  fileMetaFacet with the Parquet **footer** as the cold-restart fallback (Phase 3) — so skipping
  the `_file_metadata.json` write loses nothing. Bloom: every bloom pre-filter path now consults
  the in-RAM bloom facet first — the single-file `checkFileBloom`, the logs OR-branch +
  single-set partition paths (`bloomUnionMatch`/`bloomColumnIntersect`, `_bloom.bin` via
  `bloomCache` as the fallback), and the traces pre-filters (`bloomMayContainAll` per-partition
  hybrid with the legacy in-RAM `bloomIdx` as fallback) — so under retire the logs bloom
  observer isn't wired and traces `PersistBloomIndex` is a no-op.

  Both sides of every hybrid **keep keys they have no bloom for** (a bloom can only exclude what
  it knows), so a file holding the queried value is never dropped. **Reversible** (clear the
  flag → all sidecars resume; no migration); requires `--pmeta`; default off → byte-identical.
  `TestInteg_PmetaRetire_SkipsFileMetaSidecar`, `TestInteg_PmetaFlip_ORBranchFacet`,
  `TestInteg_PmetaFlip_BloomHybridColdRestart` (cold restart with EMPTY legacy bloom: facet
  prunes, present values kept, unknown partitions kept).

### Fixed

- **pmeta hardening — 70-finding holistic audit of the whole layer (82-agent adversarial
  review), all confirmed issues fixed.** Highlights:
 - **Data races**
   (race-detector verified): catalog `Values` iterated the live id slice after unlocking while
   flush-time `Merge` mutated it in place (torn/duplicated dropdown values); `Cardinality` ran
   the HLL estimate outside the lock. Both fixed + pinned under `-race`.
 - **Lifecycle was structurally missing**
   — facets/bundles grew forever: compaction now feeds the output file's facets and removes the
   merged-away inputs (`PmetaOnCompacted`); retention removes expired files' entries and, when a
   partition fully expires, **evicts its bundle from RAM and deletes the `_pmeta.bundle` S3
   object** (`PmetaOnFileExpired`).
 - **Self-heal wired**:
   `WarmCatalogFromS3` now consumes `WarmPartitions`' result — a missing/corrupt bundle is
   rebuilt from the manifest and re-persisted (the contract existed; `Store.Rebuild` had zero
   production callers, and the next `persistDirty` would have overwritten the S3 bundle with
   partial state).
 - **Serve-while-warming**:
   the S3-decoded bundle is now ABSORBED into a live bundle that concurrent flushes already
   populated (union per facet) instead of clobbering it; dirtiness is **generation-based** so a
   contribution landing mid-persist is never dropped from the next cycle.
 - **Bloom correctness**:
   an EMPTY bloom facet reports `ok=false` (warm-created empty facets had permanently shadowed
   the populated legacy fallback with keep-everything — bloom pruning was effectively dead after
   any restart); logs `checkFileBloom` got per-column any-of semantics (`trace_id:in(t1,t2)`
   wrongly required BOTH values — missing results) + the negated-predicate guard; the **traces
   bloom feeds are uncapped** (the capped label feed false-negatived past 100 values/field).
 - **Traces module**:
   `WarmMetadata` is now actually called (the file-meta read-flip was dead code there);
   `BackfillBloomIndex` + the duplicate in-RAM bloomIdx feed are skipped under retire.
 - **Catalog exactness**:
   a field whose per-file label list hits the extractor cap is marked high-card via
   `TruncatedFields` — the catalog never serves a silently truncated list as authoritative
   (falls to the exact scan).
 - **Codec v3**:
   header CRC covers magic..facetCount incl. the partition string (a flipped facetCount byte
   previously decoded as a VALID empty bundle); the catalog payload round-trips the high-card
   state and caps untrusted allocations. v2 bundles fail decode → self-heal rebuild.
 - **Roles**:
   select-only pods now build the catalog (was writer-gated → read-only pods scanned);
   insert-only pods no longer nil-panic in `WarmMetadata` Phase 3b; `retire-sidecar-writes`
   without `pmeta.enabled` fails fast at startup; `lakehouse_catalog_resident_bytes` updates
   every flush and includes the interning dict.
- **Bloom pre-filter could wrongly exclude files when one bloom field name is a suffix of another (`name` ⊂ `service.name`) — missing results (both modules).**

  `extractExactMatch`/`extractInValues` used bare `strings.Index`, so for the query
  `service.name:="api-gateway"` the promoted span-name column (`name`) substring-matched
  `name:="` and extracted `api-gateway` as a **span-name** candidate — the bloom check
  `span.name=api-gateway` then excluded every file (whose span-name blooms legitimately lack
  that value). A bloom false-negative = silently missing query results on the cold tier.

  Fixed with `fieldTokenIndex` (field-token boundary check — `:`/`-` count as field chars,
  matching `extractQuotedOp` and the `resource_attr:service.name`-style schema keys); pinned by
  the cross-field cases in `TestExtractExactMatch_TableDriven` (suffix-field, prefixed-attr-key,
  hyphenated-neighbor) in both modules.
- **Traces bloom facet was never fed — the #130 traces bloom read-flip was silently a no-op.**

  The traces writer passed `nil` bloomValues to `catalogObserver.OnFileFlush` (logs passed the
  real map), so the traces bloom facet stayed empty: `BloomMayContain` kept everything (safe,
  but zero pruning) and under retire-sidecar-writes traces would have lost bloom pruning
  entirely after a restart. The writer now passes the same labels map the legacy traces bloom
  (`onFlush` → `bloomIdx.AddColumns` + per-file `.bloom`) is built from — facet content
  identical to legacy.

  Caught by `TestInteg_PmetaFlip_BloomHybridColdRestart`'s absent-value assertion (the earlier
  present-value-only test passed vacuously: unknown keys are kept).

## [0.80.0] - 2026-06-09

## [0.79.0] - 2026-06-09

### Added

- **pmeta file-meta read-flip — the manifest enriches `FileInfo` from the in-RAM bundle, not the S3 sidecars (flip phase 1, `--pmeta`).**

  `WarmMetadata` now fills RowCount / min-max time / raw bytes / schema-fp from the
  `fileMetaFacet` first, and only falls back to the per-partition `_file_metadata.json` GETs for
  files the facet didn't cover — skipping the 16-way sidecar fan-out entirely when the bundle is
  complete. Still **dual-write** (the sidecar is written and is the fallback) so it is
  reversible, and **pmeta-off is unchanged** (`s.catalog==nil` → `LoadSidecars` runs as today).

  `manifest.FileMetaProvider`/`EnrichFromProvider` keep the manifest decoupled from
  `internal/pmeta`; `parquets3.catalogFileMetaProvider` bridges them; runStartup warms the
  bundle before `WarmMetadata`. The sidecar *write* retirement is the follow-up.
  `TestEnrichFromProvider`.
- **pmeta bloom read-flip — `checkFileBloom` consults the in-RAM bloom facet before a per-file `.bloom` S3 GET (`--pmeta`, logs + traces).**

  Query-time file pruning now checks `Store.BloomMayContain` (the partition `_bloom.bin` bundle,
  in RAM) before downloading the per-file `.bloom` sidecar, falling back to the download only
  for partitions the bundle doesn't carry. A bloom filter never false-negatives, so a file that
  actually holds the queried value is never excluded by either path — this can only change
  pruning *efficiency*, never correctness. The traces module preserves its AND-across-columns /
  OR-within-column semantics. `metrics … {facet_bloom_skip,file_bloom_skip}` distinguish the two
  paths.
- **pmeta labels `field_names` read-flip — `GetFieldNames` serves from the catalog before the
  legacy labelIndex (`--pmeta`, logs + traces).** Field-name dropdowns are served from the
  in-RAM catalog (range-aware: only the partitions overlapping the query window) ahead of the
  `_label_index.json` fallback. (`field_values` was already catalog-first.) `catalogFieldNames`
  unions names across the range. `TestInteg_PmetaFlip_FieldNamesAndBloom`.
- **pmeta — unified partition-metadata layer + field/value catalog (`-lakehouse.pmeta.enabled`,
  off by default).** One per-partition metadata layer (`internal/pmeta`) with pluggable facets
  replaces the scatter of per-subsystem sidecars/snapshots, behind a flag so the hot paths are
  byte-for-byte unchanged when off. Built as **dual-write + parity-gated** (the old
  `_file_metadata.json` / `_bloom.bin` / `_label_index.json` still write; each facet mirrors
  them and a test asserts they match), so it is safe to enable incrementally before the sidecars
  are retired.
 - **Field/value catalog**
   — Grafana label/field dropdowns serve from an in-RAM catalog (interned dict + sorted value
   sets) instead of scanning Parquet: cold `field_values(service.name)` over 24h went **50 s →
   24 ms** on the live e2e stack (11× faster than hot VL, ~220× faster than ClickHouse-over-S3).
   Cold-start-warmed from the manifest (zero extra S3 I/O).
 - **In-house HyperLogLog cardinality**
   (LogLog-Beta, HLL++-grade accuracy, no dependency) for high-card id columns
   (`trace_id`/`span_id`) via a flush-time tap (9.7 ns/value, 0 alloc);
   `lakehouse_catalog_field_cardinality{field}` is the cardinality-bomb early-warning.
 - **file-meta + bloom facets**
   fold `_file_metadata.json` and `_bloom.bin`; **S3 bundle persist/warm** (`poolObjectStore`,
   `persistDirty` on flush, `WarmCatalogFromS3` at startup) lets the bloom facet survive a cold
   restart.
 - **A2 cardinality cap**
   (`-lakehouse.pmeta.cardinality-threshold`, default 50 000) bounds catalog RAM;
   **refuse-sketch-enumeration** (`-lakehouse.pmeta.refuse-sketch-enumeration`) returns empty
   for declared id columns instead of scanning. Metrics:
   `lakehouse_catalog_{value_lookups_total,resident_bytes,field_cardinality}`. Both logs +
   traces; `internal/pmeta` at 91 % coverage + fuzzers. See , .
- **Benchmark tooling** — `scripts/bench/with-s3-latency.sh` injects S3 latency only for the
  wrapped command and always clears it via a trap (so it never lingers on the normal compose);
  `scripts/bench/full-scope-s3-bench.sh` compares cold-LH vs hot-VL vs ClickHouse across every
  S3/scan query class. Plus (ClickHouse-parity pure-S3 roadmap) + PB-scale resource/restore
  analyses.

### Fixed

- **`field_values` with no limit (`limit==0`) scanned 2.5M logs instead of using the in-RAM
  index.** The labelIndex/catalog fast-path was gated on `limit > 0`, so a no-limit request —
  what a Grafana dropdown sends — bypassed the index and did a full column scan (50 s with S3
  latency). The index is self-bounded, so it now serves `limit==0` too; the catalog result cap
  no longer zeroes the result on `limit==0`. `TestInteg_PmetaCatalog_NoLimitUsesIndex`.

## [0.69.0] - 2026-06-09

## [0.59.0] - 2026-06-08

### Added

- **PERF-2 — manifest count-pushdown for `stats by (field) count()` (logs + traces).**

  The common Grafana panel `* | stats by (service.name) count()` is now answered from manifest
  metadata instead of opening Parquet — the standard data-lake count-pushdown (Iceberg/Delta
  manifest stats), transparent to the existing LogsQL API and dashboards.
  `FileInfo.LabelAggregates` (field→value→rowcount) is populated at flush (capped per field →
  bounded growth), summed across the disjoint inputs at compaction, and persisted in the
  manifest snapshot; `manifest.CountByLabel` sums only files fully within the window (boundary
  files would over-count and fall through to scan).

  The read path (`countByPushdownField` + `manifestCountFastPath`) serves an **unfiltered
  single-field** query from those aggregates with **zero S3 reads**, emitting synthetic rows
  that reproduce the field's distribution (incl. the empty-value group) so the existing stats
  pipe aggregates them unchanged; a filtered query, a boundary/over-cap file, or a field without
  an aggregate falls through to scan (never wrong). The synthetic column is named/formatted
  identically to the scan path; `service.name` is wired + equivalence-proven (fast path emits
  the exact same distribution as a real scan). See
  `internal/storage/parquets3/count_pushdown_test.go`.
- **Option B — logstorage-native queryable insert buffer (cold-tier recently-flushed parity with hot VT).**

  The insert buffer can now be a real per-pod `logstorage.Storage` (the VT/VL in-memory-parts
  model) instead of a `[]schema.{Log,Trace}Row` staging slice that was reconstructed into a
  `logstorage.DataBlock` at query time. That struct→DataBlock converter kept drifting from the
  file-scan emission (missing `_stream`, `start_time_unix_nano`/`end_time_unix_nano`, map
  attrs), which made cold Jaeger/Tempo search return 0 for fresh traces, 404 the log→trace
  drilldown, and zero the Tempo service-filter.

  Selected by `insert.buffer_engine` (`buffer` default | `logstore`), with `insert.buffer_dir` +
  `insert.buffer_retention`:
 - **Write path**
   — ingest feeds the native `logstorage.LogRows` the VL/VT insert path already built straight
   into the store via the exported `MustAddRows` (dual-write; the legacy path stays the
   authoritative Parquet producer). A buffer failure can never break ingestion
   (recover-isolation + `lakehouse_buffer_store_dualwrite_failures_total`).
 - **Read path**
   — cold queries serve the recent/unflushed window from the buffer via the exported
   `Storage.RunQuery` (no struct→DataBlock conversion), byte-identical to a file scan.
 - **Durability**
   — reuses logstorage's own persistence (in-memory parts written to the buffer dir every flush
   interval, restored on `MustOpenStorage`); crash-loss window equals hot VT/VL, so no separate
   lakehouse WAL is added for the buffer.
 - **VL/VT compatibility**
   — **zero upstream modification**: pure exported-API reuse
   (`MustOpenStorage`/`MustAddRows`/`RunQuery`/`DebugFlush`/`QueryContext`); no new `deps/`
   patch.
 - **Parquet-from-buffer (shadow)**
   — the converter `DataBlockToTraceRows` (parity-proven vs the legacy insert mapping) +
   `ExportBufferToParquet` + a shadow exporter write buffer-sourced Parquet to a shadow S3
   prefix for pre-cutover validation; the legacy `[]row` path stays authoritative.
 - With `logstore` enabled end-to-end: cold `[24h]` Jaeger 0→20 (matches hot), GetTrace resolves
   across recencies incl. the freshest band, Tempo `{nestedSetParent<0}` +
   `{resource.service.name="X"}` return data, and `count()`/`stats` are at 1.00 parity (no
   double-count). See .

### Fixed

- **Cold Jaeger search returned 0 traces at 12h while hot returned 20** — the `smartCache`
  fast-path in `preFilterFiles` unioned `FindFilesByTraceID(t_i)` across the queried trace IDs
  and narrowed to that union, but the union is only a *lower bound* (smartCache records only
  files it has already fetched), so a partial cache hit collapsed candidates to one file and
  dropped the other traces' spans. Fix: take the smartCache fast-path only for single-id
  (trace-by-id) queries; multi-id `trace_id:in(...)` falls through to bloom + `_trace_idx`
  narrowing, which examines every file. Guarded by the `TestColdHotParity_*` suite mirroring the
  exact VT step-1/step-2 query shapes.
- **Buffer rows invisible to `_stream:{…}` filters — cold Jaeger/Tempo returned 0 for fresh
  data** — the buffer→DataBlock conversion omitted the `_stream` (and `_stream_id`,
  start/end-time, and map-attr) columns that the stream-selector and GetTrace queries rely on;
  recent traces vanished from search and trace-by-id until they flushed and aged out. Conversion
  now emits the full column set (and the logs buffer emits its map attrs), matching the
  file-scan path.
- **Pushdown substring-match silently zeroed `service.name` filters** — `extractQuotedOp`
  substring-matched `name:=` inside `service.name:=`, building a wrong-column pushdown the
  column-stats pre-filter then used to drop every file. Added a token-boundary check; pinned by
  `extractQuotedOp` + `buildPushDownFilter` no-collision tests.
- **Empty/0-row Parquet aborted compaction, starving the cold tier** —
  `readTraceRows`/`readLogRows` treated parquet-go's `io.EOF` on a valid 0-row file as fatal, so
  one empty input file failed a whole partition's merge; since the scheduler re-picks the oldest
  partition first, that permanently starved compaction of newer partitions (growing the
  recently-flushed reachability lag). `io.EOF` is now a clean end-of-data.
- **Option B read-merge boundary (count double-count + recent-trace 404).**

  When the `logstore` buffer serves the recent window, a per-query watermark (`bufferWatermark`
  = max `MaxTimeNs` of the scanned Parquet files) splits the time range so the buffer serves
  only data strictly newer than Parquet — eliminating a 2× `count()`/`stats` double-count over
  the overlap. The watermark is bypassed for `trace_id`-filtered queries (reader-deduped span
  retrieval, where completeness matters): `queryFiltersTraceID` detects every form including the
  phrase form `trace_id:"X"` that VT's single-trace GetTrace span fetch emits (which the AST
  value-extractor misses), so recent traces no longer 404.

  Pinned by `TestQueryBufferBridge_WatermarkPreventsDoubleCount` + `TestQueryFiltersTraceID`;
  verified live (GetTrace 30/30, count 1.00, Jaeger 20/20).

### Changed

- **Bump loki-vl-proxy from v1.56.1 to v1.58.0**

  (`deployment/docker/Dockerfile.loki-vl-proxy`, applies to all four proxy services — e2e hot +
  cold, benchmark lh + vl-lh). v1.57.0 fixes high-cardinality Drilldown label/field panels at
  24h+ (`detected_level` filters use the column-indexed `level` field, ~64× faster; single-field
  `count() by(field)` over ≥2h routes to `/select/logsql/hits`; the 24h+ querySplitting
  residual-chunk spike is suppressed) and adds the L0 hot-key cache tier to metrics. v1.58.0
  adds ring-wide cache purge: `POST /admin/cache/flush?peers=1` clears the local L0/L1/L2 caches
  and fans the purge to every L3 peer (`X-Peer-Token` auth, concurrent, 5s/peer; unreachable
  peers reported, not fatal) via the new peer-side `POST /_cache/purge` — local-only without
  `?peers`.

### CI

- **Hot/cold parity unit job** (`.github/workflows/ci.yaml::parity-unit`) runs
  `TestColdHotParity*` + field/stream parity tests under `-race` with a pass/fail/skip summary,
  surfacing the cold/hot regression classes as a single PR status.
- New fuzz targets registered in the fuzz matrix (`FuzzPreFilterFiles_TraceID`,
  `FuzzExtractFilterValuesAST_TraceID`); changelog gate accepts version-section docs; assorted
  gofmt/seed fixes.

### Docs

- **** — the Option B design (logstorage-native buffer, read-merge, durability via reused VL/VT
  persistence, the exported-API-only reuse boundary).
- ** §6.2** — corrected the zstd-level claim: parquet-go maps integer levels to four buckets
  (`Fastest`/`Default`/`Better`/`Best`), so the prior "doubles compaction CPU" text was wrong.

## [0.49.0] - 2026-06-07

### Added

- **Honest lifecycle + cold-start protection at PB scale (PR #122)** — every cliff scenario the
  3PB-on-S3 / 6-peer audit surfaced (stale snapshot, fragmented L0, simultaneous restart,
  first-ever boot, partial warmup) now has a guard and a test. The pod no longer lies about
  being ready while it is still discovering files, replaying WAL, or warming the footer cache;
  queries no longer return empty results because the local store happens to be 30 seconds behind
  S3.
 - **Three-state `/ready` contract**
   (`internal/startup/manager.go`, both `cmd/lakehouse-logs/main.go` and
   `lakehouse-traces/main.go`): the readiness handler now returns `503 not_ready` (still
   discovering or below the manifest gate), `204 serving_warming` (queries answered, background
   warmup in progress), or `200 ready` (warmup complete). The lifecycle manager splits the old
   single `ready` flag into `ServingReady` and `WarmupComplete`; serving-ready requires manifest
   gate ∧ WAL replay done. Guarded by `internal/startup/honesty_test.go` (16 sub-tests pinning
   every precondition combination).
 - **`MinManifestFiles` gate**
   (`internal/config/config.go::StartupConfig.MinManifestFiles`): operator-tunable floor — the
   pod stays out of rotation until the loaded manifest crosses this threshold. Defaults to 0
   (off) for dev/CI; at PB scale set to ~10% of expected file count to mask the first-ever S3
   LIST window. Without the gate, the first pod up returns empty results to peer fan-out for the
   full LIST duration.
 - **WAL replay gating**
   (`internal/startup/manager.go::SetWALReplayNeeded/Done`): `SetWALReplayNeeded()` is set
   before `store.StartWriter()`; `SetWALReplayDone()` is set after. `ServingReady()` returns
   false until both fire — buffered rows that hadn't been flushed when the pod crashed aren't
   silently dropped from query results during replay.
 - **Snapshot age metric + bounded shutdown persist**
   (`internal/manifest/manifest.go::SavedAt()`,
   `internal/metrics/lakehouse.go::ManifestSnapshotAgeSeconds`,
   `lakehouse_min_manifest_files_gate`): a 5-second ticker exports
   `lakehouse_manifest_snapshot_age_seconds` so operators can alert when persist is silently
   failing (e.g. PVC out of space, see `docs/operations/lifecycle.md`). Shutdown persists the
   manifest FIRST under a `cfg.Shutdown.PersistTimeout` bound (default 30 s) so restart picks up
   a fresh snapshot instead of cold-listing S3.
 - **Operator-facing tuning hints on startup**
   (`internal/startup/hints.go`): after warmup completes the pod logs structured advisory lines
   covering footer-cache headroom, snapshot staleness, buffer-peers, warmup duration vs
   ready-gate sizing. Silent when the cluster is healthy. 6 hint-categories pinned by
   `internal/startup/hints_test.go`.
 - **Full-jitter exponential backoff for S3 retries**
   (`internal/s3reader/reader.go`): replaces deterministic `100ms × 2^attempt` with `jitter =
   rand(0, min(cap, 100ms × 2^attempt))` per Marc Brooker's full-jitter formula. On simultaneous
   restart of 6+ peers all hitting S3 at once, the previous backoff phase-locked retries — the
   jitter version spreads them across the retry window. Guarded by
   `internal/s3reader/jitter_test.go` (50 concurrent retries asserting bucket spread + cap
   enforcement).
 - **Streaming gob decode with 50 GiB stat-cap**
   (`internal/manifest/manifest.go::LoadFrom`): the binary snapshot format now decodes via a
   streaming gob reader fed by an `os.Open` + `io.LimitReader`, and the file size is
   stat-rejected before any decode work. Peak RSS during restart at PB scale (1M+ files, ~150 MB
   snapshot) drops from a slurp-then-decode 2× spike to a steady streaming footprint. Guarded by
   `internal/manifest/streaming_decode_test.go` (round-trip, oversized rejection, legacy JSON
   fallback, missing-file no-op, truncated file).
 - **Warmup priority sort by `MaxTimeNs` descending**
   (`internal/storage/parquets3/warmup.go` and
   `lakehouse-traces/internal/storage/parquets3/warmup.go`): partial warmup (ctx cancelled, hit
   `WarmupMaxFiles`) now yields complete coverage of the freshest partition before moving to
   older ones. Previous lexicographic key sort meant a half-finished warmup left "last hour"
   dashboards with random partial coverage. Guarded by
   `internal/storage/parquets3/warmup_priority_test.go` (sort contract + stable tiebreaker for
   equal `MaxTimeNs`).
 - **Restart + warmup design spec**
   : the canonical reference for lifecycle phase machine, `/ready` truth table, BufferBridge
   state matrix, snapshot lifecycle, cluster-coldstart protection plan, sizing matrix by scale,
   decisions table (and what was rejected), open questions, and PR roadmap. New PRs that touch
   lifecycle code must update this doc in the same change.
 - **Lifecycle operations doc**
   (`docs/operations/lifecycle.md`): three-state `/ready` contract, ServingReady/WarmupComplete
   preconditions, full configuration reference, metrics to monitor, restart timeline table,
   "when `/ready` lies" troubleshooting.
 - **Scaling restart scenarios**
   : honest worst-case analysis for 3PB+6-peer cluster across rolling restart, simultaneous
   restart, first-ever boot, stale snapshot, fragmented L0 hot zone. Tuning recommendations
   cross-referenced from `sizing.md`.
 - **Sizing guide**
   (`docs/operations/sizing.md`): memory/CPU/PVC matrix from dev → PB-scale with worked examples
   derived from the per-component cost drivers (manifest, footer cache, smart cache, WAL replay,
   per-query budget). Capacity-planning metrics + alert thresholds.

### Fixed

- **Compactor was zeroing `raw_bytes` on every merged file**

  — the compactor's output `manifest.FileInfo` did not carry `RawBytes` forward from the input
  files; it defaulted to 0 via `omitempty`. `Size` (compressed) kept tracking correctly, so
  per-tenant `TenantSummaries` derived from the manifest aggregated correct `total_bytes`
  against under-counted `raw_bytes`. Result: `/api/v1/tenants` (and the Lakehouse Explorer
  Tenants tab that consumes it) reported `compression_ratio < 1.0` — visibly impossible
  "compressed > raw" — for any tenant whose files had been compacted.

  Compaction is a pure row-union, so summing input `RawBytes` into the merged `FileInfo` is
  exact. Pre-fix files on disk still carry `raw_bytes=0` and heal as they get rolled up into
  higher compaction levels; new compactions from this build preserve raw bytes immediately.
  Guarded by:
 - `internal/compaction/compactor_test.go::TestCompactor_PreservesRawBytes` — unit regression:
   two-file compaction with explicit raw1+raw2, asserts merged equals sum.
 - `tests/e2e/tenant_stats_consistency_test.go::TestManifest_CompactedFilesPreserveRawBytes` —
   walks `/manifest/range` and asserts compacted files (level > 0) preserve `raw_bytes` (10%
   grace for pre-fix files).
 - `tests/e2e/tenant_stats_consistency_test.go::TestTenantStats_CompressionRatioReasonable` —
   tighter than existing `NotInverted` check (16 KiB threshold → 64 KiB) and bounds ratio to
   `[1.0, 50.0]` so both inversion and double-counting trip.
 - `tests/e2e/tenant_stats_consistency_test.go::TestTenantUI_RendersCompressionAndRawBytesFields`
   — UI bundle must reference `compression_ratio` / `raw_bytes` / `total_bytes` so a missing
   column can't hide future drift.

### Added

- **Multi-tenant S3 isolation (PR #111)** — full implementation of the `docs/multi-tenancy.md`
  boundary principle: string aliases are presentation-only at external surfaces; everything
  internal stays integer-keyed.
 - **In-path S3 isolation**:
   `BatchWriter` groups rows by `(AccountID, ProjectID)` at flush and writes one Parquet file
   per tenant per partition under the resolved prefix. The compactor, retention manager,
   lifecycle scheduler, and pool registry all consume the per-tenant prefix path so a tenant's
   data can never be reached via another tenant's pool.
 - **Per-tenant config overrides**
   with global-default inheritance: lifecycle, cardinality, rate-limit, retention, and
   tenant-aware bucket selection can all be overridden per `(AccountID, ProjectID)` via a YAML
   policy file. Unspecified knobs fall back to the global defaults.
 - **Bucket isolation**
   — "one process, many buckets": the s3reader pool registry resolves a per-tenant bucket from
   the policy and maintains a separate client pool per tenant, so a single lakehouse process can
   serve isolated S3 buckets without restart.
 - **Retroactive bucket migration tool + admin endpoint**
   for moving an existing tenant from the shared bucket to its own bucket without ingest
   downtime.
 - **UI + stats API surface every tenant edge case**:
   `/api/stats` now reports per-tenant `raw_bytes`, `compactor_*`, and the new VL/manifest
   parity endpoint with a matching UI panel; `global` is the sum across all tenants rather than
   an opaque counter. Compactor tenancy fixed so cross-tenant compaction is rejected.
 - **e2e**:
   tenant stats consistency + UI breakdown tests; e2e compose mounts a YAML policy file to
   demonstrate per-tenant overrides.

## [0.39.0] - 2026-06-07

### Fixed

- **Cold Jaeger search returned 0 traces at 12h while hot returned 20**

  — VT's `GetTraceList` step 2 issues `trace_id:in(t1,t2,...,t20)` for span fetch. Our
  `smartCache` fast-path in
  `lakehouse-traces/internal/storage/parquets3/storage_query.go::preFilterFiles` was unioning
  `FindFilesByTraceID(t_i)` across all queried trace IDs and narrowing to that union — but the
  union is only a *lower bound* on the relevant file set, because `smartCache` only records
  files it has previously fetched. A partial cache hit (one tid known, the rest never seen)
  collapsed candidates to one file that held spans for at most that one tid; spans for the other
  19 vanished.

  Live blast radius: cold drilldown "Slow traces" tab returned empty at the 12h window even
  though step 1 found 20 trace IDs. Fix: take the smartCache fast-path **only for single-id
  queries** (the trace-by-id shape). Multi-id `trace_id:in(...)` falls through to bloom +
  `_trace_idx` narrowing, which examines every file and is honest about coverage. Guarded by:
 - `lakehouse-traces/internal/storage/parquets3/cold_hot_parity_test.go::TestColdHotParity_SmartCachePartialHit_MustNotNarrowSilently`
   — unit pin that exercises `preFilterFiles` with a hand-seeded metadata map (one tid cached,
   one tid missing) and asserts both files survive.
 - 8 sibling parity tests in the same file (`TraceIdxKeptFile`, `TraceIDInFilter`,
   `NegationFilter`, `UnindexedFileMustStillEmitRows`, `CombinedStreamAndTraceID`,
   `OutOfWindowReturnsZeroNoError`, `MultipleFilesNarrowingMustAgree`,
   `TraceIdxIntegrity_WriterSelfCheck`) that mirror the exact VT step-1 / step-2 query shapes
   from `vtselect/traces/query/query.go` so a regression in column resolution, time-window
   narrowing, or `_trace_idx` decoding fires at the storage layer instead of presenting as "0
   traces in the UI".
 - One known regression class is deliberately left as `t.Skip("known #99 tail")`:
   `TestColdHotParity_FieldEqByParquetName` pins `service.name:="X"` (operator-typed parquet
   column name) — the main scan path needs the same dual-emission a5576bf added to
   `parquetRowToFields`. Remove the Skip when fixed.

### Added

- **Option B — logstorage-native queryable buffer (cold-tier recently-flushed parity).**

  The insert buffer can now be a real per-pod `logstorage.Storage` (the VT/VL model) instead of
  a `[]schema.{Log,Trace}Row` staging slice that was reconstructed into a `logstorage.DataBlock`
  at query time. That struct→DataBlock converter kept drifting from the file-scan emission
  (missing `_stream`, `start_time_unix_nano`/`end_time_unix_nano`, map attrs), which made cold
  Jaeger/Tempo search return 0 for fresh traces, 404 the log→trace drilldown (Grafana `Cannot
  read properties of undefined (reading 'spanID')`), and zero the Tempo service-filter.

  Behind `insert.buffer_engine` (`buffer` default | `logstore`): the buffer is fed via the
  exported `MustAddRows` (dual-write, legacy path stays authoritative) and **queries serve the
  recent/unflushed window from it via the exported `Storage.RunQuery`** — byte-identical to a
  file scan, no conversion. With `logstore` enabled end-to-end, cold `[24h]` Jaeger goes 0→20
  (matches hot), recent trace-by-id resolves at all recencies incl. the freshest ~30s, and Tempo
  `{nestedSetParent<0}` + `{resource.service.name="X"}` both return data.

  Durability reuses logstorage's own disk parts + restore-on-open (crash-loss window == VT/VL
  hot; no LH WAL added). Pure exported-API reuse — **no VL/VT modification**. Phased (P1
  dual-write + P3 read-merge shipped; P4/P5 retire the legacy path + LH WAL). A buffer failure
  can never break ingestion (recover-isolation +
  `lakehouse_buffer_store_dualwrite_failures_total`). See .

### Changed

- **Bump loki-vl-proxy from v1.56.1 to v1.57.0**

  (`deployment/docker/Dockerfile.loki-vl-proxy`). Headline fix: high-cardinality Drilldown
  label/field panels (pod, `*_id`, `trace_id`/`span_id`) now render correctly and fast at 24h+ —
  they previously returned ~142k uncapped single-point series at short ranges or an empty matrix
  at 24h+ (VictoriaLogs stats body exceeded the 16 MB cap), rendering as a single right-edge
  spike. Now `detected_level` filters evaluate against the column-indexed `level` field (~64×
  faster, no `unpack_logfmt` re-parse; 24h pod query 26s → ~0.6s), single-field `count()
  by(field)` over ≥2h routes to `/select/logsql/hits` for full-timeline series, and the 24h+
  querySplitting residual-chunk spike is suppressed on every stats path. Also adds an L0 hot-key
  cache tier to metrics (`tier="l0"` on the `cache_tier_*` series).

### CI

- **Hot/cold parity unit job** (`.github/workflows/ci.yaml::parity-unit`) — runs
  `TestColdHotParity*` + `TestFieldEqualityAndStreamFilter` (traces) and
  `TestFieldNames_VLParity` + `TestInsertAndQuery_FieldNameParity` (logs) under `-race`, emits a
  job summary with pass/fail/skip counts and expanded failure details, uploads test artifacts.
  The same tests already run as part of `test-traces` / `test-logs`, but a dedicated named job
  makes the parity check visible in PR pages and gives reviewers a single status icon to look at
  — when any of these fail it's almost always one of the cold/hot regression classes the
  drilldown has shipped before (cf. #99, a5576bf, be8c126).

### Docs

- ** §6.2** — corrected the zstd-level claim: parquet-go's writer maps integer levels to **only
  four buckets** (`Fastest`/`Default`/`Better`/`Best`) regardless of the integer value, so the
  previous text claiming `[3, 7, 11] → [3, 9, 15]` "doubles compaction CPU" was wrong — both
  schedules land in `[Default, Better, Best]` and produce the same encode profile. New table
  shows the integer ranges and bucket mapping so operators don't tune a knob that doesn't move.

## [0.37.4] - 2026-06-04

### Security

- Bump Go toolchain from `1.26.3` to `1.26.4` across `go.mod`, `lakehouse-traces/go.mod`,
  `Dockerfile.{logs,traces,datagen}`. Resolves the two stdlib advisories that were failing
  `govulncheck` / `security-logs` / `security-traces` jobs on every CI run: **GO-2026-5039**
  (`net/textproto.Reader.ReadMIMEHeader` reached via `internal/s3reader/reader.go`) and
  **GO-2026-5037** (`crypto/x509` inefficient candidate hostname parsing reached via
  `cmd/s3proxy/main.go` and `internal/manifest/metadata_sidecar.go`). Both fixed in Go 1.26.4.

### CI

- `Dockerfile.logs` and `Dockerfile.traces` now pin the builder stage to
  `--platform=$BUILDPLATFORM` and cross-compile via `GOOS=$TARGETOS GOARCH=$TARGETARCH`. The
  previous form ran the builder under QEMU on the target architecture, which intermittently
  failed `git clone` of VictoriaLogs (~70 MB packed) with `fatal: cannot pread pack file: Bad
  address` / `invalid index-pack output` on the linux/arm64 branch of the multi-arch release
  build (run 26814378149). This silently broke every subsequent non-docs release. Native-host
  git + Go cross-compile eliminates the QEMU pack-file bug and reduces multi-arch builder time
  from minutes to ~10–15 s per architecture.
- New `.dockerignore` excludes `deps/` and `lakehouse-traces/deps/` from the build context.
  These directories are cloned + patched fresh inside the builder stage; including them via
  `COPY . .` would clobber the patched state with stale host clones from previous local builds
  at different VL/VT commits, producing cryptic "undefined symbol" errors that don't occur in CI
  (fresh checkouts have no `deps/`). Also excludes `dist/`, `bin/`, `.git/`, `.github/`, and
  ephemeral test/coverage outputs to shrink the build context.

## [0.37.3] - 2026-06-02

### Added

- **VictoriaTraces parity milestone — cold-tier Tempo `/api/v2/traces/<id>` fast path + Loki →
  Tempo drilldown (PR #105)** — closes the trace-by-ID feature gap between upstream
  VictoriaTraces and the lakehouse cold tier. Three cooperating layers:

 1. **Write-side hygiene + observability**
 (`lakehouse-traces/internal/vlstorage/insert.go`,
 `internal/metrics/lakehouse.go`). VT's vtinsert pipeline emits
 internal index rows alongside spans — `trace_id_idx_stream` rows
 carrying per-trace (start, end) bounds, and `trace_service_graph_stream`
 rows carrying service-graph edges. Both are part of VT's own query
 path and must not become degenerate spans with empty `trace_id`
 in cold Parquet. The existing detector (`vtInternalRowKind`) now
 returns the metric `kind` label, and a new
 `lakehouse_vt_internal_rows_dropped_total{kind=trace_id_idx|service_graph}`
 counter exposes how many we discard so a future regression — VT
 renaming a field or a new ingest path bypassing the filter —
 becomes visible from `/metrics` immediately.

 2. **`_trace_idx` Parquet footer fast path**
 (`lakehouse-traces/internal/storage/parquets3/trace_index_lookup.go`,
 `lakehouse-traces/internal/vtstorage_adapter/trace_index_fastpath.go`,
 `internal/traceindex/` — a new shared package). Every trace Parquet
 file written by the cold-tier batch writer already carries a
 compact per-trace `(trace_id, partition, start_ns, end_ns)` summary
 in the standard Parquet `FileMetaData.key_value_metadata` slot —
 same documented field Apache Iceberg / Delta / Hudi use for their
 own table metadata, so duckdb, pyarrow `pq.read_metadata`, and
 `parquet-tools meta` all read it cleanly. The vtstorage adapter
 now intercepts VT's `{trace_id_idx_stream=<bucket>} AND trace_id_idx:=<id>`
 stats query *before* the previous scan-rewrite path, calls
 `Storage.LookupTraceIndex(ctx, traceID)`, and emits a synthetic
 `DataBlock` with the three columns VT's `findTraceIDTimeSplitTimeRange`
 reads (`_time`, `start_time`, `end_time`). No row group is ever
 opened. Span scan remains as the fall-through for files whose
 footer is unreachable. The new
 `lakehouse_trace_index_lookups_total{result=hit|miss|error}` metric
 makes the hit ratio observable.

 3. **Compaction parity for the footer index**
 (`internal/compaction/compactor.go`,
 `internal/traceindex/traceindex.go`). Before this milestone,
 `writeCompactedTraces` dropped `_trace_idx` on every merge,
 collapsing cold-tier trace-by-ID back to a full span scan as soon
 as compaction ran. The index codec is now hoisted to the shared
 `internal/traceindex/` package so the compactor and the writer
 share one source of truth, and the compactor passes the recomputed
 index through `parquet.KeyValueMetadata(traceindex.MetadataKey, …)`.
 A regression test (`TestCompactor_PreservesTraceIndexFooter`) opens
 the merged Parquet via plain `parquet.OpenFile` — proving the file
 is still 100% spec-compliant — and asserts both per-trace bounds
 plus the VT-compatible `xxhash64(traceID) % 1024` partition value
 survive the round-trip.

 4. **Upstream bump: VictoriaTraces v0.9.2 + VictoriaLogs v1.50.0**
 (`Makefile`, `lakehouse-traces/go.mod`, `patches/{vt-traces,vl-traces}/`,
 `deployment/docker/docker-compose-{e2e,benchmark}.yml`,
 `tests/parity/docker-compose.yml`). v0.9.2's release note —
 "exclude unnecessary streams during trace search" — fixes the
 parallel hot-tier leak we'd been compensating for on cold, so the
 two-stage cleanup composes correctly across both tiers. The
 `filter string` parameter VT added to
 `GetFieldNames` / `GetFieldValues` / `GetStreamFieldNames` /
 `GetStreamFieldValues` is now forwarded through the `ExternalStorage`
 overlay; the LH adapter applies the substring narrow client-side
 (`filterValuesBySubstring`), mirroring the logs adapter pattern.
 All three patches (dispatch, flag-dedup, go-mod-replace) apply
 cleanly against the new release.

 5. **Grafana drilldown completion**
 (`deployment/docker/grafana/provisioning/datasources/datasources.yaml`).
 Tempo datasources are hardened with `nodeGraph.enabled=false`
 (VT has no service-graph endpoint), the empty `serviceMap` block
 omitted (an empty `datasourceUid` triggers phantom `/api/metrics`
 calls), `streamingEnabled.{search,query}=false` (VT exposes no
 Tempo gRPC stream-over-http surface), and `spanBar.type=None` for
 parity with the Jaeger sibling datasources. The Loki proxy derived
 fields now route trace_id clicks straight to Tempo —
 `loki-vl-proxy-cold` → `tempo-lh-cold` and `loki-vl-proxy` →
 `tempo-vt-hot` — verified live in the e2e stack by drilling a
 real cold trace ID from a Loki cold log into the LH cold Tempo
 view and rendering the spans through Grafana's Tempo plugin.

 Constraints honoured throughout: zero VT/VL upstream modification
 (everything is either an adapter behind the `ExternalStorage` overlay
 in `patches/{vl-traces,vt-traces}/` or LH-side code); zero
 non-standard Parquet encoding (every byte LH writes to S3 is readable
 by any spec-compliant tool); zero Jaeger regression (the three Jaeger
 datasources remain provisioned and unchanged — explicit "open in
 Jaeger" usage and Grafana Jaeger plugin drilldown still work).

 Verified in `deployment/docker/docker-compose-e2e.yml` after rebuild:
 all six trace drilldown paths (3 Jaeger v1 + 3 Tempo v2 across hot
 / cold / multilevel `vtselect` fan-out) return HTTP 200 with real
 trace bodies; `lakehouse_trace_index_lookups_total{result="hit"}`
 climbs on every cold trace-by-ID; the cold-tier Grafana Explore
 panel for `tempo-lh-cold` renders trace spans from a Loki cold log
 link end-to-end.

- **Election-free compaction (spec 2026-05-31)** — replaces the K8s Lease / S3-sentinel
  single-leader scheme with HRW (Highest Random Weight) partition ownership computed in-process
  on every pod. Each pod independently decides which partitions it owns by ranking peers (from
  the existing peer-cache) with xxh64 — the highest-weight peer per partition wins, with
  deterministic tie-break by peer name. The result: zero K8s coordination dependencies and ~5 MB
  binary-size reduction per module (the entire `k8s.io/client-go` REST closure + 13 transitive
  deps drop out).
 - `internal/compaction/ownership.go` — `OwnershipResolver` with AZ stratification (same-AZ
   peers preferred, fall back to all-AZ when same-AZ is drained), `IsDraining` filter,
   ring-stabilization gate (defer ownership decisions during ring change), and `RankedOwners` /
   `SecondaryOwner` / `TertiaryOwner` ladders for Tier A failover.
 - `internal/compaction/orphan_sweep.go` — two-tier orphan reclamation. Tier A: detects stale
   `LastCompactionAttempt` per partition (≥3 × Interval) and lets the secondary HRW owner steal
   compaction. Tier B: walks S3 prefix layout, hash-buckets dates across pods, and deletes
   parquet keys that survive a three-step safety gate (not-in-manifest + age-gate + post-LIST
   manifest re-snapshot).
 - `internal/compaction/fair_share.go` — per-tenant round-robin scheduler. Cursor advances
   across tenants every tick so no noisy tenant can starve others. Configurable
   `CompactionsPerTenant` budget.
 - `internal/compaction/drain_handler.go` + `Scheduler.Drain()` — HTTP `POST /lakehouse/drain`
   endpoint marks the pod as draining, waits for in-flight compactions to finish (bounded by
   `DrainTimeout`, default 90 s), and emits the `X-Lakehouse-Draining: true` header so peer pods
   exclude us from the HRW ring within one tick.
 - `manifest.AddFile` idempotency + per-partition `LastCompactionAttempt` tracking — the
   manifest now silently no-ops on duplicate (key, partition) inserts, surfacing a
   `lakehouse_manifest_addfile_duplicate_key_total` canary for hidden upload bugs.
 - **12 new compaction observability metrics**
   including `compaction_partitions_owned`, `compaction_ownership_self_in_peers`,
   `compaction_dual_ownership_total` (load-bearing — alerts on >0),
   `compaction_orphan_files_deleted_total`, `compaction_orphans_skipped` (per-reason vector),
   `compaction_deferred_stabilizing`, `compaction_deferred_ring_thrash`, `compaction_draining`,
   `compaction_aborted_during_drain_total`, `compaction_stolen_total`,
   `compaction_sweep_deferred_stabilizing` (Tier A / Tier B).
 - **41 new tests**
   (33 edge cases from spec §3 + 8 HPA recovery tests from spec §11.6). Every load-bearing
   assertion documents a negative-control revert in its leading comment — removing the
   corresponding production guard must cause the test to fail. Coverage gates met:
   `ownership.go` 96.25 % (>= 95 %), `orphan_sweep.go` 91.96 % (>= 90 %), `fair_share.go` 94.78
   % (>= 90 %).
 - **HPA-safe scaling chart defaults**
   — PDB template (`pdb.yaml`), `preStop` hook calling `POST /lakehouse/drain`, generous
   `terminationGracePeriodSeconds`, and per-component PDB toggles
   `select.podDisruptionBudget.enabled` / `insert.podDisruptionBudget.enabled`.

- **Runtime `Acquire` wiring for 4 resource-bound surfaces** — turns the K8s-style resource
  bounds added in v0.37.1 from metric-exposure-only into real backpressure with admit/reject
  semantics. Surfaces wired in both modules (logs + traces):
 - Query file workers — admit at `fileWorkerLoop`; ctx-cancel during blocked Acquire surfaces as
   "file workers limit exceeded" error (was: queued indefinitely on channel).
 - Cache memory — `LRU.SetBound`; `Put` returns silently when bound rejects, cache becomes
   best-effort (was: silent LRU eviction only).
 - Smart cache disk — `DiskCache.SetBound`; `Put` / `PutFromPath` return `ErrBoundFull` on
   rejection (was: always succeeded, watermark eviction only).
 - Query max rows — admit at `acquireQueryMaxRowsBudget`; "query max-rows budget exhausted"
   error when N concurrent queries × maxRows exceed the bound's Limit (was: per-query soft check
   only).
- New `resourcebounds.Bound.TryAcquire(n)` non-blocking API + exported `ErrBoundFull` sentinel
  for cache hot paths that cannot tolerate blocking on bound exhaustion.
- 34 new unit tests across both modules covering admit/reject paths, release on every code path
  (eviction / Delete / Clear), nil-bound passthrough, outlier admission (oversized single holder
  admitted alone), ctx-cancel during blocked Acquire, and rejection-metric increments. Each
  load-bearing assertion documents a negative-control proof ("comment out X → this test must
  fail") per the harden-and-lock rule.
- Live e2e metrics confirm load-bearing in production:
  `lakehouse_resourcebound_cache_memory_acquired_total 1162`,
  `…query_file_workers_acquired_total 2489`, `…query_max_rows_acquired_total 5` measured against
  the rebuilt e2e compose stack.
- `cache/lru.go` and `cache/disk.go`: added `SetBound`, `RejectedByBound` accessors and
  per-entry `boundRelease` closures so every code path that drops an entry (eviction, Delete,
  Clear, Update-with-replace) releases its slot back to the bound exactly once.
- `internal/storage/parquets3/storage_query.go`: extracted `processOneFile`, `fileWorkerLoop`,
  `acquireQueryMaxRowsBudget` helpers to keep `RunQuery` within the 50-line gocyclo budget after
  wiring the new admit points.

### Fixed

- **Stats API compression endpoint fallback** — the manifest-only fallback path (when registry
  is empty) was not including `RawBytes` in the response, causing compression ratio to show as 0
  in the Lakehouse UI. The fallback now correctly accumulates `RawBytes` from
  `TenantSummaries()` and includes it in per-tenant compression entries, allowing the average
  compression ratio calculation to properly compute `RawBytes / TotalBytes`.

### Removed (election-free compaction)

- **`internal/election/` package** — entire directory deleted (~5 kLOC across `auto.go`,
  `k8s.go`, `s3.go`, `noop.go`, `leader.go`, plus coverage, fuzz, integration, leak, regression,
  and soak test suites). HRW ownership (above) makes this code unnecessary.
- **`internal/compaction/sentinel.go` + `sentinel_test.go`** — the S3 sentinel lock that
  prevented two pods from compacting the same partition. Replaced by HRW: each partition has
  exactly one HRW-elected owner per tick.
- **`internal/compaction/sharding.go` + `sharding_test.go`** — the modulo-shard partition
  assignment scheme. Replaced by HRW (better rebalancing properties on N→N+1 transitions: only
  ~1/N partitions move).
- **`BloomController.SetLeader` / `IsLeader`** — bloom tuning state is per-pod (cfg / overrides
  / adjustments live on the controller instance only), so the previous leader gate was
  decorative. Every pod now auto-tunes its own bloom params.
- **`config.CompactionConfig` fields**: `LeaderElection`, `LeaseDuration`, `S3LockTTL`,
  `S3Heartbeat`, `ShardID`, `ShardCount`.
- **CLI flags**: `-lakehouse.compaction.leader-election`,
  `-lakehouse.compaction.lease-duration`, `-lakehouse.compaction.s3-heartbeat`,
  `-lakehouse.compaction.s3-lock-ttl`, `-lakehouse.compaction.shard-id`,
  `-lakehouse.compaction.shard-count`.
- **Election metrics**: `lakehouse_election_leader`, `lakehouse_election_transitions_total`,
  `lakehouse_election_health_checks_total`.
- **Chart artifacts**:
 - `charts/.../templates/compaction-rbac.yaml` (Role + RoleBinding for
   `coordination.k8s.io/leases`).
 - `charts/.../templates/tenant-rbac.yaml` (same surface for the tenant-alias sync leader — see
   spec §10 Q6; the actual alias-sync migration to HRW ownership is tracked separately).
 - `lakehouseConfig.compaction.leader_election` / `lease_duration` / `s3_lock_ttl` /
   `s3_heartbeat` / `shard_id` / `shard_count` keys from `values.yaml`.
 - `POD_NAMESPACE` downward-API env-var injection in `statefulsets.yaml` (consumed exclusively
   by the now-deleted elector).
- **E2E**: `.github/workflows/e2e-k8s.yaml`,
  `tests/e2e-k8s/{kind-config.yaml,test_leader_election.sh}`,
  `tests/verification/probe_k8s_election_failover.sh`.
- **Go dependencies tidied by `go mod tidy`** on both modules: `k8s.io/apimachinery v0.36.0`,
  `k8s.io/client-go v0.36.0`, `k8s.io/klog/v2 v2.140.0`, `k8s.io/kube-openapi`, `k8s.io/utils`,
  `sigs.k8s.io/json`, `sigs.k8s.io/randfill`, plus 13 transitive deps (`fxamacker/cbor/v2`,
  `modern-go/reflect2`, `json-iterator/go`, `munnerz/goautoneg`, `golang.org/x/term`,
  `golang.org/x/time`, `gopkg.in/inf.v0`, `go.yaml.in/yaml/v2`, `davecgh/go-spew`,
  `x448/float16`).

### Deferred

- S3 download bound runtime wiring in `lakehouse-traces` — the traces module's `getFileData`
  calls `s.pool.Download` directly without the channel-based admission point the logs module
  uses (v0.37.1's wiring point). Wiring it would require either adopting the logs-module `dlSem`
  pattern or refactoring to a different admission point — both invasive enough to deserve their
  own PR. The bound is constructed in traces for metric exposure (request/limit gauges populated
  at startup; outstanding=0 at idle). All 4 other surfaces are fully wired in traces.

### Performance

- `Bound.TryAcquire` hot path: 25.84 ns/op; rejection path: 4.49 ns/op (Apple M5 Pro).
- `cache.Put` with bound wiring: 192.6 ns/op vs 144.7 ns/op baseline (+47.9 ns, +33%).
  Bound-rejection fast-path is actually faster: 133.8 ns/op (-7.6%) — the cache no-ops without
  LRU shuffle when the bound rejects.
- `Bound.Acquire` blocking variant: 295.8 ns/op — slower than `TryAcquire` because it spawns a
  ctx-watch goroutine for the cancellation path.

## [0.37.2] - 2026-05-31

### Changed

- `applyFlags` in `cmd/lakehouse-logs/main.go` and `lakehouse-traces/main.go` split into nine
  per-section helpers (`applyTopLevelFlags`, `applyS3Flags`, `applyResourceBoundFlags`,
  `applyTopologyFlags`, `applyManifestFlags`, `applyCacheFlags`, `applyCompactionFlags`,
  `applyQueryLegacyFlags`, `applyLogsFlags` / `applyTracesFlags`, `applyTenantFlags`). Removes
  the `//nolint:gocyclo` suppression added in v0.37.1 when the K8s resource-bound triples pushed
  the flat dispatch past cyclomatic complexity 50. Each helper's complexity is linear in flag
  count per section; no behaviour change — every flag-assignment branch preserved in its
  original relative position.

## [0.37.1] - 2026-05-30

### Added

- `internal/resourcebounds` package — generalises the in-tree `fileBudget` semantics into a
  reusable `ResourceBound` primitive with K8s-style `Request` (always-reserved baseline),
  `Limit` (hard ceiling, enforced via blocking `Acquire`), `LimitCount` (per-holder count cap),
  and `ScalingPolicy` enum (`Fixed`, `LinearGrowth`, `ExponentialBackoff`). Preserves the legacy
  outlier-admit semantics (single oversized holder admitted alone when pool empty) so individual
  large parquet files remain processable. Includes `PrometheusSink` adapter and `Resolve` helper
  that handles the operator-facing flag triple resolution (new triple takes precedence,
  deprecated alias falls back with one-time warning).
- 30 new per-surface metrics in `internal/metrics/lakehouse.go` —
  `lakehouse_resourcebound_<surface>_{acquired,rejected,outstanding_bytes,outstanding_count}_total`
  + `_request` / `_limit` info gauges, for all 5 surfaces (s3_concurrent_downloads,
  query_file_workers, cache_memory, smart_cache_disk, query_max_rows).
- Five operator-facing K8s-style flag triples:
 - `-lakehouse.s3.concurrent-downloads.{request,limit,scaling}`
 - `-lakehouse.query.file-workers.{request,limit,scaling}`
 - `-lakehouse.cache.memory.{request,limit,scaling}`
 - `-lakehouse.smart-cache.disk.{request,limit,scaling}`
 - `-lakehouse.query.max-rows.{request,limit,scaling}`
- `shouldUseWildcardRangeRead` in both `internal/storage/parquets3/range_reader.go` and
  `lakehouse-traces/internal/storage/parquets3/range_reader.go` — switch that opens parquet
  files via lazy S3 ReaderAt instead of buffered full download for wildcard queries on files
  >=4MiB. Bounds wildcard heap to working-set-row-group bytes (<10MiB/file) instead of
  cumulative-file-bytes (16 workers × ~30MiB ≈ 480MiB).
- Unit tests: 22 in `internal/resourcebounds` (Bound, Resolve, PrometheusSink, ScalingPolicy,
  fileBudget-legacy-semantics) + 8 in `internal/storage/parquets3/resourcebound_wiring_test.go`
  (5-surface defaults populated, S3 deprecated alias honored, new triple takes precedence) + 5
  across both modules' `range_reader_test.go` covering the wildcard cutoff.

### Changed

- `openParquetFile` (both modules): wildcard (`projectedCols == nil`) queries on files >=4MiB
  now use lazy S3 ReaderAt + BufferedReaderAt + CoalescingReaderAt chain instead of
  `bytes.NewReader(getFileData())`. Cache hit short-circuit preserved (no per-row-group HTTP
  overhead on cached files).
- `internal/storage/parquets3/storage.go` Storage struct: adds `bounds *resourceBoundSet` and
  `s3DownloadsBound *resourcebounds.Bound` fields. S3 downloads now tick the new bound for
  metric visibility (channel-first acquire order preserves wire semantics 1:1 with pre-bound
  behaviour).
- Five YAML config field families extended on `S3Config`, `QueryConfig`, `CacheConfig`,
  `SmartCacheConfig` (additive — the existing single-value fields continue to work as deprecated
  aliases that fire one startup warning each).

### Deprecated

- `-lakehouse.s3.max-concurrent-downloads` (replaced by
  `.concurrent-downloads.{request,limit,scaling}` triple)
- `-lakehouse.query.file-workers` (replaced by `.file-workers.{request,limit,scaling}` triple)
- `-lakehouse.cache.memory-mb` (replaced by `.memory.{request,limit,scaling}` triple)
- `smart_cache.disk_limit_max` YAML (replaced by `.disk.{request,limit,scaling}` triple)
- `-lakehouse.query.max-rows` (replaced by `.max-rows.{request,limit,scaling}` triple)
- Each fires a one-time startup `logger.Warnf` deprecation warning when set without the
  corresponding new triple. Behaviour preserved exactly (flat ceiling at alias value). Scheduled
  for removal in v1.0.

### Fixed

- 7-day wildcard heap retention: 3 back-to-back 7-day wildcard runs against the production-shape
  compose stack peak at ~785-815 MiB container RSS post-Goal B (well under the 1.0 GiB target;
  pre-this-work measured peak was ~1.7 GiB per project history).
  `lakehouse_s3_range_reads_total` confirms Goal B fires for ~35% of file opens (1062/3063 in
  the 3-run sweep) — the remainder are either small files (<4MiB cutoff) or projected queries
  (use the original range-read path).
- Tombstone validation: inverted time range (StartNs > EndNs) no longer falsely matches files
- S3 endpoint SSRF protection: validates URL scheme and blocks link-local/cloud metadata IPs
- Traces `start_time_unix_nano` schema type changed from `TypeTimestampNano` to `TypeInt64` —
  now returns numeric epoch nanos matching VT format instead of RFC3339 formatted strings
- Bloom filter: disable token extraction for OR queries (`" or "`) that produce false-negative
  filtering
- Pushdown filter: disable file-level filtering for OR queries to prevent incorrect result
  exclusion
- Token bloom: skip regex (`~`), range, and `len_range` predicates that bloom filters cannot
  model
- Token bloom: skip syntax fragments containing brackets, parens, or quotes
- Parity test syntax fixes for VL v1.50.0: `replace`/`replace_regexp` require `at` keyword,
  `dedup` replaced with `uniq`, `stats count() / N` split into `stats + math` pipes, quoted
  colon-containing field names in `stats by()`
- Bloom filter (row-group): OR within column / AND across columns for `field:in(v1,v2,v3)` —
  previously AND'd same-column values, dropping every row group that didn't bloom-contain the
  first value. Caused Jaeger spans-lookup (stage 2) to return 0 traces even after stage 1 found
  trace_ids.
- File-level bloom pre-filter: handles `:in(...)` values via per-value union (`MayContainAll`
  per value, union of matches). `preFilterFiles` / `filterFilesByBloomIndex` / `checkFileBloom`
  previously only handled the single-value form.
- Row-group time range: aggregate min/max across all pages instead of trusting edge pages
  (`MinValue(0)`, `MaxValue(N-1)`). Pages within a row group are not sorted by timestamp —
  traces especially have out-of-order spans (root emits after children). Narrow time windows
  from Jaeger's expansion loop were getting false-negatives skipping row groups whose true max
  lived in a middle page.
- Adapter pipe-passthrough: vtstorage adapter passes the full query (with pipes) to
  storage.RunQuery so `queryColumns` can expand the parquet column projection to cover
  pipe-referenced fields (`partition by (trace_id)`, `fields _time, trace_id`). Previously
  `CloneWithoutPipes` stripped pipes before storage, dropping trace_id from projection — Jaeger
  tag-filtered searches returned 0.
- Projection: bare `{tag=val}` stream selector (VL canonical form omits the `_stream:` prefix)
  now triggers `_stream` projection. Quoted field names (`"span_attr:http.status_code":=200`)
  now match `referencesField`. Without these, filterStream rejected every row and tag-filtered
  queries returned 0.
- Tempo search: HTTP shim converts legacy Grafana `tags=service.name=foo` panel shape to TraceQL
  `q={resource.service.name="foo"}` when `q` is empty. Upstream VT `parseTempoAPIParam`
  overwrites the documented `q="{}"` default with empty string when client sends `tags=` only,
  then `traceql.ParseQuery("")` fails and the handler returns `{"traces":[]}`. Shim wraps the VT
  call without modifying `deps/`.
- `_stream_id` populated at insert time via VL's xxhash + "magic!" suffix algorithm, producing
  the same 48-char lowercase hex VL produces for the same `_stream` labels.
  `/select/logsql/stream_ids` was returning empty for cold rows because the external insert path
  never set the field; required by the 100% VL/VT API compatibility rule.
- Truncated service names in `/select/jaeger/api/services`: removed parquet column-index seed in
  `extractDistinctFromStats` (column-index min/max values are truncated by parquet writers at 16
  bytes per Apache Parquet PageIndex spec) and skip `detectConstantColumns` for ByteArray
  columns. Data-page scan is now the only source. Was producing
  `notification-ser`/`notification-ses` alongside `notification-service` in the services
  dropdown.
- LRU cache: `Get` returns the shared cached buffer instead of copying. Was the dominant heap
  consumer at idle (~358 MiB of transient copies on the hot path, 16 workers × ~57 files × ~2.5
  MB).
- Label-index drift on disk: `LoadLabelIndex` drops `Values` entries not accounted for in
  `ValueCounts` — one-shot sanitization for stale on-disk state from earlier buggy runs
  (truncated BYTE_ARRAY prefixes leaking into the field-values API).
- Query memory budget: per-query `MaxLiveBytes` budget + process-wide `fileBudgetSem` (256 MiB
  resident, ≤8 concurrent files) + `rgDecodeSem` (GOMAXPROCS/2 concurrent row-group decoders)
  bound peak memory inside `mem_limit=2g` for multi-day wildcards. 7-day wildcard previously
  OOM-killed the container; now completes (HTTP 200) with peak heap ~1.4 GiB and stable restart
  count. Replaced 256-deep dispatch channel with synchronous `wbMu.Lock()` writeBlock pattern
  matching VL `searchParallel`; removed row-group parallel fan-out that produced 128 concurrent
  decoders (8 rg × 16 workers).
- `debug.SetMemoryLimit` forces Go GC under the cgroup memory limit so the runtime targets RSS,
  not just Go heap.
- Helm chart bumped from 0.36.0 → next release sequence; resource defaults updated to match the
  new file-budget / live-bytes / latency-offset shape used in e2e compose.

### Added

- VT v0.9.0 fork with ExternalStorage interface — same pattern as VL fork
- VT storage adapter bridging S3/Parquet backend to VT's Jaeger and Tempo handlers
- Tempo API datasources for hot (VT disk) and cold (S3 Parquet) tiers
- Trace index in Parquet metadata for fast trace_id lookups
- Regression tests: MinTimeNs==0 sentinel handling, token bloom pipe stripping, tombstone edge
  cases
- Security tests: 29 SSRF attack vector tests for S3 endpoint validation
- Benchmarks: coalescing reader, manifest fast path, token bloom extraction, projection columns
- Parity tests: 378 tests across 21 test functions — time range, filter, pipe, stats,
  cross-validation, traces LogsQL, and full data format compatibility against VL/VT reference
- K8s scaling safety: phased shutdown orchestrator (drain → flush → persist → release) with
  per-phase timeouts
- K8s scaling safety: startup staleness detection with WAL reconciliation and cache revalidation
- K8s scaling safety: ring change detection with shadow member stabilization during scaling
  events
- K8s scaling safety: lifecycle HTTP endpoints (`/internal/lifecycle/drain`, `/ready`, `/ring`,
  `/stale`)
- K8s scaling safety: 14 new Prometheus metrics for shutdown, startup, ring change, and query
  continuity
- Helm: HPA scaleDown stabilization window, preStop drain hook, lifecycle readiness probe
- `-lakehouse.query.max-live-bytes` flag — per-query live-DataBlock byte budget (default 512
  MiB)
- `lakehouse_query_memory_budget_exceeded_total` metric — counts queries cancelled because the
  per-query budget tripped
- `internal/cache.LRU.PutNoCopy` — store buffer by reference for the S3-download hot path
- Per-component verification matrix `tests/verification/matrix.md` — 78 rows tracking every
  exposed HTTP surface (logs/traces query + insert, admin, Grafana datasources, UIs). Companion
  smoke probes `tests/verification/probe_*.sh` lock per-surface behavior:
  `probe_jaeger_search_24h.sh`, `probe_jaeger_search_24h_with_tag.sh`,
  `probe_jaeger_search_24h_full_chain.sh`, `probe_tempo_search_24h.sh`,
  `probe_logs_24h_wildcard.sh`, `probe_logs_Nday_wildcard.sh`, `probe_matrix_sweep.sh`,
  `probe_image_freshness.sh`.
- Image-freshness probe catches stale-binary deploys (compares each container image's
  `CreatedAt` to the newest source commit time).
- `_stream_id` unit tests including VL-algorithm-oracle test
  (`TestComputeStreamID_MatchesVLAlgorithm`) that re-implements VL's hash128 inline and asserts
  byte-for-byte match.
- Production-shape memory-budget integration tests:
  `TestRunQuery_ProductionShape_WildcardScalesUnderMemoryBudget` (200 files × 5000 rows) and
  `TestRunQuery_7DayProductionShape_FileBudgetBoundsPeak` (600 files × 8000 rows).
- Tempo HTTP shim test suite: `TestNormalizeTempoSearchParams` (11 cases) +
  `TestTempoTagsToTraceQL` (13 cases of the scope-prefix mapping).
- Bloom OR-in-clause regression tests: `TestS3_bloomFilterSkip_InClauseOrSemantics` (traces) +
  `TestInteg_bloomFilterSkip_InClauseOrSemantics` (logs).
- Row-group time-range page-aggregation regression tests covering out-of-order page bounds.
- Matrix sweep: 20 of the 22 `UNVERIFIED`/`DIFFER` rows in `tests/verification/matrix.md`
  flipped to `PASS` (verified upstream-equivalent against the live e2e compose stack); 2 rows
  held as `DIFFER` with VT-version notes (`T17` `/select/tempo/api/metrics/instant`, `TI2`
  `/insert/zipkin/api/v2/spans` — neither exists in VT v0.9.0). `probe_matrix_sweep.sh` is the
  regression lock: replays all 22 rows end-to-end and asserts the minimum upstream-compat
  contract per row, so any future regression on a previously-verified surface fails CI rather
  than silently flipping the matrix back to UNVERIFIED.
- `tests/verification/check_matrix_coverage.sh` + GitHub Actions `Verification Matrix Check` job
  — fails any PR that introduces a path reference in `tests/verification/matrix.md` which
  doesn't resolve to a real file on disk. Makes the matrix a self-policing contract: renaming or
  deleting a probe/test cited by the matrix now forces the matrix to be updated in the same PR.
  Scoped to in-tree directories (`tests/`, `internal/`, `cmd/`, `charts/`, `deployment/`,
  `lakehouse-traces/`, `docs/`, `scripts/`, `.github/`) — upstream VL/VT references under
  `deps/` are intentionally skipped because that directory is `.gitignore`d and only populated
  at CI build time.
- **K8s leader election test pyramid**: 15-case unit suite against an httptest-backed Lease
  server (`internal/election/k8s_test.go`), multi-candidate integration
  (`k8s_integration_test.go`), fuzz target on the state machine (`k8s_fuzz_test.go`),
  goroutine-leak guard (`k8s_leak_test.go`), opt-in 1h soak test (`k8s_soak_test.go`, build tag
  `soak`), and a regression-lock suite (`k8s_regression_test.go`) covering forbidden-import
  detection, dep-closure count, binary-size bound, FIPS round-trip, and the RenewDeadline
  liveness invariant. Coverage on `internal/election/k8s.go` measured at 90.8% (≥90% required by
  CI).
- **Real-K8s e2e for leader election**

  (`tests/e2e-k8s/test_leader_election.sh` + `.github/workflows/e2e-k8s.yaml`): spins up a
  single-node `kind` cluster, installs the LH Helm chart with 3 insert replicas, and asserts (1)
  chart renders the ServiceAccount + Role + RoleBinding with verbs
  `get,list,create,update,patch` on `coordination.k8s.io/leases`; (2) Lease object is created
  with a valid `holderIdentity`; (3) deleting the leader pod triggers a successor within
  `LeaseDuration + RenewDeadline = 40s`; (4) **negative control**: deleting the RoleBinding
  causes leader election to fail loudly with a 403 in the logs (proves the chart's RBAC is
  load-bearing); (5) two releases in different namespaces hold independent leases without
  cross-talk. Path-filtered to run only on PRs that touch election code, the chart, or
  Dockerfiles — total CI budget ~4 minutes.
- **E2E verification probes**: `probe_k8s_election_failover.sh` (httptest-based failover smoke),
  `probe_fips_active.sh` (FIPS round-trip lock), `probe_binary_size.sh` (≤40 MB ceiling for both
  binaries), `probe_image_size.sh` (≤70 MB ceiling for the distroless image). Coverage gate: CI
  `test-logs` and `test-traces` jobs now error on any `internal/<pkg>` package below 90%
  coverage.
- **Helm chart RBAC**: extends `coordination.k8s.io/leases` verbs from `get,create,update` to
  `get,list,create,update,patch` — covers `kubectl describe lease` for operators (list) and the
  elector's release-on-stop best-effort patch (patch). The new verbs are load-bearing per the
  kind e2e's negative test.
- **`internal/election/README.md` + `RUNBOOK.md`**: design doc with the allowed import surface,
  state machine diagram, coordination guarantees, and a 5-case operator triage runbook (no
  leader, 403 Forbidden, leader flapping, two leaders, lease never created).

### Changed

- Reduced `lakehouse-logs` / `lakehouse-traces` binary size from **55 MB to ~37 MB** (-18 MB,
  -33%) by replacing `k8s.io/client-go`'s heavy elector closure (full `kubernetes` clientset +
  `tools/leaderelection` + typed API modules) with a hand-rolled `rest+meta/v1` REST client in
  `internal/election/k8s.go`. K8s leader election is now always available (no build tag); the
  dep closure shrinks from ~700 to 329 packages. Adds `-trimpath` for reproducible builds. The
  earlier build-tag-gated approach was iterated to Option B so operators don't have to rebuild
  for K8s support. `TestNoForbiddenImports` regression-locks the closure against re-introducing
  `k8s.io/client-go/kubernetes`, `tools/leaderelection`, or the heavy typed-API modules.
- **Container image**: dropped the standalone `/usr/local/bin/healthcheck` binary (~3 MB) and
  folded the probe into a `lakehouse-{logs,traces} healthcheck [URL]` subcommand. The
  Dockerfile's HEALTHCHECK and docker-compose health probes call the main binary directly. Saves
  ~10% of image size and one fewer binary to maintain.
- **FIPS image variant**: added `--build-arg FIPS=1` (sets `GOFIPS140=v1.0.0`) to both
  `Dockerfile.logs` and `Dockerfile.traces`. Release workflow publishes a 2x2 matrix per
  release: `{logs,traces} × {default, FIPS}`. Tags: `:vX.Y.Z`, `:latest`, `:vX.Y.Z-fips`,
  `:latest-fips`. New `lakehouse-{logs,traces} fips-status` subcommand reports `fips140:
  enabled|disabled` for operability. FIPS mode also requires `GODEBUG=fips140=on` at runtime per
  Go 1.26 semantics.
- **Release workflow**: zstd compression on image layers + OCI media types (`--output
  type=image,compression=zstd,compression-level=3,oci-mediatypes=true,push=true`) —
  registry-side bytes shrink another ~25% over the gzip default. Optional Docker Hub mirroring
  gated on the `DOCKERHUB_USERNAME` secret being set (so forks don't fail on missing creds).
- Replace custom Jaeger handlers with VT upstream `jaeger.RequestHandler` (deleted 2451 lines)
- Deduplicate `storage.Storage` interface — traces module imports from root
- Bump VictoriaTraces hot tier from v0.8.2 to v0.9.0
- Bump loki-vl-proxy from v1.43.0 to v1.50.1
- `lakehouse.query.max-files-per-query` default flipped from 500 to **0 (unlimited)** — matches
  VL upstream which has no such cap. Memory budget (`query.max-live-bytes`, `fileBudgetSem`,
  `rgDecodeSem`) is the real safety net.
- `-search.latencyOffset` on lakehouse-traces set to 2m (matches `insert.flush-interval=120s`)
  so the upstream Jaeger search expansion loop finds freshly-flushed cold-tier spans.
- e2e compose: `mem_limit: 2g` + `restart: on-failure` on both LH containers (Docker-level
  safety net), `file-workers=16`, L1 cache 512→256 MiB to leave headroom for the file budget.
- Grafana datasource: Lakehouse Logs Cold derivedField now routes trace_id clicks to
  `victoriatraces-global` (hot+cold fan-out) instead of `victoria-lakehouse-traces` (cold only)
  so fresh trace_ids that haven't flushed to S3 yet still resolve via the hot tier instead of
  returning HTTP 404.

### CI

- `auto-release.yaml` workflow now clones + patches VictoriaTraces v0.9.0 into
  `lakehouse-traces/deps/VictoriaTraces/` before the build step. Prior releases failed with
  `replacement directory ./deps/VictoriaTraces does not exist` because the VT clone/patch was
  only wired into `ci.yaml`, not the release workflow.
- Release skip-regex extended to include `tests/verification/` (matrix.md + probe scripts) and
  design-document paths so verification-only and design-only PRs no longer trigger a no-op
  release.

## [0.36.0] - 2026-05-25

### Fixed

- Smart cache: snapshot loader now correctly handles legacy (pre-envelope) format on upgrade
- Smart cache: watermark-based LRU eviction when disk usage exceeds 90% of configured limit
- Smart cache: reconciliation uses file mtime for CreatedAt instead of current time
- Tenant name mapping: MetricLabel "both" format now includes numeric ID prefix (42:3/name)
- Tenant name mapping: OrgID validation on S3 alias load rejects invalid entries
- Tenant name mapping: persistence errors are now logged instead of silently discarded
- Partitioned bloom: manifest metadata updated after successful bloom persist

## [0.35.0] - 2026-05-25

### Performance

- Benchmark: increase trace datagen volume from 20K to 50K spans for more representative
  optimization measurements

## [0.34.0] - 2026-05-24

### Performance

**S3 I/O Layer Optimization (Phases A-J):**

- Read-ahead buffer: 256KB streaming buffer reduces small S3 reads by batching sequential access
- Range coalescing: merges nearby column ranges within 64KB gap tolerance into single S3
  requests
- Transport tuning: configurable HTTP/2 concurrency, idle connections, and response header
  timeouts
- Async row group prefetch: background goroutine pre-fetches next row group while current
  processes
- Compaction: merges small Parquet files into target 256MB blocks to reduce per-file S3 overhead
- Streaming aggregation: single-pass count/sum/min/max avoids full materialization for simple
  aggregates
- Cache-partitioned reads: AZ-aware partition modes (az-local/global/distributed) route cache
  lookups
- Cache maximization: column-level chunk caching, scan pollution protection, LRU with L2
  spilling
- Distributed compaction: CRC32 partition sharding with K8s StatefulSet auto-detection
- Select tier: self-filtering in RunQuery for hybrid fan-out, health-aware ring with failure
  tracking

### Added

- Column popularity tracking for adaptive prefetch decisions
- Write-through cache on ingest flush for immediate read availability
- QuerySpecificFiles method for gap redistribution across select nodes
- Cache-aware file ordering to maximize cache hits during query execution
- VL/VT parity test suite: 218 tests across 16 test functions validating all LogsQL endpoints,
  43 filter types, 38 pipe operations, 23 stats functions, cross-validation invariants, edge
  cases, and traces Jaeger API against VictoriaLogs/VictoriaTraces as reference implementation

### Fixed

- Traces field prefix: VT metadata fields (kind, flags, dropped_*_count, start_time_unix_nano)
  emitted without span_attr: prefix to match VT behavior
- Traces index entries: filter VT internal rows (trace_id_idx, service_graph) at insert time via
  isVTIndexRow()
- Unused wrapVLTimestampOnly removed from traces select handler

## [0.32.0] - 2026-05-23

## [0.31.0] - 2026-05-22

### Fixed

- Map column reading: properly reconstruct key-value pairs from Parquet MAP columns
  (resource.attributes, span.attributes, log.attributes, scope.attributes) and expand into
  VL-compatible attribute columns (resource_attr:*, span_attr:*, log_attr:*, scope_attr:*)
- Column projection: only activate for column-selecting pipes (fields, stats, uniq, top) rather
  than VL-internal pipes (sort, limit, offset) that VL adds automatically to queries

## [0.30.0] - 2026-05-21

### Performance

**VL/VT-inspired query optimizations (8 techniques, zero Parquet format changes):**

- Column-type-aware push-down: numeric columns use native int64 comparisons for row group
  statistics pruning instead of lexicographic string comparisons
- Constant column optimization: detects columns where min == max across all pages and skips
  deserialization, injecting the constant value directly
- Label-based file pre-filtering: evaluates query predicates (exact, prefix, GT, LT) against
  manifest-level labels to skip files before S3 download
- Dictionary page filtering: reads dictionary pages for exact-match/prefix predicates; skips row
  groups when no dictionary entry matches
- Bitmap-based pre-where filtering: reads filter columns first, builds boolean bitmap, then
  reads remaining columns only for matching rows
- Parallel row group scheduling: sorts by estimated cost (row count ascending), processes up to
  3 in parallel per file
- Pre-resolved column indices: resolves column indices once per file, reuses across all row
  groups
- Trace parent-child prefetching (traces): smart cache reverse index from trace ID → file keys
  for instant file narrowing

**Infrastructure optimizations:**

- Parquet footer LRU cache (10K entries) avoids re-parsing file metadata on repeated accesses
- Write lock optimization moves filtering out of serialized mutex, reducing contention
- Parallel row group processing (up to 3 goroutines per file) reduces per-file latency
- Timestamp-only projection for hits/stats/stats_range endpoints via context hint
- Cache warmup on startup pre-fetches recent partitions into L1/L2 and footer cache
- S3 range read capability (DownloadRange with HTTP Range header)
- Tightened all 21 benchmark targets by 25-30% reflecting combined optimization impact

## [0.29.0] - 2026-05-20

### Performance
- Concurrent query benchmark: validates latency targets at 1/10/50/100 parallel queries with
  mixed endpoint types
- Mixed read/write benchmark: measures mutual interference with ≤20% degradation target
- Config sweep script for automated `max_concurrent` / `file_workers` tuning validation
- Deployment-size recommendations for query concurrency settings

## [0.28.1] - 2026-05-20

### Fixed
- Fix errcheck lint failure on `rows.Close()` in projected reader (logs and traces modules)

## [0.28.0] - 2026-05-20

### Added
- OTEL tracing: HTTP, query, and insert paths instrumented with OpenTelemetry spans
- Benchmark CLI (`cmd/bench`): seed data and measure cold/warm/hot query latency with baseline
  JSON output

### Performance
- Manifest `GetFilesForRange` uses sorted partition index with binary search (O(log P) vs O(P))
- Column projection pushdown: queries reading 2-3 fields skip deserializing unused columns
- Traces module: added pushdown filter parity with logs module for row group stats pruning
- Expanded bloom index coverage: `host.name`, `k8s.namespace.name`, `k8s.pod.name`,
  `k8s.deployment.name`, `deployment.environment`, `span.name` (traces)
- Bloom index supports `in()` operator for multi-value exact match queries

## [0.27.2] - 2026-05-20

### Added
- Phase 0 correctness gate: golden file test infrastructure, verification tests for all output
  surfaces (LogsQL, Jaeger, insert, metrics, stats, manifest, schema), E2E regression suite,
  Helm chart template tests, architecture and performance documentation

## [0.27.1] - 2026-05-20

### Added
- Per-tenant observability: string-based tenant support for logs and traces, per-tenant stats
  API, enhanced tenants dashboard with row count accuracy
- Query performance optimization design spec (Phase 0–3 roadmap)

### Fixed
- Normalize millisecond epoch timestamps for VL hits endpoint
- Label filter false negatives on high-cardinality fields with bloom index coverage

### Changed
- Update loki-vl-proxy to v1.36.0

## [0.27.0] - 2026-05-19

### Added
- Settings profiles: 5 named presets (balanced, max-performance, max-durability,
  max-cost-savings, dev) with three-level hierarchy (global → per-signal → per-role), Helm chart
  integration via `coalesce` resolution, JSON schema validation, and comprehensive profile
  integration tests

## [0.26.0] - 2026-05-19

### Added
- Bloom age-tiering: 4-tier model (hot/warm/cold/archive) with configurable boundaries, tier
  downgrade logic (per-RG → per-file → summary → none), Filter.MergeFrom bitwise OR merge,
  SHA256 integrity checks
- PartitionedIndex: per-partition bloom management with dirty tracking, hourly/daily
  granularity, high-cardinality skip gate (>50K)
- BloomCache: LRU-cached bloom index access with lazy loading, size-based eviction, warm preload
- Bloom build on flush: automatic bloom population from trace_id and service.name when parquet
  files are written to S3
- Bloom persist: dirty partitions written to S3 as `_bloom.bin` after each flush cycle
- BloomFilterFiles: query path integration between label filtering and file workers for
  partition-level bloom skip
- Manifest PartitionMeta: per-partition bloom availability, size, and column tracking
- MetadataCompactor: automatic bloom tier transitions (hot→warm→cold→archive) with S3 persist
  callback
- BloomRebuilder: post-compaction bloom rebuild hook on existing Compactor
- TTL recompression: age-based compression levels (ZSTD 3/7/17) for hot/warm/cold data
- BloomController: auto-tuning of bloom parameters based on file rate, SSD usage, and cache
  metrics with operator pin overrides
- ConfigSync: S3-based live configuration with read/write, error tracking, and last-known
  fallback
- Bloom status API: GET /api/v1/bloom/status with tier stats, cache stats, auto-tuning state
- Cost projection engine: per-tier storage cost analysis with S3 class mapping
  (STANDARD/IA/GLACIER)
- 12 bloom-specific Prometheus metrics (build, query, tier transitions, controller adjustments)
- PREWHERE concept tests for column-selective reads and row group stats elimination
- Comprehensive bloom test suite: 132+ unit tests, 30+ integration tests, 4 E2E smoke tests, 10
  regression tests

### Changed
- Traces binary insert path rewritten to use VL upstream vlinsert handlers (same adapter pattern
  as logs)
- Automate `deps-traces` in Makefile — clones VL at commit a408207c2242 and applies patches (was
  manual)
- Fix `build-traces` Makefile target to build from correct Go module directory
- `make test` uses `-short` to skip real data benchmarks; `make test-full` for full suite with
  10m timeout
- Buffer handler hardened: Bearer auth required when configured, GET-only method restriction,
  stream tag trailing data validation

## [0.24.0] - 2026-05-16

### Added
- Tenant name mapping — bidirectional alias system (X-Scope-OrgID ↔ integer TenantID) with O(1)
  sync.Map lookups, Loki/Tempo charset validation, HTTP middleware, CRUD API, S3 persistence,
  fleet sync, and configurable Prometheus metrics format
- WAL implementation — file-based write-ahead log with gob encoding, crash recovery, truncation,
  and size tracking for insert path durability
- Parquet MAP columns — LogAttributes, ResourceAttributes, SpanAttributes, and ScopeAttributes
  stored as native Parquet MAP type columns
- LogRow.SeverityNumber field for severity-based log filtering
- TraceRow.StartTimeUnixNano field for trace span start time queries
- Stats API tenant name decoration on cost and compression endpoints

### Changed
- Replace custom insert handler with VL's upstream `vlinsert` handlers via
  `insertutil.SetLogRowsStorage()` adapter — same pattern as select path's
  `vlstorage.SetExternalStorage()`
- Full VL insert protocol parity: jsonline, Loki JSON+protobuf, ES bulk, syslog, journald,
  Datadog, OTLP, Splunk, native insert (previously only jsonline, Loki JSON, ES bulk)
- Extract buffer handler to `internal/buffer` package (from `internal/insertapi`)
- Logs binary no longer uses custom insert parsing — all protocol handling by VL upstream
- Traces binary insert path rewritten to use VL upstream vlinsert handlers (same adapter pattern
  as logs)
- Automate `deps-traces` in Makefile — clones VL at commit a408207c2242 and applies patches (was
  manual)
- Fix `build-traces` Makefile target to build from correct Go module directory
- `make test` uses `-short` to skip real data benchmarks; `make test-full` for full suite with
  10m timeout
- Buffer handler hardened: Bearer auth required when configured, GET-only method restriction,
  stream tag trailing data validation
- Apply `gofmt -s` simplifications across all Go files in both modules
- Enable gofmt, gocyclo, and misspell linters in golangci-lint v2 configs
- Add standalone `gofmt -s` check and Go Report Card badge to CI
- Treat govulncheck and helm lint warnings as CI failures
- Extract startStatsLoops from run() to reduce cyclomatic complexity

## [0.23.1] - 2026-05-14

### Fixed
- E2E datagen trace-log correlation — traces now generated first with 70% of logs sharing trace
  IDs, span IDs, and service context for realistic cross-signal testing
- Grafana datasource log→trace links — added `derivedFields` to all VictoriaLogs and Loki
  datasources with `trace_id=(\w+)` regex linking to Jaeger trace views
- ClickHouse otel_logs view promoted fields — moved service.name, k8s.*, etc. into LogAttributes
  map only (removed ResourceAttributes duplication)
- Bump loki-vl-proxy to v1.33.0

## [0.23.0] - 2026-05-14

### Added
- AZ auto-detection at startup with fallback chain (env var → AWS IMDSv2 → GCP metadata → K8s
  node label API)
- AZ-aware peer cache routing — consistent hash ring maintains same-AZ sub-ring, prefers same-AZ
  peers for L3 cache lookups
- AZ-aware buffer bridge — select pods prefer same-AZ insert pods for `/internal/buffer/query`
  fan-out
- Preferred vs strict AZ modes with configurable `az_min_peers_per_az` threshold
- AZ metrics: `lakehouse_peer_same_az_members`, `lakehouse_peer_cross_az_members`,
  `lakehouse_peer_az_requests_total`, `lakehouse_buffer_bridge_az_requests_total`
- Peer AZ reporting via `/internal/cache/stats` endpoint (`"az"` field in JSON response)
- Default topology spread constraints in Helm chart for even AZ distribution
- NODE_NAME env injection in Helm templates for K8s API AZ detection fallback
- 8 fuzz test targets across azdetect, peercache, config, and storage packages
- 60+ edge case tests for AZ components (K8s labels, ring wrap-around, concurrent access,
  special chars)
- Cross-AZ cost optimization guide with AutoMQ comparison and industry case studies
- Mermaid diagrams added to 14 documentation pages
- 4 new website landing pages: ingestion-formats, query-interfaces, loki-tempo-alternative,
  multi-tenant-observability
- SEO: OpenGraph meta tags, JSON-LD structured data, 25+ keywords, Twitter card metadata
- Docusaurus sidebar: added 9 docs, navbar dropdown menus for Use Cases and Integrations

### Fixed
- Peer AZ discovery auth header mismatch — `fetchPeerAZ()` used `Authorization: Bearer` but
  handler expects `X-Peer-Auth-Key`, causing silent failure when auth configured
- Data race in `/internal/cache/stats` endpoint — `ServeHTTP` read `selfAZ` without lock while
  `SetSelfAZ` writes with lock
- Invalid JSON in stats endpoint for non-UTF8 AZ names — `%q` format produces Go-style escapes
  not valid in JSON, switched to `json.Marshal`
- `mergeConfig()` missed boolean fields `Peer.AZAware`, `Peer.CrossAZFallback`,
  `Select.AZAware`, `Select.CrossAZFallback` — config overlay could not enable these

## [0.22.0] - 2026-05-13

### Added
- Tenant stats & storage metrics — `StatsConfig` (15 fields) and `UIConfig` (4 fields) config
  structs, `KnownTenant` for bucket-isolation cold discovery with per-tenant lifecycle/pricing
  overrides
- Per-tenant Prometheus metrics — 8 metrics (`lakehouse_tenant_files`, `_bytes`, `_raw_bytes`,
  `_rows_total`, `_ingestion_bytes_total`, `_queries_total`, `_last_write_timestamp`,
  `_last_query_timestamp`) with configurable cardinality cap
- Global storage metrics — 14 metrics (`lakehouse_storage_files_total`, `_bytes_total`,
  `_compression_ratio`, `_cost_monthly_usd`, `_bytes_by_class`, etc.) for fleet-wide storage
  visibility
- Cardinality limiter meta-metrics — `lakehouse_metrics_cardinality_limit`, `_tracked`,
  `_overflow_total`
- Stats sync metrics — 7 metrics for peer delta broadcast, S3 snapshots, CRDT merges, HeadObject
  verification
- `GaugeVec` and `FloatGaugeVec` metric helper types for per-label gauge tracking
- Helm chart updates — `lakehouseConfig.stats.*` (15 fields), `lakehouseConfig.ui.*` (4 fields),
  complete `lakehouseConfig.tenant.*` (isolation, bucket_template, known_tenants with
  lifecycle/pricing overrides)
- Tenant stats documentation (`docs/tenant-stats.md`) — 7 JSON API endpoints, CRDT fleet sync,
  storage class tracking, cost estimation, all metrics reference
- Lakehouse Explorer UI documentation (`docs/lakehouse-explorer.md`) — 3-tab Preact+uPlot
  dashboard (Storage Overview, Tenants, Cardinality), VMUI tab injection
- Updated observability docs with tenant, storage, cardinality, and stats sync metric tables
- Updated multi-tenancy docs with tenant stats, monitoring, cost allocation sections
- Updated configuration docs with stats, UI, and tenant config examples
- Updated README — tenant stats in Key Features, Observability section, Configuration section,
  Documentation navigation

### Fixed
- Auto-release workflow `[skip release]` check now examines only commit title instead of entire
  multiline message — squash-merged PRs with `[skip release]` in body paragraphs no longer
  incorrectly skip releases
- Lint/gosec/CodeQL warnings — unhandled `w.Write()` errors in VMUI inject, unchecked
  `json.Unmarshal` in stats regression tests, unused Preact `h` import, unused `getCPUTime`
  function, redundant nil check
- VMUI regression test skips missing build assets (favicon.svg, config.json) in CI instead of
  failing
- Bloom columns test expectations updated to match actual defaults (`[service.name, trace_id]`
  for logs)

## [0.21.0] - 2026-05-13

### Added
- Schema-driven FieldType system — centralized type-aware formatting for all Parquet column
  types (TypeTimestampNano, TypeInt32, TypeInt64, TypeFloat64, TypeBool, TypeString).
  `FormatValue()` on each type replaces scattered `fmt.Sprintf`/`time.Format` calls across all
  query paths (RunQuery, GetFieldNames, GetFieldValues, buffer reads). `ParseFieldType()`
  enables typed ExtraPromoted columns via config.
- `FormatField(internalName, value)` registry method for one-call schema-driven formatting in
  all read paths
- Architecture documentation with mermaid diagrams — cache architecture (L1→L2→L3→S3 tiers,
  SmartCache controller, eviction, prefetch, cross-signal, sizing), manifest system (structure,
  sync, persistence, API), storage & Parquet flow (end-to-end write/read paths, VL adapter,
  schema registry)
- CodeQL configuration to exclude vendored VictoriaLogs code from security scanning

### Fixed
- Jaeger test `TestHandleJaegerTrace_ScopeAttrAsSpanTag` assertion — handler strips
  `scope_attr:` prefix from tag keys, test now expects `lib.version` instead of
  `scope_attr:lib.version`
- ClickHouse OTEL views — `ScopeAttributes` was `Map(Nothing, Nothing)`, now `Map(String,
  String)` via typed CAST; Events/Links arrays were `Array(Nothing)`, now properly typed
  (`Array(DateTime64(9))`, `Array(String)`, `Array(Map(String, String))`); removed non-standard
  `LogStreamId` from `otel_logs`; added `TraceFlags`, `ResourceSchemaUrl`, `ScopeSchemaUrl`
  columns; traces Duration now in nanoseconds (OTEL standard) instead of milliseconds; empty
  promoted fields filtered via `mapFilter` to avoid clutter in ResourceAttributes/SpanAttributes
- Datagen now populates `ResourceAttributes` MAP column for logs (with `service.version`,
  `telemetry.sdk.name`) and `ResourceAttributes`, `SpanAttributes`, `ScopeAttributes` MAP
  columns for traces
- Grafana ClickHouse datasource config — added `logsLevelField: SeverityText`,
  `tracesDurationUnit: ns`, `tracesSpanKindField`, `tracesTraceStateField` for proper OTEL
  auto-discovery

### Changed
- Datagen seed volume increased — 10K logs + 2K traces over 72h (was 5K + 1K over 48h) to better
  populate both hot (disk 24h) and cold (S3 lakehouse) tiers
- Tenant1 seed increased — 2K logs + 500 traces over 72h (was 1K + 200 over 48h)

## [0.20.0] - 2026-05-12

### Added
- Multi-tenancy — single binary serves all tenants via header-based routing with S3 prefix
  isolation (`{AccountID}/{ProjectID}/`, default `0/0/`), matching Grafana Loki/Tempo pattern.
  Enterprise option for bucket-per-tenant isolation with separate IAM policies
- Global read mode — optional `--lakehouse.tenant.global-read-header` /
  `--lakehouse.tenant.global-read-value` for admin dashboards that query across all tenants
  (disabled by default, explicit opt-in)
- Analytics engines documentation — comprehensive guide covering 9 Parquet engines (DuckDB,
  ClickHouse, Trino, Databricks, Snowflake, StarRocks, Doris, Spark, pandas) with Grafana
  datasource status, query examples, and integration guides
- Tenant configuration flags — `--lakehouse.tenant.isolation` (prefix/bucket),
  `--lakehouse.tenant.bucket-template`, `--lakehouse.tenant.default-account`,
  `--lakehouse.tenant.default-project`, `--lakehouse.tenant.header-account`,
  `--lakehouse.tenant.header-project`, `--lakehouse.tenant.global-read-header`,
  `--lakehouse.tenant.global-read-value`
- Multi-level select architecture — vlselect/vtselect fan out queries to both hot (disk) and
  cold (lakehouse S3) storage nodes for unified hot+cold results
- VictoriaTraces hot tier in Docker Compose — standalone VT instance with 24h disk retention
- Datagen trace dual-write — `--vt-endpoint` flag pushes traces to VictoriaTraces via Zipkin
  `/api/v2/spans` alongside S3 Parquet writes
- Eleven Grafana datasources — Global VL/VT (via vlselect/vtselect), Hot VL/VT (direct disk),
  Cold logs/traces (lakehouse S3), Loki proxy (hot+cold), DuckDB analytics, ClickHouse
  analytics/logs/traces
- DuckDB Grafana datasource — in-memory DuckDB with `httpfs` extension for direct SQL on S3
  Parquet files via `read_parquet()`
- ClickHouse analytics engine — pre-configured with `lakehouse.logs` and `lakehouse.traces`
  views querying MinIO Parquet via `s3()` table function, with dedicated Grafana Logs and Traces
  datasources for native log/trace panel visualization on raw Parquet
- ClickHouse OTEL-compatible views — `lakehouse.otel_logs` and `lakehouse.otel_traces` map
  Parquet columns to OpenTelemetry standard naming (Timestamp, Body, SeverityText, ServiceName,
  TraceId, SpanName, SpanKind, Duration, StatusCode, ResourceAttributes, SpanAttributes)
- Tenant-scoped ClickHouse views — `logs_tenant_default`, `traces_tenant_default`,
  `logs_tenant_test`, `traces_tenant_test` with direct s3() glob patterns per tenant
  (workaround: `_file` virtual column unavailable through view chain)
- Raw ClickHouse views — `lakehouse.logs_raw` and `lakehouse.traces_raw` with explicit Parquet
  schema for ad-hoc SQL analytics without needing files at view creation time
- Grafana ClickHouse datasources preconfigured with OTEL mode (`otelEnabled: true`,
  `otelVersion: latest`), default tables (`otel_logs`, `otel_traces`, `logs_raw`), and
  bidirectional logs↔traces cross-linking via `tracesToLogsV2`
- Expanded datagen `_stream` labels from 2 to 5 — added `k8s.deployment.name`,
  `deployment.environment`, `cloud.region` for full Loki label filtering support
- Multi-tenancy E2E tests and CI workflow
- Gitleaks allowlist (`.gitleaks.toml`) for false positives in documentation and test example
  values
- Loki-VL-proxy Dockerfile — builds from GitHub release binary instead of non-existent GHCR
  image
- Architecture diagram in Docker Compose docs showing full data flow across all tiers

### Fixed
- Tenant-aware S3 prefix resolution — `TenantConfig.ResolvedPrefix()` and updated `AutoPrefix()`
  prepend `{AccountID}/{ProjectID}/` to signal prefix (e.g. `0/0/logs/` instead of `logs/`)
- E2E test params — add missing `step` for /hits, `query` for /field_names and /field_values,
  stats pipe syntax for /stats_query
- Datagen Dockerfile — add `GOWORK=off` to prevent go.work from pulling in lakehouse-traces
  module dependencies
- Auto-release workflow — remove auto-merge (repo setting not enabled), just create PR for
  manual merge
- Remove broken DuckDB plugin init container (v0.4.1 release has no downloadable assets)

### Changed
- Docker Compose hot tier retention reduced from 7d to 24h to match cold boundary
- Grafana default datasource changed to VictoriaLogs Global (via vlselect) for unified hot+cold
  queries
- Grafana image changed from Alpine to Ubuntu (`grafana/grafana:latest-ubuntu`) — required for
  DuckDB plugin (glibc dependency)
- Grafana ClickHouse datasource names explicitly show S3 Parquet origin
- `_stream_fields` in VL NDJSON push updated to match expanded 5-label _stream

## [0.18.2] - 2026-05-12

### Fixed
- Fix Jaeger trace search returning null data — use VT-canonical field names
  (`"resource_attr:service.name"`, `name`, `duration`) with LogsQL quoting for colon-containing
  fields
- Fix loki-vl-proxy hot+cold routing — VictoriaLogs serves hot data (<24h), lakehouse-logs
  serves cold data via `-cold-enabled` with 1h overlap
- Add `external_query.go` patch to auto-release workflow — fixes binary build failure
  (`undefined: logstorage.QueryHasPipes`)
- Update e2e compose loki-vl-proxy from broken local build path to published GHCR image v1.31.2
- Format `_time` column as RFC3339Nano instead of raw nanoseconds — fixes VL handler timestamp
  parsing for all query endpoints
- Recover from `writeBlock` panics caused by unsupported VL pipe processors (e.g.
  `CountByTimePipe` in `/hits`) — prevents query crashes, returns partial results instead
- Add `filter.go` to traces module for metadata filter scoping — traces
  `GetFieldNames`/`GetFieldValues` now correctly apply LogsQL filters
- Apply LogsQL filter scope to metadata endpoints (`GetFieldNames`, `GetFieldValues`,
  `GetStreamFieldNames`, `GetStreamFieldValues`) — previously returned unfiltered results

### Changed
- Replace custom LogsQL filter parser with VL's native `Filter.MatchRow()` — full LogsQL parity
  including OR, AND, NOT, regex, ranges, case-insensitive matching, and all filter types VL
  supports
- Apply LogsQL filter evaluation in traces `RunQuery` (was missing) — traces now filter rows
  same as logs module
- Apply `filter` substring parameter in vlstorage adapter for `GetFieldNames`, `GetFieldValues`,
  `GetStreamFieldNames`, `GetStreamFieldValues` — was previously ignored, now matches VL
  behavior
- Improve loki-vl-proxy config for Grafana Loki Drilldown — switch to translated metadata mode,
  add structured metadata emission, expand stream fields (12 labels), add derived fields for
  trace-to-logs linking, enable patterns autodetect and label values indexed cache
- Split LOC badge into separate prod code and test code badges
- Add `GOWORK=off` to Makefile — prevents build failures from incompatible VL versions across
  modules

## [0.18.1] - 2026-05-11

### Added
- **Smart cache controller** — unified cache orchestrator wrapping L1 (memory), L2 (disk), L3
  (peer), L4 (S3) with configurable TTL, hot access detection, pin tracking, and singleflight S3
  deduplication (`internal/smartcache/`)
- **Cross-signal prefetch** — bidirectional hints between `lakehouse-logs` and
  `lakehouse-traces` deployments via HTTP (`/internal/prefetch/hint`,
  `/internal/cache/evict-hint`). Logs query for `service=checkout` automatically warms trace
  data for same time window, and vice versa (`internal/crosssignal/`)
- **LogsQL filter evaluation** — post-scan field matchers (exact, substring, regex, NOT) applied
  to DataBlock rows in RunQuery, ensuring cold queries respect LogsQL semantics
  (`internal/storage/parquets3/filter.go`)
- **max_rows enforcement** — `query.max_rows` (default 10M) caps emitted rows per query via
  atomic counter, preventing unbounded cold-query resource usage
- **Internal endpoint auth** — `/internal/cache/clear` and `/internal/cache/stats` require
  Bearer token (`peer.auth_key`) when configured, matching `/internal/manifest/update` pattern
- **Prefetch engine wiring** — cross-signal handler now creates and uses a `prefetch.Engine` to
  process incoming prefetch hints (was nil/inert)
- **Parallel query file workers** — configurable bounded worker pool for concurrent Parquet file
  processing during queries, replacing sequential file scanning (`query.file_workers`, default
  8)
- **Cache sizing calculator** — adaptive cache budget estimation blending ingestion rate (early)
  and query pattern analysis (after 12h), with per-node fleet division
  (`internal/smartcache/sizing.go`)
- **Active query pinning** — files used by in-flight queries are pinned in cache with
  configurable grace period, preventing eviction under pressure
- **Connected data eviction** — trace IDs extracted from query results enable cross-signal cache
  deprioritization when traces are evicted
- **Hint batching** — cross-signal client accumulates trace ID hints and flushes on interval or
  batch size threshold, reducing HTTP overhead
- **Smart cache metrics** — 15 new Prometheus metrics: hit ratio, entries, bytes used/limit,
  evictions by reason, hot/pinned/owned entries, effective bytes, prefetch hit ratio, coverage
  hours
- **Cross-signal metrics** — 6 new metrics: eviction sent/received/pending/applied, prefetch
  sent/received
- Smart cache snapshot persistence — periodic metadata snapshots to disk for fast cache warmup
  on restart
- Smart cache eviction loop — background TTL enforcement with hot access detection and pin
  protection

### Changed
- `getFileData()` in storage now routes through SmartCacheController when available, with
  fallback to original L1→L2→L3→S3 chain
- `RunQuery` wraps `writeBlock` callback with filter evaluation, tombstone filtering, and
  max_rows enforcement before passing to caller
- `RunQuery` uses parallel file worker pool instead of sequential processing
- `queryFile` extracts trace IDs from result DataBlocks for prefetch and cross-signal hints
- Both `lakehouse-logs` and `lakehouse-traces` binaries wire up cross-signal handlers with
  active prefetch engine, eviction loop, and snapshot persistence
- Auto-release workflow now auto-merges metadata PRs to prevent version drift

## [0.17.0] - 2026-05-11

### Added
- Query rate limiting via `MaxConcurrent` semaphore — returns HTTP 429 when at capacity
- S3 retry with exponential backoff for all S3 operations (`ReadAt`, `Upload`, `Download`,
  `Delete`, `Exists`)
- Context propagation in S3 reader (replaces `context.TODO()`)
- Per-operation S3 metrics (requests, duration, errors, bytes read)
- Slow query logging with configurable threshold and query duration histograms
- VL/VT integration stubs: `GetStreamIDs`, `GetTenantIDs`, delete dispatch
  (`DeleteRunTask`/`DeleteStopTask`/`DeleteActiveTasks`)
- Tests: s3reader (Upload/Download/Delete/Exists), election (S3/K8s/auto), Jaeger handlers,
  selectapi, vlstorage adapters, S3 retry (+112 tests)
- Helm: `NOTES.txt` post-install guidance, `NetworkPolicy` template, `values.schema.json`
  validation
- CI: golangci-lint v2 config, Dependabot for Go/Actions/Docker, hardened security workflow
- Project logo

### Changed
- Replace custom `internalselect` handler (~960 lines) with VL's built-in `RequestHandler` for
  both modules
- Split `parquets3/storage.go` (1,383 lines) into `storage_query.go` and `storage_fields.go`
- Extract Jaeger handlers (~560 lines) from `handler.go` into dedicated `jaeger.go`

### Removed
- Dead code: empty `UpdatePerQueryStatsMetrics()`, unused `CircuitBreakerConfig`,
  `S3CircuitBreakerState` metric

### Fixed
- Replace custom internalselect encoding with VL's actual wire format — fixes vlselect panics
  (`growslice: len out of range`) caused by 4-byte uint32 block lengths instead of 8-byte uint64
- Add `internal/vlstorage/` thin dispatch layer bridging `storage.Storage` to VL's vlstorage
  function signatures (both logs and traces)
- Remove protocol-incompatible vlselect service from E2E compose
- Remove orphaned vlselect Grafana datasource pointing to removed service
- Fix traces-to-logs datasource uid reference (`victoria-lakehouse-logs` →
  `victoria-lakehouse-cold`)
- Delete dead `internal/protocol/` package in both logs and traces modules (replaced by VL
  encoding in #28)

### Architecture
- Split into two separate binaries: `lakehouse-logs` and `lakehouse-traces`
- Each binary has its own Go module with independent VL dependency versions
- Logs pins to VL v1.50.0, Traces pins to VL commit a408207c2242 (VT v0.8.2 compatible)
- Removed unified `cmd/lakehouse/` binary and `--lakehouse.mode` flag — mode is hardcoded per
  binary

### Logs (`lakehouse-logs`)
- Separate Dockerfile (`Dockerfile.logs`), Docker image (`ghcr.io/.../lakehouse-logs`)
- Default port `:9428`, bloom columns: `[service.name]`
- Delete API at `/delete/logsql/*`
- Mode-specific config section: `logs:` in YAML, `--lakehouse.logs.*` flags

### Traces (`lakehouse-traces`)
- Separate Go module (`lakehouse-traces/go.mod`) with VT-compatible VL dependency
- Separate Dockerfile (`Dockerfile.traces`), Docker image (`ghcr.io/.../lakehouse-traces`)
- Default port `:10428`, bloom columns: `[trace_id, service.name]`
- Delete API at `/delete/tracessql/*`
- Jaeger gRPC support: `--lakehouse.traces.jaeger-enabled`,
  `--lakehouse.traces.jaeger-grpc-addr`
- Mode-specific config section: `traces:` in YAML, `--lakehouse.traces.*` flags

### Shared
- Mode-specific config extension points (`logs:` / `traces:` sections) with accessor methods
  (`ActiveBloomColumns()`, `ActiveDeletePrefix()`, `ActiveCompatVersion()`)
- Discovery `defaultPort` parameter for mode-aware SRV resolution (9428 for logs, 10428 for
  traces)
- Helm chart: mode-aware image selection (`image.logs.repository` / `image.traces.repository`)
- CI: Fully parallel jobs for logs and traces (test, lint, build, docker, security, benchmarks)

## [0.14.0] - 2026-05-05

### Added
- `/lakehouse/info` endpoint now includes `build_time` field for operational visibility
- Traces delete support: mode-aware rewriter uses `schema.TraceRow` for traces mode,
  `schema.LogRow` for logs mode
- Delete handler registers at `/delete/tracessql/*` in traces mode, `/delete/logsql/*` in logs
  mode
- Docs: 5 new pages for Docusaurus site — read-path, kubernetes-deployment,
  docker-compose-setup, benchmarks, open-parquet-format
- Docs: Docusaurus YAML frontmatter on all 20 documentation pages
- CI: Changelog enforcement workflow — PRs with releasable changes require `[Unreleased]` entry

### Fixed
- Docs: Corrected false VL/VT compatibility claims — replaced "imports as Go module
  dependencies" with accurate "reimplements the VL/VT storage interface" (codebase is 100%
  clean-room, zero VL/VT Go imports)
- Docs: Removed non-existent `/insert/opentelemetry/v1/logs` endpoint from write-path
  documentation
- Docs: M7 Observability milestone updated from "Planned" to "Complete"
- Docs: Config count corrected from "65+ flags" to "110+ config options" (verified from code)

### Changed
- Docs: All cost tables corrected for 3 AZ replication (VL/VT runs 3 identical clusters, one per
  AZ)
- Docs: At 500GB/day 1yr 3 AZ — VL/VT $2,679/mo, Lakehouse $2,814/mo (within 5%), Loki $3,610/mo
- Docs: Compute scaled to 6× per component (3 AZ), storage × 3 for EBS, break-even and
  cumulative projections updated

## [0.12.0] - 2026-05-05

### Added
- Cost-aware deletion: VL-compatible `/delete/logsql/*` APIs with tombstone-based soft delete
- Three delete modes: `hide` (tombstone only), `permanent` (physical removal), `auto` (smart
  default)
- Tombstone query-time filtering across all query paths (zero-cost data suppression)
- Background rewriter for S3 Standard files with storage-class gating (never touches Glacier/IA)
- S3 storage class detection with lifecycle rule prediction (zero-cost age-based)
- Cost estimation endpoint (`/delete/logsql/estimate`) with per-class breakdown
- Delete verification endpoint (`/delete/logsql/verify`) for compliance auditing
- Un-delete support (remove tombstone to restore data visibility)
- Tombstone persistence to disk + S3 (survives full cluster recreation)

## [0.11.0] - 2026-05-05

### Added
- E2E: VictoriaLogs hot tier, multi-level vlselect, loki-vl-proxy in Docker Compose
- E2E: Internal Docker networking (only Grafana on port 3003)
- E2E: Loki proxy integration tests, vlselect multi-level tests, performance assertion tests
- Datagen: 5 realistic log patterns (JSON, logfmt, nginx, Java stacktrace, OTEL)
- Datagen: Dual-write to VL and S3 for hot/cold verification
- Loadtest: Benchmark mode for file size × row group × compression matrix
- Helm: Single YAML config blob in ConfigMap (no individual flag mapping)
- Helm: Common section deep-merged into components
- Helm: Separate toggleable headless services for discovery
- Helm: VPA support, extraManifests, vmauth Secret routing
- CI: Upstream sync tracks GitHub releases (not Go module versions)
- CI: Nightly benchmark workflow with artifact upload
- Docs: Performance documentation with benchmark methodology and cost projections

### Changed
- Helm: vmauth config stored as Secret instead of ConfigMap
- Helm: All components use generic HPA/VPA/PDB/ServiceMonitor/Ingress templates
- Grafana: 5 datasources (cold, hot, multi-level, Loki proxy, Jaeger)

### Removed
- Docker Compose: Host port mappings for non-Grafana services
- Helm: compaction-rbac.yaml (config in lakehouseConfig blob)

## [0.10.0] - 2026-05-04

### Added
- **Level-based Parquet compaction** — L0→L1→L2 with configurable thresholds, partition-level S3
  sentinels, and structured logging (`internal/compaction/`)
- **Leader election** — K8s Lease (primary) with S3 lock + HTTP liveness detection (fallback),
  `auto`/`k8s`/`s3`/`none` modes (`internal/election/`)
- **Peer manifest push notifications** — fire-and-forget HTTP POST to all peers on
  flush/compaction, with S3 ListObjects poll as fallback (`internal/manifest/push.go`)
- **Manifest update receiver** — `POST /internal/manifest/update` handler for cross-instance
  manifest sync
- **Load testing binary** — `cmd/loadtest/` with latency benchmarks (6 tests against plan
  targets) and throughput stress tests (insert rate, query QPS, mixed workload)
- **Compaction metrics** — 11 new Prometheus metrics: runs, files, bytes, rows, duration,
  errors, skip reasons
- **Election metrics** — leader gauge, transition counter, health check outcomes
- **Manifest push metrics** — push total, errors, peer count, received updates
- **Helm RBAC** — K8s Role/RoleBinding for Lease-based leader election when
  `compaction.enabled=true`
- **Nightly CI load test** — GitHub Actions workflow running full benchmark suite on schedule

## [0.9.0] - 2026-05-04

### Added
- **Prometheus metrics instrumentation** — ~80 metrics under `lakehouse_*` prefix: HTTP RED, S3
  operations, cache tiers, peer cache, manifest/discovery, Parquet engine, insert/writer,
  prefetch, startup/health, query
- **Grafana dashboards** — `victoria-lakehouse.json` (single-instance, 7 rows) and
  `victoria-lakehouse-cluster.json` (fleet, adds peer cache + per-instance)
- **Alerting rules** — 10 Prometheus alerting rules for critical operational conditions
- **Startup warmup sequence** — phased startup with readiness probe gating (init → disk recovery
  → S3 refresh → ready)
- **Circuit breaker** for S3 operations with configurable thresholds and recovery

## [0.8.0] - 2026-05-04

### Added
- **Write-ahead log (WAL)** — append-only crash recovery with gob-encoded log/trace entries,
  automatic replay on startup, atomic truncate after flush (`internal/wal/`)
- **VL-compatible insert APIs** — `/insert/jsonline`, `/insert/loki/api/v1/push`,
  `/insert/elasticsearch/_bulk` with full field mapping to Parquet schema
  (`internal/insertapi/`)
- **Adaptive file sizing** — per-partition byte estimates trigger flush when approaching
  `--lakehouse.insert.target-file-size` for optimal Parquet output
- **Buffer query bridge** — select pods fan out to insert pods via `/internal/buffer/query` for
  zero-delay reads of unflushed data (`internal/storage/parquets3/buffer_bridge.go`)
- **Manifest label pruning** — `FileInfo.Labels` field with `MatchesLabel()` for query-time file
  skipping without opening Parquet files
- **Manifest management** — `AllFiles()` snapshot and `RemoveFile()` for partition lifecycle
- **Label extraction** — automatic extraction of label values from log rows (10 fields) and
  trace rows (2 fields) during flush
- **WAL integration in BatchWriter** — entries written to WAL before buffering, WAL truncated on
  successful flush, replay on startup
- **Insert + select role separation** — `--lakehouse.role=all|insert|select` for independent
  scaling
- **Config extensions** — `TargetFileSize`, `WALMaxBytes`, `WALDir`, `WALEnabled`,
  `SelectConfig` with `BufferQueryEnabled`, `InsertHeadlessService`, `BufferQueryTimeout`

## [0.7.0] - 2026-05-03

### Added
- **Manifest partitions API** — `GET /manifest/partitions` with date-range filtering for
  per-date file/byte summaries
- **GetPartitions()** manifest method for partition inventory
- **PartitionsHandler** and **PartitionsResponse** types for HTTP layer

## [0.6.0] - 2026-05-03

### Added
- Filter AST engine with full LogsQL predicate support: exact match (`field:="value"`),
  substring (`field:value`), regex (`field:~"pattern"`), AND, OR, NOT, parenthesised grouping
- Playwright-based E2E UI tests validating Grafana Explore queries against live Lakehouse
  backend
- E2E integration tests for logs queries, Jaeger trace search, field enumeration, and stats
  aggregation
- Schema validation tests ensuring Parquet column mapping correctness

### Fixed
- Schema field mapping corrections for OTEL-standard column names

## [0.5.0] - 2026-05-03

### Added
- VL/VT internal select protocol (`/internal/select/*`) — 11 endpoints for cluster storage-node
  registration
- Binary DataBlock streaming with ZSTD compression for efficient cluster communication
- Prefetch engine with token-based row group read-ahead optimisation
- Register as `-storageNode` on vlselect/vtselect for transparent hot+cold fan-out

## [0.4.0] - 2026-05-02

### Added
- Distributed peer cache via consistent hash ring with headless DNS service discovery
- Peer HTTP protocol (`/internal/cache/fetch`, `/internal/cache/has`) with shared-secret auth
- Hot boundary auto-discovery from vlstorage/vtstorage `/internal/partition/list` endpoint
- Topology auto-detection: storage-node, direct, loki-proxy modes
- Static and headless service discovery for storage nodes and peers

## [0.3.0] - 2026-05-02

### Added
- L1 in-memory LRU cache for Parquet footers, bloom filters, and hot row groups
- L2 local disk cache with LRU eviction at configurable watermark
- Cache coalescence via `singleflight.Group` to deduplicate concurrent S3 fetches
- Label/attribute index with background scanning and disk persistence for sub-ms
  `field_names`/`field_values`
- Metadata persistence and recovery on restart (manifest, label index, footers)

## [0.2.0] - 2026-05-02

### Added
- Bloom filter checking for fast point lookups on `trace_id` and `service_name` columns
- Column projection — read only columns referenced by query, reducing I/O by 60-80%
- `GetStreamFieldNames`, `GetStreamFieldValues`, `GetStreams`, `GetStreamIDs` storage methods
- `GetFieldNames`, `GetFieldValues` from Parquet metadata with label index fallback
- No-op `Delete*` and `GetTenantIDs` methods for read-only cold storage

## [0.1.0] - 2026-05-02

### Added
- Initial project structure with Go module, CI/CD, Dockerfile, Helm chart skeleton
- Config namespace (`--lakehouse.*`) with YAML + flag parsing and production-ready defaults
- Mode selection: `--lakehouse.mode=logs` (port 9428) or `--lakehouse.mode=traces` (port 10428)
- S3 `io.ReaderAt` adapter for parquet-go with connection pooling and range reads
- ParquetS3Storage query engine: Hive partition pruning, row group statistics skipping,
  DataBlock emission
- SchemaRegistry mapping OTEL Parquet columns to VL/VT internal names (logs + traces profiles)
- Partition manifest with S3 ListObjects refresh and sub-ms "nothing here" fast path
- HTTP endpoints: `/health`, `/ready`, `/manifest/range`, `/manifest/partitions`,
  `/lakehouse/info`
- Public LogsQL API: all `/select/logsql/*` query endpoints (query, stats, hits, field/stream
  discovery)
- Jaeger API: `/select/jaeger/api/*` endpoints (traces, services, operations, dependencies)
- Phased startup warmup: init → disk recovery → S3 refresh → ready
- Distroless container image with multi-stage build
- GitHub Actions CI/CD: test, lint (golangci, gosec, gitleaks), build, security scanning,
  auto-release
- PR labeler, dependabot, CODEOWNERS configuration
- Documentation: architecture, configuration, cost estimates, getting started, observability,
  operations, performance, scaling, security
