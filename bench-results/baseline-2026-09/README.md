# Baseline 2026-09 (pre-upgrade: VL v1.50.0 / VT v0.9.2 / parquet-go v0.30.1)

Files:
- `run-baseline-v3.1.json` / `.md` / `.log` — **THE current perf-gate
  reference.** `scripts/bench/run.sh --signals both --s3-latency "0 100"
  --ranges "1h 24h" --iterations 20 --warmup 3` with every timed response
  validated, including a truncated `scan`'s membership/cardinality rule (see
  "Response validation (v3.1)" below). Supersedes v3, v2, and v1 — kept
  below for provenance only — do not judge new PRs against their numbers.
- `run-baseline-v3.{json,md,log}` — v3, superseded by v3.1 (see below).
- `run-baseline.json` / `run-baseline.md` — v1, superseded (see caveats below).
- `run-baseline-traces-v2.{json,md,log}` — v2 traces rerun, superseded by v3.1.
- `full-scope-lat0.csv|md`, `full-scope-lat100.csv|md`, `metrics-lat*/` — `scripts/bench/full-scope-s3-bench.sh` (e2e compose) with the per-scenario S3-ops table
- `env.txt` — image tags, git sha, host, docker version

Judge every later PR against `run-baseline-v3.1.md` (perf gate: any LH/CH >=
1.0 cell blocks; a regression is judged on **LH's own absolute p95 against
this run**, same hardware — see the trust caveat below, not the LH/VL ratio).

## Response validation (v3.1, 2026-09-13)

`run.sh` validates **every timed response**, not just its latency: a fast
wrong/empty/error answer is a broken response and is never counted as
latency. Per iteration (warmup iterations too, though warmup never counts
for latency): HTTP must be 2xx; the body must parse; the extracted result
must be non-empty *unless* the query is a documented miss scenario
(`trace_lookup`); and — for non-`scan` results — the result must be
identical to the cell's first valid result (a flapping answer is excluded,
not averaged in).

**`scan` validation semantics — a ruling, not a workaround.** VictoriaLogs
documents that `limit N` without an explicit `sort` returns rows "selected
in arbitrary order because of performance reasons … can return different
sets of logs every time" once more than N rows match. Per-iteration identity
can therefore never hold for a truncated scan, and adding `sort` to force it
would change what the query measures (a sort has its own, different cost).
So `run.sh` validates a truncated scan by **membership + cardinality**
against a reference instead: before the warmup loop, ONE untimed request per
system re-runs the same filter/window with the `limit` clause stripped, and
records `window_rows` (the TRUE match count) and, for VL/VT/LH,
`window_hash` (sha256 of the sorted set of stable row keys — `_msg` for
logs, `trace_id:span_id` for traces). When `window_rows > 1000` (the scan's
`limit`), a timed iteration is valid iff it returns exactly 1000 rows and
every one of them is a member of the reference window's key set (checked as
a python set-difference, not `comm` — a real `_msg` can contain embedded
newlines, e.g. a Java stack trace, which corrupts a newline-delimited/`comm`
comparison by silently fragmenting one key into several bogus "lines"). When
`window_rows <= 1000`, nothing was truncated, so identity is meaningful
again and applies as before. ClickHouse's `scan` has no comparable row key
and is validated/compared by `window_rows` alone.

Invalid iterations are dropped from p50/p95/p99 and the cell records
`iters_valid`/`iters_invalid`/`invalid_reasons`, and the per-cell stderr log
line now always shows `valid=<k>/<N>` next to `p95=` so a partially-invalid
cell can never read as a clean latency. `report.py` cross-checks each
engine's result against the baseline (VL/VT): beyond a 5% count tolerance
for non-`scan` results (flagging "same count, different rows" when a content
hash disagrees while the count matches), and the same tolerance against
`window_rows` for `scan` — plus, only when both `window_rows` are EXACTLY
equal, a `window_hash` match (a `scan` cell renders as
`rows=<returned>/<window_rows>[;window=<hash8>]`). Every system's cell
carries a `k/N valid` column, and a ClickHouse speedup figure only counts a
row when LH's own cell in that row is valid.

**`run-baseline-v3.1.md` result: 64/64 valid cells, 0 invalid, no `✗`
anywhere; the parity gate passed at both latency levels on both signals.**
The four `scan`/24h cells that v3 flagged (baseline "flapping", per the
membership ruling above — VictoriaLogs' own documented arbitrary-order
truncation, not a real divergence) now pass: each system's iterations are
internally consistent (every row a genuine window member, exactly 1000 of
them), and VL/LH's window hashes agree with each other.

**Trust caveat:** several v3.1 baseline p95s are under 3 ms — at that scale,
laptop run-to-run noise (scheduler jitter, page cache, Docker overhead) is on
the order of the measurement itself, so an LH/baseline ratio computed from
two ~2 ms numbers can swing by ±2× on a different run with nothing having
changed. Treat the ratios in the tables as directional. The perf gate this
run backs compares LH's own absolute p95 against this run, same hardware —
far less sensitive to this noise than the ratio is.

## Caveats (2026-09-12 harness recheck)

- **`run-baseline.md` logs table is valid** except the `trace_lookup` rows, which
  measure a miss on every system (0 results): `fetch_sample_tid` picked a
  `trace_id` via an unordered `limit 1` over a 7-day window, so the id it grabbed
  usually wasn't the one on screen by the time the query ran. Fixed for the
  traces rerun below (`fetch_sample_tid` now queries a 1h window ordered by
  time descending); the logs table was not rerun because its own
  `trace_lookup` cells are the only ones affected and the fix doesn't change
  any other logs query.
- **`run-baseline.md` traces table is superseded by `run-baseline-traces-v2.md`.**
  The original traces sweep queried VictoriaTraces with Lakehouse-only field
  names (`service.name`, `span.name`, `duration_ns`); VT v0.9.2 stores spans as
  `resource_attr:service.name`, `name`, `duration` (ns), so 12 of 28 VT/LH
  traces cells came back `0` or degenerate. The parity gate also only printed
  (`parity_gate "$signal" || true`) instead of enforcing, so the divergence
  never blocked the run.
- **Harness fixes applied** (`scripts/bench/run.sh`, `scripts/bench/report.py`):
  1. Traces `prep()` queries switched to VT-native field names (Lakehouse serves
     both dialects, so LH/CH results are unaffected). Fields containing a colon
     (`resource_attr:service.name`) must be backtick-quoted in LogsQL — both in
     `stats by (...)` and in an equality filter — or the parser misreads the
     colon as a bucket-size / second-operator clause and either errors or
     silently matches the wrong thing (`resource_attr:service.name:="x"`
     without backticks parsed but returned 0 on both tiers).
  2. `slow_spans` uses `trace_id:* duration:>50000000` (50 ms). 100 ms was the
     original threshold; the seed's spans are 5-54 ms (`cmd/datagen`), so
     >50 ms selects the slowest ~8% and is the largest round number giving a
     non-zero, exactly-equal count on VT/LH/CH (verified at
     10/20/30/40/45/50/53 ms — all matched exactly). `trace_id:*` is required,
     not optional — VictoriaTraces' internal `trace_id_idx_stream` index rows
     (fields `trace_id_idx, start_time, end_time, duration`) carry a
     trace-level `duration` and no `trace_id`; some of those exceed 100 ms, so
     a bare `duration:>N` filter over-counted on VT only (1075 vs LH's real 0)
     until `trace_id:*` was restored. `Duration` threshold made consistent in
     the ClickHouse SQL too.
  3. `fetch_sample_tid` now queries the last 1h window ordered `sort by (_time)
     desc | limit 1` and fails the run (non-zero exit) if no id comes back,
     instead of silently keeping a dummy id over an unordered 7-day `limit 1`.
  4. `parity_gate()` is now enforcing: it compares every system's `count_total`
     against the baseline (VL/VT) within ±5% and the call site aborts the run
     (`exit 1`) on any mismatch, instead of `parity_gate "$signal" || true`.
     A baseline count of `0` (or a non-numeric baseline result) is itself now
     treated as a mismatch — the gate refuses to sweep on empty data instead
     of only comparing other systems against a zero baseline.
  5. `run.sh` logs the exact prepared request per (signal, query, system) via
     `prep()` → `log "req <signal>/<query> <system> <method> <url> body=..."`
     (visible throughout `run-baseline-traces-v2.log`).
  6. `report.py` now invalidates a whole row (`baseline-empty`) when the
     baseline cell itself is 0/empty/zero-bytes, instead of only checking each
     engine's own result against a possibly-bogus baseline; and flags a
     `⚠ shape` cell when an engine's `avg_bytes` differs from the baseline's by
     more than 10× either way (payload-shape outlier, not a validity failure).
  7. Found during the traces rerun (not in the original recheck): ClickHouse's
     `scan` scalar was extracted by summing the last TSV column when it looked
     numeric — correct for count queries, wrong for a raw column scan whose
     last column (`Duration`) is itself numeric. It silently summed all
     returned durations (e.g. `18622000000`) instead of counting rows, so the
     traces `scan` CH cell was `✗` (self-inconsistent, not a real divergence)
     in every run before this fix. `measure_query`/`fetch_scalar` now thread
     the query name through and always use ROW COUNT for `scan`, regardless of
     column numeric-ness.
  8. MinIO images pinned to `quay.io/minio/mc:RELEASE.2025-04-16T18-13-26Z` /
     `quay.io/minio/minio:RELEASE.2025-04-22T22-12-26Z` in
     `deployment/docker/docker-compose-benchmark.yml`,
     `deployment/docker/docker-compose-e2e.yml`, `tests/parity/docker-compose.yml`
     (`minio/*:latest` is no longer anonymously pullable from Docker Hub).
  9. `report.py`'s header no longer hardcodes "gp3-simulated": it now reads
     `disk_profile` from the JSON rows (written by `run.sh` going forward)
     and prints that, falling back to "unspecified" for JSON produced before
     this fix (both `run-baseline.json` and `run-baseline-traces-v2.json`
     predate it, so their re-rendered headers read "unspecified" — the actual
     profile for both runs was `local-ssd`, the default, recorded below).

**Exact rerun command for `run-baseline-traces-v2.{json,md,log}`** (Step-6
stack already up, disk profile `local-ssd` — the default, not overridden):

```
scripts/bench/run.sh --signals traces --s3-latency "0 100" --ranges "1h 24h" \
  --iterations 20 --warmup 3 --no-up --no-ingest \
  --output bench-results/baseline-2026-09/run-baseline-traces-v2.json
```
- **Pre-rerun verification (Step 6, 24h window, one moment in time — see
  `run-baseline-traces-v2.log` for the actual per-run counts, which drift
  slightly between runs because the seed is a one-time 7-day backfill and the
  window keeps sliding forward):** `count_by_service` VT=17132/LH=17132,
  `service_filter` VT=3499/LH=3499, `span_name` VT=1664/LH=1664, `slow_spans`
  (50 ms) VT=1377/LH=1377, `scan` (1000-row limit) VT=1000/LH=1000 rows — all
  five pairs equal and non-zero on both tiers before the sweep ran.
- **Enforced parity gate (from `run-baseline-traces-v2.log`):** count_total at
  S3 latency 0ms — victoriatraces=16682, lakehouse=16682, clickhouse=16682; at
  100ms — victoriatraces=16668, lakehouse=16668, clickhouse=16668. Both gates
  passed (all three systems exactly equal); no `parity gate FAILED` line
  appears in the log.
- **`run-baseline-traces-v2.md` result:** 28/28 traces cells valid, 0 invalid,
  no `✗` cells, non-zero `[res]` on every scenario. LH is a median 2.7× VT/CH
  baseline (p90 4.7×) and 21× faster than ClickHouse on the same S3 Parquet.

**Exact command for `run-baseline-v3.{json,md,log}`** (fresh stack, disk
profile `local-ssd`):

```
scripts/bench/run.sh --signals both --s3-latency "0 100" --ranges "1h 24h" \
  --iterations 20 --warmup 3 \
  --output bench-results/baseline-2026-09/run-baseline-v3.json 2>&1 \
  | tee -a bench-results/baseline-2026-09/run-baseline-v3.log
python3 scripts/bench/report.py bench-results/baseline-2026-09/run-baseline-v3.json \
  bench-results/baseline-2026-09/run-baseline-v3.md
```
- **Enforced parity gate (from `run-baseline-v3.log`):** logs count_total —
  baseline (victorialogs) = 14294 at 0ms, 14270 at 100ms; traces count_total —
  baseline (victoriatraces) = 16610 at 0ms, 16578 at 100ms. No `parity gate
  MISMATCH` or `parity gate FAILED` line anywhere in the log — every system
  agreed with its signal's baseline within ±5% at both latency levels.
- **`run-baseline-v3.md` result:** 60/64 cells valid (see "Response
  validation (v3)" above for the 4 `scan`/24h exceptions and why). LH is a
  logs median 1.8× baseline (p90 4.3×) and 11× faster than ClickHouse; a
  traces median 1.9× baseline (p90 4.5×) and 19× faster than ClickHouse.
- **Unit/self tests (v3):** `python3 -m unittest discover -s scripts/bench/tests`
  (19 tests, `report.py`'s validity rules) and
  `scripts/bench/tests/extract_result_test.sh` (18 checks, `run.sh`'s
  per-response extractor/validator against fixture bodies) — both pass; see
  `docs/benchmarks/full-scope-s3.md` for how to run them.

**Exact command for `run-baseline-v3.1.{json,md,log}`** (fresh stack, disk
profile `local-ssd`):

```
scripts/bench/run.sh --signals both --s3-latency "0 100" --ranges "1h 24h" \
  --iterations 20 --warmup 3 \
  --output bench-results/baseline-2026-09/run-baseline-v3.1.json 2>&1 \
  | tee -a bench-results/baseline-2026-09/run-baseline-v3.1.log
python3 scripts/bench/report.py bench-results/baseline-2026-09/run-baseline-v3.1.json \
  bench-results/baseline-2026-09/run-baseline-v3.1.md
```
- **Enforced parity gate (from `run-baseline-v3.1.log`):** logs count_total —
  baseline (victorialogs) = 14312 at 0ms, 14292 at 100ms; traces count_total —
  baseline (victoriatraces) = 16834 at 0ms, 16826 at 100ms. No `parity gate
  MISMATCH` or `parity gate FAILED` line anywhere in the log.
- **`run-baseline-v3.1.md` result:** 64/64 cells valid (36 logs, 28 traces),
  0 invalid, no `✗` cells anywhere — including all 4 `scan`/24h cells that
  v3 flagged as baseline-flapping (see "Response validation (v3.1)" above).
  LH is a logs median 2.1× baseline (p90 4.9×) and 10× faster than
  ClickHouse; a traces median 2.2× baseline (p90 3.9×) and 15× faster than
  ClickHouse. See the trust caveat above before reading too much into the
  ratios at sub-3ms baselines.
- **Unit/self tests (v3.1):** `python3 -m unittest discover -s
  scripts/bench/tests` (26 tests — the v3 set plus the window-hash/
  window-rows scan rules) and `scripts/bench/tests/extract_result_test.sh`
  (18 checks) + `scripts/bench/tests/scan_membership_test.sh` (11 checks —
  member-ok, foreign-key, count≠limit, and a multi-line `_msg` regression
  case) — all pass; see `docs/benchmarks/full-scope-s3.md` for how to run
  them.
