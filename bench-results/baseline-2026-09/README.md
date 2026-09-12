# Baseline 2026-09 (pre-upgrade: VL v1.50.0 / VT v0.9.2 / parquet-go v0.30.1)

Files:
- `run-baseline-v3.2.json` / `.md` / `.log` — **THE current perf-gate
  reference.** `scripts/bench/run.sh --signals both --s3-latency "0 100"
  --ranges "1h 24h" --iterations 20 --warmup 3` with every timed response
  validated — a truncated `scan`'s membership/cardinality rule, group-by
  results hashed by (group, count) pairs, and every cross-system comparison
  at EXACT equality (see "Response validation (v3.2)" below). Supersedes
  v3.1, v3, v2, and v1 — kept below for provenance only — do not judge new
  PRs against their numbers.
- `run-baseline-v3.1.{json,md,log}` — v3.1, superseded by v3.2 (see below).
- `run-baseline-v3.{json,md,log}` — v3, superseded by v3.1.
- `run-baseline.json` / `run-baseline.md` — v1, superseded (see caveats below).
- `run-baseline-traces-v2.{json,md,log}` — v2 traces rerun, superseded by v3.2.
- `full-scope-lat0.csv|md`, `full-scope-lat100.csv|md`, `metrics-lat*/` — `scripts/bench/full-scope-s3-bench.sh` (e2e compose) with the per-scenario S3-ops table
- `env.txt` — image tags, git sha, host, docker version

Judge every later PR against `run-baseline-v3.2.md`. Perf gate: any LH/CH >=
1.0 cell blocks. A regression is judged on **LH's own absolute p90 and p50
against this run**, same hardware, not raw p95 and not the LH/VL ratio — see
"Why p90+p50, not p95" and the trust caveat below.

## Response validation (v3.2, 2026-09-13)

`run.sh` validates **every timed response**, not just its latency: a fast
wrong/empty/error answer is a broken response and is never counted as
latency. Per iteration (warmup iterations too, though warmup never counts
for latency): HTTP must be 2xx (curl no longer passes `-f`, so a real 4xx/5xx
is recorded as its actual code instead of collapsing into "000" — the same
bucket a dead server would use); the body must parse (a body with even ONE
unparseable line is rejected, not just a body where every line failed); the
extracted result must be non-empty *unless* the query is a documented miss
scenario (`trace_lookup`); and — for non-`scan` results — the result must be
identical to the cell's first valid result (a flapping answer is excluded,
not averaged in). Every request in one (signal, query, range, latency) cell
now shares ONE set of window bounds, computed once from a single
`time.time()` call and passed to every system — previously each system
computed its own bounds a few seconds (or, worse, a few *milliseconds*
across a whole-second boundary between the ns-precision and s-precision
helpers) after the last, so a comparison could differ by construction, not
by a real divergence. With bounds shared and the seed a static one-time
backfill (no live ingest), every system's result for a cell now MUST be
byte-identical — so `report.py` requires EXACT equality between baseline and
each engine, not a 5% tolerance (the ±5% tolerance still lives in the
pre-flight `parity_gate`, which is a sanity check on the sweep starting
conditions, not a per-cell validity rule).

**Group-by (`count_by_service`, `high_card`) is hashed by (group, count)
pairs, not summed to a total.** Reducing a group-by result to `sum(n)`
validates the TOTAL only — two systems could split the same total across a
different SET of groups and still "match". `extract_result` now returns
`rows=<groups>;hash=<sha256 of the sorted "group\x00count" pairs>` for both
ClickHouse (TSV `key\tcount`) and VL/VT/LH (JSON lines), so a
same-total-different-groups divergence is caught.

**`scan` validation is membership + cardinality, by design, not identity.**
VictoriaLogs documents that `limit N` without an explicit `sort` returns
rows "selected in arbitrary order because of performance reasons … can
return different sets of logs every time" once more than N rows match.
Per-iteration identity can therefore never hold for a truncated scan, and
adding `sort` to force it would change what the query measures (a sort has
its own, different cost). So `run.sh` validates a truncated scan by
**membership + cardinality** against a reference instead: before the warmup
loop, ONE untimed request per system re-runs the same filter/window (same
shared bounds) with the `limit` clause stripped, and records `window_rows`
(the TRUE match count) and `window_hash` (sha256 of the sorted set of stable
row keys — `_msg` for logs, `trace_id:span_id` for traces). When
`window_rows > 1000` (the scan's `limit`), a timed iteration is valid iff it
returns exactly 1000 rows and every one of them is a member of the reference
window's key set (checked as a python set-difference, not `comm` — a real
`_msg` can contain embedded newlines, e.g. a Java stack trace, which
corrupts a newline-delimited/`comm` comparison by silently fragmenting one
key into several bogus "lines"). When `window_rows <= 1000`, nothing was
truncated, so identity is meaningful again and applies as before. ClickHouse's
`scan` is now requested as `FORMAT JSONEachRow` with `Body`/`TraceId`/`SpanId`
aliased to `_msg`/`trace_id`/`span_id`, so it goes through the SAME extractor
as VL/VT/LH and gets a real `window_hash` too — it used to be validated by
row count alone.

Invalid iterations are dropped from p50/p95/p99 and the cell records
`iters_valid`/`iters_invalid`/`invalid_reasons`, and the per-cell stderr log
line always shows `valid=<k>/<N>` next to `p95=` so a partially-invalid cell
can never read as a clean latency. `report.py` requires `window_rows`/`result`
counts to match EXACTLY (see above) and, when both sides carry one, a
`window_hash`/content hash match too. A `scan` cell renders as
`rows=<returned>/<window_rows>[;window=<hash8>]`; a group-by cell renders as
`rows=<groups>;hash=<hash8>`. Every system's cell carries a `k/N valid`
column, and a ClickHouse speedup figure only counts a row when LH's own cell
in that row is valid.

**`run-baseline-v3.2.md` result: LH 64/64 rows valid (0 invalid — the
report's own "valid LH cells" summary line, LH-scoped by definition);
ClickHouse 63/64, 1 invalid; baseline (VL/VT) 64/64. The parity gate passed
at both latency levels on both signals.** The single invalid cell is
`logs/high_card/24h/lat100ms`
(ClickHouse): `result 6558 vs base 6557` — a genuine, ClickHouse-specific
1-group discrepancy at extreme cardinality (~6,557 distinct `trace_id`
groups) that the OLD sum-only group-by comparison could not see (the
row-count total, 14107, agrees exactly across all three systems — only the
NUMBER OF GROUPS differs by one). VL and LH agree with each other exactly
(same count, same hash); only ClickHouse's `GROUP BY TraceId` produces one
extra group somewhere in that window, on the SAME S3 Parquet LH reads byte
for byte. This is a real, if extremely minor (0.015%), engine-specific
finding surfaced by the new group-by hashing (previously invisible), not a
harness defect and not something to paper over by loosening the check — see
"Cells that are not fully valid" for the full writeup. Every `scan` cell
(the primary target of this round's fix) passes with exact `window_rows` and
`window_hash` equality across all three systems, including ClickHouse.

**Why p90+p50, not p95, for the perf gate:** with `--iterations 20`, p95 is
literally `sorted(samples)[19]` — the single MAXIMUM sample. One slow
outlier (GC pause, a scheduler hiccup, a cold page fault) becomes "the p95"
and can swing a whole cell by 5-10× with nothing structurally different
happening (see `multi_filter`/24h/100ms below: one outlier iteration alone
would read as a "regression" on p95 but the p50/p90 tell the true story).
The perf gate therefore compares **p90 and p50**, not p95, against this run
(p95 stays in the table for visibility). The alternative — raising
`--iterations` to ≥100 so p95 stops being the max — was considered and
rejected here only because it would roughly 5× the sweep's already
45-75-minute runtime; either fix is valid, this repo picked the cheaper one.

**Trust caveat:** several v3.2 baseline p95s are under 3 ms — at that scale,
laptop run-to-run noise (scheduler jitter, page cache, Docker overhead) is on
the order of the measurement itself, so an LH/baseline ratio computed from
two ~2 ms numbers can swing by ±2× on a different run with nothing having
changed. Treat the ratios in the tables as directional. The perf gate this
run backs compares LH's own absolute p90/p50 against this run, same
hardware — far less sensitive to this noise than the ratio is.

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

**Exact rerun command for `run-baseline-traces-v2.{json,md,log}`** (stack
already up from the pre-rerun verification below, disk profile `local-ssd`
— the default, not overridden):

```
scripts/bench/run.sh --signals traces --s3-latency "0 100" --ranges "1h 24h" \
  --iterations 20 --warmup 3 --no-up --no-ingest \
  --output bench-results/baseline-2026-09/run-baseline-traces-v2.json
```
- **Pre-rerun verification (24h window, one moment in time — see
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

**Exact command for `run-baseline-v3.2.{json,md,log}`** (fresh stack, disk
profile `local-ssd`):

```
scripts/bench/run.sh --signals both --s3-latency "0 100" --ranges "1h 24h" \
  --iterations 20 --warmup 3 \
  --output bench-results/baseline-2026-09/run-baseline-v3.2.json 2>&1 \
  | tee -a bench-results/baseline-2026-09/run-baseline-v3.2.log
python3 scripts/bench/report.py bench-results/baseline-2026-09/run-baseline-v3.2.json \
  bench-results/baseline-2026-09/run-baseline-v3.2.md
```
- **Enforced parity gate (from `run-baseline-v3.2.log`):** logs count_total —
  baseline (victorialogs) = 14129 at 0ms, 14108 at 100ms; traces count_total
  — baseline (victoriatraces) = 16658 at 0ms, 16646 at 100ms. No `parity
  gate MISMATCH` or `parity gate FAILED` line anywhere in the log.
- **`run-baseline-v3.2.md` result:** LH 64/64 rows valid, 0 invalid;
  ClickHouse 63/64 (1 invalid — see "Cells that are not fully valid" below);
  baseline 64/64. LH is a logs median 2.0× baseline (p90 3.8×) and 9× faster
  than ClickHouse; a traces median 2.5× baseline (p90 3.8×) and 14× faster than
  ClickHouse. See the trust caveat above before reading too much into the
  ratios at sub-3ms baselines.
- **Unit/self tests (v3.2):** `python3 -m unittest discover -s
  scripts/bench/tests` (46 tests), `scripts/bench/tests/extract_result_test.sh`
  (27 checks), `scripts/bench/tests/scan_membership_test.sh` (17 checks),
  `scripts/bench/tests/measure_query_test.sh` (26 checks — `measure_query`
  end to end with a stubbed `_do_req`, `_validate_iter`'s non-scan branches,
  `build_scan_window`/`strip_scan_limit`) — 116 checks total (55 before this
  round), all pass; see `docs/benchmarks/full-scope-s3.md` for how to run
  them.

### Cells that are not fully valid

`logs/high_card/24h/lat100ms`, ClickHouse only: `result 6558 vs base 6557`.
`high_card` groups by `trace_id` — at 24h this is ~6,557 distinct groups, the
highest cardinality in the whole matrix. VL and LH agree with each other
exactly (same group count, same sorted-pairs hash); ClickHouse's `GROUP BY
TraceId` over the SAME S3 Parquet LH reads byte for byte produces one extra
group. The row-count total (14107) is identical across all three systems —
only the number of distinct groups differs, by one, and only at this specific
range/latency combination (the matching 0ms cell agrees exactly across all
three, same hash: `rows=6567;hash=a653d2b4…`). This is exactly the class of
bug the group-by hashing fix (this round) exists to catch: the OLD
sum-to-total comparison could never have seen it, since the total was
(and remains) correct. It's a genuine, if extremely minor (0.015% of the
group count), ClickHouse-specific finding — not a harness defect, and not
something to fix by loosening the check. Left as a follow-up if anyone wants
to root-cause ClickHouse's `GROUP BY` behavior at this cardinality; every
other cross-system comparison in the matrix — including all 8 `scan` cells,
now checked by `window_rows`/`window_hash` exact equality across VL/VT, LH,
AND ClickHouse — passes clean.
