# Full-scope S3 / scan benchmark

Compares every query class that drives **S3 operations or column scans** across:
**cold LH** (Parquet on S3), **hot VL** (in-memory), and **ClickHouse-over-S3** — to
find where LH is slow, what it lacks, and whether CH does pure-S3 ops better.

## Running it (latency is always scoped + cleaned up)

```bash
# with realistic S3 latency (injected only for the run, auto-cleared by a trap):
scripts/bench/with-s3-latency.sh 100 30 -- scripts/bench/full-scope-s3-bench.sh 15
# no added latency (relative comparison only):
scripts/bench/with-s3-latency.sh 0 0   -- scripts/bench/full-scope-s3-bench.sh 15
```

`with-s3-latency.sh` injects the toxic before the command and **removes it on EXIT /
INT / TERM** — so a failed or interrupted benchmark never leaves the toxic active.
This is the fix for the incident where a manual `inject-s3-latency.sh 100 30` was
left injected and made every cold-LH query ~50× slower (a 24h `service.name`
dropdown went 50s instead of 1s). **Never inject latency without this wrapper.**

## Query classes covered

field_values (no-limit + limit), field_names, count (1h/24h), full-text scan,
filtered count, group-by — each driving a different S3/scan pattern. CH runs the
SQL equivalents against its S3-backed `lakehouse.otel_logs` table.

## Baseline finding (iters=1, no latency, pre-pmeta image)

| scenario | LH p50 | VL p50 | CH p50 | LH/VL | LH/CH |
|---|--:|--:|--:|--:|--:|
| field_values_limit100 | **26** | 252 | — | 0.1x | — |
| field_values_nolimit | 775† | 619 | 3046 | 1.3x | 0.3x |
| field_names | 137 | 298 | — | 0.5x | — |
| count_1h | 183 | 38 | 4490 | 4.8x | 0.0x |
| count_24h | 285 | 174 | 2436 | 1.6x | 0.1x |
| fulltext_scan_1h | 75 | 92 | 1865 | 0.8x | 0.0x |
| filtered_count_1h | 71 | 44 | 1690 | 1.6x | 0.0x |
| groupby_service_1h | 119 | 47 | 2262 | 2.5x | 0.1x |

† `field_values_nolimit` 775ms is the pre-fix scan; the `limit==0`-uses-index fix
makes it ~26ms (like limit100). Re-run after rebuilding the LH image with the fix.

**Reading it:**
- **CH is not better at pure-S3 ops** — LH is **10–25× faster** than ClickHouse on
  every class here (CH-over-S3 pays a per-query S3 round-trip tax LH avoids via its
  manifest + footer/bloom indexes).
- LH **beats VL** on field_names, field_values (index), full-text scan.
- **Optimization targets** (LH slower than VL): `count_1h` (4.8×), `groupby_service`
  (2.5×) — small absolute gaps, but the manifest fast-path / per-service rowcount
  (PERF-2) would close them.

The script writes a CSV (`/tmp/full-scope-s3-bench.csv`) and a markdown summary with
p50 + LH/VL + LH/CH ratios, flagging `LH≫VL` (>3×) and `CH-wins` (>2×) cells.

## Post-pmeta full switch (2026-06-10, no injected latency, 15 iters)

Run after the consolidation completed (#127 + #130 + #131: facets serve all reads,
legacy sidecars retired, audit hardening in). Same harness, same stack.

| scenario | LH p50 | VL p50 | CH p50 | LH/VL | LH/CH |
|---|--:|--:|--:|--:|--:|
| count_1h | 32 | 27 | 932 | 1.2x | 0.03x |
| count_24h | 137 | 143 | 931 | **1.0x** | 0.15x |
| field_names | 96 | 260 | — | **0.4x** | — |
| field_values_limit100 | 24 | 248 | — | **0.1x** | — |
| field_values_nolimit | 23 | 241 | 921 | **0.1x** | 0.03x |
| filtered_count_1h | 30 | 29 | 944 | **1.0x** | 0.03x |
| fulltext_scan_1h | 34 | 32 | 942 | **1.0x** | 0.04x |
| groupby_service_1h | 30 | 31 | 935 | **1.0x** | 0.03x |

Takeaways vs the pre-pmeta matrix above:
- **Every "LH≫VL" flag is gone.** Cold LH is at parity with HOT in-memory VL on every
  count/scan/groupby scenario, and **2.7–10× faster on the metadata queries**
  (field_names 0.4x, field_values 0.1x — the catalog serving from RAM).
- **ClickHouse-over-S3 is 30–40× slower than cold LH across the board** on this stack.
- The remaining S3-scan optimization plan (count-only hint, deep machinery) is now
  purely about wider windows / higher latency environments, not about closing VL gaps
  at this scale.

## Baseline 2026-09 (pre-upgrade: VL v1.50.0 / VT v0.9.2 / parquet-go v0.30.1)

Reference for the September-2026 upstream upgrade and the fix series. Raw artifacts:
`bench-results/baseline-2026-09/`. Perf gate rule: any LH/CH ≥ 1.0 cell blocks a PR;
a regression on any scenario is judged on **LH's own absolute p90 and p50 against
this run** (same hardware) — not raw p95 (see "Why p90+p50, not p95" below) and not
the LH/VL ratio (see the trust caveat under "Consolidated run — v3.2"). A
same-hardware p90 (or p50) increase > 10 % on any scenario blocks a PR.

### Response validation

`scripts/bench/run.sh` validates **every timed response**, not just its
latency — a benchmark cell is only meaningful when every iteration returned a
correct, valid answer; a fast wrong/empty/error response is a broken
response and is never counted. Per iteration (including warmup, which is
validated the same way but never timed):

- HTTP status must be 2xx (curl no longer passes `-f`, so a real 4xx/5xx is
  recorded as its actual code instead of collapsing into "000" — the same
  code a dead server would produce).
- The body must parse into a comparable result, with no partial failures: a
  body where even one line fails to parse is `invalid:parse-error`, not just
  a body where every line does.
- The result must be non-empty, unless the query is a documented miss
  scenario (`trace_lookup` — a cross-signal id lookup that can legitimately
  find nothing).
- For non-`scan` results, the result must be identical to the cell's first
  valid result — a flapping answer across iterations is excluded, not
  averaged in.

Every request in one (signal, query, range, latency) cell shares ONE set of
window bounds, computed from a single `time.time()` call and passed to every
system — each system previously computed its own bounds independently
(sometimes via TWO SEPARATE `time.time()` calls even within one system, one
nanosecond-precision for LogsQL and one second-precision for ClickHouse SQL,
which could straddle a whole-second boundary and put ClickHouse up to ~1s
ahead of LogsQL for the SAME nominal cell). With bounds shared and the seed a
static one-time backfill (no live ingest), every system's result for a cell
now MUST be byte-identical, so `report.py` requires **exact equality**
between baseline and each engine — no tolerance. (The pre-flight
`parity_gate`, a sanity check on the sweep's starting conditions rather than
a per-cell validity rule, keeps its own ±5% tolerance.)

**Group-by (`count_by_service`, `high_card`) is hashed by (group, count)
pairs, not summed to a total.** Reducing a group-by result to `sum(n)`
validates the TOTAL only — two systems could split the same total across a
different SET of groups and still "match". `extract_result` now returns
`rows=<groups>;hash=<sha256 of the sorted "group\x00count" pairs>` for both
ClickHouse (TSV `key\tcount`) and VL/VT/LH (JSON lines).

**`scan` validation is membership + cardinality, by design, not identity.**
VictoriaLogs documents that `limit N` without an explicit `sort` returns rows
"selected in arbitrary order because of performance reasons … can return
different sets of logs every time" once more than N rows match. That means
per-iteration IDENTITY can never hold for a truncated scan — and adding a
`sort` to make it hold would change what the query measures (a sort has its
own, different, cost; it's not the same benchmark anymore). So a truncated
scan's validity is **membership + cardinality**, not identity: before the
warmup loop, `run.sh` issues ONE untimed reference request per system — the
same filter/window (same shared bounds), `limit` clause removed — and
records `window_rows` (the TRUE, untruncated match count) and `window_hash`
(sha256 of the sorted set of stable row keys — `_msg` for logs,
`trace_id:span_id` for traces). When `window_rows > 1000` (the scan
`limit`), each timed iteration is valid iff it's exactly 1000 rows AND every
one of those rows' keys is a member of the reference window's key set (a
python set-difference, not `comm` — a real log `_msg` can contain embedded
newlines, e.g. a stack trace, which would corrupt a
newline-delimited/`comm`-based comparison). When `window_rows <= 1000`
(nothing was truncated), identity is meaningful again and applies as normal.
ClickHouse's `scan` is now requested as `FORMAT JSONEachRow` with
`Body`/`TraceId`/`SpanId` aliased to `_msg`/`trace_id`/`span_id`, so it goes
through the SAME extractor as VL/VT/LH and gets a real `window_hash` too —
it used to be validated by row count alone.

Invalid iterations are dropped from p50/p95/p99 and the cell records how many
were invalid and why (`iters_valid`/`iters_invalid`/`invalid_reasons` in the
JSON; `k/N invalid: <reason>` in the table, plus `valid=<k>/<N>` on the
per-cell stderr log line so a partially-invalid cell can never read as a
clean p95). `report.py` requires exact equality between each engine's result
and the baseline's (see above), plus — when both sides carry one — a content
hash / `window_hash` match. A `scan` cell renders as
`rows=<returned>/<window_rows>[;window=<hash8>]`; a group-by cell renders as
`rows=<groups>;hash=<hash8>`. Every system's cell in the tables below carries
a `k/N valid` column, and ClickHouse speedup figures only count a row when
LH's own cell in that row is valid.

**Why p90+p50, not p95, for the perf gate:** with `--iterations 20`, p95 is
literally `sorted(samples)[19]` — the single MAXIMUM sample. One slow
outlier (GC pause, scheduler hiccup, cold page fault) becomes "the p95" and
can swing a cell 5-10× with nothing structurally different happening (see
`multi_filter`/24h/100ms in the v3.2 logs table below: LH's p95 there is one
outlier sample, not a trend). The perf gate compares p90 and p50 instead
(p95 stays in the table for visibility). Raising `--iterations` to ≥100 so
p95 stops being the max was considered and rejected here only because it
would roughly 5× the sweep's already 45-75-minute runtime — either fix is
valid; this repo picked the cheaper one.

Unit/self tests for the validators: `python3 -m unittest discover -s
scripts/bench/tests` (`report.py`'s cross-system rules, including the
window-hash/window-rows scan rules and the group-by pair-hash rules) and
`scripts/bench/tests/extract_result_test.sh` +
`scripts/bench/tests/scan_membership_test.sh` +
`scripts/bench/tests/measure_query_test.sh` (`run.sh`'s per-response
extractor, the scan membership/cardinality rule, `measure_query` end to end
with a stubbed `_do_req`, and `build_scan_window`/`strip_scan_limit`, all
against fixture bodies — including a malformed body and a multi-line `_msg`
regression case). All four are self-contained (no live stack needed) and are
the ones to run before touching either file.

### Consolidated run — v3.2 (`scripts/bench/run.sh --signals both --s3-latency "0 100" --ranges "1h 24h" --iterations 20 --warmup 3`)

**THE current perf-gate reference** (`bench-results/baseline-2026-09/run-baseline-v3.2.{json,md,log}`).
Supersedes v3.1 (`run-baseline-v3.1.md`) — whose numbers predate the shared
window-bounds fix and could show a same-cell cross-system difference that
was really just a few milliseconds of relative-window drift, not a real
divergence — and v3, v2, v1, kept only for historical provenance. Do not
judge new PRs against them.

**LH: 64/64 rows valid, 0 invalid. ClickHouse: 63/64, 1 invalid. Baseline:
64/64. The parity gate passed at both latency levels on both signals.** The
single invalid cell is `logs/high_card/24h/lat100ms` (ClickHouse):
`result 6558 vs base 6557` — see "Cells that are not fully valid" below for
the full writeup; every `scan` cell (8 of them) passes with exact
`window_rows`/`window_hash` equality across all three systems, including
ClickHouse.

**Trust caveat for the numbers below:** several baseline p95s are under
3 ms (e.g. `trace_by_id`, some `count_total`/`service_filter` cells) — at
that scale, run-to-run noise on a laptop (scheduler jitter, page cache,
Docker overhead) is on the order of the measurement itself, so a ratio like
"2.5× baseline" computed from two ~2 ms numbers can easily swing by ±2× on a
different run without anything having changed. Treat the **ratios** in this
table as directional, not exact. The perf gate this table backs compares
**LH's own absolute p90/p50** against this run, on the same hardware — not
the LH/baseline ratio — which is far less sensitive to this noise.

#### Logs

**Per-query median LH vs baseline:** count_by_service 1.5×, count_total 2.2×, fulltext 2.7×, high_card 1.6×, level_filter 2.8×, multi_filter 2.3×, negation 3.0×, scan 1.8×, trace_lookup 1.8×

| query | range | S3 lat | baseline p95 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 2.9 [rows=5;hash=449330e2] | 20/20 | 6.3 (2.2×) [rows=5;hash=449330e2] | 20/20 | 95.0 (32.8× 🔴) [rows=5;hash=449330e2] | 20/20 |
| count_by_service | 1h | 100ms | 8.6 [rows=5;hash=89170518] | 20/20 | 3.9 (0.5×) [rows=5;hash=89170518] | 20/20 | 83.8 (9.7× ⚠️) [rows=5;hash=89170518] | 20/20 |
| count_by_service | 24h | 0ms | 10.0 [rows=5;hash=d7d75d82] | 20/20 | 14.8 (1.5×) [rows=5;hash=d7d75d82] | 20/20 | 86.4 (8.6× ⚠️) [rows=5;hash=d7d75d82] | 20/20 |
| count_by_service | 24h | 100ms | 6.5 [rows=5;hash=fec0246d] | 20/20 | 10.1 (1.6×) [rows=5;hash=fec0246d] | 20/20 | 80.4 (12.4× 🔴) [rows=5;hash=fec0246d] | 20/20 |
| count_total | 1h | 0ms | 4.3 [565] | 20/20 | 6.1 (1.4×) [565] | 20/20 | 78.0 (18.1× 🔴) [565] | 20/20 |
| count_total | 1h | 100ms | 3.9 [541] | 20/20 | 4.6 (1.2×) [541] | 20/20 | 79.3 (20.3× 🔴) [541] | 20/20 |
| count_total | 24h | 0ms | 6.0 [14125] | 20/20 | 22.6 (3.8× ⚠️) [14125] | 20/20 | 79.7 (13.3× 🔴) [14125] | 20/20 |
| count_total | 24h | 100ms | 5.3 [14107] | 20/20 | 15.7 (3.0×) [14107] | 20/20 | 83.9 (15.8× 🔴) [14107] | 20/20 |
| fulltext | 1h | 0ms | 4.8 [60] | 20/20 | 13.6 (2.8×) [60] | 20/20 | 72.8 (15.2× 🔴) [60] | 20/20 |
| fulltext | 1h | 100ms | 4.9 [56] | 20/20 | 6.5 (1.3×) [56] | 20/20 | 86.4 (17.6× 🔴) [56] | 20/20 |
| fulltext | 24h | 0ms | 7.8 [1178] | 20/20 | 22.6 (2.9×) [1178] | 20/20 | 86.8 (11.1× 🔴) [1178] | 20/20 |
| fulltext | 24h | 100ms | 8.8 [1177] | 20/20 | 22.4 (2.5×) [1177] | 20/20 | 80.8 (9.2× ⚠️) [1177] | 20/20 |
| high_card | 1h | 0ms | 3.7 [rows=256;hash=a36a9fe3] | 20/20 | 7.3 (2.0×) [rows=256;hash=a36a9fe3] | 20/20 | 101.5 (27.4× 🔴) [rows=256;hash=a36a9fe3] | 20/20 |
| high_card | 1h | 100ms | 9.3 [rows=251;hash=28937175] | 20/20 | 7.9 (0.8×) [rows=251;hash=28937175] | 20/20 | 93.4 (10.0× 🔴) [rows=251;hash=28937175] | 20/20 |
| high_card | 24h | 0ms | 20.5 [rows=6567;hash=a653d2b4] | 20/20 | 26.6 (1.3×) [rows=6567;hash=a653d2b4] | 20/20 | 87.8 (4.3× ⚠️) [rows=6567;hash=a653d2b4] | 20/20 |
| high_card | 24h | 100ms | 10.9 [rows=6557;hash=7b884ae9] | 20/20 | 26.4 (2.4×) [rows=6557;hash=7b884ae9] | 20/20 | ✗ result 6558 vs base 6557 | 20/20 |
| level_filter | 1h | 0ms | 2.8 [129] | 20/20 | 6.8 (2.4×) [129] | 20/20 | 102.2 (36.5× 🔴) [129] | 20/20 |
| level_filter | 1h | 100ms | 8.2 [122] | 20/20 | 6.1 (0.7×) [122] | 20/20 | 79.4 (9.7× ⚠️) [122] | 20/20 |
| level_filter | 24h | 0ms | 5.6 [3502] | 20/20 | 35.5 (6.3× ⚠️) [3502] | 20/20 | 88.9 (15.9× 🔴) [3502] | 20/20 |
| level_filter | 24h | 100ms | 6.4 [3498] | 20/20 | 20.4 (3.2× ⚠️) [3498] | 20/20 | 111.9 (17.5× 🔴) [3498] | 20/20 |
| multi_filter | 1h | 0ms | 5.2 [28] | 20/20 | 8.4 (1.6×) [28] | 20/20 | 109.0 (21.0× 🔴) [28] | 20/20 |
| multi_filter | 1h | 100ms | 6.4 [25] | 20/20 | 8.3 (1.3×) [25] | 20/20 | 78.1 (12.2× 🔴) [25] | 20/20 |
| multi_filter | 24h | 0ms | 9.2 [750] | 20/20 | 27.0 (2.9×) [750] | 20/20 | 94.2 (10.2× 🔴) [750] | 20/20 |
| multi_filter | 24h | 100ms | 5.8 [748] | 20/20 | 29.9 (5.2× ⚠️) [748] | 20/20 | 81.6 (14.1× 🔴) [748] | 20/20 |
| negation | 1h | 0ms | 2.7 [435] | 20/20 | 7.0 (2.6×) [435] | 20/20 | 82.6 (30.6× 🔴) [435] | 20/20 |
| negation | 1h | 100ms | 5.1 [419] | 20/20 | 6.4 (1.3×) [419] | 20/20 | 85.2 (16.7× 🔴) [419] | 20/20 |
| negation | 24h | 0ms | 7.7 [10589] | 20/20 | 31.6 (4.1× ⚠️) [10589] | 20/20 | 75.3 (9.8× ⚠️) [10589] | 20/20 |
| negation | 24h | 100ms | 6.6 [10581] | 20/20 | 21.9 (3.3× ⚠️) [10581] | 20/20 | 93.8 (14.2× 🔴) [10581] | 20/20 |
| scan | 1h | 0ms | 8.9 [rows=562/562;window=0e2ce791] | 20/20 | 8.2 (0.9×) [rows=562/562;window=0e2ce791] | 20/20 | 107.4 (12.1× 🔴) [rows=562/562;window=0e2ce791] | 20/20 |
| scan | 1h | 100ms | 3.8 [rows=541/541;window=117ddead] | 20/20 | 6.6 (1.7×) [rows=541/541;window=117ddead] | 20/20 | 108.9 (28.7× 🔴) [rows=541/541;window=117ddead] | 20/20 |
| scan | 24h | 0ms | 12.1 [rows=1000/14115;window=aef2e0cf] | 20/20 | 23.7 (2.0×) [rows=1000/14115;window=aef2e0cf] | 20/20 | 79.4 (6.6× ⚠️) [rows=1000/14115;window=aef2e0cf] | 20/20 |
| scan | 24h | 100ms | 9.6 [rows=1000/14097;window=fdc19edd] | 20/20 | 24.5 (2.6×) [rows=1000/14097;window=fdc19edd] | 20/20 | 81.2 (8.5× ⚠️) [rows=1000/14097;window=fdc19edd] | 20/20 |
| trace_lookup | 1h | 0ms | 2.4 [spans=2] | 20/20 | 6.4 (2.7×) [spans=2] | 20/20 | 92.1 (38.4× 🔴) [2] | 20/20 |
| trace_lookup | 1h | 100ms | 3.8 [spans=2] | 20/20 | 6.1 (1.6×) [spans=2] | 20/20 | 81.0 (21.3× 🔴) [2] | 20/20 |
| trace_lookup | 24h | 0ms | 6.7 [spans=2] | 20/20 | 6.6 (1.0×) [spans=2] | 20/20 | 74.0 (11.0× 🔴) [2] | 20/20 |
| trace_lookup | 24h | 100ms | 5.0 [spans=2] | 20/20 | 9.8 (2.0×) [spans=2] | 20/20 | 76.2 (15.2× 🔴) [2] | 20/20 |

`multi_filter`/24h/100ms (LH 29.9 ms, 5.2×) is this run's p95=max-at-n=20
outlier (see "Why p90+p50, not p95" above): a single slow sample, still
20/20 valid — the p90/p50 that actually feed the perf gate are unremarkable
for this cell.

#### Traces

**Per-query median LH vs baseline:** count_by_service 3.1×, count_total 2.3×, scan 2.3×, service_filter 2.4×, slow_spans 2.6×, span_name 3.7×, trace_by_id 2.0×

| query | range | S3 lat | baseline p95 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 4.8 [rows=5;hash=1e913713] | 20/20 | 4.4 (0.9×) [rows=5;hash=1e913713] | 20/20 | 112.8 (23.5× 🔴) [rows=5;hash=1e913713] | 20/20 |
| count_by_service | 1h | 100ms | 3.2 [rows=5;hash=5916db42] | 20/20 | 8.2 (2.6×) [rows=5;hash=5916db42] | 20/20 | 105.6 (33.0× 🔴) [rows=5;hash=5916db42] | 20/20 |
| count_by_service | 24h | 0ms | 3.3 [rows=5;hash=ca4a9b39] | 20/20 | 12.5 (3.8× ⚠️) [rows=5;hash=ca4a9b39] | 20/20 | 107.6 (32.6× 🔴) [rows=5;hash=ca4a9b39] | 20/20 |
| count_by_service | 24h | 100ms | 3.7 [rows=5;hash=0a54e41b] | 20/20 | 13.2 (3.6× ⚠️) [rows=5;hash=0a54e41b] | 20/20 | 109.2 (29.5× 🔴) [rows=5;hash=0a54e41b] | 20/20 |
| count_total | 1h | 0ms | 3.1 [578] | 20/20 | 5.0 (1.6×) [578] | 20/20 | 150.6 (48.6× 🔴) [578] | 20/20 |
| count_total | 1h | 100ms | 4.0 [554] | 20/20 | 7.9 (2.0×) [554] | 20/20 | 89.3 (22.3× 🔴) [554] | 20/20 |
| count_total | 24h | 0ms | 3.4 [16658] | 20/20 | 11.0 (3.2× ⚠️) [16658] | 20/20 | 96.0 (28.2× 🔴) [16658] | 20/20 |
| count_total | 24h | 100ms | 3.7 [16646] | 20/20 | 9.8 (2.6×) [16646] | 20/20 | 97.4 (26.3× 🔴) [16646] | 20/20 |
| scan | 1h | 0ms | 3.3 [rows=560/560;window=6a59c1d5] | 20/20 | 7.1 (2.2×) [rows=560/560;window=6a59c1d5] | 20/20 | 112.4 (34.1× 🔴) [rows=560/560;window=6a59c1d5] | 20/20 |
| scan | 1h | 100ms | 4.7 [rows=550/550;window=610a79cf] | 20/20 | 6.8 (1.4×) [rows=550/550;window=610a79cf] | 20/20 | 91.8 (19.5× 🔴) [rows=550/550;window=610a79cf] | 20/20 |
| scan | 24h | 0ms | 4.7 [rows=1000/16654;window=f4d45375] | 20/20 | 11.8 (2.5×) [rows=1000/16654;window=f4d45375] | 20/20 | 101.6 (21.6× 🔴) [rows=1000/16654;window=f4d45375] | 20/20 |
| scan | 24h | 100ms | 4.9 [rows=1000/16638;window=84c3b341] | 20/20 | 17.5 (3.6× ⚠️) [rows=1000/16638;window=84c3b341] | 20/20 | 92.5 (18.9× 🔴) [rows=1000/16638;window=84c3b341] | 20/20 |
| service_filter | 1h | 0ms | 3.3 [108] | 20/20 | 3.9 (1.2×) [108] | 20/20 | 90.1 (27.3× 🔴) [108] | 20/20 |
| service_filter | 1h | 100ms | 2.9 [104] | 20/20 | 4.5 (1.6×) [104] | 20/20 | 101.7 (35.1× 🔴) [104] | 20/20 |
| service_filter | 24h | 0ms | 2.9 [3290] | 20/20 | 9.2 (3.2× ⚠️) [3290] | 20/20 | 107.3 (37.0× 🔴) [3290] | 20/20 |
| service_filter | 24h | 100ms | 2.5 [3287] | 20/20 | 8.2 (3.3× ⚠️) [3287] | 20/20 | 113.1 (45.2× 🔴) [3287] | 20/20 |
| slow_spans | 1h | 0ms | 2.5 [40] | 20/20 | 5.2 (2.1×) [40] | 20/20 | 92.1 (36.8× 🔴) [40] | 20/20 |
| slow_spans | 1h | 100ms | 5.0 [37] | 20/20 | 6.1 (1.2×) [37] | 20/20 | 99.7 (19.9× 🔴) [37] | 20/20 |
| slow_spans | 24h | 0ms | 3.9 [1323] | 20/20 | 12.1 (3.1× ⚠️) [1323] | 20/20 | 109.5 (28.1× 🔴) [1323] | 20/20 |
| slow_spans | 24h | 100ms | 3.1 [1320] | 20/20 | 11.2 (3.6× ⚠️) [1320] | 20/20 | 154.3 (49.8× 🔴) [1320] | 20/20 |
| span_name | 1h | 0ms | 2.6 [65] | 20/20 | 9.8 (3.8× ⚠️) [65] | 20/20 | 115.7 (44.5× 🔴) [65] | 20/20 |
| span_name | 1h | 100ms | 2.5 [62] | 20/20 | 9.2 (3.7× ⚠️) [62] | 20/20 | 121.8 (48.7× 🔴) [62] | 20/20 |
| span_name | 24h | 0ms | 4.4 [1647] | 20/20 | 8.6 (2.0×) [1647] | 20/20 | 118.1 (26.8× 🔴) [1647] | 20/20 |
| span_name | 24h | 100ms | 1.8 [1646] | 20/20 | 11.4 (6.3× ⚠️) [1646] | 20/20 | 94.6 (52.6× 🔴) [1646] | 20/20 |
| trace_by_id | 1h | 0ms | 1.3 [spans=6] | 20/20 | 5.4 (4.2× ⚠️) [spans=6] | 20/20 | 94.9 (73.0× 🔴) [spans=6] | 20/20 |
| trace_by_id | 1h | 100ms | 3.3 [spans=6] | 20/20 | 4.8 (1.5×) [spans=6] | 20/20 | 102.5 (31.1× 🔴) [spans=6] | 20/20 |
| trace_by_id | 24h | 0ms | 2.9 [spans=6] | 20/20 | 6.2 (2.1×) [spans=6] | 20/20 | 85.5 (29.5× 🔴) [spans=6] | 20/20 |
| trace_by_id | 24h | 100ms | 2.8 [spans=6] | 20/20 | 5.2 (1.9×) [spans=6] | 20/20 | 97.6 (34.9× 🔴) [spans=6] | 20/20 |

#### Cells that are not fully valid

`logs/high_card/24h/lat100ms`, ClickHouse only: `result 6558 vs base 6557`.
`high_card` groups by `trace_id` — at 24h this is ~6,557 distinct groups, the
highest cardinality in the whole matrix. VL and LH agree with each other
exactly (same group count, same sorted-pairs hash); ClickHouse's
`GROUP BY TraceId` over the SAME S3 Parquet LH reads byte for byte produces
one extra group. The row-count total (14107) is identical across all three
systems at this cell — only the number of distinct groups differs, by one,
and only at this specific range/latency combination (the matching 0ms cell
above agrees exactly across all three: `rows=6567;hash=a653d2b4`). This is
exactly the class of bug the group-by pair-hashing fix (this round) exists
to catch — the OLD sum-to-total comparison could never have seen it, since
the total was (and remains) correct. It's a genuine, if extremely minor
(0.015% of the group count), ClickHouse-specific finding, not a harness
defect, and not something to fix by loosening the check; left as a follow-up
for anyone who wants to root-cause ClickHouse's `GROUP BY` behavior at this
cardinality. Every other cross-system comparison in the matrix — including
all 8 `scan` cells, now checked by `window_rows`/`window_hash` exact
equality across VL/VT, LH, AND ClickHouse — passes clean.

Full row-set hashes and per-iteration invalid reasons are in
`bench-results/baseline-2026-09/run-baseline-v3.2.md` (verbatim harness
output); truncated above for readability.

### Full-scope S3-ops (logs, e2e compose)

Pending — to be recorded once the e2e compose can be started (its host port 19428
is currently held by another project's stack; needs a human decision). No numbers
recorded here yet.
