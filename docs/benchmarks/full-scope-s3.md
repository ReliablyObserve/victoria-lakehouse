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
`bench-results/baseline-2026-09/`. Perf gate rule: any LH/CH ≥ 1.0 cell, or an LH/VL
regression > 10 % on any scenario versus this table, blocks a PR.

### Response validation

`scripts/bench/run.sh` validates **every timed response**, not just its
latency — a benchmark cell is only meaningful when every iteration returned a
correct, valid answer; a fast wrong/empty/error response is a broken
response and is never counted. Per iteration (including warmup, which is
validated the same way but never timed):

- HTTP status must be 2xx.
- The body must parse into a comparable result: a plain count for
  count/filter/group-by queries, `rows=<N>;hash=<sha256>` for `scan` (the
  hash covers the sorted set of stable row keys — `_msg` for logs,
  `trace_id:span_id` for traces — so a same-count-but-different-rows answer
  still diverges), `spans=<N>` for `trace_by_id`/`trace_lookup`. ClickHouse's
  `scan` is a different projection with no comparable key and is compared by
  row count only.
- The result must be non-empty, unless the query is a documented miss
  scenario (`trace_lookup` — a cross-signal id lookup that can legitimately
  find nothing).
- The result must be identical to the cell's first valid result — a flapping
  answer across iterations is excluded, not averaged in.

Invalid iterations are dropped from p50/p95/p99 and the cell records how many
were invalid and why (`iters_valid`/`iters_invalid`/`invalid_reasons` in the
JSON; `k/N invalid: <reason>` in the table). `report.py` additionally
cross-checks each engine's result against its signal's baseline (VL/VT)
beyond a 5% count tolerance and flags "same count, different rows" when a
scan's content hash disagrees while its row count matches. Every system's
cell in the tables below carries a `k/N valid` column.

Unit/self tests for the validators: `python3 -m unittest discover -s
scripts/bench/tests` (`report.py`'s cross-system rules) and
`scripts/bench/tests/extract_result_test.sh` (`run.sh`'s per-response
extractor, against fixture bodies for every query kind/system combination,
including a malformed body). Both are self-contained (no live stack needed)
and are the ones to run before touching either file.

### Consolidated run — v3 (`scripts/bench/run.sh --signals both --s3-latency "0 100" --ranges "1h 24h" --iterations 20 --warmup 3`)

**THE current perf-gate reference** (`bench-results/baseline-2026-09/run-baseline-v3.{json,md,log}`).
Supersedes the v1 (`run-baseline.md`) and v2 (`run-baseline-traces-v2.md`)
tables below this section, which predate per-iteration validation and are
kept only for historical provenance — do not judge new PRs against them.

**60/64 cells valid (34/36 logs, 26/28 traces).** The 4 invalid cells are all
`scan`/24h (both signals, both S3-latency levels) and all for the same
reason: the baseline (VL/VT) itself flaps. `scan`'s `limit 1000` truncates a
24h result set that has far more than 1000 matching rows, with no explicit
`sort` — VictoriaLogs/VictoriaTraces return a different (same-sized) subset
of rows on repeated, otherwise-identical queries (15–19 of 20 iterations
disagreed with the first). This is a genuine, reproducible property of an
unordered top-N query over a truncated result set, not a harness defect —
the 1h `scan` cells (well under the 1000-row cap, so nothing is truncated)
are fully stable and valid. See `bench-results/baseline-2026-09/README.md`
for the full writeup; fixing it would mean adding an explicit `sort` to the
`scan` query, which changes what's measured (a sort has its own cost) and is
tracked as a follow-up, not done here.

#### Logs

**Per-query median LH vs baseline:** count_by_service 1.5×, count_total 2.5×, fulltext 3.3×, high_card 2.0×, level_filter 3.3×, multi_filter 3.8×, negation 2.4×, scan 1.1×, trace_lookup 0.7×

| query | range | S3 lat | baseline p95 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 4.9 [609] | 20/20 | 5.6 (1.1×) [609] | 20/20 | 77.1 (15.7× 🔴) [609] | 20/20 |
| count_by_service | 1h | 100ms | 3.6 [597] | 20/20 | 6.6 (1.8×) [597] | 20/20 | 89.4 (24.8× 🔴) [597] | 20/20 |
| count_by_service | 24h | 0ms | 7.0 [14285] | 20/20 | 10.0 (1.4×) [14285] | 20/20 | 76.5 (10.9× 🔴) [14285] | 20/20 |
| count_by_service | 24h | 100ms | 6.1 [14255] | 20/20 | 9.1 (1.5×) [14255] | 20/20 | 87.6 (14.4× 🔴) [14255] | 20/20 |
| count_total | 1h | 0ms | 4.9 [611] | 20/20 | 5.9 (1.2×) [609] | 20/20 | 77.1 (15.7× 🔴) [609] | 20/20 |
| count_total | 1h | 100ms | 3.8 [598] | 20/20 | 5.9 (1.6×) [598] | 20/20 | 97.5 (25.7× 🔴) [598] | 20/20 |
| count_total | 24h | 0ms | 5.1 [14285] | 20/20 | 17.2 (3.4× ⚠️) [14285] | 20/20 | 78.7 (15.4× 🔴) [14285] | 20/20 |
| count_total | 24h | 100ms | 5.0 [14255] | 20/20 | 18.6 (3.7× ⚠️) [14255] | 20/20 | 86.1 (17.2× 🔴) [14255] | 20/20 |
| fulltext | 1h | 0ms | 5.1 [49] | 20/20 | 7.4 (1.5×) [49] | 20/20 | 88.5 (17.4× 🔴) [49] | 20/20 |
| fulltext | 1h | 100ms | 3.8 [48] | 20/20 | 11.6 (3.1× ⚠️) [48] | 20/20 | 95.0 (25.0× 🔴) [48] | 20/20 |
| fulltext | 24h | 0ms | 7.1 [1217] | 20/20 | 25.4 (3.6× ⚠️) [1217] | 20/20 | 91.0 (12.8× 🔴) [1217] | 20/20 |
| fulltext | 24h | 100ms | 5.7 [1215] | 20/20 | 28.7 (5.0× ⚠️) [1215] | 20/20 | 103.7 (18.2× 🔴) [1215] | 20/20 |
| high_card | 1h | 0ms | 4.5 [606] | 20/20 | 6.2 (1.4×) [606] | 20/20 | 87.0 (19.3× 🔴) [606] | 20/20 |
| high_card | 1h | 100ms | 3.8 [596] | 20/20 | 6.3 (1.7×) [594] | 20/20 | 82.3 (21.7× 🔴) [594] | 20/20 |
| high_card | 24h | 0ms | 8.9 [14284] | 20/20 | 29.7 (3.3× ⚠️) [14284] | 20/20 | 81.5 (9.2× ⚠️) [14284] | 20/20 |
| high_card | 24h | 100ms | 11.4 [14248] | 20/20 | 27.1 (2.4×) [14248] | 20/20 | 112.9 (9.9× ⚠️) [14248] | 20/20 |
| level_filter | 1h | 0ms | 2.6 [161] | 20/20 | 7.1 (2.7×) [161] | 20/20 | 92.0 (35.4× 🔴) [161] | 20/20 |
| level_filter | 1h | 100ms | 4.6 [159] | 20/20 | 5.6 (1.2×) [159] | 20/20 | 89.5 (19.5× 🔴) [159] | 20/20 |
| level_filter | 24h | 0ms | 6.3 [3603] | 20/20 | 33.3 (5.3× ⚠️) [3603] | 20/20 | 74.5 (11.8× 🔴) [3602] | 20/20 |
| level_filter | 24h | 100ms | 6.2 [3596] | 20/20 | 23.6 (3.8× ⚠️) [3596] | 20/20 | 100.7 (16.2× 🔴) [3596] | 20/20 |
| multi_filter | 1h | 0ms | 2.2 [27] | 20/20 | 9.4 (4.3× ⚠️) [27] | 20/20 | 81.7 (37.1× 🔴) [27] | 20/20 |
| multi_filter | 1h | 100ms | 4.8 [27] | 20/20 | 6.6 (1.4×) [27] | 20/20 | 100.1 (20.9× 🔴) [27] | 20/20 |
| multi_filter | 24h | 0ms | 7.4 [710] | 20/20 | 24.6 (3.3× ⚠️) [710] | 20/20 | 104.9 (14.2× 🔴) [710] | 20/20 |
| multi_filter | 24h | 100ms | 5.5 [708] | 20/20 | 40.5 (7.4× ⚠️) [708] | 20/20 | 88.1 (16.0× 🔴) [708] | 20/20 |
| negation | 1h | 0ms | 4.1 [463] | 20/20 | 8.0 (2.0×) [463] | 20/20 | 88.1 (21.5× 🔴) [463] | 20/20 |
| negation | 1h | 100ms | 3.4 [457] | 20/20 | 6.0 (1.8×) [457] | 20/20 | 105.3 (31.0× 🔴) [457] | 20/20 |
| negation | 24h | 0ms | 7.5 [10707] | 20/20 | 21.1 (2.8×) [10707] | 20/20 | 88.2 (11.8× 🔴) [10707] | 20/20 |
| negation | 24h | 100ms | 7.3 [10682] | 20/20 | 27.4 (3.8× ⚠️) [10682] | 20/20 | 86.8 (11.9× 🔴) [10682] | 20/20 |
| scan | 1h | 0ms | 9.8 [rows=606;hash=2c52d4…] | 20/20 | 5.9 (0.6×) [rows=606;hash=2c52d4…] | 20/20 | 83.4 (8.5× ⚠️) [rows=606] | 20/20 |
| scan | 1h | 100ms | 3.9 [rows=594;hash=389453…] | 20/20 | 6.4 (1.6×) [rows=594;hash=389453…] | 20/20 | 83.7 (21.5× 🔴) [rows=594] | 20/20 |
| scan | 24h | 0ms | 3.0 [rows=1000;hash=bbdeea…] | 1/20 | ✗ baseline-19/20 invalid: flapping | 1/20 | ✗ baseline-19/20 invalid: flapping | 20/20 |
| scan | 24h | 100ms | 3.0 [rows=1000;hash=b6bbbc…] | 1/20 | ✗ baseline-19/20 invalid: flapping | 1/20 | ✗ baseline-19/20 invalid: flapping | 20/20 |
| trace_lookup | 1h | 0ms | 8.3 [spans=5] | 20/20 | 4.5 (0.5×) [spans=5] | 20/20 | 80.1 (9.7× ⚠️) [5] | 20/20 |
| trace_lookup | 1h | 100ms | 3.4 [spans=5] | 20/20 | 2.7 (0.8×) [spans=5] | 20/20 | 101.7 (29.9× 🔴) [5] | 20/20 |
| trace_lookup | 24h | 0ms | 5.7 [spans=5] | 20/20 | 5.2 (0.9×) [spans=5] | 20/20 | 76.9 (13.5× 🔴) [5] | 20/20 |
| trace_lookup | 24h | 100ms | 5.1 [spans=5] | 20/20 | 3.2 (0.6×) [spans=5] | 20/20 | 95.3 (18.7× 🔴) [5] | 20/20 |

#### Traces

**Per-query median LH vs baseline:** count_by_service 1.7×, count_total 3.2×, scan 1.2×, service_filter 2.0×, slow_spans 1.7×, span_name 2.4×, trace_by_id 2.2×

| query | range | S3 lat | baseline p95 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 4.7 [736] | 20/20 | 5.3 (1.1×) [736] | 20/20 | 93.2 (19.8× 🔴) [736] | 20/20 |
| count_by_service | 1h | 100ms | 3.2 [718] | 20/20 | 4.4 (1.4×) [718] | 20/20 | 124.8 (39.0× 🔴) [718] | 20/20 |
| count_by_service | 24h | 0ms | 4.4 [16596] | 20/20 | 8.7 (2.0×) [16596] | 20/20 | 92.9 (21.1× 🔴) [16596] | 20/20 |
| count_by_service | 24h | 100ms | 12.3 [16578] | 20/20 | 36.7 (3.0×) [16578] | 20/20 | 698.7 (56.8× 🔴) [16578] | 20/20 |
| count_total | 1h | 0ms | 2.4 [736] | 20/20 | 4.8 (2.0×) [736] | 20/20 | 91.2 (38.0× 🔴) [736] | 20/20 |
| count_total | 1h | 100ms | 4.2 [718] | 20/20 | 4.6 (1.1×) [718] | 20/20 | 114.9 (27.4× 🔴) [718] | 20/20 |
| count_total | 24h | 0ms | 2.6 [16596] | 20/20 | 11.7 (4.5× ⚠️) [16596] | 20/20 | 98.2 (37.8× 🔴) [16596] | 20/20 |
| count_total | 24h | 100ms | 4.7 [16578] | 20/20 | 21.7 (4.6× ⚠️) [16578] | 20/20 | 389.9 (83.0× 🔴) [16578] | 20/20 |
| scan | 1h | 0ms | 5.8 [rows=736;hash=2257ee…] | 20/20 | 3.9 (0.7×) [rows=736;hash=2257ee…] | 20/20 | 107.9 (18.6× 🔴) [rows=736] | 20/20 |
| scan | 1h | 100ms | 7.0 [rows=718;hash=10bb3a…] | 20/20 | 12.0 (1.7×) [rows=718;hash=10bb3a…] | 20/20 | 1270.6 (181.5× 🔴) [rows=718] | 20/20 |
| scan | 24h | 0ms | 2.4 [rows=1000;hash=700539…] | 4/20 | ✗ baseline-16/20 invalid: flapping | 1/20 | ✗ baseline-16/20 invalid: flapping | 20/20 |
| scan | 24h | 100ms | 2.9 [rows=1000;hash=17a064…] | 5/20 | ✗ baseline-15/20 invalid: flapping | 1/20 | ✗ baseline-15/20 invalid: flapping | 20/20 |
| service_filter | 1h | 0ms | 2.4 [138] | 20/20 | 4.4 (1.8×) [138] | 20/20 | 101.5 (42.3× 🔴) [138] | 20/20 |
| service_filter | 1h | 100ms | 2.2 [135] | 20/20 | 5.3 (2.4×) [135] | 20/20 | 98.2 (44.6× 🔴) [135] | 20/20 |
| service_filter | 24h | 0ms | 3.5 [3296] | 20/20 | 7.7 (2.2×) [3296] | 20/20 | 108.6 (31.0× 🔴) [3296] | 20/20 |
| service_filter | 24h | 100ms | 14.1 [3292] | 20/20 | 22.6 (1.6×) [3292] | 20/20 | 786.2 (55.8× 🔴) [3292] | 20/20 |
| slow_spans | 1h | 0ms | 3.8 [52] | 20/20 | 6.0 (1.6×) [52] | 20/20 | 98.3 (25.9× 🔴) [52] | 20/20 |
| slow_spans | 1h | 100ms | 5.0 [52] | 20/20 | 8.6 (1.7×) [52] | 20/20 | 404.3 (80.9× 🔴) [52] | 20/20 |
| slow_spans | 24h | 0ms | 2.8 [1370] | 20/20 | 9.3 (3.3× ⚠️) [1370] | 20/20 | 101.6 (36.3× 🔴) [1370] | 20/20 |
| slow_spans | 24h | 100ms | 5.7 [1365] | 20/20 | 9.8 (1.7×) [1365] | 20/20 | 122.1 (21.4× 🔴) [1365] | 20/20 |
| span_name | 1h | 0ms | 2.8 [67] | 20/20 | 4.9 (1.8×) [67] | 20/20 | 92.2 (32.9× 🔴) [67] | 20/20 |
| span_name | 1h | 100ms | 1.8 [66] | 20/20 | 5.3 (2.9×) [66] | 20/20 | 212.6 (118.1× 🔴) [66] | 20/20 |
| span_name | 24h | 0ms | 3.8 [1676] | 20/20 | 7.2 (1.9×) [1676] | 20/20 | 115.2 (30.3× 🔴) [1676] | 20/20 |
| span_name | 24h | 100ms | 1.8 [1670] | 20/20 | 8.4 (4.7× ⚠️) [1670] | 20/20 | 138.4 (76.9× 🔴) [1670] | 20/20 |
| trace_by_id | 1h | 0ms | 1.5 [spans=4] | 20/20 | 3.6 (2.4×) [spans=4] | 20/20 | 108.6 (72.4× 🔴) [spans=4] | 20/20 |
| trace_by_id | 1h | 100ms | 2.5 [spans=4] | 20/20 | 5.0 (2.0×) [spans=4] | 20/20 | 109.9 (44.0× 🔴) [spans=4] | 20/20 |
| trace_by_id | 24h | 0ms | 1.8 [spans=4] | 20/20 | 5.0 (2.8×) [spans=4] | 20/20 | 128.1 (71.2× 🔴) [spans=4] | 20/20 |
| trace_by_id | 24h | 100ms | 2.5 [spans=4] | 20/20 | 3.8 (1.5×) [spans=4] | 20/20 | 121.2 (48.5× 🔴) [spans=4] | 20/20 |

Full row-set hashes and per-iteration invalid reasons are in
`bench-results/baseline-2026-09/run-baseline-v3.md` (verbatim harness output);
truncated above for readability.

### Full-scope S3-ops (logs, e2e compose)

Pending — to be recorded once the e2e compose can be started (its host port 19428
is currently held by another project's stack; needs a human decision). No numbers
recorded here yet.
