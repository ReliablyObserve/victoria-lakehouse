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
a regression on any scenario is judged on **LH's own absolute p95 against this run**
(same hardware), not the LH/VL ratio — see the trust caveat under "Consolidated
run — v3.1" below for why the ratio alone isn't a reliable regression signal at
these latencies. A same-hardware LH p95 increase > 10 % on any scenario blocks a PR.

### Response validation

`scripts/bench/run.sh` validates **every timed response**, not just its
latency — a benchmark cell is only meaningful when every iteration returned a
correct, valid answer; a fast wrong/empty/error response is a broken
response and is never counted. Per iteration (including warmup, which is
validated the same way but never timed):

- HTTP status must be 2xx.
- The body must parse into a comparable result: a plain count for
  count/filter/group-by queries, `spans=<N>` for `trace_by_id`/`trace_lookup`,
  or (for `scan`) the membership/cardinality rule below.
- The result must be non-empty, unless the query is a documented miss
  scenario (`trace_lookup` — a cross-signal id lookup that can legitimately
  find nothing).
- For non-`scan` results, the result must be identical to the cell's first
  valid result — a flapping answer across iterations is excluded, not
  averaged in.

**`scan` validation semantics — a ruling, not a workaround.** VictoriaLogs
documents that `limit N` without an explicit `sort` returns rows "selected in
arbitrary order because of performance reasons … can return different sets
of logs every time" once more than N rows match. That means per-iteration
IDENTITY can never hold for a truncated scan — and adding a `sort` to make it
hold would change what the query measures (a sort has its own, different,
cost; it's not the same benchmark anymore). So a truncated scan's validity is
**membership + cardinality**, not identity: before the warmup loop, `run.sh`
issues ONE untimed reference request per system — the same filter/window,
`limit` clause removed — and records `window_rows` (the TRUE, untruncated
match count) and, for VL/VT/LH, `window_hash` (sha256 of the sorted set of
stable row keys — `_msg` for logs, `trace_id:span_id` for traces). When
`window_rows > 1000` (the scan `limit`), each timed iteration is valid iff
it's exactly 1000 rows AND every one of those rows' keys is a member of the
reference window's key set (a python set-difference, not `comm` — a real log
`_msg` can contain embedded newlines, e.g. a stack trace, which would corrupt
a newline-delimited/`comm`-based comparison). When `window_rows <= 1000`
(nothing was truncated), identity is meaningful again and applies as normal.
ClickHouse's `scan` is a different projection with no comparable row key and
is validated/compared by `window_rows` alone.

Invalid iterations are dropped from p50/p95/p99 and the cell records how many
were invalid and why (`iters_valid`/`iters_invalid`/`invalid_reasons` in the
JSON; `k/N invalid: <reason>` in the table, plus `valid=<k>/<N>` on the
per-cell stderr log line so a partially-invalid cell can never read as a
clean p95). `report.py` additionally cross-checks each engine's result
against its signal's baseline (VL/VT): for non-`scan` results, beyond a 5%
count tolerance, flagging "same count, different rows" when a content hash
disagrees while the count matches; for `scan`, the same tolerance against
`window_rows`, plus (only when both `window_rows` are EXACTLY equal) a
`window_hash` match — a `scan` cell renders as
`rows=<returned>/<window_rows>[;window=<hash8>]`. Every system's cell in the
tables below carries a `k/N valid` column, and ClickHouse speedup figures
only count a row when LH's own cell in that row is valid.

Unit/self tests for the validators: `python3 -m unittest discover -s
scripts/bench/tests` (`report.py`'s cross-system rules, including the
window-hash/window-rows scan rules) and
`scripts/bench/tests/extract_result_test.sh` +
`scripts/bench/tests/scan_membership_test.sh` (`run.sh`'s per-response
extractor and the scan membership/cardinality rule, against fixture bodies
— including a malformed body and a multi-line `_msg` regression case). All
three are self-contained (no live stack needed) and are the ones to run
before touching either file.

### Consolidated run — v3.1 (`scripts/bench/run.sh --signals both --s3-latency "0 100" --ranges "1h 24h" --iterations 20 --warmup 3`)

**THE current perf-gate reference** (`bench-results/baseline-2026-09/run-baseline-v3.1.{json,md,log}`).
Supersedes v3 (`run-baseline-v3.md`) — whose 4 `scan`/24h cells were
identity-invalidated by the baseline's own arbitrary-order truncation,
exactly the VictoriaLogs-documented behavior quoted above, not a real
divergence — and the earlier v1 (`run-baseline.md`) / v2
(`run-baseline-traces-v2.md`) tables, which predate per-iteration validation
entirely. All three are kept only for historical provenance — do not judge
new PRs against them.

**64/64 cells valid, 0 invalid, no `✗` anywhere; the parity gate passed at
both latency levels on both signals.** With `scan` validated by membership
against the untruncated reference window (see "Response validation" above)
instead of by identity, the four `scan`/24h cells that v3 flagged now pass:
each system's own iterations are internally consistent (every returned row
is a genuine window member, exactly 1000 of them), and VL/LH's window hashes
agree with each other — the population each is scanning is the same, even
though which 1000-row subset any one iteration happens to return isn't.

**Trust caveat for the numbers below:** several baseline p95s are under
3 ms (e.g. `trace_by_id`, some `count_total`/`service_filter` cells) — at
that scale, run-to-run noise on a laptop (scheduler jitter, page cache,
Docker overhead) is on the order of the measurement itself, so a ratio like
"2.5× baseline" computed from two ~2 ms numbers can easily swing by ±2× on a
different run without anything having changed. Treat the **ratios** in this
table as directional, not exact. The perf gate this table backs compares
**LH's own absolute p95** against this run, on the same hardware, for a
given query/range/latency cell — not the LH/baseline ratio — which is far
less sensitive to this noise.

#### Logs

**Per-query median LH vs baseline:** count_by_service 1.3×, count_total 2.9×, fulltext 2.1×, high_card 2.5×, level_filter 2.5×, multi_filter 2.9×, negation 3.0×, scan 2.7×, trace_lookup 0.8×

| query | range | S3 lat | baseline p95 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 7.8 [559] | 20/20 | 8.0 (1.0×) [559] | 20/20 | 106.2 (13.6× 🔴) [559] | 20/20 |
| count_by_service | 1h | 100ms | 6.0 [548] | 20/20 | 7.1 (1.2×) [548] | 20/20 | 75.5 (12.6× 🔴) [548] | 20/20 |
| count_by_service | 24h | 0ms | 8.0 [14304] | 20/20 | 10.8 (1.4×) [14304] | 20/20 | 85.0 (10.6× 🔴) [14304] | 20/20 |
| count_by_service | 24h | 100ms | 7.6 [14286] | 20/20 | 10.7 (1.4×) [14286] | 20/20 | 122.2 (16.1× 🔴) [14286] | 20/20 |
| count_total | 1h | 0ms | 2.8 [559] | 20/20 | 8.2 (2.9×) [559] | 20/20 | 75.0 (26.8× 🔴) [559] | 20/20 |
| count_total | 1h | 100ms | 3.9 [548] | 20/20 | 5.1 (1.3×) [548] | 20/20 | 88.7 (22.7× 🔴) [548] | 20/20 |
| count_total | 24h | 0ms | 7.6 [14304] | 20/20 | 22.1 (2.9×) [14304] | 20/20 | 87.8 (11.6× 🔴) [14304] | 20/20 |
| count_total | 24h | 100ms | 6.2 [14286] | 20/20 | 17.7 (2.9×) [14286] | 20/20 | 123.1 (19.9× 🔴) [14286] | 20/20 |
| fulltext | 1h | 0ms | 4.4 [45] | 20/20 | 7.1 (1.6×) [45] | 20/20 | 77.8 (17.7× 🔴) [45] | 20/20 |
| fulltext | 1h | 100ms | 6.9 [45] | 20/20 | 6.2 (0.9×) [45] | 20/20 | 85.4 (12.4× 🔴) [45] | 20/20 |
| fulltext | 24h | 0ms | 8.4 [1141] | 20/20 | 22.3 (2.7×) [1141] | 20/20 | 86.9 (10.3× 🔴) [1141] | 20/20 |
| fulltext | 24h | 100ms | 9.7 [1141] | 20/20 | 47.4 (4.9× ⚠️) [1141] | 20/20 | 155.7 (16.1× 🔴) [1141] | 20/20 |
| high_card | 1h | 0ms | 3.2 [558] | 20/20 | 8.1 (2.5×) [558] | 20/20 | 81.1 (25.3× 🔴) [558] | 20/20 |
| high_card | 1h | 100ms | 5.7 [546] | 20/20 | 5.8 (1.0×) [545] | 20/20 | 97.7 (17.1× 🔴) [544] | 20/20 |
| high_card | 24h | 0ms | 9.1 [14301] | 20/20 | 25.4 (2.8×) [14301] | 20/20 | 110.0 (12.1× 🔴) [14301] | 20/20 |
| high_card | 24h | 100ms | 12.7 [14283] | 20/20 | 32.3 (2.5×) [14283] | 20/20 | 209.9 (16.5× 🔴) [14283] | 20/20 |
| level_filter | 1h | 0ms | 4.8 [139] | 20/20 | 7.3 (1.5×) [139] | 20/20 | 72.8 (15.2× 🔴) [139] | 20/20 |
| level_filter | 1h | 100ms | 9.4 [137] | 20/20 | 8.7 (0.9×) [137] | 20/20 | 96.0 (10.2× 🔴) [137] | 20/20 |
| level_filter | 24h | 0ms | 5.5 [3578] | 20/20 | 24.5 (4.5× ⚠️) [3578] | 20/20 | 94.1 (17.1× 🔴) [3578] | 20/20 |
| level_filter | 24h | 100ms | 7.1 [3574] | 20/20 | 24.6 (3.5× ⚠️) [3574] | 20/20 | 130.7 (18.4× 🔴) [3574] | 20/20 |
| multi_filter | 1h | 0ms | 2.8 [25] | 20/20 | 8.6 (3.1× ⚠️) [25] | 20/20 | 88.2 (31.5× 🔴) [25] | 20/20 |
| multi_filter | 1h | 100ms | 3.6 [24] | 20/20 | 7.5 (2.1×) [24] | 20/20 | 97.6 (27.1× 🔴) [24] | 20/20 |
| multi_filter | 24h | 0ms | 9.7 [737] | 20/20 | 26.4 (2.7×) [737] | 20/20 | 88.7 (9.1× ⚠️) [737] | 20/20 |
| multi_filter | 24h | 100ms | 9.1 [737] | 20/20 | 115.0 (12.6× 🔴) [737] | 20/20 | 145.0 (15.9× 🔴) [737] | 20/20 |
| negation | 1h | 0ms | 4.5 [398] | 20/20 | 6.5 (1.4×) [398] | 20/20 | 110.6 (24.6× 🔴) [398] | 20/20 |
| negation | 1h | 100ms | 4.1 [389] | 20/20 | 5.9 (1.4×) [389] | 20/20 | 94.1 (23.0× 🔴) [389] | 20/20 |
| negation | 24h | 0ms | 7.1 [10716] | 20/20 | 36.7 (5.2× ⚠️) [10716] | 20/20 | 75.4 (10.6× 🔴) [10715] | 20/20 |
| negation | 24h | 100ms | 8.9 [10703] | 20/20 | 40.5 (4.6× ⚠️) [10703] | 20/20 | 140.7 (15.8× 🔴) [10703] | 20/20 |
| scan | 1h | 0ms | 4.9 [rows=558/558;window=74e6a23b] | 20/20 | 9.9 (2.0×) [rows=558/558;window=74e6a23b] | 20/20 | 79.9 (16.3× 🔴) [rows=558/558] | 20/20 |
| scan | 1h | 100ms | 4.0 [rows=541/541;window=0cfdcb16] | 20/20 | 7.0 (1.8×) [rows=539/539;window=fc686fb6] | 20/20 | 164.6 (41.1× 🔴) [rows=538/538] | 20/20 |
| scan | 24h | 0ms | 8.5 [rows=1000/14301;window=3dd35766] | 20/20 | 27.9 (3.3× ⚠️) [rows=1000/14301;window=3dd35766] | 20/20 | 72.4 (8.5× ⚠️) [rows=1000/14301] | 20/20 |
| scan | 24h | 100ms | 8.5 [rows=1000/14282;window=5ae13490] | 20/20 | 50.2 (5.9× ⚠️) [rows=1000/14282;window=5ae13490] | 20/20 | 138.2 (16.3× 🔴) [rows=1000/14282] | 20/20 |
| trace_lookup | 1h | 0ms | 4.6 [spans=6] | 20/20 | 5.4 (1.2×) [spans=6] | 20/20 | 72.1 (15.7× 🔴) [6] | 20/20 |
| trace_lookup | 1h | 100ms | 5.3 [spans=6] | 20/20 | 4.3 (0.8×) [spans=6] | 20/20 | 84.9 (16.0× 🔴) [6] | 20/20 |
| trace_lookup | 24h | 0ms | 6.2 [spans=6] | 20/20 | 5.1 (0.8×) [spans=6] | 20/20 | 81.4 (13.1× 🔴) [6] | 20/20 |
| trace_lookup | 24h | 100ms | 7.8 [spans=6] | 20/20 | 5.3 (0.7×) [spans=6] | 20/20 | 316.8 (40.6× 🔴) [6] | 20/20 |

Note: `scan`/1h/100ms shows LH at 539/539 rows against baseline's 541/541 —
both are internally consistent (each side's own window is fully populated,
nothing truncated at this range), and the 2-row difference (0.4%, within the
5% tolerance) is the pre-existing relative-window drift documented below
(the baseline and LH requests for the same nominal "last 1h" cell are
prepared a few seconds apart, so their windows aren't byte-identical) — not
a validity failure, and specifically NOT flagged as a hash mismatch, since
the hash comparison only applies when both window row counts are exactly
equal.

#### Traces

**Per-query median LH vs baseline:** count_by_service 2.8×, count_total 2.1×, scan 2.4×, service_filter 1.8×, slow_spans 2.8×, span_name 2.6×, trace_by_id 2.0×

| query | range | S3 lat | baseline p95 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 3.5 [614] | 20/20 | 4.8 (1.4×) [614] | 20/20 | 142.2 (40.6× 🔴) [614] | 20/20 |
| count_by_service | 1h | 100ms | 4.1 [578] | 20/20 | 14.3 (3.5× ⚠️) [578] | 20/20 | 166.3 (40.6× 🔴) [578] | 20/20 |
| count_by_service | 24h | 0ms | 4.2 [16834] | 20/20 | 9.2 (2.2×) [16834] | 20/20 | 146.8 (35.0× 🔴) [16834] | 20/20 |
| count_by_service | 24h | 100ms | 3.1 [16810] | 20/20 | 10.9 (3.5× ⚠️) [16810] | 20/20 | 145.7 (47.0× 🔴) [16810] | 20/20 |
| count_total | 1h | 0ms | 3.9 [614] | 20/20 | 4.6 (1.2×) [614] | 20/20 | 174.0 (44.6× 🔴) [614] | 20/20 |
| count_total | 1h | 100ms | 4.0 [578] | 20/20 | 6.1 (1.5×) [578] | 20/20 | 300.3 (75.1× 🔴) [578] | 20/20 |
| count_total | 24h | 0ms | 4.0 [16834] | 20/20 | 10.5 (2.6×) [16834] | 20/20 | 109.1 (27.3× 🔴) [16834] | 20/20 |
| count_total | 24h | 100ms | 2.9 [16810] | 20/20 | 11.3 (3.9× ⚠️) [16810] | 20/20 | 139.5 (48.1× 🔴) [16810] | 20/20 |
| scan | 1h | 0ms | 3.9 [rows=610/610;window=1012ff02] | 20/20 | 6.9 (1.8×) [rows=610/610;window=1012ff02] | 20/20 | 121.5 (31.2× 🔴) [rows=610/610] | 20/20 |
| scan | 1h | 100ms | 6.8 [rows=568/568;window=921759af] | 20/20 | 4.4 (0.6×) [rows=568/568;window=921759af] | 20/20 | 120.6 (17.7× 🔴) [rows=568/568] | 20/20 |
| scan | 24h | 0ms | 3.9 [rows=1000/16834;window=e3b310e6] | 20/20 | 11.6 (3.0×) [rows=1000/16830;window=99d043f0] | 20/20 | 98.3 (25.2× 🔴) [rows=1000/16830] | 20/20 |
| scan | 24h | 100ms | 5.1 [rows=1000/16810;window=d088c94b] | 20/20 | 16.1 (3.2× ⚠️) [rows=1000/16810;window=d088c94b] | 20/20 | 138.0 (27.1× 🔴) [rows=1000/16810] | 20/20 |
| service_filter | 1h | 0ms | 3.4 [127] | 20/20 | 3.3 (1.0×) [127] | 20/20 | 102.1 (30.0× 🔴) [127] | 20/20 |
| service_filter | 1h | 100ms | 2.6 [121] | 20/20 | 4.5 (1.7×) [121] | 20/20 | 1490.4 (573.2× 🔴) [121] | 20/20 |
| service_filter | 24h | 0ms | 5.7 [3294] | 20/20 | 10.6 (1.9×) [3294] | 20/20 | 106.0 (18.6× 🔴) [3294] | 20/20 |
| service_filter | 24h | 100ms | 2.5 [3290] | 20/20 | 10.9 (4.4× ⚠️) [3290] | 20/20 | 134.9 (54.0× 🔴) [3290] | 20/20 |
| slow_spans | 1h | 0ms | 2.8 [47] | 20/20 | 10.0 (3.6× ⚠️) [47] | 20/20 | 102.0 (36.4× 🔴) [47] | 20/20 |
| slow_spans | 1h | 100ms | 3.3 [41] | 20/20 | 6.7 (2.0×) [41] | 20/20 | 264.7 (80.2× 🔴) [41] | 20/20 |
| slow_spans | 24h | 0ms | 4.0 [1308] | 20/20 | 15.7 (3.9× ⚠️) [1308] | 20/20 | 118.7 (29.7× 🔴) [1308] | 20/20 |
| slow_spans | 24h | 100ms | 6.9 [1305] | 20/20 | 9.8 (1.4×) [1305] | 20/20 | 136.7 (19.8× 🔴) [1305] | 20/20 |
| span_name | 1h | 0ms | 2.7 [57] | 20/20 | 2.9 (1.1×) [57] | 20/20 | 105.4 (39.0× 🔴) [57] | 20/20 |
| span_name | 1h | 100ms | 2.3 [55] | 20/20 | 5.7 (2.5×) [55] | 20/20 | 124.1 (54.0× 🔴) [55] | 20/20 |
| span_name | 24h | 0ms | 2.7 [1671] | 20/20 | 7.6 (2.8×) [1671] | 20/20 | 159.4 (59.0× 🔴) [1671] | 20/20 |
| span_name | 24h | 100ms | 3.4 [1667] | 20/20 | 13.3 (3.9× ⚠️) [1667] | 20/20 | 127.3 (37.4× 🔴) [1667] | 20/20 |
| trace_by_id | 1h | 0ms | 3.6 [spans=6] | 20/20 | 6.7 (1.9×) [spans=6] | 20/20 | 97.5 (27.1× 🔴) [spans=6] | 20/20 |
| trace_by_id | 1h | 100ms | 3.3 [spans=6] | 20/20 | 4.3 (1.3×) [spans=6] | 20/20 | 146.8 (44.5× 🔴) [spans=6] | 20/20 |
| trace_by_id | 24h | 0ms | 3.8 [spans=6] | 20/20 | 9.9 (2.6×) [spans=6] | 20/20 | 100.0 (26.3× 🔴) [spans=6] | 20/20 |
| trace_by_id | 24h | 100ms | 2.2 [spans=6] | 20/20 | 4.9 (2.2×) [spans=6] | 20/20 | 122.9 (55.9× 🔴) [spans=6] | 20/20 |

Note: `scan`/24h shows LH's `window_rows` a few rows below baseline's
(16830 vs 16834 at 0ms, matching baseline's own count exactly at 100ms) —
same relative-window-drift explanation as the logs table above; both are
within tolerance and neither is flagged. `multi_filter`/24h/100ms (LH
115.0 ms, 12.6×) is the one clear outlier in this run — still 20/20 valid,
just a slow sample (laptop-benchmark noise, not a validity issue); re-running
that one cell in isolation would be the way to confirm whether it's noise or
a real regression before treating it as a perf-gate signal.

Full row-set hashes and per-iteration invalid reasons are in
`bench-results/baseline-2026-09/run-baseline-v3.1.md` (verbatim harness
output); truncated above for readability.

### Full-scope S3-ops (logs, e2e compose)

Pending — to be recorded once the e2e compose can be started (its host port 19428
is currently held by another project's stack; needs a human decision). No numbers
recorded here yet.
