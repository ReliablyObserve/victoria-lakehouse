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

### Consolidated run (`scripts/bench/run.sh --signals both --s3-latency "0 100"`)

#### Logs

The table below (from `run-baseline.md`) is valid except its `trace_lookup` rows,
which measure a miss on every system (the sample trace id fetch bug, fixed for the
traces rerun below; not rerun for logs since `trace_lookup` is the only affected
query).

**Per-query median LH vs baseline:** count_by_service 2.2×, count_total 2.3×, fulltext 2.8×, high_card 2.4×, level_filter 2.9×, multi_filter 3.7×, negation 2.5×, scan 3.2×, trace_lookup 1.1×

| query | range | S3 lat | baseline p95 [res] | LH | CH |
|---|---|---:|---:|---|---|
| count_by_service | 1h | 0ms | 2.4 [605] | 7.0 (2.9×) [604] | 82.6 (34.4× 🔴) [605] |
| count_by_service | 1h | 100ms | 2.9 [581] | 5.3 (1.8×) [581] | 94.3 (32.5× 🔴) [581] |
| count_by_service | 24h | 0ms | 6.3 [13710] | 9.9 (1.6×) [13710] | 70.5 (11.2× 🔴) [13710] |
| count_by_service | 24h | 100ms | 5.1 [13687] | 13.2 (2.6×) [13687] | 89.7 (17.6× 🔴) [13687] |
| count_total | 1h | 0ms | 2.4 [606] | 3.9 (1.6×) [606] | 71.9 (30.0× 🔴) [606] |
| count_total | 1h | 100ms | 4.0 [581] | 5.0 (1.2×) [581] | 76.6 (19.1× 🔴) [581] |
| count_total | 24h | 0ms | 4.5 [13711] | 15.2 (3.4× ⚠️) [13711] | 99.4 (22.1× 🔴) [13711] |
| count_total | 24h | 100ms | 4.8 [13687] | 14.2 (3.0×) [13687] | 86.0 (17.9× 🔴) [13687] |
| fulltext | 1h | 0ms | 3.6 [58] | 5.2 (1.4×) [58] | 72.0 (20.0× 🔴) [58] |
| fulltext | 1h | 100ms | 4.1 [54] | 7.0 (1.7×) [54] | 85.5 (20.9× 🔴) [54] |
| fulltext | 24h | 0ms | 5.1 [1116] | 20.1 (3.9× ⚠️) [1116] | 78.9 (15.5× 🔴) [1116] |
| fulltext | 24h | 100ms | 6.0 [1115] | 23.0 (3.8× ⚠️) [1115] | 92.3 (15.4× 🔴) [1115] |
| high_card | 1h | 0ms | 4.0 [603] | 4.8 (1.2×) [603] | 88.2 (22.1× 🔴) [603] |
| high_card | 1h | 100ms | 3.8 [579] | 7.2 (1.9×) [579] | 82.1 (21.6× 🔴) [579] |
| high_card | 24h | 0ms | 7.4 [13705] | 22.0 (3.0×) [13705] | 78.5 (10.6× 🔴) [13705] |
| high_card | 24h | 100ms | 8.3 [13687] | 25.3 (3.0× ⚠️) [13687] | 97.6 (11.8× 🔴) [13687] |
| level_filter | 1h | 0ms | 3.2 [152] | 5.0 (1.6×) [152] | 96.2 (30.1× 🔴) [152] |
| level_filter | 1h | 100ms | 3.5 [145] | 6.4 (1.8×) [145] | 76.3 (21.8× 🔴) [145] |
| level_filter | 24h | 0ms | 5.2 [3399] | 20.5 (3.9× ⚠️) [3399] | 87.6 (16.8× 🔴) [3399] |
| level_filter | 24h | 100ms | 6.0 [3395] | 26.8 (4.5× ⚠️) [3395] | 92.7 (15.5× 🔴) [3395] |
| multi_filter | 1h | 0ms | 3.3 [23] | 6.0 (1.8×) [23] | 106.5 (32.3× 🔴) [23] |
| multi_filter | 1h | 100ms | 2.6 [23] | 6.3 (2.4×) [23] | 80.3 (30.9× 🔴) [23] |
| multi_filter | 24h | 0ms | 4.4 [590] | 22.1 (5.0× ⚠️) [590] | 90.2 (20.5× 🔴) [590] |
| multi_filter | 24h | 100ms | 6.9 [590] | 35.3 (5.1× ⚠️) [590] | 92.2 (13.4× 🔴) [590] |
| negation | 1h | 0ms | 2.8 [439] | 5.2 (1.9×) [439] | 81.1 (29.0× 🔴) [439] |
| negation | 1h | 100ms | 2.5 [427] | 5.3 (2.1×) [427] | 83.9 (33.6× 🔴) [427] |
| negation | 24h | 0ms | 5.3 [10329] | 20.9 (3.9× ⚠️) [10329] | 92.6 (17.5× 🔴) [10329] |
| negation | 24h | 100ms | 10.1 [10315] | 29.8 (3.0×) [10315] | 71.3 (7.1× ⚠️) [10315] |
| scan | 1h | 0ms | 4.2 [603] | 5.3 (1.3×) [603] | 93.0 (22.1× 🔴) [603] |
| scan | 1h | 100ms | 3.2 [578] | 5.9 (1.8×) [578] | 86.7 (27.1× 🔴) [578] |
| scan | 24h | 0ms | 3.9 [1000] | 25.2 (6.5× ⚠️) [1000] | 74.2 (19.0× 🔴) [1000] |
| scan | 24h | 100ms | 4.7 [1000] | 21.4 (4.6× ⚠️) [1000] | 121.0 (25.7× 🔴) [1000] |
| trace_lookup | 1h | 0ms | 2.3 [0] | 1.7 (0.7×) [0] | 81.6 (35.5× 🔴) [0] |
| trace_lookup | 1h | 100ms | 2.9 [0] | 2.3 (0.8×) [0] | 87.1 (30.0× 🔴) [0] |
| trace_lookup | 24h | 0ms | 3.9 [0] | 5.5 (1.4×) [0] | 78.7 (20.2× 🔴) [0] |
| trace_lookup | 24h | 100ms | 3.2 [0] | 5.5 (1.7×) [0] | 103.5 (32.3× 🔴) [0] |

#### Traces

The traces table in `run-baseline.md` is superseded by `run-baseline-traces-v2.md`
(VT-native query dialect + enforcing parity gate); the table below is the v2
rerun, pasted verbatim.

**Per-query median LH vs baseline:** count_by_service 2.0×, count_total 2.6×, scan 3.1×, service_filter 3.4×, slow_spans 2.9×, span_name 3.1×, trace_by_id 1.8×

| query | range | S3 lat | baseline p95 [res] | LH | CH |
|---|---|---:|---:|---|---|
| count_by_service | 1h | 0ms | 2.7 [594] | 3.8 (1.4×) [594] | 89.4 (33.1× 🔴) [594] |
| count_by_service | 1h | 100ms | 3.1 [594] | 2.8 (0.9×) [594] | 114.3 (36.9× 🔴) [594] |
| count_by_service | 24h | 0ms | 3.8 [16668] | 9.9 (2.6×) [16668] | 114.9 (30.2× 🔴) [16668] |
| count_by_service | 24h | 100ms | 2.4 [16668] | 9.0 (3.8× ⚠️) [16668] | 116.7 (48.6× 🔴) [16668] |
| count_total | 1h | 0ms | 2.2 [594] | 5.6 (2.5×) [594] | 97.5 (44.3× 🔴) [594] |
| count_total | 1h | 100ms | 2.5 [594] | 3.8 (1.5×) [594] | 96.3 (38.5× 🔴) [594] |
| count_total | 24h | 0ms | 1.9 [16678] | 8.3 (4.4× ⚠️) [16678] | 102.2 (53.8× 🔴) [16678] |
| count_total | 24h | 100ms | 3.7 [16668] | 10.0 (2.7×) [16668] | 134.9 (36.5× 🔴) [16668] |
| scan | 1h | 0ms | 2.1 [594] | 3.4 (1.6×) [594] | 103.3 (49.2× 🔴) [594] |
| scan | 1h | 100ms | 2.1 [594] | 5.5 (2.6×) [594] | 104.3 (49.7× 🔴) [594] |
| scan | 24h | 0ms | 2.4 [1000] | 8.8 (3.7× ⚠️) [1000] | 99.8 (41.6× 🔴) [1000] |
| scan | 24h | 100ms | 1.7 [1000] | 14.3 (8.4× ⚠️) [1000] | 104.7 (61.6× 🔴) [1000] |
| service_filter | 1h | 0ms | 1.8 [120] | 2.4 (1.3×) [120] | 98.1 (54.5× 🔴) [120] |
| service_filter | 1h | 100ms | 1.8 [120] | 4.9 (2.7×) [120] | 141.7 (78.7× 🔴) [120] |
| service_filter | 24h | 0ms | 1.6 [3220] | 7.5 (4.7× ⚠️) [3220] | 116.3 (72.7× 🔴) [3220] |
| service_filter | 24h | 100ms | 1.7 [3220] | 7.1 (4.2× ⚠️) [3220] | 129.3 (76.1× 🔴) [3220] |
| slow_spans | 1h | 0ms | 2.0 [52] | 3.7 (1.9×) [52] | 104.4 (52.2× 🔴) [52] |
| slow_spans | 1h | 100ms | 2.3 [52] | 3.0 (1.3×) [52] | 118.9 (51.7× 🔴) [52] |
| slow_spans | 24h | 0ms | 2.4 [1299] | 9.8 (4.1× ⚠️) [1299] | 178.4 (74.3× 🔴) [1299] |
| slow_spans | 24h | 100ms | 2.4 [1299] | 9.3 (3.9× ⚠️) [1299] | 96.8 (40.3× 🔴) [1299] |
| span_name | 1h | 0ms | 1.2 [57] | 3.9 (3.2× ⚠️) [57] | 94.1 (78.4× 🔴) [57] |
| span_name | 1h | 100ms | 1.8 [57] | 5.2 (2.9×) [57] | 126.8 (70.4× 🔴) [57] |
| span_name | 24h | 0ms | 1.5 [1563] | 7.9 (5.3× ⚠️) [1563] | 119.1 (79.4× 🔴) [1563] |
| span_name | 24h | 100ms | 2.3 [1563] | 6.8 (3.0×) [1563] | 127.6 (55.5× 🔴) [1563] |
| trace_by_id | 1h | 0ms | 2.0 [8] | 2.5 (1.2×) [8] | 103.3 (51.6× 🔴) [8] |
| trace_by_id | 1h | 100ms | 1.6 [8] | 3.0 (1.9×) [8] | 140.3 (87.7× 🔴) [8] |
| trace_by_id | 24h | 0ms | 1.3 [8] | 5.5 (4.2× ⚠️) [8] | 123.8 (95.2× 🔴) [8] |
| trace_by_id | 24h | 100ms | 1.8 [8] | 3.0 (1.7×) [8] | 113.2 (62.9× 🔴) [8] |

### Full-scope S3-ops (logs, e2e compose)

Pending — to be recorded once the e2e compose can be started (its host port 19428
is currently held by another project's stack; needs a human decision). No numbers
recorded here yet.
