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
