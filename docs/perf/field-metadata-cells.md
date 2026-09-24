# Field-metadata performance cells (field_values, field_names, streams)

Validated before/after measurement of #237 (v0.143.1, "cold field_values lists
every value in range, not a sample") on the cold path, and the proposed
conformance perf rows for these cells. Measured 2026-09-23 on a developer host
(Apple M-series, 18 cores, darwin/arm64, load average 3–14 during the runs — see
[Noise](#noise)).

- **before** = `0ac43468` (v0.143.0 release commit): `field_values` fell back to
  the sampled in-RAM label index when the pmeta catalog missed; the catalog was
  looked up by request name (`level`), not Parquet column (`severity_text`);
  `streams` / `stream_ids` / scan answers were not confined to the window.
- **after** = `7bbbdd80` (v0.143.1): catalog answers only when every file in
  range is catalogued and no partition is high-card; otherwise a projected row
  scan over every file in the window, row-level window-confined.

**Rule applied throughout: a fast wrong answer is never a win.** Every
iteration's answer is compared with the dataset's known truth; only exact
iterations are timed.

## Harness

| piece | where |
|---|---|
| logs matrix + go-bench view | `internal/storage/parquets3/field_values_bench_test.go` — `TestFieldMetadataMatrix` (JSONL, env-gated), `BenchmarkFieldMetadata` (custom metrics per cell), `TestFieldMetadataMatrixVL` (hot VictoriaLogs reference) |
| traces subset | `lakehouse-traces/internal/storage/parquets3/field_values_bench_test.go` — `TestFieldMetadataMatrixTraces` |
| interleaved A/B driver | `scripts/bench/field_metadata/run.sh` (builds both trees' test binaries, alternates before/after per round, records load average) |
| aggregation + proposed rows | `scripts/bench/field_metadata/aggregate.py <matrix.jsonl> [--rows]` |

**S3 mock** (`fmS3`): in-process, deterministic. Every read (GET, ranged GET,
HEAD, LIST) sleeps a configurable first-byte latency before the first byte —
0 ms and 100 ms were measured (100 ms ≈ the toxiproxy latency in the compose
bench). It counts GETs and body bytes, and maps each returned byte range onto
the object's Parquet layout (footer metadata + offset index) to count the
distinct **row groups** and **pages** whose bytes a request returned.

**Dataset** (deterministic generator, `fmSlotRows`): two partition hours,
12 flushes × 2000 rows per hour (24 flushed objects of ~72 KB — the production
median object is 71 KiB).

| hour | `service.name` | `level` | extras |
|---|---|---|---|
| 10:00 quiet | 8 services (+ `svc-early` only in 10:00–10:10) — < 100 distinct per file | 4 levels (+ `TRACE` only 10:00–10:10) | — |
| 11:00 busy | 150 distinct per file, 700 in the hour, + `svc-a` | 4 levels (+ `FATAL` only 11:50–12:00) | — |

`svc-a` rows never carry `DEBUG` (filtered value sets differ from unfiltered)
and never carry `trace_id` (filtered `field_names` differs from unfiltered).
`_stream` = `{service.name="<svc>"}`.

**Cells** (logs): endpoint {`field_values level`, `field_values service.name`,
`field_names`, `streams`} × pmeta {on, off} × layout {`flushed` = 24 small
objects, `compacted` = the real `compaction.Compactor` run per partition, then
`PmetaOnCompacted` exactly as the scheduler hook does → 2 objects of 3 row
groups} × window {`whole` = 10:00–11:59:59.999999999, `cut` = 10:30:00.075–
11:30:00.075} × filter {none, `service.name:="svc-a"`} × S3 latency {0, 100 ms}
= 128 cells. Traces: `field_values name`, `field_values
resource_attr:service.name`, `streams` × pmeta × window × filter × latency over
flushed objects = 48 cells.

**Per iteration**: object caches cold (memCache and footerCache replaced — the
label index and the pmeta catalog are kept, as on a running node that has
served a first query), a fresh query, one call timed, the answer validated:

- `exact` — value (or field-name) set **and** every hit count equal the truth;
- `set` — the value set alone equals the truth (the catalog and the label index
  answer `hits=1`, so they can be set-exact but never exact).

Truth is computed from the generator; the harness also checks at setup that
the LH row path returns the same per-value counts for every cell (self-check),
and `TestFieldMetadataMatrixVL` shows that upstream VictoriaLogs, fed the same
rows, returns exactly that truth in 16/16 cells × 10 iterations.

**Protocol**: 5 rounds, alternating before/after (A/B, B/A, …), 2 measured
iterations per cell per round → n = 10 per cell per build; one unrecorded
warm-up iteration per cell per round at 0 ms, none at 100 ms (latency-bound).
p50/p90 are over exact iterations only.

Reproduce:

```sh
make deps-logs deps-traces deps-vt
LATENCIES_MS=0   WARMUP=1 scripts/bench/field_metadata/run.sh /tmp/fm-0ms
LATENCIES_MS=100 WARMUP=0 scripts/bench/field_metadata/run.sh /tmp/fm-100ms
python3 scripts/bench/field_metadata/aggregate.py /tmp/fm-100ms/matrix.jsonl --rows   # proposed rows
go test ./internal/storage/parquets3/ -run '^$' -bench 'FieldMetadata/fv_level/pmeta=false/layout=flushed' -benchtime 10x
```

## Results — condensed

p50 ms over **exact** iterations (n = 10 per build); `invalid` = no exact
iteration (never timed); `[x]†` = set-exact but `hits=1` (RAM answer, not
exact). GETs / bytes are per request and deterministic. Hot VictoriaLogs
(same rows, in-process upstream storage) answers every one of these cells
exactly in **0.12–0.82 ms**.

### Unfiltered requests — where #237 changed the answer path

| cell (logs) | before | after 0 ms | after 100 ms | GETs / S3 bytes after | path after |
|---|---|---|---|---|---|
| fv `level`, pmeta off, flushed, whole | invalid (index: 4–5 of 6 values) | 7.6 | 2460 | 24 / 1.64 MB | scan |
| fv `level`, pmeta off, compacted, whole | invalid (index) | 4.7 | 1842 | 18 / 555 KB | scan |
| fv `level`, pmeta on, flushed/compacted, whole | invalid (index) | [0.009]† | [0.017]† | 0 / 0 | catalog, hits=1 |
| fv `level`, pmeta on, flushed/compacted, **cut** | [µs]† 4–6/10 set | **invalid** 0/10 | **invalid** 0/10 | 0 / 0 | catalog, hour-granular: lists TRACE/FATAL from outside the window |
| fv `level`, pmeta off, flushed, cut | [µs]† 4/10 set | 4.3 | 1329 | 13 / 911 KB | scan |
| fv `service.name` (busy), pmeta on, flushed, whole | invalid (catalog: 9 of 709) | 7.8 | 2469 | 24 / 1.64 MB | scan (high-card) |
| fv `service.name`, pmeta on, compacted, whole | invalid (catalog: 9 of 709) | 5.1 | 1841 | 18 / 555 KB | scan (compaction output high-card) |
| fv `service.name`, pmeta off, compacted, cut | invalid (index: 9–159 of 408) | 5.4 | 2462 | 24 / 579 KB | scan |
| `streams`, flushed, whole | 8.2 / 2451 (already exact) | 8.0 | 2465 | 24 / 1.64 MB | scan (unchanged) |
| `streams`, flushed, cut | invalid (458 of 408: leak) | 4.7 | 1336 | 13 / 911 KB | scan |
| `streams`, compacted, cut | invalid (709 of 408: leak) | 5.8 | 2457 | **24** (before 18) / 579 KB | scan |
| `field_names`, every cell | invalid (19 or 37 names vs 8) | invalid | invalid | 24 / 1.64 MB flushed; 2 / 169 KB compacted | footer column index |
| traces fv `name`, pmeta off, flushed, whole | invalid (index) | 1.4 | **105** | 24 / 2.17 MB | scan, **8-way parallel** |
| traces fv `resource_attr:service.name`, flushed, whole | invalid (index) | 1.5 | 104 | 24 / 2.17 MB | scan, parallel |
| traces `streams`, flushed, whole | 8.5 / 2452 | 8.6 | 2464 | 24 / 2.17 MB | scan, **serial** |

### Filtered requests (`service.name:="svc-a"`)

Unchanged by #237 on every path: the catalog was never used with a filter
and the scan's cost is the same. Δ p50 0.95–1.02× at 100 ms on every
filtered cell (e.g. flushed/whole `level` 2478 → 2491 ms, compacted/whole
3298 → 3144 ms, 30–33 GETs). At 0 ms, compacted cut-window filtered cells
are 0.72–0.77× (faster) for `field_values`: the window check now runs before
the filter, so rows outside the window are no longer filter-evaluated. Other
filtered cells at 0 ms fall within 0.72–1.25×, which is inside this host's
±25 % sub-millisecond noise; do not quote a single all-cell range at 0 ms.

## Analysis

**Nothing that was exact before got slower, except one shape.** Every cell
that was exact on v0.143.0 is within noise on v0.143.1 (0.95–1.02× at
100 ms), with one exception: **compacted objects that straddle a cut window**
now read the timestamp column for the row-level window check — 18 → 24 GETs
(**+33 %**), 1838 → 2457 ms (+34 %) at 100 ms for `streams`, the time ratio
measured against the previously invalid cell's timing; the same +6 GETs apply to
`field_values` on that shape. (Before, these answers were wrong — they leaked
rows from outside the window — so it is not a regression against a valid
baseline, but it is the cost of exactness to optimise.)

**The "regressed" cells are the ones that used to answer fast and wrong.**
Every unfiltered `field_values` cell that v0.143.0 answered from RAM
(label index, or the mis-keyed/capped catalog) was exact in 0/10 — at best
set-exact with `hits=1`, depending on which file seeded the index — e.g. 9 of
709 services, 4–5 of 6 levels. v0.143.1 answers them exactly with the
projected scan: **4–8 ms with 0 ms S3, 1.3–2.5 s with 100 ms S3**, against
0.1–0.8 ms on hot VictoriaLogs. Why they reach the scan:

1. **Catalog miss because the field is busy / high-card.** A file with
   ≥ 100 distinct values of a field (`maxLabelsPerField` = 100, the label
   extractor's cap) marks the field truncated for that partition, and
   `FieldValuesExact` refuses the whole range. The catalog's own threshold is
   50 000 (`defaultCardinalityThreshold`): the 700-value busy hour would fit
   ~70× over, but it is never fed more than 100 values per file.
2. **Compaction outputs marked high-card.** `PmetaOnCompacted` feeds the
   output's `Labels`, the union of the inputs' 100-capped label lists
   (`mergeFileLabels`), so any output whose union reaches 100 is truncated —
   even though the compactor has every merged row in memory (it extracts the
   uncapped combined bloom values from them already).
3. **Full projected scan, serial, on 100 ms S3.** The logs
   `GetFieldValues` / `GetStreams` loops scan files one after another, so
   latency is GETs × first-byte: 24 GETs → 2.46 s, 13 → 1.33 s. The traces
   `GetFieldValues` scans files with a worker pool and does the same 24 GETs
   in **105 ms**; traces `GetStreams` is serial and takes 2.46 s. Objects under
   128 KB (every flushed object here, and the production median) are
   downloaded whole: `field_values level` reads **1.64 MB to decode a
   3.6 KB column** (1.5 KB of it dictionary) — ~450× byte amplification.
   Compacted objects use ranged reads: fewer bytes (555–627 KB) but more
   serial GETs per object (9–18), so compaction alone does not cut latency
   (compacted/cut 2457 ms vs flushed/cut 1336 ms).
4. **The catalog is hour-granular.** When it does answer, a window that cuts
   an hour gets the whole hour's values (0/10 exact: `TRACE`, `FATAL` from
   outside the window) and every value carries `hits=1` (never exact; set-exact
   10/10 on whole hours, ~10–20 µs).

**`field_names` is invalid on both builds in every cell** (#237 changed only
the empty-window case, which these cells do not exercise). It lists every
Parquet column with a non-null count, including `account_id`, `project_id`,
the k8s/cloud columns written as empty strings (19 names vs 8), is not
window-confined (hits = whole-file row counts), and ignores the filter
(`trace_id` listed for `svc-a`). On compacted objects read cold the answer
is **37 names**: the tail-read footer has no column index, and
`accumulateFieldHits` then credits every chunk — including all-null optional
dedicated columns — as fully non-null; the same objects answer 19 names once
a whole-file download has warmed the footer cache, so the answer depends on
cache state.

## What a tiered design would fix

| tier | fixes cells | expected effect (from the counters above) |
|---|---|---|
| **T0 — bounded parallel per-file scan** in logs `GetFieldValues` / `GetStreams` / `GetStreamIDs` and traces `GetStreams` (the traces `GetFieldValues` worker pool already exists) | every scan cell at 100 ms | measured on traces: 24 GETs 2.46 s → 0.105 s; logs flushed whole ≈ 2.46 s → ~0.1–0.3 s |
| **T1 — exact catalog sets, fed uncapped** at flush (rows are in memory) and at compaction (union exact input sets or re-extract from merged rows, as the combined bloom already is), bounded by the 50 000 `CardinalityThreshold`, not by the 100-value label cap | `field_values service.name` whole/cut, pmeta on, both layouts | busy fields become catalog-served: 1.8–2.5 s / 18–24 GETs → ~10–20 µs / 0 GETs |
| **T2 — per-value row counts in the catalog** (per partition-hour) | pmeta-on catalog cells (`hits=1` today) | set-exact → exact at ~10–20 µs for hour-aligned windows |
| **T3 — catalog for interior hours + scan only the edge partitions** of a cut window (instead of hour-granular answers) | pmeta-on cut cells (invalid today) | exact; cost = the two straddling hours only (flushed cut: 13 GETs → ≤ 13, with T0 parallel) |
| **T4 — dictionary pages for row groups wholly inside the window** (set from the dictionary, hits from the RLE index pages of the same chunk) and a column-chunk plan for sub-128 KB objects when the footer is cached (or its chunk offsets are carried in pmeta) | pmeta-off and high-card scan cells | `level`: 3.6 KB of column vs 1.64 MB whole-object downloads (~450× fewer bytes); GETs stay 1 per object |
| **T5 — page-index skipping on straddling objects**: use the timestamp column/offset index to select window pages instead of projecting the timestamp column over every row | compacted/cut cells (the +6 GETs, +33 %) | back to ≤ 18 GETs with exact window confinement |
| **T6 — bloom/statistics/dictionary pruning for filtered requests** | filtered cells | no gain on this dataset (`svc-a` is in every object — a worst case); for a selective filter, objects/row groups whose SBBF or dictionary lacks the value are skipped |
| **T7 — `field_names` from the catalog's per-partition names + row counts**, skipping columns that are empty or all-null; filter-aware only through the scan | all `field_names` cells | makes the answer exact and cache-state independent; removes the 24 whole-object downloads for flushed layouts |

## Noise

Load average was 3–7 for the 0 ms run and 3–14 for most of the 100 ms run
(27 at its very end, a local VM outside this work), 18 cores. The 100 ms cells
are latency-bound: p90/p50 ≤ 1.1 in every exact cell, and identical-code
cells land at 0.95–1.02×. At 0 ms, identical-code cells scatter 0.8–1.25×
(e.g. `streams` pmeta on/off, same path): treat 0 ms deltas inside ±25 % as
noise. Traces records carry no truth digest in the JSONL (the traces harness
validates in-process with the same scheme), so traces cells cannot be
re-checked from the raw results alone. The hot-VictoriaLogs reference ran at load ~23 — its absolute numbers are
an upper bound.

## Not measured

- A compose-level comparison through HTTP with toxiproxy
  (`scripts/bench/run.sh`): the in-process VictoriaLogs reference replaces it
  for this step; adding `field_values` scenarios to the compose bench was not
  done.
- Warm object caches (memCache/footer cache hot): every iteration here is
  cold for objects; repeated dropdown requests on a warm node skip the GETs.
- `stream_ids` (same scan as `streams`), `limit > 0`, tombstone-active
  requests, multi-tenant scopes, `s3.projected_fetch_mode=planned` (default
  `window` measured), traces compacted layouts and traces `field_names`.
- Larger scales (thousands of objects per window, wide row groups); the
  counters scale linearly with objects in the window for the scan paths.

## Findings outside #237 (not fixed here)

1. **Row path drops a row at exactly `endNs`** when that row is the minimum of
   its row group: `rowGroupMatchesTimeRange` prunes with `rgMin < endNs`
   (end-exclusive) while `_time` filters are inclusive. The harness places its
   cut bounds between rows to keep its row-path self-check valid.
2. **`field_names` depends on cache state** on objects ≥ 128 KB (37 vs 19
   names, see above).
3. **Logs `field_values` / `streams` and traces `streams` scan serially**,
   while traces `field_values` fans out — a 23× latency difference at 100 ms
   for the same GETs.

## Full matrix

Generated by `scripts/bench/field_metadata/aggregate.py`. Columns per build:
p50 / p90 ms over exact iterations, exact k/N (and set-exact count when it
differs), median GETs and S3 body bytes per request, answer path (`catalog`,
`index` = sampled label index, `scan`). `RGs/pages after` = distinct row groups
and pages whose bytes the after build fetched.

### Hot VictoriaLogs reference

Hot VictoriaLogs (in-process upstream storage, same rows, local disk):

| cell | p50 ms | p90 ms | exact k/N |
|---|---|---|---|
| vl.field_names/window=cut/filter=none | 0.168 | 0.227 | 10/10 |
| vl.field_names/window=cut/filter=svc | 0.122 | 0.144 | 10/10 |
| vl.field_names/window=whole/filter=none | 0.187 | 0.260 | 10/10 |
| vl.field_names/window=whole/filter=svc | 0.140 | 0.152 | 10/10 |
| vl.fv_level/window=cut/filter=none | 0.378 | 0.396 | 10/10 |
| vl.fv_level/window=cut/filter=svc | 0.177 | 0.217 | 10/10 |
| vl.fv_level/window=whole/filter=none | 0.420 | 0.476 | 10/10 |
| vl.fv_level/window=whole/filter=svc | 0.209 | 0.261 | 10/10 |
| vl.fv_service/window=cut/filter=none | 0.384 | 0.434 | 10/10 |
| vl.fv_service/window=cut/filter=svc | 0.148 | 0.171 | 10/10 |
| vl.fv_service/window=whole/filter=none | 0.473 | 0.533 | 10/10 |
| vl.fv_service/window=whole/filter=svc | 0.183 | 0.213 | 10/10 |
| vl.streams/window=cut/filter=none | 0.476 | 0.556 | 10/10 |
| vl.streams/window=cut/filter=svc | 0.140 | 0.185 | 10/10 |
| vl.streams/window=whole/filter=none | 0.820 | 1.0 | 10/10 |
| vl.streams/window=whole/filter=svc | 0.147 | 0.176 | 10/10 |

| cell | before p50 ms | p90 | exact k/N | GETs | S3 bytes | path | after p50 ms | p90 | exact k/N | GETs | S3 bytes | path | RGs/pages after | Δ p50 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|

† no iteration had exact hits (catalog and label-index answers carry hits=1); bracketed timing is over set-exact iterations only.

### S3 first-byte latency 0 ms

| cell | before p50 ms | p90 | exact k/N | GETs | S3 bytes | path | after p50 ms | p90 | exact k/N | GETs | S3 bytes | path | RGs/pages after | Δ p50 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fv_level/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | [0.000]† | [0.000]† | 0/10 (set 2) | 0 | 0 | index | 5.3 | 5.7 | 10/10 | 24 | 579K | scan | 6/194 | [23084.24×]† |
| fv_level/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | 14.2 | 15.9 | 10/10 | 36 | 627K | scan | 10.3 | 10.8 | 10/10 | 36 | 627K | scan | 6/194 | 0.72× |
| fv_level/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 4.7 | 5.2 | 10/10 | 18 | 555K | scan | 6/186 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | 14.2 | 14.7 | 10/10 | 36 | 627K | scan | 14.3 | 15.0 | 10/10 | 36 | 627K | scan | 6/194 | 1.01× |
| fv_level/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | [0.000]† | [0.000]† | 0/10 (set 4) | 0 | 0 | index | 4.3 | 5.2 | 10/10 | 13 | 911K | scan | 13/1053 | [34120.83×]† |
| fv_level/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 8.1 | 8.2 | 10/10 | 13 | 911K | scan | 7.9 | 9.4 | 10/10 | 13 | 911K | scan | 13/1053 | 0.98× |
| fv_level/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 7.6 | 10.0 | 10/10 | 24 | 1.64M | scan | 24/1944 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 14.8 | 16.9 | 10/10 | 24 | 1.64M | scan | 15.1 | 19.1 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.02× |
| fv_level/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | [0.001]† | [0.002]† | 0/10 (set 6) | 0 | 0 | index | invalid | invalid | 0/10 | 0 | 0 | catalog | 0/0 | after invalid |
| fv_level/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | 13.8 | 16.4 | 10/10 | 36 | 627K | scan | 10.6 | 13.2 | 10/10 | 36 | 625K | scan | 6/194 | 0.77× |
| fv_level/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | [0.002]† | [0.004]† | 0/10 (set 10) | 0 | 0 | catalog | 0/0 | before invalid |
| fv_level/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | 14.3 | 14.7 | 10/10 | 36 | 627K | scan | 14.8 | 18.0 | 10/10 | 36 | 627K | scan | 6/194 | 1.04× |
| fv_level/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | [0.004]† | [0.004]† | 0/10 (set 4) | 0 | 0 | index | invalid | invalid | 0/10 | 0 | 0 | catalog | 0/0 | after invalid |
| fv_level/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 8.1 | 12.4 | 10/10 | 13 | 911K | scan | 8.1 | 11.2 | 10/10 | 13 | 911K | scan | 13/1053 | 0.99× |
| fv_level/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | [0.009]† | [0.010]† | 0/10 (set 10) | 0 | 0 | catalog | 0/0 | before invalid |
| fv_level/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 14.8 | 14.9 | 10/10 | 24 | 1.64M | scan | 14.9 | 21.3 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.01× |
| fv_service/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 5.4 | 5.9 | 10/10 | 24 | 579K | scan | 6/182 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | 10.3 | 10.6 | 10/10 | 24 | 579K | scan | 7.8 | 12.0 | 10/10 | 24 | 579K | scan | 6/182 | 0.76× |
| fv_service/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 5.0 | 6.6 | 10/10 | 18 | 555K | scan | 6/174 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | 10.3 | 11.3 | 10/10 | 24 | 579K | scan | 10.6 | 11.1 | 10/10 | 24 | 579K | scan | 6/182 | 1.03× |
| fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 4.2 | 4.5 | 10/10 | 13 | 911K | scan | 13/1053 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 6.9 | 7.1 | 10/10 | 13 | 911K | scan | 6.8 | 7.2 | 10/10 | 13 | 911K | scan | 13/1053 | 0.98× |
| fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 7.8 | 8.2 | 10/10 | 24 | 1.64M | scan | 24/1944 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 12.7 | 13.1 | 10/10 | 24 | 1.64M | scan | 12.7 | 13.6 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.00× |
| fv_service/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | catalog | 5.6 | 6.8 | 10/10 | 24 | 579K | scan | 6/182 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | 10.4 | 10.6 | 10/10 | 24 | 579K | scan | 7.8 | 9.9 | 10/10 | 24 | 579K | scan | 6/182 | 0.75× |
| fv_service/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | catalog | 5.1 | 5.7 | 10/10 | 18 | 555K | scan | 6/174 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | 10.8 | 13.2 | 10/10 | 24 | 579K | scan | 10.6 | 12.9 | 10/10 | 24 | 579K | scan | 6/182 | 0.98× |
| fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | catalog | 4.3 | 4.6 | 10/10 | 13 | 911K | scan | 13/1053 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 6.8 | 7.3 | 10/10 | 13 | 911K | scan | 6.8 | 7.1 | 10/10 | 13 | 911K | scan | 13/1053 | 0.99× |
| fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | catalog | 7.8 | 9.2 | 10/10 | 24 | 1.64M | scan | 24/1944 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 12.7 | 13.2 | 10/10 | 24 | 1.64M | scan | 12.7 | 13.0 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.00× |
| field_names/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 13 | 911K | scan | invalid | invalid | 0/10 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/10 | 13 | 911K | scan | invalid | invalid | 0/10 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 24 | 1.64M | scan | invalid | invalid | 0/10 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/10 | 24 | 1.64M | scan | invalid | invalid | 0/10 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 13 | 911K | scan | invalid | invalid | 0/10 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | invalid | invalid | 0/10 | 13 | 911K | scan | invalid | invalid | 0/10 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 24 | 1.64M | scan | invalid | invalid | 0/10 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | invalid | invalid | 0/10 | 24 | 1.64M | scan | invalid | invalid | 0/10 | 24 | 1.64M | scan | 24/1944 | after invalid |
| streams/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 18 | 555K | scan | 5.8 | 9.1 | 10/10 | 24 | 579K | scan | 6/288 | before invalid (fast-wrong) → now exact |
| streams/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms | 14.2 | 14.9 | 10/10 | 36 | 627K | scan | 10.9 | 15.5 | 10/10 | 36 | 627K | scan | 6/300 | 0.77× |
| streams/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms | 5.3 | 6.6 | 10/10 | 18 | 555K | scan | 5.7 | 7.6 | 10/10 | 18 | 555K | scan | 6/280 | 1.08× |
| streams/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms | 14.6 | 15.4 | 10/10 | 36 | 627K | scan | 17.3 | 21.6 | 10/10 | 36 | 627K | scan | 6/300 | 1.18× |
| streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 13 | 911K | scan | 4.7 | 6.0 | 10/10 | 13 | 911K | scan | 13/1053 | before invalid (fast-wrong) → now exact |
| streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 8.2 | 8.5 | 10/10 | 13 | 911K | scan | 8.4 | 10.9 | 10/10 | 13 | 911K | scan | 13/1053 | 1.02× |
| streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | 8.2 | 9.4 | 10/10 | 24 | 1.64M | scan | 8.0 | 11.4 | 10/10 | 24 | 1.64M | scan | 24/1944 | 0.98× |
| streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 15.4 | 19.7 | 10/10 | 24 | 1.64M | scan | 15.6 | 22.8 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.01× |
| streams/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 18 | 555K | scan | 5.9 | 8.8 | 10/10 | 24 | 579K | scan | 6/288 | before invalid (fast-wrong) → now exact |
| streams/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms | 14.5 | 15.8 | 10/10 | 36 | 627K | scan | 13.0 | 16.0 | 10/10 | 36 | 627K | scan | 6/300 | 0.89× |
| streams/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms | 5.0 | 5.1 | 10/10 | 18 | 555K | scan | 5.4 | 7.2 | 10/10 | 18 | 555K | scan | 6/280 | 1.07× |
| streams/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms | 14.8 | 15.5 | 10/10 | 36 | 627K | scan | 15.0 | 19.9 | 10/10 | 36 | 627K | scan | 6/300 | 1.01× |
| streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 13 | 911K | scan | 4.6 | 6.6 | 10/10 | 13 | 911K | scan | 13/1053 | before invalid (fast-wrong) → now exact |
| streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 8.3 | 9.7 | 10/10 | 13 | 911K | scan | 10.4 | 12.9 | 10/10 | 13 | 911K | scan | 13/1053 | 1.25× |
| streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | 7.5 | 10.1 | 10/10 | 24 | 1.64M | scan | 9.4 | 13.2 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.25× |
| streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 15.6 | 16.1 | 10/10 | 24 | 1.64M | scan | 15.9 | 19.9 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.02× |
| traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 0.856 | 0.957 | 10/10 | 13 | 1.18M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 1.4 | 1.5 | 10/10 | 13 | 1.18M | scan | 1.4 | 2.6 | 10/10 | 13 | 1.18M | scan | — | 1.07× |
| traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 1.4 | 2.0 | 10/10 | 24 | 2.17M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 3.3 | 5.3 | 10/10 | 24 | 2.17M | scan | 2.7 | 4.7 | 10/10 | 24 | 2.17M | scan | — | 0.81× |
| traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | [0.007]† | [0.008]† | 0/10 (set 6) | 0 | 0 | index | invalid | invalid | 0/10 | 0 | 0 | catalog | — | after invalid |
| traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 2.0 | 3.0 | 10/10 | 13 | 1.18M | scan | 2.0 | 3.0 | 10/10 | 13 | 1.18M | scan | — | 0.96× |
| traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | [0.014]† | [0.019]† | 0/10 (set 10) | 0 | 0 | catalog | — | before invalid |
| traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 2.3 | 4.2 | 10/10 | 24 | 2.17M | scan | 2.2 | 2.2 | 10/10 | 24 | 2.17M | scan | — | 0.92× |
| traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 0.953 | 1.1 | 10/10 | 13 | 1.18M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 1.4 | 2.9 | 10/10 | 13 | 1.18M | scan | 1.2 | 1.4 | 10/10 | 13 | 1.18M | scan | — | 0.86× |
| traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 1.5 | 2.4 | 10/10 | 24 | 2.17M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 1.8 | 2.0 | 10/10 | 24 | 2.17M | scan | 2.0 | 3.6 | 10/10 | 24 | 2.17M | scan | — | 1.10× |
| traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 1.0 | 1.2 | 10/10 | 13 | 1.18M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 1.2 | 2.1 | 10/10 | 13 | 1.18M | scan | 1.3 | 1.6 | 10/10 | 13 | 1.18M | scan | — | 1.04× |
| traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | invalid | invalid | 0/10 | 0 | 0 | index | 1.4 | 1.6 | 10/10 | 24 | 2.17M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 2.3 | 3.9 | 10/10 | 24 | 2.17M | scan | 1.9 | 2.2 | 10/10 | 24 | 2.17M | scan | — | 0.84× |
| traces.streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 13 | 1.18M | scan | 4.6 | 5.0 | 10/10 | 13 | 1.18M | scan | — | before invalid (fast-wrong) → now exact |
| traces.streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms | 8.7 | 9.5 | 10/10 | 13 | 1.18M | scan | 8.5 | 9.6 | 10/10 | 13 | 1.18M | scan | — | 0.97× |
| traces.streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms | 8.5 | 9.4 | 10/10 | 24 | 2.17M | scan | 8.6 | 11.6 | 10/10 | 24 | 2.17M | scan | — | 1.01× |
| traces.streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms | 16.1 | 16.6 | 10/10 | 24 | 2.17M | scan | 16.8 | 18.7 | 10/10 | 24 | 2.17M | scan | — | 1.04× |
| traces.streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms | invalid | invalid | 0/10 | 13 | 1.18M | scan | 4.7 | 5.4 | 10/10 | 13 | 1.18M | scan | — | before invalid (fast-wrong) → now exact |
| traces.streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms | 8.4 | 9.5 | 10/10 | 13 | 1.18M | scan | 8.2 | 8.4 | 10/10 | 13 | 1.18M | scan | — | 0.97× |
| traces.streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms | 8.3 | 9.0 | 10/10 | 24 | 2.17M | scan | 8.4 | 8.5 | 10/10 | 24 | 2.17M | scan | — | 1.01× |
| traces.streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms | 15.9 | 17.2 | 10/10 | 24 | 2.17M | scan | 16.2 | 17.2 | 10/10 | 24 | 2.17M | scan | — | 1.02× |

† no iteration had exact hits (catalog and label-index answers carry hits=1); bracketed timing is over set-exact iterations only.

### S3 first-byte latency 100 ms

| cell | before p50 ms | p90 | exact k/N | GETs | S3 bytes | path | after p50 ms | p90 | exact k/N | GETs | S3 bytes | path | RGs/pages after | Δ p50 |
|---|---|---|---|---|---|---|---|---|---|---|---|---|---|---|
| fv_level/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | [0.002]† | [0.002]† | 0/10 (set 2) | 0 | 0 | index | 2459 | 2474 | 10/10 | 24 | 579K | scan | 6/194 | [1616804.64×]† |
| fv_level/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | 3282 | 3400 | 10/10 | 32 | 611K | scan | 3329 | 3699 | 10/10 | 32 | 613K | scan | 6/194 | 1.01× |
| fv_level/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 1842 | 1851 | 10/10 | 18 | 555K | scan | 6/186 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | 3298 | 3557 | 10/10 | 32 | 611K | scan | 3144 | 3397 | 10/10 | 30 | 605K | scan | 6/194 | 0.95× |
| fv_level/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 1329 | 1337 | 10/10 | 13 | 911K | scan | 13/1053 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 1346 | 1352 | 10/10 | 13 | 911K | scan | 1344 | 1351 | 10/10 | 13 | 911K | scan | 13/1053 | 1.00× |
| fv_level/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 2460 | 2473 | 10/10 | 24 | 1.64M | scan | 24/1944 | before invalid (fast-wrong) → now exact |
| fv_level/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 2478 | 2485 | 10/10 | 24 | 1.64M | scan | 2491 | 2499 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.01× |
| fv_level/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | [0.008]† | [0.013]† | 0/10 (set 4) | 0 | 0 | index | invalid | invalid | 0/10 | 0 | 0 | catalog | 0/0 | after invalid |
| fv_level/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | 3391 | 3485 | 10/10 | 33 | 615K | scan | 3294 | 3379 | 10/10 | 32 | 611K | scan | 6/194 | 0.97× |
| fv_level/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | [0.011]† | [0.016]† | 0/10 (set 10) | 0 | 0 | catalog | 0/0 | before invalid |
| fv_level/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | 3232 | 3487 | 10/10 | 32 | 609K | scan | 3187 | 3317 | 10/10 | 31 | 607K | scan | 6/194 | 0.99× |
| fv_level/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | [0.013]† | [0.030]† | 0/10 (set 6) | 0 | 0 | index | invalid | invalid | 0/10 | 0 | 0 | catalog | 0/0 | after invalid |
| fv_level/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 1344 | 1354 | 10/10 | 13 | 911K | scan | 1348 | 1352 | 10/10 | 13 | 911K | scan | 13/1053 | 1.00× |
| fv_level/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | [0.017]† | [0.023]† | 0/10 (set 10) | 0 | 0 | catalog | 0/0 | before invalid |
| fv_level/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 2467 | 2485 | 10/10 | 24 | 1.64M | scan | 2484 | 2500 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.01× |
| fv_service/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 2462 | 2469 | 10/10 | 24 | 579K | scan | 6/182 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | 2455 | 2473 | 10/10 | 24 | 579K | scan | 2454 | 2472 | 10/10 | 24 | 579K | scan | 6/182 | 1.00× |
| fv_service/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 1846 | 1858 | 10/10 | 18 | 555K | scan | 6/174 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | 2465 | 2472 | 10/10 | 24 | 579K | scan | 2469 | 2517 | 10/10 | 24 | 579K | scan | 6/182 | 1.00× |
| fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 1333 | 1338 | 10/10 | 13 | 911K | scan | 13/1053 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 1345 | 1352 | 10/10 | 13 | 911K | scan | 1345 | 1351 | 10/10 | 13 | 911K | scan | 13/1053 | 1.00× |
| fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 2469 | 2474 | 10/10 | 24 | 1.64M | scan | 24/1944 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 2462 | 2483 | 10/10 | 24 | 1.64M | scan | 2480 | 2496 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.01× |
| fv_service/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | catalog | 2456 | 2462 | 10/10 | 24 | 579K | scan | 6/182 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | 2462 | 2472 | 10/10 | 24 | 579K | scan | 2468 | 2473 | 10/10 | 24 | 579K | scan | 6/182 | 1.00× |
| fv_service/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | catalog | 1841 | 1847 | 10/10 | 18 | 555K | scan | 6/174 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | 2466 | 2474 | 10/10 | 24 | 579K | scan | 2461 | 2474 | 10/10 | 24 | 579K | scan | 6/182 | 1.00× |
| fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | catalog | 1336 | 1341 | 10/10 | 13 | 911K | scan | 13/1053 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 1339 | 1347 | 10/10 | 13 | 911K | scan | 1348 | 1350 | 10/10 | 13 | 911K | scan | 13/1053 | 1.01× |
| fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | catalog | 2469 | 2491 | 10/10 | 24 | 1.64M | scan | 24/1944 | before invalid (fast-wrong) → now exact |
| fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 2472 | 2489 | 10/10 | 24 | 1.64M | scan | 2477 | 2495 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.00× |
| field_names/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 13 | 911K | scan | invalid | invalid | 0/10 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/10 | 13 | 911K | scan | invalid | invalid | 0/10 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 24 | 1.64M | scan | invalid | invalid | 0/10 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/10 | 24 | 1.64M | scan | invalid | invalid | 0/10 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/10 | 2 | 169K | scan | invalid | invalid | 0/10 | 2 | 169K | scan | 2/144 | after invalid |
| field_names/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 13 | 911K | scan | invalid | invalid | 0/10 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | invalid | invalid | 0/10 | 13 | 911K | scan | invalid | invalid | 0/10 | 13 | 911K | scan | 13/1053 | after invalid |
| field_names/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 24 | 1.64M | scan | invalid | invalid | 0/10 | 24 | 1.64M | scan | 24/1944 | after invalid |
| field_names/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | invalid | invalid | 0/10 | 24 | 1.64M | scan | invalid | invalid | 0/10 | 24 | 1.64M | scan | 24/1944 | after invalid |
| streams/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 18 | 555K | scan | 2457 | 2465 | 10/10 | 24 | 579K | scan | 6/288 | before invalid (fast-wrong) → now exact |
| streams/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms | 3697 | 3712 | 10/10 | 36 | 627K | scan | 3687 | 3720 | 10/10 | 36 | 627K | scan | 6/300 | 1.00× |
| streams/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms | 1838 | 1860 | 10/10 | 18 | 555K | scan | 1849 | 1886 | 10/10 | 18 | 555K | scan | 6/280 | 1.01× |
| streams/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms | 3676 | 3699 | 10/10 | 36 | 627K | scan | 3703 | 3724 | 10/10 | 36 | 627K | scan | 6/300 | 1.01× |
| streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 13 | 911K | scan | 1336 | 1353 | 10/10 | 13 | 911K | scan | 13/1053 | before invalid (fast-wrong) → now exact |
| streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 1343 | 1348 | 10/10 | 13 | 911K | scan | 1346 | 1360 | 10/10 | 13 | 911K | scan | 13/1053 | 1.00× |
| streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | 2451 | 2462 | 10/10 | 24 | 1.64M | scan | 2465 | 2476 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.01× |
| streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 2458 | 2481 | 10/10 | 24 | 1.64M | scan | 2494 | 2499 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.01× |
| streams/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 18 | 555K | scan | 2453 | 2466 | 10/10 | 24 | 579K | scan | 6/288 | before invalid (fast-wrong) → now exact |
| streams/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms | 3679 | 3704 | 10/10 | 36 | 627K | scan | 3694 | 3701 | 10/10 | 36 | 627K | scan | 6/300 | 1.00× |
| streams/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms | 1839 | 1851 | 10/10 | 18 | 555K | scan | 1847 | 1852 | 10/10 | 18 | 555K | scan | 6/280 | 1.00× |
| streams/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms | 3692 | 3714 | 10/10 | 36 | 627K | scan | 3704 | 3707 | 10/10 | 36 | 627K | scan | 6/300 | 1.00× |
| streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 13 | 911K | scan | 1333 | 1341 | 10/10 | 13 | 911K | scan | 13/1053 | before invalid (fast-wrong) → now exact |
| streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 1339 | 1350 | 10/10 | 13 | 911K | scan | 1345 | 1355 | 10/10 | 13 | 911K | scan | 13/1053 | 1.00× |
| streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | 2449 | 2470 | 10/10 | 24 | 1.64M | scan | 2462 | 2469 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.01× |
| streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 2478 | 2493 | 10/10 | 24 | 1.64M | scan | 2484 | 2500 | 10/10 | 24 | 1.64M | scan | 24/1944 | 1.00× |
| traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | [0.004]† | [0.005]† | 0/10 (set 6) | 0 | 0 | index | 103 | 105 | 10/10 | 13 | 1.18M | scan | — | [26000.28×]† |
| traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 105 | 108 | 10/10 | 13 | 1.18M | scan | 105 | 109 | 10/10 | 13 | 1.18M | scan | — | 1.00× |
| traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 105 | 109 | 10/10 | 24 | 2.17M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 106 | 113 | 10/10 | 24 | 2.17M | scan | 108 | 112 | 10/10 | 24 | 2.17M | scan | — | 1.01× |
| traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | [0.016]† | [0.021]† | 0/10 (set 10) | 0 | 0 | index | invalid | invalid | 0/10 | 0 | 0 | catalog | — | after invalid |
| traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 106 | 107 | 10/10 | 13 | 1.18M | scan | 105 | 108 | 10/10 | 13 | 1.18M | scan | — | 1.00× |
| traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | [0.044]† | [0.269]† | 0/10 (set 10) | 0 | 0 | catalog | — | before invalid |
| traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 105 | 106 | 10/10 | 24 | 2.17M | scan | 107 | 125 | 10/10 | 24 | 2.17M | scan | — | 1.02× |
| traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 104 | 109 | 10/10 | 13 | 1.18M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 105 | 109 | 10/10 | 13 | 1.18M | scan | 105 | 107 | 10/10 | 13 | 1.18M | scan | — | 1.00× |
| traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 104 | 106 | 10/10 | 24 | 2.17M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 106 | 112 | 10/10 | 24 | 2.17M | scan | 106 | 111 | 10/10 | 24 | 2.17M | scan | — | 1.00× |
| traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 105 | 105 | 10/10 | 13 | 1.18M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 105 | 108 | 10/10 | 13 | 1.18M | scan | 103 | 106 | 10/10 | 13 | 1.18M | scan | — | 0.98× |
| traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | invalid | invalid | 0/10 | 0 | 0 | index | 106 | 107 | 10/10 | 24 | 2.17M | scan | — | before invalid (fast-wrong) → now exact |
| traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 109 | 113 | 10/10 | 24 | 2.17M | scan | 108 | 111 | 10/10 | 24 | 2.17M | scan | — | 0.99× |
| traces.streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 13 | 1.18M | scan | 1339 | 1351 | 10/10 | 13 | 1.18M | scan | — | before invalid (fast-wrong) → now exact |
| traces.streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms | 1342 | 1350 | 10/10 | 13 | 1.18M | scan | 1342 | 1353 | 10/10 | 13 | 1.18M | scan | — | 1.00× |
| traces.streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms | 2452 | 2460 | 10/10 | 24 | 2.17M | scan | 2464 | 2473 | 10/10 | 24 | 2.17M | scan | — | 1.00× |
| traces.streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms | 2475 | 2495 | 10/10 | 24 | 2.17M | scan | 2481 | 2500 | 10/10 | 24 | 2.17M | scan | — | 1.00× |
| traces.streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms | invalid | invalid | 0/10 | 13 | 1.18M | scan | 1329 | 1336 | 10/10 | 13 | 1.18M | scan | — | before invalid (fast-wrong) → now exact |
| traces.streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms | 1339 | 1347 | 10/10 | 13 | 1.18M | scan | 1343 | 1352 | 10/10 | 13 | 1.18M | scan | — | 1.00× |
| traces.streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms | 2455 | 2461 | 10/10 | 24 | 2.17M | scan | 2466 | 2481 | 10/10 | 24 | 2.17M | scan | — | 1.00× |
| traces.streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms | 2476 | 2490 | 10/10 | 24 | 2.17M | scan | 2484 | 2496 | 10/10 | 24 | 2.17M | scan | — | 1.00× |

† no iteration had exact hits (catalog and label-index answers carry hits=1); bracketed timing is over set-exact iterations only.

## Three-way results: Lakehouse vs disk VictoriaLogs/VictoriaTraces vs ClickHouse on S3

`scripts/bench/run.sh --queries "fv_level fv_service streams_list fv_name"` on the benchmark stack
(MinIO behind toxiproxy, 7 days of seeded data, the same Parquet read by Lakehouse and ClickHouse),
10 timed iterations per cell after 2 warm-ups, every response validated. Raw results and the
generated report: `bench-results/field-metadata-2026-09-24/run.{json,md}`. The ClickHouse side runs
`SELECT <column> AS value, count() AS hits ... GROUP BY value` over the same window; all three systems
reduce to a hash of sorted (value, hits) pairs, so a sample or wrong hits never validates.

| Query | Window | Hot VL/VT p50 (ms) | Lakehouse p50 (ms) | ClickHouse p50 (ms) | Answer |
|---|---|---|---|---|---|
| logs `field_values level` | 1h / 6h / 24h, 0 ms S3 | 3.5 / 4.3 / 9.7 | 3.4 / 1.5 / 3.0 | 208 / 157 / 187 | **Lakehouse wrong**: every value has hits=1 (total 4 vs 553 / 14 160) |
| logs `field_values level` | same, 100 ms S3 | 1.7 / 2.3 / 3.9 | 1.1 / 1.5 / 1.2 | 84 / 83 / 74 | **Lakehouse wrong** (same) |
| logs `field_values service.name` | 1h / 6h / 24h, 0 ms S3 | 3.6 / 3.4 / 9.1 | 1.7 / 1.5 / 1.8 | 760 / 144 / 179 | **Lakehouse wrong** (hits=1) |
| logs `streams` | 1h / 6h / 24h, 0 ms S3 | 12.0 / 19.9 / 82.9 | 14.5 / 63.6 / 187.6 | 121.9 / 211.6 / 112.7 | exact; **ClickHouse faster at 24h** |
| logs `streams` | same, 100 ms S3 | 3.1 / 18.5 / 44.1 | 6.8 / 36.8 / 141.5 | 78.3 / 80.6 / 103.7 | exact; **ClickHouse faster at 24h** |
| traces `field_values name` | 1h / 6h / 24h, 0 ms S3 | 1.6 / 1.8 / 2.1 | 1.8 / 3.2 / 7.1 | 90 / 118 / 89 | exact |
| traces `field_values service` | 1h / 6h / 24h, 100 ms S3 | 1.6 / 1.5 / 1.9 | 1.5 / 2.3 / 7.2 | 89 / 94 / 117 | exact |

What this says, in the order the tiered design fixes it:

1. **Logs `field_values` hits are wrong on every cell.** The catalog answers with hits=1 per value;
   VictoriaLogs and ClickHouse agree on the real counts. The answer is fast (≈1–3 ms) and invalid —
   per-value counts in the catalog (design PR 2) make it exact at the same speed.
2. **Logs `streams` over 24h is slower than ClickHouse** (188 vs 113 ms; 142 vs 104 ms at 100 ms S3)
   and 2–3× slower than disk VictoriaLogs — the serial per-file scan (design PR 3) and the dictionary
   / page tiers (PR 5) are what close it.
3. **Traces `field_values` is exact and 10–60× faster than ClickHouse**; 1h/6h are within 1–2× of disk
   VictoriaTraces, 24h is ~3.5× (7 vs 2 ms) — the catalog-feed and dictionary tiers target that gap.

Not a field-metadata result, recorded because the run surfaced it: during seeding the traces flush
converged to 16 196 of 18 501 spans (ratio 0.875) and stayed there — a real gap, not lag, tracked
separately. Logs converged exactly (14 042 = 14 042).

## Proposed registry perf rows (not wired)

`tests/conformance/registry/schema.go` has no `perf` surface and `Row` has no
field for a latency budget or counters (only the `perf` layer name exists), and
the seed list has no field-metadata dataset. The
rows below are therefore a proposal, kept here and not under
`tests/conformance/registry/rows/`. Wiring them needs (user review — a
registry extension):

- a `perf` block on `Row`: `cell` (harness cell name), `budget`
  (`p50_ms`, `p90_ms`, `valid` = required exact k/N), `counters`
  (`s3_gets`, `s3_bytes`, `row_groups`, `pages`, `path`) — counters are
  deterministic in the harness and can be gated exactly, latencies with a
  tolerance the runner owns;
- seeds `logs.fieldmeta` / `traces.fieldmeta` = the harness generators
  (`fmSlotRows`, `fmtSlotRows`);
- a runner that executes `TestFieldMetadataMatrix[Traces]` and joins its JSONL
  on `perf.cell`.

Budgets are the v0.143.1 ("after") measurements above. Cells that are not
exact at v0.143.1 are `expect: differ` with no latency budget (a wrong answer
gets no budget); their counters are still recorded. Regenerate with
`aggregate.py <matrix.jsonl> --rows`.

<details><summary>176 proposed rows</summary>

```yaml
- { id: vl.perf.field_names.pmeta_off.layout_compacted.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_compacted.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_compacted.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_compacted.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_compacted.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_compacted.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_compacted.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_compacted.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_flushed.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms", counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_flushed.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms", counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms", counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms", counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_flushed.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms", counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_flushed.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms", counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms", counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.field_names.pmeta_off.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms", counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_compacted.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_compacted.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_compacted.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_compacted.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_compacted.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_compacted.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_compacted.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_compacted.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms", counters: { s3_gets: 2, s3_bytes: 173159, row_groups: 2, pages: 144, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_flushed.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms", counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_flushed.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms", counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms", counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms", counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_flushed.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms", counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_flushed.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms", counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms", counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.field_names.pmeta_on.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_names }, request: { method: GET, path: /select/logsql/field_names, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "field_names/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms", counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_compacted.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms", budget: { p50_ms: 5.286, p90_ms: 5.744, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_compacted.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms", budget: { p50_ms: 2459.160, p90_ms: 2474.069, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_compacted.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 10.255, p90_ms: 10.775, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_compacted.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 3329.197, p90_ms: 3699.235, valid: "10/10" }, counters: { s3_gets: 32, s3_bytes: 628031, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_compacted.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms", budget: { p50_ms: 4.695, p90_ms: 5.229, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 186, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_compacted.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms", budget: { p50_ms: 1841.505, p90_ms: 1850.798, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 186, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_compacted.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 14.303, p90_ms: 15.043, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_compacted.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 3143.728, p90_ms: 3397.307, valid: "10/10" }, counters: { s3_gets: 30, s3_bytes: 619839, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_flushed.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 4.265, p90_ms: 5.237, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_flushed.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 1328.852, p90_ms: 1336.959, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 7.874, p90_ms: 9.445, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 1344.047, p90_ms: 1350.638, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_flushed.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 7.590, p90_ms: 10.026, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_flushed.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 2459.945, p90_ms: 2472.723, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 15.119, p90_ms: 19.139, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_level.pmeta_off.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2490.542, p90_ms: 2499.458, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_level.pmeta_on.layout_compacted.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms", counters: { s3_gets: 0, s3_bytes: 0, row_groups: 0, pages: 0, path: catalog } } }
- { id: vl.perf.fv_level.pmeta_on.layout_compacted.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms", counters: { s3_gets: 0, s3_bytes: 0, row_groups: 0, pages: 0, path: catalog } } }
- { id: vl.perf.fv_level.pmeta_on.layout_compacted.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 10.586, p90_ms: 13.195, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 640319, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_on.layout_compacted.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 3294.219, p90_ms: 3378.837, valid: "10/10" }, counters: { s3_gets: 32, s3_bytes: 625983, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_on.layout_compacted.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: hits=1 from a RAM index",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms", counters: { s3_gets: 0, s3_bytes: 0, row_groups: 0, pages: 0, path: catalog } } }
- { id: vl.perf.fv_level.pmeta_on.layout_compacted.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: hits=1 from a RAM index",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms", counters: { s3_gets: 0, s3_bytes: 0, row_groups: 0, pages: 0, path: catalog } } }
- { id: vl.perf.fv_level.pmeta_on.layout_compacted.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 14.811, p90_ms: 17.975, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_on.layout_compacted.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 3187.004, p90_ms: 3316.969, valid: "10/10" }, counters: { s3_gets: 31, s3_bytes: 621887, row_groups: 6, pages: 194, path: scan } } }
- { id: vl.perf.fv_level.pmeta_on.layout_flushed.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms", counters: { s3_gets: 0, s3_bytes: 0, row_groups: 0, pages: 0, path: catalog } } }
- { id: vl.perf.fv_level.pmeta_on.layout_flushed.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms", counters: { s3_gets: 0, s3_bytes: 0, row_groups: 0, pages: 0, path: catalog } } }
- { id: vl.perf.fv_level.pmeta_on.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 8.055, p90_ms: 11.155, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_level.pmeta_on.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 1347.982, p90_ms: 1351.834, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_level.pmeta_on.layout_flushed.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: hits=1 from a RAM index",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms", counters: { s3_gets: 0, s3_bytes: 0, row_groups: 0, pages: 0, path: catalog } } }
- { id: vl.perf.fv_level.pmeta_on.layout_flushed.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: hits=1 from a RAM index",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms", counters: { s3_gets: 0, s3_bytes: 0, row_groups: 0, pages: 0, path: catalog } } }
- { id: vl.perf.fv_level.pmeta_on.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 14.929, p90_ms: 21.328, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_level.pmeta_on.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "level"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_level/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2483.746, p90_ms: 2500.256, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_compacted.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms", budget: { p50_ms: 5.429, p90_ms: 5.934, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_compacted.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms", budget: { p50_ms: 2462.149, p90_ms: 2468.970, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_compacted.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 7.776, p90_ms: 11.990, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_compacted.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 2453.870, p90_ms: 2472.167, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_compacted.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms", budget: { p50_ms: 5.027, p90_ms: 6.555, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 174, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_compacted.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms", budget: { p50_ms: 1845.671, p90_ms: 1857.978, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 174, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_compacted.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 10.647, p90_ms: 11.119, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_compacted.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2468.851, p90_ms: 2516.894, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_flushed.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 4.201, p90_ms: 4.549, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_flushed.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 1333.253, p90_ms: 1337.559, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 6.753, p90_ms: 7.208, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 1344.882, p90_ms: 1350.567, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_flushed.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 7.787, p90_ms: 8.155, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_flushed.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 2469.484, p90_ms: 2474.237, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 12.722, p90_ms: 13.644, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_service.pmeta_off.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2480.360, p90_ms: 2495.602, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_compacted.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms", budget: { p50_ms: 5.583, p90_ms: 6.777, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_compacted.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms", budget: { p50_ms: 2455.870, p90_ms: 2462.441, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_compacted.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 7.774, p90_ms: 9.885, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_compacted.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 2468.499, p90_ms: 2472.674, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_compacted.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms", budget: { p50_ms: 5.089, p90_ms: 5.749, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 174, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_compacted.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms", budget: { p50_ms: 1840.974, p90_ms: 1847.118, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 174, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_compacted.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 10.575, p90_ms: 12.907, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_compacted.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2460.747, p90_ms: 2473.741, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 182, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_flushed.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 4.289, p90_ms: 4.598, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_flushed.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 1335.757, p90_ms: 1341.398, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 6.765, p90_ms: 7.110, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 1347.604, p90_ms: 1349.883, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_flushed.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 7.791, p90_ms: 9.249, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_flushed.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 2469.191, p90_ms: 2490.708, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 12.667, p90_ms: 13.028, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.fv_service.pmeta_on.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2477.349, p90_ms: 2495.112, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_compacted.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=compacted/window=cut/filter=none/s3=0ms", budget: { p50_ms: 5.849, p90_ms: 9.068, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 288, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_compacted.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=compacted/window=cut/filter=none/s3=100ms", budget: { p50_ms: 2457.355, p90_ms: 2464.551, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 288, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_compacted.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=compacted/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 10.950, p90_ms: 15.466, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 300, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_compacted.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=compacted/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 3687.375, p90_ms: 3719.646, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 300, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_compacted.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=compacted/window=whole/filter=none/s3=0ms", budget: { p50_ms: 5.716, p90_ms: 7.635, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 280, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_compacted.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=compacted/window=whole/filter=none/s3=100ms", budget: { p50_ms: 1849.012, p90_ms: 1885.678, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 280, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_compacted.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=compacted/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 17.252, p90_ms: 21.567, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 300, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_compacted.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=compacted/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 3703.123, p90_ms: 3724.472, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 300, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_flushed.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 4.702, p90_ms: 5.987, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_flushed.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 1336.322, p90_ms: 1353.110, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 8.361, p90_ms: 10.886, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 1345.683, p90_ms: 1359.883, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_flushed.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 8.045, p90_ms: 11.421, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_flushed.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 2465.444, p90_ms: 2475.717, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 15.562, p90_ms: 22.776, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.streams.pmeta_off.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2493.867, p90_ms: 2499.255, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_compacted.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=compacted/window=cut/filter=none/s3=0ms", budget: { p50_ms: 5.871, p90_ms: 8.755, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 288, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_compacted.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=compacted/window=cut/filter=none/s3=100ms", budget: { p50_ms: 2453.486, p90_ms: 2466.229, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 593215, row_groups: 6, pages: 288, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_compacted.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=compacted/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 13.009, p90_ms: 16.010, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 300, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_compacted.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=compacted/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 3694.147, p90_ms: 3700.940, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 300, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_compacted.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=compacted/window=whole/filter=none/s3=0ms", budget: { p50_ms: 5.366, p90_ms: 7.207, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 280, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_compacted.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=compacted/window=whole/filter=none/s3=100ms", budget: { p50_ms: 1846.610, p90_ms: 1851.557, valid: "10/10" }, counters: { s3_gets: 18, s3_bytes: 568639, row_groups: 6, pages: 280, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_compacted.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=compacted/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 15.049, p90_ms: 19.938, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 300, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_compacted.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=compacted/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 3703.621, p90_ms: 3707.047, valid: "10/10" }, counters: { s3_gets: 36, s3_bytes: 642367, row_groups: 6, pages: 300, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_flushed.window_cut.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 4.590, p90_ms: 6.610, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_flushed.window_cut.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 1332.808, p90_ms: 1340.522, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 10.442, p90_ms: 12.878, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 1344.708, p90_ms: 1354.769, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 932821, row_groups: 13, pages: 1053, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_flushed.window_whole.filter_none.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 9.365, p90_ms: 13.178, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_flushed.window_whole.filter_none.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 2461.957, p90_ms: 2468.619, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 15.867, p90_ms: 19.944, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vl.perf.streams.pmeta_on.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vl, kind: select, origin: native, targets: [cold], seed: [logs.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2484.438, p90_ms: 2500.241, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 1719972, row_groups: 24, pages: 1944, path: scan } } }
- { id: vt.perf.fv_name.pmeta_off.layout_flushed.window_cut.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 0.856, p90_ms: 0.957, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_name.pmeta_off.layout_flushed.window_cut.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 103.468, p90_ms: 104.963, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_name.pmeta_off.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 1.446, p90_ms: 2.587, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_name.pmeta_off.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 105.173, p90_ms: 109.332, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_name.pmeta_off.layout_flushed.window_whole.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 1.368, p90_ms: 1.972, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_name.pmeta_off.layout_flushed.window_whole.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 105.289, p90_ms: 109.213, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_name.pmeta_off.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 2.661, p90_ms: 4.665, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_name.pmeta_off.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 107.937, p90_ms: 111.514, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_name.pmeta_on.layout_flushed.window_cut.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms", counters: { s3_gets: 0, s3_bytes: 0, path: catalog } } }
- { id: vt.perf.fv_name.pmeta_on.layout_flushed.window_cut.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: value set is not the window's",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms", counters: { s3_gets: 0, s3_bytes: 0, path: catalog } } }
- { id: vt.perf.fv_name.pmeta_on.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 1.963, p90_ms: 3.018, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_name.pmeta_on.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 105.268, p90_ms: 107.537, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_name.pmeta_on.layout_flushed.window_whole.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: hits=1 from a RAM index",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms", counters: { s3_gets: 0, s3_bytes: 0, path: catalog } } }
- { id: vt.perf.fv_name.pmeta_on.layout_flushed.window_whole.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: differ, differ_note: "0/10 exact at v0.143.1: hits=1 from a RAM index",
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms", counters: { s3_gets: 0, s3_bytes: 0, path: catalog } } }
- { id: vt.perf.fv_name.pmeta_on.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 2.159, p90_ms: 2.233, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_name.pmeta_on.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_name/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 106.571, p90_ms: 125.396, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_service.pmeta_off.layout_flushed.window_cut.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 0.953, p90_ms: 1.067, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_service.pmeta_off.layout_flushed.window_cut.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 103.906, p90_ms: 109.081, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_service.pmeta_off.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 1.234, p90_ms: 1.394, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_service.pmeta_off.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 104.850, p90_ms: 107.068, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_service.pmeta_off.layout_flushed.window_whole.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 1.502, p90_ms: 2.368, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_service.pmeta_off.layout_flushed.window_whole.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 103.593, p90_ms: 106.404, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_service.pmeta_off.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 2.033, p90_ms: 3.612, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_service.pmeta_off.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 106.042, p90_ms: 110.795, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_service.pmeta_on.layout_flushed.window_cut.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 1.024, p90_ms: 1.216, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_service.pmeta_on.layout_flushed.window_cut.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 104.587, p90_ms: 105.196, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_service.pmeta_on.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 1.284, p90_ms: 1.627, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_service.pmeta_on.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 103.271, p90_ms: 105.575, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.fv_service.pmeta_on.layout_flushed.window_whole.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 1.357, p90_ms: 1.636, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_service.pmeta_on.layout_flushed.window_whole.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "*", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 105.654, p90_ms: 107.122, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_service.pmeta_on.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 1.900, p90_ms: 2.153, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.fv_service.pmeta_on.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/field_values }, request: { method: GET, path: /select/logsql/field_values, params: {"query": "service.name:=\"svc-a\"", "field": "resource_attr:service.name"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.fv_service/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 107.618, p90_ms: 110.967, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.streams.pmeta_off.layout_flushed.window_cut.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 4.580, p90_ms: 4.963, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.streams.pmeta_off.layout_flushed.window_cut.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=false/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 1338.897, p90_ms: 1350.662, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.streams.pmeta_off.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 8.477, p90_ms: 9.613, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.streams.pmeta_off.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=false/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 1342.173, p90_ms: 1353.319, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.streams.pmeta_off.layout_flushed.window_whole.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 8.556, p90_ms: 11.606, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.streams.pmeta_off.layout_flushed.window_whole.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=false/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 2463.508, p90_ms: 2473.168, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.streams.pmeta_off.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 16.822, p90_ms: 18.703, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.streams.pmeta_off.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=false/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2481.178, p90_ms: 2499.973, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.streams.pmeta_on.layout_flushed.window_cut.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=0ms", budget: { p50_ms: 4.677, p90_ms: 5.409, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.streams.pmeta_on.layout_flushed.window_cut.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=true/layout=flushed/window=cut/filter=none/s3=100ms", budget: { p50_ms: 1329.438, p90_ms: 1335.993, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.streams.pmeta_on.layout_flushed.window_cut.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=0ms", budget: { p50_ms: 8.226, p90_ms: 8.447, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.streams.pmeta_on.layout_flushed.window_cut.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=true/layout=flushed/window=cut/filter=svc/s3=100ms", budget: { p50_ms: 1343.313, p90_ms: 1352.393, valid: "10/10" }, counters: { s3_gets: 13, s3_bytes: 1233107, path: scan } } }
- { id: vt.perf.streams.pmeta_on.layout_flushed.window_whole.filter_none.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=0ms", budget: { p50_ms: 8.406, p90_ms: 8.532, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.streams.pmeta_on.layout_flushed.window_whole.filter_none.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "*"} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=true/layout=flushed/window=whole/filter=none/s3=100ms", budget: { p50_ms: 2465.530, p90_ms: 2481.033, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.streams.pmeta_on.layout_flushed.window_whole.filter_svc.s3_0ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=0ms", budget: { p50_ms: 16.210, p90_ms: 17.215, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
- { id: vt.perf.streams.pmeta_on.layout_flushed.window_whole.filter_svc.s3_100ms, surface: vt, kind: select, origin: native, targets: [cold], seed: [traces.fieldmeta], layers: [perf], pending: true, expect: pass,
    upstream: { route: /select/logsql/streams }, request: { method: GET, path: /select/logsql/streams, params: {"query": "service.name:=\"svc-a\""} },
    compare: { type: values-with-hits, options: { hits_tolerance: "0" } }, refs: { doc: docs/perf/field-metadata-cells.md },
    perf: { cell: "traces.streams/pmeta=true/layout=flushed/window=whole/filter=svc/s3=100ms", budget: { p50_ms: 2483.920, p90_ms: 2496.385, valid: "10/10" }, counters: { s3_gets: 24, s3_bytes: 2275473, path: scan } } }
```

</details>

## Verification

Every headline cell was re-derived independently from the raw results and two
cells were re-run on separate builds (before/after interleaved); harness
latency, cold caches, counters, the generator-derived truth and the upstream
VictoriaLogs reference were checked in code. The corrections above (+33 % GETs,
the 0 ms filtered-cell range, the traces truth digest) came from that check.
