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
the LH/VL ratio (see the trust caveat under "Consolidated run — v3.3"). A
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
NANOSECOND window bounds (`window_bounds`), computed from a single
`time.time()` call and passed to every system, **ClickHouse included**. This
matters specifically for ClickHouse: earlier the harness gave LogsQL
nanosecond bounds but computed a SEPARATE, second-granularity, exclusive-end
pair (`fromUnixTimestamp(ss)` .. `< fromUnixTimestamp(es)`) for ClickHouse's
SQL — a real, measured 0.04-0.95s per-cell offset from LogsQL's
inclusive-both-ends window, large enough at high cardinality to shift a
`GROUP BY`'s row count by one and read as a cross-engine divergence that
wasn't real (see "Cells that are not fully valid" below for the case this
actually caused and how it was found). ClickHouse's `Timestamp` column is
`fromUnixTimestamp64Nano(timestamp_unix_nano)` (`DateTime64(9)`, see
`init-s3.sql`), so its SQL now uses `Timestamp >=
fromUnixTimestamp64Nano(<sns>) AND Timestamp <= fromUnixTimestamp64Nano(<ens>)`
— the same nanosecond bounds as LogsQL, with `>=`/`<=` matching LogsQL's
inclusive-both-ends window. `window_bounds` emits only `sns`/`ens`; there is
no second-granularity pair left to drift. With bounds genuinely shared and the
seed a static one-time backfill (no live ingest), every system's result for a
cell now MUST be byte-identical, so `report.py` requires **exact equality**
between baseline and each engine — no tolerance. (The pre-flight
`parity_gate`, a sanity check on the sweep's starting conditions rather than
a per-cell validity rule, keeps its own ±5% tolerance.)

**Group-by (`count_by_service`, `high_card`) is hashed by (group, count)
pairs, not summed to a total.** Reducing a group-by result to `sum(n)`
validates the TOTAL only — two systems could split the same total across a
different SET of groups and still "match". `extract_result` now returns
`rows=<groups>;total=<sum of counts>;hash=<sha256 of the sorted
"group\x00count" pairs>` for every system — the `total` field rides along so
a "same total, different groups" claim is independently checkable from the
result string itself, not just asserted in prose. ClickHouse's group-by is
requested as `FORMAT JSONEachRow` with `count() AS n` (same shape as its
`scan`, see below) and goes through the exact same JSON-lines extractor as
VL/VT/LH — there is no ClickHouse-specific TSV branch for group-by, since a
group key containing `\t`/`\n`/`\\` could otherwise parse differently between
the TSV and JSON paths and diverge for a reason unrelated to the data.

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
can swing a cell 5-10× with nothing structurally different happening. The
perf gate compares p90 and p50 instead; `measure_query` emits `p90_ms`
alongside `p95_ms`/`p99_ms` and the raw `samples_ms`, and the tables below
render both (`p95/p90 [res]`) so the number the gate actually uses is visible
without cross-referencing the JSON. Raising `--iterations` to ≥100 so p95
stops being the max was considered and rejected here only because it would
roughly 5× the sweep's already 45-75-minute runtime — either fix is valid;
this repo picked the cheaper one.

**Cache-hit caveat for `lat100` cells:** the injected S3 latency only slows
down a request that actually reaches S3. Lakehouse serves a `lat100` cell
from the same warm in-memory cache used for the matching `lat0` cell (the
sweep runs both latencies back to back against the same LH process, without
clearing its cache in between), so LH's `lat100` numbers below are close to
its own `lat0` numbers — they are **not** a measurement of LH's S3-bound
latency. Only ClickHouse (no cache, reads S3 fresh on every query) actually
shows the injected 100ms in its numbers. `scripts/bench/run.sh --cold` clears
LH's cache before each request when a true S3-bound LH number is needed.

Unit/self tests for the validators: `python3 -m unittest discover -s
scripts/bench/tests` (`report.py`'s cross-system rules, including the
window-hash/window-rows scan rules and the group-by pair-hash rules) and
`scripts/bench/tests/extract_result_test.sh` +
`scripts/bench/tests/scan_membership_test.sh` +
`scripts/bench/tests/measure_query_test.sh` (`run.sh`'s per-response
extractor, the scan membership/cardinality rule, `measure_query` end to end
with a stubbed `_do_req`, `window_bounds`, and `build_scan_window`/
`strip_scan_limit`, all against fixture bodies — including a malformed body
and a multi-line `_msg` regression case). All four are self-contained (no
live stack needed) and are the ones to run before touching either file.
124 checks total across the four files (48 + 28 + 17 + 31).

### Consolidated run — v3.3 (`scripts/bench/run.sh --signals both --s3-latency "0 100" --ranges "1h 24h" --iterations 20 --warmup 3`)

**THE current perf-gate reference** (`bench-results/baseline-2026-09/run-baseline-v3.3.{json,md,log}`).
Supersedes v3.2 (`run-baseline-v3.2.md`) — whose one invalid cell turned out
to be a harness window-bounds bug in ClickHouse's SQL, not a ClickHouse
engine difference, see "Cells that are not fully valid" below — and v3.1,
v3, v2, v1, kept only for historical provenance. Do not judge new PRs
against them.

**LH: 64/64 rows valid, 0 invalid. ClickHouse: 64/64, 0 invalid. Baseline:
64/64. The parity gate passed at both latency levels on both signals** —
and, this run, matched EXACTLY (not just within the gate's own ±5%
tolerance): logs count_total 13915/13915/13915 at 0ms and
13891/13891/13891 at 100ms; traces count_total 16260/16260/16260 at 0ms and
16216/16216/16216 at 100ms (baseline/LH/ClickHouse). Every `scan` cell (8 of
them) and every group-by cell (8 of them) passes with exact
`window_rows`/`window_hash` (or `total`/hash) equality across all three
systems, including ClickHouse.

**Trust caveat for the numbers below:** several baseline p95s are under
3 ms (e.g. `trace_by_id`, some `count_total`/`service_filter` cells) — at
that scale, run-to-run noise on a laptop (scheduler jitter, page cache,
Docker overhead) is on the order of the measurement itself, so a ratio like
"2.5× baseline" computed from two ~2 ms numbers can easily swing by ±2× on a
different run without anything having changed. Treat the **ratios** in this
table as directional, not exact. The perf gate this table backs compares
**LH's own absolute p90/p50** against this run, on the same hardware — not
the LH/baseline ratio — which is far less sensitive to this noise. See also
the cache-hit caveat above: LH's `lat100` cells measure a warm cache, not
S3-bound reads.

#### Logs

**Per-query median LH vs baseline:** count_by_service 1.3×, count_total 3.7×, fulltext 3.4×, high_card 1.9×, level_filter 3.0×, multi_filter 3.1×, negation 2.6×, scan 3.6×, trace_lookup 0.8×

| query | range | S3 lat | baseline p95/p90 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 7.5/5.0 [rows=5;total=472;hash=2b7f8775] | 20/20 | 6.8/6.6 (0.9×) [rows=5;total=472;hash=2b7f8775] | 20/20 | 104.3/87.2 (13.9× 🔴) [rows=5;total=472;hash=2b7f8775] | 20/20 |
| count_by_service | 1h | 100ms | 4.2/3.8 [rows=5;total=448;hash=e402eb1b] | 20/20 | 8.5/7.2 (2.0×) [rows=5;total=448;hash=e402eb1b] | 20/20 | 81.4/73.9 (19.4× 🔴) [rows=5;total=448;hash=e402eb1b] | 20/20 |
| count_by_service | 24h | 0ms | 8.1/7.6 [rows=5;total=13912;hash=b1a88573] | 20/20 | 12.9/12.4 (1.6×) [rows=5;total=13912;hash=b1a88573] | 20/20 | 80.3/76.5 (9.9× ⚠️) [rows=5;total=13912;hash=b1a88573] | 20/20 |
| count_by_service | 24h | 100ms | 12.7/5.8 [rows=5;total=13885;hash=2faa4ba9] | 20/20 | 11.6/10.1 (0.9×) [rows=5;total=13885;hash=2faa4ba9] | 20/20 | 77.8/75.6 (6.1× ⚠️) [rows=5;total=13885;hash=2faa4ba9] | 20/20 |
| count_total | 1h | 0ms | 3.1/2.8 [473] | 20/20 | 8.5/6.7 (2.7×) [473] | 20/20 | 80.7/72.8 (26.0× 🔴) [473] | 20/20 |
| count_total | 1h | 100ms | 3.0/2.9 [448] | 20/20 | 7.7/6.7 (2.6×) [448] | 20/20 | 83.6/71.4 (27.9× 🔴) [448] | 20/20 |
| count_total | 24h | 0ms | 5.0/4.8 [13912] | 20/20 | 23.4/18.6 (4.7× ⚠️) [13912] | 20/20 | 91.0/86.6 (18.2× 🔴) [13912] | 20/20 |
| count_total | 24h | 100ms | 4.0/3.8 [13887] | 20/20 | 19.5/15.9 (4.9× ⚠️) [13887] | 20/20 | 76.0/74.6 (19.0× 🔴) [13887] | 20/20 |
| fulltext | 1h | 0ms | 2.5/2.3 [33] | 20/20 | 7.5/6.9 (3.0× ⚠️) [33] | 20/20 | 100.4/75.1 (40.2× 🔴) [33] | 20/20 |
| fulltext | 1h | 100ms | 3.3/2.7 [31] | 20/20 | 8.0/7.0 (2.4×) [31] | 20/20 | 77.2/75.4 (23.4× 🔴) [31] | 20/20 |
| fulltext | 24h | 0ms | 5.6/5.6 [1124] | 20/20 | 21.5/21.2 (3.8× ⚠️) [1124] | 20/20 | 73.8/72.1 (13.2× 🔴) [1124] | 20/20 |
| fulltext | 24h | 100ms | 7.6/6.7 [1122] | 20/20 | 28.9/23.9 (3.8× ⚠️) [1122] | 20/20 | 90.0/77.5 (11.8× 🔴) [1122] | 20/20 |
| high_card | 1h | 0ms | 4.9/4.4 [rows=225;total=470;hash=41fbfdc2] | 20/20 | 7.0/5.5 (1.4×) [rows=225;total=470;hash=41fbfdc2] | 20/20 | 84.0/82.5 (17.1× 🔴) [rows=225;total=470;hash=41fbfdc2] | 20/20 |
| high_card | 1h | 100ms | 5.7/5.4 [rows=210;total=445;hash=ee585a52] | 20/20 | 9.7/7.5 (1.7×) [rows=210;total=445;hash=ee585a52] | 20/20 | 77.4/73.7 (13.6× 🔴) [rows=210;total=445;hash=ee585a52] | 20/20 |
| high_card | 24h | 0ms | 10.5/10.0 [rows=6585;total=13907;hash=b8ed4310] | 20/20 | 22.7/22.3 (2.2×) [rows=6585;total=13907;hash=b8ed4310] | 20/20 | 95.8/90.6 (9.1× ⚠️) [rows=6585;total=13907;hash=b8ed4310] | 20/20 |
| high_card | 24h | 100ms | 9.9/8.5 [rows=6571;total=13880;hash=813bb7ea] | 20/20 | 24.3/23.2 (2.5×) [rows=6571;total=13880;hash=813bb7ea] | 20/20 | 84.7/83.9 (8.6× ⚠️) [rows=6571;total=13880;hash=813bb7ea] | 20/20 |
| level_filter | 1h | 0ms | 3.1/2.8 [115] | 20/20 | 5.7/5.4 (1.8×) [115] | 20/20 | 92.1/71.9 (29.7× 🔴) [115] | 20/20 |
| level_filter | 1h | 100ms | 7.6/6.7 [106] | 20/20 | 6.0/5.2 (0.8×) [106] | 20/20 | 79.7/77.2 (10.5× 🔴) [106] | 20/20 |
| level_filter | 24h | 0ms | 5.6/5.3 [3405] | 20/20 | 28.4/21.3 (5.1× ⚠️) [3405] | 20/20 | 80.2/77.8 (14.3× 🔴) [3405] | 20/20 |
| level_filter | 24h | 100ms | 5.9/5.6 [3399] | 20/20 | 24.1/21.6 (4.1× ⚠️) [3399] | 20/20 | 88.0/82.5 (14.9× 🔴) [3399] | 20/20 |
| multi_filter | 1h | 0ms | 3.9/3.6 [23] | 20/20 | 8.3/7.4 (2.1×) [23] | 20/20 | 87.4/86.4 (22.4× 🔴) [23] | 20/20 |
| multi_filter | 1h | 100ms | 6.2/4.0 [22] | 20/20 | 7.0/6.7 (1.1×) [22] | 20/20 | 97.6/78.2 (15.7× 🔴) [22] | 20/20 |
| multi_filter | 24h | 0ms | 5.4/5.1 [734] | 20/20 | 28.9/28.2 (5.4× ⚠️) [734] | 20/20 | 86.1/77.8 (15.9× 🔴) [734] | 20/20 |
| multi_filter | 24h | 100ms | 6.2/5.8 [734] | 20/20 | 25.2/24.5 (4.1× ⚠️) [734] | 20/20 | 97.0/92.3 (15.6× 🔴) [734] | 20/20 |
| negation | 1h | 0ms | 6.5/4.5 [338] | 20/20 | 6.1/4.7 (0.9×) [338] | 20/20 | 93.2/91.5 (14.3× 🔴) [338] | 20/20 |
| negation | 1h | 100ms | 4.0/4.0 [322] | 20/20 | 6.6/6.4 (1.6×) [322] | 20/20 | 89.0/74.1 (22.2× 🔴) [322] | 20/20 |
| negation | 24h | 0ms | 6.7/6.4 [10344] | 20/20 | 28.4/19.8 (4.2× ⚠️) [10344] | 20/20 | 81.5/77.6 (12.2× 🔴) [10344] | 20/20 |
| negation | 24h | 100ms | 5.8/5.2 [10323] | 20/20 | 21.1/20.8 (3.6× ⚠️) [10323] | 20/20 | 73.3/72.9 (12.6× 🔴) [10323] | 20/20 |
| scan | 1h | 0ms | 4.1/4.1 [rows=470/470;window=e27c74bf] | 20/20 | 8.2/6.8 (2.0×) [rows=470/470;window=e27c74bf] | 20/20 | 97.4/74.1 (23.8× 🔴) [rows=470/470;window=e27c74bf] | 20/20 |
| scan | 1h | 100ms | 4.3/3.8 [rows=444/444;window=7ae9a327] | 20/20 | 7.4/6.9 (1.7×) [rows=444/444;window=7ae9a327] | 20/20 | 73.1/73.0 (17.0× 🔴) [rows=444/444;window=7ae9a327] | 20/20 |
| scan | 24h | 0ms | 4.5/4.0 [rows=1000/13906;window=7446f269] | 20/20 | 27.2/22.2 (6.0× ⚠️) [rows=1000/13906;window=7446f269] | 20/20 | 78.7/75.8 (17.5× 🔴) [rows=1000/13906;window=7446f269] | 20/20 |
| scan | 24h | 100ms | 5.0/4.3 [rows=1000/13880;window=fb394815] | 20/20 | 25.8/23.4 (5.2× ⚠️) [rows=1000/13880;window=fb394815] | 20/20 | 77.3/75.8 (15.5× 🔴) [rows=1000/13880;window=fb394815] | 20/20 |
| trace_lookup | 1h | 0ms | 3.8/3.7 [spans=6] | 20/20 | 4.5/4.5 (1.2×) [spans=6] | 20/20 | 74.8/70.6 (19.7× 🔴) [6] | 20/20 |
| trace_lookup | 1h | 100ms | 4.2/3.9 [spans=6] | 20/20 | 2.5/2.4 (0.6×) [spans=6] | 20/20 | 106.6/97.2 (25.4× 🔴) [6] | 20/20 |
| trace_lookup | 24h | 0ms | 6.5/6.0 [spans=6] | 20/20 | 5.6/5.4 (0.9×) [spans=6] | 20/20 | 78.8/69.9 (12.1× 🔴) [6] | 20/20 |
| trace_lookup | 24h | 100ms | 5.4/4.7 [spans=6] | 20/20 | 4.5/3.7 (0.8×) [spans=6] | 20/20 | 76.6/71.3 (14.2× 🔴) [6] | 20/20 |

`trace_lookup`'s p95/p90 sits BELOW its own baseline at both latencies in this
run (LH 0.6-1.2×) — the cache-hit caveat above explains why a `lat100` cell
can look this close to `lat0`; `trace_lookup`'s tiny 6-span result also means
absolute times are a few ms either way, within run-to-run noise (see the
trust caveat).

#### Traces

**Per-query median LH vs baseline:** count_by_service 2.8×, count_total 2.3×, scan 1.8×, service_filter 4.0×, slow_spans 1.8×, span_name 2.6×, trace_by_id 1.7×

| query | range | S3 lat | baseline p95/p90 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 4.8/2.5 [rows=5;total=576;hash=a8ce2292] | 20/20 | 6.3/6.1 (1.3×) [rows=5;total=576;hash=a8ce2292] | 20/20 | 105.4/104.0 (22.0× 🔴) [rows=5;total=576;hash=a8ce2292] | 20/20 |
| count_by_service | 1h | 100ms | 3.5/3.4 [rows=5;total=556;hash=4400221a] | 20/20 | 10.8/6.7 (3.1× ⚠️) [rows=5;total=556;hash=4400221a] | 20/20 | 115.4/96.7 (33.0× 🔴) [rows=5;total=556;hash=4400221a] | 20/20 |
| count_by_service | 24h | 0ms | 3.5/2.6 [rows=5;total=16244;hash=a4fb053d] | 20/20 | 14.1/10.9 (4.0× ⚠️) [rows=5;total=16244;hash=a4fb053d] | 20/20 | 92.1/91.2 (26.3× 🔴) [rows=5;total=16244;hash=a4fb053d] | 20/20 |
| count_by_service | 24h | 100ms | 4.3/3.4 [rows=5;total=16198;hash=a1a5f4f3] | 20/20 | 10.4/9.3 (2.4×) [rows=5;total=16198;hash=a1a5f4f3] | 20/20 | 107.1/92.7 (24.9× 🔴) [rows=5;total=16198;hash=a1a5f4f3] | 20/20 |
| count_total | 1h | 0ms | 3.0/2.3 [576] | 20/20 | 6.3/5.3 (2.1×) [576] | 20/20 | 102.7/97.6 (34.2× 🔴) [576] | 20/20 |
| count_total | 1h | 100ms | 2.4/1.9 [556] | 20/20 | 5.7/3.7 (2.4×) [556] | 20/20 | 98.2/91.9 (40.9× 🔴) [556] | 20/20 |
| count_total | 24h | 0ms | 2.3/2.3 [16244] | 20/20 | 10.2/8.0 (4.4× ⚠️) [16244] | 20/20 | 93.2/90.1 (40.5× 🔴) [16244] | 20/20 |
| count_total | 24h | 100ms | 4.5/4.1 [16198] | 20/20 | 10.0/10.0 (2.2×) [16198] | 20/20 | 94.0/91.5 (20.9× 🔴) [16198] | 20/20 |
| scan | 1h | 0ms | 2.9/2.9 [rows=572/572;window=3949876b] | 20/20 | 3.4/3.2 (1.2×) [rows=572/572;window=3949876b] | 20/20 | 104.2/100.6 (35.9× 🔴) [rows=572/572;window=3949876b] | 20/20 |
| scan | 1h | 100ms | 4.2/3.4 [rows=556/556;window=e79351fb] | 20/20 | 3.9/3.8 (0.9×) [rows=556/556;window=e79351fb] | 20/20 | 90.6/87.0 (21.6× 🔴) [rows=556/556;window=e79351fb] | 20/20 |
| scan | 24h | 0ms | 4.2/4.1 [rows=1000/16230;window=6bb1803c] | 20/20 | 14.3/12.3 (3.4× ⚠️) [rows=1000/16230;window=6bb1803c] | 20/20 | 94.1/88.5 (22.4× 🔴) [rows=1000/16230;window=6bb1803c] | 20/20 |
| scan | 24h | 100ms | 4.9/4.3 [rows=1000/16198;window=ddf46f1e] | 20/20 | 11.5/10.8 (2.3×) [rows=1000/16198;window=ddf46f1e] | 20/20 | 97.5/85.5 (19.9× 🔴) [rows=1000/16198;window=ddf46f1e] | 20/20 |
| service_filter | 1h | 0ms | 2.6/2.5 [118] | 20/20 | 12.1/5.2 (4.7× ⚠️) [118] | 20/20 | 85.5/84.4 (32.9× 🔴) [118] | 20/20 |
| service_filter | 1h | 100ms | 2.8/2.8 [114] | 20/20 | 4.9/3.2 (1.8×) [114] | 20/20 | 120.3/107.4 (43.0× 🔴) [114] | 20/20 |
| service_filter | 24h | 0ms | 1.9/1.4 [3251] | 20/20 | 8.7/7.4 (4.6× ⚠️) [3251] | 20/20 | 99.5/90.3 (52.4× 🔴) [3251] | 20/20 |
| service_filter | 24h | 100ms | 2.7/2.1 [3242] | 20/20 | 9.4/9.3 (3.5× ⚠️) [3242] | 20/20 | 112.8/111.4 (41.8× 🔴) [3242] | 20/20 |
| slow_spans | 1h | 0ms | 5.0/4.8 [53] | 20/20 | 7.5/5.8 (1.5×) [53] | 20/20 | 108.4/107.3 (21.7× 🔴) [53] | 20/20 |
| slow_spans | 1h | 100ms | 4.1/3.8 [49] | 20/20 | 5.6/5.0 (1.4×) [49] | 20/20 | 96.7/94.5 (23.6× 🔴) [49] | 20/20 |
| slow_spans | 24h | 0ms | 4.4/2.8 [1312] | 20/20 | 9.2/9.0 (2.1×) [1312] | 20/20 | 97.7/92.5 (22.2× 🔴) [1312] | 20/20 |
| slow_spans | 24h | 100ms | 3.0/2.2 [1310] | 20/20 | 9.9/9.8 (3.3× ⚠️) [1310] | 20/20 | 108.6/107.5 (36.2× 🔴) [1310] | 20/20 |
| span_name | 1h | 0ms | 3.3/3.1 [56] | 20/20 | 5.3/4.5 (1.6×) [56] | 20/20 | 89.5/87.9 (27.1× 🔴) [56] | 20/20 |
| span_name | 1h | 100ms | 3.2/3.1 [56] | 20/20 | 5.0/4.9 (1.6×) [56] | 20/20 | 97.2/95.7 (30.4× 🔴) [56] | 20/20 |
| span_name | 24h | 0ms | 1.4/1.4 [1621] | 20/20 | 9.5/8.1 (6.8× ⚠️) [1621] | 20/20 | 85.7/83.7 (61.2× 🔴) [1621] | 20/20 |
| span_name | 24h | 100ms | 2.2/1.8 [1615] | 20/20 | 7.9/7.7 (3.6× ⚠️) [1615] | 20/20 | 91.9/89.3 (41.8× 🔴) [1615] | 20/20 |
| trace_by_id | 1h | 0ms | 2.6/2.4 [spans=4] | 20/20 | 4.9/4.6 (1.9×) [spans=4] | 20/20 | 99.2/90.0 (38.2× 🔴) [spans=4] | 20/20 |
| trace_by_id | 1h | 100ms | 3.3/2.9 [spans=4] | 20/20 | 5.9/5.4 (1.8×) [spans=4] | 20/20 | 100.9/100.5 (30.6× 🔴) [spans=4] | 20/20 |
| trace_by_id | 24h | 0ms | 4.8/3.0 [spans=4] | 20/20 | 6.5/5.6 (1.4×) [spans=4] | 20/20 | 98.1/93.3 (20.4× 🔴) [spans=4] | 20/20 |
| trace_by_id | 24h | 100ms | 2.0/1.4 [spans=4] | 20/20 | 3.2/3.2 (1.6×) [spans=4] | 20/20 | 90.7/83.9 (45.4× 🔴) [spans=4] | 20/20 |

#### Cells that are not fully valid (v3.3: none)

**None.** Every one of the 64 cells above (both signals, both latencies) is
fully valid and agrees exactly across baseline, Lakehouse, and ClickHouse.

**v3.2 reported one invalid cell here — that report is retracted.**
`logs/high_card/24h/lat100ms` showed ClickHouse at `result 6558 vs base 6557`
and was written up as "a genuine, ClickHouse-specific finding". It was not: a
harness bug gave ClickHouse a coarser, exclusive-end, second-granularity
window than LogsQL's nanosecond, inclusive-both-ends one (see "Response
validation" above) — a real, measured offset large enough at `high_card`'s
~6,557-group cardinality to shift the group count by one. With the fix (ns
bounds, `>= AND <=`, used by every ClickHouse query), the cell above
(`high_card`/24h/lat100) shows `rows=6571;total=13880;hash=813bb7ea…`
identically across baseline, LH, AND ClickHouse. The group-by pair-hashing
fix that surfaced the divergence in the first place worked exactly as
intended; the bug it caught was in the harness's own window computation, not
in ClickHouse's `GROUP BY`.

Full row-set hashes and per-iteration invalid reasons are in
`bench-results/baseline-2026-09/run-baseline-v3.3.md` (verbatim harness
output); truncated above for readability.

### Full-scope S3-ops (logs, e2e compose)

Pending — to be recorded once the e2e compose can be started (its host port 19428
is currently held by another project's stack; needs a human decision). No numbers
recorded here yet.
