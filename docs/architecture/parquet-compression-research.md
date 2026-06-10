# Parquet compression research — PR 3 pre-implementation review

**Status: REVIEWED & APPROVED (2026-06-10)** — with one adjustment: **BROTLI is skipped
for now** (zstd `SpeedBestCompression` is judged sufficient); it stays in this doc as a
potential future improvement (−15% measured on the test corpus) to revisit if cold-archive
size becomes a cost driver. Approved order: tags (delta+dict) → trap fixes → sorting →
L2+ row groups, every step behind the pyarrow+duckdb readback CI gate + size/query benchmarks.

 Research for the 10 items in `parquet-compression-roadmap.md`,
done three ways and cross-checked: (A) parquet-go **v0.29.0 module source audit** (file:line
cites), (B) **empirical portability harness** — real files written via parquet-go on this
machine, read back with **pyarrow 24.0.0 AND duckdb 1.5.3**, row-level equality proven both
directions, (C) **integration analysis** of our writer/compactor/read paths (both modules).
Hard constraint honored throughout: every file stays standard-parquet readable by external
tools.

## TL;DR — the roadmap after contact with reality

| # | item | verdict | what changed |
|---|---|---|---|
| 1 | sort by (stream_id, ts) | **GO, careful** | viable + biggest win, but 3 correctness traps found (below) |
| 2 | DELTA_BINARY_PACKED ts | **GO, easy** | roadmap's API doesn't exist; real one is the `,delta` struct tag — one edit covers both modules + compactor |
| 3 | larger L2+ row groups | **GO, easy** | `RowGroupSizeByOutputLevel` config mirroring the compression schedule |
| 4 | PageIndex emission | **ALREADY SHIPPED** | parquet-go v0.29.0 writes ColumnIndex+OffsetIndex **unconditionally** — verified 100% of chunks in every test file. Write side = zero work; the value is read-side exploitation (→ PR 2 page-skipping) |
| 5 | zstd dictionary training | **REJECT — proven breakage** | empirically: frames with a Dictionary_ID fail BOTH readers (`pyarrow: Dictionary mismatch`, `duckdb: ZSTD Decompression failure`). Violates the portability constraint, full stop |
| 6 | drop low-card blooms | **mostly NO-OP** | we only emit blooms on service.name + trace_id today — there are no level/severity blooms to drop |
| 7 | column-chunk merging | **N/A — misconception** | parquet has exactly one chunk per column per row group; our files already write that way |
| 8 | schema slimming L2+ | **HOLD** | format-safe but breaks repo invariants (schemaFingerprint, struct-typed writers) + drops data — needs a product decision first |
| 9 | ZSTD-19 via klauspost | **IMPOSSIBLE as written** | klauspost has exactly 4 named tiers; `EncoderLevelFromZstd(19)` maps to `SpeedBestCompression` (≈ zstd-11). **Long-range mode is structurally meaningless inside parquet pages** — window is clamped to page size (verified in frame bytes: asked 128 MiB, got 2 MiB). True 19 would need a cgo codec (gozstd, already an indirect dep) producing standard frames |
| 10a | BROTLI archives | **GO, verify consumers** | codec exists (pure Go, Q0-11); **empirically ~15% smaller than zstd-best** on the test corpus; pyarrow + duckdb both read it. Ecosystem support narrower than zstd — gate on the consumer list |
| 10b | LZMA archives | **REJECT** | not a parquet codec (closed format enum) — cannot exist in a standard file |

**New opportunity found (not in the roadmap):** parquet-go does **not dictionary-encode by
default** — every string column ships as `DELTA_LENGTH_BYTE_ARRAY`, no dictionary pages
(verified in all test files). Adding the `dict` tag (→ standard `RLE_DICTIONARY`) on
low-cardinality string columns (`service.name`, `severity_text`, `k8s.*`…) is the *standard*
form of "dictionary compression", composes with item 1's sorting (sorted runs of dict codes
RLE beautifully), and is what the roadmap's item 5 *should* have been. **Recommended as item
5-replacement.** Caveat: with DataPageV2, dict-encoded data pages are intentionally left
uncompressed (only the dict page is compressed) — net effect must be measured, not assumed.

## The three correctness traps under item 1 (why "GO, careful")

1. **`MinTimeNs`/`MaxTimeNs` are taken from `rows[0]`/`rows[len-1]`** (writer.go:410, compactor.go:294,
   buffer_export…). Stream-first ordering breaks that: max understated / min overstated →
   **manifest range pruning can silently skip files containing matches**. Fix: O(n) min/max scan.
2. **The dual-emission fix regresses through trap 1**: the Option-B `bufferWatermark` is
   max(MaxTimeNs of scanned files) — an understated MaxTimeNs re-opens the exact 2.00×
   double-count closed by commit 549ff53.
3. **Three page-skip helpers still assume time-sorted pages** (`rowGroupFullyInRange`,
   `syntheticTimestampBlock`, `enrichManifestFromFooter` — both modules): they read
   `MinValue(0)`/`MaxValue(last)`. Under stream-first sort that can wrongly declare full
   containment and **emit out-of-range rows**. `rowGroupMatchesTimeRange` was already hardened
   to the aggregate-across-pages pattern; these three must follow BEFORE the sort lands.

Also: per-RG time ranges widen (whole-file window) → the time-based RG skip loses selectivity
on L2+ files; sorting-columns metadata should be declared (standard, pyarrow surfaces it) so
external engines can exploit the order. The Option-B trace export has its own sort
(buffer_export.go:51) that must change in lockstep — `tests/parity/` enforces byte-parity.

## Empirical support matrix (actual runs, this machine)

| file | feature | pyarrow | duckdb | rows identical | notes |
|---|---|---|---|---|---|
| zstd_best | zstd SpeedBestCompression | ✓ | ✓ | ✓ | the real "max zstd" via klauspost |
| delta_zstd | DELTA_BINARY_PACKED int64 | ✓ | ✓ | ✓ | both report the encoding |
| sorted_zstd | pre-sorted + SortingColumns meta | ✓ | ✓ | ✓ | pyarrow surfaces sorting_columns |
| brotli_q11 | BROTLI Q11 | ✓ | ✓ | ✓ | **best ratio: −15% vs zstd-best** |
| bigrowgroup | 1M rows / 1 RG | ✓ | ✓ | ✓ | |
| zstd_longwin | custom codec, 128 MiB window asked | ✓ | ✓ | ✓ | window clamped to 2 MiB — long-range moot |
| zstd_dictid | trained-dict simulation | **✗ FAIL** | **✗ FAIL** | — | the portability proof for rejecting item 5 |

PageIndex: present on 100% of column chunks in **all** files (no flag needed).

## Proposed implementation order (post-review)

1. **Item 2 (delta tags) + item 5-replacement (`dict` tags on low-card strings)** — struct-tag
   edits in `internal/schema/row.go`, one place, both modules inherit; measure on e2e data.
2. **Item 1 (sorting)** — land the three trap fixes first (min/max scan, watermark guard,
   page-aggregate helpers ×2 modules), then the sort + SortingColumns metadata, parity suite
   green, then measure.
3. **Item 3 (L2+ row groups)** — config plumb, measure on compacted files.
4. ~~Item 10a (BROTLI)~~ — **SKIPPED per review**: zstd is enough for now. Parked here as a
   potential future improvement (measured −15% vs zstd-best; both target readers fine).
5. Item 9-replacement (gozstd cgo for real 19): parked — same reasoning, zstd ceiling accepted.
6. Items 4/6/7: no write-side work. Item 8: parked pending product decision.
7. **NEW (promoted per review): dedicated columns** — configurable promotion of hot attribute
   keys into real Parquet columns (own stats/dictionaries); pairs with the dict-tag work and
   the field/value catalog's knowledge of hot keys. Lands after items 1–3 in this PR series.

**Every step ships with the multi-engine readability gate** (pyarrow + duckdb readback — the
harness from this research becomes `scripts/ci/` + a CI job) and before/after size + query
benchmarks on real e2e data. **SHIPPED (step 4 PR):** `scripts/ci/parquet-readback/`
(`gen/main.go` writes 5k-row logs+traces files with the REAL `internal/schema` structs and
the REAL writer options — zstd best, `MaxRowsPerRowGroup`, split-block blooms, `_trace_idx`
footer — plus a writer-truth manifest; `verify.py` proves aggregates, `EXCEPT ALL`
row-equality both directions, the delta/dict encodings, and 100% PageIndex coverage in BOTH
engines) + the `parquet-readback` CI job. 86 checks; the encoding expectations are derived
from the live struct tags via reflection so schema changes propagate automatically.

## Measured: step 1 (tags) on REAL stack data — 2026-06-10

`scripts/bench/compression_ab` pulled the 10 largest REAL log parquet files from the live
e2e MinIO (compacted L2, ~24.5 MB each, 245 MB total), decoded their rows, and re-encoded
the SAME rows with the baseline (untagged) vs tagged schema at two zstd levels:

| config | baseline | tagged | delta |
|---|--:|--:|--:|
| zstd-default | 256.7 MB | 251.1 MB | **−2.2%** |
| zstd-best | 240.8 MB | 235.1 MB | **−2.4%** |

Honest read: the tag win is real but small on this corpus — total bytes are dominated by the
high-entropy `body` column, which tags deliberately don't touch. The dict/delta savings
concentrate in the metadata-ish columns (they also shrink dictionaries/page headers and speed
predicate decode). The big prize remains **item 1 (sorting)** — the same A/B harness will
measure it next, on the same real files.

## Measured: step 2 (trap fixes) — no-regression verification — 2026-06-10

Step 2 is correctness-only (sort-gating traps + two bonus page-bounds bugs): **no
compression delta expected**. Verified with the same A/B harness, same 10 real files, on
the stacked branch (tags + trap fixes): **−2.2% / −2.4% — identical to step 1**, confirming
zero encoding regression. The compression payoff of step 2 arrives indirectly: it unblocks
step 3 (the (stream_id, timestamp) sort), which is the expected 30–50% item.

## Measured: step 3 (the (stream_id, timestamp) sort) — REJECTED on evidence — 2026-06-10

The roadmap's projected 30–50% win **failed empirical validation** on real stack data. Same
A/B harness (now with a `tagged+sorted` variant), same 10 real L2 log files, identical rows:

| config | baseline | tagged | tagged+sorted |
|---|--:|--:|--:|
| zstd-default | 256.7 MB | 251.1 MB (−2.2%) | **302.1 MB (+17.7%)** |
| zstd-best | 240.8 MB | 235.1 MB (−2.4%) | **272.3 MB (+13.1%)** |

Per-column attribution (`scripts/bench/compression_percol`, one 24 MB file, zstd-best,
sorted − unsorted): `body` **+2.07 MB**, `trace_id` +0.86 MB, `timestamp` +0.38 MB,
`_stream` +0.34 MB, `span_id` +0.20 MB.

**Why the projection was wrong for this corpus:** its redundancy is TEMPORAL, not
per-stream — similar log lines arrive in cross-stream bursts (zstd exploits adjacency
within its window), spans of one trace are time-adjacent (shared trace_id prefixes), and
the globally-monotonic timestamp column delta-encodes to almost nothing. Stream-first
clustering destroys all three at once; per-stream body redundancy did not make up for it.

**Verdict:** the sort is NOT shipped. The trap fixes (step 2) stand on their own as
correctness wins. Revisit only with a corpus-dependent toggle if production-shaped data
(heavy per-stream template redundancy) measurably differs — the harness decides, per the
per-PR benchmark protocol. The full sort implementation existed and passed all suites
(including parity) before being reverted on this measurement; see PR history.

## Measured: step 4 (L2+ row groups) on REAL L2 data — 2026-06-10

`RowGroupSizeByOutputLevel` landed ahead of step 3 (sorting) — default `[10000, 10000,
20000]`: L0/L1 outputs keep the historical 10k rows/group, L2+ rollups double to 20k.
Methodology = the same `scripts/bench/compression_ab` protocol: the **4 largest real
compacted-L2 log files** were pulled from the live e2e MinIO (`obs-archive`,
`0/0/logs/dt=*/hour=*/compacted-L2-*.parquet`, 122–123k rows each, **490,187 rows total**),
their rows decoded, and the **identical rows re-encoded at zstd-best** with only the
row-group size varied (10000 vs 20000; production blooms + tags in both arms):

| metric | RG=10000 (current) | RG=20000 (2×) | delta |
|---|--:|--:|--:|
| bytes (4 files) | 94,972,408 | 94,834,273 | **−0.15%** |
| row groups | 52 | 28 | **−46%** |
| total pages | 3,103 | 2,542 | **−18%** |
| pages per column chunk | 2.4 | 3.6 | +1.2 |

Honest read: the **byte** win is small (−0.15%) on this body-dominated corpus — same story
as step 1; zstd-best already erases most of the inter-row-group redundancy at these sizes.
The structural wins are metadata-side: half the row-group footer entries and per-group
dictionaries, 18% fewer page headers, and half the offset-index/manifest entries per file.
Read-path implications for PR 2's page skipping: pages get **fewer and larger** (total
pages −18%, pages per chunk 2.4→3.6 since each chunk now spans 2× rows), so page-level
pruning granularity coarsens modestly while row-group-level pruning granularity halves —
acceptable on L2+ where queries are scan-heavy; L0/L1 (the pruning-sensitive hot tail)
deliberately keep the 10k layout. The 2× layout also gives step 3's sorted runs twice the
room per dictionary/RLE run, so this number should be re-measured (same harness) once
sorting lands — measured here on steps-1+2 (unsorted) data.
