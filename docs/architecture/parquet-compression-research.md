# Parquet compression research — PR 3 pre-implementation review

**Status: awaiting review.** Research for the 10 items in `parquet-compression-roadmap.md`,
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
4. **Item 10a (BROTLI on L2+/archives)** — behind the existing per-level compression schedule;
   gate on consumer-tool list approval.
5. **Item 9-replacement (optional)**: real zstd-19 via a gozstd-backed `compress.Codec`
   (standard frames, cgo) — only if BROTLI is rejected or the cgo dependency is acceptable.
6. Items 4/6/7: no write-side work. Item 8: parked pending product decision.

**Every step ships with the multi-engine readability gate** (pyarrow + duckdb readback — the
harness from this research becomes `scripts/ci/` + a CI job) and before/after size + query
benchmarks on real e2e data.
