# Changelog

All notable changes to Victoria Lakehouse will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [Unreleased]

## [0.85.0] - 2026-06-10

### Added

- **Compression step 4 — 2× row groups on L2+ rollups: `compaction.row_group_size_by_output_level` (both modules).** New per-output-level Parquet row-group size schedule mirroring the progressive compression schedule exactly: slot N = max rows per row group for compacted output files at level N, saturating at the last slot for deeper rollups; an empty list falls back to the static `insert.row_group_size` (pre-schedule behaviour, pinned by regression test). Default `[10000, 10000, 20000]` — L0/L1 outputs keep the historical 10k rows/group, L2+ rollups double to 20k (cold rollups are scan-heavy and rarely pruned at row-group granularity). Threaded through the compactor per output level; flag `-lakehouse.compaction.row-group-size-by-output-level` in BOTH binaries; chart `values.yaml` + `values.schema.json` now document and validate the **entire `compaction.*` section** (8 grandfathered keys burned out of the helm-drift allowlist). Bug fixed en route: the YAML `compaction.compression_level_by_output_level` (and the new key) was **silently dropped by the config merge** — file values never reached the compactor; both schedules now merge and carry a round-trip regression test. **Measured** (the `scripts/bench/compression_ab` methodology: 4 largest real compacted-L2 log files from the live e2e MinIO, 490,187 rows, identical rows re-encoded at zstd-best, only the row-group size varied): **size −0.15%** (94,972,408 → 94,834,273 bytes), **row groups −46%** (52 → 28), **total pages −18%** (3,103 → 2,542; pages per column chunk 2.4 → 3.6). Honest read: the byte win is small on this body-dominated corpus — the structural win is metadata-side (half the row-group footers/dictionaries, 18% fewer page headers, half the manifest/footer entries per file) and it compounds with the upcoming (stream_id, timestamp) sort, where bigger groups give dictionaries and RLE runs 2× the room. Numbers + methodology in `docs/architecture/parquet-compression-research.md` (step-4 measured section).
- **Multi-engine parquet readback CI gate (`parquet-readback` job): every parquet encoding change now ships behind a pyarrow + duckdb readback proof.** `scripts/ci/parquet-readback/gen` writes synthetic logs + traces files (5k rows, every column populated incl. the three attribute maps, deterministic seed) using the **REAL production schemas** (`internal/schema.LogRow`/`TraceRow` — the delta/dict tags ride along) and the **REAL writer options** (zstd `SpeedBestCompression`, `MaxRowsPerRowGroup`, split-block blooms on `service.name`+`trace_id`, the `_trace_idx` KV footer), and emits a writer-truth manifest (row count, exact big-int sums of integer columns — 5k nanosecond timestamps overflow int64, so truth is exact arithmetic — distinct counts of low-card strings). `verify.py` then proves, for BOTH engines independently: aggregates == writer truth; pyarrow↔duckdb row-level equality via `EXCEPT ALL` in both directions (0 rows); `DELTA_BINARY_PACKED` on every delta-tagged column and `RLE_DICTIONARY` on every dict-tagged column (expectations derived from the live struct tags via reflection, so schema changes auto-propagate); and PageIndex (ColumnIndex + OffsetIndex) present on 100% of column chunks. 86 checks, all green locally and wired as a CI job. This is the standing gate promised in `docs/architecture/parquet-compression-research.md`.

- **S3 batch 2a — waste-feedback read-ahead: the adaptive window now SHRINKS when it fetches bytes nobody reads (both modules).** The Tier-1 grow/reset state machine was blind to waste: the combined benchmark measured **46 MB/query fetched-but-never-read on filtered counts (56% buffer hit rate) and 17 MB/query on fulltext** — sparse forward hops classify as "forward-sequential", so every abandoned window VOTED GROW for the next one. Now each window eviction computes the evicted window's never-read ratio (same high-water accounting as `lakehouse_s3_buffer_wasted_bytes_total`, allocation-free); above the threshold the next window is **halved (floored at `read_ahead_bytes`)** and the growth credit resets — growth resumes only after 2+ consecutive efficient windows. Fully-consumed sequential scans still grow to `read_ahead_max_bytes` and stay there (pinned by regression tests). New per-signal config: `s3.read_ahead_waste_threshold` (`-lakehouse.s3.read-ahead-waste-threshold`, default 0.5, `>=1` disables; + chart values/schema) and a `lakehouse_s3_readahead_shrink_total` counter. The `openRangedParquet` file-size clamps are untouched. **Unit-measured** (deterministic page-probe sim: 256 KB page per 3 MB stride over a 64 MB file at production 2 MB base / 8 MB max windows): bytes-on-wire **57.7 MB → 45.1 MB (−21.8%)**; steady-state never-read bytes per abandoned window drop from up-to-8 MB (grown max) to the 2 MB base (4× at defaults). Trade-off stated: the smaller window costs more GETs on sparse patterns (9 → 22 in the sim) — the live before/after (waste B/q on filtered/fulltext, p50s) runs post-merge via `scripts/bench/with-s3-latency.sh 100 30 scripts/bench/full-scope-s3-bench.sh`.

### Fixed

- **S3 batch 2b — compaction now HEALS missing `LabelAggregates` instead of propagating the wipe (both modules).** The compactor built the output `FileInfo` with `mergeFileLabelAggregates(g.Files)` — a merge of the INPUT files' aggregate maps, which are empty for every file written before the #138 refresh-wipe fix, so compaction could never repair pre-fix data and `count() by (field)` fast-paths kept missing compacted files forever. The compactor holds the actual merged ROWS, so it now extracts aggregates from them via the **single shared implementation** the flush writers use — `schema.ExtractLogLabelAggregates` / `schema.ExtractTraceLabelAggregates`, moved to `internal/schema/label_aggregates.go` with the shared `MaxLabelAggregateValues` cap (no duplicate field lists to drift; both modules' writers and the compactor import the same code). Every compaction cycle now monotonically heals old files. Regression tests pin: (a) **healing** — inputs with nil aggregates produce outputs with correct per-(field,value) counts (logs + traces modes); (b) **equivalence** — when inputs DO carry aggregates, row extraction equals the old input-map merge for identical data; (c) **absent-value** — a field over the per-field value cap is ABSENT from compacted output exactly like flush. Expected live effect (verified post-merge over compaction cycles): `count_24h`-class metadata answers stop degrading as partitions compact.

## [0.84.1] - 2026-06-10

### Changed

- **Compression roadmap: the (stream_id, timestamp) sort is REJECTED on real-data evidence; the A/B harness gains the tooling that proved it.** The roadmap's projected-biggest item (30–50% smaller files from stream-clustered rows) was implemented, passed both modules' full suites including flush↔export parity — and then failed the mandatory real-data measurement: **+17.7% (zstd-default) / +13.1% (zstd-best) LARGER files** across 10 real compacted-L2 files (identical rows, only the order changed). Per-column attribution shows why this corpus punishes stream-clustering: `body` +2.07 MB per 24 MB file (similar log lines arrive in cross-stream time bursts that zstd exploits via adjacency), `trace_id` +0.86 MB (spans of one trace are time-adjacent → shared prefixes), `timestamp` +0.38 MB (globally monotonic deltas compress to almost nothing; stream-sawtooth doesn't). The sort code is reverted (preserved in branch history); the step-2 trap fixes stand on their own as correctness wins. Shipped instead: `scripts/bench/compression_ab` gains a `tagged+sorted` third variant, and the new `scripts/bench/compression_percol` attributes any size delta to individual columns — every future layout experiment gets judged by the same evidence, per the per-PR benchmark protocol. Re-entry condition documented in `docs/architecture/parquet-compression-research.md`: a corpus-dependent sort toggle, only if production-shaped data (heavy per-stream template redundancy) measurably differs.

## [0.83.1] - 2026-06-10

### Fixed

- **The 30-second manifest refresh silently wiped `LabelAggregates` — the count-pushdown fast path (PERF-2) was dead in production.** `RefreshFromS3`'s preserve-enrichment merge copied ten `FileInfo` fields one by one; `LabelAggregates` (added by PERF-2 after that list was written) was not among them, so every file's aggregates vanished within one refresh interval and `* | stats by (field) count()` queries scanned ~100 files (1.7 s at 100 ms S3 latency) instead of being answered from the manifest in milliseconds. Root-caused from the new per-scenario S3-op telemetry. Fix: the merge now preserves the **entire tracked entry** on key match (S3 objects are immutable — a LIST carries no newer information), via `mergeRefreshedFilesLocked`. Anti-recurrence: `TestRefresh_PreservesEveryEnrichmentField` walks `FileInfo` by **reflection**, fills every exported field, and asserts each survives the merge — adding a field without preserving it now fails CI instead of silently regressing.

## [0.83.0] - 2026-06-10

### Added

- **S3 read-path Tier 1 (batch 1) — parquet open hygiene, adaptive read-ahead, BDP coalescing, singleflight, S3-op observability (both modules).** From the approved ClickHouse-first research (`docs/architecture/s3-optimization-research.md`): every ranged `parquet.OpenFile` now passes `SkipPageIndex/SkipBloomFilters(true)` (we prune via manifest/pmeta; internal index/bloom reads stay lazily available), `OptimisticRead(true)` (footer suffix+body in one tail GET), `FileReadMode(ReadModeAsync)` (library-native page read-ahead, bounded ~1 page in flight per column reader; `-lakehouse.s3.parquet-read-mode=sync` is the rollback) and a 1 MB read buffer (library default was 4 KB). The coalescing gap default goes 64 KB→1 MB (breakeven at real S3 RTT is megabytes — AnyBlob, VLDB 2023), read-ahead adapts 2→8 MB on sequential patterns, and the parquet magic read no longer pulls a wasted ~2 MB head window — **all clamped by file size** so small files keep precise column-projected reads. Footer/`.bloom`/pmeta-bundle GETs are singleflight-deduped (`context.WithoutCancel` so a cancelled query can't poison waiters). 8 new metric families (GETs by phase, per-open GET histogram, window waste, over-fetch, grow/reset, head-bypass, dedup) and the full-scope benchmark now records **per-scenario S3-op deltas**. New per-signal config: `s3.read_ahead_max_bytes`, `s3.read_buffer_size`, `s3.parquet_read_mode` (+flags +chart values/schema). **Measured on the live stack at 100 ms injected S3 latency: count_1h 1478→878 ms (−41%), count_24h 4133→2475 ms (−40%), `gets/open` 4–6→2.0**; plain count_24h now beats hot VL (0.9×).

## [0.82.0] - 2026-06-10

### Changed

- **pmeta is now the metadata layer, period: legacy sidecar WRITERS deleted, `pmeta.enabled` default ON.** The hard cleanup after the consolidation baked: `Manifest.WritePartitionSidecar` (the `_file_metadata.json` S3 writer), the logs `storageBloomObserver` write side (per-file `.bloom` + partition `_bloom.bin` writers) + the `BloomObserver` interface, and the traces `FlushHook` bloom feed, `PersistBloomIndex`, and `BackfillBloomIndex` are **removed** — there is no code path that writes the legacy sidecars anymore. The `-lakehouse.pmeta.retire-sidecar-writes` flag (and its config key + validation) is gone with them; `-lakehouse.pmeta.enabled` now defaults to **true** and is the explicit opt-out into a degraded mode (no catalog/bloom for new files; cold restarts warm from footers only). **Read-fallbacks for pre-pmeta data are kept one more release**: `LoadSidecars`/`LoadSidecarsForPartitions`, the `.bloom`/`bloomCache` readers, and the traces `bloomIdx` load. 58 tests of the deleted machinery were removed; tests that used it as setup now seed the read paths directly (`MarshalFileMetaSidecar` + mock S3 / `bloomS3Loader`). Post-switch benchmark recorded in `docs/benchmarks/full-scope-s3.md`: cold LH at parity with hot VL on every scan scenario and 2.7–10× faster on metadata queries; CH-over-S3 trails 30–40×.


## [0.81.0] - 2026-06-10

### Added

- **pmeta `retire-sidecar-writes` — stop writing ALL legacy sidecars the facets replace: `_file_metadata.json`, per-file `.bloom`, and partition `_bloom.bin` (`-lakehouse.pmeta.retire-sidecar-writes`, off by default, both modules).** The back half of the consolidation. File-meta: `WarmMetadata` serves from the bundle-warmed fileMetaFacet with the Parquet **footer** as the cold-restart fallback (Phase 3) — so skipping the `_file_metadata.json` write loses nothing. Bloom: every bloom pre-filter path now consults the in-RAM bloom facet first — the single-file `checkFileBloom`, the logs OR-branch + single-set partition paths (`bloomUnionMatch`/`bloomColumnIntersect`, `_bloom.bin` via `bloomCache` as the fallback), and the traces pre-filters (`bloomMayContainAll` per-partition hybrid with the legacy in-RAM `bloomIdx` as fallback) — so under retire the logs bloom observer isn't wired and traces `PersistBloomIndex` is a no-op. Both sides of every hybrid **keep keys they have no bloom for** (a bloom can only exclude what it knows), so a file holding the queried value is never dropped. **Reversible** (clear the flag → all sidecars resume; no migration); requires `--pmeta`; default off → byte-identical. `TestInteg_PmetaRetire_SkipsFileMetaSidecar`, `TestInteg_PmetaFlip_ORBranchFacet`, `TestInteg_PmetaFlip_BloomHybridColdRestart` (cold restart with EMPTY legacy bloom: facet prunes, present values kept, unknown partitions kept).

### Fixed

- **pmeta hardening — 70-finding holistic audit of the whole layer (82-agent adversarial review), all confirmed issues fixed.** Highlights:
  - **Data races** (race-detector verified): catalog `Values` iterated the live id slice after unlocking while flush-time `Merge` mutated it in place (torn/duplicated dropdown values); `Cardinality` ran the HLL estimate outside the lock. Both fixed + pinned under `-race`.
  - **Lifecycle was structurally missing** — facets/bundles grew forever: compaction now feeds the output file's facets and removes the merged-away inputs (`PmetaOnCompacted`); retention removes expired files' entries and, when a partition fully expires, **evicts its bundle from RAM and deletes the `_pmeta.bundle` S3 object** (`PmetaOnFileExpired`).
  - **Self-heal wired**: `WarmCatalogFromS3` now consumes `WarmPartitions`' result — a missing/corrupt bundle is rebuilt from the manifest and re-persisted (the contract existed; `Store.Rebuild` had zero production callers, and the next `persistDirty` would have overwritten the S3 bundle with partial state).
  - **Serve-while-warming**: the S3-decoded bundle is now ABSORBED into a live bundle that concurrent flushes already populated (union per facet) instead of clobbering it; dirtiness is **generation-based** so a contribution landing mid-persist is never dropped from the next cycle.
  - **Bloom correctness**: an EMPTY bloom facet reports `ok=false` (warm-created empty facets had permanently shadowed the populated legacy fallback with keep-everything — bloom pruning was effectively dead after any restart); logs `checkFileBloom` got per-column any-of semantics (`trace_id:in(t1,t2)` wrongly required BOTH values — missing results) + the negated-predicate guard; the **traces bloom feeds are uncapped** (the capped label feed false-negatived past 100 values/field).
  - **Traces module**: `WarmMetadata` is now actually called (the file-meta read-flip was dead code there); `BackfillBloomIndex` + the duplicate in-RAM bloomIdx feed are skipped under retire.
  - **Catalog exactness**: a field whose per-file label list hits the extractor cap is marked high-card via `TruncatedFields` — the catalog never serves a silently truncated list as authoritative (falls to the exact scan).
  - **Codec v3**: header CRC covers magic..facetCount incl. the partition string (a flipped facetCount byte previously decoded as a VALID empty bundle); the catalog payload round-trips the high-card state and caps untrusted allocations. v2 bundles fail decode → self-heal rebuild.
  - **Roles**: select-only pods now build the catalog (was writer-gated → read-only pods scanned); insert-only pods no longer nil-panic in `WarmMetadata` Phase 3b; `retire-sidecar-writes` without `pmeta.enabled` fails fast at startup; `lakehouse_catalog_resident_bytes` updates every flush and includes the interning dict.
- **Bloom pre-filter could wrongly exclude files when one bloom field name is a suffix of another (`name` ⊂ `service.name`) — missing results (both modules).** `extractExactMatch`/`extractInValues` used bare `strings.Index`, so for the query `service.name:="api-gateway"` the promoted span-name column (`name`) substring-matched `name:="` and extracted `api-gateway` as a **span-name** candidate — the bloom check `span.name=api-gateway` then excluded every file (whose span-name blooms legitimately lack that value). A bloom false-negative = silently missing query results on the cold tier. Fixed with `fieldTokenIndex` (field-token boundary check — `:`/`-` count as field chars, matching `extractQuotedOp` and the `resource_attr:service.name`-style schema keys); pinned by the cross-field cases in `TestExtractExactMatch_TableDriven` (suffix-field, prefixed-attr-key, hyphenated-neighbor) in both modules.
- **Traces bloom facet was never fed — the #130 traces bloom read-flip was silently a no-op.** The traces writer passed `nil` bloomValues to `catalogObserver.OnFileFlush` (logs passed the real map), so the traces bloom facet stayed empty: `BloomMayContain` kept everything (safe, but zero pruning) and under retire-sidecar-writes traces would have lost bloom pruning entirely after a restart. The writer now passes the same labels map the legacy traces bloom (`onFlush` → `bloomIdx.AddColumns` + per-file `.bloom`) is built from — facet content identical to legacy. Caught by `TestInteg_PmetaFlip_BloomHybridColdRestart`'s absent-value assertion (the earlier present-value-only test passed vacuously: unknown keys are kept).

## [0.80.0] - 2026-06-09

## [0.79.0] - 2026-06-09

### Added

- **pmeta file-meta read-flip — the manifest enriches `FileInfo` from the in-RAM bundle, not the S3 sidecars (flip phase 1, `--pmeta`).** `WarmMetadata` now fills RowCount / min-max time / raw bytes / schema-fp from the `fileMetaFacet` first, and only falls back to the per-partition `_file_metadata.json` GETs for files the facet didn't cover — skipping the 16-way sidecar fan-out entirely when the bundle is complete. Still **dual-write** (the sidecar is written and is the fallback) so it is reversible, and **pmeta-off is unchanged** (`s.catalog==nil` → `LoadSidecars` runs as today). `manifest.FileMetaProvider`/`EnrichFromProvider` keep the manifest decoupled from `internal/pmeta`; `parquets3.catalogFileMetaProvider` bridges them; runStartup warms the bundle before `WarmMetadata`. The sidecar *write* retirement is the follow-up. `TestEnrichFromProvider`.
- **pmeta bloom read-flip — `checkFileBloom` consults the in-RAM bloom facet before a per-file `.bloom` S3 GET (`--pmeta`, logs + traces).** Query-time file pruning now checks `Store.BloomMayContain` (the partition `_bloom.bin` bundle, in RAM) before downloading the per-file `.bloom` sidecar, falling back to the download only for partitions the bundle doesn't carry. A bloom filter never false-negatives, so a file that actually holds the queried value is never excluded by either path — this can only change pruning *efficiency*, never correctness. The traces module preserves its AND-across-columns / OR-within-column semantics. `metrics … {facet_bloom_skip,file_bloom_skip}` distinguish the two paths.
- **pmeta labels `field_names` read-flip — `GetFieldNames` serves from the catalog before the legacy labelIndex (`--pmeta`, logs + traces).** Field-name dropdowns are served from the in-RAM catalog (range-aware: only the partitions overlapping the query window) ahead of the `_label_index.json` fallback. (`field_values` was already catalog-first.) `catalogFieldNames` unions names across the range. `TestInteg_PmetaFlip_FieldNamesAndBloom`.
- **pmeta — unified partition-metadata layer + field/value catalog (`-lakehouse.pmeta.enabled`, off by default).** One per-partition metadata layer (`internal/pmeta`) with pluggable facets replaces the scatter of per-subsystem sidecars/snapshots, behind a flag so the hot paths are byte-for-byte unchanged when off. Built as **dual-write + parity-gated** (the old `_file_metadata.json` / `_bloom.bin` / `_label_index.json` still write; each facet mirrors them and a test asserts they match), so it is safe to enable incrementally before the sidecars are retired.
  - **Field/value catalog** — Grafana label/field dropdowns serve from an in-RAM catalog (interned dict + sorted value sets) instead of scanning Parquet: cold `field_values(service.name)` over 24h went **50 s → 24 ms** on the live e2e stack (11× faster than hot VL, ~220× faster than ClickHouse-over-S3). Cold-start-warmed from the manifest (zero extra S3 I/O).
  - **In-house HyperLogLog cardinality** (LogLog-Beta, HLL++-grade accuracy, no dependency) for high-card id columns (`trace_id`/`span_id`) via a flush-time tap (9.7 ns/value, 0 alloc); `lakehouse_catalog_field_cardinality{field}` is the cardinality-bomb early-warning.
  - **file-meta + bloom facets** fold `_file_metadata.json` and `_bloom.bin`; **S3 bundle persist/warm** (`poolObjectStore`, `persistDirty` on flush, `WarmCatalogFromS3` at startup) lets the bloom facet survive a cold restart.
  - **A2 cardinality cap** (`-lakehouse.pmeta.cardinality-threshold`, default 50 000) bounds catalog RAM; **refuse-sketch-enumeration** (`-lakehouse.pmeta.refuse-sketch-enumeration`) returns empty for declared id columns instead of scanning. Metrics: `lakehouse_catalog_{value_lookups_total,resident_bytes,field_cardinality}`. Both logs + traces; `internal/pmeta` at 91 % coverage + fuzzers. See `docs/architecture/metadata-consolidation.md`, `docs/architecture/field-value-catalog.md`.
- **Benchmark tooling** — `scripts/bench/with-s3-latency.sh` injects S3 latency only for the wrapped command and always clears it via a trap (so it never lingers on the normal compose); `scripts/bench/full-scope-s3-bench.sh` compares cold-LH vs hot-VL vs ClickHouse across every S3/scan query class. Plus `docs/architecture/s3-scan-optimization-plan.md` (ClickHouse-parity pure-S3 roadmap) + PB-scale resource/restore analyses.

### Fixed

- **`field_values` with no limit (`limit==0`) scanned 2.5M logs instead of using the in-RAM index.** The labelIndex/catalog fast-path was gated on `limit > 0`, so a no-limit request — what a Grafana dropdown sends — bypassed the index and did a full column scan (50 s with S3 latency). The index is self-bounded, so it now serves `limit==0` too; the catalog result cap no longer zeroes the result on `limit==0`. `TestInteg_PmetaCatalog_NoLimitUsesIndex`.

## [0.69.0] - 2026-06-09

## [0.59.0] - 2026-06-08

### Added

- **PERF-2 — manifest count-pushdown for `stats by (field) count()` (logs + traces).** The common Grafana panel `* | stats by (service.name) count()` is now answered from manifest metadata instead of opening Parquet — the standard data-lake count-pushdown (Iceberg/Delta manifest stats), transparent to the existing LogsQL API and dashboards. `FileInfo.LabelAggregates` (field→value→rowcount) is populated at flush (capped per field → bounded growth), summed across the disjoint inputs at compaction, and persisted in the manifest snapshot; `manifest.CountByLabel` sums only files fully within the window (boundary files would over-count and fall through to scan). The read path (`countByPushdownField` + `manifestCountFastPath`) serves an **unfiltered single-field** query from those aggregates with **zero S3 reads**, emitting synthetic rows that reproduce the field's distribution (incl. the empty-value group) so the existing stats pipe aggregates them unchanged; a filtered query, a boundary/over-cap file, or a field without an aggregate falls through to scan (never wrong). The synthetic column is named/formatted identically to the scan path; `service.name` is wired + equivalence-proven (fast path emits the exact same distribution as a real scan). See `internal/storage/parquets3/count_pushdown_test.go`.
- **Option B — logstorage-native queryable insert buffer (cold-tier recently-flushed parity with hot VT).** The insert buffer can now be a real per-pod `logstorage.Storage` (the VT/VL in-memory-parts model) instead of a `[]schema.{Log,Trace}Row` staging slice that was reconstructed into a `logstorage.DataBlock` at query time. That struct→DataBlock converter kept drifting from the file-scan emission (missing `_stream`, `start_time_unix_nano`/`end_time_unix_nano`, map attrs), which made cold Jaeger/Tempo search return 0 for fresh traces, 404 the log→trace drilldown, and zero the Tempo service-filter. Selected by `insert.buffer_engine` (`buffer` default | `logstore`), with `insert.buffer_dir` + `insert.buffer_retention`:
  - **Write path** — ingest feeds the native `logstorage.LogRows` the VL/VT insert path already built straight into the store via the exported `MustAddRows` (dual-write; the legacy path stays the authoritative Parquet producer). A buffer failure can never break ingestion (recover-isolation + `lakehouse_buffer_store_dualwrite_failures_total`).
  - **Read path** — cold queries serve the recent/unflushed window from the buffer via the exported `Storage.RunQuery` (no struct→DataBlock conversion), byte-identical to a file scan.
  - **Durability** — reuses logstorage's own persistence (in-memory parts written to the buffer dir every flush interval, restored on `MustOpenStorage`); crash-loss window equals hot VT/VL, so no separate lakehouse WAL is added for the buffer.
  - **VL/VT compatibility** — **zero upstream modification**: pure exported-API reuse (`MustOpenStorage`/`MustAddRows`/`RunQuery`/`DebugFlush`/`QueryContext`); no new `deps/` patch.
  - **Parquet-from-buffer (shadow)** — the converter `DataBlockToTraceRows` (parity-proven vs the legacy insert mapping) + `ExportBufferToParquet` + a shadow exporter write buffer-sourced Parquet to a shadow S3 prefix for pre-cutover validation; the legacy `[]row` path stays authoritative.
  - With `logstore` enabled end-to-end: cold `[24h]` Jaeger 0→20 (matches hot), GetTrace resolves across recencies incl. the freshest band, Tempo `{nestedSetParent<0}` + `{resource.service.name="X"}` return data, and `count()`/`stats` are at 1.00 parity (no double-count). See `docs/architecture/buffer-queryable-store-design.md`.

### Fixed

- **Cold Jaeger search returned 0 traces at 12h while hot returned 20** — the `smartCache` fast-path in `preFilterFiles` unioned `FindFilesByTraceID(t_i)` across the queried trace IDs and narrowed to that union, but the union is only a *lower bound* (smartCache records only files it has already fetched), so a partial cache hit collapsed candidates to one file and dropped the other traces' spans. Fix: take the smartCache fast-path only for single-id (trace-by-id) queries; multi-id `trace_id:in(...)` falls through to bloom + `_trace_idx` narrowing, which examines every file. Guarded by the `TestColdHotParity_*` suite mirroring the exact VT step-1/step-2 query shapes.
- **Buffer rows invisible to `_stream:{…}` filters — cold Jaeger/Tempo returned 0 for fresh data** — the buffer→DataBlock conversion omitted the `_stream` (and `_stream_id`, start/end-time, and map-attr) columns that the stream-selector and GetTrace queries rely on; recent traces vanished from search and trace-by-id until they flushed and aged out. Conversion now emits the full column set (and the logs buffer emits its map attrs), matching the file-scan path.
- **Pushdown substring-match silently zeroed `service.name` filters** — `extractQuotedOp` substring-matched `name:=` inside `service.name:=`, building a wrong-column pushdown the column-stats pre-filter then used to drop every file. Added a token-boundary check; pinned by `extractQuotedOp` + `buildPushDownFilter` no-collision tests.
- **Empty/0-row Parquet aborted compaction, starving the cold tier** — `readTraceRows`/`readLogRows` treated parquet-go's `io.EOF` on a valid 0-row file as fatal, so one empty input file failed a whole partition's merge; since the scheduler re-picks the oldest partition first, that permanently starved compaction of newer partitions (growing the recently-flushed reachability lag). `io.EOF` is now a clean end-of-data.
- **Option B read-merge boundary (count double-count + recent-trace 404).** When the `logstore` buffer serves the recent window, a per-query watermark (`bufferWatermark` = max `MaxTimeNs` of the scanned Parquet files) splits the time range so the buffer serves only data strictly newer than Parquet — eliminating a 2× `count()`/`stats` double-count over the overlap. The watermark is bypassed for `trace_id`-filtered queries (reader-deduped span retrieval, where completeness matters): `queryFiltersTraceID` detects every form including the phrase form `trace_id:"X"` that VT's single-trace GetTrace span fetch emits (which the AST value-extractor misses), so recent traces no longer 404. Pinned by `TestQueryBufferBridge_WatermarkPreventsDoubleCount` + `TestQueryFiltersTraceID`; verified live (GetTrace 30/30, count 1.00, Jaeger 20/20).

### Changed

- **Bump loki-vl-proxy from v1.56.1 to v1.58.0** (`deployment/docker/Dockerfile.loki-vl-proxy`, applies to all four proxy services — e2e hot + cold, benchmark lh + vl-lh). v1.57.0 fixes high-cardinality Drilldown label/field panels at 24h+ (`detected_level` filters use the column-indexed `level` field, ~64× faster; single-field `count() by(field)` over ≥2h routes to `/select/logsql/hits`; the 24h+ querySplitting residual-chunk spike is suppressed) and adds the L0 hot-key cache tier to metrics. v1.58.0 adds ring-wide cache purge: `POST /admin/cache/flush?peers=1` clears the local L0/L1/L2 caches and fans the purge to every L3 peer (`X-Peer-Token` auth, concurrent, 5s/peer; unreachable peers reported, not fatal) via the new peer-side `POST /_cache/purge` — local-only without `?peers`.

### CI

- **Hot/cold parity unit job** (`.github/workflows/ci.yaml::parity-unit`) runs `TestColdHotParity*` + field/stream parity tests under `-race` with a pass/fail/skip summary, surfacing the cold/hot regression classes as a single PR status.
- New fuzz targets registered in the fuzz matrix (`FuzzPreFilterFiles_TraceID`, `FuzzExtractFilterValuesAST_TraceID`); changelog gate accepts version-section docs; assorted gofmt/seed fixes.

### Docs

- **`docs/architecture/buffer-queryable-store-design.md`** — the Option B design (logstorage-native buffer, read-merge, durability via reused VL/VT persistence, the exported-API-only reuse boundary).
- **`docs/architecture/performance-machinery.md` §6.2** — corrected the zstd-level claim: parquet-go maps integer levels to four buckets (`Fastest`/`Default`/`Better`/`Best`), so the prior "doubles compaction CPU" text was wrong.

## [0.49.0] - 2026-06-07

### Added

- **Honest lifecycle + cold-start protection at PB scale (PR #122)** — every cliff scenario the 3PB-on-S3 / 6-peer audit surfaced (stale snapshot, fragmented L0, simultaneous restart, first-ever boot, partial warmup) now has a guard and a test. The pod no longer lies about being ready while it is still discovering files, replaying WAL, or warming the footer cache; queries no longer return empty results because the local store happens to be 30 seconds behind S3.
  - **Three-state `/ready` contract** (`internal/startup/manager.go`, both `cmd/lakehouse-logs/main.go` and `lakehouse-traces/main.go`): the readiness handler now returns `503 not_ready` (still discovering or below the manifest gate), `204 serving_warming` (queries answered, background warmup in progress), or `200 ready` (warmup complete). The lifecycle manager splits the old single `ready` flag into `ServingReady` and `WarmupComplete`; serving-ready requires manifest gate ∧ WAL replay done. Guarded by `internal/startup/honesty_test.go` (16 sub-tests pinning every precondition combination).
  - **`MinManifestFiles` gate** (`internal/config/config.go::StartupConfig.MinManifestFiles`): operator-tunable floor — the pod stays out of rotation until the loaded manifest crosses this threshold. Defaults to 0 (off) for dev/CI; at PB scale set to ~10% of expected file count to mask the first-ever S3 LIST window. Without the gate, the first pod up returns empty results to peer fan-out for the full LIST duration.
  - **WAL replay gating** (`internal/startup/manager.go::SetWALReplayNeeded/Done`): `SetWALReplayNeeded()` is set before `store.StartWriter()`; `SetWALReplayDone()` is set after. `ServingReady()` returns false until both fire — buffered rows that hadn't been flushed when the pod crashed aren't silently dropped from query results during replay.
  - **Snapshot age metric + bounded shutdown persist** (`internal/manifest/manifest.go::SavedAt()`, `internal/metrics/lakehouse.go::ManifestSnapshotAgeSeconds`, `lakehouse_min_manifest_files_gate`): a 5-second ticker exports `lakehouse_manifest_snapshot_age_seconds` so operators can alert when persist is silently failing (e.g. PVC out of space, see `docs/operations/lifecycle.md`). Shutdown persists the manifest FIRST under a `cfg.Shutdown.PersistTimeout` bound (default 30 s) so restart picks up a fresh snapshot instead of cold-listing S3.
  - **Operator-facing tuning hints on startup** (`internal/startup/hints.go`): after warmup completes the pod logs structured advisory lines covering footer-cache headroom, snapshot staleness, buffer-peers, warmup duration vs ready-gate sizing. Silent when the cluster is healthy. 6 hint-categories pinned by `internal/startup/hints_test.go`.
  - **Full-jitter exponential backoff for S3 retries** (`internal/s3reader/reader.go`): replaces deterministic `100ms × 2^attempt` with `jitter = rand(0, min(cap, 100ms × 2^attempt))` per Marc Brooker's full-jitter formula. On simultaneous restart of 6+ peers all hitting S3 at once, the previous backoff phase-locked retries — the jitter version spreads them across the retry window. Guarded by `internal/s3reader/jitter_test.go` (50 concurrent retries asserting bucket spread + cap enforcement).
  - **Streaming gob decode with 50 GiB stat-cap** (`internal/manifest/manifest.go::LoadFrom`): the binary snapshot format now decodes via a streaming gob reader fed by an `os.Open` + `io.LimitReader`, and the file size is stat-rejected before any decode work. Peak RSS during restart at PB scale (1M+ files, ~150 MB snapshot) drops from a slurp-then-decode 2× spike to a steady streaming footprint. Guarded by `internal/manifest/streaming_decode_test.go` (round-trip, oversized rejection, legacy JSON fallback, missing-file no-op, truncated file).
  - **Warmup priority sort by `MaxTimeNs` descending** (`internal/storage/parquets3/warmup.go` and `lakehouse-traces/internal/storage/parquets3/warmup.go`): partial warmup (ctx cancelled, hit `WarmupMaxFiles`) now yields complete coverage of the freshest partition before moving to older ones. Previous lexicographic key sort meant a half-finished warmup left "last hour" dashboards with random partial coverage. Guarded by `internal/storage/parquets3/warmup_priority_test.go` (sort contract + stable tiebreaker for equal `MaxTimeNs`).
  - **Restart + warmup design spec** (`docs/architecture/restart-and-warmup-design.md`): the canonical reference for lifecycle phase machine, `/ready` truth table, BufferBridge state matrix, snapshot lifecycle, cluster-coldstart protection plan, sizing matrix by scale, decisions table (and what was rejected), open questions, and PR roadmap. New PRs that touch lifecycle code must update this doc in the same change.
  - **Lifecycle operations doc** (`docs/operations/lifecycle.md`): three-state `/ready` contract, ServingReady/WarmupComplete preconditions, full configuration reference, metrics to monitor, restart timeline table, "when `/ready` lies" troubleshooting.
  - **Scaling restart scenarios** (`docs/architecture/scaling-restart-scenarios.md`): honest worst-case analysis for 3PB+6-peer cluster across rolling restart, simultaneous restart, first-ever boot, stale snapshot, fragmented L0 hot zone. Tuning recommendations cross-referenced from `sizing.md`.
  - **Sizing guide** (`docs/operations/sizing.md`): memory/CPU/PVC matrix from dev → PB-scale with worked examples derived from the per-component cost drivers (manifest, footer cache, smart cache, WAL replay, per-query budget). Capacity-planning metrics + alert thresholds.

### Fixed

- **Compactor was zeroing `raw_bytes` on every merged file** — the compactor's output `manifest.FileInfo` did not carry `RawBytes` forward from the input files; it defaulted to 0 via `omitempty`. `Size` (compressed) kept tracking correctly, so per-tenant `TenantSummaries` derived from the manifest aggregated correct `total_bytes` against under-counted `raw_bytes`. Result: `/api/v1/tenants` (and the Lakehouse Explorer Tenants tab that consumes it) reported `compression_ratio < 1.0` — visibly impossible "compressed > raw" — for any tenant whose files had been compacted. Compaction is a pure row-union, so summing input `RawBytes` into the merged `FileInfo` is exact. Pre-fix files on disk still carry `raw_bytes=0` and heal as they get rolled up into higher compaction levels; new compactions from this build preserve raw bytes immediately. Guarded by:
  - `internal/compaction/compactor_test.go::TestCompactor_PreservesRawBytes` — unit regression: two-file compaction with explicit raw1+raw2, asserts merged equals sum.
  - `tests/e2e/tenant_stats_consistency_test.go::TestManifest_CompactedFilesPreserveRawBytes` — walks `/manifest/range` and asserts compacted files (level > 0) preserve `raw_bytes` (10% grace for pre-fix files).
  - `tests/e2e/tenant_stats_consistency_test.go::TestTenantStats_CompressionRatioReasonable` — tighter than existing `NotInverted` check (16 KiB threshold → 64 KiB) and bounds ratio to `[1.0, 50.0]` so both inversion and double-counting trip.
  - `tests/e2e/tenant_stats_consistency_test.go::TestTenantUI_RendersCompressionAndRawBytesFields` — UI bundle must reference `compression_ratio` / `raw_bytes` / `total_bytes` so a missing column can't hide future drift.

### Added

- **Multi-tenant S3 isolation (PR #111)** — full implementation of the `docs/multi-tenancy.md` boundary principle: string aliases are presentation-only at external surfaces; everything internal stays integer-keyed.
  - **In-path S3 isolation**: `BatchWriter` groups rows by `(AccountID, ProjectID)` at flush and writes one Parquet file per tenant per partition under the resolved prefix. The compactor, retention manager, lifecycle scheduler, and pool registry all consume the per-tenant prefix path so a tenant's data can never be reached via another tenant's pool.
  - **Per-tenant config overrides** with global-default inheritance: lifecycle, cardinality, rate-limit, retention, and tenant-aware bucket selection can all be overridden per `(AccountID, ProjectID)` via a YAML policy file. Unspecified knobs fall back to the global defaults.
  - **Bucket isolation** — "one process, many buckets": the s3reader pool registry resolves a per-tenant bucket from the policy and maintains a separate client pool per tenant, so a single lakehouse process can serve isolated S3 buckets without restart.
  - **Retroactive bucket migration tool + admin endpoint** for moving an existing tenant from the shared bucket to its own bucket without ingest downtime.
  - **UI + stats API surface every tenant edge case**: `/api/stats` now reports per-tenant `raw_bytes`, `compactor_*`, and the new VL/manifest parity endpoint with a matching UI panel; `global` is the sum across all tenants rather than an opaque counter. Compactor tenancy fixed so cross-tenant compaction is rejected.
  - **e2e**: tenant stats consistency + UI breakdown tests; e2e compose mounts a YAML policy file to demonstrate per-tenant overrides.

## [0.39.0] - 2026-06-07

### Fixed

- **Cold Jaeger search returned 0 traces at 12h while hot returned 20** — VT's `GetTraceList` step 2 issues `trace_id:in(t1,t2,...,t20)` for span fetch. Our `smartCache` fast-path in `lakehouse-traces/internal/storage/parquets3/storage_query.go::preFilterFiles` was unioning `FindFilesByTraceID(t_i)` across all queried trace IDs and narrowing to that union — but the union is only a *lower bound* on the relevant file set, because `smartCache` only records files it has previously fetched. A partial cache hit (one tid known, the rest never seen) collapsed candidates to one file that held spans for at most that one tid; spans for the other 19 vanished. Live blast radius: cold drilldown "Slow traces" tab returned empty at the 12h window even though step 1 found 20 trace IDs. Fix: take the smartCache fast-path **only for single-id queries** (the trace-by-id shape). Multi-id `trace_id:in(...)` falls through to bloom + `_trace_idx` narrowing, which examines every file and is honest about coverage. Guarded by:
  - `lakehouse-traces/internal/storage/parquets3/cold_hot_parity_test.go::TestColdHotParity_SmartCachePartialHit_MustNotNarrowSilently` — unit pin that exercises `preFilterFiles` with a hand-seeded metadata map (one tid cached, one tid missing) and asserts both files survive.
  - 8 sibling parity tests in the same file (`TraceIdxKeptFile`, `TraceIDInFilter`, `NegationFilter`, `UnindexedFileMustStillEmitRows`, `CombinedStreamAndTraceID`, `OutOfWindowReturnsZeroNoError`, `MultipleFilesNarrowingMustAgree`, `TraceIdxIntegrity_WriterSelfCheck`) that mirror the exact VT step-1 / step-2 query shapes from `vtselect/traces/query/query.go` so a regression in column resolution, time-window narrowing, or `_trace_idx` decoding fires at the storage layer instead of presenting as "0 traces in the UI".
  - One known regression class is deliberately left as `t.Skip("known #99 tail")`: `TestColdHotParity_FieldEqByParquetName` pins `service.name:="X"` (operator-typed parquet column name) — the main scan path needs the same dual-emission a5576bf added to `parquetRowToFields`. Remove the Skip when fixed.

### Added

- **Option B — logstorage-native queryable buffer (cold-tier recently-flushed parity).** The insert buffer can now be a real per-pod `logstorage.Storage` (the VT/VL model) instead of a `[]schema.{Log,Trace}Row` staging slice that was reconstructed into a `logstorage.DataBlock` at query time. That struct→DataBlock converter kept drifting from the file-scan emission (missing `_stream`, `start_time_unix_nano`/`end_time_unix_nano`, map attrs), which made cold Jaeger/Tempo search return 0 for fresh traces, 404 the log→trace drilldown (Grafana `Cannot read properties of undefined (reading 'spanID')`), and zero the Tempo service-filter. Behind `insert.buffer_engine` (`buffer` default | `logstore`): the buffer is fed via the exported `MustAddRows` (dual-write, legacy path stays authoritative) and **queries serve the recent/unflushed window from it via the exported `Storage.RunQuery`** — byte-identical to a file scan, no conversion. With `logstore` enabled end-to-end, cold `[24h]` Jaeger goes 0→20 (matches hot), recent trace-by-id resolves at all recencies incl. the freshest ~30s, and Tempo `{nestedSetParent<0}` + `{resource.service.name="X"}` both return data. Durability reuses logstorage's own disk parts + restore-on-open (crash-loss window == VT/VL hot; no LH WAL added). Pure exported-API reuse — **no VL/VT modification**. Phased (P1 dual-write + P3 read-merge shipped; P4/P5 retire the legacy path + LH WAL). A buffer failure can never break ingestion (recover-isolation + `lakehouse_buffer_store_dualwrite_failures_total`). See `docs/architecture/buffer-queryable-store-design.md`.

### Changed

- **Bump loki-vl-proxy from v1.56.1 to v1.57.0** (`deployment/docker/Dockerfile.loki-vl-proxy`). Headline fix: high-cardinality Drilldown label/field panels (pod, `*_id`, `trace_id`/`span_id`) now render correctly and fast at 24h+ — they previously returned ~142k uncapped single-point series at short ranges or an empty matrix at 24h+ (VictoriaLogs stats body exceeded the 16 MB cap), rendering as a single right-edge spike. Now `detected_level` filters evaluate against the column-indexed `level` field (~64× faster, no `unpack_logfmt` re-parse; 24h pod query 26s → ~0.6s), single-field `count() by(field)` over ≥2h routes to `/select/logsql/hits` for full-timeline series, and the 24h+ querySplitting residual-chunk spike is suppressed on every stats path. Also adds an L0 hot-key cache tier to metrics (`tier="l0"` on the `cache_tier_*` series).

### CI

- **Hot/cold parity unit job** (`.github/workflows/ci.yaml::parity-unit`) — runs `TestColdHotParity*` + `TestFieldEqualityAndStreamFilter` (traces) and `TestFieldNames_VLParity` + `TestInsertAndQuery_FieldNameParity` (logs) under `-race`, emits a job summary with pass/fail/skip counts and expanded failure details, uploads test artifacts. The same tests already run as part of `test-traces` / `test-logs`, but a dedicated named job makes the parity check visible in PR pages and gives reviewers a single status icon to look at — when any of these fail it's almost always one of the cold/hot regression classes the drilldown has shipped before (cf. #99, a5576bf, be8c126).

### Docs

- **`docs/architecture/performance-machinery.md` §6.2** — corrected the zstd-level claim: parquet-go's writer maps integer levels to **only four buckets** (`Fastest`/`Default`/`Better`/`Best`) regardless of the integer value, so the previous text claiming `[3, 7, 11] → [3, 9, 15]` "doubles compaction CPU" was wrong — both schedules land in `[Default, Better, Best]` and produce the same encode profile. New table shows the integer ranges and bucket mapping so operators don't tune a knob that doesn't move.

## [0.37.4] - 2026-06-04

### Security

- Bump Go toolchain from `1.26.3` to `1.26.4` across `go.mod`, `lakehouse-traces/go.mod`, `Dockerfile.{logs,traces,datagen}`. Resolves the two stdlib advisories that were failing `govulncheck` / `security-logs` / `security-traces` jobs on every CI run: **GO-2026-5039** (`net/textproto.Reader.ReadMIMEHeader` reached via `internal/s3reader/reader.go`) and **GO-2026-5037** (`crypto/x509` inefficient candidate hostname parsing reached via `cmd/s3proxy/main.go` and `internal/manifest/metadata_sidecar.go`). Both fixed in Go 1.26.4.

### CI

- `Dockerfile.logs` and `Dockerfile.traces` now pin the builder stage to `--platform=$BUILDPLATFORM` and cross-compile via `GOOS=$TARGETOS GOARCH=$TARGETARCH`. The previous form ran the builder under QEMU on the target architecture, which intermittently failed `git clone` of VictoriaLogs (~70 MB packed) with `fatal: cannot pread pack file: Bad address` / `invalid index-pack output` on the linux/arm64 branch of the multi-arch release build (run 26814378149). This silently broke every subsequent non-docs release. Native-host git + Go cross-compile eliminates the QEMU pack-file bug and reduces multi-arch builder time from minutes to ~10–15 s per architecture.
- New `.dockerignore` excludes `deps/` and `lakehouse-traces/deps/` from the build context. These directories are cloned + patched fresh inside the builder stage; including them via `COPY . .` would clobber the patched state with stale host clones from previous local builds at different VL/VT commits, producing cryptic "undefined symbol" errors that don't occur in CI (fresh checkouts have no `deps/`). Also excludes `dist/`, `bin/`, `.git/`, `.github/`, and ephemeral test/coverage outputs to shrink the build context.

## [0.37.3] - 2026-06-02

### Added

- **VictoriaTraces parity milestone — cold-tier Tempo `/api/v2/traces/<id>`
  fast path + Loki → Tempo drilldown (PR #105)** — closes the
  trace-by-ID feature gap between upstream VictoriaTraces and the
  lakehouse cold tier. Three cooperating layers:

  1. **Write-side hygiene + observability**
     (`lakehouse-traces/internal/vlstorage/insert.go`,
     `internal/metrics/lakehouse.go`). VT's vtinsert pipeline emits
     internal index rows alongside spans — `trace_id_idx_stream` rows
     carrying per-trace (start, end) bounds, and `trace_service_graph_stream`
     rows carrying service-graph edges. Both are part of VT's own query
     path and must not become degenerate spans with empty `trace_id`
     in cold Parquet. The existing detector (`vtInternalRowKind`) now
     returns the metric `kind` label, and a new
     `lakehouse_vt_internal_rows_dropped_total{kind=trace_id_idx|service_graph}`
     counter exposes how many we discard so a future regression — VT
     renaming a field or a new ingest path bypassing the filter —
     becomes visible from `/metrics` immediately.

  2. **`_trace_idx` Parquet footer fast path**
     (`lakehouse-traces/internal/storage/parquets3/trace_index_lookup.go`,
     `lakehouse-traces/internal/vtstorage_adapter/trace_index_fastpath.go`,
     `internal/traceindex/` — a new shared package). Every trace Parquet
     file written by the cold-tier batch writer already carries a
     compact per-trace `(trace_id, partition, start_ns, end_ns)` summary
     in the standard Parquet `FileMetaData.key_value_metadata` slot —
     same documented field Apache Iceberg / Delta / Hudi use for their
     own table metadata, so duckdb, pyarrow `pq.read_metadata`, and
     `parquet-tools meta` all read it cleanly. The vtstorage adapter
     now intercepts VT's `{trace_id_idx_stream=<bucket>} AND trace_id_idx:=<id>`
     stats query *before* the previous scan-rewrite path, calls
     `Storage.LookupTraceIndex(ctx, traceID)`, and emits a synthetic
     `DataBlock` with the three columns VT's `findTraceIDTimeSplitTimeRange`
     reads (`_time`, `start_time`, `end_time`). No row group is ever
     opened. Span scan remains as the fall-through for files whose
     footer is unreachable. The new
     `lakehouse_trace_index_lookups_total{result=hit|miss|error}` metric
     makes the hit ratio observable.

  3. **Compaction parity for the footer index**
     (`internal/compaction/compactor.go`,
     `internal/traceindex/traceindex.go`). Before this milestone,
     `writeCompactedTraces` dropped `_trace_idx` on every merge,
     collapsing cold-tier trace-by-ID back to a full span scan as soon
     as compaction ran. The index codec is now hoisted to the shared
     `internal/traceindex/` package so the compactor and the writer
     share one source of truth, and the compactor passes the recomputed
     index through `parquet.KeyValueMetadata(traceindex.MetadataKey, …)`.
     A regression test (`TestCompactor_PreservesTraceIndexFooter`) opens
     the merged Parquet via plain `parquet.OpenFile` — proving the file
     is still 100% spec-compliant — and asserts both per-trace bounds
     plus the VT-compatible `xxhash64(traceID) % 1024` partition value
     survive the round-trip.

  4. **Upstream bump: VictoriaTraces v0.9.2 + VictoriaLogs v1.50.0**
     (`Makefile`, `lakehouse-traces/go.mod`, `patches/{vt-traces,vl-traces}/`,
     `deployment/docker/docker-compose-{e2e,benchmark}.yml`,
     `tests/parity/docker-compose.yml`). v0.9.2's release note —
     "exclude unnecessary streams during trace search" — fixes the
     parallel hot-tier leak we'd been compensating for on cold, so the
     two-stage cleanup composes correctly across both tiers. The
     `filter string` parameter VT added to
     `GetFieldNames` / `GetFieldValues` / `GetStreamFieldNames` /
     `GetStreamFieldValues` is now forwarded through the `ExternalStorage`
     overlay; the LH adapter applies the substring narrow client-side
     (`filterValuesBySubstring`), mirroring the logs adapter pattern.
     All three patches (dispatch, flag-dedup, go-mod-replace) apply
     cleanly against the new release.

  5. **Grafana drilldown completion**
     (`deployment/docker/grafana/provisioning/datasources/datasources.yaml`).
     Tempo datasources are hardened with `nodeGraph.enabled=false`
     (VT has no service-graph endpoint), the empty `serviceMap` block
     omitted (an empty `datasourceUid` triggers phantom `/api/metrics`
     calls), `streamingEnabled.{search,query}=false` (VT exposes no
     Tempo gRPC stream-over-http surface), and `spanBar.type=None` for
     parity with the Jaeger sibling datasources. The Loki proxy derived
     fields now route trace_id clicks straight to Tempo —
     `loki-vl-proxy-cold` → `tempo-lh-cold` and `loki-vl-proxy` →
     `tempo-vt-hot` — verified live in the e2e stack by drilling a
     real cold trace ID from a Loki cold log into the LH cold Tempo
     view and rendering the spans through Grafana's Tempo plugin.

  Constraints honoured throughout: zero VT/VL upstream modification
  (everything is either an adapter behind the `ExternalStorage` overlay
  in `patches/{vl-traces,vt-traces}/` or LH-side code); zero
  non-standard Parquet encoding (every byte LH writes to S3 is readable
  by any spec-compliant tool); zero Jaeger regression (the three Jaeger
  datasources remain provisioned and unchanged — explicit "open in
  Jaeger" usage and Grafana Jaeger plugin drilldown still work).

  Verified in `deployment/docker/docker-compose-e2e.yml` after rebuild:
  all six trace drilldown paths (3 Jaeger v1 + 3 Tempo v2 across hot
  / cold / multilevel `vtselect` fan-out) return HTTP 200 with real
  trace bodies; `lakehouse_trace_index_lookups_total{result="hit"}`
  climbs on every cold trace-by-ID; the cold-tier Grafana Explore
  panel for `tempo-lh-cold` renders trace spans from a Loki cold log
  link end-to-end.

- **Election-free compaction (spec 2026-05-31)** — replaces the K8s Lease /
  S3-sentinel single-leader scheme with HRW (Highest Random Weight) partition
  ownership computed in-process on every pod. Each pod independently decides
  which partitions it owns by ranking peers (from the existing peer-cache)
  with xxh64 — the highest-weight peer per partition wins, with deterministic
  tie-break by peer name. The result: zero K8s coordination dependencies and
  ~5 MB binary-size reduction per module (the entire `k8s.io/client-go` REST
  closure + 13 transitive deps drop out).
  - `internal/compaction/ownership.go` — `OwnershipResolver` with AZ
    stratification (same-AZ peers preferred, fall back to all-AZ when same-AZ
    is drained), `IsDraining` filter, ring-stabilization gate (defer ownership
    decisions during ring change), and `RankedOwners` / `SecondaryOwner` /
    `TertiaryOwner` ladders for Tier A failover.
  - `internal/compaction/orphan_sweep.go` — two-tier orphan reclamation.
    Tier A: detects stale `LastCompactionAttempt` per partition (≥3 × Interval)
    and lets the secondary HRW owner steal compaction. Tier B: walks S3 prefix
    layout, hash-buckets dates across pods, and deletes parquet keys that
    survive a three-step safety gate (not-in-manifest + age-gate + post-LIST
    manifest re-snapshot).
  - `internal/compaction/fair_share.go` — per-tenant round-robin scheduler.
    Cursor advances across tenants every tick so no noisy tenant can starve
    others. Configurable `CompactionsPerTenant` budget.
  - `internal/compaction/drain_handler.go` + `Scheduler.Drain()` —
    HTTP `POST /lakehouse/drain` endpoint marks the pod as draining,
    waits for in-flight compactions to finish (bounded by `DrainTimeout`,
    default 90 s), and emits the `X-Lakehouse-Draining: true` header so peer
    pods exclude us from the HRW ring within one tick.
  - `manifest.AddFile` idempotency + per-partition `LastCompactionAttempt`
    tracking — the manifest now silently no-ops on duplicate (key, partition)
    inserts, surfacing a `lakehouse_manifest_addfile_duplicate_key_total`
    canary for hidden upload bugs.
  - **12 new compaction observability metrics** including
    `compaction_partitions_owned`, `compaction_ownership_self_in_peers`,
    `compaction_dual_ownership_total` (load-bearing — alerts on >0),
    `compaction_orphan_files_deleted_total`, `compaction_orphans_skipped`
    (per-reason vector), `compaction_deferred_stabilizing`,
    `compaction_deferred_ring_thrash`, `compaction_draining`,
    `compaction_aborted_during_drain_total`, `compaction_stolen_total`,
    `compaction_sweep_deferred_stabilizing` (Tier A / Tier B).
  - **41 new tests** (33 edge cases from spec §3 + 8 HPA recovery tests from
    spec §11.6). Every load-bearing assertion documents a negative-control
    revert in its leading comment — removing the corresponding production
    guard must cause the test to fail. Coverage gates met:
    `ownership.go` 96.25 % (>= 95 %), `orphan_sweep.go` 91.96 % (>= 90 %),
    `fair_share.go` 94.78 % (>= 90 %).
  - **HPA-safe scaling chart defaults** — PDB template (`pdb.yaml`),
    `preStop` hook calling `POST /lakehouse/drain`, generous
    `terminationGracePeriodSeconds`, and per-component PDB toggles
    `select.podDisruptionBudget.enabled` / `insert.podDisruptionBudget.enabled`.

- **Runtime `Acquire` wiring for 4 resource-bound surfaces** — turns the K8s-style resource bounds added in v0.37.1 from metric-exposure-only into real backpressure with admit/reject semantics. Surfaces wired in both modules (logs + traces):
  - Query file workers — admit at `fileWorkerLoop`; ctx-cancel during blocked Acquire surfaces as "file workers limit exceeded" error (was: queued indefinitely on channel).
  - Cache memory — `LRU.SetBound`; `Put` returns silently when bound rejects, cache becomes best-effort (was: silent LRU eviction only).
  - Smart cache disk — `DiskCache.SetBound`; `Put` / `PutFromPath` return `ErrBoundFull` on rejection (was: always succeeded, watermark eviction only).
  - Query max rows — admit at `acquireQueryMaxRowsBudget`; "query max-rows budget exhausted" error when N concurrent queries × maxRows exceed the bound's Limit (was: per-query soft check only).
- New `resourcebounds.Bound.TryAcquire(n)` non-blocking API + exported `ErrBoundFull` sentinel for cache hot paths that cannot tolerate blocking on bound exhaustion.
- 34 new unit tests across both modules covering admit/reject paths, release on every code path (eviction / Delete / Clear), nil-bound passthrough, outlier admission (oversized single holder admitted alone), ctx-cancel during blocked Acquire, and rejection-metric increments. Each load-bearing assertion documents a negative-control proof ("comment out X → this test must fail") per the harden-and-lock rule.
- Live e2e metrics confirm load-bearing in production: `lakehouse_resourcebound_cache_memory_acquired_total 1162`, `…query_file_workers_acquired_total 2489`, `…query_max_rows_acquired_total 5` measured against the rebuilt e2e compose stack.
- `cache/lru.go` and `cache/disk.go`: added `SetBound`, `RejectedByBound` accessors and per-entry `boundRelease` closures so every code path that drops an entry (eviction, Delete, Clear, Update-with-replace) releases its slot back to the bound exactly once.
- `internal/storage/parquets3/storage_query.go`: extracted `processOneFile`, `fileWorkerLoop`, `acquireQueryMaxRowsBudget` helpers to keep `RunQuery` within the 50-line gocyclo budget after wiring the new admit points.

### Fixed

- **Stats API compression endpoint fallback** — the manifest-only fallback path (when registry is empty) was not including `RawBytes` in the response, causing compression ratio to show as 0 in the Lakehouse UI. The fallback now correctly accumulates `RawBytes` from `TenantSummaries()` and includes it in per-tenant compression entries, allowing the average compression ratio calculation to properly compute `RawBytes / TotalBytes`.

### Removed (election-free compaction)

- **`internal/election/` package** — entire directory deleted (~5 kLOC across
  `auto.go`, `k8s.go`, `s3.go`, `noop.go`, `leader.go`, plus coverage, fuzz,
  integration, leak, regression, and soak test suites). HRW ownership
  (above) makes this code unnecessary.
- **`internal/compaction/sentinel.go` + `sentinel_test.go`** — the S3
  sentinel lock that prevented two pods from compacting the same partition.
  Replaced by HRW: each partition has exactly one HRW-elected owner per tick.
- **`internal/compaction/sharding.go` + `sharding_test.go`** — the
  modulo-shard partition assignment scheme. Replaced by HRW (better
  rebalancing properties on N→N+1 transitions: only ~1/N partitions move).
- **`BloomController.SetLeader` / `IsLeader`** — bloom tuning state is
  per-pod (cfg / overrides / adjustments live on the controller instance
  only), so the previous leader gate was decorative. Every pod now auto-tunes
  its own bloom params.
- **`config.CompactionConfig` fields**: `LeaderElection`, `LeaseDuration`,
  `S3LockTTL`, `S3Heartbeat`, `ShardID`, `ShardCount`.
- **CLI flags**: `-lakehouse.compaction.leader-election`,
  `-lakehouse.compaction.lease-duration`, `-lakehouse.compaction.s3-heartbeat`,
  `-lakehouse.compaction.s3-lock-ttl`, `-lakehouse.compaction.shard-id`,
  `-lakehouse.compaction.shard-count`.
- **Election metrics**: `lakehouse_election_leader`,
  `lakehouse_election_transitions_total`, `lakehouse_election_health_checks_total`.
- **Chart artifacts**:
  - `charts/.../templates/compaction-rbac.yaml` (Role + RoleBinding for
    `coordination.k8s.io/leases`).
  - `charts/.../templates/tenant-rbac.yaml` (same surface for the
    tenant-alias sync leader — see spec §10 Q6; the actual alias-sync
    migration to HRW ownership is tracked separately).
  - `lakehouseConfig.compaction.leader_election` / `lease_duration` /
    `s3_lock_ttl` / `s3_heartbeat` / `shard_id` / `shard_count` keys from
    `values.yaml`.
  - `POD_NAMESPACE` downward-API env-var injection in `statefulsets.yaml`
    (consumed exclusively by the now-deleted elector).
- **E2E**: `.github/workflows/e2e-k8s.yaml`,
  `tests/e2e-k8s/{kind-config.yaml,test_leader_election.sh}`,
  `tests/verification/probe_k8s_election_failover.sh`.
- **Go dependencies tidied by `go mod tidy`** on both modules:
  `k8s.io/apimachinery v0.36.0`, `k8s.io/client-go v0.36.0`,
  `k8s.io/klog/v2 v2.140.0`, `k8s.io/kube-openapi`, `k8s.io/utils`,
  `sigs.k8s.io/json`, `sigs.k8s.io/randfill`, plus 13 transitive deps
  (`fxamacker/cbor/v2`, `modern-go/reflect2`, `json-iterator/go`,
  `munnerz/goautoneg`, `golang.org/x/term`, `golang.org/x/time`,
  `gopkg.in/inf.v0`, `go.yaml.in/yaml/v2`, `davecgh/go-spew`,
  `x448/float16`).

### Deferred

- S3 download bound runtime wiring in `lakehouse-traces` — the traces module's `getFileData` calls `s.pool.Download` directly without the channel-based admission point the logs module uses (v0.37.1's wiring point). Wiring it would require either adopting the logs-module `dlSem` pattern or refactoring to a different admission point — both invasive enough to deserve their own PR. The bound is constructed in traces for metric exposure (request/limit gauges populated at startup; outstanding=0 at idle). All 4 other surfaces are fully wired in traces.

### Performance

- `Bound.TryAcquire` hot path: 25.84 ns/op; rejection path: 4.49 ns/op (Apple M5 Pro).
- `cache.Put` with bound wiring: 192.6 ns/op vs 144.7 ns/op baseline (+47.9 ns, +33%). Bound-rejection fast-path is actually faster: 133.8 ns/op (-7.6%) — the cache no-ops without LRU shuffle when the bound rejects.
- `Bound.Acquire` blocking variant: 295.8 ns/op — slower than `TryAcquire` because it spawns a ctx-watch goroutine for the cancellation path.

## [0.37.2] - 2026-05-31

### Changed

- `applyFlags` in `cmd/lakehouse-logs/main.go` and `lakehouse-traces/main.go` split into nine per-section helpers (`applyTopLevelFlags`, `applyS3Flags`, `applyResourceBoundFlags`, `applyTopologyFlags`, `applyManifestFlags`, `applyCacheFlags`, `applyCompactionFlags`, `applyQueryLegacyFlags`, `applyLogsFlags` / `applyTracesFlags`, `applyTenantFlags`). Removes the `//nolint:gocyclo` suppression added in v0.37.1 when the K8s resource-bound triples pushed the flat dispatch past cyclomatic complexity 50. Each helper's complexity is linear in flag count per section; no behaviour change — every flag-assignment branch preserved in its original relative position.

## [0.37.1] - 2026-05-30

### Added

- `internal/resourcebounds` package — generalises the in-tree `fileBudget` semantics into a reusable `ResourceBound` primitive with K8s-style `Request` (always-reserved baseline), `Limit` (hard ceiling, enforced via blocking `Acquire`), `LimitCount` (per-holder count cap), and `ScalingPolicy` enum (`Fixed`, `LinearGrowth`, `ExponentialBackoff`). Preserves the legacy outlier-admit semantics (single oversized holder admitted alone when pool empty) so individual large parquet files remain processable. Includes `PrometheusSink` adapter and `Resolve` helper that handles the operator-facing flag triple resolution (new triple takes precedence, deprecated alias falls back with one-time warning).
- 30 new per-surface metrics in `internal/metrics/lakehouse.go` — `lakehouse_resourcebound_<surface>_{acquired,rejected,outstanding_bytes,outstanding_count}_total` + `_request` / `_limit` info gauges, for all 5 surfaces (s3_concurrent_downloads, query_file_workers, cache_memory, smart_cache_disk, query_max_rows).
- Five operator-facing K8s-style flag triples:
  - `-lakehouse.s3.concurrent-downloads.{request,limit,scaling}`
  - `-lakehouse.query.file-workers.{request,limit,scaling}`
  - `-lakehouse.cache.memory.{request,limit,scaling}`
  - `-lakehouse.smart-cache.disk.{request,limit,scaling}`
  - `-lakehouse.query.max-rows.{request,limit,scaling}`
- `shouldUseWildcardRangeRead` in both `internal/storage/parquets3/range_reader.go` and `lakehouse-traces/internal/storage/parquets3/range_reader.go` — switch that opens parquet files via lazy S3 ReaderAt instead of buffered full download for wildcard queries on files >=4MiB. Bounds wildcard heap to working-set-row-group bytes (<10MiB/file) instead of cumulative-file-bytes (16 workers × ~30MiB ≈ 480MiB).
- Unit tests: 22 in `internal/resourcebounds` (Bound, Resolve, PrometheusSink, ScalingPolicy, fileBudget-legacy-semantics) + 8 in `internal/storage/parquets3/resourcebound_wiring_test.go` (5-surface defaults populated, S3 deprecated alias honored, new triple takes precedence) + 5 across both modules' `range_reader_test.go` covering the wildcard cutoff.

### Changed

- `openParquetFile` (both modules): wildcard (`projectedCols == nil`) queries on files >=4MiB now use lazy S3 ReaderAt + BufferedReaderAt + CoalescingReaderAt chain instead of `bytes.NewReader(getFileData())`. Cache hit short-circuit preserved (no per-row-group HTTP overhead on cached files).
- `internal/storage/parquets3/storage.go` Storage struct: adds `bounds *resourceBoundSet` and `s3DownloadsBound *resourcebounds.Bound` fields. S3 downloads now tick the new bound for metric visibility (channel-first acquire order preserves wire semantics 1:1 with pre-bound behaviour).
- Five YAML config field families extended on `S3Config`, `QueryConfig`, `CacheConfig`, `SmartCacheConfig` (additive — the existing single-value fields continue to work as deprecated aliases that fire one startup warning each).

### Deprecated

- `-lakehouse.s3.max-concurrent-downloads` (replaced by `.concurrent-downloads.{request,limit,scaling}` triple)
- `-lakehouse.query.file-workers` (replaced by `.file-workers.{request,limit,scaling}` triple)
- `-lakehouse.cache.memory-mb` (replaced by `.memory.{request,limit,scaling}` triple)
- `smart_cache.disk_limit_max` YAML (replaced by `.disk.{request,limit,scaling}` triple)
- `-lakehouse.query.max-rows` (replaced by `.max-rows.{request,limit,scaling}` triple)
- Each fires a one-time startup `logger.Warnf` deprecation warning when set without the corresponding new triple. Behaviour preserved exactly (flat ceiling at alias value). Scheduled for removal in v1.0.

### Fixed

- 7-day wildcard heap retention: 3 back-to-back 7-day wildcard runs against the production-shape compose stack peak at ~785-815 MiB container RSS post-Goal B (well under the 1.0 GiB target; pre-this-work measured peak was ~1.7 GiB per project history). `lakehouse_s3_range_reads_total` confirms Goal B fires for ~35% of file opens (1062/3063 in the 3-run sweep) — the remainder are either small files (<4MiB cutoff) or projected queries (use the original range-read path).
- Tombstone validation: inverted time range (StartNs > EndNs) no longer falsely matches files
- S3 endpoint SSRF protection: validates URL scheme and blocks link-local/cloud metadata IPs
- Traces `start_time_unix_nano` schema type changed from `TypeTimestampNano` to `TypeInt64` — now returns numeric epoch nanos matching VT format instead of RFC3339 formatted strings
- Bloom filter: disable token extraction for OR queries (`" or "`) that produce false-negative filtering
- Pushdown filter: disable file-level filtering for OR queries to prevent incorrect result exclusion
- Token bloom: skip regex (`~`), range, and `len_range` predicates that bloom filters cannot model
- Token bloom: skip syntax fragments containing brackets, parens, or quotes
- Parity test syntax fixes for VL v1.50.0: `replace`/`replace_regexp` require `at` keyword, `dedup` replaced with `uniq`, `stats count() / N` split into `stats + math` pipes, quoted colon-containing field names in `stats by()`
- Bloom filter (row-group): OR within column / AND across columns for `field:in(v1,v2,v3)` — previously AND'd same-column values, dropping every row group that didn't bloom-contain the first value. Caused Jaeger spans-lookup (stage 2) to return 0 traces even after stage 1 found trace_ids.
- File-level bloom pre-filter: handles `:in(...)` values via per-value union (`MayContainAll` per value, union of matches). `preFilterFiles` / `filterFilesByBloomIndex` / `checkFileBloom` previously only handled the single-value form.
- Row-group time range: aggregate min/max across all pages instead of trusting edge pages (`MinValue(0)`, `MaxValue(N-1)`). Pages within a row group are not sorted by timestamp — traces especially have out-of-order spans (root emits after children). Narrow time windows from Jaeger's expansion loop were getting false-negatives skipping row groups whose true max lived in a middle page.
- Adapter pipe-passthrough: vtstorage adapter passes the full query (with pipes) to storage.RunQuery so `queryColumns` can expand the parquet column projection to cover pipe-referenced fields (`partition by (trace_id)`, `fields _time, trace_id`). Previously `CloneWithoutPipes` stripped pipes before storage, dropping trace_id from projection — Jaeger tag-filtered searches returned 0.
- Projection: bare `{tag=val}` stream selector (VL canonical form omits the `_stream:` prefix) now triggers `_stream` projection. Quoted field names (`"span_attr:http.status_code":=200`) now match `referencesField`. Without these, filterStream rejected every row and tag-filtered queries returned 0.
- Tempo search: HTTP shim converts legacy Grafana `tags=service.name=foo` panel shape to TraceQL `q={resource.service.name="foo"}` when `q` is empty. Upstream VT `parseTempoAPIParam` overwrites the documented `q="{}"` default with empty string when client sends `tags=` only, then `traceql.ParseQuery("")` fails and the handler returns `{"traces":[]}`. Shim wraps the VT call without modifying `deps/`.
- `_stream_id` populated at insert time via VL's xxhash + "magic!" suffix algorithm, producing the same 48-char lowercase hex VL produces for the same `_stream` labels. `/select/logsql/stream_ids` was returning empty for cold rows because the external insert path never set the field; required by the 100% VL/VT API compatibility rule.
- Truncated service names in `/select/jaeger/api/services`: removed parquet column-index seed in `extractDistinctFromStats` (column-index min/max values are truncated by parquet writers at 16 bytes per Apache Parquet PageIndex spec) and skip `detectConstantColumns` for ByteArray columns. Data-page scan is now the only source. Was producing `notification-ser`/`notification-ses` alongside `notification-service` in the services dropdown.
- LRU cache: `Get` returns the shared cached buffer instead of copying. Was the dominant heap consumer at idle (~358 MiB of transient copies on the hot path, 16 workers × ~57 files × ~2.5 MB).
- Label-index drift on disk: `LoadLabelIndex` drops `Values` entries not accounted for in `ValueCounts` — one-shot sanitization for stale on-disk state from earlier buggy runs (truncated BYTE_ARRAY prefixes leaking into the field-values API).
- Query memory budget: per-query `MaxLiveBytes` budget + process-wide `fileBudgetSem` (256 MiB resident, ≤8 concurrent files) + `rgDecodeSem` (GOMAXPROCS/2 concurrent row-group decoders) bound peak memory inside `mem_limit=2g` for multi-day wildcards. 7-day wildcard previously OOM-killed the container; now completes (HTTP 200) with peak heap ~1.4 GiB and stable restart count. Replaced 256-deep dispatch channel with synchronous `wbMu.Lock()` writeBlock pattern matching VL `searchParallel`; removed row-group parallel fan-out that produced 128 concurrent decoders (8 rg × 16 workers).
- `debug.SetMemoryLimit` forces Go GC under the cgroup memory limit so the runtime targets RSS, not just Go heap.
- Helm chart bumped from 0.36.0 → next release sequence; resource defaults updated to match the new file-budget / live-bytes / latency-offset shape used in e2e compose.

### Added

- VT v0.9.0 fork with ExternalStorage interface — same pattern as VL fork
- VT storage adapter bridging S3/Parquet backend to VT's Jaeger and Tempo handlers
- Tempo API datasources for hot (VT disk) and cold (S3 Parquet) tiers
- Trace index in Parquet metadata for fast trace_id lookups
- Regression tests: MinTimeNs==0 sentinel handling, token bloom pipe stripping, tombstone edge cases
- Security tests: 29 SSRF attack vector tests for S3 endpoint validation
- Benchmarks: coalescing reader, manifest fast path, token bloom extraction, projection columns
- Parity tests: 378 tests across 21 test functions — time range, filter, pipe, stats, cross-validation, traces LogsQL, and full data format compatibility against VL/VT reference
- K8s scaling safety: phased shutdown orchestrator (drain → flush → persist → release) with per-phase timeouts
- K8s scaling safety: startup staleness detection with WAL reconciliation and cache revalidation
- K8s scaling safety: ring change detection with shadow member stabilization during scaling events
- K8s scaling safety: lifecycle HTTP endpoints (`/internal/lifecycle/drain`, `/ready`, `/ring`, `/stale`)
- K8s scaling safety: 14 new Prometheus metrics for shutdown, startup, ring change, and query continuity
- Helm: HPA scaleDown stabilization window, preStop drain hook, lifecycle readiness probe
- `-lakehouse.query.max-live-bytes` flag — per-query live-DataBlock byte budget (default 512 MiB)
- `lakehouse_query_memory_budget_exceeded_total` metric — counts queries cancelled because the per-query budget tripped
- `internal/cache.LRU.PutNoCopy` — store buffer by reference for the S3-download hot path
- Per-component verification matrix `tests/verification/matrix.md` — 78 rows tracking every exposed HTTP surface (logs/traces query + insert, admin, Grafana datasources, UIs). Companion smoke probes `tests/verification/probe_*.sh` lock per-surface behavior: `probe_jaeger_search_24h.sh`, `probe_jaeger_search_24h_with_tag.sh`, `probe_jaeger_search_24h_full_chain.sh`, `probe_tempo_search_24h.sh`, `probe_logs_24h_wildcard.sh`, `probe_logs_Nday_wildcard.sh`, `probe_matrix_sweep.sh`, `probe_image_freshness.sh`.
- Image-freshness probe catches stale-binary deploys (compares each container image's `CreatedAt` to the newest source commit time).
- `_stream_id` unit tests including VL-algorithm-oracle test (`TestComputeStreamID_MatchesVLAlgorithm`) that re-implements VL's hash128 inline and asserts byte-for-byte match.
- Production-shape memory-budget integration tests: `TestRunQuery_ProductionShape_WildcardScalesUnderMemoryBudget` (200 files × 5000 rows) and `TestRunQuery_7DayProductionShape_FileBudgetBoundsPeak` (600 files × 8000 rows).
- Tempo HTTP shim test suite: `TestNormalizeTempoSearchParams` (11 cases) + `TestTempoTagsToTraceQL` (13 cases of the scope-prefix mapping).
- Bloom OR-in-clause regression tests: `TestS3_bloomFilterSkip_InClauseOrSemantics` (traces) + `TestInteg_bloomFilterSkip_InClauseOrSemantics` (logs).
- Row-group time-range page-aggregation regression tests covering out-of-order page bounds.
- Matrix sweep: 20 of the 22 `UNVERIFIED`/`DIFFER` rows in `tests/verification/matrix.md` flipped to `PASS` (verified upstream-equivalent against the live e2e compose stack); 2 rows held as `DIFFER` with VT-version notes (`T17` `/select/tempo/api/metrics/instant`, `TI2` `/insert/zipkin/api/v2/spans` — neither exists in VT v0.9.0). `probe_matrix_sweep.sh` is the regression lock: replays all 22 rows end-to-end and asserts the minimum upstream-compat contract per row, so any future regression on a previously-verified surface fails CI rather than silently flipping the matrix back to UNVERIFIED.
- `tests/verification/check_matrix_coverage.sh` + GitHub Actions `Verification Matrix Check` job — fails any PR that introduces a path reference in `tests/verification/matrix.md` which doesn't resolve to a real file on disk. Makes the matrix a self-policing contract: renaming or deleting a probe/test cited by the matrix now forces the matrix to be updated in the same PR. Scoped to in-tree directories (`tests/`, `internal/`, `cmd/`, `charts/`, `deployment/`, `lakehouse-traces/`, `docs/`, `scripts/`, `.github/`) — upstream VL/VT references under `deps/` are intentionally skipped because that directory is `.gitignore`d and only populated at CI build time.
- **K8s leader election test pyramid**: 15-case unit suite against an httptest-backed Lease server (`internal/election/k8s_test.go`), multi-candidate integration (`k8s_integration_test.go`), fuzz target on the state machine (`k8s_fuzz_test.go`), goroutine-leak guard (`k8s_leak_test.go`), opt-in 1h soak test (`k8s_soak_test.go`, build tag `soak`), and a regression-lock suite (`k8s_regression_test.go`) covering forbidden-import detection, dep-closure count, binary-size bound, FIPS round-trip, and the RenewDeadline liveness invariant. Coverage on `internal/election/k8s.go` measured at 90.8% (≥90% required by CI).
- **Real-K8s e2e for leader election** (`tests/e2e-k8s/test_leader_election.sh` + `.github/workflows/e2e-k8s.yaml`): spins up a single-node `kind` cluster, installs the LH Helm chart with 3 insert replicas, and asserts (1) chart renders the ServiceAccount + Role + RoleBinding with verbs `get,list,create,update,patch` on `coordination.k8s.io/leases`; (2) Lease object is created with a valid `holderIdentity`; (3) deleting the leader pod triggers a successor within `LeaseDuration + RenewDeadline = 40s`; (4) **negative control**: deleting the RoleBinding causes leader election to fail loudly with a 403 in the logs (proves the chart's RBAC is load-bearing); (5) two releases in different namespaces hold independent leases without cross-talk. Path-filtered to run only on PRs that touch election code, the chart, or Dockerfiles — total CI budget ~4 minutes.
- **E2E verification probes**: `probe_k8s_election_failover.sh` (httptest-based failover smoke), `probe_fips_active.sh` (FIPS round-trip lock), `probe_binary_size.sh` (≤40 MB ceiling for both binaries), `probe_image_size.sh` (≤70 MB ceiling for the distroless image). Coverage gate: CI `test-logs` and `test-traces` jobs now error on any `internal/<pkg>` package below 90% coverage.
- **Helm chart RBAC**: extends `coordination.k8s.io/leases` verbs from `get,create,update` to `get,list,create,update,patch` — covers `kubectl describe lease` for operators (list) and the elector's release-on-stop best-effort patch (patch). The new verbs are load-bearing per the kind e2e's negative test.
- **`internal/election/README.md` + `RUNBOOK.md`**: design doc with the allowed import surface, state machine diagram, coordination guarantees, and a 5-case operator triage runbook (no leader, 403 Forbidden, leader flapping, two leaders, lease never created).

### Changed

- Reduced `lakehouse-logs` / `lakehouse-traces` binary size from **55 MB to ~37 MB** (-18 MB, -33%) by replacing `k8s.io/client-go`'s heavy elector closure (full `kubernetes` clientset + `tools/leaderelection` + typed API modules) with a hand-rolled `rest+meta/v1` REST client in `internal/election/k8s.go`. K8s leader election is now always available (no build tag); the dep closure shrinks from ~700 to 329 packages. Adds `-trimpath` for reproducible builds. The earlier build-tag-gated approach was iterated to Option B so operators don't have to rebuild for K8s support. `TestNoForbiddenImports` regression-locks the closure against re-introducing `k8s.io/client-go/kubernetes`, `tools/leaderelection`, or the heavy typed-API modules.
- **Container image**: dropped the standalone `/usr/local/bin/healthcheck` binary (~3 MB) and folded the probe into a `lakehouse-{logs,traces} healthcheck [URL]` subcommand. The Dockerfile's HEALTHCHECK and docker-compose health probes call the main binary directly. Saves ~10% of image size and one fewer binary to maintain.
- **FIPS image variant**: added `--build-arg FIPS=1` (sets `GOFIPS140=v1.0.0`) to both `Dockerfile.logs` and `Dockerfile.traces`. Release workflow publishes a 2x2 matrix per release: `{logs,traces} × {default, FIPS}`. Tags: `:vX.Y.Z`, `:latest`, `:vX.Y.Z-fips`, `:latest-fips`. New `lakehouse-{logs,traces} fips-status` subcommand reports `fips140: enabled|disabled` for operability. FIPS mode also requires `GODEBUG=fips140=on` at runtime per Go 1.26 semantics.
- **Release workflow**: zstd compression on image layers + OCI media types (`--output type=image,compression=zstd,compression-level=3,oci-mediatypes=true,push=true`) — registry-side bytes shrink another ~25% over the gzip default. Optional Docker Hub mirroring gated on the `DOCKERHUB_USERNAME` secret being set (so forks don't fail on missing creds).
- Replace custom Jaeger handlers with VT upstream `jaeger.RequestHandler` (deleted 2451 lines)
- Deduplicate `storage.Storage` interface — traces module imports from root
- Bump VictoriaTraces hot tier from v0.8.2 to v0.9.0
- Bump loki-vl-proxy from v1.43.0 to v1.50.1
- `lakehouse.query.max-files-per-query` default flipped from 500 to **0 (unlimited)** — matches VL upstream which has no such cap. Memory budget (`query.max-live-bytes`, `fileBudgetSem`, `rgDecodeSem`) is the real safety net.
- `-search.latencyOffset` on lakehouse-traces set to 2m (matches `insert.flush-interval=120s`) so the upstream Jaeger search expansion loop finds freshly-flushed cold-tier spans.
- e2e compose: `mem_limit: 2g` + `restart: on-failure` on both LH containers (Docker-level safety net), `file-workers=16`, L1 cache 512→256 MiB to leave headroom for the file budget.
- Grafana datasource: Lakehouse Logs Cold derivedField now routes trace_id clicks to `victoriatraces-global` (hot+cold fan-out) instead of `victoria-lakehouse-traces` (cold only) so fresh trace_ids that haven't flushed to S3 yet still resolve via the hot tier instead of returning HTTP 404.

### CI

- `auto-release.yaml` workflow now clones + patches VictoriaTraces v0.9.0 into `lakehouse-traces/deps/VictoriaTraces/` before the build step. Prior releases failed with `replacement directory ./deps/VictoriaTraces does not exist` because the VT clone/patch was only wired into `ci.yaml`, not the release workflow.
- Release skip-regex extended to include `tests/verification/` (matrix.md + probe scripts) and `docs/superpowers/` (specs + plans) so verification-only and design-only PRs no longer trigger a no-op release.

## [0.36.0] - 2026-05-25

### Fixed

- Smart cache: snapshot loader now correctly handles legacy (pre-envelope) format on upgrade
- Smart cache: watermark-based LRU eviction when disk usage exceeds 90% of configured limit
- Smart cache: reconciliation uses file mtime for CreatedAt instead of current time
- Tenant name mapping: MetricLabel "both" format now includes numeric ID prefix (42:3/name)
- Tenant name mapping: OrgID validation on S3 alias load rejects invalid entries
- Tenant name mapping: persistence errors are now logged instead of silently discarded
- Partitioned bloom: manifest metadata updated after successful bloom persist

## [0.35.0] - 2026-05-25

### Performance

- Benchmark: increase trace datagen volume from 20K to 50K spans for more representative optimization measurements

## [0.34.0] - 2026-05-24

### Performance

**S3 I/O Layer Optimization (Phases A-J):**

- Read-ahead buffer: 256KB streaming buffer reduces small S3 reads by batching sequential access
- Range coalescing: merges nearby column ranges within 64KB gap tolerance into single S3 requests
- Transport tuning: configurable HTTP/2 concurrency, idle connections, and response header timeouts
- Async row group prefetch: background goroutine pre-fetches next row group while current processes
- Compaction: merges small Parquet files into target 256MB blocks to reduce per-file S3 overhead
- Streaming aggregation: single-pass count/sum/min/max avoids full materialization for simple aggregates
- Cache-partitioned reads: AZ-aware partition modes (az-local/global/distributed) route cache lookups
- Cache maximization: column-level chunk caching, scan pollution protection, LRU with L2 spilling
- Distributed compaction: CRC32 partition sharding with K8s StatefulSet auto-detection
- Select tier: self-filtering in RunQuery for hybrid fan-out, health-aware ring with failure tracking

### Added

- Column popularity tracking for adaptive prefetch decisions
- Write-through cache on ingest flush for immediate read availability
- QuerySpecificFiles method for gap redistribution across select nodes
- Cache-aware file ordering to maximize cache hits during query execution
- VL/VT parity test suite: 218 tests across 16 test functions validating all LogsQL endpoints, 43 filter types, 38 pipe operations, 23 stats functions, cross-validation invariants, edge cases, and traces Jaeger API against VictoriaLogs/VictoriaTraces as reference implementation

### Fixed

- Traces field prefix: VT metadata fields (kind, flags, dropped_*_count, start_time_unix_nano) emitted without span_attr: prefix to match VT behavior
- Traces index entries: filter VT internal rows (trace_id_idx, service_graph) at insert time via isVTIndexRow()
- Unused wrapVLTimestampOnly removed from traces select handler

## [0.32.0] - 2026-05-23

## [0.31.0] - 2026-05-22

### Fixed

- Map column reading: properly reconstruct key-value pairs from Parquet MAP columns (resource.attributes, span.attributes, log.attributes, scope.attributes) and expand into VL-compatible attribute columns (resource_attr:*, span_attr:*, log_attr:*, scope_attr:*)
- Column projection: only activate for column-selecting pipes (fields, stats, uniq, top) rather than VL-internal pipes (sort, limit, offset) that VL adds automatically to queries

## [0.30.0] - 2026-05-21

### Performance

**VL/VT-inspired query optimizations (8 techniques, zero Parquet format changes):**

- Column-type-aware push-down: numeric columns use native int64 comparisons for row group statistics pruning instead of lexicographic string comparisons
- Constant column optimization: detects columns where min == max across all pages and skips deserialization, injecting the constant value directly
- Label-based file pre-filtering: evaluates query predicates (exact, prefix, GT, LT) against manifest-level labels to skip files before S3 download
- Dictionary page filtering: reads dictionary pages for exact-match/prefix predicates; skips row groups when no dictionary entry matches
- Bitmap-based pre-where filtering: reads filter columns first, builds boolean bitmap, then reads remaining columns only for matching rows
- Parallel row group scheduling: sorts by estimated cost (row count ascending), processes up to 3 in parallel per file
- Pre-resolved column indices: resolves column indices once per file, reuses across all row groups
- Trace parent-child prefetching (traces): smart cache reverse index from trace ID → file keys for instant file narrowing

**Infrastructure optimizations:**

- Parquet footer LRU cache (10K entries) avoids re-parsing file metadata on repeated accesses
- Write lock optimization moves filtering out of serialized mutex, reducing contention
- Parallel row group processing (up to 3 goroutines per file) reduces per-file latency
- Timestamp-only projection for hits/stats/stats_range endpoints via context hint
- Cache warmup on startup pre-fetches recent partitions into L1/L2 and footer cache
- S3 range read capability (DownloadRange with HTTP Range header)
- Tightened all 21 benchmark targets by 25-30% reflecting combined optimization impact

## [0.29.0] - 2026-05-20

### Performance
- Concurrent query benchmark: validates latency targets at 1/10/50/100 parallel queries with mixed endpoint types
- Mixed read/write benchmark: measures mutual interference with ≤20% degradation target
- Config sweep script for automated `max_concurrent` / `file_workers` tuning validation
- Deployment-size recommendations for query concurrency settings

## [0.28.1] - 2026-05-20

### Fixed
- Fix errcheck lint failure on `rows.Close()` in projected reader (logs and traces modules)

## [0.28.0] - 2026-05-20

### Added
- OTEL tracing: HTTP, query, and insert paths instrumented with OpenTelemetry spans
- Benchmark CLI (`cmd/bench`): seed data and measure cold/warm/hot query latency with baseline JSON output

### Performance
- Manifest `GetFilesForRange` uses sorted partition index with binary search (O(log P) vs O(P))
- Column projection pushdown: queries reading 2-3 fields skip deserializing unused columns
- Traces module: added pushdown filter parity with logs module for row group stats pruning
- Expanded bloom index coverage: `host.name`, `k8s.namespace.name`, `k8s.pod.name`, `k8s.deployment.name`, `deployment.environment`, `span.name` (traces)
- Bloom index supports `in()` operator for multi-value exact match queries

## [0.27.2] - 2026-05-20

### Added
- Phase 0 correctness gate: golden file test infrastructure, verification tests for all output surfaces (LogsQL, Jaeger, insert, metrics, stats, manifest, schema), E2E regression suite, Helm chart template tests, architecture and performance documentation

## [0.27.1] - 2026-05-20

### Added
- Per-tenant observability: string-based tenant support for logs and traces, per-tenant stats API, enhanced tenants dashboard with row count accuracy
- Query performance optimization design spec (Phase 0–3 roadmap)

### Fixed
- Normalize millisecond epoch timestamps for VL hits endpoint
- Label filter false negatives on high-cardinality fields with bloom index coverage

### Changed
- Update loki-vl-proxy to v1.36.0

## [0.27.0] - 2026-05-19

### Added
- Settings profiles: 5 named presets (balanced, max-performance, max-durability, max-cost-savings, dev) with three-level hierarchy (global → per-signal → per-role), Helm chart integration via `coalesce` resolution, JSON schema validation, and comprehensive profile integration tests

## [0.26.0] - 2026-05-19

### Added
- Bloom age-tiering: 4-tier model (hot/warm/cold/archive) with configurable boundaries, tier downgrade logic (per-RG → per-file → summary → none), Filter.MergeFrom bitwise OR merge, SHA256 integrity checks
- PartitionedIndex: per-partition bloom management with dirty tracking, hourly/daily granularity, high-cardinality skip gate (>50K)
- BloomCache: LRU-cached bloom index access with lazy loading, size-based eviction, warm preload
- Bloom build on flush: automatic bloom population from trace_id and service.name when parquet files are written to S3
- Bloom persist: dirty partitions written to S3 as `_bloom.bin` after each flush cycle
- BloomFilterFiles: query path integration between label filtering and file workers for partition-level bloom skip
- Manifest PartitionMeta: per-partition bloom availability, size, and column tracking
- MetadataCompactor: automatic bloom tier transitions (hot→warm→cold→archive) with S3 persist callback
- BloomRebuilder: post-compaction bloom rebuild hook on existing Compactor
- TTL recompression: age-based compression levels (ZSTD 3/7/17) for hot/warm/cold data
- BloomController: auto-tuning of bloom parameters based on file rate, SSD usage, and cache metrics with operator pin overrides
- ConfigSync: S3-based live configuration with read/write, error tracking, and last-known fallback
- Bloom status API: GET /api/v1/bloom/status with tier stats, cache stats, auto-tuning state
- Cost projection engine: per-tier storage cost analysis with S3 class mapping (STANDARD/IA/GLACIER)
- 12 bloom-specific Prometheus metrics (build, query, tier transitions, controller adjustments)
- PREWHERE concept tests for column-selective reads and row group stats elimination
- Comprehensive bloom test suite: 132+ unit tests, 30+ integration tests, 4 E2E smoke tests, 10 regression tests

### Changed
- Traces binary insert path rewritten to use VL upstream vlinsert handlers (same adapter pattern as logs)
- Automate `deps-traces` in Makefile — clones VL at commit a408207c2242 and applies patches (was manual)
- Fix `build-traces` Makefile target to build from correct Go module directory
- `make test` uses `-short` to skip real data benchmarks; `make test-full` for full suite with 10m timeout
- Buffer handler hardened: Bearer auth required when configured, GET-only method restriction, stream tag trailing data validation

## [0.24.0] - 2026-05-16

### Added
- Tenant name mapping — bidirectional alias system (X-Scope-OrgID ↔ integer TenantID) with O(1) sync.Map lookups, Loki/Tempo charset validation, HTTP middleware, CRUD API, S3 persistence, fleet sync, and configurable Prometheus metrics format
- WAL implementation — file-based write-ahead log with gob encoding, crash recovery, truncation, and size tracking for insert path durability
- Parquet MAP columns — LogAttributes, ResourceAttributes, SpanAttributes, and ScopeAttributes stored as native Parquet MAP type columns
- LogRow.SeverityNumber field for severity-based log filtering
- TraceRow.StartTimeUnixNano field for trace span start time queries
- Stats API tenant name decoration on cost and compression endpoints

### Changed
- Replace custom insert handler with VL's upstream `vlinsert` handlers via `insertutil.SetLogRowsStorage()` adapter — same pattern as select path's `vlstorage.SetExternalStorage()`
- Full VL insert protocol parity: jsonline, Loki JSON+protobuf, ES bulk, syslog, journald, Datadog, OTLP, Splunk, native insert (previously only jsonline, Loki JSON, ES bulk)
- Extract buffer handler to `internal/buffer` package (from `internal/insertapi`)
- Logs binary no longer uses custom insert parsing — all protocol handling by VL upstream
- Traces binary insert path rewritten to use VL upstream vlinsert handlers (same adapter pattern as logs)
- Automate `deps-traces` in Makefile — clones VL at commit a408207c2242 and applies patches (was manual)
- Fix `build-traces` Makefile target to build from correct Go module directory
- `make test` uses `-short` to skip real data benchmarks; `make test-full` for full suite with 10m timeout
- Buffer handler hardened: Bearer auth required when configured, GET-only method restriction, stream tag trailing data validation
- Apply `gofmt -s` simplifications across all Go files in both modules
- Enable gofmt, gocyclo, and misspell linters in golangci-lint v2 configs
- Add standalone `gofmt -s` check and Go Report Card badge to CI
- Treat govulncheck and helm lint warnings as CI failures
- Extract startStatsLoops from run() to reduce cyclomatic complexity

## [0.23.1] - 2026-05-14

### Fixed
- E2E datagen trace-log correlation — traces now generated first with 70% of logs sharing trace IDs, span IDs, and service context for realistic cross-signal testing
- Grafana datasource log→trace links — added `derivedFields` to all VictoriaLogs and Loki datasources with `trace_id=(\w+)` regex linking to Jaeger trace views
- ClickHouse otel_logs view promoted fields — moved service.name, k8s.*, etc. into LogAttributes map only (removed ResourceAttributes duplication)
- Bump loki-vl-proxy to v1.33.0

## [0.23.0] - 2026-05-14

### Added
- AZ auto-detection at startup with fallback chain (env var → AWS IMDSv2 → GCP metadata → K8s node label API)
- AZ-aware peer cache routing — consistent hash ring maintains same-AZ sub-ring, prefers same-AZ peers for L3 cache lookups
- AZ-aware buffer bridge — select pods prefer same-AZ insert pods for `/internal/buffer/query` fan-out
- Preferred vs strict AZ modes with configurable `az_min_peers_per_az` threshold
- AZ metrics: `lakehouse_peer_same_az_members`, `lakehouse_peer_cross_az_members`, `lakehouse_peer_az_requests_total`, `lakehouse_buffer_bridge_az_requests_total`
- Peer AZ reporting via `/internal/cache/stats` endpoint (`"az"` field in JSON response)
- Default topology spread constraints in Helm chart for even AZ distribution
- NODE_NAME env injection in Helm templates for K8s API AZ detection fallback
- 8 fuzz test targets across azdetect, peercache, config, and storage packages
- 60+ edge case tests for AZ components (K8s labels, ring wrap-around, concurrent access, special chars)
- Cross-AZ cost optimization guide with AutoMQ comparison and industry case studies
- Mermaid diagrams added to 14 documentation pages
- 4 new website landing pages: ingestion-formats, query-interfaces, loki-tempo-alternative, multi-tenant-observability
- SEO: OpenGraph meta tags, JSON-LD structured data, 25+ keywords, Twitter card metadata
- Docusaurus sidebar: added 9 docs, navbar dropdown menus for Use Cases and Integrations

### Fixed
- Peer AZ discovery auth header mismatch — `fetchPeerAZ()` used `Authorization: Bearer` but handler expects `X-Peer-Auth-Key`, causing silent failure when auth configured
- Data race in `/internal/cache/stats` endpoint — `ServeHTTP` read `selfAZ` without lock while `SetSelfAZ` writes with lock
- Invalid JSON in stats endpoint for non-UTF8 AZ names — `%q` format produces Go-style escapes not valid in JSON, switched to `json.Marshal`
- `mergeConfig()` missed boolean fields `Peer.AZAware`, `Peer.CrossAZFallback`, `Select.AZAware`, `Select.CrossAZFallback` — config overlay could not enable these

## [0.22.0] - 2026-05-13

### Added
- Tenant stats & storage metrics — `StatsConfig` (15 fields) and `UIConfig` (4 fields) config structs, `KnownTenant` for bucket-isolation cold discovery with per-tenant lifecycle/pricing overrides
- Per-tenant Prometheus metrics — 8 metrics (`lakehouse_tenant_files`, `_bytes`, `_raw_bytes`, `_rows_total`, `_ingestion_bytes_total`, `_queries_total`, `_last_write_timestamp`, `_last_query_timestamp`) with configurable cardinality cap
- Global storage metrics — 14 metrics (`lakehouse_storage_files_total`, `_bytes_total`, `_compression_ratio`, `_cost_monthly_usd`, `_bytes_by_class`, etc.) for fleet-wide storage visibility
- Cardinality limiter meta-metrics — `lakehouse_metrics_cardinality_limit`, `_tracked`, `_overflow_total`
- Stats sync metrics — 7 metrics for peer delta broadcast, S3 snapshots, CRDT merges, HeadObject verification
- `GaugeVec` and `FloatGaugeVec` metric helper types for per-label gauge tracking
- Helm chart updates — `lakehouseConfig.stats.*` (15 fields), `lakehouseConfig.ui.*` (4 fields), complete `lakehouseConfig.tenant.*` (isolation, bucket_template, known_tenants with lifecycle/pricing overrides)
- Tenant stats documentation (`docs/tenant-stats.md`) — 7 JSON API endpoints, CRDT fleet sync, storage class tracking, cost estimation, all metrics reference
- Lakehouse Explorer UI documentation (`docs/lakehouse-explorer.md`) — 3-tab Preact+uPlot dashboard (Storage Overview, Tenants, Cardinality), VMUI tab injection
- Updated observability docs with tenant, storage, cardinality, and stats sync metric tables
- Updated multi-tenancy docs with tenant stats, monitoring, cost allocation sections
- Updated configuration docs with stats, UI, and tenant config examples
- Updated README — tenant stats in Key Features, Observability section, Configuration section, Documentation navigation

### Fixed
- Auto-release workflow `[skip release]` check now examines only commit title instead of entire multiline message — squash-merged PRs with `[skip release]` in body paragraphs no longer incorrectly skip releases
- Lint/gosec/CodeQL warnings — unhandled `w.Write()` errors in VMUI inject, unchecked `json.Unmarshal` in stats regression tests, unused Preact `h` import, unused `getCPUTime` function, redundant nil check
- VMUI regression test skips missing build assets (favicon.svg, config.json) in CI instead of failing
- Bloom columns test expectations updated to match actual defaults (`[service.name, trace_id]` for logs)

## [0.21.0] - 2026-05-13

### Added
- Schema-driven FieldType system — centralized type-aware formatting for all Parquet column types (TypeTimestampNano, TypeInt32, TypeInt64, TypeFloat64, TypeBool, TypeString). `FormatValue()` on each type replaces scattered `fmt.Sprintf`/`time.Format` calls across all query paths (RunQuery, GetFieldNames, GetFieldValues, buffer reads). `ParseFieldType()` enables typed ExtraPromoted columns via config.
- `FormatField(internalName, value)` registry method for one-call schema-driven formatting in all read paths
- Architecture documentation with mermaid diagrams — cache architecture (L1→L2→L3→S3 tiers, SmartCache controller, eviction, prefetch, cross-signal, sizing), manifest system (structure, sync, persistence, API), storage & Parquet flow (end-to-end write/read paths, VL adapter, schema registry)
- CodeQL configuration to exclude vendored VictoriaLogs code from security scanning

### Fixed
- Jaeger test `TestHandleJaegerTrace_ScopeAttrAsSpanTag` assertion — handler strips `scope_attr:` prefix from tag keys, test now expects `lib.version` instead of `scope_attr:lib.version`
- ClickHouse OTEL views — `ScopeAttributes` was `Map(Nothing, Nothing)`, now `Map(String, String)` via typed CAST; Events/Links arrays were `Array(Nothing)`, now properly typed (`Array(DateTime64(9))`, `Array(String)`, `Array(Map(String, String))`); removed non-standard `LogStreamId` from `otel_logs`; added `TraceFlags`, `ResourceSchemaUrl`, `ScopeSchemaUrl` columns; traces Duration now in nanoseconds (OTEL standard) instead of milliseconds; empty promoted fields filtered via `mapFilter` to avoid clutter in ResourceAttributes/SpanAttributes
- Datagen now populates `ResourceAttributes` MAP column for logs (with `service.version`, `telemetry.sdk.name`) and `ResourceAttributes`, `SpanAttributes`, `ScopeAttributes` MAP columns for traces
- Grafana ClickHouse datasource config — added `logsLevelField: SeverityText`, `tracesDurationUnit: ns`, `tracesSpanKindField`, `tracesTraceStateField` for proper OTEL auto-discovery

### Changed
- Datagen seed volume increased — 10K logs + 2K traces over 72h (was 5K + 1K over 48h) to better populate both hot (disk 24h) and cold (S3 lakehouse) tiers
- Tenant1 seed increased — 2K logs + 500 traces over 72h (was 1K + 200 over 48h)

## [0.20.0] - 2026-05-12

### Added
- Multi-tenancy — single binary serves all tenants via header-based routing with S3 prefix isolation (`{AccountID}/{ProjectID}/`, default `0/0/`), matching Grafana Loki/Tempo pattern. Enterprise option for bucket-per-tenant isolation with separate IAM policies
- Global read mode — optional `--lakehouse.tenant.global-read-header` / `--lakehouse.tenant.global-read-value` for admin dashboards that query across all tenants (disabled by default, explicit opt-in)
- Analytics engines documentation — comprehensive guide covering 9 Parquet engines (DuckDB, ClickHouse, Trino, Databricks, Snowflake, StarRocks, Doris, Spark, pandas) with Grafana datasource status, query examples, and integration guides
- Tenant configuration flags — `--lakehouse.tenant.isolation` (prefix/bucket), `--lakehouse.tenant.bucket-template`, `--lakehouse.tenant.default-account`, `--lakehouse.tenant.default-project`, `--lakehouse.tenant.header-account`, `--lakehouse.tenant.header-project`, `--lakehouse.tenant.global-read-header`, `--lakehouse.tenant.global-read-value`
- Multi-level select architecture — vlselect/vtselect fan out queries to both hot (disk) and cold (lakehouse S3) storage nodes for unified hot+cold results
- VictoriaTraces hot tier in Docker Compose — standalone VT instance with 24h disk retention
- Datagen trace dual-write — `--vt-endpoint` flag pushes traces to VictoriaTraces via Zipkin `/api/v2/spans` alongside S3 Parquet writes
- Eleven Grafana datasources — Global VL/VT (via vlselect/vtselect), Hot VL/VT (direct disk), Cold logs/traces (lakehouse S3), Loki proxy (hot+cold), DuckDB analytics, ClickHouse analytics/logs/traces
- DuckDB Grafana datasource — in-memory DuckDB with `httpfs` extension for direct SQL on S3 Parquet files via `read_parquet()`
- ClickHouse analytics engine — pre-configured with `lakehouse.logs` and `lakehouse.traces` views querying MinIO Parquet via `s3()` table function, with dedicated Grafana Logs and Traces datasources for native log/trace panel visualization on raw Parquet
- ClickHouse OTEL-compatible views — `lakehouse.otel_logs` and `lakehouse.otel_traces` map Parquet columns to OpenTelemetry standard naming (Timestamp, Body, SeverityText, ServiceName, TraceId, SpanName, SpanKind, Duration, StatusCode, ResourceAttributes, SpanAttributes)
- Tenant-scoped ClickHouse views — `logs_tenant_default`, `traces_tenant_default`, `logs_tenant_test`, `traces_tenant_test` with direct s3() glob patterns per tenant (workaround: `_file` virtual column unavailable through view chain)
- Raw ClickHouse views — `lakehouse.logs_raw` and `lakehouse.traces_raw` with explicit Parquet schema for ad-hoc SQL analytics without needing files at view creation time
- Grafana ClickHouse datasources preconfigured with OTEL mode (`otelEnabled: true`, `otelVersion: latest`), default tables (`otel_logs`, `otel_traces`, `logs_raw`), and bidirectional logs↔traces cross-linking via `tracesToLogsV2`
- Expanded datagen `_stream` labels from 2 to 5 — added `k8s.deployment.name`, `deployment.environment`, `cloud.region` for full Loki label filtering support
- Multi-tenancy E2E tests and CI workflow
- Gitleaks allowlist (`.gitleaks.toml`) for false positives in documentation and test example values
- Loki-VL-proxy Dockerfile — builds from GitHub release binary instead of non-existent GHCR image
- Architecture diagram in Docker Compose docs showing full data flow across all tiers

### Fixed
- Tenant-aware S3 prefix resolution — `TenantConfig.ResolvedPrefix()` and updated `AutoPrefix()` prepend `{AccountID}/{ProjectID}/` to signal prefix (e.g. `0/0/logs/` instead of `logs/`)
- E2E test params — add missing `step` for /hits, `query` for /field_names and /field_values, stats pipe syntax for /stats_query
- Datagen Dockerfile — add `GOWORK=off` to prevent go.work from pulling in lakehouse-traces module dependencies
- Auto-release workflow — remove auto-merge (repo setting not enabled), just create PR for manual merge
- Remove broken DuckDB plugin init container (v0.4.1 release has no downloadable assets)

### Changed
- Docker Compose hot tier retention reduced from 7d to 24h to match cold boundary
- Grafana default datasource changed to VictoriaLogs Global (via vlselect) for unified hot+cold queries
- Grafana image changed from Alpine to Ubuntu (`grafana/grafana:latest-ubuntu`) — required for DuckDB plugin (glibc dependency)
- Grafana ClickHouse datasource names explicitly show S3 Parquet origin
- `_stream_fields` in VL NDJSON push updated to match expanded 5-label _stream

## [0.18.2] - 2026-05-12

### Fixed
- Fix Jaeger trace search returning null data — use VT-canonical field names (`"resource_attr:service.name"`, `name`, `duration`) with LogsQL quoting for colon-containing fields
- Fix loki-vl-proxy hot+cold routing — VictoriaLogs serves hot data (<24h), lakehouse-logs serves cold data via `-cold-enabled` with 1h overlap
- Add `external_query.go` patch to auto-release workflow — fixes binary build failure (`undefined: logstorage.QueryHasPipes`)
- Update e2e compose loki-vl-proxy from broken local build path to published GHCR image v1.31.2
- Format `_time` column as RFC3339Nano instead of raw nanoseconds — fixes VL handler timestamp parsing for all query endpoints
- Recover from `writeBlock` panics caused by unsupported VL pipe processors (e.g. `CountByTimePipe` in `/hits`) — prevents query crashes, returns partial results instead
- Add `filter.go` to traces module for metadata filter scoping — traces `GetFieldNames`/`GetFieldValues` now correctly apply LogsQL filters
- Apply LogsQL filter scope to metadata endpoints (`GetFieldNames`, `GetFieldValues`, `GetStreamFieldNames`, `GetStreamFieldValues`) — previously returned unfiltered results

### Changed
- Replace custom LogsQL filter parser with VL's native `Filter.MatchRow()` — full LogsQL parity including OR, AND, NOT, regex, ranges, case-insensitive matching, and all filter types VL supports
- Apply LogsQL filter evaluation in traces `RunQuery` (was missing) — traces now filter rows same as logs module
- Apply `filter` substring parameter in vlstorage adapter for `GetFieldNames`, `GetFieldValues`, `GetStreamFieldNames`, `GetStreamFieldValues` — was previously ignored, now matches VL behavior
- Improve loki-vl-proxy config for Grafana Loki Drilldown — switch to translated metadata mode, add structured metadata emission, expand stream fields (12 labels), add derived fields for trace-to-logs linking, enable patterns autodetect and label values indexed cache
- Split LOC badge into separate prod code and test code badges
- Add `GOWORK=off` to Makefile — prevents build failures from incompatible VL versions across modules

## [0.18.1] - 2026-05-11

### Added
- **Smart cache controller** — unified cache orchestrator wrapping L1 (memory), L2 (disk), L3 (peer), L4 (S3) with configurable TTL, hot access detection, pin tracking, and singleflight S3 deduplication (`internal/smartcache/`)
- **Cross-signal prefetch** — bidirectional hints between `lakehouse-logs` and `lakehouse-traces` deployments via HTTP (`/internal/prefetch/hint`, `/internal/cache/evict-hint`). Logs query for `service=checkout` automatically warms trace data for same time window, and vice versa (`internal/crosssignal/`)
- **LogsQL filter evaluation** — post-scan field matchers (exact, substring, regex, NOT) applied to DataBlock rows in RunQuery, ensuring cold queries respect LogsQL semantics (`internal/storage/parquets3/filter.go`)
- **max_rows enforcement** — `query.max_rows` (default 10M) caps emitted rows per query via atomic counter, preventing unbounded cold-query resource usage
- **Internal endpoint auth** — `/internal/cache/clear` and `/internal/cache/stats` require Bearer token (`peer.auth_key`) when configured, matching `/internal/manifest/update` pattern
- **Prefetch engine wiring** — cross-signal handler now creates and uses a `prefetch.Engine` to process incoming prefetch hints (was nil/inert)
- **Parallel query file workers** — configurable bounded worker pool for concurrent Parquet file processing during queries, replacing sequential file scanning (`query.file_workers`, default 8)
- **Cache sizing calculator** — adaptive cache budget estimation blending ingestion rate (early) and query pattern analysis (after 12h), with per-node fleet division (`internal/smartcache/sizing.go`)
- **Active query pinning** — files used by in-flight queries are pinned in cache with configurable grace period, preventing eviction under pressure
- **Connected data eviction** — trace IDs extracted from query results enable cross-signal cache deprioritization when traces are evicted
- **Hint batching** — cross-signal client accumulates trace ID hints and flushes on interval or batch size threshold, reducing HTTP overhead
- **Smart cache metrics** — 15 new Prometheus metrics: hit ratio, entries, bytes used/limit, evictions by reason, hot/pinned/owned entries, effective bytes, prefetch hit ratio, coverage hours
- **Cross-signal metrics** — 6 new metrics: eviction sent/received/pending/applied, prefetch sent/received
- Smart cache snapshot persistence — periodic metadata snapshots to disk for fast cache warmup on restart
- Smart cache eviction loop — background TTL enforcement with hot access detection and pin protection

### Changed
- `getFileData()` in storage now routes through SmartCacheController when available, with fallback to original L1→L2→L3→S3 chain
- `RunQuery` wraps `writeBlock` callback with filter evaluation, tombstone filtering, and max_rows enforcement before passing to caller
- `RunQuery` uses parallel file worker pool instead of sequential processing
- `queryFile` extracts trace IDs from result DataBlocks for prefetch and cross-signal hints
- Both `lakehouse-logs` and `lakehouse-traces` binaries wire up cross-signal handlers with active prefetch engine, eviction loop, and snapshot persistence
- Auto-release workflow now auto-merges metadata PRs to prevent version drift

## [0.17.0] - 2026-05-11

### Added
- Query rate limiting via `MaxConcurrent` semaphore — returns HTTP 429 when at capacity
- S3 retry with exponential backoff for all S3 operations (`ReadAt`, `Upload`, `Download`, `Delete`, `Exists`)
- Context propagation in S3 reader (replaces `context.TODO()`)
- Per-operation S3 metrics (requests, duration, errors, bytes read)
- Slow query logging with configurable threshold and query duration histograms
- VL/VT integration stubs: `GetStreamIDs`, `GetTenantIDs`, delete dispatch (`DeleteRunTask`/`DeleteStopTask`/`DeleteActiveTasks`)
- Tests: s3reader (Upload/Download/Delete/Exists), election (S3/K8s/auto), Jaeger handlers, selectapi, vlstorage adapters, S3 retry (+112 tests)
- Helm: `NOTES.txt` post-install guidance, `NetworkPolicy` template, `values.schema.json` validation
- CI: golangci-lint v2 config, Dependabot for Go/Actions/Docker, hardened security workflow
- Project logo

### Changed
- Replace custom `internalselect` handler (~960 lines) with VL's built-in `RequestHandler` for both modules
- Split `parquets3/storage.go` (1,383 lines) into `storage_query.go` and `storage_fields.go`
- Extract Jaeger handlers (~560 lines) from `handler.go` into dedicated `jaeger.go`

### Removed
- Dead code: empty `UpdatePerQueryStatsMetrics()`, unused `CircuitBreakerConfig`, `S3CircuitBreakerState` metric

### Fixed
- Replace custom internalselect encoding with VL's actual wire format — fixes vlselect panics (`growslice: len out of range`) caused by 4-byte uint32 block lengths instead of 8-byte uint64
- Add `internal/vlstorage/` thin dispatch layer bridging `storage.Storage` to VL's vlstorage function signatures (both logs and traces)
- Remove protocol-incompatible vlselect service from E2E compose
- Remove orphaned vlselect Grafana datasource pointing to removed service
- Fix traces-to-logs datasource uid reference (`victoria-lakehouse-logs` → `victoria-lakehouse-cold`)
- Delete dead `internal/protocol/` package in both logs and traces modules (replaced by VL encoding in #28)

### Architecture
- Split into two separate binaries: `lakehouse-logs` and `lakehouse-traces`
- Each binary has its own Go module with independent VL dependency versions
- Logs pins to VL v1.50.0, Traces pins to VL commit a408207c2242 (VT v0.8.2 compatible)
- Removed unified `cmd/lakehouse/` binary and `--lakehouse.mode` flag — mode is hardcoded per binary

### Logs (`lakehouse-logs`)
- Separate Dockerfile (`Dockerfile.logs`), Docker image (`ghcr.io/.../lakehouse-logs`)
- Default port `:9428`, bloom columns: `[service.name]`
- Delete API at `/delete/logsql/*`
- Mode-specific config section: `logs:` in YAML, `--lakehouse.logs.*` flags

### Traces (`lakehouse-traces`)
- Separate Go module (`lakehouse-traces/go.mod`) with VT-compatible VL dependency
- Separate Dockerfile (`Dockerfile.traces`), Docker image (`ghcr.io/.../lakehouse-traces`)
- Default port `:10428`, bloom columns: `[trace_id, service.name]`
- Delete API at `/delete/tracessql/*`
- Jaeger gRPC support: `--lakehouse.traces.jaeger-enabled`, `--lakehouse.traces.jaeger-grpc-addr`
- Mode-specific config section: `traces:` in YAML, `--lakehouse.traces.*` flags

### Shared
- Mode-specific config extension points (`logs:` / `traces:` sections) with accessor methods (`ActiveBloomColumns()`, `ActiveDeletePrefix()`, `ActiveCompatVersion()`)
- Discovery `defaultPort` parameter for mode-aware SRV resolution (9428 for logs, 10428 for traces)
- Helm chart: mode-aware image selection (`image.logs.repository` / `image.traces.repository`)
- CI: Fully parallel jobs for logs and traces (test, lint, build, docker, security, benchmarks)

## [0.14.0] - 2026-05-05

### Added
- `/lakehouse/info` endpoint now includes `build_time` field for operational visibility
- Traces delete support: mode-aware rewriter uses `schema.TraceRow` for traces mode, `schema.LogRow` for logs mode
- Delete handler registers at `/delete/tracessql/*` in traces mode, `/delete/logsql/*` in logs mode
- Docs: 5 new pages for Docusaurus site — read-path, kubernetes-deployment, docker-compose-setup, benchmarks, open-parquet-format
- Docs: Docusaurus YAML frontmatter on all 20 documentation pages
- CI: Changelog enforcement workflow — PRs with releasable changes require `[Unreleased]` entry

### Fixed
- Docs: Corrected false VL/VT compatibility claims — replaced "imports as Go module dependencies" with accurate "reimplements the VL/VT storage interface" (codebase is 100% clean-room, zero VL/VT Go imports)
- Docs: Removed non-existent `/insert/opentelemetry/v1/logs` endpoint from write-path documentation
- Docs: M7 Observability milestone updated from "Planned" to "Complete"
- Docs: Config count corrected from "65+ flags" to "110+ config options" (verified from code)

### Changed
- Docs: All cost tables corrected for 3 AZ replication (VL/VT runs 3 identical clusters, one per AZ)
- Docs: At 500GB/day 1yr 3 AZ — VL/VT $2,679/mo, Lakehouse $2,814/mo (within 5%), Loki $3,610/mo
- Docs: Compute scaled to 6× per component (3 AZ), storage × 3 for EBS, break-even and cumulative projections updated

## [0.12.0] - 2026-05-05

### Added
- Cost-aware deletion: VL-compatible `/delete/logsql/*` APIs with tombstone-based soft delete
- Three delete modes: `hide` (tombstone only), `permanent` (physical removal), `auto` (smart default)
- Tombstone query-time filtering across all query paths (zero-cost data suppression)
- Background rewriter for S3 Standard files with storage-class gating (never touches Glacier/IA)
- S3 storage class detection with lifecycle rule prediction (zero-cost age-based)
- Cost estimation endpoint (`/delete/logsql/estimate`) with per-class breakdown
- Delete verification endpoint (`/delete/logsql/verify`) for compliance auditing
- Un-delete support (remove tombstone to restore data visibility)
- Tombstone persistence to disk + S3 (survives full cluster recreation)

## [0.11.0] - 2026-05-05

### Added
- E2E: VictoriaLogs hot tier, multi-level vlselect, loki-vl-proxy in Docker Compose
- E2E: Internal Docker networking (only Grafana on port 3003)
- E2E: Loki proxy integration tests, vlselect multi-level tests, performance assertion tests
- Datagen: 5 realistic log patterns (JSON, logfmt, nginx, Java stacktrace, OTEL)
- Datagen: Dual-write to VL and S3 for hot/cold verification
- Loadtest: Benchmark mode for file size × row group × compression matrix
- Helm: Single YAML config blob in ConfigMap (no individual flag mapping)
- Helm: Common section deep-merged into components
- Helm: Separate toggleable headless services for discovery
- Helm: VPA support, extraManifests, vmauth Secret routing
- CI: Upstream sync tracks GitHub releases (not Go module versions)
- CI: Nightly benchmark workflow with artifact upload
- Docs: Performance documentation with benchmark methodology and cost projections

### Changed
- Helm: vmauth config stored as Secret instead of ConfigMap
- Helm: All components use generic HPA/VPA/PDB/ServiceMonitor/Ingress templates
- Grafana: 5 datasources (cold, hot, multi-level, Loki proxy, Jaeger)

### Removed
- Docker Compose: Host port mappings for non-Grafana services
- Helm: compaction-rbac.yaml (config in lakehouseConfig blob)

## [0.10.0] - 2026-05-04

### Added
- **Level-based Parquet compaction** — L0→L1→L2 with configurable thresholds, partition-level S3 sentinels, and structured logging (`internal/compaction/`)
- **Leader election** — K8s Lease (primary) with S3 lock + HTTP liveness detection (fallback), `auto`/`k8s`/`s3`/`none` modes (`internal/election/`)
- **Peer manifest push notifications** — fire-and-forget HTTP POST to all peers on flush/compaction, with S3 ListObjects poll as fallback (`internal/manifest/push.go`)
- **Manifest update receiver** — `POST /internal/manifest/update` handler for cross-instance manifest sync
- **Load testing binary** — `cmd/loadtest/` with latency benchmarks (6 tests against plan targets) and throughput stress tests (insert rate, query QPS, mixed workload)
- **Compaction metrics** — 11 new Prometheus metrics: runs, files, bytes, rows, duration, errors, skip reasons
- **Election metrics** — leader gauge, transition counter, health check outcomes
- **Manifest push metrics** — push total, errors, peer count, received updates
- **Helm RBAC** — K8s Role/RoleBinding for Lease-based leader election when `compaction.enabled=true`
- **Nightly CI load test** — GitHub Actions workflow running full benchmark suite on schedule

## [0.9.0] - 2026-05-04

### Added
- **Prometheus metrics instrumentation** — ~80 metrics under `lakehouse_*` prefix: HTTP RED, S3 operations, cache tiers, peer cache, manifest/discovery, Parquet engine, insert/writer, prefetch, startup/health, query
- **Grafana dashboards** — `victoria-lakehouse.json` (single-instance, 7 rows) and `victoria-lakehouse-cluster.json` (fleet, adds peer cache + per-instance)
- **Alerting rules** — 10 Prometheus alerting rules for critical operational conditions
- **Startup warmup sequence** — phased startup with readiness probe gating (init → disk recovery → S3 refresh → ready)
- **Circuit breaker** for S3 operations with configurable thresholds and recovery

## [0.8.0] - 2026-05-04

### Added
- **Write-ahead log (WAL)** — append-only crash recovery with gob-encoded log/trace entries, automatic replay on startup, atomic truncate after flush (`internal/wal/`)
- **VL-compatible insert APIs** — `/insert/jsonline`, `/insert/loki/api/v1/push`, `/insert/elasticsearch/_bulk` with full field mapping to Parquet schema (`internal/insertapi/`)
- **Adaptive file sizing** — per-partition byte estimates trigger flush when approaching `--lakehouse.insert.target-file-size` for optimal Parquet output
- **Buffer query bridge** — select pods fan out to insert pods via `/internal/buffer/query` for zero-delay reads of unflushed data (`internal/storage/parquets3/buffer_bridge.go`)
- **Manifest label pruning** — `FileInfo.Labels` field with `MatchesLabel()` for query-time file skipping without opening Parquet files
- **Manifest management** — `AllFiles()` snapshot and `RemoveFile()` for partition lifecycle
- **Label extraction** — automatic extraction of label values from log rows (10 fields) and trace rows (2 fields) during flush
- **WAL integration in BatchWriter** — entries written to WAL before buffering, WAL truncated on successful flush, replay on startup
- **Insert + select role separation** — `--lakehouse.role=all|insert|select` for independent scaling
- **Config extensions** — `TargetFileSize`, `WALMaxBytes`, `WALDir`, `WALEnabled`, `SelectConfig` with `BufferQueryEnabled`, `InsertHeadlessService`, `BufferQueryTimeout`

## [0.7.0] - 2026-05-03

### Added
- **Manifest partitions API** — `GET /manifest/partitions` with date-range filtering for per-date file/byte summaries
- **GetPartitions()** manifest method for partition inventory
- **PartitionsHandler** and **PartitionsResponse** types for HTTP layer

## [0.6.0] - 2026-05-03

### Added
- Filter AST engine with full LogsQL predicate support: exact match (`field:="value"`), substring (`field:value`), regex (`field:~"pattern"`), AND, OR, NOT, parenthesised grouping
- Playwright-based E2E UI tests validating Grafana Explore queries against live Lakehouse backend
- E2E integration tests for logs queries, Jaeger trace search, field enumeration, and stats aggregation
- Schema validation tests ensuring Parquet column mapping correctness

### Fixed
- Schema field mapping corrections for OTEL-standard column names

## [0.5.0] - 2026-05-03

### Added
- VL/VT internal select protocol (`/internal/select/*`) — 11 endpoints for cluster storage-node registration
- Binary DataBlock streaming with ZSTD compression for efficient cluster communication
- Prefetch engine with token-based row group read-ahead optimisation
- Register as `-storageNode` on vlselect/vtselect for transparent hot+cold fan-out

## [0.4.0] - 2026-05-02

### Added
- Distributed peer cache via consistent hash ring with headless DNS service discovery
- Peer HTTP protocol (`/internal/cache/fetch`, `/internal/cache/has`) with shared-secret auth
- Hot boundary auto-discovery from vlstorage/vtstorage `/internal/partition/list` endpoint
- Topology auto-detection: storage-node, direct, loki-proxy modes
- Static and headless service discovery for storage nodes and peers

## [0.3.0] - 2026-05-02

### Added
- L1 in-memory LRU cache for Parquet footers, bloom filters, and hot row groups
- L2 local disk cache with LRU eviction at configurable watermark
- Cache coalescence via `singleflight.Group` to deduplicate concurrent S3 fetches
- Label/attribute index with background scanning and disk persistence for sub-ms `field_names`/`field_values`
- Metadata persistence and recovery on restart (manifest, label index, footers)

## [0.2.0] - 2026-05-02

### Added
- Bloom filter checking for fast point lookups on `trace_id` and `service_name` columns
- Column projection — read only columns referenced by query, reducing I/O by 60-80%
- `GetStreamFieldNames`, `GetStreamFieldValues`, `GetStreams`, `GetStreamIDs` storage methods
- `GetFieldNames`, `GetFieldValues` from Parquet metadata with label index fallback
- No-op `Delete*` and `GetTenantIDs` methods for read-only cold storage

## [0.1.0] - 2026-05-02

### Added
- Initial project structure with Go module, CI/CD, Dockerfile, Helm chart skeleton
- Config namespace (`--lakehouse.*`) with YAML + flag parsing and production-ready defaults
- Mode selection: `--lakehouse.mode=logs` (port 9428) or `--lakehouse.mode=traces` (port 10428)
- S3 `io.ReaderAt` adapter for parquet-go with connection pooling and range reads
- ParquetS3Storage query engine: Hive partition pruning, row group statistics skipping, DataBlock emission
- SchemaRegistry mapping OTEL Parquet columns to VL/VT internal names (logs + traces profiles)
- Partition manifest with S3 ListObjects refresh and sub-ms "nothing here" fast path
- HTTP endpoints: `/health`, `/ready`, `/manifest/range`, `/manifest/partitions`, `/lakehouse/info`
- Public LogsQL API: all `/select/logsql/*` query endpoints (query, stats, hits, field/stream discovery)
- Jaeger API: `/select/jaeger/api/*` endpoints (traces, services, operations, dependencies)
- Phased startup warmup: init → disk recovery → S3 refresh → ready
- Distroless container image with multi-stage build
- GitHub Actions CI/CD: test, lint (golangci, gosec, gitleaks), build, security scanning, auto-release
- PR labeler, dependabot, CODEOWNERS configuration
- Documentation: architecture, configuration, cost estimates, getting started, observability, operations, performance, scaling, security
