# Parquet compression + efficiency roadmap

> **STATUS UPDATE (2026-06-10) — superseded by [`parquet-compression-research.md`](parquet-compression-research.md)**
> (source-audited + empirically verified with pyarrow/duckdb readback). Per-item verdicts:
> 1 sorting **APPROVED** (gated on 3 correctness trap-fixes) · 2 delta-timestamps **IMPLEMENTED**
> (struct tags; the `parquet.Encoding()` API named below does not exist) · 3 L2+ row groups
> **APPROVED** · 4 PageIndex **ALREADY SHIPPED** (parquet-go always writes it — read-side
> exploitation moved to the S3 track) · 5 zstd dict training **REJECTED** (empirically breaks
> external readers; replaced by `dict`-tag RLE_DICTIONARY — **IMPLEMENTED**) · 6 bloom-drop
> **NO-OP** (only service.name/trace_id blooms exist) · 7 chunk merging **N/A** (misconception)
> · 8 schema slimming **PARKED** (product decision) · 9 ZSTD-19 **IMPOSSIBLE via klauspost**
> (4 tiers, ceiling ≈11; long-range moot at page granularity) · 10 BROTLI **SKIPPED per review**
> (zstd judged sufficient; −15% measured, revisit if archive cost matters) / LZMA **REJECTED**
> (not a parquet codec). NEW approved item: **dedicated columns** (hot-attribute promotion).


Standards-compliant optimizations that increase compression ratios
and read efficiency over the current setup. Every entry here stays
within the published Parquet 2.x spec — no custom framing, no
private codecs, every file produced by these optimizations is
readable by duckdb, pyarrow, parquet-tools, and Snowflake out of
the box.

Companion docs:
- `docs/architecture/metadata-and-s3-optimization.md` — current optimizations
- `docs/architecture/restart-and-warmup-design.md` — lifecycle spec

## Already shipped (PR #122)

| Optimization | Effect |
| --- | --- |
| Progressive zstd schedule `[3, 7, 11]` by compaction level | ~10-25% smaller cold files vs uniform level 3 |
| Per-tenant compression override | Per-tenant CPU/storage tradeoff |
| 64 KiB footer prefetch + 2-phase fallback | 0 wasted RTTs on large footers |
| Bloom filters on `service.name`, `trace_id` | ~80-90% file skip on tenant-scoped queries |
| Trace-id footer KV index | Sub-second trace-by-ID lookups |
| Compaction-time row union (deduplication) | No row drift across L0→L1 |

## Highest ROI follow-ups

Ordered by `(expected gain) ÷ (implementation cost)`. None of these
break the Parquet standard.

### 1. Sort rows by `(stream_id, timestamp)` before write — **est. 30-50% smaller**

Currently the writer flushes rows in arrival order; the compactor
sorts by timestamp only. Parquet's `RLE_DICTIONARY` encoding gets
huge wins when adjacent rows share dictionary indices — sorting by
the stream identity first means all rows from the same k8s pod,
service, level, and namespace cluster together in one column
chunk.

Implementation: change the sort in `internal/compaction/compactor.go::
mergeLogFiles` from `func(i, j) ... return rows[i].TimestampUnixNano
< rows[j].TimestampUnixNano` to a 2-key compare with `stream_id`
first. Same for `mergeTraceFiles`. Insert-path sort applies on
flush; small change in `BatchWriter.flush()`.

Risk: changes the time-ordered scan pattern that some queries
exploit. Mitigation: keep timestamp as the secondary sort key so
adjacent rows in the same stream stay time-ordered.

Test: parquet-tools meta on a "before" and "after" file showing
column chunk size delta.

### 2. Delta-encode timestamps (`DELTA_BINARY_PACKED`) — **est. 60-80% smaller timestamp column**

The `_time` column is currently zstd-compressed as int64. Switching
to Parquet's `DELTA_BINARY_PACKED` encoding stores the delta
between consecutive timestamps, which is typically a few bits when
data is roughly time-ordered. parquet-go supports it via column
config.

Implementation: in `writer.go` `writeLogsParquet` / `writeTracesParquet`,
add per-column encoding via `parquet.SchemaOption(parquet.Encoding(
"_time", DELTA_BINARY_PACKED))`. Same for `severity_number`,
`start_time_unix_nano`, `end_time_unix_nano`, `duration_ns` in traces.

Risk: none — DELTA_BINARY_PACKED is a Parquet 2.0 encoding
supported by every conforming reader.

### 3. Larger row groups for L2+ files — **est. 10-15% smaller cold files**

Default `RowGroupSize` is 10k. At L2+ compaction the data is cold
and read-once; row groups can be much bigger (50k-100k) so the
zstd dictionary scope is wider. Bigger dictionary = better
compression for repetitive columns.

Implementation: extend `CompactionConfig` with
`RowGroupSizeByOutputLevel []int`, default `[10000, 50000, 100000]`.
Compactor reads it at flush time. Mirror the pattern of
`CompressionLevelByOutputLevel`.

Risk: bigger row groups = more memory at read time. The smart
cache size cap already enforces this; we just have to budget L2
reads as a larger chunk. PB-scale ops will set 100k+ regardless.

### 4. PageIndex emission — **est. 2-5× faster pushdown queries**

Parquet 2.7+ defines `ColumnIndex` and `OffsetIndex` per column
chunk: per-page min/max + null counts. With these the reader can
skip whole pages (not just row groups) without ever opening the
column data. parquet-go has `WriteColumnIndex` support behind a
feature flag.

Implementation: enable
`parquet.WriteColumnIndex(true)` in the writer. At read time the
existing filter pushdown already takes ColumnIndex when present.

Risk: ~5-10% larger footer (the index lives there). Already
mitigated by the 2-phase footer fetch we just shipped.

### 5. Zstd dictionary training — **est. 20-40% smaller small columns**

Zstd supports user-supplied dictionaries. For columns with many
short repeated values (`http.url`, `k8s.pod.name`, `service.name`)
a trained dictionary cuts small-block overhead dramatically.

Implementation:
- Train a 64 KB dictionary per column type using `zstd --train`
  on a representative parquet sample.
- Embed the dictionary in the parquet file's `key_value_metadata`
  under e.g. `lakehouse.zstd_dict.<column>`.
- Reader looks up the dictionary at decode time.

Risk: needs codec wrapper. Out-of-the-box parquet readers will
still read the file (zstd-compressed blocks work with or without
a dictionary on the SAME data); only LH binaries benefit. So
strictly speaking standards-compliant, but only LH gets the
compression benefit.

### 6. Drop bloom filters on low-cardinality columns — **est. 5-10% smaller footer**

Bloom for `service.name` is high-ROI (a few hundred services in
a fleet, billion-row tables → ~99% file-skip). But blooms for
columns with low cardinality (`level`, `severity_number`,
`status.code`) add footer bytes without speeding up queries: an
equality filter on `level=ERROR` hits ~25% of files no matter
what. Dropping them shrinks the footer 5-10% and trims the
trace-index hot path.

Implementation: drop `parquet.SplitBlockFilter(10, "<col>")` for
the affected columns in `writeLogsParquet` / `writeTracesParquet`.

Risk: zero. Bloom filters are optional metadata; readers gracefully
fall back to zone-maps when absent.

### 7. Column chunk merging on compaction — **est. 5-15% smaller files**

When L0 files have many small column chunks (one per flush, then
many flushes per file), the per-chunk metadata overhead is
proportionally large. Compaction can stitch adjacent chunks from
the same row group into one big chunk during the rewrite. The
read path doesn't care — fewer chunks = less metadata, same
data.

Implementation: in `writeCompactedLogs`, set
`MaxRowsPerRowGroup` to a value where the natural row count
divides evenly. parquet-go already merges in most cases; explicit
hint avoids the edge cases.

Risk: minimal. Worst-case slightly larger row groups (see #3).

### 8. Schema slimming at L2+ — **est. 3-8% smaller cold files**

Some columns are unused at PB scale (`scope.attributes` for log-
only deployments, `span.kind` for trace data older than the
retention horizon). Compaction can drop these columns at L2+
since the data isn't going to be queried in the cold tier.

Implementation: extend `CompactionConfig` with
`DropColumnsByOutputLevel map[int][]string`. At write time
exclude the listed columns from the projection.

Risk: read code must tolerate missing columns (it already does
because parquet readers handle schema evolution).

### 9. ZSTD-19 via klauspost direct — **est. 5-10% smaller cold files**

parquet-go's zstd wrapper exposes only 4 encoder levels (Fastest
/ Default / Better / Best ≈ zstd-1/3/7/11). Switching to
`github.com/klauspost/compress/zstd` direct gives access to
levels 12-22 plus long-range mode.

Implementation: replace `parquet.Compression(&zstd.Codec{...})`
with a custom `parquet.Compression` implementation that delegates
to klauspost's encoder. Compaction's outputLevel = 11+ uses
level 19 (real zstd 19, not Best alias). Level 22 + long mode
for the deepest rollup.

Risk: more CPU at L2+ compaction (level 19 is ~2× slower than
11). Memory cost too (long mode adds 256 MB working set per
encoder). Worth it on cold data that gets read maybe once per
quarter.

### 10. BROTLI / LZMA on quarterly archives — **est. 10-15% smaller archive files**

For files older than 90 days the read frequency drops to maybe
once per quarter for audit/compliance. At that read rate
BROTLI-11 or LZMA-9 is worth the CPU — both are listed as
optional codecs in Parquet 2.x. parquet-go has codec hooks for
both.

Implementation: extend the compression schedule to support a
codec per output level: `Codec` enum with zstd/brotli/lzma.
Schedule becomes `[{zstd, 3}, {zstd, 7}, {zstd, 11}, {brotli, 11}]`.

Risk: smaller readability surface — brotli/lzma are spec-
optional, fewer non-LH tools handle them. Mitigate with a
quarterly-rollup-only opt-in.

## Out-of-scope (would break standards)

Listed so future readers can short-circuit a conversation:

- **Custom magic header / footer wrapper** — would break
  duckdb/pyarrow/parquet-tools. Reject every time.
- **Non-Parquet object format** — same.
- **Encrypted column data without a parquet-spec key descriptor**
  — Parquet has a standard encryption mode; use that instead.
- **Skip-list indexes outside `key_value_metadata`** — the
  existing trace-index lives in standard KV metadata for this
  reason.

## Implementation roadmap

| Step | Effort | Gain | When |
| --- | --- | --- | --- |
| 1. Sort by stream_id + time | 2 days | 30-50% | next PR |
| 2. DELTA_BINARY_PACKED timestamps | 1 day | 5-10% overall | next PR |
| 3. Larger L2+ row groups | half day | 5-10% cold | next PR |
| 4. PageIndex emission | 1 day | 2-5× pushdown | next PR |
| 5. Zstd dictionary training | 1 week | 20-40% small cols | follow-up |
| 6. Drop low-cardinality blooms | half day | 5-10% footer | next PR |
| 7. Column chunk merging | 1 day | 5-15% | next PR |
| 8. L2+ schema slimming | 2 days | 3-8% cold | follow-up |
| 9. klauspost zstd direct | 3 days | 5-10% cold | follow-up |
| 10. BROTLI archives | 1 week | 10-15% archive | follow-up |

**Next-PR bundle (#123 candidate)**: steps 1, 2, 3, 4, 6, 7 —
roughly two weeks of work, ~60-80% compounding storage reduction
for cold-tier files, AND faster queries via PageIndex. All on
standards-compliant Parquet.

**Follow-up PR**: step 5 (zstd dictionary) — biggest single
win but needs the training infrastructure built first.
