---
title: The trace_id_idx table
sidebar_position: 21
---

# The trace_id_idx table (traces binary)

VictoriaTraces writes, for every trace, one row into one of 1,024 log streams (`{trace_id_idx_stream="N"}`, `N = xxhash64(trace_id) % 1024`)
with the fields `trace_id_idx`, `start_time`, `end_time`, `duration` and `_msg` `-`. Trace-by-ID (Jaeger and Tempo), the Tempo duration
search, the TraceQL `traceDuration` join and the VTUI duration heatmap read those rows, and hot VictoriaTraces counts them in every query
that matches them (`*`, `stats count()`, `field_values`, `streams`).

`lakehouse-traces` runs upstream's own index worker, so the rows reach its insert buffer. Before this table the flush dropped them, the
adapter emulated VictoriaTraces' first-stage query from a per-file `_trace_idx` footer entry and rewrote or stripped the other queries that
read the rows. Now the flush stores the rows and every query reads them: the stage-1 query, the duration search and the heatmap run
unchanged on the normal read path, and an export of the rows equals hot VictoriaTraces' export of its index stream.

The table is a build-time switch while this is a spike (`tracesidx.Enabled` in `internal/tracesidx`, no configuration knob); with the
switch off the binary behaves as before.

## Layout

One set of objects per tenant and hour partition, next to the span objects:

```
<tenant>/traces/dt=2026-10-13/hour=10/<nonce>-<slice>.parquet       spans
<tenant>/traces_idx/dt=2026-10-13/hour=10/<nonce>-<slice>.parquet   trace_id_idx rows
<tenant>/traces_idx/dt=2026-10-13/hour=10/compacted-L1-<id>.parquet  after compaction
```

The idx object of a flush group has the span object's name, so the insert buffer hides it with its segment (a row is served once at every
instant) and a restarted flush window names the same object. The kind of an object is derived from its key (`/traces_idx/`), so objects
adopted from a listing or after a restart need no metadata.

| Column | Parquet type | Encoding | Upstream field |
|---|---|---|---|
| `_time` | INT64 | PLAIN | `_time` (the trace's start time, nanoseconds) |
| `trace_id_idx_stream` | INT32 | PLAIN | the stream value `N` |
| `trace_id_idx` | BYTE_ARRAY | PLAIN | `trace_id_idx` |
| `start_time` | INT64 | PLAIN | `start_time` |
| `end_time` | INT64 | PLAIN | `end_time` |
| `duration` | INT64 | PLAIN | `duration` |

`_stream`, `_stream_id` and `_msg` are not stored: they are derived at read time with VictoriaLogs' own `StreamTags`, stream-id rule and
upstream's constant `-`. Rows are sorted by `trace_id_idx` (then time, start, end, stream), so the minimum and maximum id of an object and of
every row group are monotonic fences that every engine's zone maps use. Row groups hold 250,000 rows, pages 16 KiB, the column index keeps
32-byte bounds (a whole 32-hex id), compression is zstd. The plain encodings are deliberate: Apache Spark 4.0.0 fails on the library's default
string encoding once a page holds about 10,000 values and returned wrong values for delta-coded integers behind a page index; PLAIN costs
about 6 bytes per row more and every engine reads it.

`trace_id_idx` has a standard split-block bloom filter per row group. It is sized by the row group's DISTINCT ids (10.5 bits per id) and
rounded up to a power of two (Arrow C++, hence ClickHouse, rejects other sizes, issue 341). parquet-go v0.32.0 cannot write that: its
`SplitBlockFilter(bitsPerValue)` sizes by row count, and a wrapper type leaves the header's `algorithm` field empty, which ClickHouse
rejects; the writer therefore writes the object without a filter and adds the filters itself after the page indexes, with the standard
header (`numBytes`, `SPLIT_BLOCK`, `XXHASH`, `UNCOMPRESSED`) and the footer's `bloom_filter_offset` / `bloom_filter_length`
(`internal/tracesidx/bloom.go`). The long-term fix is a parquet-go change.

The footer carries one ASCII entry, `lh.trace_idx.format`, naming the layout version, the sort key and the sizes.

## Write path

The flusher collects a group's rows, converts the span rows as before and the rows of the index stream with `vlstorage.IdxRowsOf`, and
stores the idx object first, then the span object; the group's "stored" mark follows the span object (a group with no spans: the idx
object). A crash between the two re-sends the idx object (same key, same bytes) and never leaves spans without their index rows. The drain
invariant (`rows + filtered == seen`) counts the index rows. An index row is a row of the segment it reached: upstream's index worker emits
it 20 to 40 seconds after the spans, so it usually lands in a later segment than its spans; "an index row in the buffer reaches Parquet
exactly once" is what is guaranteed and tested, not "in the same drain as its spans".

Compaction merge-sorts and re-encodes the objects of a group with the function the flush uses (`tracesidx.Marshal`); it never merges or
drops rows, because hot VictoriaTraces keeps every index row it was sent. A compacted object is byte-identical to a flush of the same rows
at the same compression level (`TestIdxCompaction_GoldenFlushedVersusCompacted`). Delete tasks apply to the table (the rewriter evaluates
the tombstone filter on the fields VictoriaLogs sees for an index row), and retention keeps an object for the longest period any rule or the
default asks for: the objects carry no labels, so no per-label rule can match them, and a trace whose spans a rule keeps must stay findable.

## Read path

A query is routed by its filter: a selector of the index stream (`{trace_id_idx_stream...}`, the stage-1 query and the heatmap) reads the
table only; a selector of another stream, a positive exact filter on a span-only field (`trace_id:=X`, `service.name:=Y`: what the file
bloom prunes on) or `field:*` on a field an index row does not have (`span_id:*`) reads spans only; anything else, including `*`, a NOT or an OR, reads both. The route decides cost only: VictoriaLogs'
filter runs on every row that is read. `field_values`, `streams` and `stream_ids` add the table to their span scan, so the empty bucket of a
span field holds the index rows, as on hot.

With an exact `trace_id_idx` in the query each object is asked for that id by a lookup (`tracesidx.Locator`) that never opens the object with
the page index on (that reads every row group's page index: measured 69 KB, 701 KB and 3.5 MB at 1 M, 10 M and 50 M rows). It reads the
footer once (cached, parsed), the bloom filters of the object when they are small (cached with the footer: an id an object does not hold
costs no read), and for a row group whose fence holds the id the id column's column index and the offset indexes of the wanted columns (one
coalesced read, the writer lays a row group's offset indexes out back to back), then the one page that can hold the id and, at the matching
row, the pages of the other wanted columns. Every slice the footer points at is bounds-checked and structurally validated before the library
decodes it (`ValidateThrift`, fuzzed), so a hostile object cannot make a lookup allocate or read more than fixed limits. A small object
(128 KiB or less) is fetched whole, once, only when the cached footer and filters do not rule the id out.

Ranges go through the existing cache tiers: `smartcache.Controller.GetRange` tries memory, the node's disk (which keeps footers, index
slices and pages across restarts), the owner over the peer ring, then the object store, with one shared fetch per range. A lookup with no
exact id (`*`, the duration join, the heatmap) reads each object whole.

## What it changes for other tools

Every object is plain Parquet; `trace_id_idx = '<id>'` uses the bloom filter and the zone maps in DuckDB, pyarrow, Polars, DataFusion,
Spark and ClickHouse (see the proof in the pull request).

## The membership summary in pmeta

A lookup of an id must not cost a request per object. pmeta keeps, per object of the table, a summary (`FacetIDSummary`, kind 7, in
the partition's bundle `<tenant>/traces_idx/dt=/hour=/_pmeta.bundle`): for each row group its row count, the smallest and largest id
(the fences), the time bounds of its rows and a copy of its bloom filter's bits. They are copies of what the object's own footer
holds, so the facet is a cache of the Parquet objects, which stay the source of truth.

- **Built** at flush from the bytes just written (`tracesidx.Summarize`, about 10 microseconds for a 5,000-row object) and after a
  compaction from the output's footer and filters (at most three small ranged reads).
- **Used** by every lookup that names an id: the objects whose summary rules the id out (fences or filter) are not read at all; an
  object without a summary, and a partition without the facet, are never skipped.
- **Rebuilt**: a lookup that reads an object whose summary is missing records it from the footer it has just read
  (`lakehouse_trace_idx_summary_learned_total`), so a lost or unknown bundle costs one pass of lookups over the objects, nothing more.
  A bundle is written on the flusher's ticks and when the process is closed.
- **Bounded**: the bundles load lazily with the other facets (one GET per partition at start-up) and follow the shared eviction
  policy; the decoder refuses any length larger than the bytes that follow it (`FuzzIDSummaryDecode`).
- **Cost**: about 2.1 bytes of summary per index row at 250,000 rows per group (the filter is 10.5 bits per distinct id rounded up to a
  power of two); measured resident bytes are in the pull request.

The summary is the answer of the table's lookups only. The span objects keep the facets they have (the file-level bloom,
`FacetBloom`, answers "which span files can hold this trace"); a second per-row-group copy of the span objects' bloom filters was
measured and not built (the by-ID read of a large span object is dominated by the data reads of the row groups that match, and the
filters sit next to each row group's data, so rebuilding a copy takes one ranged read per row group).

## Measured requests of a lookup

Measured on one stack (RustFS as S3, hot VictoriaTraces as reference), restart-cold: `docker restart` before every lookup, then the S3
requests of the lookup alone (GetObject count and KiB), 6 ids per cell, tenant 0, 0 ms added S3 latency. "Main" is the build before the
table, whose by-ID lookups are answered from the `_trace_idx` footer entry of every span file.

| seed | layer | query | main | this build |
|---|---|---|---|---|
| time order (60 flush files per hour of about 54 s) | cold | stage 1 | 120.0 / 4,854 | 1.0 / 3 |
| | cold | Tempo v2 by-ID | 120.2 / 4,865 | 2.0 / 42 |
| | compacted | stage 1 | 4.0 / 2,805 | 1.0 / 45 |
| | compacted | Tempo v2 by-ID | 121.0 / 5,554 | 2.7 / 734 |
| | buffer | Tempo v2 by-ID | 120.0 / 4,854 | 0.0 / 0 |
| every flush file covers the whole hour (6 per hour) | cold | stage 1 | 12.0 / 1,837 | 1.0 / 8 |
| | cold | Tempo v2 by-ID | 1.2 / 162 | 3.0 / 220 |
| | compacted | Tempo v2 by-ID | 1.0 / 713 | 2.5 / 734 |
| | buffer | Tempo v2 by-ID | 0.0 / 0 | 0.2 / 2 |

Compacted-layer by-ID rows include the stage-1 query over VictoriaTraces' 24-hour windows. A lookup opens 1.0 to 1.2 span objects in
every layer. On the second seed the table costs one request more than main (the page of the id; main finds the trace's bounds in the
footers it re-fetched at start-up, which is 36.8 requests at start-up against 40.4 here).

## Known limits of the spike

- At 100 TB/day an hour object has thousands of row groups; the filter copy is kept up to 256 KiB per object, so for big objects the summary has fences only and a stage-1 window reads one bloom range of each (estimate, not measured).
- With the bundles lost, the first lookups read every object once (one request each) and record their summaries; compaction bounds the
  number of objects per hour.
- The buffer bridge between insert and select pods carries span rows only, so with insert peers the unflushed index rows of a peer are not
  served by a select pod; the objects are.
- The compaction statistics (`/lakehouse/api/v1/stats/compaction`) count the idx objects in their file and byte totals and group them
  per signal like every object, but never as stale-schema files: the fingerprint describes the span schema.
- The path is the traces binary's; the logs binary and its objects are unchanged (`TestLogsObjectBytesUnchangedByTheIdxTable`).
