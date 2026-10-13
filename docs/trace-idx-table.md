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

## Known limits of the spike

- An id lookup across many small objects of one hour costs a first read per object (footer and filters) in each process; after that, only
  the objects whose filter passes (about 1 % per object) cost reads. Compaction bounds the number of objects per hour. A persistent
  membership summary (the pmeta layer) is the planned fix and is not part of the spike.
- The buffer bridge between insert and select pods carries span rows only, so with insert peers the unflushed index rows of a peer are not
  served by a select pod; the objects are.
- The compaction statistics (`/lakehouse/api/v1/stats/compaction`) count the idx objects in their file and byte totals and group them
  per signal like every object, but never as stale-schema files: the fingerprint describes the span schema.
- The path is the traces binary's; the logs binary and its objects are unchanged (`TestLogsObjectBytesUnchangedByTheIdxTable`).
