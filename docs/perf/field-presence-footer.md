# Field presence in the footer: what it costs

Every object a writer produces now carries `lakehouse.field_presence` in its Parquet footer (format: [open-parquet-format.md](../open-parquet-format.md#the-lakehousefield_presence-entry)). Nothing reads it yet, so no answer changes. This page records what it costs, measured on one laptop (Apple M-series, 18 cores, shared with other work, so absolute times are noisy; ratios from interleaved runs are what to read). Every number is labelled **measured** (with the command), or **assumed**.

## Footer growth (measured)

`TestFieldPresence_FooterSize` writes one object of 24,000 rows through the production writer and reports the size of the facet value, the footer and the object. Logs: 4 random attributes per row out of 30 names per stream; spans: 8 attributes per span out of 300 names per stream, no span events.

```
FP_SIZE_OUT=/tmp/size.jsonl GOWORK=off go test -run TestFieldPresence_FooterSize ./internal/storage/parquets3/
```

| Signal | Streams in the object | Stream-days | Facet value | Footer (with facet) | Object | Facet share of the footer | Facet bytes per stream-day |
|---|--:|--:|--:|--:|--:|--:|--:|
| logs | 100 | 100 | 7.0 KB | 50.7 KB | 625 KB | 14 % | 70 |
| logs | 1,000 | 1,000 | 42.3 KB | 85.9 KB | 752 KB | 49 % | 42 |
| logs | 10,000 | 9,087 | 373.7 KB | 417.5 KB | 1,436 KB | 90 % | 41 |
| traces | 100 | 100 | 26.0 KB | 669.9 KB | 2,345 KB | 4 % | 260 |
| traces | 1,000 | 1,000 | 129.2 KB | 773.1 KB | 2,540 KB | 17 % | 129 |
| traces | 10,000 | 9,074 | 522.4 KB | 1,166.5 KB | 3,287 KB | 45 % | 58 |

- An object with 10,000 or more streams is the case where the facet dominates a logs footer. The cost per stream-day is about 41 bytes at that scale (24 bytes of stream id that do not compress, plus rows and a set reference). The cap is 1 MiB of value, about 25,000 stream-days for logs; an object over it carries `lakehouse.field_presence_over_cap=1` and no facet (`lakehouse_field_presence_over_cap_total`).
- Spans are bigger per stream because each stream has many distinct attribute names, but their footers were already large: the trace index (`_trace_idx`) is 72 % of the footer of a small traces object before the facet (measured: a 24,000-span object, 987 KB, 715 KB of footer).
- The facet is computed from the rows of the object. In these tables a stream has few rows, which is the worst case for bytes per row.

## Write cost (measured)

Interleaved `main` and branch test binaries, 5 rounds of `-count 2` each (`-benchtime 8x`, 10 samples per side), 20,000 rows per flush: 100 streams, about 30 non-empty fields and 10 attributes per row with freshly allocated key strings; spans with 8 attributes, an event on one span in 20. `benchstat main=... branch=...`.

| Operation | main | branch | change |
|---|--:|--:|--:|
| flush writer, logs rows | 2.15 µs/row | 2.35 µs/row | +9.4 % (p=0.001) |
| flush writer, span rows (logs module) | 1.75 µs/row | 1.95 µs/row | +11.1 % |
| flush writer, span rows (traces module) | 1.79 µs/row | 2.05 µs/row | +14.8 % |
| compaction / rewrite writer, logs rows | 1.56 µs/row | 2.26 µs/row | +44 %: facet 10 % and the body token blooms the compactor did not write before (#344), 590 ns/row, 37 % |
| compaction / rewrite writer, span rows (traces module) | 1.69 µs/row | 1.97 µs/row | +17 % |

The facet alone (`BenchmarkFieldPresence_Extract`, 10 runs): logs 203 ns/row, spans 220 to 290 ns/row, and **about 1,040 ns/row when every span carries an event**: the JSON of span events and links is parsed for its field names, which costs about 0.8 µs per span that has one. The token bloom of a whole object (`BenchmarkTokenBloom_RowGroupOptions`) costs 590 ns/row; it was already paid by every flush and is now paid by compaction too.

How it is kept this low: the names of a row depend only on which of its string columns are not empty, so the read path's own row-to-field conversion runs once for each distinct pattern of non-empty columns (a few per stream) instead of once per row. Running it for every row cost 1,372 ns/row (32 % of a flush) in the first version of this change.

## Trace-by-ID on a cold object (measured)

The footer growth is paid by the query that reads every footer: a cold trace-by-ID (`trace_id:="…"`, fresh `Storage`, empty footer cache) over 3 objects of 24,000 spans each (2 spans per trace). `TestTraceByIDColdAB`, the same file compiled against `main` and against the branch, 5 interleaved rounds, 3 runs per cell and round, S3 latency injected by the mock and checked by a probe (probe column), every answer validated against the spans written (360 of 360 valid).

| Streams per object | Objects' footers | Bytes read per query main / branch | S3 GETs | p50 at 0 / 20 / 50 / 100 ms: branch ÷ main (median of the 5 round ratios) | probe |
|--:|--:|---|--:|---|---|
| 100 (40 names each) | 1.93 → 1.95 MB (+0.9 %) | 5.77 → 5.80 MB (+0.5 %) | 10 / 10 | 0.92 / 0.99 / 1.05 / 1.00 | 0.07 / 20.4 / 50.5 / 100.4 ms |
| 1,000 (300 names) | 1.93 → 2.31 MB (+19.5 %) | 5.86 → 6.46 MB (+10 %) | 10 / 10 | 0.86 / 0.98 / 0.99 / 0.97 | 0.07 / 20.5 / 50.4 / 100.4 ms |
| 10,000 (300 names) | 1.93 → 3.06 MB (+58.6 %) | 6.14 → 7.94 MB (+29 %) | 10 / 10 | 0.91 / 0.91 / 0.98 / 1.01 | 0.06 / 20.3 / 52.1 / 100.5 ms |

Reading it plainly:

- **No latency regression can be seen, and none can be excluded below about 10 %.** The machine was loaded (load average 10 to 35) and the round-to-round ratio of one cell ranges from 0.71 to 1.33; the geometric mean over all cells is 0.975. The check the change was held to was a regression above 5 %; this measurement cannot resolve 5 %, so it shows only that nothing large happens.
- **Bytes do grow, with the number of streams.** GETs stay at 10. At 10,000 streams per object a cold trace-by-ID reads 29 % more bytes; at 100 MB/s that is about 6 ms of transfer per object, spread over parallel reads, and invisible at the injected first-byte latencies. On an S3 link much slower than that it would show.
- The facet is not read by this query, but it travels in the footer tail the query fetches. Keeping it out of the cached footer (so queries never carry it) is the mitigation if the growth matters; it is not done here because nothing reads the facet yet.

### A footer that no longer fits the prefetch tail (measured)

The table above uses small objects (2 MB), whose footers are already larger than the footer prefetch tail (`max(64 KiB, size/8)`, at most 640 KiB), so both builds fetch them with a second exact read. A production-sized object is the case that matters: the trace index makes a compacted traces footer 470 to 520 KB, which the 640 KiB tail covers in one read. Re-run with `TBID_SPANS=17000 TBID_BODY_BYTES=300` (6.5 MB objects, footers of 478 KB on `main`), 4 interleaved rounds of 3 runs per latency, all answers valid:

| Streams per object | Footer per object main → branch | GETs | Bytes | Latency, branch ÷ main at 0 / 20 / 50 / 100 ms (median of 4 round ratios; range) |
|--:|---|--:|--:|---|
| 100 | 478 → 492 KB | 53 / 53 | +0.2 % | not run (the footer stays inside the tail) |
| 1,000 | 478 → 586 KB | 53 / 53 | +1.6 % | not run (the footer stays inside the tail) |
| 10,000 | 478 → 772 KB | 53 → 59 (+2 per object) | +9.5 % | 1.031 / 1.038 / 1.037 / 1.041 (1.02 to 1.07) |

At 10,000 streams per object the facet pushes a traces footer past the 640 KiB tail, so each object's footer needed a second read (the 2 extra GETs per object): a sequential round trip per cold query, +3 to 4 % latency here. Up to about 1,000 streams per object (the facet adds about 110 KB to a 480 KB footer, still inside 640 KiB) it did not. This is what the exact footer reads below remove.

### Compacted objects, the layout a cold trace-by-ID mostly reads (measured)

The same harness with `TBID_LAYOUT=compacted TBID_SPANS=17000 TBID_BODY_BYTES=1100`: the spans are written by the compaction writer (`compaction.WriteTraces`), objects of 19 MB with a footer of 460 KB on `main` (the 467 to 519 KB of a live L2 object). The branch adds two things to a compacted object: the facet and, since #344, the body token blooms the compactor did not write before (here 20 KB per object, from the filter size formula: 12.0 KB for the 10,000-row group and 8.4 KB for the 7,000-row one; the message bodies are single hex tokens, so real text would give a larger bloom). 4 interleaved rounds of 3 runs per latency, 192 of 192 answers valid.

| Streams per object | Footer per object main → branch | GETs | Bytes | Latency, branch ÷ main at 0 / 20 / 50 / 100 ms (median of 4 round ratios; range of the ratios) |
|--:|---|--:|--:|---|
| 1,000 | 460 → 590 KB (+129 KB: about 109 KB facet, 20 KB blooms) | 55 / 55 | 0 | 1.023 / 1.021 / 1.045 / 1.039 (0.89 to 1.10) |
| 10,000 | 460 → 775 KB (+314 KB: about 294 KB facet, 20 KB blooms) | 55 → 61 | +3.7 % | 0.951 / 1.024 / 0.998 / **1.069** (0.84 to 1.31; at 100 ms every round is at or above 1.03) |

At 10,000 streams per compacted object the footer passed the 640 KiB tail (the live headroom above a 460 KB footer is about 180 KB), each object needed a second footer read (+2 GETs per object, one more sequential round trip per cold query), and at the highest injected latency (100 ms) the median was 6.9 % slower. At 1,000 streams per object the footer stays inside the tail, GETs and bytes are unchanged and the latency ratios sit inside the noise of this machine. The growth is almost all the facet; the token blooms of #344 are 20 KB of the 314 KB in this fixture. The next section removes the extra round trips.

### Exact footer reads: the regression removed (measured)

Why there were three requests per object, not two (request log of one cold query at 10,000 streams, compacted layout; the mock records every ranged GET): the batch footer prefetch read the 640 KiB window, found the footer larger than that, and **dropped** it (`footer_prefetch.go`, the `tooBig` branch: the window read was wasted); the open path (`fetchFooterTail`) then read the same 640 KiB window again and, finding the same, read the footer once more with a 32 KiB look-behind. 655,360 + 655,360 + 807,118 bytes per object, three round trips, against one 655,360-byte read on `main`.

What changed (both binaries, one shared helper, `internal/parquetfooter`):

1. The flush, compaction and delete-rewrite writers record `FileInfo.FooterTail`, the bytes from the first page-index section to the end of the object, taken from the footer they just parsed. A footer read of an object with that record is **one request of exactly those bytes**, footer and page-index stripe together, whatever the footer's size.
2. Without the record (an object adopted from a listing, a snapshot from before the field) the read uses the window and, for a larger footer, reads only the head the window missed (a second request, no overlap with the first). The first read records the length in the manifest.
3. The batch prefetch spends at most `max(64 KiB, size/8)` on one object. A footer past that (an object that is mostly footer) is not probed ahead of time; the query that opens the object reads the footer exactly.

Cold trace-by-ID, `TestTraceByIDColdAB` compiled against `main`, the previous head of this PR (29674783, "S1") and this change, objects as in the table above (`TBID_LAYOUT=compacted TBID_SPANS=17000 TBID_BODY_BYTES=1100`, objects of 19 MB), 5 interleaved rounds (build order alternated per round) of 3 runs per cell, S3 latency injected and probe-verified (20.4 to 20.5, 50.5 to 50.7, 100.7 to 100.8 ms), host load average 3 to 8, **480 of 480 answers valid**. `s1b` is this change; `s1b, no record` is this change with the manifest entries lacking `FooterTail` (what a listing-adopted object looks like).

| Streams per object | Build | GETs | Critical path (round trips) | Bytes | Latency ratio to `main` at 0 / 20 / 50 / 100 ms (median of 5 round ratios) |
|--:|---|--:|--:|--:|---|
| 1,000 | S1 | 55 | 53 | +0.0 % | 1.007 / 1.000 / 1.000 / 1.001 |
| 1,000 | **S1b** | **55** | **53** | -0.1 % | 1.003 / 1.000 / 0.999 / 1.000 |
| 1,000 | S1b, no record | 55 | 53 | +0.0 % | 0.999 / 0.997 / 1.001 / 1.003 |
| 10,000 | S1 | 61 | 55 | +3.7 % | 1.032 / 1.037 / 1.035 / 1.038 (range 0.96 to 1.05) |
| 10,000 | **S1b** | **55** | **53** | +0.3 % | **1.012 / 1.007 / 1.003 / 1.001** (range 0.96 to 1.04) |
| 10,000 | S1b, no record | 58 | 54 | +0.4 % | 1.030 / 1.019 / 1.017 / 1.022 |

`main`: 55 GETs, 53 round trips on the critical path (longest chain of requests each starting after the previous ended, from the mock's timestamps; `TBID_AB_OUT` records it as `chain`), 117.4 MB read per query. With the record, the 10,000-stream case is back on `main`'s request pattern (55 GETs, same critical path) and its latency ratio is within 1.2 % at every injected latency (the S1 build was 3.2 to 3.8 % slower in every round and cell, its range of per-round ratios 1.02 to 1.05 at 20 to 100 ms). The +0.3 % bytes are the facet itself: the footer is 774 KB against 460 KB, read once instead of the 640 KiB window. Without the record the cost is one more request per object (58 GETs, one more round trip) and 1.7 to 3.0 % latency, until the first read records the length.

The harness (the mock S3 has no bandwidth model) cannot show a transfer-time cost of the extra bytes; at 100 MB/s the +0.4 MB is about 4 ms per query.

#### A small footer reads fewer bytes (measured)

The old window is fixed (64 KiB to 128 KiB on logs, up to 640 KiB on traces) whatever the footer needs; the record makes the read exact. Flushed (not compacted) span objects, `TBID_SPANS=6000 TBID_BODY_BYTES=1100 TBID_STREAMS=100 TBID_NAMES=40`, objects of 6.7 MB with a 173 KB footer: every cold footer read is **655,360 bytes on `main` and 182,896 bytes (-72 %) with the record**, the whole query reads **58.26 MB -> 56.88 MB (-2.4 %)** with the same 54 GETs and the same 52-round-trip critical path (latency ratio to `main` at 0 / 50 ms: 0.90 / 1.00, inside this machine's noise; the bytes and GETs are the deterministic result). Logs, the mock S3 fixture of three 700 KB objects with a 49 KB footer (`TestFooterTail_ColdQueryReadsFewerFooterBytes`): a whole cold `BIGMARK | stats count()` reads 1,146,135 bytes without the record and **1,022,921 with it (-10.8 %)**, 15 GETs either way; the same test on the traces fixture (footers larger than the window, so the second request disappears too): 27 GETs / 3.77 MB -> **21 GETs / 2.44 MB (-35 %)**.

Objects under 128 KiB are downloaded whole (`minFileSizeForPrefetch`) and planned-mode logs objects under 5 MB too (`s3.whole_file_threshold_bytes`); their path is unchanged, so the 71 KiB median flush objects of a live deployment gain nothing from this and lose nothing; the dividend is for the footer-range reads of larger objects, which are the ones that carry a trace index or a facet.

Reproduce: build `trace_by_id_cold_ab_test.go` into each tree (`go test -c`) and alternate the binaries with `TBID_AB_OUT=… TBID_BUILD=main|s1|s1b TBID_ROUND=n TBID_STREAMS=… TBID_NAMES=… TBID_REPEATS=3` (the header of the test file lists the knobs). `trace_by_id_cold_ab_hint_test.go` is a per-tree shim: it sets `FileInfo.FooterTail` on the builds that have the field and is empty on the builds that do not, so the file compiles against all of them; `TBID_HINT=0` leaves the record off (an object adopted from a listing) and `TBID_DUMP=1` prints every ranged GET of the first run of each cell.

### Field-metadata matrix after exact footer reads (measured)

`scripts/bench/field_metadata/run.sh` at 0 ms, one round of two iterations, both signals, 384 cells, builds `main` (ea850555+), the previous head of this PR and this change in the same harness (`perf_rows.py check` against `rows/perf/field_metadata.yaml`; the registry rows were not touched). `main` reproduces its registry rows exactly (0 of 384 differ). Change of `s3_bytes` per row against the registry (that is, against `main`), rows that read anything from S3, min / mean / max, GETs, row groups and pages unchanged unless stated:

| Signal | Layout | Rows | Against `main` (the registry) | Against the previous head |
|---|---|--:|---|---|
| logs | flushed | 62 | +1.48 % / +2.13 % / +2.89 % | 0.00 % |
| logs | peer | 62 | +1.48 % / +2.11 % / +2.89 % | 0.00 % |
| logs | compacted | 62 | -1.31 % / +2.84 % / +4.24 % | -5.19 % / -1.30 % / 0.00 % |
| traces | flushed | 62 | +0.37 % / +0.44 % / +0.53 % | 0.00 % |
| traces | peer | 62 | +0.37 % / +0.44 % / +0.53 % | 0.00 % |
| traces | compacted | 62 | -11.11 % / -2.87 % / 0.00 % (16 rows change) | -11.11 % / -2.87 % / 0.00 % |

The rows that still grow against `main` are the facet itself and, in compacted logs objects, the body token blooms that compaction now writes (#344): these objects are downloaded whole, so every byte of the footer is a byte of the object. This change does not move them (flushed and peer rows are identical to the previous head) and lowers the compacted rows. Where the footer is read by range (the 16 traces `field_names` rows on compacted objects, whose footers are 73 % of a 970 KB object) the previous head and `main` spent a wasted window read and then downloaded the whole object; the rows now read 2 GETs instead of 4 (and 1 instead of 2 for the narrow window), 11 % fewer bytes. The first version of this change read those footers exactly in the prefetch and made the same rows 54 % larger than `main`'s; the prefetch budget (`max(64 KiB, size/8)`, above) is what keeps a footer-dominated object from being probed ahead of a whole-object download.

### What the writers pay (measured)

`BenchmarkFlushWrite` (20,000 rows, `-benchtime 8x`, 5 interleaved rounds of `-count 2`, benchstat, the previous head against this change): logs 2.497 -> 2.472 µs/row (~, p=0.48), spans 2.121 -> 2.048 µs/row (~, p=0.12) in the logs module; 2.539 -> 2.424 and 2.110 -> 2.205 in the traces module (~, p=0.76 and 0.14). **No measurable change.** The writers used to parse the footer they had just written twice (column bytes, bloom bytes), decoding the page index each time; `Inspect` parses once and skips the page index. On a 60,000-row object with 60 row groups and a 200 KB footer that is 305 µs -> 120 µs (`BenchmarkInspect`, 5 runs: 296 to 323 µs against 117 to 139 µs), about 0.2 ms of a 40 to 50 ms flush: real, below the noise of the flush benchmark. The facet's own cost (the table above) is unchanged: no exact saving was found in the remaining pieces (the span event and link JSON is decoded by the read path's own function, which the drift guard holds equal to the facet; reusing the source token blooms in compaction would not be exact, because compaction regroups the rows and a bloom of the union is not the bloom of the new row group).

## External readers (measured)

`TestFieldPresence_WritesObjectsForExternalReaders` writes a flushed, a compacted and a delete-rewritten object per signal; each was opened and counted by pyarrow 25.0.1, DuckDB 1.5.6, ClickHouse 25.8.33.6 (`clickhouse local`) and Spark 4.0.0 (`spark-sql`), all with the same row counts as the writer; an independent Python decoder (`zstandard`, 60 lines, the snippet in the format page) reads the same stream-days, rows and names from the footer as the Go decoder. Trino 476 was not run locally (it needs a catalog); the `readers-trino` CI job opens objects from the production writers. The new key is ASCII, so it does not add to the non-UTF-8 footer values of #340.
