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

Reproduce: build `trace_by_id_cold_ab_test.go` into both trees (`go test -c`) and alternate the two binaries with `TBID_AB_OUT=… TBID_BUILD=main|branch TBID_ROUND=n TBID_STREAMS=… TBID_NAMES=… TBID_REPEATS=3` (the header of the test file lists the knobs).

## External readers (measured)

`TestFieldPresence_WritesObjectsForExternalReaders` writes a flushed, a compacted and a delete-rewritten object per signal; each was opened and counted by pyarrow 25.0.1, DuckDB 1.5.6, ClickHouse 25.8.33.6 (`clickhouse local`) and Spark 4.0.0 (`spark-sql`), all with the same row counts as the writer; an independent Python decoder (`zstandard`, 60 lines, the snippet in the format page) reads the same stream-days, rows and names from the footer as the Go decoder. Trino 476 was not run locally (it needs a catalog); the `readers-trino` CI job opens objects from the production writers. The new key is ASCII, so it does not add to the non-UTF-8 footer values of #340.
