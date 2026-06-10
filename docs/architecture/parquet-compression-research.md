
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
