# Dedicated columns — benchmark results

Measured on the e2e stack (MinIO + hot VL/VT + ClickHouse-over-S3), dedicated
columns enabled. Reproduce: `scripts/bench/compression_ab` (size) and
`scripts/bench/with-s3-latency.sh 100 30 -- scripts/bench/full-scope-s3-bench.sh`
(query latency, LH cold vs VL/VT hot vs CH-over-S3).

## Size (the headline) — real L2 data, identical rows re-encoded, zstd best
| signal | net size | blooms |
|---|--:|--:|
| logs | **−9.5%** | 10 |
| traces | **−8.0%** | 16 |
Promoting hot OTel attributes out of the maps into dict/plain columns shrinks
files (each key drops its per-row key-string and dict-compresses); the expanded
selective blooms are absorbed by that win. Needle filter on a promoted column
prunes **83% of row groups** (bloom) vs whole-map decode.

## Query latency — three-way LH cold vs VL hot vs CH-over-S3
LH carries a 100/30 ms injected S3-latency handicap; CH queried its object store
directly (no injection), so LH's win below is conservative.

| scenario | LH p50 | VL hot | CH-over-S3 | LH/VL | LH/CH |
|---|--:|--:|--:|--:|--:|
| count_1h | 577 ms | 38 | 3649 | 15.1× | **0.16×** |
| count_24h | 1921 ms | 157 | 4787 | 12.2× | **0.40×** |
| filtered_count_1h | 3194 ms | 44 | 3600 | 73.3× | **0.89×** |
| groupby_service_1h | 953 ms | 45 | 3807 | 21.3× | **0.25×** |
| fulltext_scan_1h | 2082 ms | 50 | 4110 | 41.3× | **0.51×** |
| field_values | 22 ms | 292 | 3869 | **0.1×** | **0.006×** |

**LH beats CH-over-S3 on every scenario** (North-Star bar met) and beats hot VL on
the metadata class. No regression from dedicated columns.

## Where the dedicated-column blooms help (the bench above does NOT show this)
The scenarios above filter `service.name`/`_msg`, not promoted high-cardinality
columns — so they don't exercise the new blooms. The benefit lands on
needle/filtered queries against a PROMOTED column. Measured live:
`k8s.pod.name:="..." | count` → **30 ms, 0 S3 GETs** — promotion gives the column
first-class metadata (label aggregates + bloom), answering from RAM instead of
scanning files (a non-promoted map attribute would scan). A `needle_promoted_col`
scenario should be added to full-scope-s3-bench.sh to track this.

## (legacy) Query latency — logs, LH cold vs VL hot (100/30 ms injected, p50 of 8)
| scenario | LH p50 | VL p50 | LH/VL | note |
|---|--:|--:|--:|---|
| count_1h | 577 ms | 38 | 15.1× | no regression vs pre-change (695) |
| count_24h | 1921 ms | 157 | 12.2× | metadata-served counts |
| filtered_count_1h | 3194 ms | 44 | 73.3× | high run-to-run variance |
| groupby_service_1h | 953 ms | 45 | 21.3× | improved vs pre-change (1412) |
| fulltext_scan_1h | 2082 ms | 50 | 41.3× | scan-bound |
| field_names | 111 ms | 299 | **0.4×** | beats hot VL |
| field_values (limit/nolimit) | 22–23 ms | 292–308 | **0.1×** | beats hot VL |

Dedicated columns introduce **no query regression** — LH tracks or beats its
pre-change baseline, and metadata queries remain faster than hot VL. The
three-way LH/VL/CH table (with the ClickHouse-over-S3 column) and the traces
suite are captured by the same harness; CH must have comparable data loaded.
