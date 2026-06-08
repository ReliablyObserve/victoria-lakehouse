# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse

p95 latency (ms), with **result-validity checks**. Baseline = VL/VT on disk (gp3-simulated under `--disk-profile gp3-loop`). LH and ClickHouse read the **same Parquet** on S3 behind the latency proxy. Each cell shows `p95 (×base) [result]`; **✗ = invalid** (errored / 0 bytes / result diverges >5% from baseline — latency NOT comparable).

| signal | query | range | S3 lat | baseline p95 [res] | LH | CH |
|---|---|---|---:|---:|---|---|
| logs | count_by_service | 1h | 0ms | 2.2 [513] | 3.6 (1.6×) [513] | 106.5 (48.4× 🔴) [513] |
| logs | count_by_service | 1h | 100ms | 2.8 [506] | 3.7 (1.3×) [506] | 108.7 (38.8× 🔴) [506] |
| logs | count_by_service | 1h | 300ms | 4.3 [506] | 4.9 (1.1×) [506] | 146.2 (34.0× 🔴) [506] |
| logs | count_by_service | 24h | 0ms | 7.9 [14246] | 13.2 (1.7×) [14246] | 101.7 (12.9× 🔴) [14246] |
| logs | count_by_service | 24h | 100ms | 6.8 [14233] | 11.4 (1.7×) [14233] | 145.5 (21.4× 🔴) [14233] |
| logs | count_by_service | 24h | 300ms | 11.8 [14229] | 13.4 (1.1×) [14229] | 97.3 (8.2× ⚠️) [14229] |
| logs | count_by_service | 6h | 0ms | 5.8 [3545] | 6.0 (1.0×) [3545] | 102.9 (17.7× 🔴) [3545] |
| logs | count_by_service | 6h | 100ms | 7.4 [3535] | 11.1 (1.5×) [3535] | 99.8 (13.5× 🔴) [3535] |
| logs | count_by_service | 6h | 300ms | 5.2 [3526] | 6.0 (1.2×) [3526] | 103.9 (20.0× 🔴) [3526] |
| logs | count_total | 1h | 0ms | 2.6 [514] | 3.0 (1.2×) [514] | 98.2 (37.8× 🔴) [515] |
| logs | count_total | 1h | 100ms | 3.0 [507] | 4.4 (1.5×) [507] | 101.7 (33.9× 🔴) [507] |
| logs | count_total | 1h | 300ms | 3.3 [506] | 4.4 (1.3×) [506] | 120.0 (36.4× 🔴) [506] |
| logs | count_total | 24h | 0ms | 10.0 [14246] | 11.8 (1.2×) [14246] | 105.0 (10.5× 🔴) [14246] |
| logs | count_total | 24h | 100ms | 7.2 [14233] | 12.8 (1.8×) [14233] | 96.8 (13.4× 🔴) [14233] |
| logs | count_total | 24h | 300ms | 7.6 [14229] | 11.2 (1.5×) [14229] | 123.1 (16.2× 🔴) [14229] |
| logs | count_total | 6h | 0ms | 2.5 [3545] | 6.1 (2.4×) [3545] | 103.7 (41.5× 🔴) [3545] |
| logs | count_total | 6h | 100ms | 4.6 [3535] | 10.0 (2.2×) [3535] | 94.8 (20.6× 🔴) [3535] |
| logs | count_total | 6h | 300ms | 5.2 [3528] | 8.3 (1.6×) [3528] | 105.7 (20.3× 🔴) [3528] |
| logs | fulltext | 1h | 0ms | 2.4 [43] | ✗ result 0 vs base 43 | 116.6 (48.6× 🔴) [43] |
| logs | fulltext | 1h | 100ms | 3.9 [41] | ✗ result 0 vs base 41 | 97.3 (24.9× 🔴) [41] |
| logs | fulltext | 1h | 300ms | 4.2 [41] | ✗ result 0 vs base 41 | 103.9 (24.7× 🔴) [41] |
| logs | fulltext | 24h | 0ms | 7.8 [1102] | ✗ result 0 vs base 1102 | 96.8 (12.4× 🔴) [1102] |
| logs | fulltext | 24h | 100ms | 6.8 [1102] | ✗ result 0 vs base 1102 | 106.0 (15.6× 🔴) [1102] |
| logs | fulltext | 24h | 300ms | 14.9 [1102] | ✗ result 0 vs base 1102 | 109.7 (7.4× ⚠️) [1102] |
| logs | fulltext | 6h | 0ms | 5.2 [274] | ✗ result 0 vs base 274 | 105.3 (20.2× 🔴) [274] |
| logs | fulltext | 6h | 100ms | 5.2 [273] | ✗ result 0 vs base 273 | 111.0 (21.3× 🔴) [273] |
| logs | fulltext | 6h | 300ms | 5.4 [273] | ✗ result 0 vs base 273 | 107.1 (19.8× 🔴) [273] |
| traces | count_by_service | 1h | 0ms | 1.2 [623] | ✗ result 548 vs base 623 | ✗ result 548 vs base 623 |
| traces | count_by_service | 1h | 100ms | 1.0 [623] | ✗ result 548 vs base 623 | ✗ result 548 vs base 623 |
| traces | count_by_service | 1h | 300ms | 1.5 [623] | ✗ result 548 vs base 623 | ✗ result 548 vs base 623 |
| traces | count_by_service | 24h | 0ms | 1.6 [18910] | ✗ result 16546 vs base 18910 | ✗ result 16546 vs base 18910 |
| traces | count_by_service | 24h | 100ms | 1.6 [18894] | ✗ result 16532 vs base 18894 | ✗ result 16532 vs base 18894 |
| traces | count_by_service | 24h | 300ms | 3.2 [18887] | ✗ result 16526 vs base 18887 | ✗ result 16526 vs base 18887 |
| traces | count_by_service | 6h | 0ms | 1.4 [4545] | ✗ result 3980 vs base 4545 | ✗ result 3980 vs base 4545 |
| traces | count_by_service | 6h | 100ms | 1.3 [4540] | ✗ result 3976 vs base 4540 | ✗ result 3976 vs base 4540 |
| traces | count_by_service | 6h | 300ms | 2.0 [4540] | ✗ result 3976 vs base 4540 | ✗ result 3976 vs base 4540 |
| traces | count_total | 1h | 0ms | 1.0 [623] | ✗ result 548 vs base 623 | ✗ result 548 vs base 623 |
| traces | count_total | 1h | 100ms | 1.2 [623] | ✗ result 548 vs base 623 | ✗ result 548 vs base 623 |
| traces | count_total | 1h | 300ms | 1.0 [623] | ✗ result 548 vs base 623 | ✗ result 548 vs base 623 |
| traces | count_total | 24h | 0ms | 1.5 [18910] | ✗ result 16546 vs base 18910 | ✗ result 16546 vs base 18910 |
| traces | count_total | 24h | 100ms | 1.4 [18894] | ✗ result 16532 vs base 18894 | ✗ result 16532 vs base 18894 |
| traces | count_total | 24h | 300ms | 3.3 [18887] | ✗ result 16526 vs base 18887 | ✗ result 16526 vs base 18887 |
| traces | count_total | 6h | 0ms | 1.1 [4545] | ✗ result 3980 vs base 4545 | ✗ result 3980 vs base 4545 |
| traces | count_total | 6h | 100ms | 1.7 [4540] | ✗ result 3976 vs base 4540 | ✗ result 3976 vs base 4540 |
| traces | count_total | 6h | 300ms | 1.2 [4540] | ✗ result 3976 vs base 4540 | ✗ result 3976 vs base 4540 |

## Where LH lags the baseline most (valid cells only)

- **2.4×** — logs/count_total/6h/0: LH 6.1ms vs baseline 2.5ms
- **2.2×** — logs/count_total/6h/100: LH 10.0ms vs baseline 4.6ms
- **1.8×** — logs/count_total/24h/100: LH 12.8ms vs baseline 7.2ms
- **1.7×** — logs/count_by_service/24h/100: LH 11.4ms vs baseline 6.8ms
- **1.7×** — logs/count_by_service/24h/0: LH 13.2ms vs baseline 7.9ms
- **1.6×** — logs/count_by_service/1h/0: LH 3.6ms vs baseline 2.2ms
- **1.6×** — logs/count_total/6h/300: LH 8.3ms vs baseline 5.2ms
- **1.5×** — logs/count_by_service/6h/100: LH 11.1ms vs baseline 7.4ms

## ⚠️ Invalid cells (NOT comparable — fix before trusting)

- lakehouse — logs/fulltext/1h/lat0ms: **result 0 vs base 43**
- lakehouse — logs/fulltext/1h/lat100ms: **result 0 vs base 41**
- lakehouse — logs/fulltext/1h/lat300ms: **result 0 vs base 41**
- lakehouse — logs/fulltext/24h/lat0ms: **result 0 vs base 1102**
- lakehouse — logs/fulltext/24h/lat100ms: **result 0 vs base 1102**
- lakehouse — logs/fulltext/24h/lat300ms: **result 0 vs base 1102**
- lakehouse — logs/fulltext/6h/lat0ms: **result 0 vs base 274**
- lakehouse — logs/fulltext/6h/lat100ms: **result 0 vs base 273**
- lakehouse — logs/fulltext/6h/lat300ms: **result 0 vs base 273**
- lakehouse — traces/count_by_service/1h/lat0ms: **result 548 vs base 623**
- clickhouse — traces/count_by_service/1h/lat0ms: **result 548 vs base 623**
- lakehouse — traces/count_by_service/1h/lat100ms: **result 548 vs base 623**
- clickhouse — traces/count_by_service/1h/lat100ms: **result 548 vs base 623**
- lakehouse — traces/count_by_service/1h/lat300ms: **result 548 vs base 623**
- clickhouse — traces/count_by_service/1h/lat300ms: **result 548 vs base 623**
- lakehouse — traces/count_by_service/24h/lat0ms: **result 16546 vs base 18910**
- clickhouse — traces/count_by_service/24h/lat0ms: **result 16546 vs base 18910**
- lakehouse — traces/count_by_service/24h/lat100ms: **result 16532 vs base 18894**
- clickhouse — traces/count_by_service/24h/lat100ms: **result 16532 vs base 18894**
- lakehouse — traces/count_by_service/24h/lat300ms: **result 16526 vs base 18887**
- clickhouse — traces/count_by_service/24h/lat300ms: **result 16526 vs base 18887**
- lakehouse — traces/count_by_service/6h/lat0ms: **result 3980 vs base 4545**
- clickhouse — traces/count_by_service/6h/lat0ms: **result 3980 vs base 4545**
- lakehouse — traces/count_by_service/6h/lat100ms: **result 3976 vs base 4540**
- clickhouse — traces/count_by_service/6h/lat100ms: **result 3976 vs base 4540**
- lakehouse — traces/count_by_service/6h/lat300ms: **result 3976 vs base 4540**
- clickhouse — traces/count_by_service/6h/lat300ms: **result 3976 vs base 4540**
- lakehouse — traces/count_total/1h/lat0ms: **result 548 vs base 623**
- clickhouse — traces/count_total/1h/lat0ms: **result 548 vs base 623**
- lakehouse — traces/count_total/1h/lat100ms: **result 548 vs base 623**
- clickhouse — traces/count_total/1h/lat100ms: **result 548 vs base 623**
- lakehouse — traces/count_total/1h/lat300ms: **result 548 vs base 623**
- clickhouse — traces/count_total/1h/lat300ms: **result 548 vs base 623**
- lakehouse — traces/count_total/24h/lat0ms: **result 16546 vs base 18910**
- clickhouse — traces/count_total/24h/lat0ms: **result 16546 vs base 18910**
- lakehouse — traces/count_total/24h/lat100ms: **result 16532 vs base 18894**
- clickhouse — traces/count_total/24h/lat100ms: **result 16532 vs base 18894**
- lakehouse — traces/count_total/24h/lat300ms: **result 16526 vs base 18887**
- clickhouse — traces/count_total/24h/lat300ms: **result 16526 vs base 18887**
- lakehouse — traces/count_total/6h/lat0ms: **result 3980 vs base 4545**
- clickhouse — traces/count_total/6h/lat0ms: **result 3980 vs base 4545**
- lakehouse — traces/count_total/6h/lat100ms: **result 3976 vs base 4540**
- clickhouse — traces/count_total/6h/lat100ms: **result 3976 vs base 4540**
- lakehouse — traces/count_total/6h/lat300ms: **result 3976 vs base 4540**
- clickhouse — traces/count_total/6h/lat300ms: **result 3976 vs base 4540**
