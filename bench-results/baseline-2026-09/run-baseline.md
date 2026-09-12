# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse

## Overall

- **52 valid LH cells, 12 invalid** (excluded). Baseline = VL/VT on disk (gp3-simulated); LH + ClickHouse read the same S3 Parquet.
- **logs**: LH median **2.0×** baseline (p90 4.6×, best 0.7×); LH is **12× faster than ClickHouse**. (36 valid / 0 invalid)
- **traces**: LH median **2.6×** baseline (p90 7.0×, best 0.6×); LH is **25× faster than ClickHouse**. (16 valid / 12 invalid)

## Logs

**Per-query median LH vs baseline:** count_by_service 2.2×, count_total 2.3×, fulltext 2.8×, high_card 2.4×, level_filter 2.9×, multi_filter 3.7×, negation 2.5×, scan 3.2×, trace_lookup 1.1×

| query | range | S3 lat | baseline p95 [res] | LH | CH |
|---|---|---:|---:|---|---|
| count_by_service | 1h | 0ms | 2.4 [605] | 7.0 (2.9×) [604] | 82.6 (34.4× 🔴) [605] |
| count_by_service | 1h | 100ms | 2.9 [581] | 5.3 (1.8×) [581] | 94.3 (32.5× 🔴) [581] |
| count_by_service | 24h | 0ms | 6.3 [13710] | 9.9 (1.6×) [13710] | 70.5 (11.2× 🔴) [13710] |
| count_by_service | 24h | 100ms | 5.1 [13687] | 13.2 (2.6×) [13687] | 89.7 (17.6× 🔴) [13687] |
| count_total | 1h | 0ms | 2.4 [606] | 3.9 (1.6×) [606] | 71.9 (30.0× 🔴) [606] |
| count_total | 1h | 100ms | 4.0 [581] | 5.0 (1.2×) [581] | 76.6 (19.1× 🔴) [581] |
| count_total | 24h | 0ms | 4.5 [13711] | 15.2 (3.4× ⚠️) [13711] | 99.4 (22.1× 🔴) [13711] |
| count_total | 24h | 100ms | 4.8 [13687] | 14.2 (3.0×) [13687] | 86.0 (17.9× 🔴) [13687] |
| fulltext | 1h | 0ms | 3.6 [58] | 5.2 (1.4×) [58] | 72.0 (20.0× 🔴) [58] |
| fulltext | 1h | 100ms | 4.1 [54] | 7.0 (1.7×) [54] | 85.5 (20.9× 🔴) [54] |
| fulltext | 24h | 0ms | 5.1 [1116] | 20.1 (3.9× ⚠️) [1116] | 78.9 (15.5× 🔴) [1116] |
| fulltext | 24h | 100ms | 6.0 [1115] | 23.0 (3.8× ⚠️) [1115] | 92.3 (15.4× 🔴) [1115] |
| high_card | 1h | 0ms | 4.0 [603] | 4.8 (1.2×) [603] | 88.2 (22.1× 🔴) [603] |
| high_card | 1h | 100ms | 3.8 [579] | 7.2 (1.9×) [579] | 82.1 (21.6× 🔴) [579] |
| high_card | 24h | 0ms | 7.4 [13705] | 22.0 (3.0×) [13705] | 78.5 (10.6× 🔴) [13705] |
| high_card | 24h | 100ms | 8.3 [13687] | 25.3 (3.0× ⚠️) [13687] | 97.6 (11.8× 🔴) [13687] |
| level_filter | 1h | 0ms | 3.2 [152] | 5.0 (1.6×) [152] | 96.2 (30.1× 🔴) [152] |
| level_filter | 1h | 100ms | 3.5 [145] | 6.4 (1.8×) [145] | 76.3 (21.8× 🔴) [145] |
| level_filter | 24h | 0ms | 5.2 [3399] | 20.5 (3.9× ⚠️) [3399] | 87.6 (16.8× 🔴) [3399] |
| level_filter | 24h | 100ms | 6.0 [3395] | 26.8 (4.5× ⚠️) [3395] | 92.7 (15.5× 🔴) [3395] |
| multi_filter | 1h | 0ms | 3.3 [23] | 6.0 (1.8×) [23] | 106.5 (32.3× 🔴) [23] |
| multi_filter | 1h | 100ms | 2.6 [23] | 6.3 (2.4×) [23] | 80.3 (30.9× 🔴) [23] |
| multi_filter | 24h | 0ms | 4.4 [590] | 22.1 (5.0× ⚠️) [590] | 90.2 (20.5× 🔴) [590] |
| multi_filter | 24h | 100ms | 6.9 [590] | 35.3 (5.1× ⚠️) [590] | 92.2 (13.4× 🔴) [590] |
| negation | 1h | 0ms | 2.8 [439] | 5.2 (1.9×) [439] | 81.1 (29.0× 🔴) [439] |
| negation | 1h | 100ms | 2.5 [427] | 5.3 (2.1×) [427] | 83.9 (33.6× 🔴) [427] |
| negation | 24h | 0ms | 5.3 [10329] | 20.9 (3.9× ⚠️) [10329] | 92.6 (17.5× 🔴) [10329] |
| negation | 24h | 100ms | 10.1 [10315] | 29.8 (3.0×) [10315] | 71.3 (7.1× ⚠️) [10315] |
| scan | 1h | 0ms | 4.2 [603] | 5.3 (1.3×) [603] | 93.0 (22.1× 🔴) [603] |
| scan | 1h | 100ms | 3.2 [578] | 5.9 (1.8×) [578] | 86.7 (27.1× 🔴) [578] |
| scan | 24h | 0ms | 3.9 [1000] | 25.2 (6.5× ⚠️) [1000] | 74.2 (19.0× 🔴) [1000] |
| scan | 24h | 100ms | 4.7 [1000] | 21.4 (4.6× ⚠️) [1000] | 121.0 (25.7× 🔴) [1000] |
| trace_lookup | 1h | 0ms | 2.3 [0] | 1.7 (0.7×) [0] | 81.6 (35.5× 🔴) [0] |
| trace_lookup | 1h | 100ms | 2.9 [0] | 2.3 (0.8×) [0] | 87.1 (30.0× 🔴) [0] |
| trace_lookup | 24h | 0ms | 3.9 [0] | 5.5 (1.4×) [0] | 78.7 (20.2× 🔴) [0] |
| trace_lookup | 24h | 100ms | 3.2 [0] | 5.5 (1.7×) [0] | 103.5 (32.3× 🔴) [0] |

## Traces

**Per-query median LH vs baseline:** count_by_service 3.1×, count_total 3.7×, slow_spans 4.7×, trace_by_id 2.3×

| query | range | S3 lat | baseline p95 [res] | LH | CH |
|---|---|---:|---:|---|---|
| count_by_service | 1h | 0ms | 1.8 [690] | 2.5 (1.4×) [690] | 104.6 (58.1× 🔴) [690] |
| count_by_service | 1h | 100ms | 1.8 [676] | 3.5 (1.9×) [676] | 102.8 (57.1× 🔴) [676] |
| count_by_service | 24h | 0ms | 2.0 [16018] | 11.5 (5.8× ⚠️) [16018] | 95.6 (47.8× 🔴) [16018] |
| count_by_service | 24h | 100ms | 3.3 [16008] | 13.9 (4.2× ⚠️) [16008] | 96.0 (29.1× 🔴) [16008] |
| count_total | 1h | 0ms | 1.5 [690] | 4.8 (3.2× ⚠️) [690] | 107.6 (71.7× 🔴) [690] |
| count_total | 1h | 100ms | 1.6 [676] | 3.1 (1.9×) [676] | 101.8 (63.6× 🔴) [676] |
| count_total | 24h | 0ms | 2.3 [16018] | 11.3 (4.9× ⚠️) [16018] | 104.5 (45.4× 🔴) [16018] |
| count_total | 24h | 100ms | 2.8 [16008] | 11.6 (4.1× ⚠️) [16008] | 188.5 (67.3× 🔴) [16008] |
| scan | 1h | 0ms | 1.8 [0] | ✗ result 690 vs base 0 | ✗ result 20091000000 vs base 0 |
| scan | 1h | 100ms | 1.4 [0] | ✗ result 672 vs base 0 | ✗ result 19481000000 vs base 0 |
| scan | 24h | 0ms | 1.2 [0] | ✗ result 1000 vs base 0 | ✗ result 28613000000 vs base 0 |
| scan | 24h | 100ms | 3.1 [0] | ✗ result 1000 vs base 0 | ✗ result 28899000000 vs base 0 |
| service_filter | 1h | 0ms | 1.0 [0] | ✗ result 140 vs base 0 | ✗ result 140 vs base 0 |
| service_filter | 1h | 100ms | 1.6 [0] | ✗ result 138 vs base 0 | ✗ result 138 vs base 0 |
| service_filter | 24h | 0ms | 1.1 [0] | ✗ result 3254 vs base 0 | ✗ result 3254 vs base 0 |
| service_filter | 24h | 100ms | 1.6 [0] | ✗ result 3252 vs base 0 | ✗ result 3252 vs base 0 |
| slow_spans | 1h | 0ms | 1.7 [0] | 3.4 (2.0×) [0] | 116.0 (68.2× 🔴) [0] |
| slow_spans | 1h | 100ms | 1.6 [0] | 3.9 (2.4×) [0] | 89.4 (55.9× 🔴) [0] |
| slow_spans | 24h | 0ms | 1.7 [0] | 13.2 (7.8× ⚠️) [0] | 79.0 (46.5× 🔴) [0] |
| slow_spans | 24h | 100ms | 2.4 [0] | 16.8 (7.0× ⚠️) [0] | 198.4 (82.7× 🔴) [0] |
| span_name | 1h | 0ms | 1.4 [0] | ✗ result 65 vs base 0 | ✗ result 65 vs base 0 |
| span_name | 1h | 100ms | 1.1 [0] | ✗ result 62 vs base 0 | ✗ result 62 vs base 0 |
| span_name | 24h | 0ms | 1.8 [0] | ✗ result 1650 vs base 0 | ✗ result 1650 vs base 0 |
| span_name | 24h | 100ms | 1.7 [0] | ✗ result 1647 vs base 0 | ✗ result 1647 vs base 0 |
| trace_by_id | 1h | 0ms | 1.1 [0] | 2.8 (2.5×) [0] | 86.7 (78.8× 🔴) [0] |
| trace_by_id | 1h | 100ms | 3.1 [0] | 1.8 (0.6×) [0] | 108.5 (35.0× 🔴) [0] |
| trace_by_id | 24h | 0ms | 1.4 [0] | 3.6 (2.6×) [0] | 96.4 (68.9× 🔴) [0] |
| trace_by_id | 24h | 100ms | 2.0 [0] | 4.2 (2.1×) [0] | 120.0 (60.0× 🔴) [0] |

## ⚠️ Invalid cells (excluded — not comparable)

- lakehouse — traces/scan/1h/lat0ms: **result 690 vs base 0**
- clickhouse — traces/scan/1h/lat0ms: **result 20091000000 vs base 0**
- lakehouse — traces/scan/1h/lat100ms: **result 672 vs base 0**
- clickhouse — traces/scan/1h/lat100ms: **result 19481000000 vs base 0**
- lakehouse — traces/scan/24h/lat0ms: **result 1000 vs base 0**
- clickhouse — traces/scan/24h/lat0ms: **result 28613000000 vs base 0**
- lakehouse — traces/scan/24h/lat100ms: **result 1000 vs base 0**
- clickhouse — traces/scan/24h/lat100ms: **result 28899000000 vs base 0**
- lakehouse — traces/service_filter/1h/lat0ms: **result 140 vs base 0**
- clickhouse — traces/service_filter/1h/lat0ms: **result 140 vs base 0**
- lakehouse — traces/service_filter/1h/lat100ms: **result 138 vs base 0**
- clickhouse — traces/service_filter/1h/lat100ms: **result 138 vs base 0**
- lakehouse — traces/service_filter/24h/lat0ms: **result 3254 vs base 0**
- clickhouse — traces/service_filter/24h/lat0ms: **result 3254 vs base 0**
- lakehouse — traces/service_filter/24h/lat100ms: **result 3252 vs base 0**
- clickhouse — traces/service_filter/24h/lat100ms: **result 3252 vs base 0**
- lakehouse — traces/span_name/1h/lat0ms: **result 65 vs base 0**
- clickhouse — traces/span_name/1h/lat0ms: **result 65 vs base 0**
- lakehouse — traces/span_name/1h/lat100ms: **result 62 vs base 0**
- clickhouse — traces/span_name/1h/lat100ms: **result 62 vs base 0**
- lakehouse — traces/span_name/24h/lat0ms: **result 1650 vs base 0**
- clickhouse — traces/span_name/24h/lat0ms: **result 1650 vs base 0**
- lakehouse — traces/span_name/24h/lat100ms: **result 1647 vs base 0**
- clickhouse — traces/span_name/24h/lat100ms: **result 1647 vs base 0**
