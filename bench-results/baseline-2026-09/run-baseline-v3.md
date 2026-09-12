# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse

## Overall

- **60 valid LH cells, 4 invalid** (excluded). Baseline = VL/VT on disk (disk profile: local-ssd); LH + ClickHouse read the same S3 Parquet. Medians below use only fully valid cells (every system's iterations 20/20, results agreeing).
- **logs**: LH median **1.8×** baseline (p90 4.3×, best 0.5×); LH is **11× faster than ClickHouse**. (34 valid / 2 invalid)
- **traces**: LH median **1.9×** baseline (p90 4.5×, best 0.7×); LH is **19× faster than ClickHouse**. (26 valid / 2 invalid)

## Logs

**Per-query median LH vs baseline:** count_by_service 1.5×, count_total 2.5×, fulltext 3.3×, high_card 2.0×, level_filter 3.3×, multi_filter 3.8×, negation 2.4×, scan 1.1×, trace_lookup 0.7×

| query | range | S3 lat | baseline p95 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 4.9 [609] | 20/20 | 5.6 (1.1×) [609] | 20/20 | 77.1 (15.7× 🔴) [609] | 20/20 |
| count_by_service | 1h | 100ms | 3.6 [597] | 20/20 | 6.6 (1.8×) [597] | 20/20 | 89.4 (24.8× 🔴) [597] | 20/20 |
| count_by_service | 24h | 0ms | 7.0 [14285] | 20/20 | 10.0 (1.4×) [14285] | 20/20 | 76.5 (10.9× 🔴) [14285] | 20/20 |
| count_by_service | 24h | 100ms | 6.1 [14255] | 20/20 | 9.1 (1.5×) [14255] | 20/20 | 87.6 (14.4× 🔴) [14255] | 20/20 |
| count_total | 1h | 0ms | 4.9 [611] | 20/20 | 5.9 (1.2×) [609] | 20/20 | 77.1 (15.7× 🔴) [609] | 20/20 |
| count_total | 1h | 100ms | 3.8 [598] | 20/20 | 5.9 (1.6×) [598] | 20/20 | 97.5 (25.7× 🔴) [598] | 20/20 |
| count_total | 24h | 0ms | 5.1 [14285] | 20/20 | 17.2 (3.4× ⚠️) [14285] | 20/20 | 78.7 (15.4× 🔴) [14285] | 20/20 |
| count_total | 24h | 100ms | 5.0 [14255] | 20/20 | 18.6 (3.7× ⚠️) [14255] | 20/20 | 86.1 (17.2× 🔴) [14255] | 20/20 |
| fulltext | 1h | 0ms | 5.1 [49] | 20/20 | 7.4 (1.5×) [49] | 20/20 | 88.5 (17.4× 🔴) [49] | 20/20 |
| fulltext | 1h | 100ms | 3.8 [48] | 20/20 | 11.6 (3.1× ⚠️) [48] | 20/20 | 95.0 (25.0× 🔴) [48] | 20/20 |
| fulltext | 24h | 0ms | 7.1 [1217] | 20/20 | 25.4 (3.6× ⚠️) [1217] | 20/20 | 91.0 (12.8× 🔴) [1217] | 20/20 |
| fulltext | 24h | 100ms | 5.7 [1215] | 20/20 | 28.7 (5.0× ⚠️) [1215] | 20/20 | 103.7 (18.2× 🔴) [1215] | 20/20 |
| high_card | 1h | 0ms | 4.5 [606] | 20/20 | 6.2 (1.4×) [606] | 20/20 | 87.0 (19.3× 🔴) [606] | 20/20 |
| high_card | 1h | 100ms | 3.8 [596] | 20/20 | 6.3 (1.7×) [594] | 20/20 | 82.3 (21.7× 🔴) [594] | 20/20 |
| high_card | 24h | 0ms | 8.9 [14284] | 20/20 | 29.7 (3.3× ⚠️) [14284] | 20/20 | 81.5 (9.2× ⚠️) [14284] | 20/20 |
| high_card | 24h | 100ms | 11.4 [14248] | 20/20 | 27.1 (2.4×) [14248] | 20/20 | 112.9 (9.9× ⚠️) [14248] | 20/20 |
| level_filter | 1h | 0ms | 2.6 [161] | 20/20 | 7.1 (2.7×) [161] | 20/20 | 92.0 (35.4× 🔴) [161] | 20/20 |
| level_filter | 1h | 100ms | 4.6 [159] | 20/20 | 5.6 (1.2×) [159] | 20/20 | 89.5 (19.5× 🔴) [159] | 20/20 |
| level_filter | 24h | 0ms | 6.3 [3603] | 20/20 | 33.3 (5.3× ⚠️) [3603] | 20/20 | 74.5 (11.8× 🔴) [3602] | 20/20 |
| level_filter | 24h | 100ms | 6.2 [3596] | 20/20 | 23.6 (3.8× ⚠️) [3596] | 20/20 | 100.7 (16.2× 🔴) [3596] | 20/20 |
| multi_filter | 1h | 0ms | 2.2 [27] | 20/20 | 9.4 (4.3× ⚠️) [27] | 20/20 | 81.7 (37.1× 🔴) [27] | 20/20 |
| multi_filter | 1h | 100ms | 4.8 [27] | 20/20 | 6.6 (1.4×) [27] | 20/20 | 100.1 (20.9× 🔴) [27] | 20/20 |
| multi_filter | 24h | 0ms | 7.4 [710] | 20/20 | 24.6 (3.3× ⚠️) [710] | 20/20 | 104.9 (14.2× 🔴) [710] | 20/20 |
| multi_filter | 24h | 100ms | 5.5 [708] | 20/20 | 40.5 (7.4× ⚠️) [708] | 20/20 | 88.1 (16.0× 🔴) [708] | 20/20 |
| negation | 1h | 0ms | 4.1 [463] | 20/20 | 8.0 (2.0×) [463] | 20/20 | 88.1 (21.5× 🔴) [463] | 20/20 |
| negation | 1h | 100ms | 3.4 [457] | 20/20 | 6.0 (1.8×) [457] | 20/20 | 105.3 (31.0× 🔴) [457] | 20/20 |
| negation | 24h | 0ms | 7.5 [10707] | 20/20 | 21.1 (2.8×) [10707] | 20/20 | 88.2 (11.8× 🔴) [10707] | 20/20 |
| negation | 24h | 100ms | 7.3 [10682] | 20/20 | 27.4 (3.8× ⚠️) [10682] | 20/20 | 86.8 (11.9× 🔴) [10682] | 20/20 |
| scan | 1h | 0ms | 9.8 [rows=606;hash=2c52d4f57a6805db28b7f4b88629f1ef3686044887ce800268f14f6b20078191] | 20/20 | 5.9 (0.6×) [rows=606;hash=2c52d4f57a6805db28b7f4b88629f1ef3686044887ce800268f14f6b20078191] | 20/20 | 83.4 (8.5× ⚠️) [rows=606] | 20/20 |
| scan | 1h | 100ms | 3.9 [rows=594;hash=38945382074ed5217cd3894c63cc0b89dd2b855a59b5693cc952fe4feef1befb] | 20/20 | 6.4 (1.6×) [rows=594;hash=38945382074ed5217cd3894c63cc0b89dd2b855a59b5693cc952fe4feef1befb] | 20/20 | 83.7 (21.5× 🔴) [rows=594] | 20/20 |
| scan | 24h | 0ms | 3.0 [rows=1000;hash=bbdeeacc8b758e7749caeae99a7292506a2bd299516aba9c441bc4c62ef7c40a] | 1/20 | ✗ baseline-19/20 invalid: flapping (differs from cell's first valid result) | 1/20 | ✗ baseline-19/20 invalid: flapping (differs from cell's first valid result) | 20/20 |
| scan | 24h | 100ms | 3.0 [rows=1000;hash=b6bbbc756d70ac43033f60f17ca0944aa21aa29007793e0b4cb74bb346af6b9b] | 1/20 | ✗ baseline-19/20 invalid: flapping (differs from cell's first valid result) | 1/20 | ✗ baseline-19/20 invalid: flapping (differs from cell's first valid result) | 20/20 |
| trace_lookup | 1h | 0ms | 8.3 [spans=5] | 20/20 | 4.5 (0.5×) [spans=5] | 20/20 | 80.1 (9.7× ⚠️) [5] | 20/20 |
| trace_lookup | 1h | 100ms | 3.4 [spans=5] | 20/20 | 2.7 (0.8×) [spans=5] | 20/20 | 101.7 (29.9× 🔴) [5] | 20/20 |
| trace_lookup | 24h | 0ms | 5.7 [spans=5] | 20/20 | 5.2 (0.9×) [spans=5] | 20/20 | 76.9 (13.5× 🔴) [5] | 20/20 |
| trace_lookup | 24h | 100ms | 5.1 [spans=5] | 20/20 | 3.2 (0.6×) [spans=5] | 20/20 | 95.3 (18.7× 🔴) [5] | 20/20 |

## Traces

**Per-query median LH vs baseline:** count_by_service 1.7×, count_total 3.2×, scan 1.2×, service_filter 2.0×, slow_spans 1.7×, span_name 2.4×, trace_by_id 2.2×

| query | range | S3 lat | baseline p95 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| count_by_service | 1h | 0ms | 4.7 [736] | 20/20 | 5.3 (1.1×) [736] | 20/20 | 93.2 (19.8× 🔴) [736] | 20/20 |
| count_by_service | 1h | 100ms | 3.2 [718] | 20/20 | 4.4 (1.4×) [718] | 20/20 | 124.8 (39.0× 🔴) [718] | 20/20 |
| count_by_service | 24h | 0ms | 4.4 [16596] | 20/20 | 8.7 (2.0×) [16596] | 20/20 | 92.9 (21.1× 🔴) [16596] | 20/20 |
| count_by_service | 24h | 100ms | 12.3 [16578] | 20/20 | 36.7 (3.0×) [16578] | 20/20 | 698.7 (56.8× 🔴) [16578] | 20/20 |
| count_total | 1h | 0ms | 2.4 [736] | 20/20 | 4.8 (2.0×) [736] | 20/20 | 91.2 (38.0× 🔴) [736] | 20/20 |
| count_total | 1h | 100ms | 4.2 [718] | 20/20 | 4.6 (1.1×) [718] | 20/20 | 114.9 (27.4× 🔴) [718] | 20/20 |
| count_total | 24h | 0ms | 2.6 [16596] | 20/20 | 11.7 (4.5× ⚠️) [16596] | 20/20 | 98.2 (37.8× 🔴) [16596] | 20/20 |
| count_total | 24h | 100ms | 4.7 [16578] | 20/20 | 21.7 (4.6× ⚠️) [16578] | 20/20 | 389.9 (83.0× 🔴) [16578] | 20/20 |
| scan | 1h | 0ms | 5.8 [rows=736;hash=2257ee1ea4169b340a25884bd2b85806a7ce45665b3453a1d63b9ba2dac5076b] | 20/20 | 3.9 (0.7×) [rows=736;hash=2257ee1ea4169b340a25884bd2b85806a7ce45665b3453a1d63b9ba2dac5076b] | 20/20 | 107.9 (18.6× 🔴) [rows=736] | 20/20 |
| scan | 1h | 100ms | 7.0 [rows=718;hash=10bb3ac364e1632639425136572c7e03feebd3935894659b15d07de6fff62359] | 20/20 | 12.0 (1.7×) [rows=718;hash=10bb3ac364e1632639425136572c7e03feebd3935894659b15d07de6fff62359] | 20/20 | 1270.6 (181.5× 🔴) [rows=718] | 20/20 |
| scan | 24h | 0ms | 2.4 [rows=1000;hash=70053977794cdf0eee1c14dfe5201844cceb47966089f4039453e2172045d9c2] | 4/20 | ✗ baseline-16/20 invalid: flapping (differs from cell's first valid result) | 1/20 | ✗ baseline-16/20 invalid: flapping (differs from cell's first valid result) | 20/20 |
| scan | 24h | 100ms | 2.9 [rows=1000;hash=17a064c1078d288e1861e294c65f56e775f80cc448d1c7df7ae7f7193c457112] | 5/20 | ✗ baseline-15/20 invalid: flapping (differs from cell's first valid result) | 1/20 | ✗ baseline-15/20 invalid: flapping (differs from cell's first valid result) | 20/20 |
| service_filter | 1h | 0ms | 2.4 [138] | 20/20 | 4.4 (1.8×) [138] | 20/20 | 101.5 (42.3× 🔴) [138] | 20/20 |
| service_filter | 1h | 100ms | 2.2 [135] | 20/20 | 5.3 (2.4×) [135] | 20/20 | 98.2 (44.6× 🔴) [135] | 20/20 |
| service_filter | 24h | 0ms | 3.5 [3296] | 20/20 | 7.7 (2.2×) [3296] | 20/20 | 108.6 (31.0× 🔴) [3296] | 20/20 |
| service_filter | 24h | 100ms | 14.1 [3292] | 20/20 | 22.6 (1.6×) [3292] | 20/20 | 786.2 (55.8× 🔴) [3292] | 20/20 |
| slow_spans | 1h | 0ms | 3.8 [52] | 20/20 | 6.0 (1.6×) [52] | 20/20 | 98.3 (25.9× 🔴) [52] | 20/20 |
| slow_spans | 1h | 100ms | 5.0 [52] | 20/20 | 8.6 (1.7×) [52] | 20/20 | 404.3 (80.9× 🔴) [52] | 20/20 |
| slow_spans | 24h | 0ms | 2.8 [1370] | 20/20 | 9.3 (3.3× ⚠️) [1370] | 20/20 | 101.6 (36.3× 🔴) [1370] | 20/20 |
| slow_spans | 24h | 100ms | 5.7 [1365] | 20/20 | 9.8 (1.7×) [1365] | 20/20 | 122.1 (21.4× 🔴) [1365] | 20/20 |
| span_name | 1h | 0ms | 2.8 [67] | 20/20 | 4.9 (1.8×) [67] | 20/20 | 92.2 (32.9× 🔴) [67] | 20/20 |
| span_name | 1h | 100ms | 1.8 [66] | 20/20 | 5.3 (2.9×) [66] | 20/20 | 212.6 (118.1× 🔴) [66] | 20/20 |
| span_name | 24h | 0ms | 3.8 [1676] | 20/20 | 7.2 (1.9×) [1676] | 20/20 | 115.2 (30.3× 🔴) [1676] | 20/20 |
| span_name | 24h | 100ms | 1.8 [1670] | 20/20 | 8.4 (4.7× ⚠️) [1670] | 20/20 | 138.4 (76.9× 🔴) [1670] | 20/20 |
| trace_by_id | 1h | 0ms | 1.5 [spans=4] | 20/20 | 3.6 (2.4×) [spans=4] | 20/20 | 108.6 (72.4× 🔴) [spans=4] | 20/20 |
| trace_by_id | 1h | 100ms | 2.5 [spans=4] | 20/20 | 5.0 (2.0×) [spans=4] | 20/20 | 109.9 (44.0× 🔴) [spans=4] | 20/20 |
| trace_by_id | 24h | 0ms | 1.8 [spans=4] | 20/20 | 5.0 (2.8×) [spans=4] | 20/20 | 128.1 (71.2× 🔴) [spans=4] | 20/20 |
| trace_by_id | 24h | 100ms | 2.5 [spans=4] | 20/20 | 3.8 (1.5×) [spans=4] | 20/20 | 121.2 (48.5× 🔴) [spans=4] | 20/20 |

## ⚠️ Invalid cells (excluded — not comparable)

- lakehouse — logs/scan/24h/lat0ms: **baseline-19/20 invalid: flapping (differs from cell's first valid result)**
- clickhouse — logs/scan/24h/lat0ms: **baseline-19/20 invalid: flapping (differs from cell's first valid result)**
- lakehouse — logs/scan/24h/lat100ms: **baseline-19/20 invalid: flapping (differs from cell's first valid result)**
- clickhouse — logs/scan/24h/lat100ms: **baseline-19/20 invalid: flapping (differs from cell's first valid result)**
- lakehouse — traces/scan/24h/lat0ms: **baseline-16/20 invalid: flapping (differs from cell's first valid result)**
- clickhouse — traces/scan/24h/lat0ms: **baseline-16/20 invalid: flapping (differs from cell's first valid result)**
- lakehouse — traces/scan/24h/lat100ms: **baseline-15/20 invalid: flapping (differs from cell's first valid result)**
- clickhouse — traces/scan/24h/lat100ms: **baseline-15/20 invalid: flapping (differs from cell's first valid result)**
