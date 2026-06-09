# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse

p95 latency (ms), with **result-validity checks**. Baseline = VL/VT on disk (gp3-simulated under `--disk-profile gp3-loop`). LH and ClickHouse read the **same Parquet** on S3 behind the latency proxy. Each cell shows `p95 (×base) [result]`; **✗ = invalid** (errored / 0 bytes / result diverges >5% from baseline — latency NOT comparable).

| signal | query | range | S3 lat | baseline p95 [res] | LH | CH |
|---|---|---|---:|---:|---|---|
| logs | count_by_service | 15m | 0ms | 2.8 [120] | 4.3 (1.5×) [119] | 93.9 (33.5× 🔴) [119] |
| logs | count_by_service | 15m | 100ms | 2.2 [102] | 2.4 (1.1×) [102] | 97.7 (44.4× 🔴) [102] |
| logs | count_by_service | 15m | 300ms | 3.0 [98] | 4.0 (1.3×) [98] | 162.9 (54.3× 🔴) [98] |
| logs | count_by_service | 1h | 0ms | 2.4 [604] | 5.2 (2.2×) [604] | 120.4 (50.2× 🔴) [604] |
| logs | count_by_service | 1h | 100ms | 4.0 [591] | 5.1 (1.3×) [591] | 107.8 (26.9× 🔴) [591] |
| logs | count_by_service | 1h | 300ms | 4.2 [577] | 5.4 (1.3×) [577] | 120.3 (28.6× 🔴) [577] |
| logs | count_by_service | 24h | 0ms | 9.5 [14299] | 13.4 (1.4×) [14299] | 109.0 (11.5× 🔴) [14299] |
| logs | count_by_service | 24h | 100ms | 7.4 [14296] | 21.9 (3.0×) [14296] | 133.0 (18.0× 🔴) [14296] |
| logs | count_by_service | 24h | 300ms | 7.0 [14284] | 12.5 (1.8×) [14284] | 101.5 (14.5× 🔴) [14284] |
| logs | count_by_service | 6h | 0ms | 3.2 [3620] | 6.8 (2.1×) [3620] | 120.3 (37.6× 🔴) [3620] |
| logs | count_by_service | 6h | 100ms | 5.4 [3609] | 8.9 (1.6×) [3609] | 106.0 (19.6× 🔴) [3609] |
| logs | count_by_service | 6h | 300ms | 5.0 [3601] | 6.6 (1.3×) [3601] | 99.2 (19.8× 🔴) [3601] |
| logs | count_total | 15m | 0ms | 2.2 [125] | 4.0 (1.8×) [125] | 98.8 (44.9× 🔴) [125] |
| logs | count_total | 15m | 100ms | 3.1 [103] | 3.1 (1.0×) [103] | 119.5 (38.5× 🔴) [103] |
| logs | count_total | 15m | 300ms | 2.3 [98] | 3.9 (1.7×) [98] | 130.3 (56.7× 🔴) [98] |
| logs | count_total | 1h | 0ms | 2.0 [605] | 4.8 (2.4×) [605] | 109.3 (54.6× 🔴) [605] |
| logs | count_total | 1h | 100ms | 4.8 [591] | 6.1 (1.3×) [591] | 116.3 (24.2× 🔴) [591] |
| logs | count_total | 1h | 300ms | 4.9 [577] | 4.3 (0.9×) [577] | 122.0 (24.9× 🔴) [577] |
| logs | count_total | 24h | 0ms | 5.0 [14299] | 18.0 (3.6× ⚠️) [14299] | 95.3 (19.1× 🔴) [14299] |
| logs | count_total | 24h | 100ms | 5.7 [14297] | 27.3 (4.8× ⚠️) [14297] | 96.2 (16.9× 🔴) [14297] |
| logs | count_total | 24h | 300ms | 6.0 [14284] | 10.6 (1.8×) [14284] | 114.0 (19.0× 🔴) [14284] |
| logs | count_total | 6h | 0ms | 5.7 [3620] | 6.9 (1.2×) [3620] | 113.7 (19.9× 🔴) [3620] |
| logs | count_total | 6h | 100ms | 5.9 [3611] | 5.8 (1.0×) [3611] | 113.7 (19.3× 🔴) [3611] |
| logs | count_total | 6h | 300ms | 4.3 [3601] | 6.5 (1.5×) [3601] | 104.1 (24.2× 🔴) [3601] |
| logs | fulltext | 15m | 0ms | 2.2 [6] | ✗ result 0 vs base 6 | 126.5 (57.5× 🔴) [6] |
| logs | fulltext | 15m | 100ms | 3.4 [4] | ✗ result 0 vs base 4 | 104.8 (30.8× 🔴) [4] |
| logs | fulltext | 15m | 300ms | 3.2 [4] | ✗ result 0 vs base 4 | 121.7 (38.0× 🔴) [4] |
| logs | fulltext | 1h | 0ms | 2.7 [51] | ✗ result 0 vs base 51 | 112.9 (41.8× 🔴) [51] |
| logs | fulltext | 1h | 100ms | 4.4 [50] | ✗ result 0 vs base 50 | 116.6 (26.5× 🔴) [50] |
| logs | fulltext | 1h | 300ms | 3.5 [49] | ✗ result 0 vs base 49 | 94.5 (27.0× 🔴) [49] |
| logs | fulltext | 24h | 0ms | 7.2 [1168] | ✗ result 0 vs base 1168 | 94.3 (13.1× 🔴) [1168] |
| logs | fulltext | 24h | 100ms | 8.2 [1168] | ✗ result 0 vs base 1168 | 102.3 (12.5× 🔴) [1168] |
| logs | fulltext | 24h | 300ms | 7.4 [1167] | ✗ result 0 vs base 1167 | 107.2 (14.5× 🔴) [1167] |
| logs | fulltext | 6h | 0ms | 5.6 [289] | ✗ result 0 vs base 289 | 123.8 (22.1× 🔴) [289] |
| logs | fulltext | 6h | 100ms | 10.3 [289] | ✗ result 0 vs base 289 | 100.0 (9.7× ⚠️) [289] |
| logs | fulltext | 6h | 300ms | 6.0 [288] | ✗ result 0 vs base 288 | 108.5 (18.1× 🔴) [288] |
| logs | level_filter | 15m | 0ms | 2.8 [28] | 3.7 (1.3×) [28] | 93.5 (33.4× 🔴) [28] |
| logs | level_filter | 15m | 100ms | 2.3 [24] | 3.9 (1.7×) [24] | 128.8 (56.0× 🔴) [24] |
| logs | level_filter | 15m | 300ms | 1.9 [23] | 5.3 (2.8×) [23] | 120.8 (63.6× 🔴) [23] |
| logs | level_filter | 1h | 0ms | 2.3 [153] | 4.4 (1.9×) [153] | 108.8 (47.3× 🔴) [153] |
| logs | level_filter | 1h | 100ms | 4.5 [153] | 5.8 (1.3×) [153] | 112.9 (25.1× 🔴) [153] |
| logs | level_filter | 1h | 300ms | 4.0 [150] | 4.8 (1.2×) [150] | 93.0 (23.2× 🔴) [150] |
| logs | level_filter | 24h | 0ms | 6.4 [3495] | 13.4 (2.1×) [3495] | 110.6 (17.3× 🔴) [3495] |
| logs | level_filter | 24h | 100ms | 6.3 [3494] | 16.1 (2.6×) [3494] | 99.4 (15.8× 🔴) [3494] |
| logs | level_filter | 24h | 300ms | 6.7 [3490] | 13.9 (2.1×) [3490] | 111.3 (16.6× 🔴) [3490] |
| logs | level_filter | 6h | 0ms | 4.5 [878] | 8.5 (1.9×) [878] | 94.0 (20.9× 🔴) [878] |
| logs | level_filter | 6h | 100ms | 5.2 [874] | 7.2 (1.4×) [874] | 117.8 (22.7× 🔴) [874] |
| logs | level_filter | 6h | 300ms | 5.7 [874] | 7.5 (1.3×) [874] | 111.2 (19.5× 🔴) [874] |
| traces | count_by_service | 15m | 0ms | 1.6 [134] | 3.2 (2.0×) [134] | 121.8 (76.1× 🔴) [134] |
| traces | count_by_service | 15m | 100ms | 2.0 [116] | 4.4 (2.2×) [116] | 121.4 (60.7× 🔴) [116] |
| traces | count_by_service | 15m | 300ms | 1.5 [100] | 3.3 (2.2×) [100] | 134.5 (89.7× 🔴) [100] |
| traces | count_by_service | 1h | 0ms | 1.7 [698] | 2.9 (1.7×) [698] | 134.8 (79.3× 🔴) [698] |
| traces | count_by_service | 1h | 100ms | 1.9 [684] | 2.9 (1.5×) [684] | 115.5 (60.8× 🔴) [684] |
| traces | count_by_service | 1h | 300ms | 1.5 [660] | 5.4 (3.6× ⚠️) [660] | 134.9 (89.9× 🔴) [660] |
| traces | count_by_service | 24h | 0ms | 2.6 [16726] | 20.0 (7.7× ⚠️) [16726] | 121.5 (46.7× 🔴) [16726] |
| traces | count_by_service | 24h | 100ms | 2.8 [16698] | 9.0 (3.2× ⚠️) [16698] | 162.4 (58.0× 🔴) [16698] |
| traces | count_by_service | 24h | 300ms | 3.0 [16678] | 9.2 (3.1× ⚠️) [16678] | 146.5 (48.8× 🔴) [16678] |
| traces | count_by_service | 6h | 0ms | 1.6 [4210] | 6.8 (4.2× ⚠️) [4210] | 132.3 (82.7× 🔴) [4210] |
| traces | count_by_service | 6h | 100ms | 4.7 [4190] | 5.4 (1.1×) [4190] | 161.7 (34.4× 🔴) [4190] |
| traces | count_by_service | 6h | 300ms | 1.8 [4184] | 7.1 (3.9× ⚠️) [4184] | 123.6 (68.7× 🔴) [4184] |
| traces | count_total | 15m | 0ms | 2.4 [134] | 2.1 (0.9×) [134] | 121.0 (50.4× 🔴) [134] |
| traces | count_total | 15m | 100ms | 1.8 [116] | 3.5 (1.9×) [116] | 156.6 (87.0× 🔴) [116] |
| traces | count_total | 15m | 300ms | 1.6 [100] | 2.3 (1.4×) [100] | 131.8 (82.4× 🔴) [100] |
| traces | count_total | 1h | 0ms | 1.8 [698] | 2.3 (1.3×) [698] | 112.4 (62.4× 🔴) [698] |
| traces | count_total | 1h | 100ms | 3.9 [684] | 2.6 (0.7×) [684] | 128.0 (32.8× 🔴) [684] |
| traces | count_total | 1h | 300ms | 1.5 [670] | 1.8 (1.2×) [670] | 123.4 (82.3× 🔴) [670] |
| traces | count_total | 24h | 0ms | 2.3 [16726] | 12.4 (5.4× ⚠️) [16726] | 158.9 (69.1× 🔴) [16726] |
| traces | count_total | 24h | 100ms | 2.7 [16698] | 9.8 (3.6× ⚠️) [16698] | 162.7 (60.3× 🔴) [16698] |
| traces | count_total | 24h | 300ms | 2.7 [16690] | 8.9 (3.3× ⚠️) [16690] | 131.8 (48.8× 🔴) [16690] |
| traces | count_total | 6h | 0ms | 1.8 [4210] | 5.5 (3.1× ⚠️) [4210] | 117.6 (65.3× 🔴) [4210] |
| traces | count_total | 6h | 100ms | 3.2 [4190] | 5.4 (1.7×) [4190] | 128.8 (40.2× 🔴) [4190] |
| traces | count_total | 6h | 300ms | 1.6 [4184] | 5.0 (3.1× ⚠️) [4184] | 132.2 (82.6× 🔴) [4184] |
| traces | service_filter | 15m | 0ms | 1.7 [0] | 1.1 (0.6×) [0] | 138.5 (81.5× 🔴) [34] |
| traces | service_filter | 15m | 100ms | 1.0 [0] | 4.4 (4.4× ⚠️) [0] | 126.1 (126.1× 🔴) [28] |
| traces | service_filter | 15m | 300ms | 1.3 [0] | 2.4 (1.8×) [0] | 196.1 (150.8× 🔴) [23] |
| traces | service_filter | 1h | 0ms | 1.3 [0] | 1.4 (1.1×) [0] | 128.7 (99.0× 🔴) [144] |
| traces | service_filter | 1h | 100ms | 1.7 [0] | 1.5 (0.9×) [0] | 131.3 (77.2× 🔴) [140] |
| traces | service_filter | 1h | 300ms | 1.3 [0] | 1.7 (1.3×) [0] | 127.8 (98.3× 🔴) [140] |
| traces | service_filter | 24h | 0ms | 1.1 [0] | 1.4 (1.3×) [0] | 130.5 (118.6× 🔴) [3381] |
| traces | service_filter | 24h | 100ms | 1.3 [0] | 1.5 (1.2×) [0] | 131.8 (101.4× 🔴) [3372] |
| traces | service_filter | 24h | 300ms | 1.2 [0] | 2.2 (1.8×) [0] | 136.8 (114.0× 🔴) [3367] |
| traces | service_filter | 6h | 0ms | 1.0 [0] | 5.6 (5.6× ⚠️) [0] | 115.6 (115.6× 🔴) [865] |
| traces | service_filter | 6h | 100ms | 1.2 [0] | 3.2 (2.7×) [0] | 172.9 (144.1× 🔴) [859] |
| traces | service_filter | 6h | 300ms | 1.0 [0] | 1.1 (1.1×) [0] | 142.9 (142.9× 🔴) [857] |

## Where LH lags the baseline most (valid cells only)

- **7.7×** — traces/count_by_service/24h/0: LH 20.0ms vs baseline 2.6ms
- **5.6×** — traces/service_filter/6h/0: LH 5.6ms vs baseline 1.0ms
- **5.4×** — traces/count_total/24h/0: LH 12.4ms vs baseline 2.3ms
- **4.8×** — logs/count_total/24h/100: LH 27.3ms vs baseline 5.7ms
- **4.4×** — traces/service_filter/15m/100: LH 4.4ms vs baseline 1.0ms
- **4.2×** — traces/count_by_service/6h/0: LH 6.8ms vs baseline 1.6ms
- **3.9×** — traces/count_by_service/6h/300: LH 7.1ms vs baseline 1.8ms
- **3.6×** — traces/count_total/24h/100: LH 9.8ms vs baseline 2.7ms

## ⚠️ Invalid cells (NOT comparable — fix before trusting)

- lakehouse — logs/fulltext/15m/lat0ms: **result 0 vs base 6**
- lakehouse — logs/fulltext/15m/lat100ms: **result 0 vs base 4**
- lakehouse — logs/fulltext/15m/lat300ms: **result 0 vs base 4**
- lakehouse — logs/fulltext/1h/lat0ms: **result 0 vs base 51**
- lakehouse — logs/fulltext/1h/lat100ms: **result 0 vs base 50**
- lakehouse — logs/fulltext/1h/lat300ms: **result 0 vs base 49**
- lakehouse — logs/fulltext/24h/lat0ms: **result 0 vs base 1168**
- lakehouse — logs/fulltext/24h/lat100ms: **result 0 vs base 1168**
- lakehouse — logs/fulltext/24h/lat300ms: **result 0 vs base 1167**
- lakehouse — logs/fulltext/6h/lat0ms: **result 0 vs base 289**
- lakehouse — logs/fulltext/6h/lat100ms: **result 0 vs base 289**
- lakehouse — logs/fulltext/6h/lat300ms: **result 0 vs base 288**
