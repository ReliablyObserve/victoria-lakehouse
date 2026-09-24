# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse

## Overall

- **18 valid LH cells, 12 invalid** (excluded). Baseline = VL/VT on disk (disk profile: local-ssd); LH + ClickHouse read the same S3 Parquet. Medians below use only fully valid cells (every system's iterations 20/20, results agreeing).
- **logs**: LH median **2.7×** baseline (p90 3.9×, best 0.8×); LH is **2× faster than ClickHouse**. (6 valid / 12 invalid)
- **traces**: LH median **3.0×** baseline (p90 4.5×, best 0.4×); LH is **16× faster than ClickHouse**. (12 valid / 0 invalid)

## Logs

**Per-query median LH vs baseline:** streams_list 2.7×

| query | range | S3 lat | baseline p95/p90 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| fv_level | 1h | 0ms | 10.3/10.3 [rows=4;total=553;hash=a3686570] | 10/10 | ✗ same count (4), different rows (hash mismatch) | 10/10 | 515.6/515.6 (50.1× 🔴) [rows=4;total=553;hash=a3686570] | 10/10 |
| fv_level | 1h | 100ms | 2.2/2.2 [rows=4;total=536;hash=a886ec23] | 10/10 | ✗ same count (4), different rows (hash mismatch) | 10/10 | 110.8/110.8 (50.4× 🔴) [rows=4;total=536;hash=a886ec23] | 10/10 |
| fv_level | 24h | 0ms | 34.9/34.9 [rows=4;total=14160;hash=a1e53bc1] | 10/10 | ✗ same count (4), different rows (hash mismatch) | 10/10 | 492.4/492.4 (14.1× 🔴) [rows=4;total=14160;hash=a1e53bc1] | 10/10 |
| fv_level | 24h | 100ms | 5.7/5.7 [rows=4;total=14160;hash=a1e53bc1] | 10/10 | ✗ same count (4), different rows (hash mismatch) | 10/10 | 95.4/95.4 (16.7× 🔴) [rows=4;total=14160;hash=a1e53bc1] | 10/10 |
| fv_level | 6h | 0ms | 7.4/7.4 [rows=4;total=3413;hash=e4d860ce] | 10/10 | ✗ same count (4), different rows (hash mismatch) | 10/10 | 238.7/238.7 (32.3× 🔴) [rows=4;total=3413;hash=e4d860ce] | 10/10 |
| fv_level | 6h | 100ms | 8.7/8.7 [rows=4;total=3407;hash=ef803682] | 10/10 | ✗ same count (4), different rows (hash mismatch) | 10/10 | 96.9/96.9 (11.1× 🔴) [rows=4;total=3407;hash=ef803682] | 10/10 |
| fv_service | 1h | 0ms | 6.3/6.3 [rows=5;total=552;hash=6be4990d] | 10/10 | ✗ same count (5), different rows (hash mismatch) | 10/10 | 3384.8/3384.8 (537.3× 🔴) [rows=5;total=552;hash=6be4990d] | 10/10 |
| fv_service | 1h | 100ms | 4.6/4.6 [rows=5;total=536;hash=5e06f1dd] | 10/10 | ✗ same count (5), different rows (hash mismatch) | 10/10 | 98.2/98.2 (21.3× 🔴) [rows=5;total=536;hash=5e06f1dd] | 10/10 |
| fv_service | 24h | 0ms | 22.1/22.1 [rows=5;total=14160;hash=c6b7d588] | 10/10 | ✗ same count (5), different rows (hash mismatch) | 10/10 | 229.4/229.4 (10.4× 🔴) [rows=5;total=14160;hash=c6b7d588] | 10/10 |
| fv_service | 24h | 100ms | 4.8/4.8 [rows=5;total=14160;hash=c6b7d588] | 10/10 | ✗ same count (5), different rows (hash mismatch) | 10/10 | 89.5/89.5 (18.6× 🔴) [rows=5;total=14160;hash=c6b7d588] | 10/10 |
| fv_service | 6h | 0ms | 5.9/5.9 [rows=5;total=3413;hash=259467ff] | 10/10 | ✗ same count (5), different rows (hash mismatch) | 10/10 | 181.2/181.2 (30.7× 🔴) [rows=5;total=3413;hash=259467ff] | 10/10 |
| fv_service | 6h | 100ms | 3.6/3.6 [rows=5;total=3407;hash=100c60ac] | 10/10 | ✗ same count (5), different rows (hash mismatch) | 10/10 | 90.8/90.8 (25.2× 🔴) [rows=5;total=3407;hash=100c60ac] | 10/10 |
| streams_list | 1h | 0ms | 59.9/59.9 [rows=547;total=547;hash=3b872440] | 10/10 | 122.4/122.4 (2.0×) [rows=547;total=547;hash=3b872440] | 10/10 | 144.5/144.5 (2.4×) [rows=547;total=547;hash=3b872440] | 10/10 |
| streams_list | 1h | 100ms | 3.9/3.9 [rows=535;total=535;hash=092d215b] | 10/10 | 14.8/14.8 (3.8× ⚠️) [rows=535;total=535;hash=092d215b] | 10/10 | 90.2/90.2 (23.1× 🔴) [rows=535;total=535;hash=092d215b] | 10/10 |
| streams_list | 24h | 0ms | 92.3/92.3 [rows=14160;total=14160;hash=f9fb2b7b] | 10/10 | 318.5/318.5 (3.5× ⚠️) [rows=14160;total=14160;hash=f9fb2b7b] | 10/10 | 126.5/126.5 (1.4×) [rows=14160;total=14160;hash=f9fb2b7b] | 10/10 |
| streams_list | 24h | 100ms | 56.2/56.2 [rows=14160;total=14160;hash=f9fb2b7b] | 10/10 | 220.2/220.2 (3.9× ⚠️) [rows=14160;total=14160;hash=f9fb2b7b] | 10/10 | 114.1/114.1 (2.0×) [rows=14160;total=14160;hash=f9fb2b7b] | 10/10 |
| streams_list | 6h | 0ms | 97.2/97.2 [rows=3413;total=3413;hash=d5e1aa20] | 10/10 | 80.0/80.0 (0.8×) [rows=3413;total=3413;hash=d5e1aa20] | 10/10 | 316.2/316.2 (3.3× ⚠️) [rows=3413;total=3413;hash=d5e1aa20] | 10/10 |
| streams_list | 6h | 100ms | 29.6/29.6 [rows=3403;total=3403;hash=361c2b7a] | 10/10 | 45.2/45.2 (1.5×) [rows=3403;total=3403;hash=361c2b7a] | 10/10 | 93.8/93.8 (3.2× ⚠️) [rows=3403;total=3403;hash=361c2b7a] | 10/10 |

## Traces

**Per-query median LH vs baseline:** fv_name 3.8×, fv_service 1.7×

| query | range | S3 lat | baseline p95/p90 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| fv_name | 1h | 0ms | 2.0/2.0 [rows=10;total=610;hash=ed9f1add] | 10/10 | 8.7/8.7 (4.3× ⚠️) [rows=10;total=610;hash=ed9f1add] | 10/10 | 171.3/171.3 (85.7× 🔴) [rows=10;total=610;hash=ed9f1add] | 10/10 |
| fv_name | 1h | 100ms | 2.0/2.0 [rows=10;total=600;hash=7060b39e] | 10/10 | 6.4/6.4 (3.2× ⚠️) [rows=10;total=600;hash=7060b39e] | 10/10 | 107.4/107.4 (53.7× 🔴) [rows=10;total=600;hash=7060b39e] | 10/10 |
| fv_name | 24h | 0ms | 2.4/2.4 [rows=10;total=16220;hash=363511a7] | 10/10 | 10.9/10.9 (4.5× ⚠️) [rows=10;total=16220;hash=363511a7] | 10/10 | 158.7/158.7 (66.1× 🔴) [rows=10;total=16220;hash=363511a7] | 10/10 |
| fv_name | 24h | 100ms | 2.9/2.9 [rows=10;total=16220;hash=363511a7] | 10/10 | 8.9/8.9 (3.1× ⚠️) [rows=10;total=16220;hash=363511a7] | 10/10 | 121.1/121.1 (41.8× 🔴) [rows=10;total=16220;hash=363511a7] | 10/10 |
| fv_name | 6h | 0ms | 2.6/2.6 [rows=10;total=3938;hash=e623573d] | 10/10 | 12.6/12.6 (4.8× ⚠️) [rows=10;total=3938;hash=e623573d] | 10/10 | 154.9/154.9 (59.6× 🔴) [rows=10;total=3938;hash=e623573d] | 10/10 |
| fv_name | 6h | 100ms | 3.2/3.2 [rows=10;total=3924;hash=09d725d3] | 10/10 | 3.8/3.8 (1.2×) [rows=10;total=3924;hash=09d725d3] | 10/10 | 100.7/100.7 (31.5× 🔴) [rows=10;total=3924;hash=09d725d3] | 10/10 |
| fv_service | 1h | 0ms | 2.8/2.8 [rows=5;total=610;hash=50561bd2] | 10/10 | 8.2/8.2 (2.9×) [rows=5;total=610;hash=50561bd2] | 10/10 | 100.0/100.0 (35.7× 🔴) [rows=5;total=610;hash=50561bd2] | 10/10 |
| fv_service | 1h | 100ms | 3.1/3.1 [rows=5;total=606;hash=879c1d16] | 10/10 | 1.8/1.8 (0.6×) [rows=5;total=606;hash=879c1d16] | 10/10 | 105.7/105.7 (34.1× 🔴) [rows=5;total=606;hash=879c1d16] | 10/10 |
| fv_service | 24h | 0ms | 3.9/3.9 [rows=5;total=16220;hash=bd4c2fff] | 10/10 | 13.6/13.6 (3.5× ⚠️) [rows=5;total=16220;hash=bd4c2fff] | 10/10 | 184.0/184.0 (47.2× 🔴) [rows=5;total=16220;hash=bd4c2fff] | 10/10 |
| fv_service | 24h | 100ms | 3.5/3.5 [rows=5;total=16220;hash=bd4c2fff] | 10/10 | 8.8/8.8 (2.5×) [rows=5;total=16220;hash=bd4c2fff] | 10/10 | 118.9/118.9 (34.0× 🔴) [rows=5;total=16220;hash=bd4c2fff] | 10/10 |
| fv_service | 6h | 0ms | 4.6/4.6 [rows=5;total=3938;hash=226b8289] | 10/10 | 4.2/4.2 (0.9×) [rows=5;total=3938;hash=226b8289] | 10/10 | 91.0/91.0 (19.8× 🔴) [rows=5;total=3938;hash=226b8289] | 10/10 |
| fv_service | 6h | 100ms | 11.3/11.3 [rows=5;total=3924;hash=c4e8bcfb] | 10/10 | 4.2/4.2 (0.4×) [rows=5;total=3924;hash=c4e8bcfb] | 10/10 | 109.9/109.9 (9.7× ⚠️) [rows=5;total=3924;hash=c4e8bcfb] | 10/10 |

## ⚠️ Invalid cells (excluded — not comparable)

- lakehouse — logs/fv_level/1h/lat0ms: **same count (4), different rows (hash mismatch)**
- lakehouse — logs/fv_level/1h/lat100ms: **same count (4), different rows (hash mismatch)**
- lakehouse — logs/fv_level/24h/lat0ms: **same count (4), different rows (hash mismatch)**
- lakehouse — logs/fv_level/24h/lat100ms: **same count (4), different rows (hash mismatch)**
- lakehouse — logs/fv_level/6h/lat0ms: **same count (4), different rows (hash mismatch)**
- lakehouse — logs/fv_level/6h/lat100ms: **same count (4), different rows (hash mismatch)**
- lakehouse — logs/fv_service/1h/lat0ms: **same count (5), different rows (hash mismatch)**
- lakehouse — logs/fv_service/1h/lat100ms: **same count (5), different rows (hash mismatch)**
- lakehouse — logs/fv_service/24h/lat0ms: **same count (5), different rows (hash mismatch)**
- lakehouse — logs/fv_service/24h/lat100ms: **same count (5), different rows (hash mismatch)**
- lakehouse — logs/fv_service/6h/lat0ms: **same count (5), different rows (hash mismatch)**
- lakehouse — logs/fv_service/6h/lat100ms: **same count (5), different rows (hash mismatch)**
