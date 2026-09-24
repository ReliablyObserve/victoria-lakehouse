# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse

## Overall

- **36 valid LH cells, 0 invalid** (excluded). Baseline = VL/VT on disk (disk profile: local-ssd); LH + ClickHouse read the same S3 Parquet. Medians below use only fully valid cells (every system's iterations 20/20, results agreeing).
- **logs**: LH median **1.4×** baseline (p90 3.8×, best 0.5×); LH is **12× faster than ClickHouse**. (18 valid / 0 invalid)
- **traces**: LH median **3.8×** baseline (p90 8.4×, best 1.2×); LH is **9× faster than ClickHouse**. (18 valid / 0 invalid)

## Logs

**Per-query median LH vs baseline:** fv_level 1.5×, fv_service 1.6×, streams_list 1.4×

| query | range | S3 lat | baseline p95/p90 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| fv_level | 1h | 0ms | 4.8/4.8 [rows=4;total=717;hash=1d649320] | 10/10 | 5.0/5.0 (1.0×) [rows=4;total=717;hash=1d649320] | 10/10 | 82.7/82.7 (17.2× 🔴) [rows=4;total=717;hash=1d649320] | 10/10 |
| fv_level | 1h | 100ms | 3.5/3.5 [rows=4;total=709;hash=39e0ba40] | 10/10 | 7.2/7.2 (2.1×) [rows=4;total=709;hash=39e0ba40] | 10/10 | 135.8/135.8 (38.8× 🔴) [rows=4;total=709;hash=39e0ba40] | 10/10 |
| fv_level | 24h | 0ms | 6.7/6.7 [rows=4;total=14359;hash=96739310] | 10/10 | 5.6/5.6 (0.8×) [rows=4;total=14359;hash=96739310] | 10/10 | 79.2/79.2 (11.8× 🔴) [rows=4;total=14359;hash=96739310] | 10/10 |
| fv_level | 24h | 100ms | 5.9/5.9 [rows=4;total=14342;hash=1e8b055b] | 10/10 | 5.7/5.7 (1.0×) [rows=4;total=14342;hash=1e8b055b] | 10/10 | 96.7/96.7 (16.4× 🔴) [rows=4;total=14342;hash=1e8b055b] | 10/10 |
| fv_level | 6h | 0ms | 2.3/2.3 [rows=4;total=3748;hash=8abc28eb] | 10/10 | 8.8/8.8 (3.8× ⚠️) [rows=4;total=3748;hash=8abc28eb] | 10/10 | 99.0/99.0 (43.0× 🔴) [rows=4;total=3748;hash=8abc28eb] | 10/10 |
| fv_level | 6h | 100ms | 4.4/4.4 [rows=4;total=3740;hash=601969d7] | 10/10 | 15.7/15.7 (3.6× ⚠️) [rows=4;total=3740;hash=601969d7] | 10/10 | 117.0/117.0 (26.6× 🔴) [rows=4;total=3740;hash=601969d7] | 10/10 |
| fv_service | 1h | 0ms | 2.3/2.3 [rows=5;total=716;hash=43bd79e3] | 10/10 | 8.0/8.0 (3.5× ⚠️) [rows=5;total=716;hash=43bd79e3] | 10/10 | 84.0/84.0 (36.5× 🔴) [rows=5;total=716;hash=43bd79e3] | 10/10 |
| fv_service | 1h | 100ms | 2.3/2.3 [rows=5;total=708;hash=81e04c99] | 10/10 | 7.3/7.3 (3.2× ⚠️) [rows=5;total=708;hash=81e04c99] | 10/10 | 107.0/107.0 (46.5× 🔴) [rows=5;total=708;hash=81e04c99] | 10/10 |
| fv_service | 24h | 0ms | 12.7/12.7 [rows=5;total=14359;hash=1fef23a8] | 10/10 | 6.8/6.8 (0.5×) [rows=5;total=14359;hash=1fef23a8] | 10/10 | 106.3/106.3 (8.4× ⚠️) [rows=5;total=14359;hash=1fef23a8] | 10/10 |
| fv_service | 24h | 100ms | 12.2/12.2 [rows=5;total=14342;hash=9bd3d152] | 10/10 | 6.4/6.4 (0.5×) [rows=5;total=14342;hash=9bd3d152] | 10/10 | 88.6/88.6 (7.3× ⚠️) [rows=5;total=14342;hash=9bd3d152] | 10/10 |
| fv_service | 6h | 0ms | 7.1/7.1 [rows=5;total=3748;hash=4c825f24] | 10/10 | 10.6/10.6 (1.5×) [rows=5;total=3748;hash=4c825f24] | 10/10 | 80.9/80.9 (11.4× 🔴) [rows=5;total=3748;hash=4c825f24] | 10/10 |
| fv_service | 6h | 100ms | 5.7/5.7 [rows=5;total=3740;hash=3fd2c324] | 10/10 | 9.5/9.5 (1.7×) [rows=5;total=3740;hash=3fd2c324] | 10/10 | 119.2/119.2 (20.9× 🔴) [rows=5;total=3740;hash=3fd2c324] | 10/10 |
| streams_list | 1h | 0ms | 5.2/5.2 [rows=716;total=716;hash=c95847d6] | 10/10 | 7.2/7.2 (1.4×) [rows=716;total=716;hash=c95847d6] | 10/10 | 92.6/92.6 (17.8× 🔴) [rows=716;total=716;hash=c95847d6] | 10/10 |
| streams_list | 1h | 100ms | 5.8/5.8 [rows=707;total=707;hash=19d770da] | 10/10 | 28.8/28.8 (5.0× ⚠️) [rows=707;total=707;hash=19d770da] | 10/10 | 97.8/97.8 (16.9× 🔴) [rows=707;total=707;hash=19d770da] | 10/10 |
| streams_list | 24h | 0ms | 70.5/70.5 [rows=14359;total=14359;hash=21bb97e2] | 10/10 | 94.9/94.9 (1.3×) [rows=14359;total=14359;hash=21bb97e2] | 10/10 | 201.4/201.4 (2.9×) [rows=14359;total=14359;hash=21bb97e2] | 10/10 |
| streams_list | 24h | 100ms | 53.2/53.2 [rows=14342;total=14342;hash=8b02a535] | 10/10 | 63.4/63.4 (1.2×) [rows=14342;total=14342;hash=8b02a535] | 10/10 | 160.5/160.5 (3.0× ⚠️) [rows=14342;total=14342;hash=8b02a535] | 10/10 |
| streams_list | 6h | 0ms | 15.8/15.8 [rows=3748;total=3748;hash=4c0d2182] | 10/10 | 18.2/18.2 (1.2×) [rows=3748;total=3748;hash=4c0d2182] | 10/10 | 80.6/80.6 (5.1× ⚠️) [rows=3748;total=3748;hash=4c0d2182] | 10/10 |
| streams_list | 6h | 100ms | 23.7/23.7 [rows=3740;total=3740;hash=17219222] | 10/10 | 33.0/33.0 (1.4×) [rows=3740;total=3740;hash=17219222] | 10/10 | 131.3/131.3 (5.5× ⚠️) [rows=3740;total=3740;hash=17219222] | 10/10 |

## Traces

**Per-query median LH vs baseline:** fv_name 3.1×, fv_service 4.3×, streams_list 4.6×

| query | range | S3 lat | baseline p95/p90 [res] | valid | LH | valid | CH | valid |
|---|---|---:|---:|---:|---|---:|---|---:|
| fv_name | 1h | 0ms | 1.7/1.7 [rows=10;total=832;hash=c8461996] | 10/10 | 5.8/5.8 (3.4× ⚠️) [rows=10;total=832;hash=c8461996] | 10/10 | 75.2/75.2 (44.2× 🔴) [rows=10;total=832;hash=c8461996] | 10/10 |
| fv_name | 1h | 100ms | 2.0/2.0 [rows=10;total=820;hash=86063213] | 10/10 | 5.6/5.6 (2.8×) [rows=10;total=820;hash=86063213] | 10/10 | 242.6/242.6 (121.3× 🔴) [rows=10;total=820;hash=86063213] | 10/10 |
| fv_name | 24h | 0ms | 13.2/13.2 [rows=10;total=16554;hash=6390ff40] | 10/10 | 18.1/18.1 (1.4×) [rows=10;total=16554;hash=6390ff40] | 10/10 | 155.1/155.1 (11.8× 🔴) [rows=10;total=16554;hash=6390ff40] | 10/10 |
| fv_name | 24h | 100ms | 2.2/2.2 [rows=10;total=16546;hash=b6db85a7] | 10/10 | 10.9/10.9 (5.0× ⚠️) [rows=10;total=16546;hash=b6db85a7] | 10/10 | 103.2/103.2 (46.9× 🔴) [rows=10;total=16546;hash=b6db85a7] | 10/10 |
| fv_name | 6h | 0ms | 2.2/2.2 [rows=10;total=4206;hash=896a8a8b] | 10/10 | 13.4/13.4 (6.1× ⚠️) [rows=10;total=4206;hash=896a8a8b] | 10/10 | 77.1/77.1 (35.0× 🔴) [rows=10;total=4206;hash=896a8a8b] | 10/10 |
| fv_name | 6h | 100ms | 11.1/11.1 [rows=10;total=4194;hash=bab0688a] | 10/10 | 13.6/13.6 (1.2×) [rows=10;total=4194;hash=bab0688a] | 10/10 | 104.6/104.6 (9.4× ⚠️) [rows=10;total=4194;hash=bab0688a] | 10/10 |
| fv_service | 1h | 0ms | 2.2/2.2 [rows=5;total=832;hash=24134080] | 10/10 | 11.5/11.5 (5.2× ⚠️) [rows=5;total=832;hash=24134080] | 10/10 | 100.1/100.1 (45.5× 🔴) [rows=5;total=832;hash=24134080] | 10/10 |
| fv_service | 1h | 100ms | 2.3/2.3 [rows=5;total=820;hash=bcb6cee2] | 10/10 | 4.0/4.0 (1.7×) [rows=5;total=820;hash=bcb6cee2] | 10/10 | 90.9/90.9 (39.5× 🔴) [rows=5;total=820;hash=bcb6cee2] | 10/10 |
| fv_service | 24h | 0ms | 3.8/3.8 [rows=5;total=16554;hash=0f0b3b42] | 10/10 | 10.5/10.5 (2.8×) [rows=5;total=16554;hash=0f0b3b42] | 10/10 | 83.7/83.7 (22.0× 🔴) [rows=5;total=16554;hash=0f0b3b42] | 10/10 |
| fv_service | 24h | 100ms | 2.2/2.2 [rows=5;total=16546;hash=9deb4670] | 10/10 | 11.5/11.5 (5.2× ⚠️) [rows=5;total=16546;hash=9deb4670] | 10/10 | 100.5/100.5 (45.7× 🔴) [rows=5;total=16546;hash=9deb4670] | 10/10 |
| fv_service | 6h | 0ms | 2.2/2.2 [rows=5;total=4206;hash=98cfc559] | 10/10 | 7.3/7.3 (3.3× ⚠️) [rows=5;total=4206;hash=98cfc559] | 10/10 | 90.3/90.3 (41.0× 🔴) [rows=5;total=4206;hash=98cfc559] | 10/10 |
| fv_service | 6h | 100ms | 3.1/3.1 [rows=5;total=4194;hash=4f464cfc] | 10/10 | 72.6/72.6 (23.4× 🔴) [rows=5;total=4194;hash=4f464cfc] | 10/10 | 139.9/139.9 (45.1× 🔴) [rows=5;total=4194;hash=4f464cfc] | 10/10 |
| streams_list | 1h | 0ms | 3.2/3.2 [rows=50;total=832;hash=37912451] | 10/10 | 5.8/5.8 (1.8×) [rows=50;total=832;hash=37912451] | 10/10 | 94.5/94.5 (29.5× 🔴) [rows=50;total=832;hash=37912451] | 10/10 |
| streams_list | 1h | 100ms | 2.0/2.0 [rows=50;total=820;hash=cd982a02] | 10/10 | 4.7/4.7 (2.4×) [rows=50;total=820;hash=cd982a02] | 10/10 | 98.9/98.9 (49.5× 🔴) [rows=50;total=820;hash=cd982a02] | 10/10 |
| streams_list | 24h | 0ms | 2.1/2.1 [rows=50;total=16554;hash=eaaaed5e] | 10/10 | 14.0/14.0 (6.7× ⚠️) [rows=50;total=16554;hash=eaaaed5e] | 10/10 | 131.7/131.7 (62.7× 🔴) [rows=50;total=16554;hash=eaaaed5e] | 10/10 |
| streams_list | 24h | 100ms | 2.6/2.6 [rows=50;total=16546;hash=a8f9aa36] | 10/10 | 13.0/13.0 (5.0× ⚠️) [rows=50;total=16546;hash=a8f9aa36] | 10/10 | 104.9/104.9 (40.3× 🔴) [rows=50;total=16546;hash=a8f9aa36] | 10/10 |
| streams_list | 6h | 0ms | 3.0/3.0 [rows=50;total=4206;hash=77266179] | 10/10 | 25.1/25.1 (8.4× ⚠️) [rows=50;total=4206;hash=77266179] | 10/10 | 113.2/113.2 (37.7× 🔴) [rows=50;total=4206;hash=77266179] | 10/10 |
| streams_list | 6h | 100ms | 1.8/1.8 [rows=50;total=4194;hash=a3230f9c] | 10/10 | 7.6/7.6 (4.2× ⚠️) [rows=50;total=4194;hash=a3230f9c] | 10/10 | 147.1/147.1 (81.7× 🔴) [rows=50;total=4194;hash=a3230f9c] | 10/10 |
