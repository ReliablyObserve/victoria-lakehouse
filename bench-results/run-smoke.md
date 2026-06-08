# Cold-tier benchmark — LH vs VL/VT (baseline) vs ClickHouse

p95 latency (ms). **Baseline = VL/VT on disk** (gp3-simulated when run with `--disk-profile gp3-loop`). `×N` = slower than baseline; ⚠️ ≥3×, 🔴 ≥10×. LH and ClickHouse read the **same Parquet** on S3 behind the latency proxy.

| signal | query | range | S3 lat | baseline p95 | LH p95 (×base) | CH p95 (×base) |
|---|---|---|---:|---:|---:|---:|
| logs | count_by_service | 1h | 0ms | 2.0 | 2.6 (1.3×) | — (—) |
| logs | count_total | 1h | 0ms | 2.1 | 2.9 (1.4×) | — (—) |
| logs | fulltext | 1h | 0ms | 2.1 | 2.7 (1.3×) | — (—) |

## Where LH lags the baseline most

- **1.4×** — logs/count_total/1h/0: LH 2.9ms vs baseline 2.1ms
- **1.3×** — logs/count_by_service/1h/0: LH 2.6ms vs baseline 2.0ms
- **1.3×** — logs/fulltext/1h/0: LH 2.7ms vs baseline 2.1ms
