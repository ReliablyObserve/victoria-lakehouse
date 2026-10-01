---
title: Cost Comparison
sidebar_position: 17
---

# Victoria Lakehouse vs Loki and Tempo

The numbers come from the cost model in [Cost Estimates](cost-estimates.md) (`scripts/cost/`), which prices every system as a production HA deployment on AWS list prices. Logs and traces together, 60/40, at 1 query per second.

## Monthly cost

| Ingest/day, retention | Lakehouse | Loki + Tempo | Lakehouse saves |
|---|---:|---:|---:|
| 0.1 TB, 30 days | $137 | $731 | 81% |
| 0.1 TB, 1 year | $246 | $909 | 73% |
| 1 TB, 30 days | $367 | $1,315 | 72% |
| 1 TB, 1 year | $1,453 | $3,088 | 53% |
| 10 TB, 30 days | $3,646 | $23,435 | 84% |
| 10 TB, 1 year | $14,504 | $41,157 | 65% |
| 100 TB, 1 year | $142k (projected, not ready at this scale) | $445k | 68% |

**Lakehouse CPU is assumed** (the same per-MB/s rule ClickHouse publishes for its own sizing) until it is measured. Loki and Tempo sizing comes from Grafana's published sizing tiers and guides, which are generous baselines; a well-tuned deployment can be leaner.

## Where the difference comes from

| | Lakehouse | Loki + Tempo |
|---|---|---|
| Copies on S3 | one | one |
| Ingest path | stateless insert nodes, durable local buffer, one Parquet write per flush | Loki ingesters with replication factor 3; Tempo 3.x behind Kafka (RF3) |
| Compression | logs 6.1x, traces 9.4x (measured, ZSTD Parquet) | logs about 4x, traces about 5x (typical; not measured here) |
| Storage format | plain Apache Parquet, readable by DuckDB, ClickHouse, Spark, Trino, pyarrow | Loki chunks (Loki API only); Tempo vParquet blocks |
| Query languages | LogsQL, LogQL (through loki-vl-proxy), Jaeger, Tempo/TraceQL, SQL on the files | LogQL, TraceQL |

## Performance

Lakehouse vs Loki and Tempo has **not been benchmarked yet**. The comparison that exists is for VictoriaLogs behind loki-vl-proxy against Loki: on 36 Grafana Logs Drilldown queries over 8 M log entries, Loki returned data for 9 and silently empty results for 13, while the proxy over VictoriaLogs returned data for 35 and was 10–24× faster where both answered ([loki-vl-proxy measurements](https://github.com/ReliablyObserve/loki-vl-proxy/blob/main/docs/honest-tldr.md)). That result is for VictoriaLogs, not Lakehouse.

For Lakehouse's measured latency against VictoriaLogs, VictoriaTraces and ClickHouse, see [Performance](performance.md) and `bench-results/baseline-2026-09`.
