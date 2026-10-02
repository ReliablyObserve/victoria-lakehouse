## LH position by competitor category

A verdict is win, lose or tie for a buyer choosing **today**, with the reasons. Planned items (📐) earn no credit.

### Object-storage-native, open-format stores
| Competitor | Verdict | Why LH wins | Why LH loses |
|---|---|---|---|
| OpenObserve | **Lose** | Read interfaces: LH has LogsQL, Jaeger, Tempo and Loki-via-proxy; OO has SQL and PromQL only. External readers proven in CI, where OO makes only a generic claim. Apache-2.0 with no 50 GB/day gate. No Postgres/NATS. | Maturity (22k stars, ~159 contributors, Cloud). Three signals + RUM. Tantivy full-text index. Compaction to 2 GB. Durable-ish ack in its Helm defaults (fsync on) vs LH's RAM-buffer default. |
| Parseable | **Tie** | Read APIs: Parseable has SQL only. Multi-tenancy and HA in OSS, where Parseable gates them. Compaction exists, though 🐞, while Parseable's OSS code has none. Apache-2.0 vs AGPL. | Parseable is older (2022), has a Cloud, and a cleaner hive layout down to the minute. Both have weak ack durability. |
| Grafana Tempo 3.x (traces) | **Lose** for traces-only buyers | One design for logs and traces. A native Jaeger API. External reads proven. No Kafka in the stack. | Full TraceQL + metrics GA. Writes acked by Kafka. Trace by ID is not the 11-12 s of LH's #294. Grafana-native Drilldown. Mature with a managed Cloud. |
| Quickwit | **Lose / tie** | Open files: Quickwit's tantivy splits are not readable externally. Loki/Tempo-style APIs. Ownership risk on Quickwit: Datadog-owned, after a 25-month release gap. | A full inverted index. A Binance 100 PB reference (vendor). A mature ES-compatible API. |
| GreptimeDB | **Tie** | Loki/Tempo/Jaeger-level query compatibility; Greptime has no LogQL and its traces are experimental. External reads proven. | Metrics. WAL-before-ack durability. Inverted and full-text indexes. Older project (2022). |
| IceGate | **Win today, rising threat** | Breadth and relative maturity; IceGate is a 37-star prototype. | An Iceberg catalog, a WAL on S3, and Loki + Tempo APIs. It is the design LH may be compared against once it matures. |
| Observe by Snowflake (Iceberg) | **Different market**; threat on the "open data" argument | Self-hosted, Apache-2.0, query APIs on the same files. | Iceberg v3 + REST catalog. A full commercial UI. Snowflake backing. (Still a private preview.) |
| Coralogix / Cribl Lake | **Different market** | Typed Parquet columns; Coralogix's archive holds three JSON strings and Cribl's default is JSON. Open source. | Full SaaS products with archives in the customer's own bucket. |

### Victoria upstream (VictoriaLogs / VictoriaTraces)
**Complement today, potential substitute tomorrow.**
- LH adds what upstream lacks: S3 retention, open Parquet, and tiering mirrored per tenant [U].
- LH loses on everything else: hot latency, ops simplicity, ack durability (upstream writes local disk parts within seconds), and answer correctness (the cold divergences).
- **PR #1155** (draft) would give VL read-only S3 offload. LH then keeps three things: open Parquet readable by other engines, Jaeger/Tempo on cold traces, and the tenant-aware layout.

### Grafana Loki / Tempo (self-hosted)
**Lose overall, win on files and footprint.**
- Loki and Tempo win on ecosystem, Drilldown apps, RF/Kafka durability, years of production use and managed Cloud.
- LH wins on:
  - open Parquet logs (Loki chunks and DOBJ are proprietary);
  - no Kafka (Tempo 3.x microservices need it);
  - the cost model at 10 TB/day over 3 years with the same lifecycle: $10.4k vs $34.3k per month. That figure is **computed** with an **assumed** LH CPU, not measured. Loki/Tempo were never benchmarked side by side.

### Elasticsearch / OpenSearch
**Lose on search, win on storage shape.**
- ES/OpenSearch win: inverted-index full-text, mature UIs, SIEM.
- LH wins on:
  - one S3 copy without a JVM cluster or warm-node cache;
  - open files (Lucene is not readable);
  - the license: Elastic's frozen tier is 🔒 Enterprise.

### ClickHouse family (ClickHouse, ClickStack, SigNoz, Uptrace, gigapipe, Hydrolix)
**Lose on engine and scale proof, win on compatibility and open files.**
- ClickHouse wins:
  - a GA text index;
  - Netflix and Super Bowl scale references (vendor);
  - in LH's own cost model, a single-replica ClickHouse is cheaper at long retention (it compresses ~12× vs LH's 6.1×/9.4×);
  - LH's best 4-CPU word-filter result still trails by ~7% (measured, unmerged draft PR #310).
- LH wins:
  - Loki/Jaeger/Tempo-compatible APIs with no proxy rewrite of queries into SQL; only gigapipe offers similar compatibility on ClickHouse;
  - the primary store is readable in place, while MergeTree parts are not and ClickHouse Parquet/Iceberg is an export copy;
  - no Keeper.

### Commercial SaaS (Datadog, Splunk, New Relic, Honeycomb, Dynatrace, Chronosphere, Sumo, Grafana Cloud, CloudWatch, GCP, Azure, Axiom)
**Not a like-for-like product comparison.**
- LH wins on: data ownership, open files, no per-GB ingest fee (list prices run from $0.07/GB at Elastic Serverless to $0.50/GB at CloudWatch Standard; Datadog adds $1.70 per million indexed events at 15 days), and portability.
- LH loses on: everything operational (managed service, alerting, UI breadth, support, SLAs).
- CloudWatch S3 Tables (Iceberg) and Datadog BYOC Logs now erode the "own your data" argument inside those ecosystems.

---
