## Executive summary

**Where LH stands.** LH is a 5-month-old, pre-1.0, single-maintainer open-source cold tier. It puts the VictoriaLogs/VictoriaTraces ingest and query surface on plain Parquet files in S3. It has **no production users** (owner statement, 2026-09-23) and 4 GitHub stars. Its own docs say it is **not ready above ~50 TB/day**. On maturity, durability defaults and cold-path correctness it is behind every established system in this survey.

**What is unique.** No other system in the survey combines all four of these:
1. **The widest open interface set among object-storage-native stores** (see the Interfaces section). It takes 8 native write protocols and serves 3 native read APIs: LogsQL, the Jaeger query API, and the Tempo HTTP API with a TraceQL subset. The Loki API is a fourth, through the separate loki-vl-proxy. All of these are the upstream VL/VT handlers mounted unchanged.
2. **The stored files are the product.** They are plain Parquet with hive `dt=`/`hour=` paths, read in place with no export step:
   - pyarrow and DuckDB, proven by a CI readback gate;
   - ClickHouse, checked on every benchmark run against LogsQL answers.
   No other object-storage-native store in the survey names even one external engine for its primary files, except the Iceberg entrants.
3. **Apache-2.0 with no enterprise gate.** OpenObserve, Parseable, Loki and Tempo are AGPL, and their HA, multi-tenancy, RBAC or live tail sit in paid tiers.
4. **A small footprint:** 2 binaries × 2 roles + S3, with no Kafka, Keeper, Postgres or NATS.

**Where LH loses** (all confirmed in the LH repository):
- **Durability.** The default ack means "buffered in pod memory", and that window is lost on a crash. The durable logstore engine is opt-in and still in progress. `insert.ack_mode` is accepted but read by no binary.
- **Correctness.** Cold answers differ from hot VL/VT in known query shapes: #319, #318, #289, #324 and the parity known-failure list B2/B3/B4/B7.
- **Multi-replica.** Select replicas compact and duplicate rows 2-4× (#311). Several instances on one bucket each compact the same data (#290). pmeta for every tenant lands under tenant 0's prefix (#307).
- **Compaction.** It works only within an hour. The median file is ~71 KB, yet the stats report "healthy" (#281). Compaction v2 is only designed.
- **Cold latency.** Trace by ID takes ~11-12 s (#294). The first query after a restart takes 45-51 s (#308). The mixed load gate fails (#320). The best word-filter result is still about **7% slower than ClickHouse at 4 CPUs** (366 vs 341 ms; measured, in unmerged draft PR #310, on an earlier head and not re-run).
- **Feature gaps.** No metrics signal, no full-text inverted index, no Iceberg/Delta catalog, live tail only on hot data, Traces Drilldown unverified, no managed offering.

**Main competitive threats.**
1. **VictoriaLogs native object storage.** Issue #48 is open, and PR #1155 (a draft since 2026-03-05) adds a read-only offload of retained partitions to S3. If it lands, VL users get cheap retention without a second system. LH would keep these differentiators: open Parquet with external readers, and Jaeger/Tempo on cold traces.
2. **Iceberg-based entrants.**
   - Observe by Snowflake writes Iceberg v3 tables to the customer's S3 behind a REST catalog; it names Spark, DuckDB, Trino and PyIceberg; private preview.
   - CloudWatch Logs exposes S3 Tables in Iceberg at no extra storage charge.
   - IceGate is an Apache-2.0 prototype with the Loki and Tempo APIs on top of Iceberg.
   All three offer catalog discovery and snapshot isolation; LH has neither.
3. **OpenObserve's maturity and traction.** 22k stars, ~159 contributors, Parquet on S3, all three signals plus RUM, a Tantivy full-text index, compaction up to 2 GB and a managed Cloud. It loses to LH on read interfaces (SQL and PromQL only) and on its license.
4. **Grafana Tempo 3.x for traces.** A documented Parquet schema on S3, full TraceQL and TraceQL metrics, writes acked by Kafka, native Grafana Drilldown and a managed Cloud.
5. **ClickHouse/ClickStack.** Its text index is GA, it has Netflix-scale references, and in LH's own cost model a single-replica ClickHouse is cheaper at long retention.

**Bottom line.** LH's defensible position is narrow and real: **open-interface, open-file, Apache-2.0 cold storage for teams already on the Victoria stack and on Grafana.** It is not yet a credible production choice against any mature system. The ranked fix list in the fix list lists what would change that.

---
