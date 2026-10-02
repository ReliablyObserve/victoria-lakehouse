## Interfaces: write paths, read paths and direct file reads

This is LH's strongest dimension. Counting rules (applied identically to every system, each counted from its own docs):
- **Write protocols (n):** distinct ingest protocol families accepted by the product's own server endpoints: OTLP, Loki push, ES bulk, Splunk HEC, Datadog, syslog, journald, Jaeger, Zipkin, Prometheus RW, Influx LP, SQL-wire INSERT, the own/native API, and each built-in queue consumer (Kafka, Kinesis, Pulsar, SQS).
  - External collectors and separately deployed components do not count.
  - HTTP vs gRPC and JSON vs protobuf of one protocol count once.
  - Unconfirmed protocols are excluded and named.
- **Read APIs (n):** distinct query APIs served natively.
  - The product's own query language/API counts once.
  - Each compatible ecosystem API counts once: Loki, Tempo/TraceQL, Jaeger, Zipkin, ES search, Prometheus, and SQL wire or Flight SQL.
  - An API that needs a separately deployed component is shown as "(+n via …)".
  - UIs and Grafana datasources are not counted.
- **External engines reading the stored files (n of 7):** of DuckDB, ClickHouse, Spark, Trino, Athena, pyarrow and Polars, how many the vendor names (or CI proves) as reading the **stored files in place, while the system serves them, with no export step**.
  - A generic "any Parquet tool can read it" claim counts as 0 named.
  - Archives and export copies are marked separately.

### LH write paths (upstream `vlinsert` / `vtinsert`, mounted unchanged)

| Protocol | Status | Evidence (label) |
|---|---|---|
| jsonline (`/insert/jsonline`) | ✅ | e2e tests (repository audit) |
| VL native / multitenant native | ✅ route served; cold end-to-end not exercised | `UPSTREAM_COVERAGE.md` (repository audit) |
| Loki push, JSON + protobuf | ✅ | manual probe, May 2026 (`tests/verification/matrix.md`): JSON ingest + readback PASS (LI2); protobuf only a reachability check (LI3) |
| Elasticsearch `_bulk` (+ stub endpoints) | ✅ | manual probe PASS (LI4) |
| Splunk HEC | ✅ | manual probe PASS (LI8) |
| Datadog logs v2 | ✅ | manual probe PASS (LI6) |
| journald | ✅ | manual probe PASS (LI7) |
| OTLP/HTTP logs (protobuf) | ✅ route served | manual probe LI5 checked reachability only, not an ingest round trip; registry row not executed |
| OTLP/HTTP traces | ✅ | e2e tests |
| syslog (TCP/UDP) | ❓ | the binaries start no syslog listener |
| OTLP gRPC | ❓ | not in the coverage inventory |
| Jaeger / Zipkin ingest | ❌ | VT does not register them (row TI2) |
| Kafka, OTel Arrow, Prometheus RW | ❌ | not offered |

**Count: 8** (jsonline, VL native, Loki push, ES bulk, Splunk HEC, Datadog, journald, OTLP). The manual probe results are older than the VT 0.12 bump and are not in CI. The registry rows for these routes are all `pending`.

### LH read paths

| API / surface | Status | Evidence |
|---|---|---|
| VictoriaLogs LogsQL HTTP API (`/select/logsql/*`) | ✅ native, with known cold divergences (🐞 #319, #318, #289, #324) | parity suite 428+ passing, 33 known failures (repository audit) |
| VictoriaTraces Jaeger query API | ✅ native | e2e `tests/e2e/traces_jaeger_test.go` + parity |
| VictoriaTraces Tempo HTTP API + TraceQL subset + TraceQL metrics set | 🟡 VT subset, 5 documented shape differences, 🐞 #316 | repository audit |
| Loki API (LogQL) | 🟡 **only via loki-vl-proxy, a separate component and repo**; its Drilldown suite runs against VL, not yet against LH | README (repository audit) |
| Grafana datasources | ✅ stock VictoriaLogs, Jaeger, Tempo and ClickHouse datasources, no plugin; Traces Drilldown [U]; Tempo service graph empty | repository audit |
| VMUI / VTUI / LH Explorer | ✅ (🐞 #265 VTUI heatmap empty on cold) | repository audit |

**Count: 3 native (+1 via proxy).**

### LH direct file reads (no export step)

| Engine | Status | Label |
|---|---|---|
| DuckDB | ✅ documented example run against real files, every check equal to the truth the fixture computed from the rows it sent | **CI-proven** (`parquet-readers` workflow; also the `parquet-readback` gate) |
| pyarrow | ✅ same, plus the readback gate's aggregates and row-level equality | **CI-proven** |
| Spark | ✅ `spark.read.parquet` on s3a, every check equal to the truth | **CI-proven** |
| ClickHouse | 🟡 S3 table engine; an equality filter on any bloom column fails on files whose bloom filter is not a power-of-two size while bloom push down is on (#341) | **CI-proven, open issue** |
| Trino | 🟡 Hive connector; tenant IDs of 2^31 and above read negative (#342) | **CI-proven, open issue** |
| Polars | 🔴 refuses every traces file, every raw logs file and the compacted logs files that carry a body token bloom: footer metadata is not UTF-8 (#340); only some compacted logs files read | **CI-observed, not counted as verified** |
| Athena | 🟡 Trino SQL dialect; documented, not run | **claimed** |

**Count: 5 verified / 1 partial / 1 claimed.**

### Reach, and its honest limits

Because LH stores plain Parquet, every Parquet-capable tool is a potential client, with no export job and no second copy: data science notebooks (pandas, Polars, pyarrow), BI tools on Trino/Athena/Spark, security analytics, and ad-hoc SQL in DuckDB or ClickHouse. The logs/traces APIs and the analytics engines share one S3 copy.

The limits are stated plainly:
- **No Iceberg/Delta catalog.** Readers must know the layout (`…/dt=YYYY-MM-DD/hour=HH/*.parquet`) or use globs with hive partitioning. There are no snapshots, so a glob that runs during compaction can see inputs and outputs together. Compaction publishes its output before it deletes the sources (#287 "add-then-remove"), so such a glob can count rows twice (inference from #287, not tested).
- **LH-specific schema.** It uses OTLP-native column names, MAP spill for non-promoted attributes, `ded_s01..08` slot columns and tenant columns. External users query LH's schema, not the VL/VT field model. The schema is documented in `docs/open-parquet-format.md`.
- **LH's metadata helps only LH.** pmeta and the footer key-value token blooms and trace index are invisible to other engines; the Parquet SBBF blooms and row-group stats do help them. Footers can reach 2.1 MB (#303), which external readers also pay for.

**How the alternatives compare on reach:**
- **Tempo:**
  - The data is Parquet, and the schema is documented column by column.
  - But one row holds a whole trace in deeply nested groups. Dedicated columns change meaning per block (the mapping is in `meta.json`), and a block exists only once its `meta.json` does.
  - No external engine is documented, and nobody in the GitHub issues mentions DuckDB.
  - So: **readable in principle, not shown in practice** (sourced docs; not tested).
- **OpenObserve:**
  - The data is Parquet (zstd), but the live file list is in **Postgres**, and paths are `yyyy/MM/dd/HH` without `key=value` partitions.
  - The schema evolves per stream, and an optional per-stream Vortex format is not Parquet.
  - Compaction deletes inputs only after a 120-minute delay (`ZO_COMPACT_DELETE_FILES_DELAY_MINUTES`), so a bucket glob can double-count during that window (inference from config, not tested).
  - The vendor says "any tool that reads Parquet" but names no engine.
- **Parseable:**
  - It has the best raw layout for globs (`date=/hour=/minute=` hive paths) plus JSON manifests per ingestor.
  - But the OSS code does no compaction, so files multiply as nodes × streams × minutes.
  - No engine is named.
- **Iceberg entrants:**
  - Observe (Iceberg v3, REST catalog, names Spark/DuckDB/Trino/PyIceberg; private preview), CloudWatch S3 Tables (Iceberg, Athena/Redshift; new data only) and IceGate (own S3-backed catalog, Trino via Iceberg REST; prototype).
  - They give **catalog discovery, snapshot isolation and schema evolution**, which LH lacks.
  - Against them LH's advantages are self-hosting, Apache-2.0, and VL/VT/Jaeger/Tempo APIs on the same files.
- **Coralogix archive:** documented pandas and Athena recipes, but each file holds only three JSON-string columns, so typed pruning is lost.
- **Everyone else** (Loki, VL/VT, Elastic, OpenSearch, ClickHouse family, Quickwit, Axiom, SaaS): the primary store is not readable by external engines. Open formats there are export copies or archives.

---
