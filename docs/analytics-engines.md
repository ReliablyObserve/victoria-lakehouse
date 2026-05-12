---
title: Analytics Engines
sidebar_position: 20
---

# Analytics Engines — Grafana Integration

Victoria Lakehouse stores data as standard Apache Parquet on S3. Any Parquet-capable engine can query it directly. This page covers all engines with known Grafana datasource support — both as standalone query tools and as Grafana-integrated datasources.

## Engine Overview

| Engine | Grafana Plugin | Plugin Status | License | S3 Parquet Method |
|---|---|---|---|---|
| [DuckDB](https://duckdb.org/) | `motherduck-duckdb-datasource` | Unsigned, GitHub only | Free | `read_parquet()` via `httpfs` |
| [ClickHouse](https://clickhouse.com/) | `grafana-clickhouse-datasource` | Official (Grafana Labs), 27.7M downloads | Free | `s3()` table function |
| [Trino](https://trino.io/) | `trino-datasource` | Community-signed, in Grafana catalog, 1.4M downloads | Free | Hive connector with Parquet SerDe |
| [Databricks](https://www.databricks.com/) | `grafana-databricks-datasource` | Official (Grafana Labs) | Enterprise only | Delta Lake / Parquet on S3 |
| [Snowflake](https://www.snowflake.com/) | `grafana-snowflake-datasource` | Official (Grafana Labs) | Enterprise only | External stage on S3 |
| [StarRocks](https://www.starrocks.io/) / [Apache Doris](https://doris.apache.org/) | Built-in MySQL datasource | MySQL wire compat | Free | S3 external table / `FILES()` |
| [Apache Spark](https://spark.apache.org/) | None | No Grafana plugin exists | — | `spark.read.parquet()` |
| [Presto](https://prestodb.io/) | None | Abandoned community plugin (7 stars) | — | Hive connector |

## Free-Tier Engines (Grafana OSS Compatible)

### DuckDB

**In-memory, zero infrastructure.** Runs inside the Grafana DuckDB datasource plugin — no server needed. Best for ad-hoc investigation, incident response, and local analysis.

- **Plugin**: [`motherduck-duckdb-datasource`](https://github.com/motherduckdb/grafana-duckdb-datasource) v0.4.1
- **Install**: Manual download from GitHub (unsigned — requires `GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS`)
- **Requires**: Grafana Ubuntu image (`grafana/grafana:latest-ubuntu`) — DuckDB uses glibc, not compatible with Alpine

```sql
-- Query logs directly from S3
SELECT * FROM read_parquet('s3://obs-archive/logs/dt=2026-05-01/**/*.parquet',
                           hive_partitioning=true)
WHERE severity_text = 'ERROR'
LIMIT 100;

-- Trace latency analysis
SELECT "service.name", QUANTILE_CONT(duration_ns / 1e6, 0.95) as p95_ms
FROM read_parquet('s3://obs-archive/traces/**/*.parquet', hive_partitioning=true)
GROUP BY "service.name" ORDER BY p95_ms DESC;
```

**Links**: [DuckDB S3 docs](https://duckdb.org/docs/extensions/httpfs/s3api.html) · [Grafana plugin](https://github.com/motherduckdb/grafana-duckdb-datasource)

### ClickHouse

**Server-based analytics engine.** Queries S3 Parquet via the `s3()` table function — no data import needed. ClickHouse can also serve as a Grafana **Logs** and **Traces** datasource via field mapping.

- **Plugin**: [`grafana-clickhouse-datasource`](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) (Official, Grafana-signed)
- **Install**: `GF_INSTALL_PLUGINS=grafana-clickhouse-datasource` (auto from catalog)
- **Modes**: Analytics (SQL), Logs panel (body/severity/timestamp mapping), Traces panel (trace_id/span_id/duration mapping)

```sql
-- Analytics: error rate per service
SELECT "service.name", countIf(severity_text = 'ERROR') * 100.0 / count() AS error_pct
FROM s3('http://minio:9000/obs-archive/logs/**/*.parquet', 'minioadmin', 'minioadmin', 'Parquet')
GROUP BY "service.name" ORDER BY error_pct DESC;

-- Use pre-configured views (if ClickHouse has init-s3.sql):
SELECT * FROM lakehouse.logs WHERE severity_text = 'ERROR' LIMIT 100;
SELECT * FROM lakehouse.traces WHERE "service.name" = 'api-gateway' LIMIT 100;
```

**Grafana Logs panel config** (datasource jsonData):
```yaml
defaultTable: logs
logsTimestampField: timestamp_unix_nano
logsBodyField: body
logsSeverityField: severity_text
logsServiceNameField: "service.name"
```

**Grafana Traces panel config** (datasource jsonData):
```yaml
defaultTable: traces
tracesTimestampField: timestamp_unix_nano
tracesDurationField: duration_ns
tracesTraceIdField: trace_id
tracesSpanIdField: span_id
tracesParentSpanIdField: parent_span_id
tracesServiceNameField: "service.name"
tracesOperationNameField: "span.name"
tracesStatusCodeField: "status.code"
```

**Links**: [ClickHouse S3 table function](https://clickhouse.com/docs/sql-reference/table-functions/s3) · [Grafana plugin](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/)

### Trino

**Distributed SQL query engine.** Queries S3 Parquet via the Hive connector with Parquet SerDe. Ideal for cross-dataset joins, federated queries, and large-scale analytics.

- **Plugin**: [`trino-datasource`](https://grafana.com/grafana/plugins/trino-datasource/) v1.0.11 (community-signed, in Grafana catalog)
- **Install**: `GF_INSTALL_PLUGINS=trino-datasource` (auto from catalog)

```sql
-- Create external table pointing to S3 Parquet
CREATE SCHEMA IF NOT EXISTS lakehouse WITH (location = 's3a://obs-archive/');

-- Query logs
SELECT body, severity_text, "service.name"
FROM hive.lakehouse.logs
WHERE dt = '2026-05-01'
  AND severity_text = 'ERROR'
ORDER BY timestamp_unix_nano DESC
LIMIT 100;

-- Cross-join logs and traces by trace_id
SELECT l.body, t."span.name", t.duration_ns / 1e6 as ms
FROM hive.lakehouse.logs l
JOIN hive.lakehouse.traces t ON l.trace_id = t.trace_id
WHERE l.trace_id = 'abc123'
ORDER BY t.timestamp_unix_nano;
```

**Links**: [Trino Hive S3 connector](https://trino.io/docs/current/connector/hive-s3.html) · [Grafana plugin](https://grafana.com/grafana/plugins/trino-datasource/)

### StarRocks / Apache Doris

**MySQL-compatible OLAP engines.** Both can query S3 Parquet via external tables and are accessible through Grafana's built-in MySQL datasource — no extra plugin needed.

```sql
-- StarRocks: query S3 Parquet via FILES()
SELECT * FROM FILES(
  "path" = "s3://obs-archive/logs/dt=2026-05-01/**/*.parquet",
  "format" = "parquet",
  "aws.s3.access_key" = "minioadmin",
  "aws.s3.secret_key" = "minioadmin"
) LIMIT 100;

-- Apache Doris: S3 external table
CREATE CATALOG lakehouse PROPERTIES (
  "type" = "hms",
  "hive.metastore.uris" = "thrift://hms:9083"
);
SELECT * FROM lakehouse.db.logs WHERE dt = '2026-05-01';
```

**Links**: [StarRocks external tables](https://docs.starrocks.io/docs/data_source/External_table/) · [Doris Hive catalog](https://doris.apache.org/docs/lakehouse/datalake-analytics/hive/)

## Enterprise-Only Engines (Grafana Enterprise Required)

### Databricks

**Unified analytics platform.** Reads Parquet from S3 natively. Official Grafana datasource requires Grafana Enterprise license.

- **Plugin**: [`grafana-databricks-datasource`](https://grafana.com/grafana/plugins/grafana-databricks-datasource/) (Official, Grafana-signed, Enterprise only)

```sql
-- Databricks SQL
SELECT * FROM parquet.`s3://obs-archive/logs/dt=2026-05-01/`
WHERE severity_text = 'ERROR';
```

**Links**: [Databricks external data](https://docs.databricks.com/en/connect/storage/index.html) · [Grafana plugin](https://grafana.com/grafana/plugins/grafana-databricks-datasource/)

### Snowflake

**Cloud data warehouse.** Queries S3 Parquet via external stages. Official Grafana datasource requires Grafana Enterprise license.

- **Plugin**: [`grafana-snowflake-datasource`](https://grafana.com/grafana/plugins/grafana-snowflake-datasource/) (Official, Grafana-signed, Enterprise only)

```sql
-- Create external stage
CREATE STAGE lakehouse_stage URL = 's3://obs-archive/'
  CREDENTIALS = (AWS_KEY_ID = '...' AWS_SECRET_KEY = '...');

-- Query Parquet files
SELECT $1:body::STRING, $1:severity_text::STRING
FROM @lakehouse_stage/logs/dt=2026-05-01/ (FILE_FORMAT => (TYPE = PARQUET));
```

**Links**: [Snowflake external stages](https://docs.snowflake.com/en/user-guide/data-load-s3) · [Grafana plugin](https://grafana.com/grafana/plugins/grafana-snowflake-datasource/)

## CLI/SDK-Only Engines (No Grafana Plugin)

### Apache Spark

**Batch analytics and ML pipelines.** No Grafana datasource plugin exists. Use for large-scale ETL, anomaly detection, and training ML models on observability data.

```python
logs = spark.read.parquet("s3a://obs-archive/logs/")
logs.filter(logs.severity_text == "ERROR").groupBy("service.name").count().show()
```

**Links**: [Spark Parquet docs](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html)

### pandas

**Python data analysis.** No Grafana integration needed — used in notebooks and scripts.

```python
import pandas as pd
df = pd.read_parquet("s3://obs-archive/logs/dt=2026-05-01/")
df[df['severity_text'] == 'ERROR'].groupby('service.name').size()
```

**Links**: [pandas read_parquet](https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html)

## Multi-Tenant Analytics

All engines work with Victoria Lakehouse's S3 prefix isolation. Each tenant's data is a self-contained Hive-partitioned dataset at its own prefix:

```
s3://obs-archive/{AccountID}/{ProjectID}/logs/dt=YYYY-MM-DD/hour=HH/*.parquet
```

Point any engine at the tenant's prefix for scoped analytics. With bucket-per-tenant isolation, point at the tenant's bucket instead. See [Multi-Tenancy](multi-tenancy.md) for details.

## Docker Compose Setup

The [Docker Compose environment](docker-compose-setup.md) includes DuckDB and ClickHouse pre-configured with MinIO S3 access. All 11 Grafana datasources are auto-provisioned. See the compose docs for example queries and configuration.
