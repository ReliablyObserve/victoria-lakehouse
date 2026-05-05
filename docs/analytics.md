---
title: Analytics
sidebar_position: 19
---

# Analytics with Open Parquet Format

Victoria Lakehouse stores all data as standard Apache Parquet files on S3 with Hive partitioning. Any tool that reads Parquet can query this data directly — no proprietary format, no vendor lock-in.

## S3 Layout

```
s3://obs-archive/{tenant}/
  logs/
    dt=2026-01-15/hour=00/00001-abc.parquet
    dt=2026-01-15/hour=01/00002-def.parquet
    ...
  traces/
    dt=2026-01-15/hour=00/00001-ghi.parquet
    ...
```

- **Hive partitioning**: `dt=YYYY-MM-DD/hour=HH` for efficient time-range pruning
- **Compression**: ZSTD (optimal ratio for observability data)
- **Row groups**: ~10,000 rows each with column statistics and bloom filters
- **Bloom filters**: `service.name`, `trace_id` for fast point lookups

## DuckDB

[DuckDB](https://duckdb.org/) is an in-process analytical database. It reads Parquet directly from S3 with zero setup — ideal for ad-hoc investigation, incident response, and local analysis.

### Setup

```sql
-- Install and load httpfs extension for S3 access
INSTALL httpfs;
LOAD httpfs;

-- Configure S3 credentials
SET s3_region = 'us-east-1';
SET s3_access_key_id = 'your-access-key';
SET s3_secret_access_key = 'your-secret-key';

-- For IAM roles / instance profiles:
SET s3_use_ssl = true;
-- DuckDB auto-detects instance profile credentials
```

### Query Examples — Logs

```sql
-- Count logs by severity for a specific day
SELECT severity_text, COUNT(*) as cnt
FROM read_parquet('s3://obs-archive/logs/dt=2026-05-01/hour=*//*.parquet',
                  hive_partitioning=true)
GROUP BY severity_text
ORDER BY cnt DESC;

-- Search for error logs from a specific service
SELECT timestamp_unix_nano, body, severity_text, "service.name"
FROM read_parquet('s3://obs-archive/logs/dt=2026-05-*/hour=*//*.parquet',
                  hive_partitioning=true)
WHERE "service.name" = 'api-gateway'
  AND severity_text IN ('ERROR', 'FATAL')
ORDER BY timestamp_unix_nano DESC
LIMIT 100;

-- Top error messages in the last week
SELECT body, COUNT(*) as occurrences,
       MIN(timestamp_unix_nano) as first_seen,
       MAX(timestamp_unix_nano) as last_seen
FROM read_parquet('s3://obs-archive/logs/dt=2026-04-2*/hour=*//*.parquet',
                  hive_partitioning=true)
WHERE severity_text = 'ERROR'
GROUP BY body
ORDER BY occurrences DESC
LIMIT 20;

-- Log volume per namespace per hour (capacity planning)
SELECT dt, hour,
       "k8s.namespace.name" as namespace,
       COUNT(*) as log_count,
       SUM(LENGTH(body)) as total_bytes
FROM read_parquet('s3://obs-archive/logs/dt=2026-05-*/hour=*//*.parquet',
                  hive_partitioning=true)
GROUP BY dt, hour, namespace
ORDER BY dt, hour, log_count DESC;

-- Extract fields from MAP columns (resource.attributes)
SELECT timestamp_unix_nano, body,
       resource_attributes['deployment.environment'] as env,
       resource_attributes['k8s.deployment.name'] as deployment,
       log_attributes['http.status_code'] as status
FROM read_parquet('s3://obs-archive/logs/dt=2026-05-01/hour=*//*.parquet',
                  hive_partitioning=true)
WHERE log_attributes['http.status_code'] = '500'
LIMIT 50;

-- Correlate logs with trace IDs
SELECT l.timestamp_unix_nano, l.body, l."service.name",
       l.trace_id, l.span_id
FROM read_parquet('s3://obs-archive/logs/dt=2026-05-01/hour=1*//*.parquet',
                  hive_partitioning=true) l
WHERE l.trace_id = 'abc123def456'
ORDER BY l.timestamp_unix_nano;
```

### Query Examples — Traces

```sql
-- Slowest spans for a service
SELECT "span.name", "service.name",
       duration_ns / 1e6 as duration_ms,
       trace_id, span_id
FROM read_parquet('s3://obs-archive/traces/dt=2026-05-01/hour=*//*.parquet',
                  hive_partitioning=true)
WHERE "service.name" = 'payment-service'
ORDER BY duration_ns DESC
LIMIT 20;

-- P50/P95/P99 latency by service
SELECT "service.name",
       QUANTILE_CONT(duration_ns / 1e6, 0.50) as p50_ms,
       QUANTILE_CONT(duration_ns / 1e6, 0.95) as p95_ms,
       QUANTILE_CONT(duration_ns / 1e6, 0.99) as p99_ms,
       COUNT(*) as span_count
FROM read_parquet('s3://obs-archive/traces/dt=2026-05-01/hour=*//*.parquet',
                  hive_partitioning=true)
GROUP BY "service.name"
ORDER BY p99_ms DESC;

-- Error rate by service and span name
SELECT "service.name", "span.name",
       COUNT(*) as total,
       SUM(CASE WHEN "status.code" = 2 THEN 1 ELSE 0 END) as errors,
       ROUND(100.0 * SUM(CASE WHEN "status.code" = 2 THEN 1 ELSE 0 END) / COUNT(*), 2) as error_pct
FROM read_parquet('s3://obs-archive/traces/dt=2026-05-01/hour=*//*.parquet',
                  hive_partitioning=true)
GROUP BY "service.name", "span.name"
HAVING COUNT(*) > 100
ORDER BY error_pct DESC;

-- Full trace reconstruction
SELECT trace_id, span_id, parent_span_id,
       "service.name", "span.name",
       timestamp_unix_nano,
       duration_ns / 1e6 as duration_ms,
       "status.code"
FROM read_parquet('s3://obs-archive/traces/dt=2026-05-01/hour=*//*.parquet',
                  hive_partitioning=true)
WHERE trace_id = 'abc123def456789'
ORDER BY timestamp_unix_nano;

-- Service dependency map (which services call which)
SELECT parent_svc.name as caller,
       child."service.name" as callee,
       COUNT(*) as call_count,
       AVG(child.duration_ns / 1e6) as avg_duration_ms
FROM read_parquet('s3://obs-archive/traces/dt=2026-05-01/hour=*//*.parquet',
                  hive_partitioning=true) child
JOIN read_parquet('s3://obs-archive/traces/dt=2026-05-01/hour=*//*.parquet',
                  hive_partitioning=true) parent_svc
  ON child.parent_span_id = parent_svc.span_id
  AND child.trace_id = parent_svc.trace_id
GROUP BY caller, callee
ORDER BY call_count DESC;
```

### DuckDB CLI One-Liners

```bash
# Quick error count for today
duckdb -c "
  SELECT severity_text, COUNT(*)
  FROM read_parquet('s3://obs-archive/logs/dt=2026-05-04/hour=*//*.parquet', hive_partitioning=true)
  GROUP BY severity_text
"

# Export a day of logs to local Parquet for offline analysis
duckdb -c "
  COPY (
    SELECT * FROM read_parquet('s3://obs-archive/logs/dt=2026-05-01/hour=*//*.parquet', hive_partitioning=true)
  ) TO 'logs-2026-05-01.parquet' (FORMAT PARQUET, COMPRESSION ZSTD)
"

# Export traces to CSV for spreadsheet analysis
duckdb -c "
  COPY (
    SELECT \"service.name\", \"span.name\", duration_ns/1e6 as ms, \"status.code\"
    FROM read_parquet('s3://obs-archive/traces/dt=2026-05-01/hour=*//*.parquet', hive_partitioning=true)
    WHERE \"service.name\" = 'api-gateway'
  ) TO 'api-traces.csv' (HEADER, DELIMITER ',')
"
```

## Apache Spark

[Apache Spark](https://spark.apache.org/) is ideal for large-scale analytics, ML pipelines, and ETL across months or years of data.

### Setup (PySpark)

```python
from pyspark.sql import SparkSession

spark = SparkSession.builder \
    .appName("lakehouse-analytics") \
    .config("spark.hadoop.fs.s3a.endpoint", "s3.us-east-1.amazonaws.com") \
    .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem") \
    .config("spark.hadoop.fs.s3a.aws.credentials.provider",
            "com.amazonaws.auth.DefaultAWSCredentialsProviderChain") \
    .config("spark.sql.parquet.enableVectorizedReader", "true") \
    .config("spark.sql.sources.partitionOverwriteMode", "dynamic") \
    .getOrCreate()
```

### Query Examples — Logs

```python
# Read logs with Hive partition discovery
logs = spark.read.parquet("s3a://obs-archive/logs/")

# Register as SQL table
logs.createOrReplaceTempView("logs")

# Monthly log volume trend
monthly_volume = spark.sql("""
    SELECT DATE_TRUNC('month', TO_TIMESTAMP(timestamp_unix_nano / 1000000000)) as month,
           `service.name`,
           COUNT(*) as log_count,
           SUM(LENGTH(body)) / (1024*1024*1024) as volume_gb
    FROM logs
    WHERE dt BETWEEN '2026-01-01' AND '2026-05-01'
    GROUP BY month, `service.name`
    ORDER BY month, volume_gb DESC
""")
monthly_volume.show(50)

# Anomaly detection: services with unusual error rates
error_anomaly = spark.sql("""
    WITH daily_errors AS (
        SELECT dt, `service.name`,
               COUNT(*) as total,
               SUM(CASE WHEN severity_text = 'ERROR' THEN 1 ELSE 0 END) as errors
        FROM logs
        WHERE dt BETWEEN '2026-04-01' AND '2026-05-01'
        GROUP BY dt, `service.name`
    ),
    stats AS (
        SELECT `service.name`,
               AVG(errors * 1.0 / total) as avg_error_rate,
               STDDEV(errors * 1.0 / total) as stddev_error_rate
        FROM daily_errors
        WHERE total > 1000
        GROUP BY `service.name`
    )
    SELECT d.dt, d.`service.name`,
           d.errors * 1.0 / d.total as error_rate,
           s.avg_error_rate,
           (d.errors * 1.0 / d.total - s.avg_error_rate) / s.stddev_error_rate as z_score
    FROM daily_errors d
    JOIN stats s ON d.`service.name` = s.`service.name`
    WHERE (d.errors * 1.0 / d.total - s.avg_error_rate) / s.stddev_error_rate > 3
    ORDER BY z_score DESC
""")
error_anomaly.show()

# Top log patterns (template extraction)
from pyspark.sql.functions import regexp_replace, col, count

log_patterns = logs.filter(col("severity_text") == "ERROR") \
    .withColumn("pattern", regexp_replace(col("body"), r"\d+", "N")) \
    .withColumn("pattern", regexp_replace(col("pattern"), r"[0-9a-f]{8,}", "HEX")) \
    .groupBy("pattern") \
    .agg(count("*").alias("occurrences")) \
    .orderBy(col("occurrences").desc()) \
    .limit(50)
log_patterns.show(truncate=80)
```

### Query Examples — Traces

```python
# Read traces
traces = spark.read.parquet("s3a://obs-archive/traces/")
traces.createOrReplaceTempView("traces")

# Service latency SLO report (monthly)
slo_report = spark.sql("""
    SELECT `service.name`,
           DATE_TRUNC('month', TO_TIMESTAMP(timestamp_unix_nano / 1000000000)) as month,
           COUNT(*) as total_spans,
           PERCENTILE(duration_ns / 1e6, 0.50) as p50_ms,
           PERCENTILE(duration_ns / 1e6, 0.95) as p95_ms,
           PERCENTILE(duration_ns / 1e6, 0.99) as p99_ms,
           SUM(CASE WHEN `status.code` = 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as error_pct,
           SUM(CASE WHEN duration_ns / 1e6 < 500 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as within_slo_pct
    FROM traces
    WHERE dt BETWEEN '2026-01-01' AND '2026-05-01'
    GROUP BY `service.name`, month
    ORDER BY month, `service.name`
""")
slo_report.show(100)

# Service dependency graph (for visualization)
dependencies = spark.sql("""
    SELECT p.`service.name` as source,
           c.`service.name` as target,
           COUNT(*) as request_count,
           AVG(c.duration_ns / 1e6) as avg_latency_ms,
           SUM(CASE WHEN c.`status.code` = 2 THEN 1 ELSE 0 END) * 100.0 / COUNT(*) as error_pct
    FROM traces c
    JOIN traces p ON c.parent_span_id = p.span_id AND c.trace_id = p.trace_id
    WHERE c.dt = '2026-05-01'
    GROUP BY source, target
    HAVING COUNT(*) > 10
    ORDER BY request_count DESC
""")

# Save as Parquet for visualization tools
dependencies.write.mode("overwrite").parquet("s3a://obs-archive/analytics/service-graph/")
```

### Spark ETL: Aggregate and Compact

```python
# Daily aggregation job — compact hourly files into daily summaries
daily_summary = spark.sql("""
    SELECT dt,
           `service.name`,
           severity_text,
           COUNT(*) as log_count,
           SUM(LENGTH(body)) as total_bytes,
           MIN(timestamp_unix_nano) as min_ts,
           MAX(timestamp_unix_nano) as max_ts
    FROM logs
    WHERE dt = '2026-05-01'
    GROUP BY dt, `service.name`, severity_text
""")

daily_summary.write \
    .mode("overwrite") \
    .partitionBy("dt") \
    .parquet("s3a://obs-archive/analytics/daily-log-summary/")
```

## Trino

[Trino](https://trino.io/) (formerly PrestoSQL) provides federated SQL queries across data sources. It excels at interactive analytics on large datasets and can join lakehouse data with other data sources (PostgreSQL, MySQL, etc.).

### Catalog Configuration

```properties
# etc/catalog/lakehouse.properties
connector.name=hive
hive.metastore=glue
# Or for standalone Hive Metastore:
# hive.metastore.uri=thrift://hive-metastore:9083
hive.s3.aws-access-key=your-access-key
hive.s3.aws-secret-key=your-secret-key
hive.s3.endpoint=s3.us-east-1.amazonaws.com
hive.s3-file-system-type=HADOOP_DEFAULT
hive.parquet.use-column-names=true
```

### Table Registration

```sql
-- Create schema
CREATE SCHEMA IF NOT EXISTS lakehouse.observability;

-- Register logs table (Hive-partitioned Parquet)
CREATE TABLE IF NOT EXISTS lakehouse.observability.logs (
    timestamp_unix_nano BIGINT,
    body VARCHAR,
    severity_text VARCHAR,
    severity_number INTEGER,
    "service.name" VARCHAR,
    "k8s.namespace.name" VARCHAR,
    "k8s.pod.name" VARCHAR,
    trace_id VARBINARY,
    span_id VARBINARY,
    _stream VARCHAR,
    _stream_id VARCHAR,
    "resource.attributes" MAP(VARCHAR, VARCHAR),
    "log.attributes" MAP(VARCHAR, VARCHAR),
    "scope.name" VARCHAR,
    dt VARCHAR,
    hour INTEGER
)
WITH (
    external_location = 's3://obs-archive/logs/',
    format = 'PARQUET',
    partitioned_by = ARRAY['dt', 'hour']
);

-- Register traces table
CREATE TABLE IF NOT EXISTS lakehouse.observability.traces (
    timestamp_unix_nano BIGINT,
    start_time_unix_nano BIGINT,
    trace_id VARBINARY,
    span_id VARBINARY,
    parent_span_id VARBINARY,
    "span.name" VARCHAR,
    "span.kind" INTEGER,
    "status.code" INTEGER,
    "status.message" VARCHAR,
    duration_ns BIGINT,
    "service.name" VARCHAR,
    "resource.attributes" MAP(VARCHAR, VARCHAR),
    "span.attributes" MAP(VARCHAR, VARCHAR),
    "scope.name" VARCHAR,
    "scope.attributes" MAP(VARCHAR, VARCHAR),
    dt VARCHAR,
    hour INTEGER
)
WITH (
    external_location = 's3://obs-archive/traces/',
    format = 'PARQUET',
    partitioned_by = ARRAY['dt', 'hour']
);

-- Discover partitions
CALL lakehouse.system.sync_partition_metadata('observability', 'logs', 'FULL');
CALL lakehouse.system.sync_partition_metadata('observability', 'traces', 'FULL');
```

### Query Examples

```sql
-- Cross-service error correlation
-- Find services whose error rate increased when another service had an incident
WITH hourly_errors AS (
    SELECT dt, hour, "service.name",
           COUNT(*) as total,
           SUM(CASE WHEN severity_text = 'ERROR' THEN 1 ELSE 0 END) as errors
    FROM lakehouse.observability.logs
    WHERE dt BETWEEN '2026-04-28' AND '2026-05-04'
    GROUP BY dt, hour, "service.name"
),
incident_hours AS (
    SELECT dt, hour
    FROM hourly_errors
    WHERE "service.name" = 'payment-service'
      AND errors * 1.0 / total > 0.05
)
SELECT h.dt, h.hour, h."service.name",
       h.errors * 1.0 / h.total as error_rate
FROM hourly_errors h
JOIN incident_hours i ON h.dt = i.dt AND h.hour = i.hour
WHERE h."service.name" != 'payment-service'
  AND h.errors * 1.0 / h.total > 0.01
ORDER BY h.dt, h.hour, error_rate DESC;

-- Join logs with traces — find error logs with their trace context
SELECT l.timestamp_unix_nano,
       l."service.name",
       l.body,
       t."span.name",
       t.duration_ns / 1e6 as span_ms,
       t."status.code"
FROM lakehouse.observability.logs l
JOIN lakehouse.observability.traces t
  ON l.trace_id = t.trace_id
  AND l.dt = t.dt
WHERE l.severity_text = 'ERROR'
  AND l.dt = '2026-05-01'
ORDER BY l.timestamp_unix_nano DESC
LIMIT 100;

-- Compliance audit: data retention verification
SELECT dt,
       COUNT(DISTINCT hour) as hours_covered,
       COUNT(*) as total_records,
       MIN(timestamp_unix_nano) as earliest,
       MAX(timestamp_unix_nano) as latest
FROM lakehouse.observability.logs
GROUP BY dt
ORDER BY dt;

-- Multi-source join: logs + business data
-- (Trino can join observability data with business databases)
SELECT l."service.name", l.body,
       o.order_id, o.customer_id, o.status
FROM lakehouse.observability.logs l
JOIN postgres.orders.orders o
  ON l."log.attributes"['order_id'] = CAST(o.order_id AS VARCHAR)
WHERE l.dt = '2026-05-01'
  AND l.severity_text = 'ERROR'
  AND l."service.name" = 'order-service';
```

### AWS Glue Integration

For automatic partition discovery with AWS Glue Catalog:

```python
# glue-crawler.py — Register lakehouse Parquet with AWS Glue
import boto3

glue = boto3.client('glue', region_name='us-east-1')

# Create database
glue.create_database(
    DatabaseInput={
        'Name': 'observability',
        'Description': 'Victoria Lakehouse observability data'
    }
)

# Create crawler for automatic partition discovery
glue.create_crawler(
    Name='lakehouse-logs-crawler',
    Role='arn:aws:iam::123456789012:role/GlueCrawlerRole',
    DatabaseName='observability',
    Targets={
        'S3Targets': [
            {'Path': 's3://obs-archive/logs/', 'Exclusions': []},
            {'Path': 's3://obs-archive/traces/', 'Exclusions': []}
        ]
    },
    SchemaChangePolicy={
        'UpdateBehavior': 'UPDATE_IN_DATABASE',
        'DeleteBehavior': 'LOG'
    },
    Schedule='cron(0 */1 * * ? *)',  # Hourly
    Configuration='{"Version":1.0,"Grouping":{"TableGroupingPolicy":"CombineCompatibleSchemas"}}'
)
```

## ClickHouse

[ClickHouse](https://clickhouse.com/) can read Parquet directly from S3 for high-performance analytical queries.

```sql
-- Query logs directly from S3
SELECT severity_text, count() as cnt
FROM s3('https://obs-archive.s3.us-east-1.amazonaws.com/logs/dt=2026-05-01/hour=*//*.parquet',
        'access_key', 'secret_key', 'Parquet')
GROUP BY severity_text
ORDER BY cnt DESC;

-- Create an external table for repeated queries
CREATE TABLE lakehouse_logs
ENGINE = S3('https://obs-archive.s3.us-east-1.amazonaws.com/logs/dt=*/hour=*//*.parquet',
            'access_key', 'secret_key', 'Parquet')
AS SELECT * FROM s3('https://obs-archive.s3.us-east-1.amazonaws.com/logs/dt=2026-05-01/hour=00/00001-*.parquet',
                    'access_key', 'secret_key', 'Parquet')
LIMIT 0;
```

## Pandas / Jupyter Notebooks

For data science workflows:

```python
import pandas as pd
import pyarrow.parquet as pq
import s3fs

# Connect to S3
fs = s3fs.S3FileSystem(
    key='your-access-key',
    secret='your-secret-key',
    client_kwargs={'region_name': 'us-east-1'}
)

# Read specific partitions
dataset = pq.ParquetDataset(
    'obs-archive/logs/dt=2026-05-01',
    filesystem=fs,
    filters=[('severity_text', '=', 'ERROR')]
)
df = dataset.read().to_pandas()

# Analyze error patterns
print(f"Total errors: {len(df)}")
print(f"\nTop services with errors:")
print(df['service.name'].value_counts().head(10))

# Time series of errors
df['timestamp'] = pd.to_datetime(df['timestamp_unix_nano'], unit='ns')
hourly = df.set_index('timestamp').resample('1h').size()
hourly.plot(title='Errors per Hour', figsize=(12, 4))
```

## Common Analytics Use Cases

| Use Case | Tool | Example |
|---|---|---|
| Ad-hoc incident investigation | DuckDB | Search for error patterns during an outage |
| Monthly SLO reporting | Spark / Trino | Compute p50/p95/p99 latency by service across months |
| Capacity planning | Spark | Log volume trends, growth forecasting |
| Service dependency mapping | Spark / Trino | Build dependency graph from parent-child spans |
| Compliance audit | Trino | Verify data retention and access patterns |
| Anomaly detection | Spark (ML) | Detect unusual error rate patterns |
| Cost allocation | Trino | Log/trace volume per team/namespace for chargeback |
| Data science / ML training | Pandas / Spark | Feature extraction from traces for latency prediction |
| Cross-source joins | Trino | Join observability data with business databases |
| Data quality monitoring | DuckDB | Check for gaps in partition coverage |
