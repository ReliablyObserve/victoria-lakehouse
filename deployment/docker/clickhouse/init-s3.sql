-- ClickHouse OTEL-compatible views for Lakehouse S3 Parquet data
-- Schema matches the OpenTelemetry ClickHouse exporter exactly so
-- Grafana's OTEL mode auto-discovers columns with zero configuration.
-- Reference: https://opentelemetry.io/docs/specs/otel/logs/data-model/

CREATE DATABASE IF NOT EXISTS lakehouse;

-- ==========================================================================
-- OTEL Logs — matches otel-collector clickhouseexporter log schema
-- ==========================================================================

CREATE OR REPLACE VIEW lakehouse.otel_logs AS
SELECT
    fromUnixTimestamp64Nano(timestamp_unix_nano) AS Timestamp,
    toDateTime(fromUnixTimestamp64Nano(timestamp_unix_nano)) AS TimestampTime,
    toUInt32(0) AS TraceFlags,
    severity_text AS SeverityText,
    toInt32(severity_number) AS SeverityNumber,
    body AS Body,
    `service.name` AS ServiceName,
    trace_id AS TraceId,
    trace_id AS traceID,
    span_id AS SpanId,
    `scope.name` AS ScopeName,
    '' AS ScopeVersion,
    '' AS ResourceSchemaUrl,
    '' AS ScopeSchemaUrl,
    `resource.attributes` AS ResourceAttributes,
    mapConcat(
        `log.attributes`,
        mapFilter((k, v) -> v != '',
            mapFromArrays(
                ['level', 'service.name', 'k8s.namespace.name', 'k8s.pod.name',
                 'k8s.deployment.name', 'k8s.node.name', 'deployment.environment',
                 'cloud.region', 'host.name', 'trace_id', 'span_id', 'scope.name'],
                [severity_text, `service.name`, `k8s.namespace.name`, `k8s.pod.name`,
                 `k8s.deployment.name`, `k8s.node.name`, `deployment.environment`,
                 `cloud.region`, `host.name`, trace_id, span_id, `scope.name`]
            )
        )
    ) AS LogAttributes,
    CAST(map() AS Map(String, String)) AS ScopeAttributes,
    CAST([] AS Array(DateTime64(9))) AS `Events.Timestamp`,
    CAST([] AS Array(String)) AS `Events.Name`,
    CAST([] AS Array(Map(String, String))) AS `Events.Attributes`,
    CAST([] AS Array(String)) AS `Links.TraceId`,
    CAST([] AS Array(String)) AS `Links.SpanId`,
    CAST([] AS Array(String)) AS `Links.TraceState`,
    CAST([] AS Array(Map(String, String))) AS `Links.Attributes`
FROM s3(
    'http://minio:9000/obs-archive/*/*/logs/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, body String, severity_text String, severity_number Int32,
     `service.name` String, `k8s.namespace.name` String, `k8s.pod.name` String,
     `k8s.deployment.name` String, `k8s.node.name` String, `deployment.environment` String,
     `cloud.region` String, `host.name` String, trace_id String, span_id String,
     `_stream` String, `_stream_id` String, `scope.name` String,
     `resource.attributes` Map(String, String), `log.attributes` Map(String, String)'
);

-- ==========================================================================
-- OTEL Traces — trace_id → timestamp index (required by Grafana plugin)
-- ==========================================================================

CREATE OR REPLACE VIEW lakehouse.otel_traces_trace_id_ts AS
SELECT
    trace_id AS TraceId,
    fromUnixTimestamp64Nano(min(timestamp_unix_nano)) AS Start,
    fromUnixTimestamp64Nano(max(timestamp_unix_nano)) AS End
FROM s3(
    'http://minio:9000/obs-archive/*/*/traces/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, trace_id String'
)
GROUP BY trace_id;

-- ==========================================================================
-- OTEL Traces — matches otel-collector clickhouseexporter span schema
-- ==========================================================================

CREATE OR REPLACE VIEW lakehouse.otel_traces AS
SELECT
    fromUnixTimestamp64Nano(timestamp_unix_nano) AS Timestamp,
    toDateTime(fromUnixTimestamp64Nano(timestamp_unix_nano)) AS TimestampTime,
    trace_id AS TraceId,
    span_id AS SpanId,
    parent_span_id AS ParentSpanId,
    `span.name` AS SpanName,
    CASE `span.kind`
        WHEN 0 THEN 'SPAN_KIND_UNSPECIFIED'
        WHEN 1 THEN 'SPAN_KIND_INTERNAL'
        WHEN 2 THEN 'SPAN_KIND_SERVER'
        WHEN 3 THEN 'SPAN_KIND_CLIENT'
        WHEN 4 THEN 'SPAN_KIND_PRODUCER'
        WHEN 5 THEN 'SPAN_KIND_CONSUMER'
        ELSE 'SPAN_KIND_UNSPECIFIED'
    END AS SpanKind,
    `span.kind` AS SpanKindNumber,
    `service.name` AS ServiceName,
    duration_ns AS Duration,
    toFloat64(duration_ns) / 1000000.0 AS DurationMs,
    CASE `status.code`
        WHEN 0 THEN 'STATUS_CODE_UNSET'
        WHEN 1 THEN 'STATUS_CODE_OK'
        WHEN 2 THEN 'STATUS_CODE_ERROR'
        ELSE 'STATUS_CODE_UNSET'
    END AS StatusCode,
    `status.code` AS StatusNumber,
    `status.message` AS StatusMessage,
    `scope.name` AS ScopeName,
    '' AS ScopeVersion,
    '' AS TraceState,
    toUInt32(0) AS TraceFlags,
    '' AS ResourceSchemaUrl,
    '' AS ScopeSchemaUrl,
    `resource.attributes` AS ResourceAttributes,
    `span.attributes` AS SpanAttributes,
    CAST(map() AS Map(String, String)) AS ScopeAttributes,
    CAST([] AS Array(DateTime64(9))) AS `Events.Timestamp`,
    CAST([] AS Array(String)) AS `Events.Name`,
    CAST([] AS Array(Map(String, String))) AS `Events.Attributes`,
    CAST([] AS Array(String)) AS `Links.TraceId`,
    CAST([] AS Array(String)) AS `Links.SpanId`,
    CAST([] AS Array(String)) AS `Links.TraceState`,
    CAST([] AS Array(Map(String, String))) AS `Links.Attributes`
FROM s3(
    'http://minio:9000/obs-archive/*/*/traces/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, start_time_unix_nano Int64, trace_id String, span_id String,
     parent_span_id String, `span.name` String, `span.kind` Int32,
     `status.code` Int32, `status.message` String, duration_ns Int64,
     `service.name` String, `scope.name` String,
     `deployment.environment` String, `cloud.region` String,
     `host.name` String, `k8s.namespace.name` String,
     `k8s.deployment.name` String, `k8s.node.name` String,
     `http.method` String, `http.status_code` String,
     `http.url` String, `db.system` String,
     `db.statement` String,
     `resource.attributes` Map(String, String), `span.attributes` Map(String, String),
     `scope.attributes` Map(String, String)'
);

-- ==========================================================================
-- Convenience: raw views (no OTEL mapping) for ad-hoc analytics
-- ==========================================================================

CREATE OR REPLACE VIEW lakehouse.logs_raw AS
SELECT *
FROM s3(
    'http://minio:9000/obs-archive/*/*/logs/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, body String, severity_text String, severity_number Int32,
     `service.name` String, `k8s.namespace.name` String, `k8s.pod.name` String,
     `k8s.deployment.name` String, `k8s.node.name` String, `deployment.environment` String,
     `cloud.region` String, `host.name` String, trace_id String, span_id String,
     `_stream` String, `_stream_id` String, `scope.name` String,
     `resource.attributes` Map(String, String), `log.attributes` Map(String, String)'
);

CREATE OR REPLACE VIEW lakehouse.traces_raw AS
SELECT *
FROM s3(
    'http://minio:9000/obs-archive/*/*/traces/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, start_time_unix_nano Int64, trace_id String, span_id String,
     parent_span_id String, `span.name` String, `span.kind` Int32,
     `status.code` Int32, `status.message` String, duration_ns Int64,
     `service.name` String, `scope.name` String,
     `deployment.environment` String, `cloud.region` String,
     `host.name` String, `k8s.namespace.name` String,
     `k8s.deployment.name` String, `k8s.node.name` String,
     `http.method` String, `http.status_code` String,
     `http.url` String, `db.system` String,
     `db.statement` String,
     `resource.attributes` Map(String, String), `span.attributes` Map(String, String),
     `scope.attributes` Map(String, String)'
);

-- ==========================================================================
-- Tenant-scoped views (direct s3() calls — _file virtual column not
-- available through view chain, so each tenant gets its own glob)
-- ==========================================================================

CREATE OR REPLACE VIEW lakehouse.logs_tenant_default AS
SELECT *
FROM s3(
    'http://minio:9000/obs-archive/0/0/logs/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, body String, severity_text String, severity_number Int32,
     `service.name` String, `k8s.namespace.name` String, `k8s.pod.name` String,
     `k8s.deployment.name` String, `k8s.node.name` String, `deployment.environment` String,
     `cloud.region` String, `host.name` String, trace_id String, span_id String,
     `_stream` String, `_stream_id` String, `scope.name` String,
     `resource.attributes` Map(String, String), `log.attributes` Map(String, String)'
);

CREATE OR REPLACE VIEW lakehouse.traces_tenant_default AS
SELECT *
FROM s3(
    'http://minio:9000/obs-archive/0/0/traces/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, start_time_unix_nano Int64, trace_id String, span_id String,
     parent_span_id String, `span.name` String, `span.kind` Int32,
     `status.code` Int32, `status.message` String, duration_ns Int64,
     `service.name` String, `scope.name` String,
     `deployment.environment` String, `cloud.region` String,
     `host.name` String, `k8s.namespace.name` String,
     `k8s.deployment.name` String, `k8s.node.name` String,
     `http.method` String, `http.status_code` String,
     `http.url` String, `db.system` String,
     `db.statement` String,
     `resource.attributes` Map(String, String), `span.attributes` Map(String, String),
     `scope.attributes` Map(String, String)'
);

CREATE OR REPLACE VIEW lakehouse.logs_tenant_test AS
SELECT *
FROM s3(
    'http://minio:9000/obs-archive/1/1/logs/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, body String, severity_text String, severity_number Int32,
     `service.name` String, `k8s.namespace.name` String, `k8s.pod.name` String,
     `k8s.deployment.name` String, `k8s.node.name` String, `deployment.environment` String,
     `cloud.region` String, `host.name` String, trace_id String, span_id String,
     `_stream` String, `_stream_id` String, `scope.name` String,
     `resource.attributes` Map(String, String), `log.attributes` Map(String, String)'
);

CREATE OR REPLACE VIEW lakehouse.traces_tenant_test AS
SELECT *
FROM s3(
    'http://minio:9000/obs-archive/1/1/traces/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, start_time_unix_nano Int64, trace_id String, span_id String,
     parent_span_id String, `span.name` String, `span.kind` Int32,
     `status.code` Int32, `status.message` String, duration_ns Int64,
     `service.name` String, `scope.name` String,
     `deployment.environment` String, `cloud.region` String,
     `host.name` String, `k8s.namespace.name` String,
     `k8s.deployment.name` String, `k8s.node.name` String,
     `http.method` String, `http.status_code` String,
     `http.url` String, `db.system` String,
     `db.statement` String,
     `resource.attributes` Map(String, String), `span.attributes` Map(String, String),
     `scope.attributes` Map(String, String)'
);
