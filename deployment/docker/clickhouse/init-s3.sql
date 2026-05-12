-- ClickHouse OTEL-compatible views for Lakehouse S3 Parquet data
-- Maps Parquet column names to OpenTelemetry standard naming convention
-- Compatible with Grafana ClickHouse datasource OTEL mode

CREATE DATABASE IF NOT EXISTS lakehouse;

-- ==========================================================================
-- OTEL Logs — maps Parquet logs to OpenTelemetry log schema
-- ==========================================================================

CREATE OR REPLACE VIEW lakehouse.otel_logs AS
SELECT
    fromUnixTimestamp64Nano(timestamp_unix_nano) AS Timestamp,
    body AS Body,
    severity_text AS SeverityText,
    severity_number AS SeverityNumber,
    `service.name` AS ServiceName,
    trace_id AS TraceId,
    span_id AS SpanId,
    `scope.name` AS ScopeName,
    '' AS ScopeVersion,
    `_stream` AS LogStreamId,
    mapConcat(
        `resource.attributes`,
        mapFromArrays(
            ['k8s.namespace.name', 'k8s.pod.name', 'k8s.deployment.name',
             'k8s.node.name', 'deployment.environment', 'cloud.region', 'host.name'],
            [`k8s.namespace.name`, `k8s.pod.name`, `k8s.deployment.name`,
             `k8s.node.name`, `deployment.environment`, `cloud.region`, `host.name`]
        )
    ) AS ResourceAttributes,
    `log.attributes` AS LogAttributes,
    map() AS ScopeAttributes
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
-- OTEL Traces — maps Parquet traces to OpenTelemetry span schema
-- ==========================================================================

CREATE OR REPLACE VIEW lakehouse.otel_traces AS
SELECT
    fromUnixTimestamp64Nano(timestamp_unix_nano) AS Timestamp,
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
    `service.name` AS ServiceName,
    duration_ns / 1000000 AS Duration,
    CASE `status.code`
        WHEN 0 THEN 'STATUS_CODE_UNSET'
        WHEN 1 THEN 'STATUS_CODE_OK'
        WHEN 2 THEN 'STATUS_CODE_ERROR'
        ELSE 'STATUS_CODE_UNSET'
    END AS StatusCode,
    `status.message` AS StatusMessage,
    `scope.name` AS ScopeName,
    '' AS ScopeVersion,
    '' AS TraceState,
    mapConcat(
        `resource.attributes`,
        mapFromArrays(
            ['deployment.environment', 'cloud.region', 'host.name',
             'k8s.namespace.name', 'k8s.deployment.name', 'k8s.node.name'],
            [`resource_attr:deployment.environment`, `resource_attr:cloud.region`, `resource_attr:host.name`,
             `resource_attr:k8s.namespace.name`, `resource_attr:k8s.deployment.name`, `resource_attr:k8s.node.name`]
        )
    ) AS ResourceAttributes,
    mapConcat(
        `span.attributes`,
        mapFromArrays(
            ['http.method', 'http.status_code', 'http.url', 'db.system', 'db.statement'],
            [`span_attr:http.method`, `span_attr:http.status_code`, `span_attr:http.url`,
             `span_attr:db.system`, `span_attr:db.statement`]
        )
    ) AS SpanAttributes,
    map() AS ScopeAttributes,
    [] AS `Events.Timestamp`,
    [] AS `Events.Name`,
    [] AS `Events.Attributes`,
    [] AS `Links.TraceId`,
    [] AS `Links.SpanId`,
    [] AS `Links.TraceState`,
    [] AS `Links.Attributes`
FROM s3(
    'http://minio:9000/obs-archive/*/*/traces/dt=*/hour=*/*.parquet',
    'minioadmin', 'minioadmin', 'Parquet',
    'timestamp_unix_nano Int64, start_time_unix_nano Int64, trace_id String, span_id String,
     parent_span_id String, `span.name` String, `span.kind` Int32,
     `status.code` Int32, `status.message` String, duration_ns Int64,
     `service.name` String, `scope.name` String,
     `resource_attr:deployment.environment` String, `resource_attr:cloud.region` String,
     `resource_attr:host.name` String, `resource_attr:k8s.namespace.name` String,
     `resource_attr:k8s.deployment.name` String, `resource_attr:k8s.node.name` String,
     `span_attr:http.method` String, `span_attr:http.status_code` String,
     `span_attr:http.url` String, `span_attr:db.system` String,
     `span_attr:db.statement` String,
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
     `resource_attr:deployment.environment` String, `resource_attr:cloud.region` String,
     `resource_attr:host.name` String, `resource_attr:k8s.namespace.name` String,
     `resource_attr:k8s.deployment.name` String, `resource_attr:k8s.node.name` String,
     `span_attr:http.method` String, `span_attr:http.status_code` String,
     `span_attr:http.url` String, `span_attr:db.system` String,
     `span_attr:db.statement` String,
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
     `resource_attr:deployment.environment` String, `resource_attr:cloud.region` String,
     `resource_attr:host.name` String, `resource_attr:k8s.namespace.name` String,
     `resource_attr:k8s.deployment.name` String, `resource_attr:k8s.node.name` String,
     `span_attr:http.method` String, `span_attr:http.status_code` String,
     `span_attr:http.url` String, `span_attr:db.system` String,
     `span_attr:db.statement` String,
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
     `resource_attr:deployment.environment` String, `resource_attr:cloud.region` String,
     `resource_attr:host.name` String, `resource_attr:k8s.namespace.name` String,
     `resource_attr:k8s.deployment.name` String, `resource_attr:k8s.node.name` String,
     `span_attr:http.method` String, `span_attr:http.status_code` String,
     `span_attr:http.url` String, `span_attr:db.system` String,
     `span_attr:db.statement` String,
     `resource.attributes` Map(String, String), `span.attributes` Map(String, String),
     `scope.attributes` Map(String, String)'
);
