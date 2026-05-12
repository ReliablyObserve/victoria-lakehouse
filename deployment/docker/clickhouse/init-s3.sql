-- Pre-configure S3/MinIO access for Parquet analytics
-- These views let Grafana users query lakehouse data without knowing S3 credentials

-- Create a database for lakehouse analytics
CREATE DATABASE IF NOT EXISTS lakehouse;

-- Logs view: query all log Parquet files from MinIO (all tenants)
CREATE OR REPLACE VIEW lakehouse.logs AS
SELECT *
FROM s3(
  'http://minio:9000/obs-archive/*/logs/**/*.parquet',
  'minioadmin', 'minioadmin', 'Parquet'
);

-- Traces view: query all trace Parquet files from MinIO (all tenants)
CREATE OR REPLACE VIEW lakehouse.traces AS
SELECT *
FROM s3(
  'http://minio:9000/obs-archive/*/traces/**/*.parquet',
  'minioadmin', 'minioadmin', 'Parquet'
);

-- Tenant-scoped views: default tenant (0/0)
CREATE OR REPLACE VIEW lakehouse.logs_tenant_default AS
SELECT *
FROM s3(
  'http://minio:9000/obs-archive/0/0/logs/**/*.parquet',
  'minioadmin', 'minioadmin', 'Parquet'
);

CREATE OR REPLACE VIEW lakehouse.traces_tenant_default AS
SELECT *
FROM s3(
  'http://minio:9000/obs-archive/0/0/traces/**/*.parquet',
  'minioadmin', 'minioadmin', 'Parquet'
);

-- Tenant-scoped views: test tenant (1/1)
CREATE OR REPLACE VIEW lakehouse.logs_tenant_test AS
SELECT *
FROM s3(
  'http://minio:9000/obs-archive/1/1/logs/**/*.parquet',
  'minioadmin', 'minioadmin', 'Parquet'
);

CREATE OR REPLACE VIEW lakehouse.traces_tenant_test AS
SELECT *
FROM s3(
  'http://minio:9000/obs-archive/1/1/traces/**/*.parquet',
  'minioadmin', 'minioadmin', 'Parquet'
);

-- Logs for a specific date partition (example — use in Grafana with variables)
CREATE OR REPLACE VIEW lakehouse.logs_today AS
SELECT *
FROM s3(
  concat('http://minio:9000/obs-archive/0/0/logs/dt=', formatDateTime(today(), '%Y-%m-%d'), '/**/*.parquet'),
  'minioadmin', 'minioadmin', 'Parquet'
);

-- Traces for a specific date partition
CREATE OR REPLACE VIEW lakehouse.traces_today AS
SELECT *
FROM s3(
  concat('http://minio:9000/obs-archive/0/0/traces/dt=', formatDateTime(today(), '%Y-%m-%d'), '/**/*.parquet'),
  'minioadmin', 'minioadmin', 'Parquet'
);
