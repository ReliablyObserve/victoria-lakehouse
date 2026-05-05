---
title: Docker Compose Setup
sidebar_position: 7
---

# Docker Compose Setup

Victoria Lakehouse provides a complete Docker Compose environment for local development, testing, and evaluation. The compose file at `deployment/docker/docker-compose-e2e.yml` starts all components needed for an end-to-end workflow: S3-compatible storage, data generation, log and trace lakehouse instances, a hot VictoriaLogs tier, multi-level select, a Loki-compatible proxy, and Grafana with pre-configured datasources.

## Quick Start

```bash
# Clone the repository
git clone https://github.com/ReliablyObserve/victoria-lakehouse.git
cd victoria-lakehouse

# Build and start all services
docker compose -f deployment/docker/docker-compose-e2e.yml up --build

# Or run in background
docker compose -f deployment/docker/docker-compose-e2e.yml up --build -d
```

Once all services are healthy, open Grafana at [http://localhost:3003](http://localhost:3003) (anonymous admin access enabled).

## Services

The compose file defines the following services on a shared `lakehouse-net` bridge network:

### MinIO (S3-compatible storage)

```yaml
minio:
  image: minio/minio:latest
  command: server /data --console-address ":9001"
  environment:
    MINIO_ROOT_USER: minioadmin
    MINIO_ROOT_PASSWORD: minioadmin
```

MinIO provides S3-compatible object storage. The `minio-init` sidecar creates the `obs-archive` bucket automatically on first start using the MinIO CLI (`mc mb local/obs-archive`).

- **API endpoint**: `http://minio:9000` (internal)
- **Console**: not exposed by default; add `ports: ["9001:9001"]` to access the web UI

### Data Generation

Two datagen services populate the environment with realistic test data:

**datagen-seed** runs once on startup and writes 48 hours of historical data:

```bash
--logs=5000 --traces=1000 --hours-back=48 --dual-write --vl-endpoint=http://victorialogs:9428
```

This writes Parquet files directly to S3 (MinIO) and also pushes logs to VictoriaLogs via `/insert/jsonline` for dual-write testing.

**datagen-continuous** runs indefinitely after seeding completes, generating fresh data every 30 seconds:

```bash
--logs=500 --traces=200 --hours-back=1 --interval=30s --dual-write
```

The datagen tool produces five realistic log patterns (JSON access logs, logfmt, nginx combined, Java stack traces, OTEL-formatted) across five services (`api-gateway`, `user-service`, `order-service`, `payment-service`, `notification-service`) with full OTEL semantic convention attributes.

### Lakehouse Logs

```yaml
lakehouse-logs:
  command:
    - "-lakehouse.mode=logs"
    - "-lakehouse.s3.bucket=obs-archive"
    - "-lakehouse.s3.endpoint=http://minio:9000"
    - "-lakehouse.s3.access-key=minioadmin"
    - "-lakehouse.s3.secret-key=minioadmin"
    - "-lakehouse.s3.force-path-style=true"
    - "-lakehouse.manifest.refresh-interval=30s"
```

Serves VictoriaLogs-compatible select APIs backed by Parquet files on MinIO. The manifest refreshes every 30 seconds to pick up newly generated data quickly.

- **Internal endpoint**: `http://lakehouse-logs:9428`
- **Health check**: `GET /health` every 5 seconds

### Lakehouse Traces

```yaml
lakehouse-traces:
  command:
    - "-lakehouse.mode=traces"
    - "-lakehouse.s3.endpoint=http://minio:9000"
    - "-lakehouse.s3.force-path-style=true"
```

Serves Jaeger-compatible trace query APIs backed by the same MinIO bucket.

- **Internal endpoint**: `http://lakehouse-traces:10428`
- **Health check**: `GET /health` every 5 seconds

### VictoriaLogs (Hot Tier)

```yaml
victorialogs:
  image: victoriametrics/victoria-logs:v1.20.0-victorialogs
  command:
    - "-storageDataPath=/data"
    - "-retentionPeriod=7d"
```

A standalone VictoriaLogs instance acting as the hot tier with 7-day retention and EBS-equivalent local storage.

### vlselect (Multi-Level Select)

```yaml
vlselect:
  command:
    - "-storageNode=victorialogs:9428"
    - "-storageNode=lakehouse-logs:9428"
    - "-httpListenAddr=:9471"
```

Fans out queries to both the hot VictoriaLogs instance and cold lakehouse-logs, demonstrating the multi-level select architecture. Recent data comes from VictoriaLogs (fast, EBS-backed), while historical data comes from lakehouse (S3-backed).

- **Internal endpoint**: `http://vlselect:9471`

### Loki-VL-proxy

```yaml
loki-vl-proxy:
  image: ghcr.io/reliablyobserve/loki-vl-proxy:latest
  environment:
    BACKEND_URL: "http://lakehouse-logs:9428"
```

Translates Loki API requests to VictoriaLogs API, allowing Grafana's built-in Loki datasource to query lakehouse data using LogQL syntax.

- **Internal endpoint**: `http://loki-vl-proxy:3100`

### Grafana

```yaml
grafana:
  image: grafana/grafana:latest
  ports:
    - "3003:3000"
  environment:
    GF_AUTH_ANONYMOUS_ENABLED: "true"
    GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
    GF_INSTALL_PLUGINS: "victoriametrics-logs-datasource"
```

Pre-configured with five datasources via provisioning files in `deployment/docker/grafana/provisioning/`:

| Datasource | Type | URL | Purpose |
|---|---|---|---|
| Victoria Lakehouse Logs (Cold) | VictoriaLogs | `http://lakehouse-logs:9428` | Direct cold tier queries |
| Victoria Lakehouse Traces (Jaeger) | Jaeger | `http://lakehouse-traces:10428` | Trace search and visualization |
| VictoriaLogs Hot | VictoriaLogs | `http://victorialogs:9428` | Hot tier only |
| Multi-Level Select (Hot+Cold) | VictoriaLogs | `http://vlselect:9471` | Unified hot + cold |
| Loki via Proxy | Loki | `http://loki-vl-proxy:3100` | LogQL-compatible access |

- **Grafana UI**: [http://localhost:3003](http://localhost:3003)

## Volumes

The compose file defines four named volumes:

| Volume | Mount | Purpose |
|---|---|---|
| `vl-data` | `/data` on victorialogs | VictoriaLogs hot storage |
| `lakehouse-cache-logs` | `/data/lakehouse` on lakehouse-logs | L2 disk cache + manifest persistence |
| `lakehouse-cache-traces` | `/data/lakehouse` on lakehouse-traces | L2 disk cache + manifest persistence |
| `grafana-data` | `/var/lib/grafana` on grafana | Grafana state |

## Startup Order

The compose file uses health checks and `depends_on` conditions to ensure correct startup order:

1. **minio** starts and becomes healthy
2. **minio-init** creates the `obs-archive` bucket, then exits
3. **victorialogs** starts and becomes healthy
4. **datagen-seed** writes historical data to MinIO and VictoriaLogs, then exits
5. **lakehouse-logs** and **lakehouse-traces** start (depend on seed completion)
6. **datagen-continuous** begins generating fresh data every 30 seconds
7. **vlselect** and **loki-vl-proxy** start (depend on their backends)
8. **grafana** starts last (depends on both lakehouse services)

## Verifying the Setup

After all services are healthy:

```bash
# Check lakehouse health
curl http://localhost:9428/health    # if ports are exposed
docker compose -f deployment/docker/docker-compose-e2e.yml exec lakehouse-logs \
  /usr/local/bin/healthcheck http://localhost:9428/health

# Check data availability
docker compose -f deployment/docker/docker-compose-e2e.yml exec lakehouse-logs \
  wget -qO- http://localhost:9428/manifest/range

# Query logs via the lakehouse
docker compose -f deployment/docker/docker-compose-e2e.yml exec lakehouse-logs \
  wget -qO- "http://localhost:9428/select/logsql/query?query=*&limit=5"
```

## Customizing for Development

To test code changes, the compose file builds from the repository root using `Dockerfile` (for lakehouse) and `Dockerfile.datagen` (for datagen):

```bash
# Rebuild after code changes
docker compose -f deployment/docker/docker-compose-e2e.yml up --build lakehouse-logs lakehouse-traces

# View logs
docker compose -f deployment/docker/docker-compose-e2e.yml logs -f lakehouse-logs

# Stop everything and clean up volumes
docker compose -f deployment/docker/docker-compose-e2e.yml down -v
```

To expose lakehouse ports directly for local development tools:

```yaml
# Add to lakehouse-logs service
ports:
  - "9428:9428"

# Add to lakehouse-traces service
ports:
  - "10428:10428"

# Add to minio service (for DuckDB/analytics access)
ports:
  - "9000:9000"
  - "9001:9001"
```
