# Getting Started

Victoria Lakehouse serves cold observability data from Parquet files on S3. It runs as a single binary in either `logs` or `traces` mode.

## Prerequisites

- S3-compatible storage (AWS S3, MinIO, Cloudflare R2) with Parquet files in Hive partition layout
- Go 1.23+ (for building from source)
- Docker (for container deployment)
- Helm 3 (for Kubernetes deployment)

## Installation

### Binary

```bash
# Build from source
git clone https://github.com/ReliablyObserve/victoria-lakehouse.git
cd victoria-lakehouse
make build

# Run
./bin/lakehouse \
  --lakehouse.mode=logs \
  --lakehouse.s3.bucket=obs-archive \
  --lakehouse.s3.region=us-east-1
```

### Docker

```bash
docker run -p 9428:9428 \
  ghcr.io/reliablyobserve/victoria-lakehouse:latest \
  --lakehouse.mode=logs \
  --lakehouse.s3.bucket=obs-archive \
  --lakehouse.s3.region=us-east-1
```

For MinIO (local development):

```bash
docker run -p 9428:9428 \
  ghcr.io/reliablyobserve/victoria-lakehouse:latest \
  --lakehouse.mode=logs \
  --lakehouse.s3.bucket=obs-archive \
  --lakehouse.s3.endpoint=http://minio:9000 \
  --lakehouse.s3.access-key=minioadmin \
  --lakehouse.s3.secret-key=minioadmin \
  --lakehouse.s3.force-path-style=true
```

### Docker Compose (E2E with MinIO)

```bash
docker compose -f deployment/docker/docker-compose-e2e.yml up
```

This starts:
- MinIO (S3-compatible) on port 9000/9001
- Victoria Lakehouse (logs mode) on port 9428
- Victoria Lakehouse (traces mode) on port 10428

### Helm

```bash
# Logs mode
helm install lakehouse-logs oci://ghcr.io/reliablyobserve/charts/victoria-lakehouse \
  --set mode=logs \
  --set s3.bucket=obs-archive \
  --set s3.region=us-east-1

# Traces mode
helm install lakehouse-traces oci://ghcr.io/reliablyobserve/charts/victoria-lakehouse \
  --set mode=traces \
  --set s3.bucket=obs-archive \
  --set s3.region=us-east-1
```

With auto-discovery (recommended for cluster mode):

```bash
helm install lakehouse-logs oci://ghcr.io/reliablyobserve/charts/victoria-lakehouse \
  --set mode=logs \
  --set s3.bucket=obs-archive \
  --set s3.region=us-east-1 \
  --set discovery.headlessService=vlstorage.monitoring.svc.cluster.local \
  --set discovery.partitionAuthKey=secret
```

## Deployment Patterns

### Pattern 1: Multi-Select Storage Node (Recommended)

Register Victoria Lakehouse as a `-storageNode` on vlselect/vtselect:

```bash
# VictoriaLogs cluster
vlselect --storageNode=vlstorage-1:9428,vlstorage-2:9428,lakehouse-logs:9428

# VictoriaTraces cluster
vtselect --storageNode=vtstorage-1:10428,vtstorage-2:10428,lakehouse-traces:10428
```

Victoria Lakehouse auto-discovers the hot boundary by polling storage nodes' `/internal/partition/list` endpoint. Queries within the hot range get an empty response in <1ms.

### Pattern 2: Direct Grafana Query (Standalone)

Point Grafana datasources directly at Victoria Lakehouse:

```yaml
datasources:
  - name: Cold Logs
    type: victorialogs-datasource
    url: http://lakehouse-logs:9428

  - name: Cold Traces
    type: jaeger
    url: http://lakehouse-traces:10428
```

### Pattern 3: Loki-VL-proxy Upstream

Route cold queries through Loki-VL-proxy:

```
COLD_BACKEND_URL=http://lakehouse-logs:9428
COLD_BOUNDARY=7d
COLD_ENABLED=true
```

## YAML Config File

Instead of flags, use a YAML config:

```yaml
# /etc/lakehouse/config.yaml
lakehouse:
  mode: logs
  s3:
    bucket: obs-archive
    region: us-east-1
  cache:
    memory_limit: 1GB
    disk_path: /data/lakehouse/cache
    disk_limit: 100GB
  discovery:
    headless_service: vlstorage.monitoring.svc.cluster.local
    partition_auth_key: secret
```

```bash
lakehouse --lakehouse.config=/etc/lakehouse/config.yaml
```

CLI flags override YAML values.

## Verifying the Setup

After starting, check these endpoints:

```bash
# Liveness (always 200 once HTTP server starts)
curl http://localhost:9428/health

# Readiness (200 after startup warmup completes)
curl http://localhost:9428/ready

# Data range served
curl http://localhost:9428/manifest/range

# Build and config info
curl http://localhost:9428/lakehouse/info

# Prometheus metrics
curl http://localhost:9428/metrics
```

## Next Steps

- [Configuration Reference](configuration.md) — all 55+ flags with defaults
- [Architecture](architecture.md) — internal design, Parquet schema, query flow
- [Operations](operations.md) — day-2 operations, scaling, troubleshooting
- [Security](security.md) — hardening, network policies, credential handling
