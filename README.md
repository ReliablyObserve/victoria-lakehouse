# Victoria Lakehouse

[![CI](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/ci.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/ci.yaml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/ReliablyObserve/victoria-lakehouse)](https://go.dev/)
[![Release](https://img.shields.io/github/v/release/ReliablyObserve/victoria-lakehouse)](https://github.com/ReliablyObserve/victoria-lakehouse/releases)
[![License](https://img.shields.io/github/license/ReliablyObserve/victoria-lakehouse)](LICENSE)

**S3-backed cold storage select for VictoriaLogs and VictoriaTraces.** Serve historical observability data from Parquet files on S3, while existing VL/VT clusters handle hot data on EBS.

- **Drop-in VL/VT storage node.** Register as a `-storageNode` on vlselect/vtselect. Queries spanning hot and cold data work transparently.
- **60-96% cost reduction.** S3 is 3-6x cheaper than EBS per GB. At 1 PB/month with multi-AZ, hybrid deployments save $3-9.5M/year compared to all-EBS.
- **Sub-millisecond fast path.** Queries within the hot tier's range get an immediate empty response via the partition manifest. Zero S3 I/O.
- **Open Parquet files.** DuckDB, Trino, Spark, and ClickHouse read the same files directly for analytics.

---

## The Cost Case

S3 storage is inherently multi-AZ (11 nines durability) at no extra cost. EBS requires per-AZ replicas for HA, tripling storage cost.

| Ingestion | Retention | All-EBS (3 AZ) | Hybrid (1mo hot + S3 cold) | Savings |
|---|---|---|---|---|
| 250 GB/mo | 1 year | $282/mo | $138/mo (all-S3) | 51% |
| 500 GB/mo | 1 year | $564/mo | $141/mo (all-S3) | 75% |
| 1 PB/mo | 1 year | $303,000/mo | $48,513/mo | 84% |
| 1 PB/mo | 2 years | $591,000/mo | $60,963/mo | 90% |

Full cost worksheet: [Cost Estimates](docs/cost-estimates.md)

---

## Quick Start

### Docker

```bash
docker run -p 9428:9428 \
  ghcr.io/reliablyobserve/victoria-lakehouse:latest \
  --lakehouse.mode=logs \
  --lakehouse.s3.bucket=obs-archive \
  --lakehouse.s3.region=us-east-1
```

### Docker Compose (with MinIO)

```bash
docker compose -f deployment/docker/docker-compose-e2e.yml up
```

### Helm

```bash
helm install lakehouse-logs oci://ghcr.io/reliablyobserve/charts/victoria-lakehouse \
  --set mode=logs \
  --set s3.bucket=obs-archive \
  --set s3.region=us-east-1 \
  --set discovery.headlessService=vlstorage.monitoring.svc.cluster.local \
  --set discovery.partitionAuthKey=secret
```

### Grafana Datasource (Direct Access)

Point a VictoriaLogs datasource directly at Victoria Lakehouse for standalone cold queries:

```yaml
datasources:
  - name: Cold Logs (Lakehouse)
    type: victorialogs-datasource
    access: proxy
    url: http://lakehouse-logs:9428
```

For full setup, cluster integration, and deployment patterns, see [Getting Started](docs/getting-started.md).

---

## How It Works

Victoria Lakehouse forks VictoriaLogs vlselect and replaces the storage layer with a Parquet/S3 backend. All 14 HTTP handlers, the LogsQL parser, and response serialization are reused unchanged. From the outside, it looks like a regular VL/VT storage node.

### Query Flow

```
1. Query arrives (via vlselect fan-out or direct)
2. Partition manifest check (<1ms, in-memory)
   - If query is within hot boundary -> return empty immediately
   - If query has cold data -> continue
3. Resolve Hive partitions (dt=YYYY-MM-DD/hour=HH)
4. For each Parquet file:
   a. Row group statistics -> skip non-matching groups
   b. Bloom filter check -> skip for point lookups (trace_id, service_name)
   c. Column projection -> read only needed columns
   d. Filter evaluation -> apply LogsQL filters
   e. Emit DataBlocks to callback
5. Pipe processors (stats, sort, limit) run on emitted DataBlocks
```

### Multi-Tier Cache

```
L1: Memory cache (footers, blooms, hot pages)      -> <10ms
L2: Local disk cache (EBS gp3, full Parquet files)  -> <50ms
L3: Distributed peer cache (consistent hash routing) -> <30ms
L4: S3 (source of truth, range reads)                -> 50-150ms
```

### Three Deployment Patterns

**Pattern 1: Multi-Select Storage Node (recommended)**
```
vlselect --storageNode=vlstorage-1,vlstorage-2,lakehouse-logs
```
Transparent integration. Zero changes to Grafana. Hot boundary auto-discovered.

**Pattern 2: Direct Grafana Query (standalone)**
```
Grafana -> lakehouse-logs:9428 (cold logs)
Grafana -> lakehouse-traces:10428 (cold traces)
```

**Pattern 3: Loki-VL-proxy Upstream**
```
Grafana -> Loki-VL-proxy -> hot: vlselect / cold: lakehouse-logs
```

---

## Modes

Single binary, two modes:

| Mode | Flag | Port | API | Use Case |
|---|---|---|---|---|
| Logs | `--lakehouse.mode=logs` | 9428 | VL `/select/logsql/*` | Cold log queries |
| Traces | `--lakehouse.mode=traces` | 10428 | VT `/select/logsql/*` + Jaeger | Cold trace queries |

---

## Key Features

- **Auto-discovery of hot boundary** via `/internal/partition/list` on vlstorage/vtstorage. Zero manual config.
- **Partition manifest** for sub-ms "nothing here" responses. Recent queries cost zero S3 I/O.
- **Bloom filters** on `trace_id` and `service_name` for fast point lookups.
- **Correlated prefetch**: log query warms trace Parquet for same time+service, and vice versa.
- **Read-ahead**: sequential time scans prefetch next partitions.
- **Metadata persistence**: manifest, label index, and cache survive restarts.
- **Distributed peer cache**: consistent hash routing across fleet instances via headless DNS.
- **Schema auto-discovery**: OTLP column names in Parquet, mapped to VL/VT names at query time.
- **SQS/SNS support**: optional near-real-time manifest updates from S3 event notifications.

---

## Configuration

Minimal config (mode + S3 bucket) works out of the box. All 55+ flags have production-ready defaults.

```yaml
lakehouse:
  mode: logs
  s3:
    bucket: obs-archive
    region: us-east-1
  discovery:
    headless_service: vlstorage.monitoring.svc.cluster.local
    partition_auth_key: "${PARTITION_AUTH_KEY}"
```

Full reference: [Configuration](docs/configuration.md)

---

## Observability

- **~80 Prometheus metrics** under `lakehouse_*` prefix (RED, USE, S3, cache, peer, manifest, Parquet engine, prefetch, startup)
- **Grafana dashboards** (single-instance + cluster + supplementary panels for VL/VT dashboards)
- **10 alerting rules** with severity and annotations
- **Structured JSON logs** via `slog`

See [Observability](docs/observability.md).

---

## Security

- **Distroless runtime image** (`gcr.io/distroless/static-debian12:nonroot`) — no shell, no package manager
- **Non-root execution** (UID 65534)
- **Read-only root filesystem** in Kubernetes
- **Stripped binaries** (`-s -w` linker flags)
- **Drop all capabilities** (`capabilities.drop: ["ALL"]`)
- **Seccomp profile** (`RuntimeDefault`)
- **CI security gates**: govulncheck, gosec, Trivy, gitleaks, CodeQL

See [Security](docs/security.md).

---

## Parquet Schema

Victoria Lakehouse reads OTLP-standard Parquet files. Column names use **OTEL semantic convention dot-notation** directly (e.g., `service.name`, `k8s.namespace.name`) for zero-translation compatibility with OTEL Collector exporters and standard tooling. High-frequency fields are promoted to top-level columns (with statistics + bloom filters). Everything else goes in MAP columns.

| Promoted (Logs) | Promoted (Traces) |
|---|---|
| `timestamp_unix_nano` | `timestamp_unix_nano`, `start_time_unix_nano` |
| `body`, `severity_text` | `trace_id`, `span_id`, `parent_span_id` |
| `service.name` | `span.name`, `service.name` |
| `k8s.namespace.name`, `k8s.pod.name` | `status.code`, `duration_ns` |
| `trace_id`, `span_id` | `resource.attributes` (MAP) |
| `resource.attributes`, `log.attributes` (MAP) | `span.attributes`, `scope.attributes` (MAP) |

S3 layout: Hive partitioned by `dt=YYYY-MM-DD/hour=HH`.

Full schema reference: [Architecture](docs/architecture.md)

---

## Performance Targets

| Operation | Target p95 |
|---|---|
| Manifest "nothing here" fast path | <1ms |
| Point query (trace_id, bloom filter) | <100ms |
| Time-range scan (1h) | <500ms |
| stats_query (aggregation) | <300ms |
| field_names / field_values | <1ms (label index) |

See [Performance](docs/performance.md).

---

## Documentation

### Core
- [Getting Started](docs/getting-started.md)
- [Configuration](docs/configuration.md)
- [Architecture](docs/architecture.md)
- [Operations](docs/operations.md)
- [Security](docs/security.md)
- [Observability](docs/observability.md)
- [Performance](docs/performance.md)
- [Scaling](docs/scaling.md)
- [Cost Estimates](docs/cost-estimates.md)

---

## Development

```bash
make build          # Build binary + healthcheck
make test           # Run unit tests with race detector
make lint           # golangci-lint
make docker         # Build container image
make e2e            # Full E2E with MinIO + VL cluster
```

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
