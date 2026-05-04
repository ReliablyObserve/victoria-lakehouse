# Victoria Lakehouse

[![CI](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/ci.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/ci.yaml)
[![Security](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/security.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/security.yaml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/ReliablyObserve/victoria-lakehouse)](https://go.dev/)
[![Release](https://img.shields.io/github/v/release/ReliablyObserve/victoria-lakehouse)](https://github.com/ReliablyObserve/victoria-lakehouse/releases)
[![Lines of Code](https://img.shields.io/badge/go%20loc-24.2k-blue)](https://github.com/ReliablyObserve/victoria-lakehouse)
[![Tests](https://img.shields.io/badge/tests-832%20passed-brightgreen)](#tests)
[![License](https://img.shields.io/github/license/ReliablyObserve/victoria-lakehouse)](LICENSE)

**S3-backed cold storage for VictoriaLogs and VictoriaTraces.** Read and write historical observability data as Parquet files on S3, while existing VL/VT clusters handle hot data on EBS.

- **Drop-in VL/VT storage node.** Register as a `-storageNode` on vlselect/vtselect. Queries spanning hot and cold data work transparently.
- **Write path with crash recovery.** VL-compatible insert APIs (`/insert/jsonline`, Loki push, ES bulk) buffer data, flush to S3 Parquet, and survive process crashes via WAL.
- **Zero-delay reads.** Select pods query insert pods for unflushed buffer data, merging with S3 results for immediate read-after-write visibility.
- **60-96% cost reduction.** S3 is 3-6x cheaper than EBS per GB. At 1 PB/month with multi-AZ, hybrid deployments save $3-9.5M/year compared to all-EBS.
- **Sub-millisecond fast path.** Queries within the hot tier's range get an immediate empty response via the partition manifest. Zero S3 I/O.
- **Disaster recovery.** When the hot cluster is down (outage, upgrade, migration), lakehouse serves all data from S3 — slower but always available.
- **Open Parquet files.** DuckDB, Trino, Spark, and ClickHouse read the same files directly for analytics, compliance, and ML.

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

## Architecture

Victoria Lakehouse is a clean-room reimplementation of VL/VT APIs backed by Parquet files on S3. It integrates with vlagent (logs) and OTEL Collector (traces) to mirror data to both hot and cold tiers simultaneously, providing unlimited retention, disaster recovery, and open-format analytics.

```mermaid
graph TB
    subgraph "Data Collection"
        K8S["Kubernetes Pods /<br/>Infrastructure"] --> VA["vlagent<br/>(logs)"]
        APP["Applications<br/>(OTEL SDK)"] --> OC["OTEL Collector<br/>(traces)"]
    end

    subgraph "Hot Tier — 1 Month (EBS, multi-AZ)"
        VA -->|mirror 1| VLI["vlinsert"]
        OC -->|export 1| VTI["vtinsert"]
        VLI --> VLSTO["vlstorage"]
        VTI --> VTSTO["vtstorage"]
        VLSEL["vlselect"]
        VTSEL["vtselect"]
        VLSEL --> VLSTO
        VTSEL --> VTSTO
    end

    subgraph "Cold Tier — Unlimited (S3) — Victoria Lakehouse"
        VA -->|mirror 2| LHI["lakehouse-insert"]
        OC -->|export 2| LHI
        LHI --> WAL["WAL"]
        WAL --> BUF["Buffers"]
        BUF -->|flush| S3[("S3 Parquet<br/>(11 nines)")]
        LHS["lakehouse-select"]
        LHS --> S3
        LHS -.->|buffer query| BUF
    end

    subgraph "Consumers"
        GF["Grafana"] --> VLSEL
        GF --> VTSEL
        VLSEL -->|cold fan-out| LHS
        VTSEL -->|cold fan-out| LHS
        DDB["DuckDB / Trino<br/>Spark / ClickHouse"] --> S3
    end

    style S3 fill:#e76f51,color:#fff
    style LHI fill:#5a189a,color:#fff
    style LHS fill:#2d6a4f,color:#fff
    style VA fill:#264653,color:#fff
    style OC fill:#264653,color:#fff
```

**Key points:**
- **vlagent** mirrors logs to both VictoriaLogs (hot, 1 month, EBS) and Lakehouse (cold, unlimited, S3)
- **OTEL Collector** fans out traces to both VictoriaTraces (hot) and Lakehouse (cold)
- **vlselect/vtselect** transparently fan out queries to hot + cold — users see unified results
- **Lakehouse as DR**: when hot cluster is down, Grafana queries lakehouse directly (slower but always available)
- **Open Parquet**: DuckDB, Trino, Spark, ClickHouse query S3 directly for analytics, compliance, ML

For detailed collector configs and DR playbooks, see [Deployment Architecture](docs/deployment-architecture.md).

### Query Flow

```mermaid
flowchart LR
    Q["Query"] --> M{"Manifest<br/>Check"}
    M -->|"Within hot<br/>boundary"| E["Empty<br/>Response<br/>&lt;1ms"]
    M -->|"Has cold<br/>data"| P["Resolve<br/>Partitions"]
    P --> RG{"Row Group<br/>Stats"}
    RG -->|"No match"| Skip["Skip"]
    RG -->|"Match"| BF{"Bloom<br/>Filter"}
    BF -->|"Absent"| Skip
    BF -->|"Possible"| Col["Read<br/>Columns"]
    Col --> Filt["Apply<br/>Filters"]
    Filt --> DB["Emit<br/>DataBlocks"]

    style E fill:#2d6a4f,color:#fff
    style Skip fill:#6c757d,color:#fff
```

### Multi-Tier Cache

```mermaid
flowchart TD
    Q["Query"] --> L1{"L1: Memory<br/>&lt;10ms"}
    L1 -->|Hit| R["Return"]
    L1 -->|Miss| L2{"L2: Disk (EBS)<br/>&lt;50ms"}
    L2 -->|Hit| R
    L2 -->|Miss| L3{"L3: Peer Cache<br/>&lt;30ms"}
    L3 -->|Hit| R
    L3 -->|Miss| L4["L4: S3<br/>50-150ms"]
    L4 --> R

    style L1 fill:#2d6a4f,color:#fff
    style L2 fill:#264653,color:#fff
    style L3 fill:#5a189a,color:#fff
    style L4 fill:#e76f51,color:#fff
```

### Deployment Patterns

```mermaid
graph LR
    subgraph "Pattern 1: Hot+Cold with vlagent/OTEL (recommended)"
        VA["vlagent"] -->|mirror| VLI["vlinsert<br/>(hot)"]
        VA -->|mirror| LHI["lakehouse-insert<br/>(cold)"]
        OC["OTEL<br/>Collector"] -->|export| VTI["vtinsert"]
        OC -->|export| LHI
        G1["Grafana"] --> VS1["vlselect"]
        VS1 --> VLS1["vlstorage"]
        VS1 -->|cold| LHS1["lakehouse-select"]
    end
```

```mermaid
graph LR
    subgraph "Pattern 2: Standalone (single binary)"
        C2["Clients"] -->|insert| LH2["lakehouse<br/>role=all"]
        G2["Grafana"] -->|select| LH2
        LH2 --> S2[("S3")]
    end
```

```mermaid
graph LR
    subgraph "Pattern 3: Scaled insert + select"
        C3["Clients"] --> LHI3["insert-0,1,2"]
        LHI3 --> S3[("S3")]
        G3["Grafana"] --> LHS3["select-0,1,2"]
        LHS3 --> S3
        LHS3 -.->|buffer query| LHI3
    end
```

```mermaid
graph LR
    subgraph "Pattern 4: Disaster Recovery"
        G4["Grafana"] --> VMA["vmauth<br/>first_available"]
        VMA -->|primary| VS4["vlselect"]
        VMA -->|fallback| LH4["lakehouse-select"]
    end
```

```mermaid
graph LR
    subgraph "Pattern 5: Analytics (open Parquet)"
        S5[("S3 Parquet")] --> DDB["DuckDB"]
        S5 --> TRI["Trino"]
        S5 --> SPK["Spark"]
        S5 --> CH["ClickHouse"]
    end
```

---

## Modes and Roles

Single binary, two modes, three roles:

| Mode | Flag | Port | API | Use Case |
|---|---|---|---|---|
| Logs | `--lakehouse.mode=logs` | 9428 | VL `/select/logsql/*` + `/insert/*` | Cold log storage |
| Traces | `--lakehouse.mode=traces` | 10428 | VT `/select/logsql/*` + Jaeger | Cold trace storage |

| Role | Flag | Description |
|---|---|---|
| All | `--lakehouse.role=all` (default) | Insert + select in one process |
| Insert | `--lakehouse.role=insert` | Write path only, flush to S3 |
| Select | `--lakehouse.role=select` | Read path only, query S3 + buffers |

---

## Key Features

### Write Path
- **VL-compatible insert APIs**: `/insert/jsonline`, `/insert/loki/api/v1/push`, `/insert/elasticsearch/_bulk` — same protocols as VictoriaLogs.
- **Write-ahead log (WAL)**: crash-safe durability with gob-encoded append-only log and automatic replay on restart.
- **Adaptive file sizing**: per-partition byte estimates trigger flush when approaching `--lakehouse.insert.target-file-size` for optimal Parquet file sizes.
- **Buffer query bridge**: select pods fan out to insert pods via `/internal/buffer/query` for zero-delay reads of unflushed data.
- **Manifest label pruning**: `FileInfo.Labels` enables query-time file skipping based on label values without opening Parquet files.

### Read Path
- **Auto-discovery of hot boundary** via `/internal/partition/list` on vlstorage/vtstorage. Zero manual config.
- **Partition manifest** for sub-ms "nothing here" responses. Recent queries cost zero S3 I/O.
- **Bloom filters** on `trace_id` and `service_name` for fast point lookups.
- **Correlated prefetch**: log query warms trace Parquet for same time+service, and vice versa.
- **Read-ahead**: sequential time scans prefetch next partitions.

### Infrastructure
- **Metadata persistence**: manifest, label index, and cache survive restarts.
- **Distributed peer cache**: consistent hash routing across fleet instances via headless DNS.
- **Schema auto-discovery**: OTLP column names in Parquet, mapped to VL/VT names at query time.
- **SQS/SNS support**: optional near-real-time manifest updates from S3 event notifications.

---

## Configuration

Minimal config (mode + S3 bucket) works out of the box. All 65+ flags have production-ready defaults.

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
- [Getting Started](docs/getting-started.md) — quick start, ingestion, deployment patterns
- [Deployment Architecture](docs/deployment-architecture.md) — vlagent, OTEL Collector, hot/cold tiers, DR
- [Configuration](docs/configuration.md) — all 65+ flags with defaults
- [Architecture](docs/architecture.md) — internal design, Parquet schema, query flow
- [Operations](docs/operations.md) — day-2 operations, scaling, troubleshooting

### Use Cases & Analytics
- [Use Cases](docs/use-cases.md) — DR, compliance, capacity planning, cost allocation, ML
- [Analytics](docs/analytics.md) — DuckDB, Trino, Spark, ClickHouse, Pandas examples

### Operations
- [Security](docs/security.md) — hardening, network policies, credential handling
- [Observability](docs/observability.md) — metrics, dashboards, alerting rules
- [Performance](docs/performance.md) — benchmarks, tuning, targets
- [Scaling](docs/scaling.md) — horizontal and vertical scaling guides
- [Cost Estimates](docs/cost-estimates.md) — EBS vs S3 cost comparison

---

## Current Status

| Milestone | Status | Key Deliverables |
|---|---|---|
| M1: Foundation | Complete | Go module, config, CI/CD, Helm chart, Dockerfile |
| M2: ParquetS3Storage Core | Complete | Schema registry, manifest, query engine, bloom filters, column projection, stream methods |
| M3: Cache + Persistence | Complete | L1 memory LRU, L2 disk LRU, singleflight coalescence, label index, metadata persistence |
| M4: Discovery + Peer Cache | Complete | Hot boundary auto-discovery, consistent hash peer cache, `/manifest/range` API |
| M5: VL/VT Cluster Integration | Complete | `/internal/select/*` binary protocol, storage node registration |
| M6: Filter AST + E2E | Complete | Full LogsQL predicate engine, Playwright E2E, schema validation |
| M8-Phase A: Write Durability | Complete | WAL crash recovery, insert APIs, adaptive flush, buffer query bridge, manifest labels |
| M7: Observability | Planned | Metrics instrumentation, Grafana dashboards, alerting rules |

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
