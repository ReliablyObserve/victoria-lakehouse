<p align="center">
  <img src="docs/logo.png" alt="Victoria Lakehouse" width="200">
</p>

# Victoria Lakehouse

[![CI](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/ci.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/ci.yaml)
[![Security](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/security.yaml/badge.svg?branch=main&event=push)](https://github.com/ReliablyObserve/victoria-lakehouse/actions/workflows/security.yaml)
[![Go Version](https://img.shields.io/github/go-mod/go-version/ReliablyObserve/victoria-lakehouse)](https://go.dev/)
[![Release](https://img.shields.io/github/v/release/ReliablyObserve/victoria-lakehouse)](https://github.com/ReliablyObserve/victoria-lakehouse/releases)
[![Prod Code](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ReliablyObserve/victoria-lakehouse/badges/prod-loc.json)](https://github.com/ReliablyObserve/victoria-lakehouse)
[![Test Code](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ReliablyObserve/victoria-lakehouse/badges/test-loc.json)](https://github.com/ReliablyObserve/victoria-lakehouse)
[![Tests](https://img.shields.io/endpoint?url=https://raw.githubusercontent.com/ReliablyObserve/victoria-lakehouse/badges/tests.json)](#tests)
[![License](https://img.shields.io/github/license/ReliablyObserve/victoria-lakehouse)](LICENSE)

**S3-backed cold storage for VictoriaLogs and VictoriaTraces.** Two dedicated binaries — `lakehouse-logs` and `lakehouse-traces` — each 100% API-compatible with VL/VT. Same endpoints, same protocols, same query language. Implements the VL/VT storage interface with an S3 Parquet backend. Registers as a `-storageNode` and works transparently alongside existing VL/VT clusters.

> **Two binaries, one architecture.** `lakehouse-logs` reimplements the VL storage layer. `lakehouse-traces` reimplements the VT storage layer. Both use Parquet on S3 and expose identical HTTP APIs, LogsQL query syntax, binary DataBlock protocol, and insert endpoints as their upstream counterparts. Each binary pins to its own VL/VT dependency version for maximum compatibility.

- **Drop-in VL/VT storage node.** Register as a `-storageNode` on vlselect/vtselect. Queries spanning hot and cold data work transparently.
- **Write path with crash recovery.** VL-compatible insert APIs (`/insert/jsonline`, Loki push, ES bulk) buffer data, flush to S3 Parquet, and survive process crashes via WAL.
- **Zero-delay reads.** Select pods query insert pods for unflushed buffer data, merging with S3 results for immediate read-after-write visibility.
- **Open format + S3 durability.** 22% cheaper than Loki/Tempo. Within 5% of VL/VT EBS cost at 1yr, cheapest at 3yr+ with Glacier tiering. S3's 11-nines durability for compliance.
- **Sub-millisecond fast path.** Queries within the hot tier's range get an immediate empty response via the partition manifest. Zero S3 I/O.
- **Disaster recovery.** When the hot cluster is down (outage, upgrade, migration), lakehouse serves all data from S3 — slower but always available.
- **Cost-aware deletion.** VL-compatible delete APIs with tombstone-based soft delete. Three modes: `hide` (instant, $0), `permanent` (physical removal), `auto` (smart). Glacier-safe — never triggers retrieval fees.
- **Open Parquet files.** DuckDB, Trino, Spark, and ClickHouse read the same files directly for analytics, compliance, and ML.

---

## The Cost Case

VL/VT's 47-70x compression makes EBS-only cheapest for short retention. With 3 AZ replication, VL/VT EBS and Lakehouse Hybrid are within 5% of each other. Lakehouse adds **open Parquet format, S3 11-nines durability, disaster recovery, and Glacier tiering** — and is always cheaper than Loki/Tempo.

| Scenario (500 GB/day, 1yr, 3 AZ) | VL/VT EBS Only | Lakehouse Hybrid | Loki + Tempo |
|---|---|---|---|
| **Monthly cost** | **$2,679/mo** | $2,814/mo | $3,610/mo |
| **Compression** | 47-70x | 6x (Parquet) | 3.5x |
| **Query speed (cold)** | <10ms (all EBS) | <500ms (Parquet) | 1-10s |
| **Data format** | Proprietary | **Open Parquet** | Proprietary |
| **S3 durability** | EBS per-AZ | **11 nines** | 11 nines |
| **Glacier tiering** | N/A | **Yes (cheapest at 3yr+)** | No (compaction breaks it) |
| **Analytics access** | VL/VT API only | **DuckDB, Spark, Trino** | Loki API only |
| **Disaster recovery** | N/A | **Independent cold tier** | N/A |

Full cost worksheet: [Cost Estimates](docs/cost-estimates.md) | Deep comparison vs Loki/Tempo: [Cost Comparison](docs/cost-comparison.md)

---

## Quick Start

### Docker

```bash
# Logs (VL-compatible, port 9428)
docker run -p 9428:9428 \
  ghcr.io/reliablyobserve/lakehouse-logs:latest \
  --lakehouse.s3.bucket=obs-archive \
  --lakehouse.s3.region=us-east-1

# Traces (VT-compatible, port 10428)
docker run -p 10428:10428 \
  ghcr.io/reliablyobserve/lakehouse-traces:latest \
  --lakehouse.s3.bucket=obs-archive \
  --lakehouse.s3.region=us-east-1
```

### Docker Compose (with MinIO)

```bash
docker compose -f deployment/docker/docker-compose-e2e.yml up
```

Starts 13 services: MinIO (S3), VictoriaLogs + VictoriaTraces (hot tiers, 24h), lakehouse-logs + lakehouse-traces (cold S3), vlselect + vtselect (multi-level select), loki-vl-proxy (hot+cold routing), ClickHouse (analytics), and Grafana with 11 pre-configured datasources. See [Docker Compose Setup](docs/docker-compose-setup.md).

### Helm

```bash
# Deploy logs cold tier
helm install lakehouse-logs oci://ghcr.io/reliablyobserve/charts/victoria-lakehouse \
  --set lakehouseConfig.mode=logs \
  --set lakehouseConfig.s3.bucket=obs-archive \
  --set lakehouseConfig.s3.region=us-east-1 \
  --set lakehouseConfig.discovery.headless_service=vlstorage.monitoring.svc.cluster.local

# Deploy traces cold tier (separate release, same chart)
helm install lakehouse-traces oci://ghcr.io/reliablyobserve/charts/victoria-lakehouse \
  --set lakehouseConfig.mode=traces \
  --set lakehouseConfig.s3.bucket=obs-archive \
  --set lakehouseConfig.s3.region=us-east-1 \
  --set lakehouseConfig.discovery.headless_service=vtstorage.monitoring.svc.cluster.local
```

### Grafana Datasource (Direct Access)

Point a VictoriaLogs datasource directly at Victoria Lakehouse for standalone cold queries:

```yaml
datasources:
  - name: Cold Logs (Lakehouse)
    type: victorialogs-datasource
    access: proxy
    url: http://lakehouse-logs:9428
  - name: Cold Traces (Lakehouse)
    type: jaeger
    access: proxy
    url: http://lakehouse-traces:10428
```

For full setup, cluster integration, and deployment patterns, see [Getting Started](docs/getting-started.md).

---

## Architecture

Victoria Lakehouse reimplements the VL/VT storage interface (`RunQuery`, `GetFieldNames`, `GetFieldValues`, `GetStreams`, etc.) backed by Parquet files on S3. All HTTP APIs (`/select/logsql/*`, `/insert/jsonline`, `/insert/loki/api/v1/push`, `/insert/elasticsearch/_bulk`, `/delete/logsql/*`), the binary DataBlock protocol, and the LogsQL query engine are implemented from the VL/VT spec — same endpoints, same wire format, same query syntax.

It integrates with any log/trace shipper — [vlagent](https://docs.victoriametrics.com/victorialogs/data-ingestion/vlogscli/), [Fluent Bit](https://fluentbit.io/), [Vector](https://vector.dev/), [OTEL Collector](https://opentelemetry.io/docs/collector/), [Fluentd](https://www.fluentd.org/), [Logstash](https://www.elastic.co/logstash), [Promtail](https://grafana.com/docs/loki/latest/send-data/promtail/) — to mirror data to both hot and cold tiers simultaneously, providing unlimited retention, disaster recovery, and open-format analytics. For Grafana users, [loki-vl-proxy](https://github.com/ReliablyObserve/loki-vl-proxy) provides automatic hot+cold routing with full **Grafana Loki Drilldown** compatibility — queries for the last 24h go to VictoriaLogs (hot), older queries route to lakehouse (cold).

```mermaid
graph TB
    subgraph "Data Collection"
        K8S["Kubernetes / Infrastructure"]
        LOG["Log Shippers<br/>vlagent · Fluent Bit · Vector<br/>OTEL Collector · Fluentd"]
        APP["Applications (OTEL SDK / Zipkin)"]
        TRACE["Trace Collectors<br/>OTEL Collector · Jaeger Agent"]
        K8S --> LOG
        APP --> TRACE
    end

    subgraph "Hot Tier — 1 Month (EBS, multi-AZ)"
        VLI["vlinsert"] --> VLSTO["vlstorage"]
        VTI["vtinsert"] --> VTSTO["vtstorage"]
        VLSEL["vlselect"] --> VLSTO
        VTSEL["vtselect"] --> VTSTO
    end

    subgraph "Cold Tier — Unlimited (S3) — Victoria Lakehouse"
        LHL["lakehouse-logs<br/>(insert)"] --> WAL1["WAL → Buffers"]
        LHT["lakehouse-traces<br/>(insert)"] --> WAL2["WAL → Buffers"]
        WAL1 -->|flush| S3[("S3 Parquet<br/>(11 nines)")]
        WAL2 -->|flush| S3
        LHLS["lakehouse-logs<br/>(select)"] --> S3
        LHTS["lakehouse-traces<br/>(select)"] --> S3
        LHLS -.->|buffer query| WAL1
        LHTS -.->|buffer query| WAL2
    end

    LOG -->|"mirror (hot)"| VLI
    LOG -->|"mirror (cold)"| LHL
    TRACE -->|"export (hot)"| VTI
    TRACE -->|"export (cold)"| LHT

    subgraph "Consumers"
        GF["Grafana"] --> VLSEL
        GF --> VTSEL
        VLSEL -->|cold fan-out| LHLS
        VTSEL -->|cold fan-out| LHTS
    end

    subgraph "Analytics — Direct S3 Parquet"
        GF --> DDB["DuckDB · ClickHouse · Trino"]
        DDB --> S3
        TRI["Spark · Databricks · Snowflake<br/>StarRocks · Doris · pandas"] --> S3
    end

    style S3 fill:#e76f51,color:#fff
    style LHL fill:#5a189a,color:#fff
    style LHT fill:#5a189a,color:#fff
    style LHLS fill:#2d6a4f,color:#fff
    style LHTS fill:#2d6a4f,color:#fff
    style LOG fill:#264653,color:#fff
    style TRACE fill:#264653,color:#fff
    style DDB fill:#ff6b35,color:#fff
```

**Key points:**
- **Any log shipper** (vlagent, Fluent Bit, Vector, OTEL Collector, Fluentd, Logstash, Promtail) mirrors to both VictoriaLogs (hot) and `lakehouse-logs` (cold) — all support VL's `/insert/jsonline`, Loki push, or ES bulk API
- **Any trace shipper** (OTEL Collector, Jaeger Agent) fans out to both VictoriaTraces (hot) and `lakehouse-traces` (cold) via OTLP or Zipkin protocols
- **vlselect/vtselect** transparently fan out queries to hot + cold — users see unified results
- **Lakehouse as DR**: when hot cluster is down, Grafana queries lakehouse directly (slower but always available)
- **Open Parquet analytics**: [DuckDB](https://duckdb.org/docs/extensions/httpfs/s3api.html) via `read_parquet()`, [ClickHouse](https://clickhouse.com/docs/sql-reference/table-functions/s3) via `s3()` table function, [Trino](https://trino.io/docs/current/connector/hive-s3.html), [Spark](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html), and [pandas](https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html) all query S3 Parquet directly for analytics, compliance, and ML

For detailed collector configs, shipper examples, and DR playbooks, see [Deployment Architecture](docs/deployment-architecture.md).

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
    subgraph "Pattern 1: Hot+Cold dual-write (recommended)"
        VA["vlagent / Fluent Bit<br/>Vector / OTEL Collector"] -->|mirror| VLI["vlinsert<br/>(hot)"]
        VA -->|mirror| LHL["lakehouse-logs<br/>(cold)"]
        OC["OTEL Collector<br/>Jaeger Agent"] -->|export| VTI["vtinsert<br/>(hot)"]
        OC -->|export| LHT["lakehouse-traces<br/>(cold)"]
        G1L["Grafana<br/>(logs)"] --> VLS1["vlselect"]
        VLS1 -->|hot| VLSTO1["vlstorage"]
        VLS1 -->|cold| LHLS["lakehouse-logs"]
        G1T["Grafana<br/>(traces)"] --> VTS1["vtselect"]
        VTS1 -->|hot| VTSTO1["vtstorage"]
        VTS1 -->|cold| LHTS["lakehouse-traces"]
    end
```

```mermaid
graph LR
    subgraph "Pattern 2: Standalone"
        C2L["Clients<br/>(logs)"] -->|insert| LHL2["lakehouse-logs<br/>role=all"]
        C2T["Clients<br/>(traces)"] -->|insert| LHT2["lakehouse-traces<br/>role=all"]
        G2["Grafana"] -->|select| LHL2
        G2 -->|select| LHT2
        LHL2 --> S2[("S3")]
        LHT2 --> S2
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
        VMA -->|fallback| LH4["lakehouse-logs<br/>(select)"]
    end
```

```mermaid
graph LR
    subgraph "Pattern 5: Multi-Level Select (Hot+Cold unified)"
        VA5["vlagent"] -->|mirror| VLI5["vlinsert"]
        VA5 -->|mirror| LHI5["lakehouse-logs<br/>(insert)"]
        OC5["OTEL Collector"] -->|export| VTI5["vtinsert"]
        OC5 -->|export| LTI5["lakehouse-traces<br/>(insert)"]
        G5L["Grafana<br/>(logs)"] --> VLS5["vlselect"]
        VLS5 -->|hot| VLSTO5["vlstorage<br/>(disk)"]
        VLS5 -->|cold| LHL5["lakehouse-logs<br/>(S3)"]
        G5T["Grafana<br/>(traces)"] --> VTS5["vtselect"]
        VTS5 -->|hot| VTSTO5["vtstorage<br/>(disk)"]
        VTS5 -->|cold| LHT5["lakehouse-traces<br/>(S3)"]
    end
```

```mermaid
graph LR
    subgraph "Pattern 6: Loki-VL-proxy (Hot+Cold)"
        G6["Grafana<br/>Loki Drilldown"] --> LVP["loki-vl-proxy"]
        LVP -->|"hot (<24h)"| VL6["VictoriaLogs"]
        LVP -->|"cold (>24h)"| LH6["lakehouse-logs<br/>(select)"]
    end
```

```mermaid
graph LR
    subgraph "Pattern 7: Analytics (open Parquet on S3)"
        G7["Grafana"] --> DDB["DuckDB<br/>(in-memory plugin)"]
        G7 --> CH7["ClickHouse<br/>(server)"]
        G7 --> TRI7["Trino<br/>(Hive connector)"]
        DDB -->|"read_parquet()"| S7[("S3 Parquet")]
        CH7 -->|"s3() table fn"| S7
        TRI7 -->|"Hive SerDe"| S7
        SPK["Spark · Databricks<br/>Snowflake · StarRocks"] --> S7
    end
```

**Analytics Engines with Grafana Datasources:**

| Engine | Grafana Plugin | Status | License |
|---|---|---|---|
| [DuckDB](https://duckdb.org/docs/extensions/httpfs/s3api.html) | [`motherduck-duckdb-datasource`](https://github.com/motherduckdb/grafana-duckdb-datasource) | Unsigned, GitHub only | Free |
| [ClickHouse](https://clickhouse.com/docs/sql-reference/table-functions/s3) | [`grafana-clickhouse-datasource`](https://grafana.com/grafana/plugins/grafana-clickhouse-datasource/) | Official (Grafana Labs), 27.7M downloads | Free |
| [Trino](https://trino.io/docs/current/connector/hive-s3.html) | [`trino-datasource`](https://grafana.com/grafana/plugins/trino-datasource/) | Community-signed, in catalog, 1.4M downloads | Free |
| [Databricks](https://docs.databricks.com/en/connect/storage/index.html) | [`grafana-databricks-datasource`](https://grafana.com/grafana/plugins/grafana-databricks-datasource/) | Official (Grafana Labs) | Enterprise only |
| [Snowflake](https://docs.snowflake.com/en/user-guide/data-load-s3) | [`grafana-snowflake-datasource`](https://grafana.com/grafana/plugins/grafana-snowflake-datasource/) | Official (Grafana Labs) | Enterprise only |
| [StarRocks](https://docs.starrocks.io/docs/data_source/External_table/) / [Doris](https://doris.apache.org/docs/lakehouse/datalake-analytics/hive/) | Built-in MySQL datasource | MySQL wire protocol compat | Free |
| [Spark](https://spark.apache.org/docs/latest/sql-data-sources-parquet.html) | None | No plugin exists | — |
| [pandas](https://pandas.pydata.org/docs/reference/api/pandas.read_parquet.html) | None | CLI/notebook only | — |

Full engine comparison with query examples: [Analytics Engines](docs/analytics-engines.md)

---

## Binaries and Roles

Two separate binaries, each pinned to its own VL/VT upstream version for maximum API compatibility. Same Go codebase (shared `internal/` packages for cache, manifest, S3, config), different entry points and schemas. Each binary is a standalone static binary (no CGo) with a distroless Docker image (<20MB compressed).

| Binary | Port | Upstream Compat | Insert APIs | Select APIs | Docker Image |
|---|---|---|---|---|---|
| `lakehouse-logs` | 9428 | VL v1.50.0 | `/insert/jsonline`, `/insert/loki/api/v1/push`, `/insert/elasticsearch/_bulk` | `/select/logsql/*`, `/delete/logsql/*`, `/internal/select/*` | `ghcr.io/reliablyobserve/lakehouse-logs` |
| `lakehouse-traces` | 10428 | VT v0.8.2 | `/insert/jsonline`, Zipkin `/api/v2/spans`, OTLP | `/select/logsql/*`, Jaeger `/select/jaeger/api/*`, `/delete/tracessql/*` | `ghcr.io/reliablyobserve/lakehouse-traces` |

Each binary supports three roles for independent scaling:

| Role | Flag | Description | Use Case |
|---|---|---|---|
| All | `--lakehouse.role=all` (default) | Insert + select in one process, direct S3 access | Single-node, dev/test, small deployments |
| Insert | `--lakehouse.role=insert` | Write path only — receives data, buffers, flushes Parquet to S3 | Scale write throughput independently |
| Select | `--lakehouse.role=select` | Read path only — queries S3 + buffer query to insert pods | Scale read concurrency independently |

**Why two binaries**: VictoriaLogs and VictoriaTraces evolve at different cadences and have different API surfaces (Jaeger for traces, Loki push for logs). Separate binaries let each track its upstream independently — a VL version bump never blocks VT and vice versa. Separate Go modules prevent dependency conflicts between the two VL/VT versions.

---

## Key Features

### Write Path
- **VL-compatible insert APIs**: `/insert/jsonline`, `/insert/loki/api/v1/push`, `/insert/elasticsearch/_bulk` — same protocols as VictoriaLogs.
- **Write-ahead log (WAL)**: crash-safe durability with gob-encoded append-only log and automatic replay on restart.
- **Adaptive file sizing**: per-partition byte estimates trigger flush when approaching `--lakehouse.insert.target-file-size` for optimal Parquet file sizes.
- **Buffer query bridge**: select pods fan out to insert pods via `/internal/buffer/query` for zero-delay reads of unflushed data.
- **Manifest label pruning**: `FileInfo.Labels` enables query-time file skipping based on label values without opening Parquet files.

### Read Path
- **Schema-driven FieldType system**: centralized type-aware formatting for all Parquet column types (INT64 nanoseconds to RFC3339Nano, INT32 to decimal, etc.) via `FieldType.FormatValue()`. Eliminates scattered `fmt.Sprintf`/`time.Format` calls — all query paths use the schema registry for consistent output.
- **Auto-discovery of hot boundary** via `/internal/partition/list` on vlstorage/vtstorage. Zero manual config.
- **Partition manifest** for sub-ms "nothing here" responses. Recent queries cost zero S3 I/O.
- **LogsQL filter evaluation**: field matchers (exact, substring, regex, NOT) are applied post-scan to filter DataBlock rows at the storage layer.
- **max_rows enforcement**: `query.max_rows` (default 10M) caps emitted rows per query, preventing unbounded cold-query resource usage.
- **Bloom filters** on `trace_id` and `service_name` for fast point lookups.
- **Parallel file workers**: configurable bounded worker pool for concurrent Parquet file processing (default 8 workers).
- **Correlated prefetch**: log query warms trace Parquet for same time+service, and vice versa.
- **Read-ahead**: sequential time scans prefetch next partitions.

### Smart Cache
- **Unified cache controller** orchestrating L1 (memory), L2 (disk), L3 (peer), L4 (S3) with per-entry TTL, hot access detection, and singleflight S3 deduplication.
- **Active query pinning**: files used by in-flight queries are pinned in cache with configurable grace period, preventing eviction under load.
- **Cache sizing calculator**: adaptive budget estimation blending ingestion rate (early) and query pattern analysis (after 12h uptime), with per-node fleet division.
- **Snapshot persistence**: metadata snapshots to disk for fast cache warmup on restart.
- **15 Prometheus metrics**: hit ratio, entries, bytes used/limit, evictions by reason, hot/pinned entries, coverage hours, prefetch hit ratio.

### Cross-Signal Prefetch
- **Bidirectional hints** between `lakehouse-logs` and `lakehouse-traces` deployments. A logs query for `service=checkout` automatically warms trace Parquet for the same time window, and vice versa.
- **Works across separate binaries/deployments** — logs and traces don't need to be co-located. Hints are exchanged via HTTP (`/internal/prefetch/hint`, `/internal/cache/evict-hint`).
- **Connected data eviction**: when trace cache entries are evicted, correlated log entries are deprioritized.
- **Hint batching**: trace ID hints are accumulated and flushed on interval or batch size threshold, reducing HTTP overhead.
- **Auth key support**: optional `X-Cross-Signal-Key` header for securing cross-deployment communication.

### Deletion
- **Three-tier strategy**: tombstone (instant, $0) -> selective rewrite (S3 Standard only) -> lifecycle expiry (Glacier/IA).
- **`lakehouse-logs`**: `/delete/logsql/*` endpoints. **`lakehouse-traces`**: `/delete/tracessql/*` endpoints.
- **Three modes**: `hide` (tombstone only, never rewrites), `permanent` (physical removal), `auto` (smart default).
- **Cost estimation**: `/delete/logsql/estimate` (or `/delete/tracessql/estimate`) returns per-storage-class cost breakdown before executing.
- **Verification**: `/delete/logsql/verify` (or `/delete/tracessql/verify`) confirms tombstoned data is invisible (normal mode) or physically deleted (deep mode).
- **Un-delete**: remove a tombstone to restore data visibility instantly.
- **Glacier-safe**: never triggers retrieval fees. Tombstone suppresses reads; data ages out via lifecycle.
- **GDPR compliant**: immediate inaccessibility satisfies right-to-erasure. Optional physical delete for strict compliance.

### Loki Drilldown Compatibility
- **loki-vl-proxy hot+cold routing** with automatic time-based query routing: recent queries to VictoriaLogs (hot), older queries to lakehouse (cold), with configurable overlap.
- **Translated metadata mode** (`-metadata-field-mode=translated`) and structured metadata emission for full Grafana Loki Drilldown support.
- **Trace-to-logs linking** via derived fields — click a trace ID in Grafana to jump to correlated logs.

### Multi-Tenancy
- **Single binary, all tenants**: one lakehouse-logs/traces process serves all tenants simultaneously via header-based routing. Same pattern as Grafana Loki and Tempo.
- **S3 prefix isolation**: each tenant gets `{AccountID}/{ProjectID}/` S3 prefix (default `0/0/`). Tenant data is never mixed in shared files.
- **Enterprise bucket isolation**: optional `--lakehouse.tenant.isolation=bucket` with `--lakehouse.tenant.bucket-template` for IAM-level hard isolation.
- **Global read mode**: configurable `X-Lakehouse-Global-Read` header allows admin/Grafana dashboards to query across all tenants (must be explicitly enabled).
- **vmauth header extraction**: `X-Scope-AccountID` / `X-Scope-ProjectID` headers for tenant routing.
- **Analytics compatible**: all Parquet tools (DuckDB, ClickHouse, Trino, Spark) query per-tenant prefix directly.
- **Cost attribution**: per-prefix S3 Inventory or per-bucket billing for tenant cost allocation.

### Infrastructure
- **Metadata persistence**: manifest, label index, cache metadata, and smart cache snapshots survive restarts.
- **Distributed peer cache**: consistent hash routing across fleet instances via headless DNS.
- **Schema auto-discovery**: OTLP column names in Parquet, mapped to VL/VT names at query time. Schema registry carries per-column type information (FieldType) for type-aware formatting, extensible via `--lakehouse.schema.extra-promoted` with typed columns (string, int32, int64, float64, bool, timestamp_nano).
- **SQS/SNS support**: optional near-real-time manifest updates from S3 event notifications.

---

## Configuration

Minimal config (S3 bucket) works out of the box. All 110+ config options have production-ready defaults. Each binary automatically applies mode-appropriate defaults (port, S3 prefix, bloom columns, delete prefix).

### Shared Config (both binaries)

```yaml
lakehouse:
  s3:
    bucket: obs-archive
    region: us-east-1
  discovery:
    headless_service: vlstorage.monitoring.svc.cluster.local
    partition_auth_key: "${PARTITION_AUTH_KEY}"
```

### Smart Cache & Cross-Signal Config

```yaml
lakehouse:
  smart_cache:
    max_age: 24h
    hot_access_threshold: 3
    hot_window: 10m
    target_hours: 24
    snapshot_interval: 60s
    query_grace_period: 5m
  cross_signal:
    enabled: true
    endpoint: http://lakehouse-traces:10428  # for lakehouse-logs
    auth_key: "${CROSS_SIGNAL_KEY}"
    max_batch: 100
    batch_interval: 500ms
  query:
    file_workers: 8
```

### Multi-Tenancy Config

```yaml
lakehouse:
  tenant:
    prefix_template: "{AccountID}/{ProjectID}/"  # S3 prefix per tenant
    isolation: prefix           # prefix (default) | bucket (enterprise)
    bucket_template: ""         # e.g., "obs-{AccountID}-{ProjectID}" for bucket isolation
    default_account: "0"        # single-tenant default AccountID
    default_project: "0"        # single-tenant default ProjectID
    header_account: "X-Scope-AccountID"   # HTTP header for AccountID
    header_project: "X-Scope-ProjectID"   # HTTP header for ProjectID
    global_read_header: ""      # e.g., "X-Lakehouse-Global-Read" — cross-tenant reads via custom header
    global_read_value: ""       # required value for the custom header
    global_read_token: ""       # Bearer token for cross-tenant reads via Authorization header
```

Full reference: [Multi-Tenancy](docs/multi-tenancy.md)

### Mode-Specific Config

Each binary reads its own section for mode-specific overrides:

```yaml
lakehouse:
  # lakehouse-logs reads this section
  logs:
    bloom_columns: [service.name]
    delete_prefix: /delete/logsql

  # lakehouse-traces reads this section
  traces:
    bloom_columns: [trace_id, service.name]
    delete_prefix: /delete/tracessql
    jaeger_enabled: true
    jaeger_grpc_addr: ":16685"
```

### Mode-Specific Flags

```bash
# lakehouse-logs flags
--lakehouse.logs.bloom-columns=service.name
--lakehouse.logs.delete-prefix=/delete/logsql

# lakehouse-traces flags
--lakehouse.traces.bloom-columns=trace_id,service.name
--lakehouse.traces.delete-prefix=/delete/tracessql
--lakehouse.traces.jaeger-enabled=true
--lakehouse.traces.jaeger-grpc-addr=:16685
```

Full reference: [Configuration](docs/configuration.md)

---

## Observability

- **~100 Prometheus metrics** under `lakehouse_*` prefix (RED, USE, S3, cache, peer, manifest, Parquet engine, prefetch, smart cache, cross-signal, startup)
- **Grafana dashboards** (single-instance + cluster + supplementary panels for VL/VT dashboards)
- **10 alerting rules** with severity and annotations
- **Structured JSON logs** via `slog`

See [Observability](docs/observability.md).

---

## Security

### Container Hardening
- **Distroless runtime image** (`gcr.io/distroless/static-debian12:nonroot`) — no shell, no package manager, minimal attack surface
- **Non-root execution** (UID 65534) — never runs as root
- **Read-only root filesystem** in Kubernetes — prevents runtime filesystem modification
- **Stripped binaries** (`-s -w` linker flags) — no debug symbols in production
- **Drop all capabilities** (`capabilities.drop: ["ALL"]`) — principle of least privilege
- **Seccomp profile** (`RuntimeDefault`) — syscall filtering

### Authentication & Authorization
- **Internal endpoint auth**: `/internal/cache/*`, `/internal/manifest/update`, `/internal/prefetch/hint` endpoints require Bearer token when configured (`peer.auth_key`, `partition_auth_key`)
- **Cross-signal auth**: optional `X-Cross-Signal-Key` header for securing cross-deployment prefetch hints between logs and traces instances
- **S3 credential isolation**: each binary has its own S3 credentials via flags, environment variables, or IAM roles
- **Multi-tenant isolation**: S3 prefix per tenant (`{AccountID}/{ProjectID}/`) with explicit default `0/0/`, single binary serving all tenants. Enterprise option for bucket-per-tenant with separate IAM policies. Optional global read mode for admin dashboards. See [Multi-Tenancy](docs/multi-tenancy.md)

### CI Security Pipeline
- **[govulncheck](https://pkg.go.dev/golang.org/x/vuln/cmd/govulncheck)** — Go vulnerability database scanning
- **[gosec](https://github.com/securego/gosec)** — Go security linter
- **[Trivy](https://trivy.dev/)** — container image vulnerability scanning
- **[gitleaks](https://gitleaks.io/)** — secret detection in git history
- **[CodeQL](https://codeql.github.com/)** — semantic code analysis (Go, Python, JavaScript)
- **[golangci-lint](https://golangci-lint.run/)** — includes security-related linters (errcheck, gosec, staticcheck)

See [Security](docs/security.md).

---

## Parquet Schema

Victoria Lakehouse reads and writes **OTLP-standard Parquet files**. Column names use OTEL semantic convention dot-notation directly (e.g., `service.name`, `k8s.namespace.name`) for zero-translation compatibility with OTEL Collector exporters and standard tooling. High-frequency fields are promoted to top-level columns with statistics and optional bloom filters. Everything else is preserved in MAP columns — no data is ever lost.

**S3 layout**: Hive partitioned `s3://{bucket}/{AccountID}/{ProjectID}/{signal}/dt=YYYY-MM-DD/hour=HH/{batch}.parquet` (default tenant: `0/0/`)

### Logs Schema

| Column | Type | Bloom | Description |
|---|---|---|---|
| `timestamp_unix_nano` | INT64 | | Log timestamp (nanoseconds) |
| `body` | STRING | | Log message body |
| `severity_text` | STRING | | Log level (INFO, ERROR, etc.) |
| `severity_number` | INT32 | | OTEL severity number (1-24) |
| `service.name` | STRING | Yes | Originating service |
| `k8s.namespace.name` | STRING | | Kubernetes namespace |
| `k8s.pod.name` | STRING | | Kubernetes pod |
| `k8s.deployment.name` | STRING | | Kubernetes deployment |
| `k8s.node.name` | STRING | | Kubernetes node |
| `deployment.environment` | STRING | | production, staging, canary |
| `cloud.region` | STRING | | AWS/GCP region |
| `host.name` | STRING | | Hostname |
| `trace_id` | STRING | Yes | Correlated trace ID |
| `span_id` | STRING | | Correlated span ID |
| `_stream` / `_stream_id` | STRING | | VL stream identity |
| `scope.name` | STRING | | Instrumentation scope name |
| `resource.attributes` | MAP(STRING,STRING) | | All resource attributes not promoted |
| `log.attributes` | MAP(STRING,STRING) | | All log record attributes |

### Traces Schema

| Column | Type | Bloom | Description |
|---|---|---|---|
| `timestamp_unix_nano` | INT64 | | Span end time (nanoseconds) |
| `start_time_unix_nano` | INT64 | | Span start time (nanoseconds) |
| `trace_id` | STRING | Yes | Trace identifier |
| `span_id` | STRING | | Span identifier |
| `parent_span_id` | STRING | | Parent span for tree structure |
| `span.name` | STRING | | Operation name |
| `span.kind` | INT32 | | Span kind (1=Internal, 2=Server, 3=Client, 4=Producer, 5=Consumer) |
| `service.name` | STRING | Yes | Originating service |
| `duration_ns` | INT64 | | Span duration (nanoseconds) |
| `status.code` | INT32 | | Span status (0=Unset, 1=OK, 2=Error) |
| `status.message` | STRING | | Error details |
| `scope.name` | STRING | | Instrumentation library name |
| `resource.attributes` | MAP(STRING,STRING) | | Resource attributes (environment, region, K8s metadata) |
| `span.attributes` | MAP(STRING,STRING) | | Span attributes (HTTP method, status code, DB system) |
| `scope.attributes` | MAP(STRING,STRING) | | Instrumentation scope attributes |

All typed columns (INT32, INT64) are stored as native Parquet types with column statistics for efficient predicate pushdown. The schema registry provides centralized type-aware formatting via `FieldType.FormatValue()` for the VL/VT read path (e.g., INT64 nanoseconds to RFC3339Nano timestamps, INT32 to decimal strings).

Any tool that reads Parquet can query these files directly — DuckDB, ClickHouse, Trino, Spark, Databricks, Snowflake, StarRocks, Doris, pandas. Full engine list with Grafana plugin status: [Analytics Engines](docs/analytics-engines.md). Schema reference: [Open Parquet Format](docs/open-parquet-format.md)

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

### Getting Started
- [Getting Started](docs/getting-started.md) — quick start, first query in 5 minutes
- [Docker Compose Setup](docs/docker-compose-setup.md) — full local environment with MinIO, hot/cold tiers, Grafana (11 datasources)
- [Configuration](docs/configuration.md) — all 110+ config options with production-ready defaults

### Architecture & Design
- [Architecture](docs/architecture.md) — internal design, Parquet schema, query flow, cache tiers
- [Deployment Architecture](docs/deployment-architecture.md) — collector configs (vlagent, Fluent Bit, Vector, OTEL Collector), hot/cold tiers, DR playbooks
- [Write Path](docs/write-path.md) — insert APIs, WAL crash recovery, adaptive flush, buffer query bridge
- [Deletion Strategy](docs/deletion-strategy.md) — cost-aware tombstone + selective rewrite, Glacier-safe, three modes

### Analytics (Open Parquet)
- [Analytics](docs/analytics.md) — DuckDB, ClickHouse, Trino, Spark, pandas query examples on S3 Parquet, 11 Grafana datasource reference, ClickHouse OTEL views setup
- [Analytics Engines](docs/analytics-engines.md) — all 9 engines with Grafana datasource status (DuckDB, ClickHouse, Trino, Databricks, Snowflake, StarRocks, Doris, Spark, pandas)
- [Open Parquet Format](docs/open-parquet-format.md) — full schema reference, typed columns, bloom filters, compression, row group statistics, external tool examples
- [Use Cases](docs/use-cases.md) — disaster recovery, compliance/audit, capacity planning, cost allocation, ML pipelines
- [Multi-Tenancy](docs/multi-tenancy.md) — S3 prefix isolation, bucket-per-tenant enterprise option, vmauth integration, analytics tool compatibility

### Operations
- [Operations](docs/operations.md) — day-2 operations, scaling, troubleshooting
- [Security](docs/security.md) — container hardening, auth, network policies, credentials
- [Observability](docs/observability.md) — ~100 metrics, Grafana dashboards, 10 alerting rules
- [Performance](docs/performance.md) — benchmarks, tuning guides, p95 targets
- [Scaling](docs/scaling.md) — horizontal (insert/select separation) and vertical scaling

### Cost & Comparison
- [Cost Estimates](docs/cost-estimates.md) — EBS vs S3 vs Glacier cost breakdown
- [Cost Comparison vs Loki/Tempo](docs/cost-comparison.md) — comprehensive competitive analysis at 500 GB/day

---

## Current Status

All core milestones are **complete**. The project is in production-readiness and feature expansion phase.

| Milestone | Status | Description |
|---|---|---|
| **M1: Foundation** | Complete | Go module, CI/CD pipeline (test, lint, build, security), Dockerfile (distroless), Helm chart, config namespace with 110+ flags |
| **M2: ParquetS3Storage** | Complete | Schema registry (OTLP → VL/VT), partition manifest, Parquet query engine with row group stats skip, bloom filters, column projection, all 11 VL storage interface methods |
| **M3: Cache + Persistence** | Complete | L1 memory LRU, L2 disk LRU, singleflight S3 dedup, label/attribute index, metadata persistence to disk for fast restart |
| **M4: Discovery + Peer Cache** | Complete | Hot boundary auto-discovery via `/internal/partition/list`, consistent hash peer cache (L3) via headless DNS, `/manifest/range` API |
| **M5: Cluster Integration** | Complete | `/internal/select/*` binary protocol with ZSTD DataBlock streaming, `-storageNode` registration on vlselect/vtselect |
| **M6: Filter AST + E2E** | Complete | Full LogsQL predicate engine (exact, substring, regex, NOT, AND, OR, ranges), Playwright E2E, schema validation |
| **M7: Observability** | Complete | ~100 Prometheus metrics, Grafana dashboards (single-instance + cluster), 10 alerting rules, circuit breaker, structured JSON logging |
| **M8: Write Path** | Complete | VL-compatible insert APIs (`/insert/jsonline`, Loki push, ES bulk), WAL with crash recovery, adaptive flush, buffer query bridge for zero-delay reads |
| **M9: Compaction** | Complete | Background merge of small Parquet files, size-tiered strategy, manifest atomic updates, tombstone integration |
| **M10: Testing & Helm** | Complete | E2E test suite (VL + vlselect + loki-vl-proxy chain), benchmarks, Victoria-pattern Helm chart, upstream sync GHA |
| **M11: Cost-Aware Deletion** | Complete | Three-tier deletion (tombstone → rewrite → lifecycle), `/delete/logsql/*` and `/delete/tracessql/*` APIs, storage-class detection, Glacier-safe, verify endpoint |
| **Binary Split** | Complete | Separate `lakehouse-logs` + `lakehouse-traces` binaries with independent Go modules, mode-specific config/flags/schemas |
| **Smart Cache** | Complete | Unified cache controller (L1-L4), active query pinning, cache sizing calculator, snapshot persistence, cross-signal prefetch between logs↔traces |
| **E2E Compose** | Complete | Full Docker Compose with MinIO, VL/VT hot tiers, vlselect/vtselect multi-level select, loki-vl-proxy, DuckDB + ClickHouse analytics, 11 Grafana datasources |

---

## Development

```bash
# Logs binary
make build-logs       # Build lakehouse-logs
make test-logs        # Run logs module tests with race detector
make docker-logs      # Build logs Docker image

# Traces binary
make build-traces     # Build lakehouse-traces
make test-traces      # Run traces module tests with race detector
make docker-traces    # Build traces Docker image

# Both
make build            # Build both binaries
make test             # Run all tests
make lint             # golangci-lint both modules
make e2e              # Full E2E with MinIO + VL cluster
```

---

## License

Apache License 2.0. See [LICENSE](LICENSE).
