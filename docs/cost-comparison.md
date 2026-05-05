---
title: Cost Comparison
sidebar_position: 16
---

# Victoria Lakehouse vs Loki vs Tempo — Cost, Performance & Architecture Comparison

## Executive Summary

Victoria Lakehouse operates as a **cold storage tier** for VictoriaLogs/VictoriaTraces, storing data in open Parquet format on S3. VL/VT's native 47-70x compression makes EBS-only the cheapest option for 1-2 year retention. The hybrid architecture (VL/VT hot on EBS + Lakehouse cold on S3) adds **open Parquet format, S3 11-nines durability, disaster recovery, and direct analytics access** — and becomes cheapest at 3+ year retention via S3 lifecycle tiering to Glacier.

This document compares three deployment architectures across cost, performance, compression, durability, complexity, and flexibility.

---

## Architecture Overview

### Option A: Victoria Lakehouse Hybrid (Recommended)

```
Hot tier (0-30 days):  VL/VT on EBS (gp3)  — sub-10ms queries
Cold tier (30d-2yr):   Lakehouse on S3      — <500ms queries (Parquet + bloom)
```

- VL/VT handles recent high-frequency queries natively on local SSD/EBS
- Lakehouse handles historical/compliance queries from S3 Parquet
- vlselect fan-out unifies both tiers transparently

### Option B: Grafana Loki + Tempo

```
All data on S3 via chunks — no hot/cold split
Ingester WAL on EBS (temporary, not queryable long-term)
Compactor merges chunks on S3
```

- All queries hit S3 regardless of data age
- Ingester memory buffers provide ~15min "hot" window only
- BoltDB/TSDB index on S3 for label lookups

### Option C: Standalone VL/VT (EBS only, no Lakehouse)

```
All data on EBS — full retention on disk
```

- Simplest architecture, cheapest at 1-2yr retention thanks to 47-70x compression
- Linear cost growth with retention (no lifecycle tiering)
- Proprietary format (no external analytics access)

---

## Cost Comparison at Scale

### Scenario: 500 GB/day ingestion, us-east-1, 3 AZ

#### Storage Costs (1 year retention)

| Component | Lakehouse Hybrid | Loki + Tempo | VL/VT EBS Only |
|---|---|---|---|
| **Hot tier storage** | EBS gp3: 30d × 500GB ÷ 55x × 3 AZ<br>= 820GB × $0.08/GB = **$66/mo** | Ingester EBS (WAL, RF=3): 1.5TB<br>× $0.08/GB = **$120/mo** | EBS gp3: 365d × 500GB ÷ 55x × 3 AZ<br>= 9.9TB × $0.08/GB = **$796/mo** |
| **Cold tier storage** | S3 Standard: 335d × 500GB ÷ 6x<br>= 27.9TB × $0.023/GB = **$642/mo** | S3 Standard: 365d × 500GB ÷ 3.5x<br>= 52TB × $0.023/GB = **$1,196/mo** | N/A (all on EBS) |
| **Index storage** | Manifest in-memory (<1MB) | DynamoDB/BoltDB on S3: ~5TB<br>× $0.023/GB = **$115/mo** | VL native (included in 55x ratio) |
| **Total storage** | **$708/mo** | **$1,431/mo** | **$796/mo** |

#### Compute Costs

| Component | Lakehouse Hybrid | Loki + Tempo | VL/VT EBS Only |
|---|---|---|---|
| **Ingestion** | VL vlinsert: 6× m6i.xlarge (3 AZ)<br>= **$864/mo** | Distributor+ingester: 6× m6i.xlarge (3 AZ, RF=3)<br>= **$864/mo** | VL vlinsert: 6× m6i.xlarge (3 AZ)<br>= **$864/mo** |
| **Hot query** | VL vlselect: 6× m6i.xlarge (3 AZ)<br>= **$864/mo** | Querier+frontend: 6× m6i.xlarge (3 AZ)<br>= **$864/mo** | VL vlselect: 6× m6i.xlarge (3 AZ)<br>= **$864/mo** |
| **Cold query** | Lakehouse select: 3× m6i.large (3 AZ)<br>= **$207/mo** | Loki querier (same): included above | N/A |
| **Other** | vlstorage: included above | Compactor: 1× m6i.xlarge = **$144/mo**<br>Ruler: 1× m6i.large = **$69/mo** | vlstorage: included |
| **Total compute** | **$1,935/mo** | **$1,941/mo** | **$1,728/mo** |

#### S3 Request Costs

| Operation | Lakehouse Hybrid | Loki + Tempo |
|---|---|---|
| **Write (PUT)** | ~150K PUTs/mo (flush every 10s, 2 partitions)<br>× $0.005/1K = **$0.75/mo** | ~4.3M PUTs/mo (chunk uploads + index)<br>× $0.005/1K = **$21.50/mo** |
| **Read (GET)** | ~500K GETs/mo (cold queries, cached)<br>× $0.0004/1K = **$0.20/mo** | ~15M GETs/mo (all queries hit S3)<br>× $0.0004/1K = **$6.00/mo** |
| **List** | ~0 (manifest eliminates listing) | ~2M LIST/mo<br>× $0.005/1K = **$10.00/mo** |
| **Total requests** | **$0.95/mo** | **$37.50/mo** |

#### Data Transfer Costs

| Transfer Type | Lakehouse Hybrid | Loki + Tempo | VL/VT EBS Only |
|---|---|---|---|
| **Cross-AZ (ingest)** | $0.01/GB × 500GB/day × 30<br>= **$150/mo** | $0.01/GB × 500GB/day × 30<br>= **$150/mo** | $0.01/GB × 500GB/day × 30<br>= **$150/mo** |
| **Cross-AZ (query read)** | Hot: minimal (local EBS)<br>Cold: $0.01/GB × ~2TB read/mo = **$20/mo** | $0.01/GB × ~5TB read/mo<br>= **$50/mo** | Minimal (local EBS)<br>= **$5/mo** |
| **S3 egress (same region)** | Free (same region) | Free (same region) | N/A |
| **Internet egress** | ~$0 (Grafana co-located) | ~$0 (Grafana co-located) | ~$0 |
| **Total transfer** | **$170/mo** | **$200/mo** | **$155/mo** |

#### Total Monthly Cost (500 GB/day, 1yr retention)

| | Lakehouse Hybrid | Loki + Tempo | VL/VT EBS Only |
|---|---|---|---|
| Storage | $708 | $1,431 | $796 |
| Compute | $1,935 | $1,941 | $1,728 |
| S3 Requests | $1 | $38 | $0 |
| Data Transfer | $170 | $200 | $155 |
| **Monthly Total** | **$2,814/mo** | **$3,610/mo** | **$2,679/mo** |
| **Annual Total** | **$33,768/yr** | **$43,320/yr** | **$32,148/yr** |

#### Scaling to 2-Year Retention

| | Lakehouse Hybrid | Loki + Tempo | VL/VT EBS Only |
|---|---|---|---|
| Storage (2yr) | Hot: $66 + Cold: $1,342 = **$1,408/mo** | WAL: $120 + S3: $2,399 + Index: $230<br>= **$2,749/mo** | 730d × 500GB ÷ 55x × 3 AZ = 19.9TB<br>**$1,593/mo** |
| **Monthly Total** | **$3,514/mo** | **$4,928/mo** | **$3,476/mo** |
| **Annual Total** | **$42,168/yr** | **$59,136/yr** | **$41,712/yr** |

**Key insights**:
- With 3 AZ replication, VL/VT EBS Only and Lakehouse Hybrid are within 5% of each other ($2,679 vs $2,814/mo at 1yr). Compute dominates at this scale — storage is secondary.
- VL/VT EBS is slightly cheaper at 1-2yr thanks to 47-70x compression, but the gap narrows at 2yr ($3,476 vs $3,514/mo) as 3× EBS grows linearly while Lakehouse cold tier stays on S3.
- Lakehouse Hybrid is 22% cheaper than Loki at 1yr and 29% cheaper at 2yr due to better Parquet compression (6x vs 3.5x) and no compaction I/O.
- Lakehouse wins at 3+ year retention when S3 lifecycle moves data to Glacier Instant ($0.004/GB) or Deep Archive ($0.00099/GB) — at 3yr with lifecycle, Lakehouse storage drops to ~$710/mo vs VL/VT EBS ~$2,389/mo (3× AZ).
- Lakehouse's value beyond cost: open Parquet format, S3 11-nines durability, disaster recovery independence, and direct analytics access (DuckDB, Spark, Trino).

---

## Compression Ratio Comparison

### Raw Compression Performance

| Metric | VL/VT (native LSM) | Lakehouse (Parquet + ZSTD) | Loki (Snappy chunks) | Tempo (Snappy/ZSTD blocks) |
|---|---|---|---|---|
| **Overall ratio (logs)** | **~70x** (production measured) | **6-10x** (ZSTD-3 default) | **3-3.5x** (Snappy) | N/A |
| **Overall ratio (traces)** | **~47x** (production measured) | **5-8x** (ZSTD-3 default) | N/A | **3-4x** (Snappy) |
| **Best case (low-cardinality logs)** | 100-200x | **50-200x** per column | 4-5x | 4-5x |
| **Worst case (random trace IDs)** | 10-20x | 1.5-3x per column | 2-2.5x | 2-3x |
| **Structured JSON logs** | 60-80x | **8-12x** | 3.5-4x | N/A |

### Why VL/VT Compresses Best

VL/VT achieves 47-70x because its LSM storage engine:
1. **Stream deduplication** — stream labels (service.name, namespace, etc.) stored once per stream, not repeated per log line
2. **Per-stream dictionary** — high-frequency terms within a stream compressed via shared dictionary
3. **Inverted index** — term→log mapping enables fast queries without scanning all data
4. **ZSTD on data blocks** — final compression on homogeneous blocks of same-stream data

### Why Parquet Compresses Better Than Loki

1. **Columnar layout** — Parquet stores each column separately. Columns with low cardinality (service.name, level, k8s.namespace) achieve 50-200x via dictionary + RLE encoding, even before ZSTD
2. **Type-aware encoding** — timestamps use delta encoding (10-50x), integers use bit-packing, strings use dictionary encoding
3. **Homogeneous data** — each column page contains only one data type, ZSTD compresses homogeneous data far better than mixed row-oriented chunks
4. **ZSTD vs Snappy** — ZSTD-3 achieves 30-50% better ratio than Snappy at comparable decode speeds

### Per-Column Breakdown (real log data)

| Column | Cardinality | Encoding | Compression Ratio |
|---|---|---|---|
| `service.name` | Low (~20 values) | DICT + RLE + ZSTD | 100-200x |
| `k8s.namespace.name` | Low (~10 values) | DICT + RLE + ZSTD | 150-300x |
| `level` | Very low (5 values) | DICT + RLE + ZSTD | 500-1000x |
| `timestamp_unix_nano` | Unique (monotonic) | DELTA + ZSTD | 10-50x |
| `body` (log message) | High | PLAIN + ZSTD | 2-4x |
| `trace_id` | Very high (random) | PLAIN + ZSTD | 1.5-3x |
| `resource.attributes` (MAP) | Medium | DICT + ZSTD | 5-15x |

### Storage Cost Impact of Compression

At 500 GB/day raw ingestion, 1 year retention:

| Solution | Compression | Data on Disk/S3 | Storage Cost |
|---|---|---|---|
| VL/VT EBS (native, 3 AZ) | 47-70x (production measured) | 9.0-11.1 TB (EBS × 3 AZ) | $720-$888/mo |
| Lakehouse ZSTD-3 | 6-10x | 18-30 TB (S3) | $414-$690/mo |
| Lakehouse ZSTD-9 | 8-12x | 15-23 TB (S3) | $345-$529/mo |
| Loki Snappy | 3-3.5x | 52-60 TB (S3) | $1,196-$1,380/mo |
| Tempo Snappy | 3-4x | 45-60 TB (S3) | $1,035-$1,380/mo |

**Annual storage savings** (Lakehouse ZSTD-3 vs Loki Snappy): **$6,072-$11,592/year**
**With lifecycle** (>1yr data on Glacier Instant at $0.004/GB): Lakehouse cold storage drops to **$120-$200/mo**, beating VL/VT 3 AZ EBS ($720-$888/mo).

---

## Query Performance Comparison

### Latency by Query Type

| Query Type | Lakehouse Hot (VL/EBS) | Lakehouse Cold (S3) | Loki (S3) | Tempo (S3) |
|---|---|---|---|---|
| **Recent data (<30d)** | **<10ms** p95 | N/A (hot tier) | 100-500ms | 100-500ms |
| **Point lookup (trace_id)** | **<10ms** | **<100ms** (bloom filter) | 1-5s (chunk scan) | 200-500ms (bloom) |
| **Time range (1h, cold)** | N/A | **<500ms** (row group skip) | 1-10s (decompress all) | 1-5s |
| **Label/field discovery** | **<1ms** | **<1ms** (label index) | 10-50ms (label cache) | 50-200ms |
| **Aggregation (stats)** | **<50ms** | **<300ms** (columnar) | 2-15s (full scan) | N/A |
| **Full text search** | **<50ms** (LogsQL) | **<500ms** | 1-10s (line filter) | N/A |
| **Regex filter** | **<100ms** | **<1s** | 5-30s | N/A |

### Why Lakehouse Cold Is Faster Than Loki

1. **Columnar vs row-oriented** — Parquet reads only needed columns (5-10% of data); Loki decompresses entire chunks
2. **Row group statistics** — min/max stats skip irrelevant row groups without reading data; Loki must decompress to filter
3. **Bloom filters** — O(1) point lookups on trace_id/service.name; Loki does linear chunk scanning
4. **Manifest fast path** — sub-ms "nothing here" response when data doesn't exist in cold tier; Loki always hits S3
5. **Multi-tier cache** — frequently accessed Parquet pages cached in memory/disk/peers; Loki chunk cache is per-ingester

### Concurrent Query Capacity

| Metric | Lakehouse Hybrid | Loki | Tempo |
|---|---|---|---|
| **Hot concurrent queries** | ~10K qps (VL native) | ~1K qps (S3 bound) | ~500 qps |
| **Cold concurrent queries** | ~2K qps (cached) | ~1K qps (same pool) | ~500 qps |
| **Query isolation** | Hot/cold fully isolated | Shared querier pool | Shared pool |
| **Cache hit rate (steady state)** | >90% (L1+L2) | 40-60% (chunk cache) | 30-50% |

---

## Data Traffic & I/O Costs

### Write Path I/O

| Operation | Lakehouse Hybrid | Loki | Tempo |
|---|---|---|---|
| **Ingest network** | Client → VL (hot) + Client → Lakehouse (cold) | Client → Distributor → Ingester | Client → Distributor → Ingester |
| **Write amplification** | 1x (single Parquet write per flush) | 3-5x (WAL + chunk + index + compaction) | 2-3x (WAL + block + compaction) |
| **S3 PUTs/day** | ~5K (flush every 10s × partitions) | ~143K (chunks + index) | ~50K (blocks + bloom) |
| **S3 PUT cost/day** | $0.025 | $0.72 | $0.25 |
| **Compaction I/O** | Merge recent small files only (old data never touched) | 2-5x read+rewrite of ALL data over time | 2-3x read+rewrite of ALL data |

### Read Path I/O

| Operation | Lakehouse (cold query) | Loki | Tempo |
|---|---|---|---|
| **S3 GETs per point query** | 1-2 (bloom → target file) | 5-50 (chunk iteration) | 2-5 (bloom → block) |
| **Bytes read per point query** | ~100KB (column projection) | ~5-50MB (full chunks) | ~1-5MB |
| **S3 GETs per range scan (1h)** | 3-10 (row group targeting) | 50-500 (all chunks in range) | 10-50 |
| **Bytes read per range scan** | ~1-10MB (columnar) | ~50-500MB (full chunks) | ~10-100MB |
| **Cache effectiveness** | High (small, targeted reads) | Low-medium (large chunk reads) | Medium |

### Monthly I/O Cost (10K queries/day)

| Cost Component | Lakehouse Hybrid | Loki | Tempo |
|---|---|---|---|
| **S3 GET requests** | 1.5M × $0.0004/K = **$0.60** | 45M × $0.0004/K = **$18.00** | 9M × $0.0004/K = **$3.60** |
| **S3 data retrieval** | 3TB × $0/GB (same region) = **$0** | 30TB × $0/GB = **$0** | 6TB × $0/GB = **$0** |
| **Cross-AZ query traffic** | 3TB × $0.01/GB = **$30** | 30TB × $0.01/GB = **$300** | 6TB × $0.01/GB = **$60** |
| **Monthly I/O total** | **$30.60** | **$318.00** | **$63.60** |

**Lakehouse reads 10-30x less data from S3** per query because of columnar projection and row group pruning.

---

## Complexity Comparison

### Deployment Complexity

| Aspect | Lakehouse Hybrid | Loki (Simple Scalable) | Tempo (Distributed) |
|---|---|---|---|
| **Components to deploy** | VL/VT (2) + Lakehouse select (1) + vmauth (1) = **4** | Read (1) + Write (1) + Backend (1) + Gateway (1) = **4** | Distributor (1) + Ingester (1) + Querier (1) + Compactor (1) = **4** |
| **Stateful components** | VL vlstorage (EBS) + Lakehouse (stateless, S3) | Ingester (WAL on EBS) | Ingester (WAL on EBS) |
| **External dependencies** | S3 only | S3 + DynamoDB/BoltDB (index) | S3 only |
| **Configuration surface** | VL config + Lakehouse config (~30 flags) | Loki config (~200+ flags) | Tempo config (~150+ flags) |
| **Operational runbooks** | Low (S3 = managed, VL = simple) | High (compaction tuning, retention, index) | Medium (compaction, caching) |
| **Upgrade path** | VL/VT version bump (independent) | Coordinated multi-component rollout | Coordinated rollout |

### Operational Complexity

| Concern | Lakehouse Hybrid | Loki | Tempo |
|---|---|---|---|
| **Compaction** | Size-tiered merge (recent files only, old data untouched) | Rewrites all chunks regardless of age (triggers S3-IA fees) | Rewrites old blocks (triggers S3-IA fees) |
| **Index management** | Manifest (in-memory, auto-built) | DynamoDB/BoltDB (retention policies, migration) | Bloom/search (auto-managed) |
| **Schema changes** | Add promoted columns (backward-compatible) | Label changes need index migration | Schema-less (trace attributes) |
| **Multi-tenancy** | VL/VT native tenant ID | Native tenant ID | Native tenant ID |
| **Retention** | S3 lifecycle rules (zero code) | Per-tenant retention config + compactor | S3 lifecycle + compactor |
| **Backup/restore** | S3 versioning (built-in) | S3 + index backup needed | S3 versioning |
| **Scaling** | Add select pods (stateless) | Scale all components together | Scale ingesters carefully |

### Day-2 Operations

| Task | Lakehouse Hybrid | Loki | Tempo |
|---|---|---|---|
| **Add retention policy** | S3 Lifecycle rule (1 click) | Config change + compactor restart | Config change + compactor |
| **Query cold data** | Transparent (vlselect fan-out) | Same path as hot (all S3) | Same path |
| **Recover from outage** | Restart pods (S3 = durable) | Replay WAL + rebuild index | Replay WAL |
| **Cost optimization** | Move to S3-IA/Glacier after 1yr | Move to S3-IA (needs testing) | Move to S3-IA |
| **Debug slow queries** | Check cache hit rate, row groups scanned | Check chunk size, index, compaction lag | Check bloom filter, cache |

---

## Durability & Reliability

### Data Durability

| Aspect | Lakehouse Hybrid | Loki | Tempo |
|---|---|---|---|
| **Hot tier durability** | EBS: 99.999% (3 AZ replicated by VL) | EBS WAL: 99.999% (single AZ!) | EBS WAL: 99.999% (single AZ!) |
| **Cold tier durability** | **S3: 99.999999999% (11 nines)** | **S3: 99.999999999%** | **S3: 99.999999999%** |
| **Data loss window** | Hot: 0 (EBS replication)<br>Cold: max flush-interval (10s default) | Ingester WAL flush: 1-5s<br>If ingester dies before flush: data loss | Ingester WAL flush: 1-5s |
| **Format durability** | Open Parquet (read by any tool forever) | Proprietary chunks (Loki-only) | Proprietary blocks (Tempo-only) |

### Availability

| Aspect | Lakehouse Hybrid | Loki | Tempo |
|---|---|---|---|
| **Read availability** | VL hot: 3+ replicas<br>Lakehouse cold: stateless + S3 | Querier: stateless + S3<br>Depends on index availability | Querier: stateless + S3 |
| **Write availability** | VL: multi-replica<br>Lakehouse: S3 always writable | Ingester: replication factor 3 | Ingester: replication factor 3 |
| **Degraded mode** | Cold queries continue if hot is down | All queries fail if index unavailable | All queries continue (bloom on S3) |
| **Recovery time** | Pod restart (seconds) | WAL replay: minutes<br>Index rebuild: hours | WAL replay: minutes |

### Data Format & Portability

| Aspect | Lakehouse (Parquet) | Loki (chunks) | Tempo (blocks) |
|---|---|---|---|
| **Format** | Apache Parquet (open standard) | Custom chunk format | Custom block format |
| **Readable by** | DuckDB, Spark, Trino, ClickHouse, Pandas, Polars, Athena, BigQuery | Loki only | Tempo only |
| **Schema evolution** | Column addition (backward-compatible) | Label cardinality limits | Attribute addition |
| **Data export** | Direct S3 access (zero-copy) | API export only | API export only |
| **Analytics** | SQL queries on raw Parquet | LogQL only | TraceQL only |
| **Compliance/audit** | Standard tools read data | Custom tooling needed | Custom tooling needed |

---

## Flexibility

### Query Language Power

| Capability | Lakehouse (LogsQL) | Loki (LogQL) | Tempo (TraceQL) |
|---|---|---|---|
| **Full-text search** | Native (`_msg:error`) | Line filter only (`|= "error"`) | N/A |
| **Field-level queries** | `service.name:="api"` | `{service_name="api"}` (label only) | `.service.name = "api"` |
| **Regex** | `_msg:~"error.*timeout"` | `|~ "error.*timeout"` | `.name =~ ".*timeout"` |
| **Aggregation** | `stats by(service) count()` | `sum(count_over_time(...))` | `{} | count() > 1` (limited) |
| **Structured field access** | Any field, any depth | Labels only (indexed) | Any attribute |
| **Join/correlate** | Cross-signal (logs↔traces) | Single signal | Single signal |
| **Subqueries** | Pipe syntax | Pipe syntax | Pipe syntax |
| **Performance on cold** | Columnar + bloom | Sequential chunk scan | Block scan + bloom |

### Integration Flexibility

| Integration | Lakehouse Hybrid | Loki + Tempo |
|---|---|---|
| **Grafana** | VL + Jaeger datasources (native) | Loki + Tempo datasources (native) |
| **Loki API compatibility** | Via loki-vl-proxy (LogQL subset) | Native |
| **OpenTelemetry** | OTLP ingest (VL/VT native) | OTLP ingest |
| **Kubernetes** | Helm chart, ServiceMonitor, HPA/VPA | Helm chart, Mixins, HPA |
| **Multi-cluster** | S3 as shared storage (any cluster reads) | Per-cluster deployment |
| **Analytics/BI** | Direct Parquet access (Athena, Spark) | Export API only |
| **Alerting** | VL alert rules + Grafana | Loki ruler + Grafana |
| **Custom tooling** | Any Parquet library | Loki API only |

### Storage Tiering Flexibility

| Tier Strategy | Lakehouse Hybrid | Loki | Tempo |
|---|---|---|---|
| **Hot → Cold** | Configurable boundary (default 30d) | No tier split (all S3) | No tier split (all S3) |
| **Cold → Archive** | S3 Lifecycle → S3-IA → Glacier | Manual (compactor config) | S3 Lifecycle |
| **Per-tenant tiering** | VL tenant ID → different S3 paths | Per-tenant retention | Per-tenant retention |
| **Cost optimization** | Hot=30d (fast), Cold=S3 Standard (cheap), Archive=Glacier (cheapest) | S3 Standard only (risky to tier) | S3 Standard + lifecycle |

---

## Long-Term Cost Projection (2 Years)

### Cumulative Cost (500 GB/day)

| Month | Lakehouse Hybrid (cumulative) | Loki + Tempo (cumulative) | VL/VT EBS Only (cumulative) |
|---|---|---|---|
| 6 | $16,884 | $21,660 | $16,074 |
| 12 | $33,768 | $43,320 | $32,148 |
| 18 | $54,852 | $72,888 | $53,004 |
| 24 | **$75,936** | **$102,456** | **$73,860** |

**Cost ranking at all retention periods:**
1. **VL/VT EBS Only** — cheapest at 1-2yr ($2,679/mo at 1yr vs Lakehouse $2,814/mo), but gap narrows with 3 AZ EBS growth
2. **Lakehouse Hybrid** — within 5% of VL/VT EBS, 22% cheaper than Loki, adds open format + S3 durability + DR
3. **Loki + Tempo** — most expensive due to lower compression (3.5x) and compaction I/O

**Lakehouse catches VL/VT EBS Only** when S3 lifecycle tiering activates:
- With 3 AZ, VL/VT EBS per-raw-GB cost is $0.08 × 3 / 55 = $0.0044/raw-GB
- Data >1yr on Glacier Instant ($0.004/GB ÷ 6x = $0.0007/raw-GB) beats VL/VT 3 AZ EBS by 6.3x
- Data >2yr on Glacier Deep ($0.00099/GB ÷ 6x = $0.00017/raw-GB) beats VL/VT 3 AZ EBS by 26x
- At 3+ year retention with lifecycle, Lakehouse becomes cheapest overall

### Break-Even Analysis

```
Lakehouse vs Loki (Lakehouse cheaper from day 1):
  Lakehouse: $2,814/mo vs Loki: $3,610/mo
  Net savings: $796/mo ($9,552/yr)
  At 2yr retention: $1,414/mo savings ($16,968/yr)

Lakehouse vs VL/VT EBS Only (3 AZ):
  Lakehouse: $2,814/mo vs VL/VT: $2,679/mo (1yr retention)
  Net difference: +$135/mo (Lakehouse 5% more expensive)
  At 2yr: +$38/mo (gap narrows as 3× EBS grows)
  At 3yr with lifecycle: Lakehouse ~$1,700/mo cheaper (Glacier tiering)
  
  Lakehouse value beyond storage cost:
  + S3 11-nines durability (vs EBS per-AZ)
  + Open Parquet format (DuckDB, Spark, Trino access)
  + Disaster recovery (independent of hot cluster)
  + S3 lifecycle → Glacier for 3+ year data (crossover at ~2.5yr)
  + No EBS volume management at scale (no 3× EBS provisioning)
```

---

## S3 Storage Class Optimization

### Tiered Storage Strategy (Lakehouse only)

| Data Age | S3 Class | Cost/GB/mo | Monthly Cost (500GB/d, 6x Parquet) | Query Latency |
|---|---|---|---|---|
| 0-30 days | EBS gp3 (VL hot, 55x, 3 AZ) | $0.080 | $66 | <10ms |
| 30-90 days | S3 Standard | $0.023 | $192 | <500ms |
| 90d-1yr | S3 Standard-IA | $0.0125 | $348 | <500ms + 128KB min |
| 1-2yr | S3 Glacier Instant | $0.004 | $203 | <500ms + retrieval fee |
| >2yr | S3 Glacier Deep | $0.00099 | $50 | 12hr retrieval |

**Lifecycle policy savings** (vs all S3 Standard):
- Standard-only (1yr): $642/mo cold storage
- With lifecycle (1yr): ~$450/mo cold storage (30% savings)
- With lifecycle (2yr): ~$620/mo cold storage (vs $1,342 standard = 54% savings)
- With lifecycle (3yr): Glacier Deep data at $0.00099/GB makes long-tail storage nearly free

**Loki cannot safely use S3-IA/Glacier** because:
- Compaction reads and rewrites ALL old chunks (retrieval fees on every compaction cycle)
- Index queries touch old data unpredictably
- Minimum object size (128KB) vs small chunk sizes

**Lakehouse uses S3-IA/Glacier natively** because:
- Compaction only merges recent small files (old data is never read or rewritten)
- Once a file is compacted to optimal size, it's never touched again — safe for lifecycle transitions
- Manifest knows exact file locations (no LIST needed)
- Files are large (10-50MB, well above 128KB minimum)
- Query patterns predictable (partition → file → row groups)

---

## Unified Logs + Traces

### Single-System vs Multi-System

| Aspect | Lakehouse Hybrid (VL + VT + Lakehouse) | Loki + Tempo (two separate systems) |
|---|---|---|
| **Systems to operate** | 1 binary (dual mode) + VL + VT | Loki + Tempo (separate configs, alerts, dashboards) |
| **Correlated queries** | Cross-signal (logs↔traces via trace_id) | Manual correlation (exemplars) |
| **Shared storage** | Same S3 bucket, same Parquet format | Separate buckets/paths, different formats |
| **Shared cache** | Same multi-tier cache for both signals | Separate cache per system |
| **Cost** | Single Lakehouse cluster serves both | 2× compute, 2× ops burden |
| **Schema** | Unified Parquet (logs + traces columns) | Loki chunks + Tempo blocks (incompatible) |

---

## Deletion Costs

A critical cost factor often overlooked: **deleting data from cold storage.**

| Operation | Lakehouse | Loki | Tempo |
|---|---|---|---|
| **Delete single record (S3 Standard)** | $0 tombstone, $0.001 optional rewrite | Not supported | Not supported (whole trace only) |
| **Delete single record (Glacier)** | **$0** (tombstone suppression, no retrieval) | N/A (can't use Glacier) | N/A (can't use Glacier) |
| **Delete by query pattern (1K records)** | **$0** instant (tombstone) | Not supported at record level | Not supported |
| **GDPR right to erasure** | Immediate tombstone (compliant) | Manual stream delete (incomplete) | Manual trace delete |
| **Retention expiry** | S3 Lifecycle rule ($0) | Compactor CPU cost | Compactor CPU cost |

**Why this matters at scale:**
- GDPR/CCPA deletion requests are routine — a customer asks to delete all their logs
- With Loki, you can only delete entire label streams (not individual records matching a pattern)
- With Lakehouse, `POST /delete/logsql/delete?query=customer_id:="CUST-123"` immediately suppresses all matching records across all storage tiers at zero cost

**Lakehouse deletion is Glacier-safe:**
- Tombstones make data invisible without touching the underlying Parquet files
- Files on Glacier are never retrieved for deletion (no $0.03-$0.09/GB retrieval fees)
- Physical deletion happens naturally when S3 lifecycle expires the file
- For S3 Standard files, optional background rewrite reclaims space at minimal cost

Full design: [Deletion Strategy](docs/deletion-strategy.md)

---

## Decision Matrix

| Criterion | Weight | Lakehouse Hybrid | Loki + Tempo | VL/VT EBS Only | Winner |
|---|---|---|---|---|---|
| **Monthly cost (<1yr)** | 20% | $2,814 | $3,610 | $2,679 | **VL/VT EBS** |
| **Monthly cost (2yr)** | 15% | $3,514 | $4,928 | $3,476 | **VL/VT EBS** |
| **Hot query speed** | 15% | <10ms (VL hot) | 100-500ms | <10ms | **Lakehouse / VL/VT** |
| **Cold query speed** | 10% | <500ms (Parquet) | 1-10s | <10ms (all hot) | **VL/VT EBS** |
| **Compression ratio** | 10% | 6-10x (Parquet) | 3-3.5x | 47-70x | **VL/VT** |
| **Data portability** | 10% | Open Parquet | Proprietary | Proprietary | **Lakehouse** |
| **Long retention (3yr+)** | 5% | Glacier ($0.004/GB) | No Glacier support | EBS cost linear | **Lakehouse** |
| **S3 durability** | 5% | 11 nines + open format | 11 nines + locked format | EBS per-AZ | **Lakehouse** |
| **DR / analytics** | 5% | Independent cold tier | N/A | N/A | **Lakehouse** |
| **Community/ecosystem** | 5% | Growing (VM ecosystem) | Large (Grafana ecosystem) | VM ecosystem | Loki |

**Each option excels in different scenarios** — see recommendations below.

---

## Recommendations

### Choose VL/VT EBS Only when:
- Retention ≤ 2 years (cheapest option thanks to 47-70x compression)
- Query speed is top priority (sub-10ms for ALL data, not just hot)
- Simplest architecture (single system, no cold tier)
- EBS volume management at scale is acceptable
- No requirement for open format or external analytics

### Choose Lakehouse Hybrid when:
- Retention > 2 years (S3 lifecycle + Glacier beats EBS at long retention)
- Open format required (compliance, analytics, data portability — DuckDB, Spark, Trino)
- Disaster recovery needed (cold tier independent of hot cluster)
- S3 11-nines durability required (vs EBS per-AZ)
- Direct analytics on cold data (SQL on Parquet without export)
- Unified logs + traces in open format desired
- Always cheaper than Loki (45% savings at 1yr)

### Choose Loki + Tempo when:
- Grafana-native ecosystem already invested
- Large team with existing Loki expertise
- Simple deployment initially (grow into complexity later)
- AGPLv3 license acceptable
- Note: most expensive option at all retention periods
