---
title: Multi-Tenancy
sidebar_position: 14
---

# Multi-Tenancy

Victoria Lakehouse supports **logical multi-tenancy within a single binary** — one `lakehouse-logs` or `lakehouse-traces` process serves all tenants simultaneously. Both binaries use the **identical tenant configuration** — the same flags, headers, S3 prefix templates, and global read auth apply to logs and traces. Tenant isolation is enforced at the S3 prefix level using the same pattern as Grafana Loki and Grafana Tempo.

## Architecture

### How It Works — Single Binary, Multiple Tenants

```mermaid
graph LR
    subgraph "Tenant Routing (single binary)"
        LH["lakehouse-logs / lakehouse-traces"]
        LH --> EXT["Extract AccountID:ProjectID<br/>from HTTP headers"]
        EXT --> RES["Resolve S3 prefix<br/>via template"]
        RES --> MAN["Tenant-scoped manifest<br/>lookup"]
    end

    TA["Tenant A Request<br/>(X-Scope-AccountID: 100<br/>X-Scope-ProjectID: 1)"] --> LH
    TB["Tenant B Request<br/>(X-Scope-AccountID: 200<br/>X-Scope-ProjectID: 5)"] --> LH

    MAN -->|"100/1/logs/**"| SA[("S3: 100/1/")]
    MAN -->|"200/5/logs/**"| SB[("S3: 200/5/")]

    style SA fill:#2d6a4f,color:#fff
    style SB fill:#5a189a,color:#fff
    style LH fill:#264653,color:#fff
```

1. Every incoming request carries `AccountID` and `ProjectID` via HTTP headers (from vmauth, API gateway, or direct)
2. The prefix template `{AccountID}/{ProjectID}/` resolves to the tenant's S3 prefix
3. The manifest is tenant-scoped internally: `map[tenantKey]map[partition][]FileInfo` — a query for tenant-A only sees tenant-A's file index
4. All S3 reads and writes are scoped to that prefix — a tenant cannot access another tenant's data
5. When no tenant headers are present, the default `0/0/` prefix is used (single-tenant mode)

### S3 Layout

```
s3://obs-archive/
  0/0/                          ← default tenant (single-tenant / no headers)
    logs/dt=2026-01-15/hour=00/batch-001.parquet
    traces/dt=2026-01-15/hour=00/batch-003.parquet

  100/1/                        ← tenant 100:1
    logs/dt=2026-01-15/hour=00/batch-004.parquet
    traces/dt=2026-01-15/hour=00/batch-005.parquet

  200/5/                        ← tenant 200:5
    logs/dt=2026-01-15/hour=00/batch-006.parquet
```

### Enterprise: Bucket-Per-Tenant Isolation

For regulated environments requiring IAM-level hard isolation, the same single binary can resolve different S3 buckets per tenant:

```yaml
lakehouse:
  tenant:
    isolation: bucket
    bucket_template: "obs-{AccountID}-{ProjectID}"
```

Each tenant gets its own S3 bucket with independent IAM policies, encryption keys, and lifecycle rules. The binary resolves the bucket name from the template at runtime — still one binary, still one process.

## Configuration

### CLI Flags

```bash
# Tenant routing (required for multi-tenant)
--lakehouse.tenant.prefix-template="{AccountID}/{ProjectID}/"
--lakehouse.tenant.default-account=0
--lakehouse.tenant.default-project=0
--lakehouse.tenant.header-account=X-Scope-AccountID
--lakehouse.tenant.header-project=X-Scope-ProjectID

# Enterprise bucket isolation
--lakehouse.tenant.isolation=bucket
--lakehouse.tenant.bucket-template="obs-{AccountID}-{ProjectID}"

# Global read mode (admin dashboards)
--lakehouse.tenant.global-read-header=X-Lakehouse-Global-Read
--lakehouse.tenant.global-read-value=super-secret-admin-key
```

### Flag Reference

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.tenant.prefix-template` | `{AccountID}/{ProjectID}/` | S3 prefix pattern per tenant |
| `--lakehouse.tenant.isolation` | `prefix` | Isolation mode: `prefix` (shared bucket) or `bucket` (separate buckets) |
| `--lakehouse.tenant.bucket-template` | (empty) | Bucket name pattern for `bucket` isolation mode |
| `--lakehouse.tenant.default-account` | `0` | Default AccountID when header is absent (single-tenant mode) |
| `--lakehouse.tenant.default-project` | `0` | Default ProjectID when header is absent (single-tenant mode) |
| `--lakehouse.tenant.header-account` | `X-Scope-AccountID` | HTTP header for AccountID extraction |
| `--lakehouse.tenant.header-project` | `X-Scope-ProjectID` | HTTP header for ProjectID extraction |
| `--lakehouse.tenant.global-read-header` | (empty, disabled) | HTTP header name to trigger global read across all tenants |
| `--lakehouse.tenant.global-read-value` | (empty) | Required value for the global read header (acts as a shared secret) |
| `--lakehouse.tenant.global-read-token` | (empty, disabled) | Bearer token for global read via `Authorization: Bearer <token>` header |

### YAML

```yaml
lakehouse:
  tenant:
    prefix_template: "{AccountID}/{ProjectID}/"
    isolation: prefix           # prefix | bucket
    bucket_template: ""         # only for isolation=bucket
    default_account: "0"        # single-tenant default
    default_project: "0"        # single-tenant default
    header_account: "X-Scope-AccountID"
    header_project: "X-Scope-ProjectID"
    global_read_header: ""      # empty = disabled (custom header method)
    global_read_value: ""       # shared secret for custom header method
    global_read_token: ""       # empty = disabled (Bearer token method)
```

### Single-Tenant (Default)

Out of the box, Victoria Lakehouse runs in single-tenant mode. All data uses the default `0/0/` prefix. No headers needed:

```
s3://obs-archive/0/0/logs/dt=2026-01-15/hour=00/batch.parquet
```

### Multi-Tenant with vmauth

[vmauth](https://docs.victoriametrics.com/vmauth/) extracts tenant IDs from request paths or headers and forwards them to the single lakehouse binary:

```yaml
# vmauth config — routes all tenants to the SAME lakehouse binary
unauthorized_user:
  url_map:
    - src_paths:
        - "/insert/.*"
        - "/select/.*"
      url_prefix: "http://lakehouse-logs:9428"
      headers:
        - "X-Scope-AccountID: {accountID}"
        - "X-Scope-ProjectID: {projectID}"
```

The lakehouse binary extracts these headers per-request and routes to the correct S3 prefix. No need for separate deployments per tenant.

### Deployment Patterns

```mermaid
graph TB
    subgraph "Pattern A: Single Binary — All Tenants (default)"
        VA["vmauth<br/>(tenant routing)"] --> LH1["lakehouse-logs<br/>(all tenants)"]
        VA --> LH2["lakehouse-traces<br/>(all tenants)"]
        LH1 --> S1[("S3 obs-archive<br/>100/1/ · 200/5/ · 300/3/")]
        LH2 --> S1
    end
```

```mermaid
graph TB
    subgraph "Pattern B: Scaled — All Tenants"
        VA2["vmauth"] --> INS["insert-0,1,2<br/>(all tenants)"]
        VA2 --> SEL["select-0,1,2<br/>(all tenants)"]
        INS --> S2[("S3")]
        SEL --> S2
    end
```

```mermaid
graph TB
    subgraph "Pattern C: Bucket Isolation"
        VA3["vmauth"] --> LH3["lakehouse-logs<br/>(single binary)"]
        LH3 -->|"tenant 100/1"| B1[("obs-100-1")]
        LH3 -->|"tenant 200/5"| B2[("obs-200-5")]
        LH3 -->|"tenant 300/3"| B3[("obs-300-3")]
    end
```

| Pattern | Description | When to Use |
|---|---|---|
| **A: Single binary, all tenants** | One lakehouse process handles all tenants via header routing | Default, up to hundreds of tenants |
| **B: Scaled insert + select** | Multiple insert/select pods behind a load balancer, all serving all tenants | High throughput, many tenants |
| **C: Bucket isolation** | Single binary, but each tenant in a separate S3 bucket | IAM-level isolation without separate deployments |
| **D: Dedicated fleet per tenant** | Separate deployment per tenant (each with its own binary) | Extreme isolation, compliance, noisy-neighbor avoidance |

### Enterprise Bucket-Per-Tenant

For strict regulatory requirements (HIPAA, SOC2, FedRAMP):

```yaml
lakehouse:
  tenant:
    isolation: bucket
    bucket_template: "obs-{AccountID}-{ProjectID}"
```

Each bucket can have:
- Independent IAM policies (cross-account access control)
- Separate KMS encryption keys
- Independent lifecycle rules (different retention per tenant)
- Separate S3 Access Logs for compliance audit

## Tenant-Scoped Internals

### Manifest

The manifest tracks files per tenant. Queries only see their own tenant's files:

```
manifest.tenants = {
  "100/1": {
    "dt=2026-01-15/hour=00": [file1.parquet, file2.parquet],
    "dt=2026-01-15/hour=01": [file3.parquet],
  },
  "200/5": {
    "dt=2026-01-15/hour=00": [file4.parquet],
  },
}
```

### Write Path

`MustAddRows` extracts tenant from the request context (set by header middleware) and writes to the tenant-scoped S3 prefix. Each tenant's data is flushed independently.

### Read Path

`RunQuery` resolves the tenant from `QueryContext.TenantIDs`, looks up only that tenant's manifest entries, and scans only that tenant's Parquet files. Cross-tenant data leakage is impossible at the storage layer.

### Delete Path

Tombstones are tenant-scoped. Each tenant's tombstones are stored at `s3://{bucket}/{tenant}/_tombstones/`. A delete request for tenant-A cannot affect tenant-B's data.

### Compaction

Compaction runs per-tenant. Each tenant's files are merged independently.

### Metrics

All metrics include a `tenant` label when multi-tenancy is enabled:
- `lakehouse_insert_rows_total{tenant="100/1"}`
- `lakehouse_query_duration_seconds{tenant="200/5"}`
- `lakehouse_s3_bytes_read_total{tenant="100/1"}`

## Analytics Tool Compatibility

S3 prefix isolation preserves full compatibility with all Parquet tools. Each tenant's prefix is a self-contained Hive-partitioned dataset:

| Tool | Per-Tenant Query |
|---|---|
| **DuckDB** | `read_parquet('s3://bucket/100/1/logs/**/*.parquet')` |
| **ClickHouse** | `s3('http://s3/bucket/100/1/logs/**/*.parquet', 'Parquet')` |
| **Trino** | External table with `location = 's3://bucket/100/1/logs/'` |
| **Spark** | `spark.read.parquet("s3a://bucket/100/1/logs/")` |
| **pandas** | `pd.read_parquet("s3://bucket/100/1/logs/")` |

With bucket isolation, each tenant is a different bucket:

| Tool | Per-Tenant Query (bucket isolation) |
|---|---|
| **DuckDB** | `read_parquet('s3://obs-100-1/logs/**/*.parquet')` |
| **ClickHouse** | `s3('http://s3/obs-100-1/logs/**/*.parquet', 'Parquet')` |

## Cost Attribution

### Prefix Isolation

```bash
# Storage per tenant
aws s3 ls s3://obs-archive/100/1/ --recursive --summarize
# Total Objects: 42,103
# Total Size: 148.3 GiB
```

Use S3 Storage Lens with prefix-level grouping or S3 Inventory reports for automated cost allocation.

### Bucket Isolation

Native per-bucket billing. Each tenant's storage cost appears as a separate line item in AWS Cost Explorer.

## Comparison with Industry Patterns

| System | Tenancy Model | Process Model | Isolation |
|---|---|---|---|
| **Victoria Lakehouse** | S3 prefix per tenant (or bucket) | Single binary, all tenants | Physical (path/bucket) |
| **Grafana Loki** | S3 prefix per tenant | Single binary, all tenants | Physical (path) |
| **Grafana Tempo** | S3 prefix per tenant | Single binary, all tenants | Physical (path) |
| **ClickHouse Cloud** | Row-level security | Shared process | Logical (software) |
| **Snowflake** | Account-level | Separate compute | Physical (account) |
| **Databricks** | Schema-per-tenant | Shared cluster | Physical (path) |

## Global Read Mode (Cross-Tenant Admin Access)

In multi-tenant deployments, you often need admin dashboards in Grafana that show data across **all** tenants — for example, a platform-wide error rate dashboard, capacity planning, or SLO reporting. Global read mode enables this explicitly.

### Configuration

Global read is **disabled by default** and must be explicitly enabled. Two authentication methods are supported — use either or both:

**Method 1: Custom header + shared secret** (simpler, good for internal use)

```yaml
lakehouse:
  tenant:
    global_read_header: "X-Lakehouse-Global-Read"
    global_read_value: "super-secret-admin-key"
```

```bash
--lakehouse.tenant.global-read-header=X-Lakehouse-Global-Read
--lakehouse.tenant.global-read-value=super-secret-admin-key
```

**Method 2: Bearer token** (standard HTTP auth, Grafana-native)

```yaml
lakehouse:
  tenant:
    global_read_token: "eyJhbGciOiJIUzI1NiIs..."
```

```bash
--lakehouse.tenant.global-read-token=eyJhbGciOiJIUzI1NiIs...
```

When configured, requests must include `Authorization: Bearer <token>` AND have no tenant headers (or the global read header). This method is preferred for Grafana integration because Grafana natively supports Bearer token auth in datasource config.

**Method 3: Both (defense-in-depth)**

Configure both methods. The request must satisfy at least one to get global read access.

### How It Works

When a request authenticates for global read (via header+value or Bearer token), the read path scans **all tenant prefixes** in the manifest and returns merged results:

```bash
# Normal tenant-scoped query (only sees tenant 100/1 data)
curl -H "X-Scope-AccountID: 100" -H "X-Scope-ProjectID: 1" \
  "http://lakehouse-logs:9428/select/logsql/query?query=*"

# Global read via custom header
curl -H "X-Lakehouse-Global-Read: super-secret-admin-key" \
  "http://lakehouse-logs:9428/select/logsql/query?query=*"

# Global read via Bearer token
curl -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIs..." \
  "http://lakehouse-logs:9428/select/logsql/query?query=*"
```

### Grafana Integration

Create a separate Grafana datasource for global read access. Choose one auth method:

**Option A: Custom header** (Grafana custom HTTP headers)

```yaml
# Logs — global read
- name: "Lakehouse Logs Global (All Tenants)"
  type: victoriametrics-logs-datasource
  access: proxy
  url: http://lakehouse-logs:9428
  jsonData:
    httpHeaderName1: "X-Lakehouse-Global-Read"
  secureJsonData:
    httpHeaderValue1: "super-secret-admin-key"

# Traces — global read (same key works for both binaries)
- name: "Lakehouse Traces Global (All Tenants)"
  type: jaeger
  access: proxy
  url: http://lakehouse-traces:10428
  jsonData:
    httpHeaderName1: "X-Lakehouse-Global-Read"
  secureJsonData:
    httpHeaderValue1: "super-secret-admin-key"
```

**Option B: Bearer token/key** (Grafana native auth — token and key are interchangeable)

```yaml
# Logs — global read via Bearer token
- name: "Lakehouse Logs Global (All Tenants)"
  type: victoriametrics-logs-datasource
  access: proxy
  url: http://lakehouse-logs:9428
  jsonData:
    httpHeaderName1: "Authorization"
  secureJsonData:
    httpHeaderValue1: "Bearer eyJhbGciOiJIUzI1NiIs..."

# Traces — global read via Bearer token (same token works)
- name: "Lakehouse Traces Global (All Tenants)"
  type: jaeger
  access: proxy
  url: http://lakehouse-traces:10428
  jsonData:
    httpHeaderName1: "Authorization"
  secureJsonData:
    httpHeaderValue1: "Bearer eyJhbGciOiJIUzI1NiIs..."
```

Use these datasources for admin dashboards only. Regular tenant-scoped datasources use vmauth routing with `X-Scope-AccountID`/`X-Scope-ProjectID` headers.

> **Both binaries share the same tenant config.** Set `--lakehouse.tenant.*` flags identically on both `lakehouse-logs` and `lakehouse-traces` deployments. The same token/key works for both.

### CLI Access

```bash
# Global read on logs (custom header)
curl -H "X-Lakehouse-Global-Read: super-secret-admin-key" \
  "http://lakehouse-logs:9428/select/logsql/query?query=severity_text:ERROR&limit=100"

# Global read on traces (same key)
curl -H "X-Lakehouse-Global-Read: super-secret-admin-key" \
  "http://lakehouse-traces:10428/select/logsql/query?query=*&limit=100"

# Using Bearer token/key (interchangeable with custom header)
curl -H "Authorization: Bearer eyJhbGciOiJIUzI1NiIs..." \
  "http://lakehouse-logs:9428/select/logsql/query?query=severity_text:ERROR&limit=100"

# Regular tenant-scoped access
curl -H "X-Scope-AccountID: 100" -H "X-Scope-ProjectID: 1" \
  "http://lakehouse-logs:9428/select/logsql/query?query=severity_text:ERROR&limit=100"
```

### Security

- **Disabled by default**: no global read header, value, or token configured — no cross-tenant reads possible
- **Dual auth support**: custom header+secret for simple internal use; Bearer token for Grafana-native integration and standard tooling
- **Read-only**: global read mode only affects queries (select endpoints). Insert and delete operations always require a tenant scope
- **Token management**: store tokens in Kubernetes secrets, rotate periodically. Tokens are compared using constant-time comparison to prevent timing attacks
- **Audit**: global read requests are logged with `global_read=true` label in structured logs and metrics, including the auth method used
- **Network isolation**: restrict global read to internal admin networks. Do NOT expose through public-facing vmauth routes or load balancers

### Analytics with Global Read

External Parquet tools (DuckDB, ClickHouse, Trino, Spark) can achieve global read by globbing across all tenant prefixes:

```sql
-- DuckDB: read across all tenants
SELECT * FROM read_parquet('s3://obs-archive/*/*/logs/**/*.parquet', hive_partitioning=true);

-- ClickHouse: all tenants
SELECT * FROM s3('http://minio:9000/obs-archive/*/*/logs/**/*.parquet', 'key', 'secret', 'Parquet');
```

This works because each tenant prefix follows the same Hive partition structure.

## Security Considerations

- **Physical path isolation**: each tenant's data is at a separate S3 prefix. Even within one binary, the code resolves the prefix from request headers and never reads across prefixes.
- **vmauth as the auth boundary**: tenant routing and authentication happens at vmauth. The lakehouse binary trusts the headers.
- **Defense-in-depth with S3 IAM**: for bucket isolation, each tenant's IAM policy restricts access to its own bucket. Even if the application is compromised, cross-tenant access is blocked at IAM.
- **Audit trail**: S3 Access Logs and CloudTrail record all object-level operations per tenant.
- **No row-level filtering**: we do NOT mix tenants in shared Parquet files. Each file belongs to exactly one tenant. This eliminates the risk class where a missing `WHERE tenant=X` leaks data.
- **Global read is opt-in**: cross-tenant reads require explicit configuration of both header name and secret value. Without this configuration, no request can read across tenants.

## Tenant Statistics & Monitoring

Victoria Lakehouse tracks per-tenant storage statistics in real-time. See [Tenant Stats](tenant-stats.md) for the full reference.

### Per-Tenant Prometheus Metrics

Subject to a configurable cardinality cap (`stats.metrics_cardinality_limit`, default 100):

```
lakehouse_tenant_files{tenant="100/1"}        # File count
lakehouse_tenant_bytes{tenant="100/1"}        # Compressed bytes
lakehouse_tenant_rows_total{tenant="100/1"}   # Cumulative rows
lakehouse_tenant_queries_total{tenant="100/1"} # Cumulative queries
```

When the cap is reached, additional tenants are still visible in the JSON API but not emitted as Prometheus metrics.

### JSON API

Per-tenant drill-down, cost allocation, and cardinality analysis:

| Endpoint | Description |
|----------|-------------|
| `GET /lakehouse/api/v1/tenants` | Tenant summary list |
| `GET /lakehouse/api/v1/tenants/{accountID}/{projectID}` | Tenant drill-down |
| `GET /lakehouse/api/v1/stats/cost` | Cost breakdown by tenant |
| `GET /lakehouse/api/v1/cardinality/fields?tenant=100/1` | Per-tenant field cardinality |

### Lakehouse Explorer UI

The built-in [Lakehouse Explorer](lakehouse-explorer.md) provides a visual tenant dashboard with storage breakdown, cost allocation, and field cardinality analysis. It integrates into VL/VT's VMUI as an optional tab.

### Cost Allocation

The cost estimation model tracks storage class distribution per tenant and applies configurable per-GB pricing. This enables:

- **Chargeback**: bill each tenant for their actual S3 cost
- **Showback**: visibility into cost distribution without billing
- **Lifecycle savings**: show how much each tenant saves from S3 lifecycle transitions

Per-tenant lifecycle and pricing overrides are supported in bucket-isolation mode via `tenant.known_tenants[]`.
