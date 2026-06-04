---
title: Multi-Tenancy
sidebar_position: 14
---

# Multi-Tenancy

Victoria Lakehouse supports **logical multi-tenancy within a single binary** — one `lakehouse-logs` or `lakehouse-traces` process serves all tenants simultaneously. Both binaries use the **identical tenant configuration** — the same flags, headers, S3 prefix templates, and global read auth apply to logs and traces. Tenant isolation is enforced at the S3 prefix level using the same pattern as Grafana Loki and Grafana Tempo.

**Two tenant identification methods:**
- **Integer headers** (VL/VT native): `X-Scope-AccountID` + `X-Scope-ProjectID` — direct integer IDs
- **String aliases** (Loki/Tempo compatible): `X-Scope-OrgID: prod-team-eu_staging` — human-readable names mapped to integer IDs

## Architecture

### How It Works — Single Binary, Multiple Tenants

```mermaid
graph LR
    subgraph "Tenant Routing (single binary)"
        LH["lakehouse-logs / lakehouse-traces"]
        LH --> TR["TenantResolver<br/>(OrgID → integers)"]
        TR --> EXT["Extract AccountID:ProjectID<br/>from headers or alias"]
        EXT --> RES["Resolve S3 prefix<br/>via template"]
        RES --> MAN["Tenant-scoped manifest<br/>lookup"]
    end

    TA["Tenant A<br/>X-Scope-OrgID: prod-team-eu_staging"] --> LH
    TB["Tenant B<br/>X-Scope-AccountID: 200<br/>X-Scope-ProjectID: 5"] --> LH
    TC["Tenant C<br/>X-Scope-OrgID: dev_default"] --> LH

    MAN -->|"42/3/logs/**"| SA["S3: 42/3/"]
    MAN -->|"200/5/logs/**"| SB["S3: 200/5/"]
    MAN -->|"1/1/logs/**"| SC["S3: 1/1/"]

    style SA fill:#2d6a4f,color:#fff
    style SB fill:#5a189a,color:#fff
    style SC fill:#e76f51,color:#fff
    style LH fill:#264653,color:#fff
    style TR fill:#e9c46a,color:#000
```

1. Every incoming request carries tenant identity via **integer headers** (`X-Scope-AccountID` + `X-Scope-ProjectID`) or a **string alias** (`X-Scope-OrgID`)
2. When `X-Scope-OrgID` is present, the TenantResolver translates it to integer IDs via the configured alias map
3. The prefix template `{AccountID}/{ProjectID}/` (or `{OrgID}/` for standalone) resolves to the tenant's S3 prefix
4. The manifest is tenant-scoped internally: `map[tenantKey]map[partition][]FileInfo` — a query for tenant-A only sees tenant-A's file index
5. All S3 reads and writes are scoped to that prefix — a tenant cannot access another tenant's data
6. When no tenant headers are present, the default `0/0/` prefix is used (single-tenant mode)
7. Requests using integer headers bypass the resolver entirely (zero overhead)

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

### Tenant Name Mapping (X-Scope-OrgID)

Victoria Lakehouse supports Loki/Tempo-compatible string-based tenant identification via the `X-Scope-OrgID` header. String aliases are mapped to VL/VT integer `{AccountID, ProjectID}` pairs at the system boundary — all internal operations remain pure integer.

**Boundary principle:** String aliases exist only at external surfaces (HTTP headers, API responses, metrics labels, UI, logs). Everything inside uses integer IDs for VL/VT compatibility.

#### Alias Format

Aliases follow the [Loki/Tempo tenant ID spec](https://github.com/grafana/dskit/blob/main/tenant/tenant.go):
- **Allowed characters:** `a-z A-Z 0-9 ! - _ . * ' ( )`
- **Max length:** 150 bytes
- **Reserved:** `|` (multi-tenant), `:` (metadata), `/` (not allowed)
- **Compound convention:** Use underscore `_` to combine account and project: `prod-team-eu_staging`

```bash
# String alias (Loki/Tempo compatible)
curl -H "X-Scope-OrgID: prod-team-eu_staging" \
  "http://lakehouse-logs:9428/select/logsql/query?query=*"

# Equivalent integer headers (VL/VT native)
curl -H "X-Scope-AccountID: 42" -H "X-Scope-ProjectID: 3" \
  "http://lakehouse-logs:9428/select/logsql/query?query=*"
```

#### Three Mapping Sources

| Source | Priority | Persistence | Use Case |
|--------|----------|-------------|----------|
| **Static config** | Highest | Config file / Helm | Known tenants at deploy time |
| **Runtime API** | Medium | S3 `_meta/tenant-aliases.json` | Dynamic tenant onboarding without restarts |
| **Auto-discovery** | Lowest | S3 (immediate) | Dynamic environments, first-seen auto-registration |

Static config aliases cannot be overridden by runtime aliases. Runtime aliases are synced across the fleet via the existing stats delta mechanism (default 30s).

#### Configuration

```yaml
lakehouse:
  tenant:
    orgid_header: "X-Scope-OrgID"      # Header name for string tenant ID
    auto_register: false                 # Auto-register unknown OrgIDs
    alias_sync_interval: "30s"           # Fleet sync interval for runtime aliases
    metrics_format: "id"                 # Prometheus label: id | name | both
    aliases:                             # Static alias mappings
      prod-team-eu_staging:
        account_id: 42
        project_id: 3
      prod-team-eu_prod:
        account_id: 42
        project_id: 7
      dev_default:
        account_id: 1
        project_id: 1
```

```bash
--lakehouse.tenant.orgid-header=X-Scope-OrgID
--lakehouse.tenant.auto-register=false
--lakehouse.tenant.alias-sync-interval=30s
--lakehouse.tenant.metrics-format=id
```

#### Aliases CRUD API

Manage aliases at runtime without restarts:

```bash
# List all aliases
curl http://lakehouse-logs:9428/lakehouse/api/v1/tenants/aliases

# Create alias
curl -X POST http://lakehouse-logs:9428/lakehouse/api/v1/tenants/aliases \
  -d '{"org_id":"staging_analytics","account_id":50,"project_id":1}'

# Delete alias
curl -X DELETE http://lakehouse-logs:9428/lakehouse/api/v1/tenants/aliases/staging_analytics
```

Runtime aliases are persisted to `s3://{bucket}/_meta/tenant-aliases.json` and broadcast to all fleet nodes.

#### S3 Prefix Templates

Three template modes for S3 key organization:

| Template | S3 Key Example | Use Case |
|----------|---------------|----------|
| `{AccountID}/{ProjectID}/` (default) | `42/3/logs/dt=2026-05-15/...` | VL/VT compatible |
| `{OrgID}/` | `prod-team-eu_staging/logs/dt=2026-05-15/...` | Standalone deployment |
| `{OrgID}/{ProjectID}/` | `prod-team-eu/3/logs/dt=2026-05-15/...` | Hybrid string + integer |

The `{OrgID}` template requires at least one alias configured (or `auto_register: true`) and is not compatible with VL/VT upstream storage node integration.

#### Prometheus Metrics Format

Controlled by `--lakehouse.tenant.metrics-format`:

| Value | Label Example | Notes |
|-------|--------------|-------|
| `id` (default) | `tenant="42:3"` | Zero change from current behavior |
| `name` | `tenant="prod-team-eu_staging"` | Falls back to `"42:3"` if no alias |
| `both` | `tenant="42:3"` + `tenant_name="prod-team-eu_staging"` | Extra label — opt-in for cardinality |

#### Display Names on External Surfaces

When aliases are configured, friendly names appear on all external surfaces:

- **API responses**: `"name": "prod-team-eu_staging"` field added to all tenant entries
- **Explorer UI**: tenant selector shows friendly names, tables show name when available
- **Structured logs**: `tenant=prod-team-eu_staging tenant_id=42:3`
- **Prometheus metrics**: configurable via `metrics_format`

Integer IDs are always available alongside names for correlation and debugging.

### Enterprise: Bucket-Per-Tenant Isolation

For regulated environments requiring IAM-level hard isolation, the same single binary can resolve different S3 buckets per tenant. Two modes are available:

**Templated** (every tenant follows the same pattern):

```yaml
lakehouse:
  tenant:
    isolation: bucket
    bucket_template: "obs-{AccountID}-{ProjectID}"
```

**Mixed** (most tenants share the default bucket; a few get dedicated buckets via overrides):

```yaml
lakehouse:
  tenant:
    overrides:
      "1002:0":
        s3:
          bucket: obs-acme       # acme-corp gets a dedicated bucket
      acme-archive:
        s3:
          bucket: obs-archive    # alias-keyed; resolved on alias-sync tick
```

In mixed mode the s3reader `PoolRegistry` caches a separate `ClientPool` per bucket. The writer's `SetTenantBucket(account, project) → bucket` resolver stamps `manifest.FileInfo.Bucket` on every flushed Parquet so reads route back to the right bucket. Sidecars and the fleet-wide manifest stay in the default bucket so a single manifest still resolves files across many tenant buckets — the only sharded thing is the data files themselves.

### Retroactive Bucket Migration

When a tenant gets a bucket override after writes have already started, existing Parquet objects need to move from the shared bucket to the tenant's new dedicated bucket. The admin endpoint handles this without ingest downtime:

```bash
$ curl -sX POST -H 'X-Lakehouse-Global-Read: <admin-key>' \
       --data '{"tenant_key":"1002:0","target_bucket":"obs-archive"}' \
       http://lakehouse-logs:9428/lakehouse/api/v1/admin/tenant/migrate | jq
{
  "account_id": 1002, "project_id": 0, "target_bucket": "obs-archive",
  "files_scanned": 38, "files_moved": 38,
  "bytes_moved": 412382912, "duration_ms": 8412
}
```

The migrator works file-by-file in this exact order to keep crash recovery safe:

1. **S3 server-side copy** to the new bucket (source still readable).
2. **Manifest flip** — `manifest.SetFileBucket` points the existing file entry at the new bucket. Reads immediately resolve to the new location; the entry is rewritten atomically.
3. **Delete source** in the old bucket.

A crash between steps 2 and 3 leaves orphaned bytes in the old bucket (cleanable by S3 lifecycle or the orphan sweeper); a crash between 1 and 2 leaves the new copy unreferenced (cleanable the same way). The order never leaves a dangling manifest pointer.

The endpoint is closed by default. Access is gated by the same global-read credential surface as cross-tenant reads — either:
- `X-Lakehouse-Global-Read: <value>` matching `tenant.global_read_value`, or
- `Authorization: Bearer <token>` matching `tenant.global_read_token`.

A missing or wrong credential returns `403 admin auth required`.

## Per-Tenant Policy Overrides

Some tenants need different retention, cardinality caps, ingest rate limits, lifecycle transitions, or S3 buckets than the fleet defaults. The `tenant.overrides` map carries these without restarts or per-tenant binaries.

### Schema

```yaml
lakehouse:
  tenant:
    overrides:
      # Key is either "account:project" (integer-keyed, deterministic)
      # or an OrgID alias (resolved at startup + on every alias-sync tick).
      "1002:0":
        retention: 2160h            # 90 days — overrides global retention
        cardinality:
          max_streams: 5000         # per-tenant distinct-stream cap
          max_fields:  1000         # per-tenant distinct-field cap
        ingest:
          max_bytes_per_sec: 5242880  # 5 MiB/s token bucket
          max_rows_per_sec:  10000
        lifecycle:                  # per-tenant S3 transitions (shadows global rules)
          - { transition_days: 7,  storage_class: ONEZONE_IA }
          - { transition_days: 60, storage_class: GLACIER }
        s3:
          bucket: obs-acme          # per-tenant bucket override (see Bucket-Per-Tenant above)

      acme-corp:                    # alias-keyed entry — resolves once acme-corp is registered
        retention: 720h             # 30 days
```

Every field is optional. Zero or missing means "inherit the global setting" — there is no special inheritance keyword. This makes config diffs honest: `retention: 0` does not silently mean "use 30 days from elsewhere"; it means "follow whatever the global retention is right now."

### Consumers

Each override flows to exactly one subsystem:

| Override | Consumer | Behavior |
|---|---|---|
| `retention` | `retention.Manager` | synthesizes a match rule on the file's `account_id`/`project_id` manifest labels; the existing rules engine handles eviction. No special-case code path. |
| `cardinality.max_streams` / `max_fields` | `tenant.CardinalityLimiter` mounted as `TenantCardinalityGate` on the vlstorage insert paths (logs + traces) | per-tenant distinct counters with fast-path RLock for known entries; overflow drops at the boundary before the writer sees the row. |
| `ingest.max_bytes_per_sec` / `max_rows_per_sec` | `tenant.IngestRateLimiter` + `tenant.RateLimitMiddleware` | independent byte/sec + row/sec token buckets per tenant; pre-flight Content-Length check returns `429 Too Many Requests` + `X-RateLimit-Limit-Bytes` / `X-RateLimit-Remaining-Bytes` / `X-RateLimit-Retry-After-Ms` headers. |
| `lifecycle` | `delete.StorageClassDetector.SetTenantRules` consumed by both the manual `predict` handler and the background rewriter scheduler | tenant-keyed rule lookup parses `(account, project)` from the file's S3 key prefix, so manual predictions and the rewriter agree on what storage class a file should end up in. |
| `s3.bucket` | s3reader `PoolRegistry` + writer `SetTenantBucket`/`SetTenantPool` | tenant's reads/writes route to the dedicated bucket; manifest stamps the bucket so post-migration reads still resolve correctly. |

### Policy API

```bash
# List all configured overrides + their resolution state
$ curl -s http://lakehouse-logs:9428/lakehouse/api/v1/tenants/policy | jq
{
  "entries": [
    {
      "account_id": 1002, "project_id": 0, "org_id": "acme-corp",
      "retention": "2160h0m0s",
      "max_streams": 5000, "max_fields": 1000,
      "max_bytes_per_sec": 5242880, "max_rows_per_sec": 10000,
      "lifecycle": [
        {"transition_days": 7,  "storage_class": "ONEZONE_IA"},
        {"transition_days": 60, "storage_class": "GLACIER"}
      ],
      "bucket": "obs-acme"
    },
    {"account_id": 1, "project_id": 1, "retention": "168h0m0s"}
  ],
  "pending_aliases": []     // empty = every alias-keyed entry resolved
}

# Per-tenant view — same payload as /tenants/{id} plus a `policy` block
$ curl -s http://lakehouse-logs:9428/lakehouse/api/v1/tenants/1002:0 | jq .policy
{
  "retention": "2160h0m0s",
  "max_streams": 5000,
  "bucket": "obs-acme"
}
```

The PolicyRegistry caches resolved entries in a `sync.Map` keyed by `(account, project)` and re-runs alias resolution on the configured `tenant.alias_sync_interval` tick. Late-registered tenants (via auto-register or runtime CRUD) pick up their alias-keyed overrides on the next tick without a process restart.

### Forward-Compatibility Notes

- **Default behavior unchanged.** Empty `tenant.overrides:` map preserves prefix-only isolation with no per-tenant limits and no bucket routing.
- **Manifest schema.** `FileInfo.Bucket` is `omitempty` — old manifests load unchanged; new manifests stamp it only when bucket routing is active.
- **Path mutability.** Forward changes (template change → new writes hit new prefix/bucket) are automatic. Moving existing files requires hitting the `/admin/tenant/migrate` endpoint described above.

## Configuration

### CLI Flags

```bash
# Tenant routing (required for multi-tenant)
--lakehouse.tenant.prefix-template="{AccountID}/{ProjectID}/"
--lakehouse.tenant.default-account=0
--lakehouse.tenant.default-project=0
--lakehouse.tenant.header-account=X-Scope-AccountID
--lakehouse.tenant.header-project=X-Scope-ProjectID

# Tenant name mapping (Loki/Tempo compatible)
--lakehouse.tenant.orgid-header=X-Scope-OrgID
--lakehouse.tenant.metrics-format=id
--lakehouse.tenant.auto-register=false
--lakehouse.tenant.alias-sync-interval=30s

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
| `--lakehouse.tenant.prefix-template` | `{AccountID}/{ProjectID}/` | S3 prefix pattern. Supports `{AccountID}`, `{ProjectID}`, `{OrgID}` |
| `--lakehouse.tenant.isolation` | `prefix` | Isolation mode: `prefix` (shared bucket) or `bucket` (separate buckets) |
| `--lakehouse.tenant.bucket-template` | (empty) | Bucket name pattern for `bucket` isolation mode |
| `--lakehouse.tenant.default-account` | `0` | Default AccountID when header is absent (single-tenant mode) |
| `--lakehouse.tenant.default-project` | `0` | Default ProjectID when header is absent (single-tenant mode) |
| `--lakehouse.tenant.header-account` | `X-Scope-AccountID` | HTTP header for AccountID extraction |
| `--lakehouse.tenant.header-project` | `X-Scope-ProjectID` | HTTP header for ProjectID extraction |
| `--lakehouse.tenant.orgid-header` | `X-Scope-OrgID` | HTTP header for string-based tenant identification (Loki/Tempo compatible) |
| `--lakehouse.tenant.metrics-format` | `id` | Prometheus tenant label format: `id`, `name`, or `both` |
| `--lakehouse.tenant.auto-register` | `false` | Auto-register unknown X-Scope-OrgID values as new aliases |
| `--lakehouse.tenant.alias-sync-interval` | `30s` | Fleet sync interval for runtime-added aliases |
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

    # Tenant Name Mapping (X-Scope-OrgID)
    orgid_header: "X-Scope-OrgID"   # Loki/Tempo compatible header
    metrics_format: "id"             # id | name | both
    auto_register: false             # auto-register unknown OrgIDs
    alias_sync_interval: "30s"       # fleet sync interval
    aliases:                         # static alias → integer ID mappings
      prod-team-eu_staging:
        account_id: 42
        project_id: 3
      prod-team-eu_prod:
        account_id: 42
        project_id: 7
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

All metrics include a `tenant` label when multi-tenancy is enabled. The label format is controlled by `--lakehouse.tenant.metrics-format`:

```
# metrics_format=id (default)
lakehouse_insert_rows_total{tenant="42:3"}
lakehouse_query_duration_seconds{tenant="200:5"}

# metrics_format=name (when alias configured)
lakehouse_insert_rows_total{tenant="prod-team-eu_staging"}
lakehouse_query_duration_seconds{tenant="200:5"}  # fallback if no alias

# metrics_format=both
lakehouse_insert_rows_total{tenant="42:3",tenant_name="prod-team-eu_staging"}
```

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
# Default (metrics_format=id)
lakehouse_tenant_files{tenant="42:3"}
lakehouse_tenant_bytes{tenant="42:3"}
lakehouse_tenant_rows_total{tenant="42:3"}
lakehouse_tenant_queries_total{tenant="42:3"}

# With metrics_format=name and alias configured
lakehouse_tenant_files{tenant="prod-team-eu_staging"}
lakehouse_tenant_bytes{tenant="prod-team-eu_staging"}
```

When the cap is reached, additional tenants are still visible in the JSON API but not emitted as Prometheus metrics.

### JSON API

Per-tenant drill-down, cost allocation, cardinality analysis, and alias management:

| Endpoint | Description |
|----------|-------------|
| `GET /lakehouse/api/v1/tenants` | Tenant summary list (includes `name` field when alias configured) |
| `GET /lakehouse/api/v1/tenants/{accountID}/{projectID}` | Tenant drill-down by integer IDs |
| `GET /lakehouse/api/v1/tenants/{orgId}` | Tenant drill-down by alias (e.g., `prod-team-eu_staging`) |
| `GET /lakehouse/api/v1/tenants/aliases` | List all alias mappings |
| `POST /lakehouse/api/v1/tenants/aliases` | Create or update alias |
| `DELETE /lakehouse/api/v1/tenants/aliases/{orgId}` | Remove alias |
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
