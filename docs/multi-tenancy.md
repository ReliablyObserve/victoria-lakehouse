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

1. Every incoming request carries tenant identity via **integer headers** (`X-Scope-AccountID` + `X-Scope-ProjectID`, or VL/VT's native `AccountID` + `ProjectID`) or a **string alias** (`X-Scope-OrgID`)
2. When `X-Scope-OrgID` is present, the TenantResolver translates it to integer IDs via the configured alias map
3. The prefix template `{AccountID}/{ProjectID}/` resolves to the tenant's S3 prefix for writes
4. The manifest keeps per-tenant aggregates (the partitions each tenant owns), so a read resolves exactly the requesting tenant's objects — see [Read Scoping](#read-scoping-which-data-a-request-sees)
5. Every read — row queries, hits, stats, field and stream enumeration, pmeta catalog answers, Jaeger/Tempo, and the unflushed buffer window — is scoped to that one tenant
6. When no tenant headers are present, the request is the default tenant `0:0` (prefix `0/0/`), exactly like upstream VL/VT
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

### Read Scoping (Which Data a Request Sees)

A select request is answered from **exactly one tenant** — the same rule upstream VictoriaLogs and VictoriaTraces apply. Lakehouse widens a read to every tenant only for a request that presents the configured [global-read credential](#global-read-mode-cross-tenant-admin-access).

| Request | Answered from |
|---|---|
| `AccountID`/`ProjectID` (or `X-Scope-*`, or a resolved `X-Scope-OrgID`) headers for tenant `A:P` | tenant `A:P` only |
| no tenant headers | tenant `0:0` only |
| headers for a tenant that holds no data | an empty answer — never a fall-through to other tenants |
| `/internal/select/*` with `tenant_ids=[…]` (VL's cluster protocol, e.g. from a `vlselect` node) | exactly the listed tenants |
| valid global-read header or bearer token | every tenant |
| wrong global-read value, or global read not configured | the tenant from the headers (`0:0` without headers) — never widened |

This applies on both binaries to every read surface that returns stored data: `/select/logsql/query`, `hits`, `stats_query`, `stats_query_range`, `facets`, `field_names`, `field_values`, `stream_field_values`, `streams`, `stream_ids`, and the Jaeger (`/select/jaeger/api/*`) and Tempo (`/select/tempo/api/*`) APIs. `/select/tenant_ids` reports the tenants the cold tier actually holds. (`stream_field_names` returns the configured stream field names, which are the same schema for every tenant.)

How each part of the read path is scoped:

- **Cold-tier objects.** The object list for a query comes from the manifest's per-tenant aggregates for the request tenant — for the row scan and for the metadata fast paths that answer without opening Parquet (timestamp-only queries, `count()` pushdown). Before any object is opened, its key is checked once more against the request tenant; an object that does not belong is dropped and counted in `lakehouse_tenant_scope_violations_total{site}` (a non-zero value indicates a defect, not normal operation).
- **Field and stream enumeration.** `field_names`, `field_values` and `streams` read only the tenant's objects. The pmeta catalog is keyed by the tenant partition (the full key directory), so a catalog answer unions only the tenant's partitions. `field_values` never answers from the in-memory label index (its values are a sample and not time-scoped). That index is not tenant-keyed — it holds the field names of every object it was built from — so its `field_names` fallback answers only a request for the one tenant the manifest holds objects for (objects under the legacy untenanted layout count as tenant `0:0`, so a default-tenant deployment part-way through adopting the prefix template keeps the fast path). Every other request — another tenant, an unknown tenant, `0:0` in a deployment whose only tenant is someone else, or a multi-tenant list — uses the tenant-partitioned catalog or a scan.
- **Trace-by-id lookups (traces).** The `_trace_idx` footer lookup that resolves a trace's time bounds before its spans are fetched reads only the tenant's objects, so it cannot confirm that another tenant holds a trace ID.
- **Unflushed rows.** The co-located insert buffer scopes by tenant natively. In multi-pod deployments the select pod asks every insert pod's `/internal/buffer/query` for one tenant (`account_id`, `project_id`, `tenant_scope=v1`), each pod filters its buffer and echoes the tenant in `X-Lakehouse-Tenant-Scope`, and the select pod re-checks every row. A pod that does not echo the header (an older build during a rolling upgrade) has its rows dropped — the unflushed window of that pod is missing until it flushes, rather than merged unscoped. Buffered rows are merged only when newer than the Parquet the query already read for the same tenant (a per-tenant flush watermark), so a global read neither counts a flushed row twice nor hides one tenant's unflushed rows behind another tenant's newer flush.
- **Legacy layout.** Objects written before the tenant prefix template existed (a static `s3.prefix` such as `logs/`, no `{AccountID}/{ProjectID}/` segment in the key) were ingested without tenant headers, so they belong to tenant `0:0`: a `0:0` request includes them, every other tenant never sees them. Deployments that ingest more than one tenant must use the tenant prefix template.
- **Bucket-per-tenant.** A tenant with an `s3.bucket` override has its objects in that bucket; the client pool derives the bucket from the object key (`{AccountID}/{ProjectID}/…`). Because the object list is already the tenant's, a scoped request only issues S3 requests against the tenant's own bucket, and an unscoped (`0:0`) request only against the default bucket.

Cost: object selection walks the partitions inside the query window and skips the ones the tenant has no objects in (a binary search into the manifest's time-sorted partition index, the same walk the unscoped path uses). Two shapes, measured on an Apple M5 Pro with the old and new code interleaved (medians of 6 runs of `BenchmarkTenantScope_FileSelection`, `-benchtime 1s`):

| shape | previous unscoped walk | scoped selection |
|---|---|---|
| 50 tenants × 168 hourly partitions × 4 objects, one tenant's 7-day query | 9.1 ms, 40 MiB, 28 allocs | **0.27 ms, 0.78 MiB, 690 allocs** |
| 2 tenants × 8,760 hourly partitions (a year), one-hour query | 0.26 µs, 0.9 KiB, 5 allocs | **0.32 µs, 0.8 KiB, 7 allocs** |

The second shape is the dashboard shape, and it is the one that regressed while the selection iterated the tenant's whole partition set: 2.3 ms, 959 KiB and 43,807 allocations per call — per `hits`, `field_values` and `streams` request — because every partition the tenant had ever written was parsed. Those numbers are the "before" of the second row before the partition-index walk landed. Neither figure counts the objects of other tenants that a scoped query no longer opens.

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
| **Static config** | Highest | Config file / flag (re-applied every startup) | Known tenants at deploy time |
| **Runtime API** | Medium | S3 `_meta/tenant-aliases.json` | Dynamic tenant onboarding without restarts |
| **Auto-discovery** | Lowest | S3 (periodic persist + startup reload) | Dynamic environments, first-seen auto-registration |

Static config aliases cannot be overridden by runtime aliases. Runtime aliases are synced across the fleet via the existing stats delta mechanism (default 30s) **and** persisted to S3 by a periodic loop (`startTenantAliasPersist`, on the alias-sync interval) so first-seen auto-registered tenants survive restarts — see [Durability & reconstruction](#durability--reconstruction).

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
# Static aliases without a config file (orgid:account:project, comma-separated):
--lakehouse.tenant.alias=prod-team-eu_staging:42:3,dev_default:1:1
```

The `--lakehouse.tenant.alias` flag is the flag-only equivalent of the `aliases:` YAML map — handy for flag-driven deployments (Docker Compose, bare `docker run`). It is re-applied on **every** startup, so it is the deterministic reconstruction baseline even if the S3 snapshot is lost.

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

#### Durability & reconstruction

Tenant names must never silently disappear. Three layers keep the alias map durable and self-healing:

1. **Config baseline (reconstruction).** Static aliases — from the `aliases:` YAML map or the `--lakehouse.tenant.alias` flag — are re-applied on **every** startup. Even if the S3 snapshot is deleted or corrupt, the configured tenants reconstruct deterministically.
2. **Periodic S3 persistence.** A background loop (`startTenantAliasPersist`, runs on `alias_sync_interval`) writes the resolver's full alias set — config **plus** runtime-API **plus** auto-registered (`X-Scope-OrgID`) entries — to `_meta/tenant-aliases.json` whenever it changes. This is what makes first-seen auto-registered tenants survive a restart (previously they lived only in memory and their names vanished on redeploy).
3. **Startup reload + fleet sync.** On boot the resolver loads the S3 snapshot (after applying the config baseline, so config always wins), and the `SyncPusher` continuously gossips registrations across peers. A node that missed a registration re-learns it from a peer or the next reload.

Net effect: a name registered once — by config, by API, or by first ingest — is reconstructed on every subsequent start. Loss of the S3 object degrades only to the config baseline, never to bare integer IDs for configured tenants.

#### S3 Prefix Templates

The tenant is the two leading segments of every object key, and the template that produces them must name both:

| Template | S3 Key Example | Accepted |
|----------|---------------|----------|
| `{AccountID}/{ProjectID}/` (default) | `42/3/logs/dt=2026-05-15/...` | yes — VL/VT compatible |
| `tenants/{AccountID}/{ProjectID}/` | `tenants/42/3/logs/dt=2026-05-15/...` | yes — a fixed prefix in front is fine |
| *(empty)* | `logs/dt=2026-05-15/...` | yes — the single-tenant (legacy) layout; the data belongs to tenant `0:0` |
| `{OrgID}/`, `{OrgID}/{ProjectID}/` | — | no — rejected at startup |
| `{AccountID}/`, `{ProjectID}/` | — | no — rejected at startup |

Startup refuses a template that does not carry both `{AccountID}` and `{ProjectID}`, or that carries a placeholder the writer does not expand. The writer expands only those two: anything else stays in the key literally (`{OrgID}/3/logs/…`), which makes the object untenanted — and an untenanted object is tenant `0:0`'s data, so every other tenant would write into `0:0`'s layout and never read its own rows back. A single segment is just as wrong: the signal directory (`logs/`, `traces/`) is then parsed as the missing segment. String OrgIDs stay presentation-only — an alias maps to an account/project pair, and the pair is what reaches S3 (see [Tenant Name Mapping](#tenant-name-mapping-x-scope-orgid)).

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

In mixed mode the s3reader `PoolRegistry` caches a separate `ClientPool` per bucket, and the writer's `SetTenantBucket(account, project) → bucket` resolver sends each tenant's flushed Parquet to its bucket. Reads use the same routing in reverse: the client pool's bucket router derives the bucket from the object key's `{AccountID}/{ProjectID}/` segments, so a read of a tenant's object reaches that tenant's bucket (see [Read Scoping](#read-scoping-which-data-a-request-sees) for which objects a request may read). `manifest.FileInfo.Bucket` is set by the bucket migration below. Sidecars and the fleet-wide manifest stay in the default bucket — the only sharded thing is the data files themselves.

The periodic manifest refresh lists every dedicated bucket under its tenant's prefix next to the default bucket, so objects that live only in a tenant's bucket stay in the manifest. The dedicated buckets are listed concurrently (at most 4 at a time) and merged in the order they are registered, so the manifest a refresh produces does not depend on which LIST answered first; a key found in both the shared and the dedicated bucket (a migration in progress) is kept once, as the dedicated-bucket copy.

**When a dedicated bucket cannot be listed** the whole refresh fails and the previous manifest is kept — for every tenant, not just that one — because the alternative is silently dropping the unreachable tenant's objects out of the manifest. Objects written after the last successful refresh stay invisible to queries until the bucket is reachable again, so this needs an operator:

- each failure is counted in `lakehouse_manifest_tenant_bucket_list_errors_total{bucket}` (the series of every registered dedicated bucket is exported at zero, so the counter is visible before the first failure), and the `LakehouseTenantBucketListFailing` alert fires after 10 minutes of failures;
- the refresh logs `list tenant bucket <bucket>/<prefix>` with the S3 error;
- check that the bucket exists, that the pod's credentials still reach it, and that its policy grants `s3:ListBucket` on the tenant prefix. Removing the tenant's `s3.bucket` override (after migrating its objects back) also clears it, since an unregistered bucket is never listed.

Current limitation: the per-tenant `overrides[].s3.bucket` form above is what installs bucket routing. `isolation: bucket` with `bucket_template` is validated at startup but does not install per-tenant routing on its own.

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
        retention:
          keep: 2160h               # 90 days — overrides global retention
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
        compaction:                 # per-tenant compaction policy
          compression_level_by_output_level: [3, 11, 11]  # invest more CPU at L1 than the global default

      acme-corp:                    # alias-keyed entry — resolves once acme-corp is registered
        retention:
          keep: 720h                # 30 days
        compaction:
          compression_level_by_output_level: [1]          # ingest-fast tenant — uniform fast zstd, accept larger files
```

Every field is optional. Zero or missing means "inherit the global setting" — there is no special inheritance keyword. This makes config diffs honest: an empty `retention.keep` does not silently mean "use 30 days from elsewhere"; it means "follow whatever the global retention is right now."

### Consumers

Each override flows to exactly one subsystem:

| Override | Consumer | Behavior |
|---|---|---|
| `retention` (`keep`) | `retention.Manager` | synthesizes a match rule on the file's `account_id`/`project_id` manifest labels; the existing rules engine handles eviction. No special-case code path. |
| `cardinality.max_streams` / `max_fields` | `tenant.CardinalityLimiter` mounted as `TenantCardinalityGate` on the vlstorage insert paths (logs + traces) | per-tenant distinct counters with fast-path RLock for known entries; overflow drops at the boundary before the writer sees the row. |
| `ingest.max_bytes_per_sec` / `max_rows_per_sec` | `tenant.IngestRateLimiter` + `tenant.RateLimitMiddleware` | independent byte/sec + row/sec token buckets per tenant; pre-flight Content-Length check returns `429 Too Many Requests` + `X-RateLimit-Limit-Bytes` / `X-RateLimit-Remaining-Bytes` / `X-RateLimit-Retry-After-Ms` headers. |
| `lifecycle` | `delete.StorageClassDetector.SetTenantRules` consumed by both the manual `predict` handler and the background rewriter scheduler | tenant-keyed rule lookup parses `(account, project)` from the file's S3 key prefix, so manual predictions and the rewriter agree on what storage class a file should end up in. |
| `compaction.compression_level_by_output_level` | `compaction.SchedulerConfig.TenantCompressionLookup` closure → per-tenant slice resolved on every Compactor construction | when the slice is set, output files for that tenant's compactions use the per-tenant level instead of the global progressive schedule. Empty falls through to global; slot saturation matches the global rule (out-of-range output levels use the last slot). |
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
| `--lakehouse.tenant.prefix-template` | `{AccountID}/{ProjectID}/` | S3 prefix pattern. Must contain `{AccountID}` and `{ProjectID}` (both are expanded; any other placeholder is rejected at startup). Empty = the single-tenant legacy layout |
| `--lakehouse.tenant.isolation` | `prefix` | Isolation mode: `prefix` (shared bucket) or `bucket` (separate buckets) |
| `--lakehouse.tenant.bucket-template` | `""` | Bucket name pattern for `bucket` isolation mode |
| `--lakehouse.tenant.default-account` | `0` | Default AccountID when header is absent (single-tenant mode) |
| `--lakehouse.tenant.default-project` | `0` | Default ProjectID when header is absent (single-tenant mode) |
| `--lakehouse.tenant.header-account` | `X-Scope-AccountID` | HTTP header for AccountID extraction |
| `--lakehouse.tenant.header-project` | `X-Scope-ProjectID` | HTTP header for ProjectID extraction |
| `--lakehouse.tenant.orgid-header` | `X-Scope-OrgID` | HTTP header for string-based tenant identification (Loki/Tempo compatible) |
| `--lakehouse.tenant.metrics-format` | `id` | Prometheus tenant label format: `id`, `name`, or `both` |
| `--lakehouse.tenant.auto-register` | `false` | Auto-register unknown X-Scope-OrgID values as new aliases |
| `--lakehouse.tenant.alias-sync-interval` | `30s` | Fleet sync interval for runtime-added aliases |
| `--lakehouse.tenant.global-read-header` | `""` (disabled) | HTTP header name to trigger global read across all tenants |
| `--lakehouse.tenant.global-read-value` | `""` | Required value for the global read header (acts as a shared secret) |
| `--lakehouse.tenant.global-read-token` | `""` (disabled) | Bearer token for global read via `Authorization: Bearer <token>` header |

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

## Verifying parity against VL/VT

Operators worried "is our LH tenant view actually matching what VL/VT says?" can hit
`GET /lakehouse/api/v1/admin/parity?window=24h` (auth-gated by the global-read
header). The response compares the embedded VL stats path against the
manifest's `LiveAggregateWindow` for the same window and accounts for
VT-internal index rows that the writer intentionally drops.

See [docs/parity-and-gaps.md](parity-and-gaps.md) for the full expected-drift
behavior plus the running register of cold-tier feature gaps relative to
upstream VL/VT.

## Tenant-Scoped Internals

### Manifest

The manifest indexes files by hour partition and keeps a per-tenant aggregate of the partitions (and file/byte/row totals) each tenant owns, derived from the object keys. A read for tenant `100:1` walks only the partitions tenant `100:1` owns and only the files under its `100/1/` prefix:

```
files      = { "dt=2026-01-15/hour=00": [100/1/…/file1, 200/5/…/file4], "dt=2026-01-15/hour=01": [100/1/…/file3] }
aggregates = { "100/1": {partitions: [hour=00, hour=01]}, "200/5": {partitions: [hour=00]} }
legacy     = objects whose key has no tenant segment — served as tenant 0:0
```

### Write Path

`MustAddRows` extracts tenant from the request context (set by header middleware) and writes to the tenant-scoped S3 prefix. Each tenant's data is flushed independently.

### Read Path

Every read (`RunQuery` and the field/stream enumeration calls) resolves the tenant from `QueryContext.TenantIDs` — exactly one tenant, `0:0` when the request had no tenant headers — and asks the manifest for that tenant's objects only; a validated global-read request resolves to all tenants instead. The resulting object list is re-checked key by key against the tenant before anything is opened, and the same list drives the metadata fast paths, the scan, the pmeta catalog answers, and the unflushed buffer window. See [Read Scoping](#read-scoping-which-data-a-request-sees).

### Delete Path

Tombstones are tenant-scoped. A delete request is resolved to its tenant like a select (headers,
`X-Scope-OrgID` aliases, `0:0` without headers) and its tombstone names that tenant; query-time
suppression, field enumeration, the rewriter and compaction apply a tombstone only to objects
(attributed by key, as the read path does) and buffered rows of its own tenants, so a delete by
tenant A cannot hide or remove tenant B's data. The delete API lists, verifies and un-deletes only
the caller's own tombstones; the global-read credential sees all of them. The records themselves
live under the deployment's one prefix (`{prefix}_tombstones/{id}.json`), each carrying its
`Tenants`; a record naming none is rejected, never applied. Tenants resolve in both forms: integer
`AccountID`/`ProjectID` headers (upstream's) and string `X-Scope-OrgID` through the aliases (the
lakehouse extension).
See [deletion-strategy.md → Tenant Scope](deletion-strategy.md#tenant-scope).

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

When configured, a request that includes `Authorization: Bearer <token>` is a global read. This method is preferred for Grafana integration because Grafana natively supports Bearer token auth in datasource config.

**Method 3: Both (defense-in-depth)**

Configure both methods. The request must satisfy at least one to get global read access.

### How It Works

When a request authenticates for global read (via header+value or Bearer token), the read path covers **every tenant** — every tenant's cold-tier objects and every tenant's unflushed rows — and returns merged results. The credential is checked on every select request (LogsQL, Jaeger and Tempo APIs) with a constant-time comparison; tenant headers on the same request do not narrow it. A missing or wrong credential leaves the request on its own tenant (`0:0` without headers) — it is never rejected and never widened. Each widened request increments `lakehouse_global_read_queries_total`.

Without a global-read credential configured, no request can read across tenants.

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
