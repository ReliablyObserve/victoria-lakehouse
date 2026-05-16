# Tenant Name Mapping Design

## Context

Victoria Lakehouse uses VL/VT's integer-based tenant identity: `TenantID { AccountID uint32, ProjectID uint32 }`. All internal operations, S3 paths, VL/VT protocol, and storage dispatch use these integers. This is non-negotiable for upstream compatibility.

However, operators and external systems need human-readable tenant names:
- **Loki/Tempo gateways** send `X-Scope-OrgID` as a string (e.g., `prod-team-eu_staging`)
- **Operators** want dashboards, metrics, and APIs to show friendly names instead of `42:7`
- **Multi-tenant UIs** need readable tenant selectors

This design adds a bidirectional tenant name mapping layer that translates between string aliases and integer IDs at the system boundary, while keeping all VL/VT internals pure integer.

## Requirements

| # | Requirement |
|---|---|
| R1 | Support `X-Scope-OrgID` header (Loki/Tempo compatible) as input for tenant identification |
| R2 | Map string aliases to `{AccountID, ProjectID}` pairs with O(1) lookup on hot path |
| R3 | Display friendly names in all external surfaces: stats API, Prometheus metrics, Explorer UI, logs |
| R4 | Support compound aliases using underscore separator: `org_project` (e.g., `prod-team-eu_staging`) |
| R5 | Support string-based S3 prefix templates (`{OrgID}/`) for standalone deployments |
| R6 | Three mapping sources: static config (startup), runtime API (CRUD), auto-discovery (optional) |
| R7 | Persist runtime aliases to S3 for fleet convergence and restart survival |
| R8 | Configurable Prometheus metrics format: integer-only (default), name-only, or both labels |
| R9 | Stats API accepts both alias strings and integer pairs for tenant lookup |
| R10 | Zero performance impact on requests that do not use `X-Scope-OrgID` |
| R11 | Zero modifications to VL/VT upstream code |

## Loki/Tempo Compatibility

From Grafana `dskit/tenant/tenant.go` (authoritative source):

**Allowed characters in X-Scope-OrgID:**
```
a-z A-Z 0-9 ! - _ . * ' ( )
```

**Constraints:**
- Max 150 bytes per tenant ID
- Cannot be `.` or `..`
- `|` reserved for multi-tenant queries (`X-Scope-OrgID: tenantA|tenantB`)
- `:` reserved for metadata (`tenantID:key=value`)
- `/` NOT allowed

**Design decision:** Use underscore (`_`) as the compound separator for `account_project` aliases. Underscore is Loki/Tempo safe, S3 safe (single path segment), and unambiguous with hyphens commonly used in org names.

**Examples:**
```
prod-team-eu_staging    valid Loki/Tempo tenant ID
prod-team-eu_prod       valid Loki/Tempo tenant ID  
dev_default             valid Loki/Tempo tenant ID
my-single-tenant        valid (flat, no compound)
```

## Architecture

### Boundary Principle

String aliases exist only at the system boundary. Everything inside the boundary uses integer IDs:

```
EXTERNAL (string aliases)              INTERNAL (integer IDs)
                                       
X-Scope-OrgID: prod-team-eu_staging    
        |                              
        v                              
   TenantResolver.Resolve()            
        |                              
        v                              
   AccountID: 42, ProjectID: 3  -----> VL/VT handlers, storage,
                                       manifest, S3 operations
                                       
   TenantResolver.DisplayName()  <---- stats, metrics, API responses
        |
        v
   "prod-team-eu_staging"              returned to user
```

### TenantResolver Component

**File:** `internal/tenant/resolver.go`

Single component handling both directions:

```go
type TenantResolver struct {
    forward  sync.Map  // string alias -> TenantID{AccountID, ProjectID}
    reverse  sync.Map  // "42:3" -> string alias
    config   ResolverConfig
}

type TenantID struct {
    AccountID uint32
    ProjectID uint32
}

// Inbound: string alias -> integer IDs
func (r *TenantResolver) Resolve(orgID string) (TenantID, bool)

// Outbound: integer IDs -> display string
// Returns "42:3" fallback if no alias configured
func (r *TenantResolver) DisplayName(accountID, projectID uint32) string

// Format tenant for Prometheus label based on config
func (r *TenantResolver) MetricLabel(accountID, projectID uint32) string

// Check if orgID matches Loki/Tempo charset
func ValidateOrgID(orgID string) error
```

**Performance characteristics:**
- `sync.Map` for lock-free concurrent reads (hot path)
- Writes are infrequent (config reload, API mutations) — `sync.Map` write overhead acceptable
- `Resolve()` on missing key: one map miss (~10ns), returns `found=false`
- Requests without `X-Scope-OrgID` header: zero resolver calls (middleware short-circuits)

### HTTP Middleware (Inbound Translation)

**File:** `internal/tenant/middleware.go`

Inserted before VL/VT handlers in the HTTP chain:

```go
func (r *TenantResolver) Middleware(next http.Handler) http.Handler
```

Logic:
1. Check for `X-Scope-OrgID` header
2. If absent -> pass through unchanged (existing integer headers work, zero overhead)
3. If present -> `resolver.Resolve(orgID)`
4. If resolved -> set `X-Scope-AccountID` and `X-Scope-ProjectID` headers, remove `X-Scope-OrgID`
5. If not resolved AND `auto_register: true` -> register new alias, then resolve
6. If not resolved AND `auto_register: false` -> return HTTP 400 `{"error": "unknown tenant", "org_id": "..."}`

**Existing integer header requests are completely untouched** — the middleware checks one header and returns immediately.

### S3 Prefix Templates (Option B)

**File:** `internal/config/config.go` (extend `TenantConfig`)

Three template modes:

```yaml
# Mode 1: VL/VT compatible (default) - two integer segments
tenant:
  prefix_template: "{AccountID}/{ProjectID}/"
  # S3 key: 42/3/logs/dt=2026-05-15/hour=14/abc.parquet

# Mode 2: Standalone Lakehouse - single string segment
tenant:
  prefix_template: "{OrgID}/"
  # S3 key: prod-team-eu_staging/logs/dt=2026-05-15/hour=14/abc.parquet

# Mode 3: Hybrid - string org with integer project
tenant:
  prefix_template: "{OrgID}/{ProjectID}/"
  # S3 key: prod-team-eu/3/logs/dt=2026-05-15/hour=14/abc.parquet
```

**Template variable detection:**
- `{AccountID}` and `{ProjectID}` -> 2 tenant segments in S3 key
- `{OrgID}` alone -> 1 tenant segment
- `{OrgID}` + `{ProjectID}` -> 2 tenant segments (string + integer)

**`ResolvedPrefix()` changes:**
- When template contains `{OrgID}`, resolve via alias (reverse lookup from current request's integer IDs)
- When template contains `{AccountID}/{ProjectID}`, resolve with integers (current behavior)

**`TenantSummaries()` changes:**
- Count template variables to determine how many path segments to consume before the signal directory
- `{AccountID}/{ProjectID}/` -> consume 2 segments, parse as integers
- `{OrgID}/` -> consume 1 segment, look up in reverse map for integer IDs

**Validation:**
- `{OrgID}` template requires at least one alias configured (otherwise no mapping exists)
- `{OrgID}` template mutually exclusive with VL/VT upstream storage node integration (standalone only)
- Alias strings used in S3 keys must pass `ValidateOrgID()` — Loki/Tempo charset enforced

### Stats API Changes

**File:** `internal/stats/api.go`

**Response changes** — add `name` field to all tenant response structs:

```go
type TenantEntry struct {
    AccountID        string  `json:"account_id"`
    ProjectID        string  `json:"project_id"`
    Name             string  `json:"name,omitempty"`  // NEW: alias if configured
    // ... existing fields unchanged
}

type TenantCostEntry struct {
    AccountID  string  `json:"account_id"`
    ProjectID  string  `json:"project_id"`
    Name       string  `json:"name,omitempty"`  // NEW
    // ... existing fields unchanged
}

type TenantCompressionEntry struct {
    AccountID        string  `json:"account_id"`
    ProjectID        string  `json:"project_id"`
    Name             string  `json:"name,omitempty"`  // NEW
    // ... existing fields unchanged
}
```

**Routing changes** — accept alias in URL:

```
GET /lakehouse/api/v1/tenants/42/7                   existing, always works
GET /lakehouse/api/v1/tenants/prod-team-eu_staging    NEW: alias lookup
```

Detection: if path segment contains non-digit characters, treat as alias and resolve. Otherwise parse as integer pair.

**Aliases CRUD API** (new endpoints):

```
GET    /lakehouse/api/v1/tenants/aliases                list all mappings
POST   /lakehouse/api/v1/tenants/aliases                create/update mapping
DELETE /lakehouse/api/v1/tenants/aliases/{orgId}         remove mapping
```

**POST body:**
```json
{
  "org_id": "prod-team-eu_staging",
  "account_id": 42,
  "project_id": 3
}
```

### Prometheus Metrics

**File:** `internal/metrics/lakehouse.go`

Controlled by `--lakehouse.tenant.metrics-format` flag:

| Value | Tenant label | Example |
|---|---|---|
| `id` (default) | `tenant="42:3"` | Zero change from current behavior |
| `name` | `tenant="prod-team-eu_staging"` | Falls back to `"42:3"` if no alias |
| `both` | `tenant="42:3"` + `tenant_name="prod-team-eu_staging"` | Extra label, operator accepts cardinality |

Implementation: thin wrapper around `GaugeVec.Set()` / `CounterVec.Inc()` that calls `resolver.MetricLabel()` to format the label value before passing to the underlying metric.

**Cardinality note:** `name` mode does not increase cardinality (same number of unique values, just different strings). `both` mode doubles the tenant label dimension — gated behind explicit opt-in.

### Explorer UI

**File:** `website/` (Docusaurus frontend)

When aliases are configured (detected from API response `name` field):
- Tenant selector dropdown shows friendly names
- Tenant detail pages show name prominently, integers as secondary
- All tables with tenant columns show `name` when available, `account_id:project_id` otherwise
- No frontend config needed — purely driven by API response data

### Structured Logging

When resolver has aliases, log lines include the friendly name:

```
level=info msg="query completed" tenant=prod-team-eu_staging tenant_id=42:3 duration=45ms
```

The `tenant` field uses the display name, `tenant_id` always shows integers for grep/correlation.

### Persistence and Fleet Sync

**Static config** (loaded at startup, highest priority):

```yaml
lakehouse:
  tenant:
    aliases:
      prod-team-eu_staging:
        account_id: 42
        project_id: 3
      prod-team-eu_prod:
        account_id: 42
        project_id: 7
      dev_default:
        account_id: 1
        project_id: 1
    auto_register: false
    metrics_format: "id"
```

**S3 persistence** (for runtime-added aliases):
- Path: `s3://{bucket}/_meta/tenant-aliases.json`
- Written on every alias change via CRUD API
- Loaded on startup after static config
- Static config aliases cannot be overridden by runtime aliases

**Fleet sync** via existing stats delta mechanism:
- Add `Aliases map[string]AliasDef` to `TenantDelta` struct
- All nodes converge within one sync interval (default 30s)
- Conflict resolution: static config wins > earliest creation timestamp

**Auto-discovery** (optional, `auto_register: true`):
- First `X-Scope-OrgID` seen for unknown org creates a mapping
- AccountID and ProjectID auto-assigned from next available integers
- Persisted to S3 immediately
- Broadcast to fleet via delta sync

## Configuration

### New flags

| Flag | Default | Description |
|---|---|---|
| `--lakehouse.tenant.metrics-format` | `id` | Prometheus label format: `id`, `name`, `both` |
| `--lakehouse.tenant.auto-register` | `false` | Auto-register unknown X-Scope-OrgID values |
| `--lakehouse.tenant.alias-sync-interval` | `30s` | Fleet sync interval for runtime aliases |
| `--lakehouse.tenant.orgid-header` | `X-Scope-OrgID` | Header name for string tenant ID |

### Helm values

```yaml
lakehouse:
  tenant:
    # Existing fields (unchanged)
    prefix_template: "{AccountID}/{ProjectID}/"
    isolation: "prefix"
    header_account: "X-Scope-AccountID"
    header_project: "X-Scope-ProjectID"
    default_account: "0"
    default_project: "0"

    # New fields
    orgid_header: "X-Scope-OrgID"
    metrics_format: "id"          # id | name | both
    auto_register: false
    alias_sync_interval: "30s"
    aliases:                       # static alias mappings
      prod-team-eu_staging:
        account_id: 42
        project_id: 3
      prod-team-eu_prod:
        account_id: 42
        project_id: 7
```

## Validation Rules

1. Alias strings must pass Loki/Tempo charset: `[a-zA-Z0-9!-_.*'()]`, max 150 bytes
2. Alias cannot be `.` or `..`
3. Alias cannot contain `|` (multi-tenant separator) or `:` (metadata separator)
4. Each alias must map to exactly one `{AccountID, ProjectID}` pair
5. Each `{AccountID, ProjectID}` pair can have at most one alias
6. `{OrgID}` prefix template requires at least one alias or `auto_register: true`
7. `{OrgID}` prefix template is not compatible with VL/VT upstream storage node mode
8. Underscore in alias is purely conventional (for readability) — not enforced as a separator

## File Structure

| File | Responsibility |
|---|---|
| `internal/tenant/resolver.go` | TenantResolver: forward/reverse maps, Resolve(), DisplayName(), MetricLabel() |
| `internal/tenant/resolver_test.go` | Resolver unit tests |
| `internal/tenant/middleware.go` | HTTP middleware: X-Scope-OrgID translation |
| `internal/tenant/middleware_test.go` | Middleware tests |
| `internal/tenant/handler.go` | Aliases CRUD API handlers |
| `internal/tenant/handler_test.go` | Handler tests |
| `internal/tenant/persistence.go` | S3 read/write for tenant-aliases.json |
| `internal/tenant/persistence_test.go` | Persistence tests |
| `internal/tenant/validation.go` | ValidateOrgID(), Loki/Tempo charset check |
| `internal/tenant/validation_test.go` | Validation tests |
| `internal/config/config.go` | Extend TenantConfig with alias fields |
| `internal/stats/api.go` | Add `name` field to response structs, alias URL routing |
| `internal/metrics/lakehouse.go` | MetricLabel() wrapper for tenant label formatting |
| `internal/manifest/manifest.go` | Template-aware TenantSummaries() parsing |
| `cmd/lakehouse-logs/main.go` | Wire resolver, register middleware and alias handlers |

## Non-Goals

- Multi-tenant query federation (`X-Scope-OrgID: A|B`) — future work
- Tenant metadata passthrough (`tenantID:key=value`) — future work
- Per-tenant rate limiting based on alias — out of scope
- Alias-based RBAC / authorization — out of scope
- Renaming aliases (delete + recreate) — no rename API needed

## Migration Path

1. **Zero-config upgrade**: existing deployments see no change (no aliases configured, integer-only)
2. **Add aliases**: operator adds `tenant.aliases` to config, restarts — friendly names appear in API/UI
3. **Enable metrics names**: operator sets `metrics_format: name`, updates dashboards
4. **Switch S3 template**: operator migrates to `{OrgID}/` template for new data (old integer-prefixed data still readable)
5. **Enable auto-register**: operator enables for dynamic environments
6. **CRUD API**: operator manages aliases at runtime without restarts
