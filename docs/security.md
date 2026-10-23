---
title: Security
sidebar_position: 12
---

# Security

## Container Hardening

Victoria Lakehouse uses a hardened container image:

- **Base image**: `gcr.io/distroless/static-debian12:nonroot` — no shell, no package manager, no libc
- **Non-root execution**: runs as UID 65534 (`nonroot` user)
- **Stripped binary**: built with `-s -w` linker flags (no debug symbols)
- **Static binary**: `CGO_ENABLED=0` — no dynamic library dependencies
- **Multi-stage build**: builder stage discarded, only binary copied to runtime
- **FIPS 140-3 variant**: the default image (`vX.Y.Z`) is built with Go's standard crypto and is not FIPS. Only the `-fips` image (`vX.Y.Z-fips`, built with `--build-arg FIPS=1`) is compiled with the Go FIPS 140-3 module (`GOFIPS140=v1.0.0`) and runs with `fips140=on` by default; `lakehouse-logs fips-status` and `lakehouse-traces fips-status` report which one a binary is.

## Kubernetes Security Context

Default Helm chart values enforce a secure-by-default posture:

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 65534
  runAsGroup: 65534
  fsGroup: 65534
  seccompProfile:
    type: RuntimeDefault

containerSecurityContext:
  readOnlyRootFilesystem: true
  allowPrivilegeEscalation: false
  capabilities:
    drop: ["ALL"]
  runAsNonRoot: true
  runAsUser: 65534
  runAsGroup: 65534
  seccompProfile:
    type: RuntimeDefault
```

- **Read-only root filesystem**: only `/data/lakehouse` (PVC mount) is writable
- **No privilege escalation**: `allowPrivilegeEscalation: false`
- **All capabilities dropped**: `capabilities.drop: ["ALL"]`
- **Seccomp**: `RuntimeDefault` profile active

## CI Security Gates

| Gate | Tool | What It Checks |
|---|---|---|
| Dependency vulnerabilities | `govulncheck` | Known CVEs in Go dependencies |
| Static analysis | `gosec` | Go-specific security issues (OWASP) |
| Container vulnerabilities | `Trivy` | OS-level + app-level CVEs in image |
| Secret scanning | `gitleaks` | Accidentally committed credentials |
| Code analysis | `CodeQL` | Semantic vulnerability patterns (weekly) |

## Credential Handling

### S3 Credentials

**Preferred**: IAM roles via IRSA (Kubernetes) or instance profiles (EC2). No credentials in config.

**Static credentials** (`--lakehouse.s3.access-key/secret-key`) are supported for development (MinIO) but should not be used in production. These values are never logged or exposed in metrics.

### Discovery Auth Key

`--lakehouse.discovery.partition-auth-key` authenticates requests to vlstorage/vtstorage `/internal/partition/list` endpoints (polled with `POST`, the only method VictoriaTraces v0.12.0 accepts there; the key stays in the URL query). Must match the `-partitionManageAuthKey` value on storage nodes.

### Peer Key

`peer.auth_key` (or `-lakehouse.peer.auth-key`, which overrides it) is the key the pods present to each other's
internal endpoints and require on their own: the buffer bridge (`/internal/buffer/query`), the peer cache, manifest
push, stats and tenant sync. Set the same key on every pod of a deployment. See
[Internal endpoints and the peer key](#internal-endpoints-and-the-peer-key).

## Network Boundaries

```mermaid
graph TD
    subgraph "Public (Grafana, Users)"
    G[Grafana] -->|/select/logsql/*| LB[Load Balancer]
    G -->|/select/jaeger/*| LB
    end

    subgraph "Cluster Network"
    LB --> S[Lakehouse Select]
    S -->|/internal/cache/*| P[Peer Instances]
    S -->|/internal/buffer/query| I[Lakehouse Insert]
    VL[vlselect] -->|/internal/select/*| S
    end

    subgraph "AWS"
    S --> S3[(S3)]
    I --> S3
    end

    style G fill:#4CAF50,color:#fff
    style S3 fill:#FF9800,color:#fff
```

### Internal Endpoints (cluster-only)

These endpoints should NOT be exposed externally:

| Endpoint | Served by | Purpose | Protection |
|---|---|---|---|
| `/internal/select/*` | select pods | VictoriaLogs' cluster protocol (binary DataBlock), for a `vlselect`/`vtselect` in front | upstream's: network, `-httpAuth.*`, `-internalselect.disable` |
| `/internal/buffer/query` | insert pods | the select pods' buffer bridge: each tenant's not yet flushed rows | peer key (when `peer.auth_key` is set; `all_tenants=true` always needs it), `-internalselect.disable` |
| `/internal/cache/fetch`, `/internal/cache/has` | every pod | peer cache data transfer and probe | peer key (`X-Peer-Auth-Key`), when `peer.auth_key` is set |
| `/internal/cache/stats`, `/internal/cache/clear` | every pod | peer cache metrics and reset | peer key, when `peer.auth_key` is set |
| `/internal/manifest/update`, `/internal/stats/sync`, `/internal/tenant/sync` (logs binary) | every pod | manifest push, stats and tenant gossip | peer key, when `peer.auth_key` is set |
| `POST`/`DELETE /lakehouse/api/v1/tenants/aliases` | every pod | alias admin API (not cluster-internal, but guarded by the same key) | peer key, when `peer.auth_key` is set |

Use Kubernetes NetworkPolicy to restrict `/internal/*` to the cluster CIDR, and put vmauth in front of the
pods for clients (the chart's vmauth routes only `/insert/*` and `/internal/insert` to insert pods).

<a id="peer-authentication-opt-in-off-by-default"></a>

### Peer authentication

The pods call each other's internal endpoints (manifest push, stats and tenant gossip, the peer cache, the select
pods' buffer bridge). Peer authentication makes those endpoints refuse a request that does not carry the shared
peer key. **It is on whenever a key is configured (`peer.auth_key`), and a default install has no key, so nothing is
enforced there:** every endpoint below answers a request without a credential, as upstream's internal endpoints do.
Without a key, any client that can reach a pod's port can call them: keep the pods on a protected network (a
NetworkPolicy, vmauth in front of the clients), or set a key.

`peer.auth_enabled` (`-lakehouse.peer.auth-enabled`, the chart's `peerAuth.enabled`) is a three-state setting:

| `peer.auth_enabled` | With `peer.auth_key` set | Without a key |
|---|---|---|
| unset (the default) | the pod sends the key and requires it | nothing to enforce |
| `false` | the pod sends the key and does not require it (send-only; step 1 of a rollout) | nothing to enforce |
| `true` | the pod sends the key and requires it | the process does not start |

What a configured key is required on (unless `peer.auth_enabled` is `false`), in both binaries (the tenant row is the
logs binary's; the traces binary mounts no tenant sync):

| Endpoint | Kind | Why it takes the key |
|---|---|---|
| `POST /internal/manifest/update` | mutating | adds and removes files in the pod's view of the bucket |
| `POST /internal/cache/clear` | mutating | drops the pod's caches |
| `POST /internal/stats/sync` | mutating | merges another pod's tenant statistics |
| `POST /internal/tenant/sync` | mutating | merges another pod's tenant aliases |
| `POST`/`DELETE /lakehouse/api/v1/tenants/aliases` | mutating | the alias admin API, guarded by the same key |
| `GET /internal/buffer/query` | read-only | each tenant's not yet flushed rows |
| `GET /internal/cache/fetch`, `/internal/cache/has`, `/internal/cache/stats` | read-only | cached Parquet bytes and the pod's cache statistics |

The read-only endpoints follow the same key because they expose data of every tenant (cached blocks, unflushed rows)
and cluster internals. Not covered by the key, and unchanged: upstream's `/internal/select/*`, `/internal/insert` and
`/internal/delete/*` (protected as upstream does: network, `-httpAuth.*`; `-internalselect.disable` turns the
select endpoints off, and `/internal/delete/*` answers only when `-internaldelete.enable` is set, off by default); the cross-signal hints `/internal/prefetch/hint` and `/internal/cache/evict-hint` (their own
`cross_signal.auth_key`); `/internal/lifecycle/*` (the chart's preStop hook calls `/internal/lifecycle/drain` from
localhost without a key); and the admin writes outside `/internal` (`/lakehouse/drain`,
`/lakehouse/compaction/recompact`, `/lakehouse/api/v1/tenants/policy`, `/lakehouse/api/v1/admin/tenant/migrate`).

**Turning it on without a flag day.** The key and its enforcement can be rolled out separately, so a rolling restart
never refuses a pod:

1. Configure the key on every pod (`peer.auth_key`, or the chart's `peerAuth.existingSecret`) and set
   `peer.auth_enabled: false` (`-lakehouse.peer.auth-enabled=false`, the chart's `peerAuth.enabled: false`). A pod
   sends the key on every peer call whenever it has one, but still accepts requests without it, so pods that have it
   and pods that do not (yet) work together. `/internal/buffer/query` also serves `all_tenants=true` to a caller that
   presents the key, so global-read queries see the unflushed rows from this step on.
2. Once every pod has restarted with the key, remove `peer.auth_enabled` (or set it to `true`) and restart the pods
   again. The pods now refuse a request without the key (401). Pods that restarted early and pods that did not are
   still compatible, because all of them already send the key.

`peer.auth_enabled: true` without a key does not start: the process exits with
`peer.auth_enabled (-lakehouse.peer.auth-enabled) is true but peer.auth_key (-lakehouse.peer.auth-key) is empty`. With
Helm the key should come from a Secret (`peerAuth.existingSecret`); `peerAuth.enabled: true` with no Secret, or with a
key in `lakehouseConfig.peer.auth_key`, fails the render.

**Upgrading from release v0.167.0.** That release, and any later one that predates this behaviour, enforced the key
only when `peer.auth_enabled` was set to `true`: a deployment with a key and no `peer.auth_enabled` took requests
without a credential while it ran. Earlier releases, and the releases that restore it, enforce a configured key unless
`peer.auth_enabled` is `false`. If you run v0.167.0 with a key and intend to stay unenforced for now, set
`peer.auth_enabled: false` before upgrading; otherwise make sure every pod holds the same key (they send it whenever
they have one) before the upgrade rolls through.
With Helm, `helm upgrade --reuse-values` keeps the previous release's computed `peerAuth.enabled: false` (the default
of that chart) and so keeps passing the send-only flag; use `--reset-then-reuse-values`, or pass
`--set peerAuth.enabled=null`, to take the new default. `helm install`'s notes say which state the release is in.

**Seeing the state.** Off or on, the state is visible in four places:

- one startup warning when the pod's internal endpoints take requests without a credential and it has peers or a
  split insert/select role (one text for no key, one for a key with `peer.auth_enabled: false`);
- the gauge `lakehouse_peer_auth_enabled` (1 when the pod requires the key, 0 otherwise: the effective state);
- `GET /lakehouse/info`, field `peer_auth`: `{"enabled": true, "key_configured": true}` when the pod enforces its key,
  `{"enabled": false, "key_configured": true}` when it only sends it (never the key);
- the chart's `peerAuth` values and `helm install` notes.

**Always on, whatever `peer.auth_enabled` says.** Neither can refuse a legitimate peer:

- every key comparison is constant time (`internal/peerauth`);
- the request bodies of `/internal/manifest/update`, `/internal/stats/sync` and `/internal/tenant/sync` are capped at
  `peer.max_request_body_bytes` (`-lakehouse.peer.max-request-body-bytes`, default 64 MiB; larger bodies get 413), and a
  zstd stats body may not expand past the same size. Measured (see the tests): a stats push for 20 000 tenants is 9.6 MB
  (273 KB zstd), a tenant sync for 50 000 aliases is 3.8 MB, a manifest update of 2 000 fully annotated files is 3.7 MB,
  so the default leaves about 7 times the largest of them (the fleet sizes behind these bodies are assumed, not measured).

The default vmauth configuration in the chart is not changed by this setting: it forwards every path to the logs
select pods, `/internal/*` included. Put vmauth routes for the paths clients may use only, or a NetworkPolicy, in front
of the pods.

### Cross-signal hints and requests that carry a key

The cross-signal hint endpoints (`/internal/prefetch/hint`, `/internal/cache/evict-hint`) take `cross_signal.auth_key`
in the `X-Cross-Signal-Key` header when one is set. The comparison is constant time (`internal/peerauth`), the JSON
body is capped at 4 MiB (about 100 000 trace IDs; a larger body gets 413, a sender batches 100 IDs by default), and
the key is checked before the body is read. Which endpoints take a key is unchanged.

Every HTTP client that sends a peer key or the cross-signal key to a peer (the peer cache, the manifest, stats and
tenant pushes, the select pods' buffer bridge, the zone probe and the hint client) refuses redirects: a peer that
answers with a redirect gets no second request, so a key is never repeated at a location the operator did not
configure. The redirect answer counts as a failed call, as any other non-200 answer does.

### Internal endpoints and the peer key

VictoriaLogs and VictoriaTraces protect their own internal endpoints (`/internal/select/*`, `/internal/insert`) by
the network: the cluster components "must run in a protected internal network", with
[vmauth](https://docs.victoriametrics.com/victoriametrics/vmauth/) in front for clients, `-httpAuth.*` for basic
auth, and `-internalselect.disable` / `-internalinsert.disable` to turn the endpoints off. Without these the
endpoints take no credential: anyone who can reach a `vlstorage` node can read any tenant through
`/internal/select/*`. Lakehouse follows that model and adds the peer key, because its insert pods are also the
ingest endpoint: clients that may only write reach the same port as `/internal/buffer/query`.

`/internal/buffer/query` answers like this:

| Pod's setting | Request | Answer |
|---|---|---|
| key set, `peer.auth_enabled` unset or true | no `Authorization: Bearer`, or another key | **401** `Expected to receive non-empty Authorization: Bearer <key> when peer.auth_key is set` / `The provided Bearer key doesn't match peer.auth_key` (upstream's status for a missing or wrong key) |
| key set, `peer.auth_enabled` unset or true | the key, one tenant (`account_id` + `project_id`) | 200, that tenant's unflushed rows only |
| key set, `peer.auth_enabled` unset or true | the key, `all_tenants=true` | 200, every tenant's unflushed rows (a select pod's global-read query) |
| key set, `peer.auth_enabled: false` | one tenant, with or without a key | 200, that tenant's unflushed rows only, without a credential (as upstream's `/internal/select/*`) |
| key set, `peer.auth_enabled: false` | `all_tenants=true` with the key | 200, every tenant's unflushed rows (step 1 of the rollout above) |
| key set, `peer.auth_enabled: false` | `all_tenants=true` without the key, or with another | **403** `all_tenants=true is served only to an authenticated peer; set the same peer.auth_key on every pod` |
| no key (default) | one tenant | 200, that tenant's unflushed rows only, without a credential (as upstream's `/internal/select/*`) |
| no key (default) | `all_tenants=true` | **403** (same message) |
| any | `-internalselect.disable` on the insert pod | **400** `requests to /internal/buffer/query are disabled with -internalselect.disable command-line flag` (upstream's answer and status), before anything else is looked at |

The select pods' bridge sends `Authorization: Bearer <peer.auth_key>` on every request. A peer that refuses it
(401/403) is counted in `lakehouse_buffer_bridge_errors_total{reason="auth"}` and raises
`LakehouseBufferBridgeAuthRefused`: the queries that hit it were answered without that pod's unflushed rows.

**Risk without enforcement.** With peer authentication off (no key, the default, as in upstream, or `peer.auth_enabled: false`), anyone who can reach an insert pod's
port can read any single tenant's unflushed rows by naming its `AccountID`/`ProjectID`: the rows of the active and
sealed buffer segments plus the grace period after they are written (about 15 minutes at the defaults). Every
tenant at once (`all_tenants=true`) is never served without the key. This is the exposure of upstream's
`/internal/select/*` on `vlstorage`, on a port that ingest clients can reach. Keep the pods on a protected
network, or set a key; global-read queries need the key to see unflushed rows at all.

With peer authentication on, the peer key guards the insert pods' direct `/internal/buffer/query` endpoint. A select pod's `/select/*` and
`/internal/select/*` still serve whatever tenant the request names (upstream's model), including the unflushed rows
it reads through the bridge, so the key does not close every buffer read: protect those routes with
`-httpAuth.*`, vmauth or the network, as for upstream.

**Setting the key.**

- Config file: `peer: {auth_key: <key>}`, the same on every pod of both roles; it is enforced unless `auth_enabled: false` (send-only) is set, and `auth_enabled: true` makes a missing key a startup error.
- Flag: `-lakehouse.peer.auth-key=<key>` and `-lakehouse.peer.auth-enabled[=true|false]`, which override the config file (the enabled flag only when it is given, on the command line or through the environment). VictoriaMetrics' flag handling
  expands `%{ENV_VAR}` in it (`-lakehouse.peer.auth-key=%{LAKEHOUSE_PEER_AUTH_KEY}`) and hides flags named
  `*key*` from `/flags` and `/metrics`.
- Helm: create a Secret and set `peerAuth.existingSecret` (and `peerAuth.secretKey`, default `peer-auth-key`):

  ```sh
  kubectl create secret generic lakehouse-peer --from-literal=peer-auth-key="$(openssl rand -hex 32)"
  # step 1: every pod gets the key and sends it; nothing is refused yet
  helm upgrade --install lh charts/victoria-lakehouse --set peerAuth.existingSecret=lakehouse-peer --set peerAuth.enabled=false
  # step 2, after step 1 has rolled out: the pods require it (peerAuth.enabled unset enforces the key as well)
  helm upgrade --install lh charts/victoria-lakehouse --set peerAuth.existingSecret=lakehouse-peer --set peerAuth.enabled=true
  ```

  Every Lakehouse pod gets the key as `LAKEHOUSE_PEER_AUTH_KEY` and passes it with the flag; it never enters the
  ConfigMap. `lakehouseConfig.peer.auth_key` also works for step 1 but renders the key into the ConfigMap, and
  `peerAuth.enabled=true` refuses it; setting both is refused. Rotate by updating the Secret and restarting every pod; pods holding different keys refuse each
  other's bridge requests until all have restarted.

**Turning the buffer query off.** Upstream's `-internalselect.disable` turns off `/internal/buffer/query` on insert pods and
`/internal/select/*` on select pods. Select pods then see an insert pod's rows only once they are in object
storage; set `select.buffer_query_enabled: false` on them so they do not ask. A select pod that does ask counts
each "disabled" answer as `lakehouse_buffer_bridge_errors_total{reason="disabled"}` (alert
`LakehouseBufferBridgeDisabled`) and still answers 200 without those rows. A `role=all` pod with
`discovery.peer_headless_service` is its own peer: with the flag set it refuses its own bridge call, so its own
unflushed rows are missing from the queries it serves (a warning is logged at startup). Run select pods apart from
the pods that ingest, or leave the flag unset there.

The peer key is trimmed of surrounding whitespace (a Secret file usually ends in a newline); a key with whitespace or
a control character inside is refused at startup, and so is a key that is only whitespace (it would otherwise
run the pod without a key).

**`-httpAuth.*`.** VictoriaMetrics' HTTP server applies `-httpAuth.username`/`-httpAuth.password` to every path
except `/health`, `/metrics`, `/flags` and a few others, including `/internal/*`. Lakehouse's peer clients (the
buffer bridge, the peer cache, manifest, stats and tenant sync) do not send basic auth, so on a deployment that
sets `-httpAuth.*` on its pods those channels are refused (tracked in #396). Use the peer key between pods and put
basic auth or vmauth in front for clients.

### Public Endpoints

| Endpoint | Purpose |
|---|---|
| `/select/logsql/*` | Query API (VL/VT compatible) |
| `/health` | Liveness |
| `/ready` | Readiness |
| `/manifest/range` | Data range info |
| `/lakehouse/info` | Build info |
| `/metrics` | Prometheus scrape |

## Tenant Isolation

Victoria Lakehouse uses **S3 prefix isolation** for multi-tenancy — the same pattern as Grafana Loki and Grafana Tempo. Each tenant's data lives in a separate S3 prefix, providing physical data separation.

### Prefix Isolation (Default)

```
--lakehouse.tenant.prefix-template="{AccountID}/{ProjectID}/"
--lakehouse.tenant.default-account=0
--lakehouse.tenant.default-project=0
```

`AccountID` and `ProjectID` are extracted from vmauth headers (`X-Scope-AccountID`, `X-Scope-ProjectID`). A query from tenant-A cannot access tenant-B's S3 prefix. Single-tenant deployments use the default `0/0/` prefix.

S3 layout per tenant:
```
s3://obs-archive/{AccountID}/{ProjectID}/logs/dt=YYYY-MM-DD/hour=HH/*.parquet
s3://obs-archive/{AccountID}/{ProjectID}/traces/dt=YYYY-MM-DD/hour=HH/*.parquet
```

### Enterprise: Bucket-Per-Tenant Isolation

For regulated environments requiring IAM-level hard isolation (HIPAA, SOC2, FedRAMP):

```
--lakehouse.tenant.isolation=bucket
--lakehouse.tenant.bucket-template="obs-{AccountID}-{ProjectID}"
```

Each tenant gets its own S3 bucket with independent:
- IAM policies (cross-account access control)
- KMS encryption keys
- Lifecycle rules (different retention per tenant)
- S3 Access Logs for compliance audit
- CloudTrail object-level logging

### Security Properties

| Property | Prefix Isolation | Bucket Isolation |
|---|---|---|
| Data separation | Physical (S3 path) | Physical (S3 bucket) |
| IAM boundary | Shared bucket IAM | Per-bucket IAM |
| Encryption keys | Shared KMS key | Per-tenant KMS |
| Audit trail | S3 Access Logs by prefix | Per-bucket Access Logs |
| Cost attribution | S3 Inventory by prefix | Per-bucket billing |
| Parquet tool access | Per-tenant glob pattern | Per-tenant bucket |

Full documentation: [Multi-Tenancy](multi-tenancy.md)

## S3 Access

Victoria Lakehouse requires read-only S3 access:

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "s3:GetObject",
        "s3:ListBucket",
        "s3:HeadObject"
      ],
      "Resource": [
        "arn:aws:s3:::obs-archive",
        "arn:aws:s3:::obs-archive/*"
      ]
    }
  ]
}
```

No write access required. Victoria Lakehouse is a read-only consumer of Parquet files.

For SQS event notifications, add:

```json
{
  "Effect": "Allow",
  "Action": [
    "sqs:ReceiveMessage",
    "sqs:DeleteMessage",
    "sqs:GetQueueAttributes"
  ],
  "Resource": "arn:aws:sqs:us-east-1:123456:lakehouse-s3-events"
}
```
