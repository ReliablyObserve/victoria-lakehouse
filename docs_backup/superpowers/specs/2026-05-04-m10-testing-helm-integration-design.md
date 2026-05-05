# M10: Testing, Helm & Integration — Design Spec

## Overview

M10 enhances Victoria Lakehouse with four areas: (1) E2E testing overhaul with VictoriaLogs, multi-level vlselect, and loki-vl-proxy, (2) empirical performance benchmarks with Parquet file optimization, (3) Victoria-pattern Helm chart rewrite, and (4) upstream release tracking GHA.

## 1. E2E Testing Overhaul

### Docker Compose Redesign

The existing `docker-compose-e2e.yml` exposes host ports for all services. M10 rewrites it to use an internal Docker network with **only Grafana exposed on port 3003** (avoiding conflicts with loki-vl-proxy's e2e-compat stack on port 3002).

**New services added:**
- `victorialogs` — VL single-node on internal port 9428 (hot tier for last 1h)
- `vlselect` — multi-level select with `-storageNode` fan-out to both VL (hot) and lakehouse-logs (cold)
- `loki-vl-proxy` — Loki API compatibility layer routing cold queries to lakehouse

**Networking:**
- All services on internal `lakehouse-net` Docker network
- No host port mappings except `grafana:3003→3000`
- E2E tests run via `docker compose run` inside the network, OR use env-var-based URLs

**Container details:**

| Service | Image | Internal Port | Depends On |
|---|---|---|---|
| minio | minio/minio:latest | 9000, 9001 | — |
| minio-init | minio/mc:latest | — | minio (healthy) |
| datagen-seed | build from repo | — | minio-init (completed) |
| datagen-continuous | build from repo | — | datagen-seed (completed) |
| victorialogs | victoriametrics/victoria-logs:latest | 9428 | — |
| lakehouse-logs | build from repo | 9428 | datagen-seed (completed) |
| lakehouse-traces | build from repo | 10428 | datagen-seed (completed) |
| vlselect | victoriametrics/victoria-logs:latest | 9471 | victorialogs, lakehouse-logs |
| loki-vl-proxy | ghcr.io/reliablyobserve/loki-vl-proxy:latest | 3100 | lakehouse-logs |
| grafana | grafana/grafana:latest | 3000→host:3003 | lakehouse-logs, lakehouse-traces |

### Datagen Improvements

**Realistic log patterns** — 5 generators:
1. `jsonAccessLog` — structured JSON with method, path, status, duration, request_id
2. `logfmtLog` — logfmt key=value pairs (level, msg, component, duration)
3. `nginxCombinedLog` — standard nginx combined access log format
4. `javaStackTrace` — multi-line Java exception with stack frames
5. `otelLog` — OTEL-formatted log with resource/scope attributes

**Dual-write** — New `--dual-write` flag and `--vl-endpoint` flag:
- Writes Parquet to S3 (cold path, existing)
- Pushes NDJSON to VictoriaLogs via `/insert/jsonline` (hot path, new)
- VL gets the same data as S3, enabling hot/cold query verification

**Continuous timestamps** — Datagen tracks last-generated timestamp per partition and generates forward from there (no gaps, no overlap).

### E2E Test Additions

**`loki_proxy_test.go`** — Tests through loki-vl-proxy:
- `/loki/api/v1/query_range` with LogQL
- `/loki/api/v1/labels` and `/loki/api/v1/label/{name}/values`
- Verify cold data from lakehouse is returned through proxy

**`vlselect_multilevel_test.go`** — Tests through multi-level vlselect:
- Query via vlselect port 9471
- Verify data from both hot VL and cold lakehouse appears
- Time range queries spanning hot/cold boundary

**`perf_test.go`** — Performance assertion tests:
- Manifest fast path < 1ms
- Bloom filter point query < 100ms
- Time range scan < 500ms

**Helper updates** — env-var-based URLs:
```go
const (
    logsBaseURL     = envOrDefault("LOGS_BASE_URL", "http://localhost:19428")
    tracesBaseURL   = envOrDefault("TRACES_BASE_URL", "http://localhost:20428")
    lokiProxyURL    = envOrDefault("LOKI_PROXY_URL", "http://localhost:3100")
    vlselectURL     = envOrDefault("VLSELECT_URL", "http://localhost:9471")
)
```

### Grafana Datasource Updates

Add datasources for:
- VictoriaLogs hot tier (direct)
- vlselect multi-level (unified hot+cold)
- Loki via loki-vl-proxy

## 2. Empirical Performance Benchmarks

### File Size Optimization Matrix

Test Parquet file sizes across: 1MB, 5MB, 10MB, 50MB, 100MB, 500MB.
For each size, vary row group sizes: 1K, 5K, 10K, 50K, 100K rows.

Measure:
- Write throughput (MB/s)
- Read latency for point query (bloom filter)
- Read latency for range scan (1h)
- S3 GET request count per query

### Compression Ratio Benchmarks

Test ZSTD levels 1-19 with realistic log data. Per-column breakdown showing which columns benefit most from compression. Measure compression ratio, write time, read time.

### MinIO vs S3 Baseline

Establish MinIO baseline latencies. Document extrapolation formula for real S3:
- MinIO local: ~1-5ms per GET
- S3 Standard: ~50-150ms per GET (first byte)
- Extrapolation: `estimated_s3_latency = minio_latency + s3_first_byte_overhead`

### Cost Projections

Based on measured data:
- S3 storage cost per GB at each compression level
- S3 request cost per query type
- Optimal file size recommendation

## 3. Victoria-Pattern Helm Chart

### Architecture

The chart follows Victoria's pattern (`victoria-logs-cluster`):

| Component | K8s Resource | Why |
|---|---|---|
| select | StatefulSet | Needs EBS disk cache persistence |
| insert | StatefulSet | Needs EBS for WAL + disk cache |
| vmauth | Deployment | Stateless routing proxy |

### Config as Single YAML Blob

The lakehouse binary supports `--lakehouse.config=<path>`. The Helm chart:
1. Renders all lakehouse config values into a single YAML blob
2. Stores it in a ConfigMap
3. Mounts it at `/etc/lakehouse/config.yaml`
4. Passes `--lakehouse.config=/etc/lakehouse/config.yaml` as the only arg

This avoids mapping individual flags in the chart templates.

### values.yaml Structure

```yaml
common:                    # Deep-merged into each component
  nodeSelector: {}
  tolerations: []
  affinity: {}
  resources: {}
  podSecurityContext: {}
  securityContext: {}

lakehouseConfig:           # Single YAML blob → ConfigMap
  mode: logs
  role: all
  s3:
    bucket: ""
    region: us-east-1
  cache:
    memoryLimit: 512MB
  # ... all lakehouse config here

select:
  enabled: true
  replicaCount: 2
  persistence:
    enabled: true
    size: 50Gi
  service:
    type: ClusterIP
  headlessService:
    enabled: true
  horizontalPodAutoscaler:
    enabled: false
  verticalPodAutoscaler:
    enabled: false
  podDisruptionBudget:
    enabled: false
  ingress:
    enabled: false
  serviceMonitor:
    enabled: false
  # component-specific overrides deep-merge over common

insertComponent:
  enabled: true
  # same structure as select

vmauth:
  enabled: false
  image:
    repository: victoriametrics/vmauth
    tag: v1.106.1
  config: ""  # auto-generated if empty
```

### Template Structure

| Template | Description |
|---|---|
| `_helpers.tpl` | Labels, names, image, common merge logic |
| `configmap.yaml` | Single lakehouse config YAML blob |
| `select-statefulset.yaml` | Select StatefulSet |
| `select-service.yaml` | Select ClusterIP service |
| `select-headless-service.yaml` | Select headless service (toggleable) |
| `insert-statefulset.yaml` | Insert StatefulSet |
| `insert-service.yaml` | Insert ClusterIP service |
| `insert-headless-service.yaml` | Insert headless service (toggleable) |
| `vmauth-deployment.yaml` | vmauth Deployment |
| `vmauth-service.yaml` | vmauth Service |
| `vmauth-secret.yaml` | vmauth routing config (Secret, not ConfigMap) |
| `serviceaccount.yaml` | Per-component service accounts |
| `hpa.yaml` | Generic HPA iterating over components |
| `vpa.yaml` | Generic VPA iterating over components |
| `pdb.yaml` | Generic PDB iterating over components |
| `servicemonitor.yaml` | Generic ServiceMonitor iterating over components |
| `ingress.yaml` | Generic Ingress iterating over components |
| `extra-manifests.yaml` | User-supplied arbitrary K8s manifests |

### HPA/VPA/PDB — Generic Templates

Following Victoria's pattern, these iterate over all components:

```yaml
{{- range $name, $spec := dict "select" .Values.select "insert" .Values.insertComponent }}
{{- if and $spec.enabled (dig "horizontalPodAutoscaler" "enabled" false $spec) }}
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ include "victoria-lakehouse.fullname" $ }}-{{ $name }}
spec:
  scaleTargetRef:
    kind: StatefulSet
    name: {{ include "victoria-lakehouse.fullname" $ }}-{{ $name }}
  minReplicas: {{ $spec.horizontalPodAutoscaler.minReplicas }}
  maxReplicas: {{ $spec.horizontalPodAutoscaler.maxReplicas }}
{{- end }}
{{- end }}
```

### Headless Services

Separate toggleable resources (not part of main service):
- `select-headless-service.yaml` — for peer cache discovery
- `insert-headless-service.yaml` — for buffer query discovery by select pods

### extraManifests

User can add arbitrary K8s resources:

```yaml
extraManifests:
  - apiVersion: v1
    kind: ConfigMap
    metadata:
      name: custom-config
    data:
      key: value
```

### vmauth Routing

Auto-generated when `vmauth.config` is empty:
- `/insert/*` → insert service
- Everything else → select service
- Uses `discover_backend_ips: true` for direct pod routing
- Stored as Secret (not ConfigMap) following Victoria pattern

## 4. Upstream Sync GHA

### Problem

Current `upstream-check.yaml` checks Go module versions — but victoria-lakehouse has ZERO VL/VT Go imports (clean-room). The workflow is inert.

### Solution

Rewrite to track VL/VT **GitHub releases** (not Go deps):

1. Store known versions in `.upstream-versions.json`
2. Daily cron fetches latest releases via GitHub API
3. On new release: update `.upstream-versions.json`, update Docker Compose image tags, create PR with changelog link

```json
{
  "victorialogs": "v1.20.0-victorialogs",
  "victoriatraces": "v1.5.0-victoriatraces"
}
```

### Docker Compose Image Updates

When a new VL/VT release is detected, the workflow updates:
- `docker-compose-e2e.yml` image tags for `victorialogs` and `vlselect` services
- Any other compose files referencing VL/VT images

### PR Content

```
## Upstream Release Update

| Component | Previous | Latest |
|---|---|---|
| VictoriaLogs | v1.19.0 | v1.20.0 |

### Changelog
- [VL v1.20.0 release notes](https://github.com/VictoriaMetrics/VictoriaLogs/releases/tag/v1.20.0)

### Review Checklist
- [ ] E2E tests pass with new images
- [ ] No breaking API changes
- [ ] Performance regression check
```

## Non-Goals

- Write path implementation (M8 scope)
- VL/VT Go imports (clean-room only)
- Production S3 benchmarks (MinIO extrapolation only)
- Compaction changes (M9 complete)
