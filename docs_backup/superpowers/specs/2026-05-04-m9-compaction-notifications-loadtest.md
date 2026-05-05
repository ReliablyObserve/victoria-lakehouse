# M9: Compaction, Peer Manifest Notifications, Load Testing

## Goal

Add level-based Parquet compaction with leader election, internal peer manifest push notifications, and a load testing suite that validates performance targets from the project plan.

## Architecture

In-process compaction runs as a background goroutine in the lakehouse binary, gated by leader election (K8s Lease primary, S3 lock with liveness detection fallback). After each flush or compaction, the instance pushes manifest updates to all peers via HTTP. The existing S3 ListObjects poll serves as a fallback. A dedicated load test binary validates latency and throughput targets against a live lakehouse stack.

## Tech Stack

- Go 1.26, parquet-go v0.29.0, aws-sdk-go-v2 (existing)
- k8s.io/client-go for K8s leader election (new dependency)
- Existing headless DNS discovery for peer notification
- Existing docker-compose E2E stack (MinIO + datagen) for load tests

---

## 1. Compaction Engine

### 1.1 Level-Based Compaction

Three compaction levels tracked via the existing `FileInfo.CompactionLevel` field:

| Level | Source | Trigger | Output |
|-------|--------|---------|--------|
| L0 | Raw flush files (from writer) | Partition has >= `min_files_l0` (default 10) L0 files | 1-N L1 files at TargetFileSize |
| L1 | L1 files from previous compaction | Partition has >= `min_files_l1` (default 10) L1 files | 1-N L2 files at TargetFileSize |
| L2 | Final tier | No further compaction | -- |

CompactionLevel is set on FileInfo at write time:
- Writer flush: `CompactionLevel = 0`
- L0 -> L1 compaction: `CompactionLevel = 1`
- L1 -> L2 compaction: `CompactionLevel = 2`

### 1.2 Scheduler

Background goroutine gated by `--lakehouse.compaction.enabled` (default false). Runs on a timer controlled by `--lakehouse.compaction.interval` (default 5m).

Each tick:
1. Check leader status. If not leader, skip.
2. Scan manifest for eligible partitions, oldest first.
3. For each eligible partition (up to `--lakehouse.compaction.max-concurrent`, default 1):
   a. Check partition-level sentinel. If locked, skip (increment `skipped{reason="locked"}` counter).
   b. Write S3 sentinel key `{prefix}/dt=.../hour=.../_compacting`.
   c. Read source Parquet files from S3 via cache layer.
   d. Deserialize all rows, merge into single sorted slice (sort by timestamp).
   e. Write new Parquet file(s) using existing `writeLogsParquet`/`writeTracesParquet` with:
      - `CompactionLevel = source_level + 1`
      - Bloom filters on `service.name` and `trace_id`
      - ZSTD compression at configured level
      - Row group size from config
   f. Upload to S3: `{prefix}/dt=YYYY-MM-DD/hour=HH/compacted-L{level}-{uuid}.parquet`
   g. Update manifest: `AddFile()` for new files, `RemoveFile()` for source files.
   h. Delete source files from S3.
   i. Remove sentinel key.
   j. Push manifest update to peers.
4. Log completion with partition, input files, output files, rows merged, duration.

### 1.3 Partition Eligibility

A partition is eligible for compaction when:
- It has >= `min_files_l0` files at level 0 (for L0->L1 compaction)
- OR it has >= `min_files_l1` files at level 1 (for L1->L2 compaction)
- AND no `_compacting` sentinel exists for that partition
- AND the partition is older than `--lakehouse.compaction.min-age` (default 1h) to avoid compacting actively-written partitions

L0->L1 compaction is prioritized over L1->L2 (reduce file count first).

### 1.4 Safety

- Source files are deleted ONLY after new files are uploaded and manifest is updated.
- On crash mid-compaction: orphaned output files are harmless. Next manifest refresh via S3 ListObjects discovers them. Sentinel has a staleness timeout (10m) so the partition is not permanently locked.
- Compaction output uses a distinct key prefix (`compacted-L{level}-`) to distinguish from flush output.
- SchemaFingerprint is checked: all source files in a merge must have the same fingerprint. Mismatched files are skipped with a warning log.

### 1.5 Compaction Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `lakehouse_compaction_runs_total` | Counter | Compaction cycles started |
| `lakehouse_compaction_files_input_total` | Counter | Source files read |
| `lakehouse_compaction_files_output_total` | Counter | Output files written |
| `lakehouse_compaction_bytes_read_total` | Counter | Bytes read from source files |
| `lakehouse_compaction_bytes_written_total` | Counter | Bytes written to S3 |
| `lakehouse_compaction_rows_merged_total` | Counter | Total rows processed |
| `lakehouse_compaction_duration_seconds` | Histogram | Per-partition compaction time |
| `lakehouse_compaction_errors_total` | Counter | Failed compaction attempts |
| `lakehouse_compaction_level_files` | GaugeVec (level) | Current file count at each level |
| `lakehouse_compaction_skipped_total` | CounterVec (reason) | Skipped partitions: "locked", "not_leader", "below_threshold", "too_recent", "schema_mismatch" |

### 1.6 Compaction Logging

Structured JSON log lines at each step:

- `compaction.scan`: partitions scanned, eligible count, skipped count by reason
- `compaction.start`: partition, level, input file count, total input bytes
- `compaction.read`: partition, file key, rows read, bytes read
- `compaction.write`: partition, output key, rows written, bytes written, compression ratio
- `compaction.complete`: partition, level, input files, output files, rows merged, duration, bytes saved
- `compaction.error`: partition, level, error message, input files (for debugging)
- `compaction.skip`: partition, reason (locked/not_leader/below_threshold/too_recent/schema_mismatch)

All log lines include `"component": "compaction"` for filtering.

### 1.7 Configuration

| Flag | Default | Description |
|------|---------|-------------|
| `--lakehouse.compaction.enabled` | `false` | Enable compaction scheduler |
| `--lakehouse.compaction.interval` | `5m` | Scan interval |
| `--lakehouse.compaction.max-concurrent` | `1` | Max partitions compacted in parallel |
| `--lakehouse.compaction.min-files-l0` | `10` | L0 files to trigger L0->L1 |
| `--lakehouse.compaction.min-files-l1` | `10` | L1 files to trigger L1->L2 |
| `--lakehouse.compaction.min-age` | `1h` | Minimum partition age before eligible |
| `--lakehouse.compaction.leader-election` | `auto` | Election mode: auto, k8s, s3, none |
| `--lakehouse.compaction.lease-duration` | `15s` | K8s lease duration |
| `--lakehouse.compaction.s3-lock-ttl` | `60s` | S3 lock expiry (backup for health check) |
| `--lakehouse.compaction.s3-heartbeat` | `15s` | S3 lock heartbeat interval |

YAML config equivalent under `lakehouse.compaction.*`.

### 1.8 New Package: `internal/compaction/`

Files:
- `scheduler.go` — `Scheduler` struct: timer loop, partition scanning, eligibility rules
- `compactor.go` — `Compactor` struct: read files, merge rows, write output, update manifest
- `policy.go` — `LevelPolicy`: min-files thresholds, age checks, level promotion rules
- `sentinel.go` — Partition-level S3 sentinel write/check/cleanup

### 1.9 New Package: `internal/election/`

Files:
- `leader.go` — `Leader` interface: `IsLeader() bool`, `Start(ctx)`, `Stop()`
- `k8s.go` — `K8sElector`: wraps `client-go/tools/leaderelection` with Lease resource
- `s3.go` — `S3Elector`: S3 lock file with heartbeat + HTTP liveness checking
- `noop.go` — `NoopElector`: always returns `IsLeader() = true`
- `auto.go` — `AutoElector`: tries K8s, falls back to S3

---

## 2. Leader Election

### 2.1 K8s Leader Election (Primary)

Uses `k8s.io/client-go/tools/leaderelection` with a Lease object.

- Lease name: `lakehouse-compaction-{mode}` (e.g., `lakehouse-compaction-logs`)
- Namespace: auto-detected from downward API or `POD_NAMESPACE` env var
- Lease duration: 15s (configurable)
- Renew deadline: 10s
- Retry period: 2s
- Identity: pod name from `POD_NAME` env var or hostname

Callbacks:
- `OnStartedLeading`: set `isLeader = true`, log "compaction leader elected"
- `OnStoppedLeading`: set `isLeader = false`, log "compaction leadership lost"
- `OnNewLeader`: log "new compaction leader: {identity}"

Helm RBAC (created when `compaction.enabled=true`):
```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: {{ .Release.Name }}-compaction-leader
rules:
  - apiGroups: ["coordination.k8s.io"]
    resources: ["leases"]
    verbs: ["get", "create", "update"]
```

### 2.2 S3 Lock with Liveness Detection (Fallback)

Lock file: `s3://{prefix}/_compaction_lock.json`

```json
{
  "holder": "lakehouse-insert-2",
  "address": "10.0.3.42:9428",
  "acquired_at": "2026-05-04T18:00:00Z",
  "heartbeat": "2026-05-04T18:01:30Z"
}
```

**Acquisition flow:**
1. Read existing lock from S3.
2. If no lock exists: write lock with self as holder. Re-read to confirm. If confirmed, become leader.
3. If lock exists and holder is self: renew heartbeat.
4. If lock exists and holder is another instance:
   a. HTTP GET `http://{address}/health` with 3s timeout.
   b. If holder responds 200: back off. Retry after 30-60s random jitter.
   c. If holder unreachable: wait random jitter 0-10s, then attempt takeover (write lock with self as holder). Re-read to confirm ownership.
5. If lock exists and heartbeat is older than `s3-lock-ttl` (60s default): treat as dead, attempt takeover regardless of health check result.

**Heartbeat loop:** Every `s3-heartbeat` (15s), re-write lock file with updated heartbeat timestamp.

**Metrics:**
| Metric | Type | Description |
|--------|------|-------------|
| `lakehouse_election_leader` | Gauge | 1 if this instance is leader, 0 otherwise |
| `lakehouse_election_transitions_total` | Counter | Leadership transitions |
| `lakehouse_election_health_checks_total` | CounterVec (result) | Health check outcomes: "alive", "dead", "timeout" |

### 2.3 Election Mode Selection

`--lakehouse.compaction.leader-election` values:

| Mode | Behavior |
|------|----------|
| `auto` | Check for `KUBERNETES_SERVICE_HOST` env var. If set, try K8s Lease. If K8s Lease creation fails (RBAC, not in cluster), fall back to S3 lock. |
| `k8s` | Require K8s Lease. Fail startup if not in K8s cluster or RBAC missing. |
| `s3` | Always use S3 lock with liveness. Works anywhere with S3 access. |
| `none` | No election. All instances run compaction. Partition sentinels prevent double-compaction. Suitable for single-instance deployments. |

---

## 3. Internal Peer Manifest Push

### 3.1 Push Protocol

On every flush or compaction complete, the instance pushes manifest changes to all discovered peers:

```
POST /internal/manifest/update
Content-Type: application/json
Authorization: Bearer {shared-secret}

{
  "added": [
    {"key": "logs/dt=2026-05-04/hour=10/abc.parquet", "size": 134217728, "row_count": 50000, ...},
    ...
  ],
  "removed": ["logs/dt=2026-05-04/hour=10/old1.parquet", "logs/dt=2026-05-04/hour=10/old2.parquet"],
  "source": "lakehouse-insert-0"
}
```

### 3.2 Sender

`internal/manifest/push.go` — `Pusher` struct:

```go
type Pusher struct {
    discovery  *discovery.Discovery
    authSecret string
    client     *http.Client  // 2s timeout
    logger     *slog.Logger
}

func (p *Pusher) Notify(ctx context.Context, added []FileInfo, removed []string)
```

- Discovers peers via headless DNS (same mechanism peer cache uses)
- Parallel HTTP POST to all peers, fire-and-forget
- 2s timeout per peer. Failures logged but not retried (pull catches up).
- Skips self (compares with own address).

### 3.3 Receiver

New handler registered in `newMux()`:

```go
mux.HandleFunc("/internal/manifest/update", manifestUpdateHandler)
```

- Validates shared-secret auth header
- Parses added/removed from JSON body
- Calls `manifest.AddFile()` for each added file
- Calls `manifest.RemoveFile()` for each removed file
- Returns 200 OK (or 401 if auth fails)

### 3.4 Integration Points

The Pusher is called from:
- `BatchWriter.FlushAll()` — after successful S3 upload, push added files
- `Compactor.compact()` — after compaction complete, push added + removed files

### 3.5 Fallback

The existing `manifest.RefreshInterval` (default 5m) S3 ListObjects poll catches any missed pushes. No changes needed to the existing refresh mechanism.

### 3.6 Metrics

| Metric | Type | Description |
|--------|------|-------------|
| `lakehouse_manifest_push_total` | Counter | Push notifications sent |
| `lakehouse_manifest_push_errors_total` | Counter | Failed push attempts |
| `lakehouse_manifest_push_peers` | Gauge | Number of peers notified |
| `lakehouse_manifest_update_received_total` | Counter | Manifest updates received from peers |

---

## 4. Load Testing

### 4.1 Test Binary

`cmd/loadtest/main.go` — standalone binary that connects to a running lakehouse instance.

```
loadtest --target=http://localhost:9428 --mode=latency --duration=60s
loadtest --target=http://localhost:9428 --mode=throughput --duration=120s
loadtest --target=http://localhost:9428 --mode=mixed --duration=120s
loadtest --target=http://localhost:9428 --mode=all --duration=300s
```

### 4.2 Latency Benchmarks (validate plan targets)

| Test | Target p95 | Method |
|------|-----------|--------|
| Manifest fast path | <1ms | Query time range within hot boundary (returns empty) |
| Bloom point query | <100ms | `/select/logsql/query` with `trace_id:="<known-id>"` |
| Time-range scan (1h) | <500ms | `/select/logsql/query` with 1h `_time` range |
| Stats aggregation | <300ms | `/select/logsql/stats_query` over 1h range |
| field_names | <1ms | `/select/logsql/field_names` |
| field_values | <1ms | `/select/logsql/field_values?field=service.name` |

Each test runs N iterations (default 100), records latencies, computes p50/p95/p99.

### 4.3 Throughput Stress Tests

| Test | Method |
|------|--------|
| Max insert rate | Ramp concurrent `/insert/jsonline` writers (1, 2, 4, 8, 16, 32). Report max sustained rows/s before p99 > 1s or error rate > 1%. |
| Max concurrent queries | Ramp parallel `/select/logsql/query` (1, 2, 4, 8, 16, 32). Report max QPS before p99 > 2s. |
| Mixed workload | 70% insert / 30% query at increasing concurrency. Find saturation point. |
| Compaction under load | Enable compaction, run insert + query, measure query latency impact vs baseline. |

### 4.4 Output

JSON report:

```json
{
  "mode": "all",
  "duration": "300s",
  "target": "http://localhost:9428",
  "latency_benchmarks": {
    "manifest_fast_path": {"p50_ms": 0.2, "p95_ms": 0.8, "p99_ms": 1.1, "target_p95_ms": 1.0, "pass": true},
    "bloom_point_query": {"p50_ms": 45, "p95_ms": 88, "p99_ms": 120, "target_p95_ms": 100, "pass": true},
    ...
  },
  "throughput_tests": {
    "max_insert_rate": {"rows_per_sec": 125000, "concurrency_at_max": 16},
    "max_query_qps": {"qps": 42, "concurrency_at_max": 8},
    ...
  },
  "pass": true
}
```

Human-readable summary printed to stdout. JSON written to `--output` file.

### 4.5 CI Integration

Optional long-running CI job (not in default PR checks):
```yaml
- name: loadtest
  if: github.event_name == 'schedule'
  run: |
    docker compose -f deployment/docker/docker-compose-e2e.yml up -d
    go run ./cmd/loadtest --target=http://localhost:19428 --mode=all --duration=120s --output=loadtest-results.json
```

Runs on schedule (nightly) or manual trigger. Fails if any latency benchmark exceeds target.

### 4.6 New Package: `cmd/loadtest/`

Files:
- `main.go` — CLI parsing, mode dispatch, report output
- `latency.go` — Latency benchmark implementations
- `throughput.go` — Throughput stress test implementations
- `report.go` — JSON/text report generation

---

## 5. Implementation Order

| Phase | Scope | Dependencies |
|-------|-------|-------------|
| 1 | `internal/election/` — Leader interface, K8s, S3, Noop, Auto electors | None |
| 2 | `internal/compaction/` — Scheduler, Compactor, LevelPolicy, Sentinel | election package |
| 3 | `internal/manifest/push.go` + receiver handler — Peer notification | manifest package |
| 4 | Wire compaction + push into `cmd/lakehouse/main.go` | compaction, manifest/push |
| 5 | Compaction + election metrics and config flags | metrics, config |
| 6 | `cmd/loadtest/` — Latency benchmarks | Running lakehouse instance |
| 7 | `cmd/loadtest/` — Throughput stress tests | Running lakehouse instance |
| 8 | Helm RBAC + compaction config in values.yaml | Helm chart |
| 9 | Integration tests + CI job | All above |

---

## 6. Non-Goals (Out of Scope)

- SQS/SNS for external S3 bucket event notifications (deferred to future milestone)
- Cross-region manifest sync
- Compaction of files across different schema fingerprints (skipped with warning)
- Tiered storage classes (S3 IA, Glacier) for compacted files
- Separate compaction worker binary (can be extracted later if needed)
