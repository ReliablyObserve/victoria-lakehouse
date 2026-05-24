# K8s Scaling Safety Layer — Design Spec

**Date:** 2026-05-24
**Goal:** Zero-data-loss scaling for Victoria Lakehouse under Kubernetes HPA, StatefulSet lifecycle events, and topology changes — with full Helm chart alignment.

**Reference Architecture:** Scenario B — 3 combined nodes + 10 select nodes with consistent hash ring.

---

## 1. Deployment Topology Modes

The Helm chart supports three deployment modes, selectable per signal (logs/traces):

### Mode A: Combined + Select Tier (Recommended Production)
```
                    ┌─────────────────────────────┐
                    │         vmauth / LB          │
                    └──────┬──────────────┬────────┘
                   writes  │              │ reads
                           v              v
                ┌──────────────┐  ┌──────────────────┐
                │  Combined    │  │   Select Tier     │
                │  (insert +   │  │   (read-only,     │
                │   select +   │  │    queries S3 +   │
                │   compaction)│  │    combined for    │
                │              │  │    unflushed data) │
                │  StatefulSet │  │   StatefulSet      │
                │  HPA-C       │  │   HPA-A            │
                │  3 replicas  │  │   10 replicas      │
                └──────────────┘  └──────────────────┘
                       │                   │
                       v                   v
                    ┌─────────────────────────┐
                    │         S3 / MinIO       │
                    └─────────────────────────┘
```

- **Combined nodes:** Handle writes (insert), reads (select), compaction. Fixed or slowly-scaling StatefulSet with own HPA (HPA-C). Each has WAL, disk cache, manifest persistence on PV.
- **Select tier:** Read-only pods that query S3 directly + query combined nodes for unflushed data via buffer bridge. Fast-scaling StatefulSet with own HPA (HPA-A). Disk cache on PV, no WAL.
- **Two independent HPAs:** HPA-C scales combined (conservative, based on CPU/memory/write throughput), HPA-A scales select tier aggressively (based on query latency/CPU).

### Mode B: Split Insert + Select (Advanced)
Combined nodes split into dedicated insert-only and select-only StatefulSets. This is effectively Mode A where combined nodes are decomposed — the select tier from Mode A still applies as an additional read scaling layer.

### Mode C: Combined-Only (Simple/Dev)
All nodes are combined (insert + select + compaction). Single HPA. Simplest but hardest to scale safely for reads vs writes independently.

**Helm values structure:**
```yaml
logs:
  # Mode A: combined + select tier
  combined:
    enabled: true
    replicaCount: 3
    horizontalPodAutoscaler:
      enabled: true
      minReplicas: 3
      maxReplicas: 6
      targetCPUUtilizationPercentage: 70
  select:
    enabled: true  # Select tier in front of combined
    replicaCount: 10
    horizontalPodAutoscaler:
      enabled: true
      minReplicas: 5
      maxReplicas: 20
      targetCPUUtilizationPercentage: 60
  insert:
    enabled: false  # Not used in Mode A (combined handles writes)
```

---

## 2. Graceful Shutdown — Guaranteed Flush

### Problem
When K8s sends SIGTERM, the pod has `terminationGracePeriodSeconds` (default 60s) to flush all buffered data, persist WAL, sync manifest to S3, and release leader election lease. If any step fails or times out, data loss occurs.

### Design

**Shutdown sequence (ordered, with time budget):**

```
SIGTERM received (T=0)
  │
  ├─ Phase 1: Drain (T=0 to T=delay)
  │   - Stop accepting new writes (readiness probe → 503)
  │   - LB/vmauth drains in-flight requests
  │   - shutdown.delay = 5s (configurable)
  │
  ├─ Phase 2: Flush (T=delay to T=delay+flush_timeout)
  │   - writer.FlushAll() — flush all partition buffers to S3
  │   - WAL truncate after successful flush
  │   - Manifest push to peers (notify files added)
  │   - flush_timeout = 30s (configurable)
  │
  ├─ Phase 3: Persist (T=flush_end to T=flush_end+10s)
  │   - manifest.SaveTo(disk) — persist enriched manifest to PV
  │   - manifest.SaveTo(S3) — backup to S3 (_meta/manifest-snapshot-<pod>.json)
  │   - cache metadata snapshot to disk
  │   - tombstone state to disk
  │   - stats snapshot to S3
  │
  ├─ Phase 4: Release (T=persist_end to T=persist_end+5s)
  │   - leader.Stop() — release K8s Lease (compaction leadership)
  │   - Notify peers of departure (ring update)
  │   - Close S3 connections
  │
  └─ Phase 5: Exit (T=total)
      - Process exits cleanly
      - Total budget: delay(5) + flush(30) + persist(10) + release(5) = 50s
      - Must complete within terminationGracePeriodSeconds(60s)
```

**Safety invariant:** `shutdown.delay + flush_timeout + persist_timeout + release_timeout < terminationGracePeriodSeconds - 5s` (5s safety margin).

**Helm values:**
```yaml
lakehouseConfig:
  shutdown:
    delay: 5s
    max_graceful_duration: 7s  # existing
    flush_timeout: 30s         # NEW
    persist_timeout: 10s       # NEW
    release_timeout: 5s        # NEW
    # Validation: sum must be < terminationGracePeriodSeconds - 5s
```

**Metrics exposed:**
- `lakehouse_shutdown_phase_duration_seconds{phase="drain|flush|persist|release"}`
- `lakehouse_shutdown_flush_rows_total` — rows flushed during shutdown
- `lakehouse_shutdown_success` — 1 if clean exit, 0 if forced

### Insert Node Specifics
- FlushAll must succeed before WAL truncate
- If FlushAll fails (S3 unreachable), WAL is preserved on PV for replay on next start
- If S3 upload partially succeeds, manifest records which files were uploaded
- Retry logic: 3 attempts with exponential backoff within flush_timeout budget

### Select Node Specifics
- No WAL or write buffers — shutdown is simpler
- Drain in-flight queries (respect max_graceful_duration)
- Persist cache metadata snapshot to disk (warm restart optimization)
- Notify peers of ring departure

---

## 3. HPA Scale-Up — Zero Data Loss

### Select Tier Scale-Up (Mode A)

**Trigger:** HPA detects CPU > 60% or custom metric (query latency p99 > threshold).

**Sequence:**
1. K8s creates new select pod
2. Pod enters `PhaseInit` — not in readiness, LB doesn't route traffic
3. `PhaseDiskRecovery` — check PV for cached manifest snapshot, load if fresh
4. `PhaseS3Refresh` — full S3 manifest refresh (authoritative)
5. `PhasePeerSync` — discover peers via headless service DNS SRV, join hash ring
6. `PhaseCacheWarmup` — warm owned partitions (configurable depth)
7. `PhaseReady` — readiness probe returns 200, LB starts routing

**Ring rebalancing on join:**
- New pod joins ring → some partitions shift ownership
- Existing pods detect ring change via peer discovery refresh (30s interval)
- No immediate cache invalidation — old owners serve requests until ring stabilizes
- Gradual migration: new owner fetches from S3 on first query, caches locally

**Helm values:**
```yaml
lakehouseConfig:
  startup:
    serve_stale: false           # existing
    warmup_window: 24h           # existing
    max_warmup_time: 5m          # existing
    peer_sync_timeout: 30s       # NEW: max time to discover peers before marking ready
    require_manifest_sync: true  # NEW: block readiness until S3 manifest is loaded
```

### Combined Node Scale-Up

**Additional steps beyond select:**
1. Auto-detect shard ID from hostname ordinal (existing: `AutoDetectShardID()`)
2. Join compaction shard ring — only compact owned partitions
3. Start WAL + flush loop
4. If PV contains stale WAL from previous pod lifecycle, replay first (see Section 5)

---

## 4. HPA Scale-Down — Coordinated Drain

### Select Tier Scale-Down

**Trigger:** HPA detects sustained low CPU.

**Sequence:**
1. K8s selects pod for termination (highest ordinal in StatefulSet)
2. SIGTERM sent → graceful shutdown (Section 2)
3. Pod removed from headless service DNS (immediate)
4. Other pods detect ring change on next peer refresh
5. Partitions owned by departing pod redistribute to remaining pods
6. Departing pod's cached data is NOT transferred — next owner fetches from S3

**Safety:** No data loss risk since select pods have no writes. Only concern is query continuity — PDB ensures `minAvailable` pods always serve.

### Combined Node Scale-Down (CRITICAL)

**Additional safety requirements:**
1. **WAL drain:** FlushAll must succeed before shutdown
2. **Compaction handoff:** If this pod is compaction leader, release lease AFTER current compaction job completes (or cancel with rollback)
3. **Buffer bridge drain:** Select tier pods querying this combined node for unflushed data must be notified to re-route
4. **Manifest sync:** All files written by this pod's final flush must be visible in S3 manifest before exit

**Sequence:**
1. SIGTERM received
2. Mark NOT READY (stop accepting writes from LB)
3. Wait for in-flight writes to complete (shutdown.delay)
4. FlushAll() — all buffered data to S3
5. Truncate WAL (only after FlushAll succeeds)
6. Push manifest update to peers (new files from final flush)
7. If compaction leader: wait for current job, release lease
8. Persist manifest to disk + S3
9. Notify peers of departure
10. Exit

**Helm values for PDB (existing, should be enabled for combined):**
```yaml
logs:
  combined:
    podDisruptionBudget:
      enabled: true
      minAvailable: 2  # At least 2 of 3 combined nodes must be up
```

---

## 5. Stale PV Detection on StatefulSet Pod Restart

### Problem
StatefulSet pods reuse PVs. After a pod restarts (crash, node drain, HPA scale-down then scale-up), its PV may contain:
- **Stale WAL:** Unflushed rows from hours/days ago (may already be flushed by another pod or lost)
- **Stale manifest snapshot:** Out-of-date file listing (files may have been compacted/deleted)
- **Stale disk cache:** Parquet footers and data pages that reference deleted/compacted files
- **Stale label index:** Field names that no longer exist

### Design: Staleness Detection on Startup

**New startup phase: `PhaseStaleCheck` (after DiskRecovery, before S3Refresh):**

```
PhaseInit
  │
  ├─ PhaseDiskRecovery
  │   └─ Replay WAL (if exists)
  │
  ├─ PhaseStaleCheck (NEW)
  │   ├─ Read disk manifest snapshot timestamp
  │   ├─ Compare with current time → staleness_duration
  │   ├─ If staleness_duration > stale_threshold:
  │   │   ├─ Log WARNING: "Stale PV detected (age: Xh). Performing full resync."
  │   │   ├─ Invalidate disk cache (mark all entries expired)
  │   │   ├─ Invalidate label index
  │   │   └─ WAL reconciliation (see below)
  │   └─ If fresh: proceed normally
  │
  ├─ PhaseS3Refresh
  │   └─ Full manifest from S3 (authoritative)
  │
  ├─ PhasePeerSync
  │   └─ Discover peers, join ring
  │
  ├─ PhaseCacheWarmup
  │   └─ Warm owned partitions
  │
  └─ PhaseReady
```

**WAL Reconciliation Logic:**
1. Read WAL entries (timestamps + partition keys)
2. For each WAL entry's partition, check S3 manifest: does a file exist covering that time range?
3. If yes → WAL entry was already flushed (by this pod before crash, or by another pod) → skip
4. If no → WAL entry contains data not yet in S3 → re-flush to S3
5. Truncate WAL after reconciliation

**Disk Cache Invalidation on Stale PV:**
- Don't delete cache files immediately (expensive I/O on startup)
- Mark all cache entries as `needs_revalidation`
- On first access, check S3 ETag/LastModified before serving from cache
- Background revalidation loop cleans up stale entries

**Helm values:**
```yaml
lakehouseConfig:
  startup:
    stale_threshold: 1h           # NEW: PV age beyond which full resync triggers
    wal_reconciliation: true      # NEW: enable WAL reconciliation on stale PV
    cache_revalidation: true      # NEW: revalidate disk cache entries on stale PV
    max_resync_time: 10m          # NEW: max time for stale PV resync before marking ready
```

**Metrics:**
- `lakehouse_startup_stale_pv_detected` — 1 if stale PV detected on startup
- `lakehouse_startup_staleness_hours` — age of PV data at startup
- `lakehouse_startup_wal_reconciled_rows` — rows reconciled from stale WAL
- `lakehouse_startup_cache_invalidated_entries` — cache entries invalidated

---

## 6. Peer Discovery and Ring Rebalancing

### Current State
- DNS SRV via headless service for peer discovery
- Consistent hash ring with 150 vnodes per peer
- Health-aware routing with failure counting
- AZ-aware lookup for locality

### Enhancements for Scaling Safety

**Ring Change Detection:**
```go
type RingChangeEvent struct {
    Added   []string  // new peer addresses
    Removed []string  // departed peer addresses
    Ring    *Ring     // new ring state
}
```

**Ring change notification flow:**
1. Peer refresh loop detects DNS change (new/removed SRV records)
2. Build new ring, compute diff (added/removed peers)
3. Emit `RingChangeEvent` to subscribers:
   - Cache controller: recompute partition ownership
   - Query coordinator: update fan-out targets
   - Manifest pusher: update peer list
4. Graceful transition period: serve from old and new owner for `ring_stabilize_duration`

**Helm values:**
```yaml
lakehouseConfig:
  discovery:
    peer_refresh_interval: 30s     # existing
    ring_stabilize_duration: 60s   # NEW: overlap period during ring changes
    ring_change_notify: true       # NEW: push ring change events to peers
```

**Verified Handoff Protocol (for cache ownership transfer):**
1. Old owner continues serving cached data during stabilize_duration
2. New owner starts fetching owned partitions from S3
3. After stabilize_duration, old owner evicts transferred partitions
4. No explicit data transfer between peers — S3 is the source of truth

---

## 7. Query Distribution During Topology Changes

### Select Tier Query Routing

**During ring change (stabilize_duration window):**
- Queries for partitions with ownership change are served by BOTH old and new owner
- Deduplication at query coordinator level (if fan-out query)
- For single-pod queries (no fan-out), route to whichever owner has cached data
- Fall back to S3 if neither has cache

**Buffer Bridge Adaptation:**
- Select pods query combined nodes for unflushed data
- When combined node count changes, select pods rediscover via headless service
- Buffer bridge queries all combined nodes (small fleet, broadcast is fine)
- If combined node is shutting down, returns 503 → select pod retries other combined nodes

**Circuit breaker per peer:**
- Existing: 5 failures → open circuit → 30s timeout → half-open probe
- During scale-down: peers in SIGTERM phase respond with HTTP 503 + `X-Lakehouse-Draining: true` header
- Select pods receiving this header immediately remove peer from ring (don't wait for DNS update)

---

## 8. Helm Chart Changes Summary

### New Values
```yaml
lakehouseConfig:
  shutdown:
    flush_timeout: 30s
    persist_timeout: 10s
    release_timeout: 5s

  startup:
    stale_threshold: 1h
    wal_reconciliation: true
    cache_revalidation: true
    max_resync_time: 10m
    peer_sync_timeout: 30s
    require_manifest_sync: true

  discovery:
    ring_stabilize_duration: 60s
    ring_change_notify: true
```

### StatefulSet Template Changes
- Add `preStop` lifecycle hook for graceful drain notification
- Add `X-Lakehouse-Draining` header support in readiness probe logic
- Ensure `terminationGracePeriodSeconds` > sum of shutdown phase timeouts + 5s margin
- Add startup probe with generous `failureThreshold` (120 * 5s = 10min for stale PV resync)

### PreStop Hook
```yaml
lifecycle:
  preStop:
    exec:
      command:
        - /bin/sh
        - -c
        - |
          # Notify peers of imminent departure
          wget -qO- http://localhost:${PORT}/internal/lifecycle/drain || true
          # Wait for shutdown delay to let LB drain
          sleep ${SHUTDOWN_DELAY}
```

### PDB Recommendations
```yaml
logs:
  combined:
    podDisruptionBudget:
      enabled: true
      minAvailable: 2      # 2 of 3 combined must be up
  select:
    podDisruptionBudget:
      enabled: true
      minAvailable: 7      # 7 of 10 select must be up (70%)
```

### HPA Configuration
```yaml
logs:
  combined:
    horizontalPodAutoscaler:
      enabled: true
      minReplicas: 3
      maxReplicas: 6
      targetCPUUtilizationPercentage: 70
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 300  # 5min cooldown
          policies:
            - type: Pods
              value: 1
              periodSeconds: 300          # Max 1 pod per 5min
        scaleUp:
          stabilizationWindowSeconds: 60
          policies:
            - type: Pods
              value: 2
              periodSeconds: 60           # Max 2 pods per minute

  select:
    horizontalPodAutoscaler:
      enabled: true
      minReplicas: 5
      maxReplicas: 20
      targetCPUUtilizationPercentage: 60
      behavior:
        scaleDown:
          stabilizationWindowSeconds: 180  # 3min cooldown
          policies:
            - type: Percent
              value: 20
              periodSeconds: 120          # Max 20% per 2min
        scaleUp:
          stabilizationWindowSeconds: 30
          policies:
            - type: Pods
              value: 5
              periodSeconds: 60           # Max 5 pods per minute
```

---

## 9. Internal Lifecycle API

New internal HTTP endpoints for scaling coordination:

| Endpoint | Method | Purpose |
|----------|--------|---------|
| `/internal/lifecycle/drain` | POST | Signal pod is draining (preStop hook) |
| `/internal/lifecycle/ready` | GET | Detailed readiness with phase info |
| `/internal/lifecycle/ring` | GET | Current ring state + owned partitions |
| `/internal/lifecycle/stale` | GET | PV staleness status |
| `/internal/manifest/snapshot` | POST | Trigger manifest snapshot to disk+S3 |

---

## 10. Testing and Hardening

### Unit Tests
- Shutdown sequence: verify flush completes before WAL truncate
- Stale PV detection: mock PV with old timestamps, verify resync triggers
- WAL reconciliation: verify duplicate detection against S3 manifest
- Ring rebalancing: verify partition ownership transfer on add/remove
- Cache invalidation: verify stale entries marked for revalidation

### Integration Tests
- Docker Compose with 3 combined + 3 select nodes
- Kill combined node during write → verify no data loss after restart
- Scale select tier from 3 to 5 to 2 → verify query continuity
- Restart pod with stale PV → verify WAL reconciliation and cache revalidation
- Concurrent compaction during scale-down → verify no orphan files

### E2E Tests (K8s)
- Deploy via Helm with HPA enabled
- Generate load → trigger HPA scale-up → verify ring rebalancing
- Remove load → trigger HPA scale-down → verify graceful drain
- `kubectl delete pod` during write → verify WAL recovery
- Node drain (`kubectl drain`) → verify PDB prevents simultaneous eviction
- Simulate stale PV by stopping pod for 2h → restart → verify full resync

### Regression Tests
- Row count parity: LH result == VL/VT result for all query types
- Field preservation: all fields survive compaction + restart cycle
- Timestamp ordering: sorted output invariant holds after ring change
- No duplicate rows after WAL reconciliation
- No missing rows after scale-down drain

---

## 11. Metrics Dashboard

### Scaling Events Panel
- `lakehouse_ring_peers_total` — current ring size
- `lakehouse_ring_change_events_total{type="add|remove"}` — ring change frequency
- `lakehouse_ring_stabilize_in_progress` — 1 during stabilize_duration

### Shutdown Health Panel
- `lakehouse_shutdown_phase_duration_seconds{phase="..."}` — per-phase timing
- `lakehouse_shutdown_flush_rows_total` — rows flushed during shutdown
- `lakehouse_shutdown_success` — clean exit rate

### Startup Health Panel
- `lakehouse_startup_phase_duration_seconds{phase="..."}` — per-phase timing
- `lakehouse_startup_stale_pv_detected` — stale PV detection rate
- `lakehouse_startup_wal_reconciled_rows` — WAL reconciliation volume

### Query Continuity Panel
- `lakehouse_query_peer_errors_total{type="draining|unreachable|timeout"}` — peer errors during scaling
- `lakehouse_buffer_bridge_fallback_total` — buffer bridge failures during combined scale events
