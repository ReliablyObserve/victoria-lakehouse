# Scaling: restart scenarios at PB scale

This is the honest cold-start / restart analysis for a Lakehouse
cluster at multi-PB scale with 6 peer pods. Read this before
designing your deployment, because the wrong defaults at this
scale create user-visible empty-data windows that vary from
"invisible during normal rolling deploys" to "1-5 min cluster-wide
blackout on a simultaneous restart".

> Best-case numbers in this doc are from the e2e compose at small
> scale; worst-case projections extrapolate to PB scale based on
> the underlying cost drivers. Reality at your scale may be
> different — calibrate with `lakehouse_startup_total_seconds`
> and `lakehouse_manifest_snapshot_age_seconds` in steady-state.

## Cost drivers

| Phase | What scales with | Mitigations applied |
| --- | --- | --- |
| Disk recovery (snapshot load) | manifest file count × ~100 bytes/entry on gob | binary gob format; streaming decode (planned) |
| Footer cache snapshot load | `FooterMaxItems` × ~50 KB each | async load off /ready path (planned) |
| S3 manifest refresh | manifest delta since snapshot | snapshot persisted every 5 min; only deltas LISTed |
| Cache warmup | `WarmupPartitions × WarmupMaxFiles` × 50 ms S3 fetch | priority warmup (planned), backoff+jitter (planned) |
| Buffer restore | logstore parts restored on open | gated on /ready via lifecycle manager |

## Scenarios

### 1. Rolling restart, 1 of 6 (the good day)

```
t=0       restart pod 0
t=2-3 s   /ready=200, queries fully served
```

- 5 healthy peers' BufferBridges have warm buffers
- This pod's manifest loads from local disk snapshot (~3 s on NVMe at 1 GB)
- Background S3 refresh catches up the delta (~50 ms for 5-min-old snapshot)
- vtselect routes around the restarting pod until /ready=200

**User impact:** invisible. No 503s, no empty results.

### 2. Simultaneous restart, all 6 (deploy / node failure cascade)

```
t=0       all 6 pods restart
t=5-15 s  all 6 reach /ready=200, BufferBridges call each other
          ── BUT every peer's buffer is empty post-restart ──
t=0-120 s no in-memory buffer ANYWHERE in cluster. Recent data
          (last ~2 min before restart) is in the logstore buffer parts on disk,
          restoring. Queries see ONLY S3-flushed data (5+ min
          stale at best).
t=120 s+  buffer restore done, new ingest fills buffers
t=300 s+  steady state
```

**User impact:** 1-5 min window where queries return ONLY
S3-flushed data. Recent (last few minutes) data is invisible.
Dashboards showing "last minute" go blank.

**Mitigations:**
- Stagger restarts. `kubectl rollout restart` already does this
  via maxUnavailable.
- Increase `cfg.shutdown.persist_timeout` so the final snapshot
  completes before SIGKILL on all 6.
- (Planned) Cluster-wide "data-cold" flag — return 503 with
  `Retry-After` while all peers are cold rather than empty 200.

### 3. First-ever boot, no snapshot, fresh PVC

```
t=0       container starts (no snapshot on disk)
t=~1 s    disk recovery no-op (nothing to load)
t=~1 s    ServingReady false — MinManifestFiles gate not met
t=~1 s    /ready=503 (correct — no data to serve)
          ── background S3 LIST runs for 3-5 min on 5M files ──
t=180-300s manifest fills, MinManifestFiles gate clears
t=180-300s /ready flips to 200, queries return real data
```

**User impact:** k8s readiness probe keeps the pod out of rotation
until the S3 LIST completes. No empty-200 lies.

**Pre-`MinManifestFiles`-gate behaviour:** /ready=200 fired at
t=1 s while the manifest was empty. vtselect routed traffic, every
query returned 0 rows for 3-5 min. This was the documented worst
honesty case.

### 4. First-ever boot, peers exist (adding a pod to running cluster)

```
t=0       container starts on fresh PVC
t=~1 s    disk recovery no-op
t=~1 s    ServingReady gated by MinManifestFiles
          ── background S3 LIST runs ──
t=30-90 s manifest fills, /ready=200
```

**User impact:** vtselect routes around this pod for the first
30-90 s; existing pods serve all traffic. Once /ready=200 this
pod joins the rotation.

### 5. Stale snapshot (>1h downtime)

```
t=0       restart after 1 h+ downtime
t=~3 s    disk recovery loads stale manifest (1 h old)
t=~3 s    MinManifestFiles passes (manifest is full, just stale)
t=~3-10 s /ready=200; serve-while-warming reports 204 during background
          ── background S3 refresh catches up ──
          ── queries during this window may hit 404 stale entries ──
t=10-30 s manifest delta synced from S3
t=30-300 s background bloom backfill etc.
```

**User impact:** 30 s-5 min of partial results. Stale-entry 404s
self-heal via `handle404Recovery` (one round trip + manifest evict
per stale file).

### 6. Fragmented L0 hot zone (worst-case wide scan)

Not a restart scenario, but the worst-case query timing at scale:

- 200 k L0 files in last 24 h
- Footer cache holds 10 k (default cap)
- Wide-window query with field filter (`service.name=X | stats count()`)
  needs footer per file for bloom check
- 200 k - 10 k = 190 k cache misses
- At concurrency 16, 50 ms per S3 footer fetch: **~10 min per query**

**Mitigation:** bump `cfg.cache.footer_max_items` to 200 k, but
that's ~10 GB RAM. Or shorten the time window. Or wait for
compaction to merge L0 files.

## Mitigation summary

| Scenario | Mitigation | Status |
| --- | --- | --- |
| 1 (rolling) | none needed | n/a |
| 2 (all-restart cold buffer) | data-cold cluster flag | planned (P2) |
| 3 (fresh PVC) | MinManifestFiles gate | **landed** |
| 4 (fresh PVC, peers up) | none needed (gate self-resolves) | **landed** |
| 5 (stale snapshot) | handle404Recovery + periodic refresh | already in place |
| 6 (fragmented L0) | tune footer_max_items + WarmupPartitions | configurable |

## What we deliberately don't do

- **Streaming-from-peers cold-tier reads.** A fresh pod with empty
  manifest CAN'T transparently proxy historical queries to peers
  today — each pod independently walks S3. Solving this requires
  manifest-delta-log or distributed snapshot work that's RFC-stage.
  Filed under P5 backlog.

- **Live-tail across simultaneous restart.** During scenario 2,
  data ingested in the last 2 min of pre-restart life is in the
  logstore buffer parts on disk only. We could send it to S3 on SIGTERM but the latency
  budget doesn't allow it (S3 PUT can take seconds). Accept the
  2-5 min cold-buffer window or run with `maxUnavailable: 1` so
  this scenario never happens.

## Tuning recommendations

For a cluster that ingests >100 GB/day per peer:

```yaml
startup:
  min_manifest_files: 10000        # gate fresh-PVC honesty
  serve_while_warming: true        # 204 routing during warmup
  max_warmup_time: 10m             # tolerate bigger S3 LIST

shutdown:
  persist_timeout: 60s             # bigger snapshot needs more time

cache:
  footer_max_items: 100000         # cover fragmented L0 hot zone
  warmup_partitions: 12            # pre-load last 12 h on /ready
  warmup_max_files: 2000

manifest:
  refresh_interval: 30s            # tighter than 5-min default
  persist_interval: 2m             # halve staleness window
```
