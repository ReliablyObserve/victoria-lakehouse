# Lifecycle & readiness

This page documents what `/ready` reports, what each lifecycle phase
means, and what to monitor in production. Read this if you're
operating a Lakehouse cluster at multi-PB scale — the wrong
readiness gating at scale shows up as routing traffic to pods that
return empty data.

## The three-state `/ready`

`/ready` returns one of three HTTP codes:

| Code | Meaning | k8s probe behaviour |
| ---: | --- | --- |
| **503** | Not ready. ServingReady is false — disk recovery still running, WAL not replayed, or manifest below `MinManifestFiles` threshold | Pod NOT routed |
| **204** | Serving but background warmup in progress. ServingReady=true, WarmupComplete=false | Pod routed (any 2xx) |
| **200** | Fully ready. ServingReady=true AND WarmupComplete=true | Pod routed always |

The split lets soft routers (vtselect peer fan-out, k8s readinessProbe
with `successThreshold: 1`) accept partial-data pods while strict
deployments (helm readinessProbe with `successThreshold > 1`, AWS
ALB target group health checks) wait for full warmup.

### Switching between strict and serve-while-warming

```yaml
# config.yaml
startup:
  serve_while_warming: true   # default false (strict 200-or-503)
```

When `serve_while_warming: true`, /ready returns 204 once
`ServingReady` is true. Otherwise it stays at 503 until 200.

## `ServingReady` preconditions

A pod is ServingReady (the 204/200 floor) only after ALL of these:

1. **Disk recovery complete** — manifest snapshot loaded from
   `cfg.manifest.persist_path`. Almost always sub-second; the only
   slow case is a 1 GB+ snapshot on slow disk.

2. **WAL replay complete** — for insert role only. The on-disk
   write-ahead log is replayed back into in-memory buffers before
   accepting reads. Skipped for select-only pods.

3. **`MinManifestFiles` gate cleared** — manifest holds at least
   `cfg.startup.min_manifest_files` entries. Default 0 = gate off.
   Production at PB scale should set this above the smallest
   healthy partition count so the first-ever-boot scenario can't
   lie about readiness while the background S3 LIST runs.

## `WarmupComplete` preconditions

Background goroutine after ServingReady, all of these complete:

1. **S3 manifest refresh** — `RefreshManifest` walks the bucket for
   new files since the snapshot. Bounded by `cfg.startup.max_warmup_time`
   (default 5 min).

2. **Label index warmup** — `WarmLabelIndex` builds the field-name
   cache from manifest metadata.

3. **Cache warmup** — `WarmupCache` prefetches footers for the most
   recent N partitions (configured via `cfg.cache.warmup_partitions`).

4. **Bloom backfill** — `BackfillBloomIndex` populates bloom filters
   for files that don't have them in the footer yet.

## Configuration reference

```yaml
startup:
  # Lower bound for /ready=200. Default 0 (gate off, fine for
  # dev/CI). Production: set above smallest healthy partition
  # count. Suggested per scale:
  #   tiny dev cluster  →  100
  #   single-node prod  →  1000
  #   PB-scale fleet    → 10000+
  min_manifest_files: 10000

  # If true, /ready returns 204 ("warming") instead of 503
  # while background warmup runs. k8s `successThreshold: 1`
  # routes 204 traffic; AWS ALB requires 200 by default.
  serve_while_warming: true

  # Existing settings still apply:
  warmup_window: 5m
  max_warmup_time: 5m
  serve_stale: false

shutdown:
  # Max time the manifest snapshot save can hold the SIGTERM
  # grace window. Default 30s; bump if the snapshot is huge.
  # k8s sends SIGKILL after the grace period — losing the
  # snapshot means the next boot reverts to a stale one.
  persist_timeout: 30s
```

## Metrics to monitor

| Metric | What it tells you | Alert when |
| --- | --- | --- |
| `lakehouse_serving_ready` | 1 once ServingReady true | unexpectedly 0 in steady state |
| `lakehouse_warmup_complete` | 1 once WarmupComplete true | stays 0 longer than `max_warmup_time` |
| `lakehouse_manifest_snapshot_age_seconds` | wall-clock age of local snapshot | > 6× `manifest.persist_interval` (~30 min default) |
| `lakehouse_min_manifest_files_gate` | configured threshold (debug visibility) | n/a |
| `lakehouse_manifest_files` | current file count | < `min_manifest_files` for >5 min |
| `lakehouse_startup_phase` | phase enum (0..6) | stuck at any non-Ready value >10 min |
| `lakehouse_startup_total_seconds` | last cold start total | regression vs baseline by 2x+ |
| `lakehouse_footer_cache_entries` | current footer-cache size | < ~80% of pre-shutdown count 5 min after restart (snapshot prefetch failing) |
| `lakehouse_buffer_bridge_az_requests_total` | buffer-bridge fan-out by AZ type | self-loop label > 0 confirms single-node mode is serving its own buffer |

## Restart snapshot pair

Two artifacts persist at shutdown into `cfg.Manifest.PersistPath`:

| File | Purpose | Cost on shutdown | Cost on load |
| --- | --- | --- | --- |
| `manifest-snapshot.json` (binary gob format) | Re-hydrates the in-memory file index without re-listing S3 | ~10-50 ms for 1 M files | ~50-200 ms streaming decode (capped at 50 GiB) |
| `footer-cache-snapshot.bin` | LRU-ordered key list for async footer prefetch on the next start | < 1 ms even at 100k keys | < 1 ms parse; the actual S3 range-reads run in the background after `/ready=200` |

Both writes are guarded by `cfg.Shutdown.PersistTimeout` (default 30 s) so a misbehaving local disk can't extend pod termination beyond the kube `terminationGracePeriodSeconds` budget. The footer-cache snapshot is intentionally key-only: a list is a few hundred KiB even at million-file scale, and the actual footer bytes get re-fetched from S3 by the asynchronous prefetch — keeping the snapshot itself cheap to write and impossible to bloat.

## Restart timeline expectations

See `docs/architecture/scaling-restart-scenarios.md` for the full
worst-case analysis at PB scale. Quick reference for a healthy
6-peer cluster:

| Scenario | /ready=200 | Queries complete |
| --- | --- | --- |
| Rolling restart, 1 of 6 | 5-15 s | immediate (via peers) |
| Simultaneous restart, all 6 | 5-15 s | **1-5 min later** (buffer cold-start cluster-wide) |
| First-ever boot, peers exist | 1-5 s | 30-90 s |
| First-ever boot, no peers | **gated by MinManifestFiles** | 3-5 min (full S3 LIST) |
| Stale snapshot (>1h downtime) | 5-15 s | 30 s-5 min (depends on compaction churn during downtime) |

## When `/ready` lies (and how to catch it)

The `MinManifestFiles` gate exists because the original /ready=200
flipped true the moment the HTTP listener bound, regardless of
whether the manifest had any data. Symptoms of a lying /ready:

- Grafana dashboards refresh empty after a deploy
- `lakehouse_serving_ready=1` but `lakehouse_manifest_files=0`
- vtselect fan-out reports "all peers responded" but returned
  zero rows

Set `min_manifest_files` above your smallest healthy partition
count, and the gate refuses /ready=200 until the manifest fills.
