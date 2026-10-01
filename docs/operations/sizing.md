# Sizing guide

How to size memory, CPU, disk (PVC), and peer count for a
Lakehouse deployment. The numbers below are derived from the
[restart-and-warmup design](../architecture/restart-and-warmup-design.md)
and the per-component cost drivers,
not picked from a marketing slide — calibrate against your
actual `lakehouse_*` metrics in steady state.

## TL;DR table

| Scale | Manifest files | Peers (insert+select) | Memory per pod | PVC per pod | Notes |
| --- | ---: | --- | ---: | ---: | --- |
| Dev / CI | < 1 k | 1+1 | 1 GiB | 10 GiB | defaults fine |
| Small prod | 10 k | 2+2 | 2 GiB | 50 GiB | `min_manifest_files=1000` |
| Medium | 100 k | 3+3 | 4 GiB | 100 GiB | `footer_max_bytes=1073741824` (1 GiB) |
| Large | 1 M | 6+6 | 8 GiB | 200 GiB | tune persist interval |
| PB-scale | 5 M+ | 10+ | 16 GiB | 500 GiB | see PB sizing below; several terms are not bounded yet — see [scale limits](../petabyte-scale-audit.md) |

> Peer count above is `insert pods` + `select pods`. In `topology=all`
> deployments the two roles share a pod and the numbers can be
> consolidated.

> Both binaries bound the footer cache by **bytes**, with
> `cache.footer_max_bytes` (flag `-lakehouse.cache.footer-max-bytes`;
> `0` = 256 MiB on logs, 512 MiB on traces). An entry is charged its
> raw footer plus page-index tail and the decoded metadata, about
> 0.2 MB for a flush-sized logs object, about 1.2-1.5 MB for a
> 10 MB compacted logs object with token blooms and about 1.3-1.9 MB
> for a traces L2 object with `_trace_idx` (measured, see
> [read path](../read-path.md#footer-cache-and-zero-get-open)). The
> cache holds as many footers as fit.
>
> Per-file manifest RAM and resident pmeta size at these file counts
> are **not measured** — the figures below
> are estimates from struct shapes and per-component cost drivers.
> The [scale limits page](../petabyte-scale-audit.md) lists which
> terms grow with file count, partitions × tenants, or replicas.

## Memory budget breakdown

```
Per pod, steady state:

  + Manifest in-memory state       ≈ 200 bytes × file_count  (estimate)
  + Footer cache                   ≤ cache.footer_max_bytes  (byte-bounded; default
                                      256 MiB logs, 512 MiB traces; entry ≈ 0.2-1.9 MB, measured)
  + pmeta resident bundles         ≈ grows with partitions × tenants; no eviction
                                      of live partitions — watch
                                      lakehouse_catalog_resident_bytes
  + Smart cache (L1)               ≈ cfg.cache.memory_mb MiB
  + Buffer restore (segment parts)    ≈ 100 MiB peak during startup
  + Per-query memory (max_live_bytes)  default 512 MiB × query.max_concurrent (default 32)
  + Background goroutine pools     ≈ 100-200 MiB
  + Go runtime overhead            ≈ 300 MiB
  ────────────────────────────────────────────────
  ≈ baseline + workload spikes
```

### Worked examples

**Small prod cluster, 10k files, default footer cache (256 MiB, logs):**

```
manifest        : 200 B × 10k    = 2 MB
footer cache    : ≤ 256 MiB (byte budget; ~1,300 flush-sized footers)
smart cache L1  : 256 MB
insert buffer   : 100 MB (recent ingest)
query memory    : 512 MB × max_concurrent=8 = 4 GB (worst-case)
goroutines      : 200 MB
Go runtime      : 300 MB
────────────────────────────────────────
steady state    : ≈ 1.1 GB
worst-case query burst : ≈ 5.1 GB
```

Pod memory limit: **2 GB** with steady-state queries; **8 GB**
if running heavy concurrent wildcard scans.

**PB-scale cluster, 5M files, footer_max_bytes=8 GiB (traces pod):**

```
manifest        : 200 B × 5M     = 1 GB   (estimate)
footer cache    : ≤ 8 GiB budget  = ~4,500-6,000 traces L2 footers at ~1.3-1.9 MB (logs pod default: 256 MiB)
pmeta bundles   : not measured at this scale — excluded from the total
smart cache L1  : 1 GB (cfg.cache.memory_mb=1024)
insert buffer   : 200 MB (recent ingest)
query memory    : 512 MB × max_concurrent=16 = 8 GB
goroutines      : 500 MB
Go runtime      : 500 MB
────────────────────────────────────────
steady state    : ≈ 11 GB
worst-case query burst : ≈ 19 GB
```

Pod memory limit: **16 GB** with the disk-backed smart cache
absorbing query spikes; **24 GB** for hot-path workloads. Leave
headroom for resident pmeta bundles on top of this: they are held for
every live partition of every tenant, and at this file count their
size has not been measured
([scale limits](../petabyte-scale-audit.md#pmeta-residency)).

## CPU budget

| Workload | Per-pod CPU |
| --- | --- |
| Idle (manifest refresh only) | 0.1 cores |
| Steady ingest (1 GB/s into LH cold) | 1-2 cores per pod |
| Concurrent wildcard scans (last 24 h) | 2-4 cores per pod |
| Compaction-heavy partition rewrite | 2-3 cores burst |

Set CPU `requests` at the steady-ingest baseline and `limits` at
2-3× that for headroom. CPU throttling during compaction shows up
as `lakehouse_compaction_partitions_in_flight` plateauing.

## PVC (persistent disk) sizing

Per-pod PVC holds:

```
+ Manifest snapshot                  ≈ 100 B × file_count
+ Footer cache key list              ≈ one length-prefixed object key per cached footer
                                       (keys only — footers are re-fetched from S3
                                       asynchronously after /ready)
+ Insert buffer (insert.buffer_dir)  ≈ see "Insert buffer volume" below; insert pods only
+ Smart cache L2 (disk)              ≈ cfg.cache.disk_max_mb MiB
+ Tombstones                         ≈ negligible unless heavy delete traffic
+ Lifecycle / readiness state        ≈ < 10 MiB
```

### Worked example — PB-scale

```
manifest snapshot     : 100 B × 5M    = 500 MB  (estimate)
footer cache key list : 200k keys     < 40 MB   (code sizes 1 M keys at well under 200 MiB)
insert buffer         : see the formula below (insert pods only)
smart cache L2        : 100 GB (cfg.cache.disk_max_mb)
────────────────────────────────────────
PVC size              ≈ 105-130 GB
```

Recommend **PVC = 1.5× the working-set L2 cache** for headroom on
compaction temp files and log rotation. The `data` PVC in the helm
chart defaults to 50 Gi; bump to 200 Gi for PB-scale.

### Insert buffer volume

The insert buffer (`insert.buffer_dir`) holds every acknowledged row until the
segment holding it is written to S3, plus the committed-segment grace. It lives on
the insert StatefulSet's PVC (`logs.insert.persistence` / `traces.insert.persistence`,
on by default); select pods hold no buffer and need no volume for it.

```
buffer disk ≈ ingest_bytes_per_s × (buffer_flush_interval + drain time + grace)
                / upstream_compression_ratio
            + ingest_bytes_per_s × outage_seconds / upstream_compression_ratio
            + 1 GiB free-space floor
```

`grace = 2 × manifest.refresh_interval + 30 s`. With the defaults
(`buffer_flush_interval` 5m, `manifest.refresh_interval` 5m) a segment is held for
about 7.5 minutes of ingest in steady state; during an object-store outage sealed
segments accumulate for as long as it lasts. The last term is the margin below
which upstream makes the volume read-only and inserts get 429.

Measured on synthetic data (the compression ratio depends on the data; measure
yours): upstream's on-disk compression is about 13-15x for logs and about 5x for
traces. At 1 TB/day per pod that is about **0.4 GB steady plus 3.1 GB per outage
hour for logs** and about 2.7 times that for traces. The 50 Gi default therefore
absorbs about **15 hours (logs) or 5.5 hours (traces) of object-store outage at
1 TB/day per pod**, not counting the cache and other data sharing the volume. Scale
the volume with the outage you want to ride out; alert on
`lakehouse_buffer_oldest_pending_age_seconds` and on
`lakehouse_insert_rejected_total{reason="read_only"}`.

## Peer count

The single most leveraged knob for restart resilience.

| Peers | Restart resilience | Cold-buffer window |
| ---: | --- | --- |
| 1 | none — every restart is a cluster outage | full warmup time |
| 2 | rolling restart works if `maxUnavailable: 1` | invisible on rolling |
| 3-5 | tolerates one peer failure mid-deploy | invisible on rolling |
| 6-10 | survives bad nodes; peer fan-out covers partial gaps | invisible |
| 10+ | over-provisioned for resilience; diminishing returns | invisible |

**Minimum for production: 2.** BufferBridge needs at least one
peer with a warm buffer when this pod restarts. With 1 peer
(only this pod), every restart is a cluster-wide cold-buffer
window (see scaling-restart-scenarios.md scenario 2).

## Config knobs by scale

This release does not read `startup.min_manifest_files`, `startup.serve_while_warming`
or the `cache.warmup_*` keys from the config file. The warmup
settings have flags (`-lakehouse.cache.warmup-partitions`,
`-lakehouse.cache.warmup-max-files`); the other two have none and stay at their
defaults. The examples keep them, marked, to show the intended values.

Dev / CI:

```yaml
lakehouse:
  startup:
    min_manifest_files: 0  # not read from the config file in this release
    serve_while_warming: false  # not read from the config file in this release
  cache:
    footer_max_bytes: 268435456  # 256 MiB, the logs default
    warmup_partitions: 6  # not read from the config file in this release
```

# Small prod (10k files, 2-3 peers)
startup:
  min_manifest_files: 1000  # not read from the config file in this release
  serve_while_warming: true  # not read from the config file in this release
cache:
  footer_max_bytes: 268435456  # 256 MiB, the logs default
  warmup_partitions: 6  # not read from the config file in this release
manifest:
  refresh_interval: 30s
  persist_interval: 5m
query:
  max_concurrent: 8  # default 32; matches the small worked example above

```yaml
lakehouse:
  startup:
    min_manifest_files: 1000  # not read from the config file in this release
    serve_while_warming: true  # not read from the config file in this release
  cache:
    footer_max_bytes: 268435456  # 256 MiB, the logs default
    warmup_partitions: 6  # not read from the config file in this release
  manifest:
    refresh_interval: 30s
    persist_interval: 5m
```

# Large / PB-scale (1M+ files, 6-10 peers)
startup:
  min_manifest_files: 100000  # not read from the config file in this release
  serve_while_warming: true  # not read from the config file in this release
  max_warmup_time: 10m
cache:
  footer_max_bytes: 8589934592  # 8 GiB (traces pod)
  warmup_partitions: 24  # not read from the config file in this release
  warmup_max_files: 5000  # not read from the config file in this release
  memory_mb: 1024
  disk_max_mb: 102400  # 100 GB L2
manifest:
  refresh_interval: 30s  # every refresh re-lists every key (code default 5m);
                         # weigh against LIST cost — see scale limits
  persist_interval: 2m
shutdown:
  persist_timeout: 60s
query:
  max_concurrent: 16  # default 32; matches the PB-scale worked example above
```

## What scales linearly vs sub-linearly

| Resource | Scales with | Linearly? |
| --- | --- | --- |
| Manifest memory | file_count, on every replica | yes |
| Footer cache | footer_max_bytes (both binaries; byte-bounded) | no — bounded by the budget; hit rate falls as files grow |
| Resident pmeta bundles | live partitions × tenants | yes — live partitions are never evicted |
| Buffer restore time | buffer parts on disk / restore rate | yes |
| S3 LIST during refresh | objects under the enumerated prefixes, per replica, per interval | yes — every refresh re-lists every key; pages within a prefix are sequential, at most 8 tenant prefixes in parallel, 2-minute timeout |
| Compaction selection, retention, size-stats recompute | file_count | yes — each starts from a full copy of the manifest |
| Query latency (wide window) | file_count visited | sub-linearly with bloom + footer cache |
| Restart `/ready` time | snapshot size + startup manifest enumeration (full LIST, ≤ 5 min) + pmeta bundle GETs | yes for the snapshot decode, the enumeration and the bundle GETs; the footer-cache reload runs async after `/ready=200` |
| Cold storage size | bytes ingested × compression_ratio | sub-linearly — progressive compaction schedule reduces L1+ files ~25%, L2+ files another ~10% |

## What does NOT scale (gotchas)

- **`query.max_concurrent` × `query.max_live_bytes`** is the
  worst-case memory burst from queries. The defaults (32 × 512 MiB
  = 16 GiB) apply regardless of cluster size; on a small pod lower
  `max_concurrent`, or it will OOM-kill before the disk fills.

- **Every manifest refresh is a full enumeration.** First-ever boot
  and every periodic refresh list every key under the tenant
  prefixes (or the single configured prefix): pages within a prefix
  are sequential, at most 8 tenant
  prefixes run in parallel, and the refresh must finish within
  5 minutes at startup and 2 minutes per periodic cycle, or it fails
  and the manifest keeps its previous contents. How long a PB-scale
  enumeration takes is **not measured**. The `MinManifestFiles` gate
  keeps the pod out of rotation until the manifest crosses the
  threshold — without the gate /ready lies. See
  [scale limits](../petabyte-scale-audit.md#manifest-refresh).

- **The footer cache is a fixed byte budget.** Both binaries hold at
  most `cache.footer_max_bytes` of footers (default 256 MiB logs,
  512 MiB traces); wide queries over more files than fit pay the
  footer fetch (one to three S3 round trips) for the rest.

- **Resident pmeta has no eviction for live partitions.** It grows
  with retention × tenants until retention expires a partition;
  alert on `lakehouse_catalog_resident_bytes`.

- **Simultaneous restart** of all peers cannot be made invisible —
  scaling out doesn't help when every peer's buffer is empty.
  Stagger restarts — the chart deploys a StatefulSet and sets no
  `updateStrategy`, so the Kubernetes default rolling update
  replaces one pod at a time.

## Metrics for capacity planning

```
# Steady-state working set
lakehouse_manifest_files
lakehouse_footer_cache_entries
lakehouse_catalog_resident_bytes
lakehouse_cache_bytes_used
lakehouse_cache_disk_bytes

# Query workload
lakehouse_query_duration_seconds (p50, p95, p99)
lakehouse_concurrent_select_current

# Memory pressure
process_resident_memory_bytes
lakehouse_cache_evictions_total

# Restart cost
lakehouse_startup_total_seconds
lakehouse_manifest_snapshot_age_seconds

# Throughput
lakehouse_insert_rows_total
lakehouse_insert_bytes_uploaded_total
```

Set alerts on `process_resident_memory_bytes > 0.85 × limit`
and `lakehouse_manifest_snapshot_age_seconds > 6× persist_interval`.
