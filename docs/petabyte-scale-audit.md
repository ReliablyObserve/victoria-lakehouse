---
title: Scale limits and roadmap
sidebar_label: Scale limits
---

# Scale limits and roadmap

What scales in the Lakehouse cold tier today, where each component's practical
ceiling is, what breaks past that ceiling, and what the planned change is.

Every claim on this page points at a code path in this repository. Where a number
is an arithmetic projection rather than an observation it says so, and where
nothing has been measured it says **not measured** instead of guessing.

## Reference workload

The terms below are quoted against one deployment shape so they are comparable:

- ~5 TB/day ingested, ~5 PB at rest, 30-day hot / 36-month cold split
- ~150 k new objects/day, ~50 M live objects at steady state
- hourly partitions (`dt=YYYY-MM-DD/hour=HH`), tenant-prefixed keys
- several replicas per signal, logs and traces as separate deployments

| Term | Meaning |
| --- | --- |
| `F` | live files tracked by the manifest |
| `P` | live partitions (hour × day) |
| `T` | tenants |
| `R` | replicas |
| `N` | S3 objects under the enumerated prefixes |

The byte axis is not the problem. Parquet bodies, range reads, the disk cache and
the query executor all scale with bytes touched, and bytes touched is bounded by
partition pruning, footer statistics and blooms. **The terms that do not scale yet
are `F`, `P × T` and `R` in the metadata plane.**

## Summary

| Component | Scales with | Where it stands today | Failure mode past that | Planned change |
| --- | --- | --- | --- | --- |
| Manifest RAM | `O(F)` on each of `R` replicas | full file map resident on every replica; ~200 B/file *(estimate, not measured)* → ~1 GB/pod at 5 M files | per-pod RAM and snapshot decode grow with the whole corpus, not the query window | per-partition index derived from the pmeta file-meta facet, loaded by window |
| Whole-manifest walks | `O(F)` per tick, run or request | `AllFiles()` deep-copies every entry; called on every compaction tick, retention run, refresh, trace-id lookup and two stats API requests | background loops and admin requests cost the whole corpus, however little they act on | partition-scoped iterators and incremental aggregates, as already done for tenant summaries |
| Manifest refresh | `O(N)` per interval per replica | full re-enumeration of every tenant/signal prefix each `manifest.refresh_interval` (default 5 min), under a hard 2-minute timeout; no time window | LIST pages `≈ N/1000` per replica per refresh (~50 k at 50 M objects); once a refresh cannot finish in 2 minutes it fails every cycle and the manifest stops picking up files that peer push missed | poll only partitions newer than the snapshot; make push the primary path; periodic full reconcile |
| pmeta residency | `O(P × T)` | every live partition's bundle stays resident; eviction only when a partition fully expires | metadata RAM and warm GETs grow with partitions × tenants | lazy load by window, age-LRU under `resourcebounds`, day-level bundles after rollup |
| pmeta multi-writer | writers per partition | one bundle key per partition, written with an unconditional `PutObject` | concurrent writers replace each other's bundle — last write wins | per-peer shard keys, merge-on-read, owner-side consolidation |
| Footer cache | files in the query window × `T` | **logs: fixed at 10 000 entries, `cache.footer_max_items` ignored**; traces: configurable, auto-retuned within [10 000, 100 000] | the query working set is evicted by background scans; the logs cache cannot be sized up at all | wire logs to the traces path; fold into the file-meta facet as its page cache |
| Label index | distinct values per field | 10 000 values/field cap with truncation; field-name LRU exists but is off by default | high-cardinality fields keep an arbitrary subset of values | retire into the pmeta field catalog facet |
| Compaction throughput | partitions retired per pod-hour | `compaction.max_concurrent: 1`, `compaction.interval: 5m` → ≤ 12 partitions/h/pod; inputs fully materialised in memory | when new (tenant, partition) work outpaces what the scheduler retires, the small-file backlog grows without bound | size-targeted flush, streaming k-way merge under a byte budget, parallel partitions |
| Object size at flush | rows and *raw* bytes | flush at 50 000 buffered rows, or estimated **uncompressed** bytes ≥ `insert.target_file_size` (128 MiB), or the interval | compressed objects land far below the target → request overhead and read amplification on cold queries | compressed-size target + minimum-age hold; one object per (tenant, partition) per flush |
| Trace-id lookup | `O(F)` | the fast path reads the footer of every live file; a miss then falls through to a span scan | a lookup for an absent or unindexed id pays a footer read per live file *and* the scan | resolve the window first; day-level id sketch in the pmeta bundle |
| Retention | `O(F)` per run | per-file TTL evaluation over the whole manifest each run | run cost tracks the corpus, not what is expiring | partition-level drop |
| Cold start | `O(F)` decode + `O(P × T)` GETs | streaming snapshot decode; one bundle GET per live partition | boot time grows with the corpus; a simultaneous restart of all peers cannot be hidden | lazy pmeta by window; snapshot + push instead of full enumeration |

## What changed since the previous revision of this page

The previous revision listed five must-fix items. Four are fixed and the fifth is
fixed for the only path that uses it. One of its numbers was wrong and another was
never measured; both are corrected here.

| Previous item | State now | Evidence |
| --- | --- | --- |
| Manifest per-key lookups are `O(n)` | **Fixed.** A `byKey` index maps every file key to its partition; `findFileLocked` resolves the partition in `O(1)` and scans only that partition's slice. `SetFileBucket`, `UpdateFileColumnStats` and `EnrichFileMetadata` all use it | `internal/manifest/manifest.go:158`, `:1295` — v0.39.0 |
| `TenantSummaries` full-scans every call | **Fixed.** An incremental `tenantAggregates` cache is maintained on add/remove/enrich and rebuilt on refresh and snapshot load; `TenantSummaries` is `O(T log T)`. The same cache lets `GetFilesForRangeTenant` skip partitions a tenant has no files in | `internal/manifest/manifest.go:186`, `:891`, `:2046` — v0.39.0 |
| `KeysUnderPrefix` is `O(n)` | **Fixed for its caller.** A date-bucketed prefix resolves to at most 24 hourly partitions, and orphan sweep — the only non-test caller — passes exactly that. An empty or non-date prefix still scans everything; nothing in production passes one | `internal/manifest/manifest.go:1503`, `internal/compaction/orphan_sweep.go:340` — v0.39.0 |
| `RefreshFromS3` lists the entire bucket | **Fixed.** With a tenant-prefixed key template the refresh discovers tenant prefixes and enumerates each (narrowed to the binary's own signal) in parallel; the full-bucket LIST is only a fallback. It is still a *full* enumeration per interval — see [Manifest refresh](#manifest-refresh) | `internal/manifest/manifest.go:738` — v0.39.0 |
| Manifest snapshot is JSON | **Fixed.** Binary gob with a magic prefix and legacy-JSON detection; loading streams the decode under a size cap | `internal/manifest/manifest.go:1644`, `:1693` — v0.39.0, streaming decode v0.49.0 |
| Label index reads full Parquet bodies at startup | **Mostly fixed.** Warmup samples at most 10 files; the index is persisted to local disk by both binaries, and the traces binary also keeps a copy in S3. The per-field value cap still truncates | `internal/storage/parquets3/storage.go:1301`, `internal/cache/persist.go:179` |
| Footer cache "fixed at 10 000 entries" | **Fixed in traces only.** Traces honours `cache.footer_max_items` and re-sizes after every refresh. Logs still constructs the cache with a hardcoded 10 000 | `lakehouse-traces/internal/storage/parquets3/storage.go:235`, `:1259` vs `internal/storage/parquets3/storage.go:260` |
| Footer entries are ~5 KB | **Not measured.** The configuration comment assumes ~5 KB, the [sizing guide](operations/sizing.md) budgets ~50 KiB; entry size varies with row-group count and, for traces, the `_trace_idx` key-value | `internal/config/config.go:467` |
| Query fan-out defaults to 8 workers | **Wrong number.** `query.file_workers` defaults to 64 | `internal/config/config.go:1004` |

Also landed since that revision: compaction drops merged-away inputs from pmeta
and retention evicts a fully expired partition's bundle from RAM and deletes it
from S3 (v0.81.0); the legacy metadata sidecar writers (`_file_metadata.json`,
per-file `.bloom`, partition `_bloom.bin`) were deleted and `pmeta.enabled`
defaults on (v0.82.0); bundles are keyed per
tenant-scoped partition (v0.100.0); a pod stays out of rotation until its manifest
crosses `startup.min_manifest_files` (v0.49.0).

## Per-component detail

### Manifest RAM

Every replica holds the full `partition → []FileInfo` map plus the `byKey` index.
Range selection binary-searches the sorted partition list, then sorts the selected
files on every call (`GetFilesForRange`, `internal/manifest/manifest.go:995`).

The ~200 B per file used by the [sizing guide](operations/sizing.md) is an
estimate from the struct shape, **not a measurement**. It puts the manifest at
~1 GB per pod at 5 M files, and at the reference workload an order of magnitude
more — on every replica, whatever window is being queried.

**Planned:** derive a per-partition file index from the pmeta file-meta facet,
load it by query window, and keep file lists pre-sorted so selection does not
re-sort per query.

### Whole-manifest walks

`AllFiles()` (`internal/manifest/manifest.go:1197`) returns a deep copy of every
`FileInfo` in the manifest, taken under the read lock. It is the entry point for
several loops and handlers that only need a slice of the corpus:

| Caller | Cadence | Code |
| --- | --- | --- |
| compaction candidate selection | every compaction tick, every pod | `internal/compaction/scheduler.go:318` |
| retention | every retention run | `internal/retention/retention.go:142` |
| size-stats recompute | every manifest refresh, both binaries | `cmd/lakehouse-logs/main.go:1538`, `lakehouse-traces/main.go:1547` |
| trace-id fast path | every trace-by-id lookup | `lakehouse-traces/internal/storage/parquets3/trace_index_lookup.go:61` |
| tenant detail API | every request for a tenant that has data | `internal/stats/api.go:600` |
| storage-class breakdown API | every request | `internal/stats/api.go:957` |

Each of these costs `O(F)` time and a transient `O(F)` allocation regardless of
how much it acts on. This is the same shape the previous revision of this page
flagged for `TenantSummaries`, and the same fix applies.

**Planned:** partition-scoped iterators that visit only the partitions a caller
needs, and incremental aggregates for the statistics that are currently
recomputed from scratch.

### Manifest refresh

`RefreshFromS3` (`internal/manifest/manifest.go:738`) discovers tenant prefixes
and enumerates each one, narrowed to the binary's own signal, with bounded
parallelism; with a single-prefix key template it enumerates that prefix. Either
way it enumerates **every key**: there is no time window and no "newer than the
snapshot" filter. Every replica does this every `manifest.refresh_interval`
(code default 5 minutes, `internal/config/config.go:959`; the sizing guide's
profiles set 30 s). After each refresh both binaries also recompute the size-stats
aggregate over every file (`cmd/lakehouse-logs/main.go:1538`,
`lakehouse-traces/main.go:1547`).

The request cost is arithmetic, not measured. LIST returns up to 1 000 keys per
page, so `N` objects cost about `N/1000` pages per replica per refresh. At 50 M
objects and 6 replicas:

| `refresh_interval` | LIST requests/day | at $0.005 per 1 000 |
| --- | ---: | ---: |
| 5 min (code default) | ~86 M | ~$430/day |
| 30 s (sizing guide profiles) | ~860 M | ~$4 300/day |

There is also a hard time ceiling. The periodic refresh runs under a 2-minute
context timeout (`cmd/lakehouse-logs/main.go:1524`, `lakehouse-traces/main.go:1533`;
the startup refresh gets 5 minutes, `cmd/lakehouse-logs/main.go:1412`). At most 8
tenant prefixes are enumerated in parallel (`tenantRefreshMaxParallel`,
`internal/manifest/manifest.go:48`), and pages within one prefix are fetched
sequentially, because each LIST page needs the previous page's continuation token.
A refresh that does not finish in time returns an error and changes nothing: the
manifest keeps its previous contents and the next tick starts over from the first
page.

The arithmetic for where that bites, not a measurement: 50 k pages in 120 s over 8
streams needs each page round-trip to take ≤ ~19 ms, and a single tenant holding
most of the corpus gets only one stream. Past that point, files that peer push did
not deliver are never discovered by polling. Real S3 page latency at this scale is
**not measured**.

A peer-to-peer manifest push already exists (`internal/manifest/push.go`), but it
supplements polling rather than replacing it. `manifest.sqs_queue_url` is declared
in the configuration but nothing consumes it yet.

**Planned:** make change notification the primary path, restrict polling to
partitions newer than the loaded snapshot, keep a periodic full reconcile for
correctness, and use S3 Inventory for the cold range.

### pmeta residency

One `_pmeta.bundle` per tenant-scoped partition holds the field catalog,
cardinality sketches, per-file metadata and bloom facets. The resident model and a
live measurement — 7.1 MB resident for 1 109 files / 1.88 GB — are on the
[PB-scale resources](architecture/pb-scale-resources-pmeta.md) page.

Two things bound residency today, and one does not:

- **Compaction** removes merged-away inputs' facet entries.
- **Retention** removes expired files' entries, and when a partition empties out
  of the manifest its bundle is evicted from RAM and its S3 object deleted
  (`PmetaOnFileExpired`, `internal/storage/parquets3/pmeta_wire.go:478`).
- **Nothing** evicts an old but still-live partition. `Store` offers `Remove` and
  `RemoveFiles` (`internal/pmeta/store.go:235`, `:260`); the per-bundle byte
  estimate is used for reporting (`lakehouse_catalog_resident_bytes`), not by
  any eviction policy.

So resident metadata scales with `P × T`: every partition inside retention keeps
its bundle in RAM, for every tenant with data in it. The extrapolation on the
PB-scale resources page is stated for a single tenant; multiply by the number of
active tenants per partition for a multi-tenant fleet.

Guardrails that exist today: `lakehouse_catalog_resident_bytes` to alert on, and
`pmeta.cardinality_threshold` (effective default 50 000,
`internal/storage/parquets3/pmeta_wire.go:59`), past which a field stops storing
values in the catalog.

**Planned:** lazy load by query window, an age-LRU under `resourcebounds`, a
compressed payload, and day-level bundles once a partition is rolled up.

### pmeta multi-writer safety

Dirty bundles are persisted on the flush cycle with a plain `PutObject` to one key
per partition — no conditional write, ETag precondition or shard suffix
(`internal/pmeta/persist.go:57`). When two replicas persist the same partition,
the later PUT replaces the earlier object wholesale instead of merging into it, so
the persisted bundle no longer carries what only the earlier writer contributed.
Warm-time self-heal rebuilds bundles that are missing or corrupt, not ones that are
valid but incomplete.

HRW ownership already gives each partition a single compaction owner; the exposed
path is insert replicas flushing into the same hour.

**Planned:** per-peer shard keys, merge-on-read across shards, and consolidation
into one object by the partition's owner during compaction.

### Footer cache

The traces binary passes `cache.footer_max_items` to the cache and re-sizes it
after every successful refresh; left at zero it auto-tunes to 0.05 % of the
manifest's file count, clamped to [10 000, 100 000]
(`lakehouse-traces/internal/storage/parquets3/storage.go:1259`).

**The logs binary does not.** It calls `NewFooterCache(10000)` unconditionally
(`internal/storage/parquets3/storage.go:260`), so on logs `cache.footer_max_items`
only feeds a startup hint that recommends a larger value
(`internal/startup/hints.go:65`). Raising it has no effect on the logs cache.

Entry size is **not measured** and is not a constant: it tracks row-group count,
and for traces the embedded `_trace_idx` key-value can dominate. Size against the
pessimistic ~50 KiB the sizing guide uses until it is measured.

**Planned:** wire the logs binary to the same configurable, auto-retuned path;
then make the cache the file-meta facet's page cache, with per-tenant fairness.

### Label index

Values are capped at 10 000 per field and the excess is dropped rather than ranked
(`internal/cache/persist.go:179`, `:199`). Field names have an LRU, but
`cache.label_index_max_fields` defaults to 0, which leaves the number of fields
unbounded. Warmup samples at most 10 files spread across the range
(`internal/storage/parquets3/storage.go:1301`), and the index is persisted so a
restart does not re-read bodies.

Past the value cap, a high-cardinality field keeps an arbitrary subset — this
surfaces as incomplete values, not as a slowdown.

**Planned:** retire the index into the pmeta field catalog facet, which already
tracks per-field cardinality exactly or as a sketch.

### Compaction throughput and memory

Compaction defaults to `max_concurrent: 1` and `interval: 5m`
(`internal/config/config.go:1058`), so a pod retires at most 12 partitions per
hour, and only the partitions HRW assigns to it. Each tick starts from a full copy
of the manifest (see [Whole-manifest walks](#whole-manifest-walks)). Merging reads every input
object's bytes and decodes all of their rows into memory before writing the output
(`mergeLogFiles`, `internal/compaction/compactor.go:458`).

At the reference workload what matters is the balance, not any single number: the
system is stable only while partitions retired per pod-hour, summed over pods,
exceeds the rate at which new (tenant, partition) work appears. Below that line
the small-file population grows monotonically, and every cold query pays for it
in requests.

**Planned:** a size target at flush, a streaming k-way merge bounded by a byte
budget instead of whole-input materialisation, several partitions in parallel, an
atomic manifest swap, and tombstone and bloom rebuild in the same pass.

### Object size at flush

`checkSizeThreshold` (`internal/storage/parquets3/writer.go:265`) flushes when
buffered rows reach `insert.max_buffer_rows` (default 50 000,
`internal/config/config.go:1016`), or when a partition's **estimated raw bytes**
reach `insert.target_file_size` (default 128 MiB, `internal/config/config.go:283`),
plus the periodic flush interval. Because the byte trigger compares *uncompressed*
bytes, and the buffer fans out per (tenant, partition), written objects are far
smaller than the 128 MiB the setting suggests.

The resulting object size distribution is **not measured** in this repository.
The direction is clear — more, smaller objects than intended — and the cost is
per-file request overhead and read amplification on cold queries.

**Planned:** target *compressed* size, hold a partition for a minimum age before
flushing it, and write one object per (tenant, partition) per flush.

### Trace-id lookup

A trace-by-id query first tries the footer index: `LookupTraceIndex`
(`lakehouse-traces/internal/storage/parquets3/trace_index_lookup.go:61`, called
from `lakehouse-traces/internal/vtstorage_adapter/adapter.go:74`) starts from
`manifest.AllFiles()` and reads each file's footer under bounded concurrency,
looking for the id in the `_trace_idx` key-value. There is no time pruning before
the fan-out.

A hit returns the trace's time bounds straight from metadata. A miss is
deliberately not authoritative — the index can lag fresh writes and older objects
may not carry it — so the query falls through to a span scan, whose file set is
narrowed by the same footer key-value while keeping unindexed files
(`filterFilesByTraceIdx`,
`lakehouse-traces/internal/storage/parquets3/storage_query.go:685`;
`lakehouse_trace_idx_prefilter_files_total` reports dropped, matched and kept
files). A lookup for an absent id therefore costs a footer read per live file
*and* the scan.

**Planned:** resolve the time window first and fan out only inside it, backed by a
day-level trace-id sketch carried in the pmeta bundle.

### Retention

`RunOnce` (`internal/retention/retention.go:140`) walks `manifest.AllFiles()` and
evaluates the effective TTL of every file on every run, so a run's cost tracks the
size of the corpus rather than the amount actually expiring.

**Planned:** drop at partition granularity, touching only the partitions that
cross the retention boundary.

### Cold start

The manifest snapshot is decoded as a stream under a size cap rather than read
whole (v0.49.0), which removes the peak-RSS spike, but decoding is still `O(F)`.
Warming pmeta costs one GET per live partition with bounded concurrency
(`WarmPartitions`, `internal/pmeta/persist.go:81`), so `O(P × T)`. The footer
cache persists only its key list and re-fetches those footers asynchronously
after the pod is ready. `startup.min_manifest_files` keeps a pod out of rotation
until its manifest is credible.

A simultaneous restart of every peer cannot be hidden by any of this — stagger
restarts (`maxUnavailable: 1`, the chart default).

## What scales today

Verified as bounded, or bounded by an operator setting:

- **Partition-keyed file map.** 30 days × 24 h = 720 partitions, and range
  selection binary-searches them instead of scanning.
- **Footer statistics and blooms narrow before any body read.** A wide query
  reads footers and metadata, not bodies — the load-bearing step.
- **Query fan-out is bounded.** `query.file_workers` (default 64) and the
  `resourcebounds` limits cap per-query concurrency.
- **Compaction work is partition-scoped and HRW-sharded** — each pod merges only
  the partitions it owns (`OwnsPartition`), one at a time by default
  (`internal/compaction/scheduler.go:324`). Candidate *selection* is still `O(F)`; see
  [Whole-manifest walks](#whole-manifest-walks).
- **One metadata object per partition.** The per-partition sidecars and per-file
  bloom objects were folded into `_pmeta.bundle`: warming a partition is one GET
  (`WarmPartitions`) and a flush persists one PUT per dirty partition
  (`PersistDirty`, `internal/pmeta/persist.go:41`).
- **Per-field catalog size is hard-bounded.** Past `pmeta.cardinality_threshold`
  a field stops storing values and is answered by the exact scan path instead.

## Not measured

Listed so the gaps stay visible:

- per-file manifest RAM (the ~200 B figure is a struct-shape estimate)
- footer cache entry size (the two public estimates differ by 10×)
- flushed object size distribution at a realistic tenant fan-out
- pmeta bundle size per partition at scale, and its compressibility
- LIST volume, duration and cost of a refresh against a large bucket
- boot time and warm GET count anywhere near the reference workload
- anything above one replica per signal: CI runs every end-to-end suite against
  a single replica, and the three-instance logs overlay
  (`deployment/docker/docker-compose-cluster.override.yml`) is not run by any CI
  job — every `× R` term on this page is arithmetic

## Order of work

1. **Bound resident metadata** — lazy pmeta load by window plus an age-LRU, so
   per-pod RAM tracks the query window rather than retention.
2. **Make refresh incremental** — push first, polling restricted to partitions
   newer than the snapshot, periodic full reconcile.
3. **Fix the write lifecycle** — compressed-size target at flush, streaming merge
   in compaction, several partitions in parallel.
4. **Make multi-writer metadata safe** — per-peer pmeta shards with merge-on-read
   and owner-side consolidation.
5. **Stop walking the whole manifest** — prune by window before the trace-id
   fan-out, drop retention by partition, and give compaction selection and the
   stats handlers partition-scoped iterators or incremental aggregates.
6. **Close the small gaps** — wire the logs footer cache to the configurable path;
   retire the label index into the catalog facet.

Items 1, 2 and 6 are largely independent. Item 3 is what makes the per-file
overhead on cold queries go away, and item 4 is a correctness prerequisite for
more than one writer per partition.

## Related

- [Sizing guide](operations/sizing.md) — memory, CPU, PVC and peer count per scale
- [PB-scale resources](architecture/pb-scale-resources-pmeta.md) — measured pmeta
  residency baseline and its extrapolation
- [Performance machinery](performance-machinery.md) — every narrowing and caching
  mechanism in one inventory
- [Scaling](scaling.md) — vertical and horizontal scaling
- [Roadmap](roadmap.md)
