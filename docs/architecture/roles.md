---
title: Roles and what writes to S3
sidebar_position: 7
---

# Roles and what writes to S3

Both binaries run in one of three roles (`--lakehouse.role`):

| Role | Accepts writes | Reads | Writes to S3 or rewrites objects in the background |
|---|---|---|---|
| `insert` | yes | the rows of its own buffer | **yes** |
| `select` | no | S3, plus the insert pods' unflushed rows | **never** |
| `all` (default) | yes | both | **yes** |

**A select pod never writes to the bucket, never compacts, never rewrites and never deletes objects in the background.** It does not matter what the config says: a select pod with `compaction.enabled: true` (the default, and what a shared config file gives it) starts no compaction scheduler and no orphan sweep, and logs one INFO line per skipped job, for example `compaction and the orphan sweep not started: role=select only reads, and only the roles all and insert run background maintenance`.

The rule has one implementation: `config.RunsMaintenance()` is true for the roles `all` and `insert`, and every background job below is gated on it.

## Why

Compaction merges small objects into one, uploads the result and deletes the sources. Two pods that do this over the same partition each upload their own output (the output key is random per pod) before either deletes the sources, and the rows are stored once per pod. Ownership by highest-random-weight hashing only separates the pods that see each other in the peer ring; a select pod that was configured with compaction on joined the ring and compacted the partitions it owned (issue 311: every row stored two to four times in the bucket). Retention on a select pod would delete objects the insert pods still serve.

## Which jobs are gated

Both binaries (`cmd/lakehouse-logs/main.go` and `lakehouse-traces/main.go`).

| Job | Writes | Where | Role gate |
|---|---|---|---|
| Compaction scheduler and its Tier A/B orphan sweep | PUT merged objects, DELETE sources | `setupCompaction` | gated |
| Retention (drops expired objects and their pmeta bundles) | DELETE | `startRetention` | gated |
| Stats snapshot (periodic and at shutdown) | PUT `stats/snapshot.json` | `startStatsLoops`, `runShutdown` | gated |
| Stats-aggregate sidecar | PUT `_meta/stats-aggregate.json` | `saveStatsAggregate` | gated |
| One-time legacy pmeta bundle cleanup | DELETE bundles, PUT marker | `cleanupLegacyPmeta` | gated |
| Tombstone retention GC (`delete.Options.Maintenance`) | DELETE the keys of tombstones whose data is past retention | `attachTombstones` | gated: set from `cfg.RunsMaintenance()`; a select pod never deletes |
| Label-index persist to S3 (traces binary) | PUT | `Storage.Close`, manifest refresh | gated |
| pmeta bundle persist on each flush and at close | PUT bundles | buffer flusher, `Storage.Close` | by construction: only the insert-side writer owns a catalog observer |
| Insert buffer flusher, segment markers | PUT objects and markers | insert wiring | by construction: select has no writer |

Not gated, because they are driven by one request and are safe from any pod:

| Write | Why it stays on select |
|---|---|
| Tombstone keys (`internal/delete`) | Created when a delete or an un-delete request arrives (upstream's delete API runs on select pods): the definition and, for a hide delete, the undo key, each with `If-None-Match` and never overwritten, so two pods can never clobber each other. A failed create is an error to the caller and nothing is queued. |
| Tombstone sync (`startTombstoneSync`) | Runs on every node, read-only: lists the keys created since the last look and reads the new ones, so a delete taken on any pod is enforced on every pod within one poll interval (one minute). The first sync is part of a select pod's readiness. |
| Tenant alias registry (`internal/tenant/registry.go`) | Allocated when a request carries a new org id; conditional PUT with `If-Match`, so concurrent pods never overwrite each other. |
| `POST /lakehouse/api/v1/admin/tenant/migrate` | An explicit, authenticated operator action. |
| `POST /lakehouse/compaction/recompact`, `/lakehouse/drain` | They need the scheduler; **on a select pod they now answer 503** ("compaction not enabled"), as they do on any pod with compaction off. |

Read-only jobs stay on select: manifest refresh, peer discovery, the caches, stats gossip over HTTP, and loading the stats snapshot and the aggregate sidecar at start.

A test pins the list: `TestMaintenanceInventory_EveryS3WriterIsGated` fails when a function in the main package starts a bucket-mutating job without checking the role and without an entry (with its reason) in the test's list.

## Behaviour with several pods

- **Several insert pods, several select pods.** Only the insert (or `all`) pods compact. The ownership ring is the writer pods alone, resolved from `select.insert_headless_service` (the chart sets it for every pod of a signal, because insert and select pods share one config), so a select pod that shares the peer ring owns no partition and adding or removing select pods does not change who compacts. Without that key the ring is the whole peer ring, a warning is logged, and the partitions a select pod would own are never compacted (measured on 672 partitions with one insert pod: the insert pod owns 34%, 27%, 21% and 11% of them with 2, 3, 4 and 8 select pods in the ring, so 66% to 89% would never be compacted). Compaction is deferred, not reassigned, for the stabilization window after any ring change. No select pod issues a PUT or DELETE for maintenance.
- **S3 requests.** Writers: unchanged. Select pods: the maintenance PUT, COPY and DELETE requests stop; reads are unchanged.
- **Deletes taken by a select pod.** The tombstone is created in S3 before the call returns and hides the rows on that pod at once. Every other pod, writer or select, lists the new keys every minute and enforces the delete from then on, so a delete or an un-delete taken on any pod reaches all of them within one interval, with no restart. A pod that cannot refresh for three minutes answers reads with `503` instead of showing rows that may be deleted. Single-role `all` deployments are unaffected. See [Deletion strategy](../deletion-strategy.md).

## How a pod finds itself in the ring

A pod listens on `:9428` (`:10428` for traces) and service discovery lists the pods behind a headless service as `IP:port` (A records) or `hostname:port` (SRV records of a StatefulSet, or dashed-IP names such as `10-1-2-3.svc...`). The two never compare equal, and compaction ownership and the peer cache both need to know which entry is this pod. They ask `internal/selfaddr`, which accepts a member when its port is the listen port and one of these holds:

1. the member is the listen address exactly (a pod alone in its ring);
2. the member's host is an IP literal that is one of this pod's addresses (IPv4 or IPv6, zones stripped): `POD_IP`, `POD_IPS` and the interface addresses, loopback excluded;
3. the first DNS label of the member's host is the first label of this pod's hostname (`POD_NAME`, then `os.Hostname()`): the StatefulSet SRV form `lh-logs-insert-0.<svc>.<ns>.svc.cluster.local`;
4. the first label is a dashed IP of this pod (`10-1-2-3`, `fd00--5`);
5. only when none of 1 to 4 matched anything: the member's hostname resolves, from a cache refreshed in the background (2 s timeout, at most 8 lookups in flight, reused for a minute), to one of this pod's addresses.

Exactly one matching member is this pod; none or several leave it unresolved. Interface addresses and hostnames are re-read once a minute, a decision for an unchanged list is reused for 5 s, and no step touches the network on the query or ownership path.

**An unresolved pod owns nothing.** With a non-empty discovered ring and no single entry for itself, a pod compacts no partition, runs no orphan sweep and never adds its listen address to the list as an extra member (adding it is what left partitions without an owner or with several). In the peer cache an unresolved pod keeps asking its peers for every key, including its own. A pod not yet in DNS owns nothing and the other writers cover its share. The pod logs why, once per change. A pod that knows no peers while discovery is configured (DNS failed, the headless service lists nobody yet) owns nothing either, and `lakehouse_ring_self_resolved{ring="compaction"}` reads 0: falling back to "alone" would make every pod of a fleet own every partition during a DNS outage. A pod with no discovery configured at all is alone in its ring and owns everything.

| Metric | Meaning |
|---|---|
| `lakehouse_ring_self_resolved{ring}` | 1 when exactly one entry is this pod, 0 when none or several match. `ring` is `compaction` or `peercache`. |
| `lakehouse_ring_self_in_peers{ring}` | 1 when that entry is also in the set the ring hashes over; 0 for a pod that resolved but is draining, and 0 whenever it did not resolve. |

Alert `LakehouseNoWriterInCompactionRing` (critical) fires when every ready writer has read 0 for 10 minutes, that is when nothing is being compacted. Alert `LakehouseRingSelfUnresolved` fires when `lakehouse_ring_self_resolved{ring="compaction"}` is 0 on a ready pod for 5 minutes (not for the peer-cache ring: a pod that is not behind the peer headless service, for example an insert pod when that service is the select headless service, is correctly not in it and reads 0 there). What to set: the chart injects `POD_NAME`, `POD_IP` and `POD_IPS` through the downward API; a hand-written manifest should do the same, and the headless service must list the pod on its listen port.

**What a pod that finds itself changes for the cache.** Keys the pod owns are no longer requested from the pod itself over HTTP. In the smart cache (roles `all` and `select`) the pod reads them from its own disk cache and S3 and stores them on its disk cache; before, a pod that did not find itself never stored a key there. Consequences, from the defaults (sourced, not measured on a real volume): the L2 cache fills up to `cache.disk_limit` (50GB) at its eviction watermark (0.8, about 40 GB), and on a `role=all` pod that disk is the same 50Gi volume as the insert buffer (`/data/lakehouse`), so size the volume for both or lower `cache.disk_limit`; S3 GETs for repeated reads of owned keys fall. Startup cache warmup (opt-in, `cache.warmup_partitions`) warms the files a pod owns; a pod that did not find itself warmed none. Keys the pod does not own still go to their owner over HTTP.

Measured on 672 partitions (8 tenants x 84 days), each of W writers listening on `:9428`, discovery listing them as StatefulSet hostnames, IPs, dashed-IP names or IPv6 (the scheduler's ownership only, `TestOwnership_EveryForm_ExactlyOneOwner`): before, 48 to 52% of the partitions had no owner with one writer, and with 2, 3 and 8 writers 66%, 75% and 88% had none while 32%, 25% and 12% were owned by every writer at once; after, 0% and 0% for every form and W.

## Known gaps

- Several insert pods that see different peers (a ring change, DNS lag) can still compact the same partition (the rule that prevents it is a separate piece of work, tracked in issue 311). This page and the role gate make sure select pods are not among them.
- The orphan sweep's Tier A lets the secondary owner of a partition take it when the secondary has no attempt time for it, and attempt times are kept per pod, so with a healthy, agreeing ring the secondary can still merge a partition the primary is merging. This was already the case before the self-address fix (it ran on every writer for the partitions the listen address ranked second); it is not changed here. Tracked in [#602](https://github.com/ReliablyObserve/victoria-lakehouse/issues/602).
- Physical removal of deleted rows is not enabled: every delete mode hides the rows at query time, and the multi-writer rewrite is planned together with the compaction work that makes concurrent rewrites safe ([Deletion strategy](../deletion-strategy.md#what-the-modes-do-today)).
- The stats snapshot is written by writer pods from their registry. A select pod's query counters (`last_query_at`) reach it only through stats gossip, so without peer gossip a select pod's last-query time is not persisted.
- Other places that compare the listen address with discovered addresses were not changed: [#603](https://github.com/ReliablyObserve/victoria-lakehouse/issues/603).
- Availability zones do not take part in compaction ownership. Every pod hashes over the whole writer list, so a partition has one scheduler owner whatever zones the pods run in and whether `peer.az_aware` is on. Compaction reads and writes S3 only, so a pod's zone gains nothing for it. (An earlier per-zone subset gave every partition one owner per zone once pods found themselves in the ring; it is removed, with a test over 2 zones x 2 writers and 3 zones x 1 writer.)
- Found while reviewing the self-address fix, not changed by it: the peer cache handler never serves the smart-cache disk cache, so on select pods a fetch from a peer always misses ([#605](https://github.com/ReliablyObserve/victoria-lakehouse/issues/605)); its in-memory map has no bound ([#606](https://github.com/ReliablyObserve/victoria-lakehouse/issues/606)); an insert-only chart release with more than one replica sets no `select.insert_headless_service`, so every pod owns every partition ([#607](https://github.com/ReliablyObserve/victoria-lakehouse/issues/607)); `applySelfFilter` has no production caller ([#608](https://github.com/ReliablyObserve/victoria-lakehouse/issues/608)); a pod that is correctly absent from a ring re-resolves that ring's hostnames in DNS every minute ([#609](https://github.com/ReliablyObserve/victoria-lakehouse/issues/609)).
- A dual-stack headless service can list one pod twice (its IPv4 and its IPv6 address, both on the listen port). Both entries match the pod, so it is reported as ambiguous and owns nothing in that ring (`lakehouse_ring_self_resolved` 0, alert `LakehouseRingSelfUnresolved`); list one address family, or use the StatefulSet SRV form. Not tested on a real dual-stack cluster.
- Not verified against a real cluster: the exact SRV answers of CoreDNS for a StatefulSet headless service, and its dashed IPv6 pod names. The rules above accept the documented forms (hostname first label, dashed IPv4, dashed IPv6 in the compressed and expanded forms); a form they do not accept leaves the pod unresolved, which is reported by the gauge and the alert.
- A select pod keeps the pmeta entries of files that other pods compacted or expired until it restarts (RAM only, no effect on answers; [#595](https://github.com/ReliablyObserve/victoria-lakehouse/issues/595)).

## Tests

`cmd/lakehouse-logs/maintenance_gate_test.go` and `lakehouse-traces/maintenance_gate_test.go`:
- a table over the roles and the jobs (a select pod starts none, `insert` and `all` start all of them);
- the real main-package start-up functions for `role=select` with every job enabled: no scheduler or sweep, and not one PUT, COPY or DELETE reaches the bucket (the same scenario on an insert pod does write, so the silence is the gate);
- two starts of a select pod (a restart), with the start-up warm-ups, a query and a close: every request is counted and none is a PUT, COPY or DELETE;
- one insert pod and two select pods over one bucket with compaction enabled on all three: only the insert pod writes, the bucket holds every row exactly once (before the gate it held every row three times), and a query on each select pod returns every row once;
- a delete taken on a select pod is enforced on the other select pod and on both writers within one poll interval, and every PUT under `_tombstones/` carries `If-None-Match` (`tombstone_multiinstance_test.go`).
