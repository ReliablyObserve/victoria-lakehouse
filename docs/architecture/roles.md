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

Compaction merges small objects into one, uploads the result and deletes the sources. Two pods that do this over the same partition each upload their own output (the output key is random per pod) before either deletes the sources, and the rows are stored once per pod. Ownership by highest-random-weight hashing only separates the pods that see each other in the peer ring; a select pod that was configured with compaction on joined the ring and compacted the partitions it owned (issue 311: every row stored two to four times in the bucket). Retention on a select pod would delete objects the insert pods still serve, and a delete rewrite would replace an object another pod is compacting.

## Which jobs are gated

Both binaries (`cmd/lakehouse-logs/main.go` and `lakehouse-traces/main.go`).

| Job | Writes | Where | Role gate |
|---|---|---|---|
| Compaction scheduler and its Tier A/B orphan sweep | PUT merged objects, DELETE sources | `setupCompaction` | gated |
| Delete rewrite scheduler (rewrites objects without tombstoned rows, deletes the source) | PUT, DELETE | `startDeleteRewrite` | gated |
| Retention (drops expired objects and their pmeta bundles) | DELETE | `startRetention` | gated |
| Stats snapshot (periodic and at shutdown) | PUT `stats/snapshot.json` | `startStatsLoops`, `runShutdown` | gated |
| Stats-aggregate sidecar | PUT `_meta/stats-aggregate.json` | `saveStatsAggregate` | gated |
| One-time legacy pmeta bundle cleanup | DELETE bundles, PUT marker | `cleanupLegacyPmeta` | gated |
| Interrupted-rewrite resolution at start (`resolveInterruptedRewrites`) | Writes tombstone records (blind PUT) | `runStartup` | gated: only a pod that runs rewrites can have interrupted ones; a select pod that resolved a writer's live Prepared record would undo it |
| Label-index persist to S3 (traces binary) | PUT | `Storage.Close`, manifest refresh | gated |
| pmeta bundle persist on each flush and at close | PUT bundles | buffer flusher, `Storage.Close` | by construction: only the insert-side writer owns a catalog observer |
| Insert buffer flusher, segment markers | PUT objects and markers | insert wiring | by construction: select has no writer |

Not gated, because they are driven by one request and are safe from any pod:

| Write | Why it stays on select |
|---|---|
| Tombstone files (`tombstone_persist.go`) | Written when a delete request arrives (upstream's delete API runs on select pods). One object per tombstone id, so two pods never write the same key. |
| Tombstone reload (`startTombstoneReload`) | None: it only reads. It adds a record, merges progress, or drops a removed tombstone in memory | Runs on every node: a select pod has to enforce deletes taken elsewhere. |
| Tombstone write retry (`startTombstoneWriteRetry`) | Runs on every node, once a minute (the rewrite scheduler's own pass runs every verify interval, 6 h by default, and a select pod has none): a select pod takes the delete requests, so a PUT that S3 rejected must still be retried there, or the delete would live on that pod's disk alone. |
| Tenant alias registry (`internal/tenant/registry.go`) | Allocated when a request carries a new org id; conditional PUT with `If-Match`, so concurrent pods never overwrite each other. |
| `POST /lakehouse/api/v1/admin/tenant/migrate` | An explicit, authenticated operator action. |
| `POST /lakehouse/compaction/recompact`, `/lakehouse/drain` | They need the scheduler; **on a select pod they now answer 503** ("compaction not enabled"), as they do on any pod with compaction off. |

Read-only jobs stay on select: manifest refresh, peer discovery, the caches, stats gossip over HTTP, and loading the stats snapshot and the aggregate sidecar at start.

A test pins the list: `TestMaintenanceInventory_EveryS3WriterIsGated` fails when a function in the main package starts a bucket-mutating job without checking the role and without an entry (with its reason) in the test's list.

## Behaviour with several pods

- **Several insert pods, several select pods.** Only the insert (or `all`) pods compact. The ownership ring is the writer pods alone, resolved from `select.insert_headless_service` (the chart sets it for every pod of a signal, because insert and select pods share one config), so a select pod that shares the peer ring owns no partition and adding or removing select pods does not change who compacts. Without that key the ring is the whole peer ring, a warning is logged, and the partitions a select pod would own are never compacted (measured on 672 partitions with one insert pod: the insert pod owns 34%, 27%, 21% and 11% of them with 2, 3, 4 and 8 select pods in the ring, so 66% to 89% would never be compacted). Compaction is deferred, not reassigned, for the stabilization window after any ring change. No select pod issues a PUT or DELETE for maintenance.
- **S3 requests.** Writers: unchanged. Select pods: the maintenance PUT, COPY and DELETE requests stop; reads are unchanged.
- **Deletes taken by a select pod.** The tombstone is written to S3 (and retried until it lands). Every pod re-reads the tombstone objects every `delete.tombstone_reload_interval` (default 1 minute, read-only), so the other select pods enforce the delete at once and the writer that owns each partition rewrites its objects; the others see the result at their next manifest refresh. See [Deletion strategy](../deletion-strategy.md#tombstones-across-pods-reload-retirement-and-rewrite-ownership). Fixes [#594](https://github.com/ReliablyObserve/victoria-lakehouse/issues/594).

## Known gaps

- Several insert pods that see different peers can still compact the same partition (the rule that prevents it is a separate piece of work, tracked in issue 311). This page and the role gate make sure select pods are not among them.
- The stats snapshot is written by writer pods from their registry. A select pod's query counters (`last_query_at`) reach it only through stats gossip, so without peer gossip a select pod's last-query time is not persisted.
- A pod finds its own entry in the ring by port and by an IP that belongs to its host (`compaction.SelfMember`), because it knows itself as `:9428` while discovery lists it as `10.0.1.5:9428`; before, each pod hashed as an extra `:9428` member next to its own entry and every insert pod claimed the partitions that member won. Discovery that returns host names instead of IPs (SRV records) does not match, and a pod then falls back to the old behaviour for itself.
- A select pod keeps the pmeta entries of files that other pods compacted or expired until it restarts (RAM only, no effect on answers; [#595](https://github.com/ReliablyObserve/victoria-lakehouse/issues/595)).

## Tests

`cmd/lakehouse-logs/maintenance_gate_test.go` and `lakehouse-traces/maintenance_gate_test.go`:
- a table over the roles and the jobs (a select pod starts none, `insert` and `all` start all of them);
- the real main-package start-up functions for `role=select` with every job enabled: no scheduler, sweep or rewriter, and not one PUT, COPY or DELETE reaches the bucket (the same scenario on an insert pod does write, so the silence is the gate);
- two starts of a select pod (a restart), with the start-up warm-ups, a query and a close: every request is counted and none is a PUT, COPY or DELETE;
- one insert pod and two select pods over one bucket with compaction enabled on all three: only the insert pod writes, the bucket holds every row exactly once (before the gate it held every row three times), and a query on each select pod returns every row once;
- a select pod whose tombstone PUT fails sees it in the bucket after the bucket recovers.
