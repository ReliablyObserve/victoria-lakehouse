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

Compaction merges small objects into one, uploads the result and deletes the sources. Two pods that do this over the same partition each upload their own output (the output key is random per pod) before either deletes the sources, and the rows are stored once per pod. Ownership by highest-random-weight hashing only separates the pods that see each other in the peer ring; a select pod that was configured with compaction on joined the ring and compacted the partitions it owned (issue 311: every row two to four times in the bucket). Retention on a select pod would delete objects the insert pods still serve, and a delete rewrite would replace an object another pod is compacting.

## Which jobs are gated

Both binaries; line numbers are in `cmd/lakehouse-logs/main.go` (the traces binary mirrors them in `lakehouse-traces/main.go`).

| Job | Writes | Where | Role gate |
|---|---|---|---|
| Compaction scheduler and its Tier A/B orphan sweep | PUT merged objects, DELETE sources | `setupCompaction` | gated |
| Delete rewrite scheduler (rewrites objects without tombstoned rows, deletes the source) | PUT, DELETE | `startDeleteRewrite` | gated |
| Retention (drops expired objects and their pmeta bundles) | DELETE | `startRetention` | gated |
| Stats snapshot (periodic and at shutdown) | PUT `stats/snapshot.json` | `startStatsLoops`, `runShutdown` | gated |
| Stats-aggregate sidecar | PUT `_meta/stats-aggregate.json` | `saveStatsAggregate` | gated |
| One-time legacy pmeta bundle cleanup | DELETE bundles, PUT marker | `cleanupLegacyPmeta` | gated |
| Label-index persist to S3 (traces binary) | PUT | `Storage.Close`, manifest refresh | gated |
| pmeta bundle persist on each flush and at close | PUT bundles | buffer flusher, `Storage.Close` | by construction: only the insert-side writer owns a catalog observer |
| Insert buffer flusher, segment markers | PUT objects and markers | insert wiring | by construction: select has no writer |

Not gated, because they are driven by one request and are safe from any pod:

| Write | Why it stays on select |
|---|---|
| Tombstone files (`tombstone_persist.go`) | Written when a delete request arrives (upstream's delete API runs on select pods). One object per tombstone id, so two pods never write the same key. |
| Tenant alias registry (`internal/tenant/registry.go`) | Allocated when a request carries a new org id; conditional PUT with `If-Match`, so concurrent pods never overwrite each other. |
| `POST /lakehouse/api/v1/admin/tenant/migrate` | An explicit, authenticated operator action. |
| `POST /lakehouse/compaction/recompact`, `/lakehouse/drain` | They need the scheduler; on a select pod they answer 503. |

Read-only jobs stay on select: manifest refresh, peer discovery, the caches, stats gossip over HTTP, and loading the stats snapshot and the aggregate sidecar at start.

A test pins the list: `TestMaintenanceInventory_EveryS3WriterIsGated` fails when a function in the main package starts a bucket-mutating job without checking the role and without an entry (with its reason) in the test's list.

## Behaviour with several pods

- **Several insert pods, several select pods.** Only the insert (or `all`) pods compact, each partition by its HRW owner. Adding or removing select pods changes nothing about what is written, and no select pod issues a PUT or DELETE for maintenance.
- **S3 requests.** Writers: unchanged. Select pods: the maintenance PUT, COPY and DELETE requests stop; reads are unchanged.
- **Tombstones created through a select pod.** They are stored in S3 and hide rows on every pod that loads them. A pod loads tombstones when it starts, so the delete rewrite of such a tombstone runs on an insert pod once that pod has loaded it; a periodic reload on writer pods is a follow-up (see the known gaps below).

## Known gaps

- Several insert pods that see different peers can still compact the same partition (the rule that prevents it is a separate piece of work, tracked in issue 311). This page and the role gate make sure select pods are not among them.
- Writer pods load tombstones at start only, so a tombstone created on a select pod is rewritten after the next writer restart; until then it hides rows but its objects are not rewritten.

## Tests

`cmd/lakehouse-logs/maintenance_gate_test.go` and `lakehouse-traces/maintenance_gate_test.go`:
- a table over the roles and the jobs (a select pod starts none, `insert` and `all` start all of them);
- the real main-package start-up functions for `role=select` with every job enabled: no scheduler, sweep or rewriter, and not one PUT, COPY or DELETE reaches the bucket (the same scenario on an insert pod does write, so the silence is the gate);
- one insert pod and two select pods over one bucket with compaction enabled on all three: only the insert pod writes, and the bucket holds every row exactly once (before the gate it held every row three times).
