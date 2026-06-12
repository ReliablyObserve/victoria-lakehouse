# Stats Aggregate Cache — per-field storage + metadata sizes, cluster-wide

Status: **landed (logs + traces) — foundation + Phases A–E**, on both `:29428` and `:20428`: per-field on-S3 storage in the Cardinality Explorer (scaled→exact as `ColumnBytes` backfills); Storage Overview metadata footprint tiles + **Fleet instances**; Storage Details per-field storage+metadata table; Tenants exact per-tenant metadata (tenant-isolated pmeta bundles); per-instance fleet gossip. The S3 sidecar cache + the on-S3 metadata are incremental (never scanned).

## Motivation

The Lakehouse stats API and UI need per-field **storage** size, per-field **metadata** size, overall **metadata footprint** (memory / disk / S3), and per-tenant metadata — surfaced in the Cardinality Explorer, Storage Overview, Storage Details, and Tenants views. Computing these by scanning the whole manifest (O(files × columns)) on every UI refresh / API call would not scale (PB-scale = 50M files). We need a **materialized aggregate**, maintained by diffs and read in O(1), persisted in S3 as a small cache so a fresh instance never does a cold full sweep.

## What goes where (architecture compliance)

The cluster-wide / persist / compact machinery already exists; each new stat routes to the layer whose guarantees fit (verified against the code):

| Stat | Home | Why |
| --- | --- | --- |
| Per-field **storage** bytes | **manifest** (`FileInfo.ColumnBytes`) | manifest is cluster-wide, snapshot-persisted, and **compacted** (compactor re-derives from merged rows) → no drift, unlike the cumulative `TenantRegistry`. |
| Per-field **metadata** bytes (bloom + catalog) | **pmeta** facet `EstimateBytes` | facets are merged + persisted + compaction-aware already. |
| Overall metadata size (S3, cluster-wide) | **stats aggregate** (periodic `ListObjectsV2` sweep of the meta prefix) | one cheap periodic sweep, cached. |
| Per-instance metadata size (memory `ResidentBytes`, disk `DiskCache.Size`) | **registry CRDT gossip** (per-node maps) | inherently per-node; reuse `SyncPusher`/`SyncHandler` + snapshot. |

## The aggregate object

A standalone S3 sidecar under the pmeta/meta prefix (`_stats/aggregate.json`), mirrored in RAM on every instance:

```
StatsAggregate {
  Generation, UpdatedAt
  PerField  map[field]  { StorageBytes, MetadataBytes, Rows, Files }
  PerTenant map[tenant] { StorageBytes, MetadataBytes, Rows, Files }
  Totals    { Storage, Raw, Rows, Files }
  S3MetaBytes  // cluster-wide meta footprint from the periodic LIST sweep
}
```

Per-instance memory/disk numbers are **not** in this object — they are per-node and travel on the registry gossip.

## Maintenance — diffs through one chokepoint

Every file add (flush **and** compaction output) and remove (compaction inputs) routes through `manifest.AddFile` / `RemoveFile`. The manifest exposes `SetChangeObserver(onAdd, onRemove)` (fired under the write lock, beside the existing incremental `tenantAggregates`). The aggregate subscribes:

- `onAdd(fi)`  → `PerField[col] += fi.ColumnBytes[col]`, `+= rows/files`.
- `onRemove(fi)` → subtract the removed file's `ColumnBytes` (the manifest still has the full `FileInfo` at removal time → exact, not estimated).

One observer captures flush + compaction; **O(columns/file) per change, never a full scan.**

## Reads, persistence, reconcile

- **Reads:** the stats API reads the in-RAM aggregate (O(1) map lookups). No manifest walk per request — this is the cache that keeps UI refreshes / API calls cheap.
- **Persist:** snapshot to the sidecar on an interval (mirrors `TenantRegistry.MarshalSnapshot`/`LoadSnapshot` + the `SnapshotInterval` ticker).
- **Startup:** load the sidecar (no cold full sweep), then reconcile against the warm-loaded manifest.
- **Periodic full reconcile** ("whole-storage verification"): on a *longer* interval, recompute from the full manifest (the source of truth), correct any incremental drift, re-persist. Bulk manifest refreshes also trigger a recompute.

### Cluster-wide correctness
The manifest is already shared + refreshed across instances, so the per-field/per-tenant aggregate is a deterministic function of it and converges on every instance. The S3 sidecar is purely a **cache + cold-start accelerator**, never the source of truth — so divergent writers self-correct at the next reconcile.

## Phases

- **0 — foundation (landed):** `FileInfo.ColumnBytes` captured from the Parquet footer at flush; `manifest.SetChangeObserver` add/remove hook.
- **A — landed (logs + traces):** compactor re-derives `ColumnBytes`; the `StatsAggregate` component (subscribe to the manifest hook + recompute-on-warm + reconcile-on-refresh; the S3 sidecar `SaveToS3`/`LoadFromS3` cold-start cache, persisted after each reconcile + loaded on startup); `/cardinality/fields` `+storage_bytes`; the UI **Storage** column, scaled to the live on-S3 total during the `ColumnBytes` backfill window. Traces-module parity landed: the traces writer captures per-column footer bytes and the traces binary wires its own `StatsAggregate` (observer + recompute-on-warm + reconcile-on-refresh + APIConfig), so `:20428` populates the Storage column identically.
- **B — landed (logs + traces):** Storage Overview **Metadata footprint** tiles — pmeta RAM (`PmetaResidentBytes`) + disk cache (`DiskCacheBytes`), both this-node, plus the cluster on-S3 metadata total. The on-S3 figure is tracked **incrementally** — each pmeta bundle records its encoded byte size on persist / warm-load / compaction (`Bundle.PersistedSize` → `Store.PersistedBytes`), so it's a live sum that **never lists S3**. `/stats/overview` `+meta_resident_bytes`/`+meta_disk_bytes`/`+meta_s3_bytes`. e2e: `TestStatsOverviewMetadata`, `TestCardinalityStorageBytes`.
- **C — landed (logs + traces):** Storage Details is now a sortable per-field table (Field · Storage · Metadata · Cardinality · Bloom) from `/stats/fields`. Per-field metadata is **exact** — the bloom + field-catalog facets expose per-field bytes (`BytesByField` = per-column bloom bitset + per-field value-set/HLL), summed by `Store.MetadataBytesByField` (incremental, no scan); storage reuses the Cardinality Explorer's covered→total scaling. Tenant facet dropped (tenants have their own tab).
- **D — landed (logs + traces):** each node gossips its metadata footprint (pmeta RAM + disk cache) through the tenant-stats CRDT via an **orthogonal** per-node `NodeMeta` channel (separate map, piggy-backed on `TenantDelta`, LWW by per-node `Gen`, S3-snapshot-preserved) — the tenant CRDT cursors/convergence are untouched (the idempotent/commutative/associative/monotonic guard suite passes unchanged). `/stats/instances` `+per-node {resident, disk, s3}`; the Storage Overview **Fleet instances** table (self-row marked). The local node records its own footprint via `registry.SetNodeMeta` after warm + on the refresh tick.
- **E — landed (logs + traces):** pmeta bundles are now keyed by a **tenant-scoped partition** (`<account>/<project>/<signal>/dt=/hour=` via `manifest.ExtractTenantPartition`) instead of a global `dt=/hour=`, so each tenant's metadata is physically isolated under its own S3 prefix (mirroring the data path). `/tenants` `+metadata_bytes` — exact per-tenant size from `Store.PersistedBytesByTenant` (incremental, no scan) — surfaced as the Tenants **Metadata** column. Every pmeta path (flush, compaction, warm-from-manifest, warm-from-S3, cold-start enrich, bloom reads, retention) derives the partition from the file key; the manifest's partition count + retention keep the pure `dt=/hour=` key. A one-time, marker-gated, manifest-driven migration (`CleanupLegacyGlobalBundles`) deletes the orphaned global bundles. **Migration note:** the 4 high-card sketched ids (trace_id/span_id/container.id/service.instance.id) re-derive from data — the old *global* HLLs can't be split per-tenant — so they re-accumulate from new flushes; dimensional fields rebuild full from the manifest.

Each phase ships backend + UI (in the single shared `internal/ui/static/lakehouse-ui.js`) + a rebuild + live verify, and updates docs (internal + public) at its boundary.
