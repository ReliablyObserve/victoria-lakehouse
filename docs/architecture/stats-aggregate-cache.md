# Stats Aggregate Cache — per-field storage + metadata sizes, cluster-wide

Status: **design verified; foundation landed** (per-column capture + manifest change-observer). Phases A–E wire the surfaces.

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

- **0 — foundation (landed):** `FileInfo.ColumnBytes` captured from the Parquet footer at flush (both modules); `manifest.SetChangeObserver` add/remove hook.
- **A:** compactor re-derives `ColumnBytes`; `StatsAggregate` component (subscribe + persist + reconcile); `/cardinality/fields` `+storage_bytes`; UI Storage column.
- **B:** overview `+metadata` (mem `ResidentBytes` + disk `DiskCache.Size` + S3 sweep); overview metadata tiles (mem/disk/S3).
- **C:** `/stats/storage` per-field `{storage, metadata}`; Storage Details → per-field table (drop the tenant facet).
- **D:** per-instance mem/disk gossiped via the registry node-maps; per-instance breakdown in the UI.
- **E:** `/tenants` `+metadata_bytes`; Tenants metadata column.

Each phase ships backend + UI (in the single shared `internal/ui/static/lakehouse-ui.js`) + a rebuild + live verify, and updates docs (internal + public) at its boundary.
