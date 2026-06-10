# Resource & cost at PB scale — with the pmeta consolidation

How the unified metadata layer changes RAM / CPU / disk / S3-storage / S3-ops, and
why LH is ready for petabyte-scale cold tier. Grounded in a live measurement of the
pmeta-on stack, then extrapolated.

## Measured baseline (live, pmeta on)

| signal | value |
|---|---|
| indexed Parquet | **1.88 GB** across **1,109 files** |
| `lakehouse_catalog_resident_bytes` | **7.1 MB** |
| container memory | 1.13 GiB / 2 (≈ **0.6 %** of which is the catalog) |
| container CPU | 1.5 % |
| `field_values` (24h, no-limit) | **24 ms** (was 50 s) |

The Store is **7 MB for ~1.9 GB of data** — but that 7 MB is *not* a function of
raw bytes ingested. It is a function of **cardinality × partitions** (the catalog
facet) **plus the live file count** (the file-meta and bloom facets). Getting the
model right matters at PB scale, so here it is honestly.

## The resident-memory model — what the Store actually scales with

`lakehouse_catalog_resident_bytes` covers ALL facets in the bundles plus the
shared dict. Three terms:

1. **Catalog facet — cardinality × partitions.** Per partition, the **distinct**
   values of each low-card field (interned to `uint32` ids, sorted) plus one
   fixed-size HLL per sketched field, held globally:
   - `service.name`, `k8s.namespace.name`, `deployment.environment`,
     `cloud.region`, … have **bounded cardinality** (tens to low-thousands) no
     matter how many petabytes flow through. A value is stored **once per
     partition it appears in**, as a 4-byte id.
   - High-card id columns (`trace_id`, `span_id`) are **never enumerated** — they
     get a **fixed 16 KB HLL** each (A2, one per field, in-RAM only),
     independent of how many billions of distinct ids exist.
2. **File-meta facet — live FILE count.** One entry per live manifest file
   (key + times/counts ≈ 48 B + the per-file label map, itself capped at 100
   values/field). This is the `_file_metadata.json` fold — it scales with **how
   many files the manifest holds**, not with bytes.
3. **Bloom facet — live FILE count × bloom bytes.** One bloom filter per file
   per bloom column at 1 % fp ≈ **~1.2 B per distinct value** in that file's
   column. This is the `_bloom.bin` fold; for id-heavy bloom columns
   (`trace_id`) it is the dominant per-file term.

So Store RAM ≈
`partitions × fields × avg_distinct/partition × 4 B + sketched_fields × 16 KB`
`+ live_files × (file-meta entry + bloom bytes)`. It grows with
**time/retention** (more partitions), **field fan-out**, and **live file count**
— *not* with bytes ingested. The file-count term is the one the earlier draft of
this doc understated.

### What bounds the file-count term (now wired)

- **Compaction** merges small files into fewer larger ones AND removes the
  merged-away inputs' facet entries (`PmetaOnCompacted` → `Store.RemoveFiles`),
  so dead keys do not accumulate in RAM or in the persisted bundle. (The
  compacted output gets no bloom entry — absent keys are always kept, which is
  sound; new flushes carry their own blooms.)
- **Retention** removes expired files' entries (`PmetaOnFileExpired`); a
  fully-expired partition's bundle is **evicted from RAM and its `_pmeta.bundle`
  S3 object deleted** — bundles no longer accumulate past retention.

Net: the Store tracks the **live manifest**, with the same file count the
manifest itself (and the footer cache, and every query plan) already scales
with — pmeta adds no *new* unbounded axis.

### Extrapolation to 1 PB

Assume hourly partitions, 30-day retention, ~50 catalogued low-card fields, ~200
distinct values/field/partition (generous), 2 sketched id columns, and
compaction holding the manifest to ~150 live files/partition (~108 k files) at
~3 KB of file-meta + bloom bytes per file:

```
partitions  = 24 × 30                      = 720
low-card    = 720 × 50 × 200 × 4 B         ≈ 28 MB
sketches    = 2 × 16 KB                    ≈ 0.03 MB
dict (interned strings, shared)            ≈ a few MB
file-meta + bloom = 108k files × ~3 KB     ≈ 300 MB   ← the file-count term
                                           --------
resident Store (full 30-day live corpus)   ≈ low hundreds of MB, dominated by
                                             per-file metadata — bounded by
                                             compaction, not by bytes ingested
```

The catalog half stays **tens of MB** at 1 PB (partition count and cardinality,
not bytes, drive it — same order as the 7 MB measured at 1.9 GB). The per-file
half is the same information the legacy `_file_metadata.json`/`_bloom.bin`
sidecars held; pmeta makes it resident per live file and **bounded by
compaction + retention** (above). The remaining lever is **A3**: there is **no
time-based paging of old-but-live partitions yet** — every partition still in
the manifest keeps its bundle resident, so the hot-window-only residency
(single-digit MB hot set) is designed but not implemented.

**Guardrail:** the A2 cardinality cap (`pmeta.cardinality_threshold`, effective
default 50 000) hard-bounds any single field, so a misbehaving high-card field
can't blow RAM — it flips to a 16 KB sketch.
`lakehouse_catalog_resident_bytes` is the alert signal.

## S3 storage & operations — the consolidation *reduces* both

| per partition | before | after the flip |
|---|---|---|
| sidecar objects | **3** (`_file_metadata.json`, `_bloom.bin`, `_label_index.json`) | **1** (`_pmeta.bundle`) |
| warm GETs | 3 (+ the 16-way `LoadSidecars` fan-out) | **1** GET/partition |
| flush PUTs | up to 3 | 1 (dirty bundle) |

At PB scale with millions of partitions over the bucket lifetime, that's a **3×
reduction in metadata objects and S3 metadata ops** — fewer PUT/GET/LIST requests
(direct AWS cost), less LIST pagination at warm, and a smaller object-count footprint
(S3 charges per request and the manifest LIST cost scales with object count). The
Parquet data objects are unchanged (and stay standard-tool-readable — no custom
framing).

## CPU & flush cost

- Facet building at flush **reuses the already-extracted label map** (the same one the
  bloom path uses) — **no extra column scan**. The HLL tap streams off the row structs
  at **9.7 ns/value, 0 allocs**, so cardinality sketching is effectively free.
- One bundle encode + PUT per dirty partition per flush (replaces 3 sidecar writes).
- Warm: one GET + decode per partition (replaces the 3-sidecar parallel load), with
  manifest-derive as the self-heal fallback.

CPU stayed at ~1.5 % with pmeta on. No measurable flush-path regression.

## Disk

- Local cache (`smartcache` L2) holds **Parquet column chunks** (GBs, watermark-evicted)
  — **unchanged** by pmeta.
- The pmeta bundles are KB/partition; the resident `Store` is the tens-of-MB above.
  Net new disk: negligible.

## PB-scale readiness summary

| dimension | status at PB scale |
|---|---|
| **RAM** | catalog half cardinality-bounded (tens of MB); per-file half (file-meta + bloom) scales with **live file count**, bounded by compaction (dead-key removal) + retention (bundle eviction + S3 GC); A2 cap is a hard per-field guardrail; A3 paging of old-but-live partitions is the open item |
| **S3 storage** | **fewer** objects (1 bundle vs 3 sidecars/partition) |
| **S3 ops** | **fewer** (1 GET/partition warm, 1 PUT/flush) — lower request cost + faster LIST |
| **CPU** | flat (facets reuse existing extraction; HLL add is 9.7 ns) |
| **Disk** | unchanged (column-chunk cache dominates; bundles are KB) |
| **Self-heal** | wired: a missing/corrupt bundle (or skipped facet) is rebuilt from the manifest at warm (`Store.Rebuild`, dirty → the repaired bundle replaces the broken S3 object); bloom content re-fills from new flushes |

**Bottom line:** the consolidation makes LH *more* PB-ready, not less — it trades 3
per-partition sidecars for 1 bundle, and keeps metadata RAM tied to **cardinality
(flat) plus live file count (bounded by compaction + retention, now wired as
`PmetaOnCompacted`/`PmetaOnFileExpired`)** rather than bytes ingested, with the one
per-field unbounded axis (high-card fields) cut off by a fixed-size sketch + a hard
cap. The remaining scale lever is **A3 time-tiering** (page old-but-live partitions'
bundles out of RAM) — designed and unblocked by the now-wired S3 bundle
persist/warm, but **not implemented yet**: today the full live corpus stays
resident.
