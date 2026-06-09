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

The catalog is **7 MB for ~1.9 GB of data** — but that 7 MB is *not* a function of
data volume. It is a function of **cardinality × partitions**, which is the whole
point for PB-scale.

## Why the catalog does NOT scale with data volume

The catalog stores, per partition, the **distinct** values of each low-card field
(interned to `uint32` ids, sorted) plus a fixed-size HLL per high-card field. At PB
scale:

- `service.name`, `k8s.namespace.name`, `deployment.environment`, `cloud.region`, …
  have **bounded cardinality** (tens to low-thousands) no matter how many petabytes
  flow through. A value is stored **once per partition it appears in**, as a 4-byte id.
- High-card id columns (`trace_id`, `span_id`) are **never enumerated** — they get a
  **fixed 16 KB HLL** each (A2), independent of how many billions of distinct ids exist.

So catalog RAM ≈ `partitions × fields × avg_distinct_values_per_partition × 4 B`
`+ fields × 16 KB`. It grows with **time/retention** (more partitions) and field
fan-out, **not** with bytes ingested.

### Extrapolation to 1 PB

Assume hourly partitions, 30-day retention, ~50 catalogued low-card fields, ~200
distinct values/field/partition (generous), plus 10 sketched id columns:

```
partitions  = 24 × 30                      = 720
low-card    = 720 × 50 × 200 × 4 B         ≈ 28 MB
sketches    = 10 × 16 KB                   ≈ 0.16 MB
dict (interned strings, shared)            ≈ a few MB
                                           --------
resident catalog (full 30-day corpus)      ≈ tens of MB
```

At **1 PB across 720 partitions** the catalog is **tens of MB** — the same order as
the 7 MB measured at 1.9 GB, because partition count and cardinality, not bytes, drive
it. With **A3 time-tiering** (only hot partitions resident, older ones paged to the
S3 bundle and loaded on demand) the *hot* resident set is single-digit MB.

**Guardrail:** the A2 cardinality cap (`pmeta.cardinality_threshold`, default 50 000)
hard-bounds any single field, so a misbehaving high-card field can't blow RAM — it
flips to a 16 KB sketch. `lakehouse_catalog_resident_bytes` is the alert signal.

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
| **RAM** | cardinality-bounded (tens of MB full-corpus, single-digit MB hot with A3 tiering); A2 cap is a hard guardrail |
| **S3 storage** | **fewer** objects (1 bundle vs 3 sidecars/partition) |
| **S3 ops** | **fewer** (1 GET/partition warm, 1 PUT/flush) — lower request cost + faster LIST |
| **CPU** | flat (facets reuse existing extraction; HLL add is 9.7 ns) |
| **Disk** | unchanged (column-chunk cache dominates; bundles are KB) |
| **Self-heal** | catalog + file-meta re-derivable from the manifest; bloom from the bundle |

**Bottom line:** the consolidation makes LH *more* PB-ready, not less — it trades 3
per-partition sidecars for 1 bundle, keeps metadata RAM tied to cardinality (which is
flat) rather than data volume, and bounds the one unbounded axis (high-card fields)
with a fixed-size sketch + a hard cap. The remaining scale lever is **A3 time-tiering**
(page cold partitions' bundles out of RAM), already designed and unblocked by the
now-wired S3 bundle persist/warm.
