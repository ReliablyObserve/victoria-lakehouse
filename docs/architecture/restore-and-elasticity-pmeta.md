# Restore & elasticity with pmeta — add / restore / remove an instance

How fast a lakehouse instance comes back (from disk and from S3), what pmeta adds to
that, and where to optimise for fast scale-up / restart / scale-down.

## Measured

A rebuilt cold-LH pod (pmeta on, **1,109 files / 1.88 GB**, PVC cache intact) went
`Starting → READY + warmup_complete=1` in **8 s**. `field_values` served at 24 ms
immediately after. That is the **warm-restart** number (disk present).

## The three restore scenarios

| scenario | manifest | catalog (cat + file-meta) | bloom | typical cost |
|---|---|---|---|---|
| **Warm restart** (PVC intact) | snapshot from **disk** + S3 delta refresh | re-derived from the in-RAM manifest (**no S3**) | bundle GETs *or* legacy `_bloom.bin` | ~8 s @ 1.9 GB |
| **Cold add** (new pod, no PVC) | S3 LIST + snapshot GET | manifest-derive (**no extra S3**) | `WarmCatalogFromS3` — **1 GET/partition** | manifest-load-bound |
| **Scale-down** (remove) | graceful shutdown writes final snapshot | — | `persistDirty` flushes dirty bundles | sub-second |

**Key pmeta property:** the catalog + file-meta facets are **re-derived from the
manifest**, which is already resident — so they add **zero S3 round-trips** to restore.
Only the **bloom** facet needs S3 (it can't be rebuilt from the manifest); that's the
`WarmCatalogFromS3` path — **one GET per partition**, parallel (concurrency 8).

## What pmeta costs at restore — and the scale cliff

| path | complexity | at 1 PB (~720 partitions, millions of files) |
|---|---|---|
| catalog manifest-derive (`WarmCatalog`) | **O(files)** — iterate every file's labels | iterating *millions* of files is the dominant restore cost |
| bloom bundle-load (`WarmCatalogFromS3`) | **O(partitions)** — one GET each | ~720 GETs / 8 ≈ 90 rounds × ~30 ms ≈ **~3 s** |

So at PB scale the **O(files) manifest-derive is the bottleneck**, not S3. The fix is
already in place to avoid it: the **bundle persist/warm** loads the *whole* catalog
state per partition with **one GET** (O(partitions)) instead of re-walking O(files).
We should make bundle-load the **primary** warm path and fall back to manifest-derive
only for partitions whose bundle is missing/corrupt (the `NeedsRebuild` set), rather
than running both — that turns restore from O(files) into O(partitions).

## Why restore latency to *first serve* is already bounded

Two existing lifecycle features (tasks #72, #80) mean an instance does **not** wait for
the full corpus to warm before answering:

- **Priority warmup** — recent partitions warm first.
- **Serve-while-warming** — `/ready` returns 200 once a minimum is warm; queries serve
  against warmed partitions while the tail loads lazily.

So **scale-up to first-useful-query** is bounded by warming the *hot* partitions
(seconds), independent of total corpus size — the long tail loads in the background.

## Optimisation levers (to accelerate add / restore / remove)

| lever | effect | status |
|---|---|---|
| **Bundle-load primary, manifest-derive only for `NeedsRebuild`** | restore O(files) → O(partitions) — the big PB-scale win | **wired, needs the read-order flip** |
| **One `_pmeta-snapshot.bin`** listing all bundle keys + warm hints | a single GET tells the pod exactly what to warm (no LIST fan-out) | designed (§2 of consolidation), not built |
| **Tune bundle-GET concurrency** (currently 8) | scale-up speed on fat pipes | one-line config |
| **A3 time-tiering** | only hot-partition bundles loaded resident; cold ones paged on query | designed |
| **Catalog is tiny (7 MB)** | fast to transfer/restore; could even be peer-fetched from a warm sibling on scale-up | peer-cache exists for column chunks; could extend |
| **Graceful-shutdown persistDirty** | scale-down leaves bundles current → next pod warms fast | wired |

## Verdict

- **Restart / restore today (≤ few GB): ~8 s**, dominated by manifest snapshot + S3
  delta, *not* by pmeta (catalog adds 7 MB derived in-RAM).
- **At PB scale** the restore cost is the **O(files) manifest walk**, and the cure is
  already built — make **bundle-load the primary warm** (O(partitions), one GET each)
  and reserve manifest-derive for self-heal. That, plus priority-warmup +
  serve-while-warming, keeps **scale-up-to-first-serve in seconds** at any corpus size.
- **Scale-down** is already sub-second (final snapshot + dirty-bundle flush), so pods
  can be added/removed elastically without a slow drain.

**Action item:** flip `WarmCatalogFromS3` to primary + `WarmCatalog` to the
`NeedsRebuild`-only fallback (currently both run). That is the single change that makes
restore O(partitions) instead of O(files) at PB scale.
