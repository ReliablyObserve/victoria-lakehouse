# Cold-Tier Field/Value Catalog — Design

> Status: design (implementation sequenced as Track A1→A3 below). Closes the
> interactive-Grafana gap where cold LH feels slower than hot VL/VT for
> label/field dropdowns. Companion to [buffer-queryable-store-design.md](buffer-queryable-store-design.md)
> (#109) and the PERF roadmap in [performance-machinery.md](performance-machinery.md).

## 1. Motivation — what is actually slow (measured)

`count()`/`stats` queries are LH's best case (manifest pushdown opens 0 files).
Grafana's *interactive* experience is the opposite. Measured on the benchmark
stack (24 h window, both warm, LH on uncapped minio vs VL on disk):

| Grafana query shape | VL | LH | gap |
|---|---|---|---|
| `count()` / histogram / field_names | 4–7 ms | 7–8 ms | 1.6–1.8× (fine) |
| **retrieve 100 rows + sort by `_time`** (log list panel) | 14 ms | **63 ms** | **4.5×** |
| filter + retrieve + sort | 9 ms | 28 ms | 3.1× |
| **6-panel dashboard (concurrent)** | 47 ms | **151 ms** | **3.2×** |
| **label/field value dropdowns** (high-card) | RAM-instant | scan-backed | "fast cached, slow uncached" |

Root cause is uniform: **VL/VT answer interactive queries from an in-memory
inverted index; LH reconstructs them from S3-backed metadata or files.** This
doc addresses the **dropdown / field-value-discovery** leg. The list-panel
(retrieve+sort) and concurrency legs are Track B (#109/#76).

## 2. Exactness contract (the guarantees)

These are the properties that must hold — they answer the two questions that
drove this design ("do sketches hurt search?" and "do dropdowns return the exact
matching values?").

1. **Search results are always exact.** HLL sketches are *never* in the
   filter/return path. `service.name:="x"`, `trace_id:="…"`, free-text `error`
   all go through the real bloom + label index + Parquet scan → exact rows. A
   sketch can never drop or invent a row.
2. **Low-card dropdowns are exact, including typeahead.** Fields up to a tunable
   cardinality threshold (default ~50–100k distinct) keep their **exact value
   strings** in the catalog dictionary. Typing `api` does a substring/prefix
   match over the exact string set → exactly `api-gateway`, `api-worker`, … with
   no false positives and nothing missing. Results are **time-scoped** to the
   dashboard range via the partition bitmaps.
3. **Lookup BY a value is always exact — even for high-card fields.** Searching
   `trace_id:="abc123…"`, `span_id:="…"`, `request_id:="…"` is an exact
   filter/lookup through the trace-id index + bloom + exact scan, **completely
   independent of the catalog and HLL**. High-card classification never touches
   this path — it is the same exact cold trace-id lookup verified by the parity
   loop. **You never lose the ability to search for a specific trace/span id.**
4. **High-card classification only disables *enumeration*, not lookup.** For a
   field above the threshold (`trace_id`, `span_id`, `request_id`, unbounded
   URLs) the catalog will not *list every value* in a facet dropdown — it
   returns an HLL cardinality estimate (~0.81 % error at p=14) instead. This is
   identical to VL/VT, which don't enumerate these either, and matches the real
   workflow (you paste/type a specific id to look it up; you don't scroll a
   dropdown of millions of ids).
5. **Exact-on-demand escape hatch.** A field can be pinned "always exact"
   regardless of threshold; its value list is paged from S3 rather than held
   resident. Exact distinct *counts* for high-card fields fall back to a precise
   scan only when explicitly requested.

Net: **fast/approximate only on the *count* of fields you'd never enumerate;
exact on everything you search or typeahead.**

## 2a. Configuration — the threshold knob (with good defaults)

Classification is **automatic by observed cardinality**, with a configurable
threshold and explicit per-field overrides. A field starts exact; if its
observed distinct count crosses the threshold it is promoted to count-only and
its resident value list is dropped (the catalog keeps the merged HLL). Overrides
win over the threshold.

```yaml
catalog:
  # Fields whose observed distinct values exceed this become count-only (HLL):
  # no value ENUMERATION in dropdowns. Does NOT affect searching BY a value.
  cardinality_threshold: 50000          # default — every human-meaningful facet stays exact

  # Force EXACT regardless of threshold (pages its value list from S3 if huge):
  always_exact_fields: []               # e.g. ["k8s.pod.name"] if you must typeahead it

  # Force COUNT-ONLY regardless of threshold (save RAM on known-unbounded ids):
  always_sketch_fields:                 # defaults — known unbounded id columns
    - trace_id
    - span_id
    - request_id
    - _stream_id

  hll_precision: 14                     # ~0.81% error; pinned globally, versioned in the sidecar
```

**Default behavior:** threshold `50000` keeps every realistic facet (service,
environment, namespace, pod, host, status, method, route…) exact-typeahead;
only the pre-listed unbounded-id columns go count-only. Cost of the default: a
field at the 50k ceiling costs ~1.5 MB of dictionary RAM; the ~30 typical
low-card fields sit far below, so the global dict stays ~5–15 MB (§7).

**Tuning:** raise `cardinality_threshold` (or add to `always_exact_fields`) to
keep a huge-cardinality field type-searchable — it pages its value list from S3
instead of staying fully resident, trading a little first-keystroke latency for
exact typeahead. Lower it to shed RAM at the cost of more count-only fields.
Changing classification is safe at runtime: it only flips whether a field's
value list is served vs its HLL estimate; it never affects search results.

## 3. Data structure — extend, don't rebuild

### Extend
`cache.LabelIndex` (`internal/cache/persist.go:22-41`) stays as the low-card
value store (its `Values []string` + `ValueCounts` is what dropdowns want) and
reuses its persist/marshal/merge plumbing. **Its `Values[]` cap is NOT widened**
— that path is the RAM-blowup risk.

### New — `internal/catalog` (signal-agnostic)
```go
// (1) Global interned value dictionary — low-card values only, one per Storage,
//     fully resident. Strings stored once, referenced by ValueID (uint32).
type Dict struct { ids map[string]uint32; strs []string; fields map[string]uint32 }

// (2) Per-partition, per-field facet — ONE bitmap per (partition,field), not per file.
type PartitionFacet struct {
    Values map[uint32]*roaring.Bitmap   // low-card: ValueIDs present in this partition
    HLL    map[uint32]*hll.Sketch       // high-card: sparse, resident; dense paged from S3
    hllOffset map[uint32]s3Range
}

// (3) Catalog = dict + facets + classification, fully resident (hot window).
type Catalog struct { dict *Dict; facets map[string]*PartitionFacet; highCard map[string]bool }
```
The per-file `FileInfo.Labels map[string][]string` (`metadata_sidecar.go:23`) is
O(files×fields×card) bloat; the catalog replaces it with **one bitmap per
(partition,field)** keyed on interned ids. Libraries: `RoaringBitmap/roaring`
and `axiomhq/hyperloglog` (p=14) — both live **only in sidecars, never inside
`.parquet`** (preserves Pure-Parquet-on-S3).

## 4. Build hooks (flush + compaction)

- **Flush** mirrors `bloomObserver.OnFileFlush` (`writer.go:433` logs / `:518`
  traces): a parallel `catalogObserver.OnFileFlush(partition, key, labels,
  bloomValues)` fed by the **already-extracted** label/bloom value sets — no new
  column scan. Low-card → intern + set bits; high-card → feed HLL. Batched per
  flush; dirty-partition marking like `partitioned.go:58`.
- **Compaction** is a **no-op for same-partition merges** — value→partition
  membership and HLL union are invariant under file merge (the big win over
  per-file `mergeFileLabels`). Only partition-dropping (retention) mutates the
  catalog (`DropPartition`). A `catalog_compaction_repartition_total` metric must
  stay 0; if compaction ever moves rows across partitions, fall back to
  dirty-rebuild rather than trust stale bits.

## 5. Persistence + cold-load (fast even uncached)

- `_field_catalog.bin` per partition (roaring + sparse HLL), `_value_dict.bin`
  one global object, `_field_hll.bin` per partition (dense HLL, paged on demand).
  Binary, magic+version prefixed, sidecars beside `_file_metadata.json` — reuse
  `WritePartitionSidecar`/`LoadSidecars` (16-way parallel GET).
- Cold-load extends `runStartup` after `WarmLabelIndex`/`WarmMetadata`: disk
  snapshot (instant) → S3 sidecar load (O(partitions) range reads, **not**
  O(files) parquet opens) → merge. `/ready=200` gated on `catalogResident`, so
  the **first** dropdown query after a cold start is fast, not just cached ones.
- Closes a logs-module gap: logs never push the label index to S3 (only traces
  do); the catalog sidecar closes it for **both** modules with one format.

## 6. Read path

Single integration point at `GetFieldValues`/`GetFieldNames`
(`storage_fields.go:384`), inserted **before** any file scan:
- **high-card** → refuse enumeration, return HLL estimate;
- **low-card, unfiltered** → union in-range partition bitmaps → ids → dict
  strings, fully RAM (the typeahead path);
- **filtered** → catalog pre-prunes partitions lacking the filter value, then
  falls through to the existing projected scan on strictly fewer files.

## 7. PB-scale resident-RAM cost (honest)

Parameters: 50k partitions, 100 fields (~30 low-card, ~30 high-card), HLL p=14.

| Component | Full-corpus resident | Hot-window resident (the real design) |
|---|---|---|
| Global dict (low-card strings) | ~5–15 MB (does not scale with partitions) | ~5–15 MB |
| Per-partition low-card bitmaps | ~150 MB (50k × ~3 KB) | **~12 MB** (~4k hot partitions) |
| High-card HLL | ~750 MB if per-partition resident ❌ | **~0.4 MB** (one merged sketch/field; per-partition paged) |
| **Resident total** | **~900 MB — violates budget** | **~22 MB** |
| Disk (S3 sidecars) | ~3–20 GB (~0.002 % of 1 PB) | same |
| Data-file space | **0 extra** (metadata only) | 0 |
| CPU | near-zero ongoing; ~1–3 % at compaction; **reduces** query CPU | same |

**Load-bearing decision:** the "tens of MB" guarantee holds **only** under
**time-tiered residency** (hot window resident, older partitions paged from
sidecar on demand). Full-corpus residency is ~900 MB and is not a tuning knob to
turn on at PB scale. Track A3 must land before any PB-scale claim.

## 8. Sequenced build plan

- **A1 (smallest PR, proves it end-to-end):** `internal/catalog` (`Dict` +
  low-card bitmaps, **no HLL**); flush hook (logs); `_field_catalog.bin`
  sidecar write + cold-load; read fast-path for `service.name` behind a flag.
  Proof: cold pod, query-cache off, `field_values?field=service.name` answers
  from RAM with 0 parquet opens, value set matches the labelIndex scan. Extends
  the existing logs e2e (does not replace).
- **A2:** HLL layer + high-card classification + `IsHighCard` refusal; per-field
  merged resident sketch + paged dense sidecar.
- **A3:** time-tiered residency (§7 mitigation) + compaction-no-op invariant
  metric + traces `span_attr:*` map-key integration.
- **B:** recent-rows retrieve path (#109/#76) — `_time`-prune + union
  buffer-resident values so dropdowns show not-yet-flushed values.
- **C:** PERF-2 filtered-count fold-in — catalog partition-prune before the
  count fold.

## 9. Risks (stated plainly)

- **RAM:** full-corpus residency busts the budget (~900 MB). Tens-of-MB holds
  only with A3 time-tiering — a load-bearing decision, not optional.
- **HLL accuracy:** p=14 → ~0.81 % standard error; union of per-partition
  sketches is lossless (register-max), so merging does **not** compound error.
  Risk is only mismatched precision across pods/versions → pin p=14 globally,
  version the sidecar, refuse-merge on mismatch.
- **Roaring churn:** real only on cross-partition compaction; same-partition
  merge is a no-op. Guard with the repartition=0 invariant metric.
- **Dict id stability across pods:** `_value_dict.bin` is authoritative; pods
  remap local ids at merge. Single global dict object lands in A1.

## 10. Constraints honored

No VL/VT upstream modification (exported APIs only); every `.parquet` stays
standard-tool readable (catalog lives strictly in sidecars, never in data files);
extends the existing Jaeger/VT-compliant e2e rather than replacing it. Every
implementation PR (A1–C) ships with the matching update to this doc and the
PERF roadmap table.
