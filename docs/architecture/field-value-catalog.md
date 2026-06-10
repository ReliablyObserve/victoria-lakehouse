# Cold-Tier Field/Value Catalog — Design

> Status: **A1 + A2 shipped** (#127/#130/#131) as the `FacetFieldCatalog` facet of
> the unified partition-metadata layer (`internal/pmeta` — see
> [metadata-consolidation.md](metadata-consolidation.md) §8); A3 (time-tiered
> residency) remains open. The shipped data structures are the pmeta `Dict` +
> per-partition value sets and an in-house HLL — §§3–5 below sketch the original
> design (`internal/catalog`, roaring, per-facet sidecars) and are kept as the
> rationale record; persistence actually landed as the per-partition
> `_pmeta.bundle`. Closes the
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
   cardinality threshold (default 50,000 distinct) keep their **exact value
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
4. **High-card classification only disables *catalog enumeration*, not lookup —
   and never serves a truncated list.** For a field above the threshold (or in
   `always_sketch_fields`) the catalog stops storing values (bounding RAM) and
   its `Values()` returns nil, so `field_values` **falls through to the exact
   legacy scan** — the answer is still exact, just slower. With
   `refuse_sketch_enumeration` on, a declared `always_sketch_fields` id column
   returns *empty* instead of scanning (identical to VL/VT, which don't
   enumerate these either; threshold-crossers are NOT refused). The field's
   distinct count is exposed as an HLL estimate (~0.81 % error at p=14) via
   `lakehouse_catalog_field_cardinality{field}` / `Store.Cardinality`, not in
   the `field_values` response.
5. **A truncated extraction is never authoritative.** The flush-time label
   extractor caps each field at `maxLabelsPerField` (100) distinct values per
   file; a field at the cap *may* be incomplete, so the contribution marks it
   in `TruncatedFields` and the catalog flags it high-card → the read path
   answers from the exact scan. The catalog never serves a silently-truncated
   value list. (There is no per-field "always exact" pin today; the knob is
   raising `pmeta.cardinality_threshold`.)

Net: **fast/approximate only on the *count* of fields you'd never enumerate;
exact on everything you search or typeahead.**

## 2a. Configuration — the real surface (`pmeta`, both modules)

Classification is **automatic by observed cardinality**, with a configurable
threshold and an explicit force-high-card override. A field starts exact; if its
observed distinct count crosses the threshold it is marked high-card and its
resident value list is dropped (reads fall through to the exact scan, §2.4).
The whole catalog lives under the `pmeta` config key (it ships as a facet of
the unified partition-metadata layer):

```yaml
pmeta:
  # Master switch for the whole layer (catalog + file-meta + bloom facets).
  # Off (the default) → no Store is built; flush/query paths are unchanged.
  enabled: false

  # Per-field distinct-value cap before a field is classified high-card: the
  # catalog stops storing its values (RAM bound) and field_values falls through
  # to the exact scan — never a truncated list. NOTE: 0 currently maps to the
  # 50000 default (there is no "unlimited" setting today).
  cardinality_threshold: 0

  # Forced high-card regardless of threshold (known unbounded id columns).
  # DEFAULT IS EMPTY — [trace_id, span_id] is the recommended setting (it is
  # what the e2e compose runs). The HLL cardinality tap covers trace_id/span_id
  # ONLY (they are the only id columns with row-struct fields); other names here
  # are still excluded from the catalog but get no sketch — startup logs a warning.
  always_sketch_fields: [trace_id, span_id]

  # When true, field_values for an always_sketch_fields column returns EMPTY
  # instead of scanning to enumerate it (matches VL/VT; lookup BY value is
  # unaffected). Threshold-crossers are NOT refused. Default false (opt-in:
  # it is a behavior change for those fields).
  refuse_sketch_enumeration: false

  # Stop writing the legacy sidecars the facets replace (_file_metadata.json,
  # per-file .bloom, partition _bloom.bin). Requires enabled; reversible —
  # clear it and the sidecars resume. Default false.
  retire_sidecar_writes: false
```

CLI flags (both `lakehouse-logs` and `lakehouse-traces`) map 1:1:

| flag | yaml key |
|---|---|
| `-lakehouse.pmeta.enabled` | `pmeta.enabled` |
| `-lakehouse.pmeta.cardinality-threshold` | `pmeta.cardinality_threshold` |
| `-lakehouse.pmeta.always-sketch-fields` (comma-separated) | `pmeta.always_sketch_fields` |
| `-lakehouse.pmeta.refuse-sketch-enumeration` | `pmeta.refuse_sketch_enumeration` |
| `-lakehouse.pmeta.retire-sidecar-writes` | `pmeta.retire_sidecar_writes` |

Keys that appeared in earlier drafts of this doc but **do not exist**:
`catalog:` (it is `pmeta:`), `always_exact_fields` (no per-field exact pin —
raise the threshold instead) and `hll_precision` (pinned at p=14 in code, not
configurable; see below).

**Default behavior:** the effective threshold `50000` keeps every realistic
facet (service, environment, namespace, pod, host, status, method, route…)
exact-typeahead; only the configured unbounded-id columns are forced high-card.
Cost of the default: a field at the 50k ceiling costs ~1.5 MB of dictionary RAM;
the ~30 typical low-card fields sit far below, so the global dict stays
~5–15 MB (§7).

**Tuning:** raise `cardinality_threshold` to keep a bigger field
exact-typeahead (it stays fully resident — there is no S3-paged value list
today, that is A3 territory); lower it to shed RAM at the cost of more
scan-backed fields. Changing classification only flips whether a field's value
list is served from RAM vs the exact scan; it never affects search results.

**Exactness/lifecycle notes (shipped behavior):**

- *Per-file label cap → `TruncatedFields` → high-card → scan.* A field at the
  flush extractor's `maxLabelsPerField` (100) cap is marked in the
  contribution's `TruncatedFields` and classified high-card, so the catalog can
  never serve a truncated list as authoritative (§2.5).
- *HLL tap is trace_id/span_id only.* The flush tap streams id values straight
  off the row structs; only `trace_id`/`span_id` exist as struct fields.
- *HLL sketches are in-RAM only.* They are held on the `Store` (one merged
  sketch per field), are NOT persisted in the `_pmeta.bundle`, and reset on
  restart — `lakehouse_catalog_field_cardinality` re-accumulates from new
  flushes.

### What HLL precision means (plain version — pinned at p=14 in code)

HyperLogLog answers "**how many distinct values**" without storing the values.
It keeps `2^p` tiny counters ("registers"), where `p` is the precision — pinned
at **14** in code (`defaultHLLPrecision`, `internal/pmeta/hll.go`); it is *not*
a config knob. More registers → a more accurate estimate **and** more memory —
that is the *only* thing precision changes. It affects the **approximate
distinct-count** reported for
high-card fields (e.g. "≈ 5,200 distinct trace_ids"); it never affects search,
value lists, or low-card facets.

| precision `p` | registers | typical error | RAM per sketch (dense) |
|---|---|---|---|
| 10 | 1,024 | ~3.25 % | ~1 KB |
| 12 | 4,096 | ~1.6 % | ~4 KB |
| **14 (pinned)** | **16,384** | **~0.81 %** | **~12 KB** |
| 16 | 65,536 | ~0.4 % | ~48 KB |

At `14`: a "≈ 1,000,000 distinct" readout lands within ~±8,000 — plenty for
a UI hint, at 12 KB per high-card field's merged sketch. **One** sketch is held
per field, on the `Store`, globally (not per partition), in RAM only — sketches
are not persisted and reset on restart. Changing precision would be a code
change; sketches of different `p` refuse to merge (a mismatch is refused, not
silently corrupted).

### Estimator: HLL++-grade accuracy via LogLog-Beta (in-house, no dep)

The sketch (`internal/pmeta/hll.go`) is in-house — no dependency. It uses a 64-bit
hash (so the `2^32` large-range correction of original **HLL** [Flajolet et al.,
2007] is not needed) and the **LogLog-Beta** estimator [Qin/Kim/Tung, 2016], a
table-free polynomial that delivers the accuracy of **HLL++** [Heule/Nunkesser/Hall,
2013] *without* shipping HLL++'s ~6,000-constant empirical bias tables — which are
error-prone to hand-embed and verify. Merge is lossless register-max, so unioning
per-partition sketches does not compound error.

Measured (p=14, `hll_test.go`), relative error vs true cardinality:

| true N | classic HLL | LogLog-Beta |
|---:|---:|---:|
| 5,000 | 0.08 % | 0.28 % |
| 20,000 | 0.59 % | 0.45 % |
| **40,000** (≈ 2.5·m, the bias-prone cutover) | **3.25 %** | **0.67 %** |
| 100,000 | 0.21 % | 0.20 % |
| 300,000 | 0.41 % | 0.41 % |

The mid-range (≈ m … few·m) is exactly where original HLL is biased and HLL++
exists to fix it — here LogLog-Beta is **~5× more accurate** (3.25 % → 0.67 %),
and within ~0.7 % across the whole range. Tests cover accuracy, the
HLL-vs-LogLog-Beta comparison, lossless merge, marshal round-trip, and a decoder
fuzzer (in CI). LogLog-Beta's polynomial is fitted for `p=14`; other precisions
fall back to the classic estimator (+ linear counting).

### Verification (does it work, accurately, with no errors / no false results)

The sketch is held one-per-field on the `Store` (fed at flush via
`FileContribution.HighCardValues`; `Store.Cardinality(field)` reads it). Coverage in
`internal/pmeta/hll_test.go` + `hll_bench_test.go`:

- **Accuracy** across `n = 100 … 1,000,000` and **five value distributions**
  (sequential, hex, common-prefix, sparse, uuid-like) — all within **3 %**.
- **No-false guard**: tiny `n` (0,1,5,25,100) near-exact (±3); full range never zero
  for non-empty input and **never off by >10 %** (no order-of-magnitude misses).
- **Merge = union** (overlap not double-counted); precision-mismatch refused;
  **marshal round-trip** preserves the estimate.
- **Integration ("our case")**: 50 flushed files × 1,000 `trace_id`s →
  `Store.Cardinality("trace_id")` within 3 % of 50,000, unknown field → 0, and
  re-flushing seen values does **not** inflate the count.
- **Fuzzers** (in CI): `FuzzHLLUnmarshal` (decoder) + `FuzzHLLAdd` (arbitrary values).
  `-race` clean.

Performance (p=14): **`add` 9.7 ns/op, 0 allocs** (folding values at flush is
effectively free), `estimate` ~10 µs (per query, scans 16,384 registers), `merge`
~40 µs, `marshal` ~1.2 µs.

### What the cardinality sketch actually buys us

The sketch answers one question cheaply — *"how many distinct values does this
field have?"* (16 KB/field, `add` 9.7 ns). In Victoria Lakehouse that buys:

1. **Finishes the high-card dropdown.** The catalog gives exact value lists for
   low-card fields (`service.name`, `env`); the sketch handles the high-card half:
   instead of scanning cold Parquet to enumerate `trace_id`/`span_id`/`user_id`
   (slow, and a useless truncated list of random IDs), it answers
   **"≈ 4.7M distinct — search by exact value"** instantly. Exact-value lookup is
   already fast and untouched.
2. **Cardinality-bomb early warning.** A buggy service spraying unique values into a
   field (a timestamp in `k8s.pod.name`) is the classic cardinality explosion.
   `lakehouse_catalog_field_cardinality{field}` graphs it and **alerts** when a field
   spikes — caught at ingest, per field, for ~16 KB.
3. **Query pre-flight / planning.** `count by (field)` on card=12 → run it; on
   card=4.7M → warn/refuse before it explodes. Also informs bloom-vs-dictionary
   encoding choices.
4. **Principled high-card classification.** A2's cap decision can use the true
   aggregate cardinality (not just a per-partition count), so a field that's low-card
   per file but high-card across the corpus is classified correctly.

**Cost is asymmetric:** ~0.5 MB total for ~30 fields, `add` is free at flush
(streamed off the row structs, 0 allocs). **Limits:** it's an estimate (±~1 %, a
hint not a ledger), it gives distinct-COUNT not per-value frequencies, and it does
not speed up *finding* a specific value (that's the trace index).

**Wiring (both modules):** the flush tap (`catalogObserver.tapLogRows`/`tapTraceRows`)
streams the configured `always_sketch_fields` id columns into `Store.AddCardinality`;
`Store.Cardinality(field)` + the gauge read it. Verified e2e
(`TestInteg_PmetaCatalog_CardinalityTapE2E`): a real `BatchWriter` flush of 5,000
`trace_id`s → `Cardinality` within 3 % and the gauge published.

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

## 6a. Observability (a deliberately small metric set)

Just enough to answer "is the catalog working, and is it within budget" — **not**
a metric per field (that would explode at PB scale).

| Metric | Type | Why it matters |
|---|---|---|
| `lakehouse_catalog_resident_bytes` | gauge | RAM the catalog holds — the budget guardrail; alert if it approaches the limit. |
| `lakehouse_catalog_fields_total{class="exact\|sketch"}` | gauge | How many fields landed exact vs count-only — shows where the threshold sits. |
| `lakehouse_catalog_value_lookups_total{source="catalog\|scan"}` | counter | Dropdown answers from the catalog vs fallback scan — the hit rate that *proves* the speedup. |
| `lakehouse_catalog_partitions{state="resident\|paged"}` | gauge | Confirms time-tiering is actually shedding cold partitions. |
| `lakehouse_catalog_cold_load_seconds` | histogram | Startup catalog-load time — the "fast even cold, not just cached" guarantee. |
| `lakehouse_catalog_compaction_repartition_total` | counter | Safety invariant — **must stay 0**; if it climbs, bitmaps may be stale (alert + dirty-rebuild). |

Six series total. Per-field cardinality is available on demand through the
existing field API, not as per-field metrics.

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
