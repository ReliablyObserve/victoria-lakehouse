# Cold-Tier Metadata Consolidation — Proposal

> Status: **proposal / design review** (not yet implemented). Driven by the
> maintainer observation that LH has many overlapping moving parts (manifest,
> several sidecars, several snapshots, several caches/indexes) that duplicate
> *lifecycle* machinery and are hard to debug/control. Companion to
> [field-value-catalog.md](field-value-catalog.md) (which should be born as a
> facet of this layer) and [performance-machinery.md](performance-machinery.md).
>
> Note: §3 below says "WAL" stays separate — read that as the **membuffer**
> (logstorage parts on PVC). There is no separate LH WAL; see the corrected
> durability note in performance-machinery.md.

# Victoria Lakehouse Cold-Tier Metadata Consolidation

## (1) OVERLAP MAP

Each row is one lifecycle concern. "Impls" counts genuinely independent code paths today; the goal is to collapse the high-count rows.

| Lifecycle concern | Independent impls today | Subsystems / locations |
|---|---|---|
| **Resident index struct** | **6** | `manifest.labelIndex` (3-level inverted); `cache.LabelIndex` (forward, value-counts); `bloomindex.PartitionedIndex` (map[part]→Index); `bloomindex.BloomCache` (per-file, neg-cache); `FooterCache` (parsed `*parquet.File`); `smartcache.MetadataMap` (`EntryMeta`) |
| **S3 sidecar format** | **5** | `_file_metadata.json` (manifest); `_bloom.bin` per-partition; `{fileKey}.bloom` per-file; `_label_index.json` (traces only); footer KV `_trace_idx` + `_bloom_body_rg_N` (in-Parquet, **must stay**) |
| **Local snapshot** | **4** | `manifest.gob` (`BMNF`); `footer-cache-snapshot.bin` (`LHFC`); `smartcache.meta.json`; `label-index.json` |
| **Write-hook (build-on-flush)** | **5** | `WritePartitionSidecar` (async post-compaction); `storageBloomObserver.OnFileFlush`→`PersistDirty`; `writeFileBloom` (async per-file upload); `PersistLabelIndexToS3` (ticker); footer-KV builders in `writer.go` (`computeTraceIndex`, `buildTokenBloomMetadata`) |
| **Parallel-GET loader** | **4** | `LoadSidecars` (16-way GET `_file_metadata.json`); `prefetchFooters` (16-way footer range-reads); `bloomS3Loader`/`BloomCache.Warm` (per-partition GET `_bloom.bin`); `loadLabelIndexFromS3` (single GET) |
| **Warmup** | **5 phases, 4 owners** | `WarmMetadata` (sidecar+footer+smallfile); `WarmLabelIndex` (sample 10 files); `BloomCache.Warm`/`BackfillBloomIndex`; `PrefetchFromCacheSnapshot`; `LoadSnapshot` (smartcache) — all separately sequenced in `runStartup` |
| **Eviction / tiering** | **5** | manifest: none; `cache.LabelIndex` LRU (field-cap); `FooterCache` LRU (item-cap); `BloomCache` LRU (1024); SmartCache L1 size-LRU (100%) + L2 watermark-LRU (80%) — **inconsistent thresholds** |
| **Dirty-tracking** | **5** | manifest incremental (`AddFile`/`RemoveFile`/cliff-guard); `PartitionedIndex.dirty` map[part]bool; `LabelIndex` LRU-touch; SmartCache `put()` implicit; footer KV = none (atomic w/ flush) |
| **Metrics namespace** | **4+** | `WriterTraceIdx*`; `BloomBuild*`; L1/L2/`ManifestSnapshotAge`; `peercache.Stats` — no shared "metadata facet" namespace, can't compare hit-rates across index types |

**Honest read of the map:** the duplication is real and concentrated in **resident structs (6), sidecar formats (5), and snapshots (4)**. But it is *not* uniform — three rows below are justified separation (see §3). The biggest, cheapest win is that **every facet is already per-partition or per-file but each invented its own S3 object, its own GET loop, and its own dirty bit.** They differ only in payload, not in lifecycle.

---

## (2) THE UNIFIED ABSTRACTION

### Core idea
One new package `internal/pmeta` ("partition metadata") owns the lifecycle (S3 object, snapshot, parallel-GET, dirty-tracking, warmup, eviction, metrics). Everything that today is "a thing persisted per-partition next to the data" becomes a **Facet** plugged into a single per-partition **Bundle**, serialized into **one S3 object per partition** (`{prefix}{partition}/_pmeta.bundle`) read with **one GET**.

Footer-embedded data (`_trace_idx`, `_bloom_body_rg_N`) **stays in the Parquet footer** (constraint: standard-tool readable, never custom framing) — but its *in-RAM lifecycle* (cache, warmup, eviction) is unified by registering a **read-only facet** whose loader reads footer KV instead of the bundle. This is the crucial subtlety: unifying the *runtime* doesn't require moving bytes out of the Parquet file.

### Interfaces (`internal/pmeta`)

```go
package pmeta

// FacetKind is a stable wire tag for one section of the bundle.
type FacetKind uint8
const (
    FacetBloom       FacetKind = 1 // was bloomindex.PartitionedIndex (_bloom.bin)
    FacetFileMeta    FacetKind = 2 // was manifest _file_metadata.json
    FacetLabels      FacetKind = 3 // was cache.LabelIndex _label_index.json
    FacetColumnStats FacetKind = 4 // was manifest column_stats / partition_stats
    FacetFieldCatalog FacetKind = 5 // NEW in-flight field/value catalog — born here
    FacetTraceIdx    FacetKind = 6 // VIRTUAL: backed by footer KV, not bundle bytes
)

// Facet is the per-partition unit of metadata. One implementation per index type.
type Facet interface {
    Kind() FacetKind
    // Encode/Decode operate on this partition's slice only — bundle frames them.
    Encode(w io.Writer) error
    Decode(r io.Reader) error
    // Merge applies newly-flushed file contributions (called from the ONE write-hook).
    Merge(delta FileContribution)
    // EstimateBytes drives the ONE eviction policy.
    EstimateBytes() int64
}

// FacetFactory builds an empty facet for a partition (registry pattern).
type FacetFactory func(partition string) Facet

// Bundle = all facets for one partition, the unit of GET/PUT/snapshot/dirty.
type Bundle struct {
    Partition string
    facets    map[FacetKind]Facet
    dirty     atomic.Bool   // ONE dirty bit per partition (replaces 5)
    sizeBytes atomic.Int64
}

// Store owns ALL lifecycle for ALL facets across ALL partitions.
type Store struct {
    reg      map[FacetKind]FacetFactory
    bundles  sync.Map // partition -> *Bundle
    pool     S3Pool   // s3reader.ClientPool adapter (exported API only)
    prefix   string
    evictor  *tieredEvictor // ONE policy, ONE watermark
    snap     *snapshotMgr   // ONE local snapshot (_pmeta-snapshot.bin)
    m        *facetMetrics  // ONE metrics namespace: pmeta_*
}
```

Lifecycle methods (each replaces N scattered ones):

```go
func (s *Store) Register(k FacetKind, f FacetFactory)                       // wire facets once
func (s *Store) OnFileFlush(partition, fileKey string, c FileContribution)  // THE write-hook
func (s *Store) PersistDirty(ctx) error                                     // THE dirty-flush (1 PUT/dirty partition)
func (s *Store) WarmPartitions(ctx, parts []string, concurrency int) error  // THE parallel-GET loader (one bundle GET each)
func (s *Store) SaveSnapshot() / LoadSnapshot()                             // THE local snapshot
func (s *Store) Get(partition string, k FacetKind) (Facet, bool)            // query-path accessor
```

### Bundle wire format (one S3 object, standard-tool-agnostic — it's a *sidecar*, not in any `.parquet`)
```
magic "LHPM\x02" | partitionLen:u16 | partition | facetCount:u8
  per facet: kind:u8 | flags:u8 | len:u32 | payload[len]
```
Self-describing and skippable: an old reader skips unknown facet kinds by `len`. Each facet's payload is its *existing* marshal format (e.g. `bloomindex.Marshal` v2 bytes go in verbatim) — so we reuse the proven serializers and only add a framing header. The bundle is a normal binary object; it touches **no `.parquet` file**, so Parquet portability is untouched.

### How each existing subsystem maps

| Existing | Becomes | Delete vs adapter |
|---|---|---|
| `bloomindex.PartitionedIndex` + `_bloom.bin` + `storageBloomObserver.PersistDirty`/`DirtyPartitions` | `bloomFacet` wrapping `bloomindex.Index` (one per partition) | **Keep** `bloomindex.Index/Marshal/Unmarshal` (proven). **Delete** `partitioned.go` dirty-map + `_bloom.bin` path in `bloom_build.go`; `storageBloomObserver` → thin adapter calling `Store.OnFileFlush`. |
| manifest `_file_metadata.json` (`FileMetaSidecar`, `WritePartitionSidecar`, `LoadSidecars`) | `fileMetaFacet` (same `rc/mn/mx/rb/sf/lb` short-key payload) | **Delete** `metadata_sidecar.go` write/parallel-load (≈206 LOC). Manifest keeps `m.files`/`byKey`/`tenantAggregates`/`sortedPartitions` and *reads* the facet during warmup via adapter. |
| `cache.LabelIndex` + `_label_index.json` (traces) + `manifest.labelIndex` | **one** `labelsFacet` (forward value-counts) + manifest's inverted view rebuilt **from** the facet | **Delete** `_label_index.json` path + traces-only divergence. `cache.persist.LabelIndex` struct kept as the facet's in-RAM type. `manifest.labelIndex` becomes a **derived view** (no second source of truth → fixes Overlap-2 truncation skew). |
| `manifest/column_stats.go`, `partition_stats.go` | `columnStatsFacet` | Logic kept, **moves** into facet; manifest calls `Store.Get(part, FacetColumnStats)`. |
| **In-flight field/value catalog** | `fieldCatalogFacet` — **born as a facet**, never a 4th sidecar | New code, ~1 facet impl, ~0 new lifecycle. |
| `_trace_idx` footer KV + `traceindex` | `traceIdxFacet` (**virtual**: `Decode` reads from cached `*parquet.File` footer KV, `Encode` is a no-op — footer is written by `writer.go` as today) | **Keep** `traceindex.go` + footer write in `writer.go` **unchanged** (portability). Only the *cache/warmup/metrics* unify. |
| Token bloom `_bloom_body_rg_N` | **stays in footer**, surfaced via `FooterCache` (unchanged) | Not a bundle facet — row-group-scoped, lives with row-group data. (§3) |
| `FooterCache` + `footer-cache-snapshot.bin` | Stays its own cache **but** registers with the `tieredEvictor` + `pmeta_*` metrics | Adapter only — footers are *parsed objects*, not bundle bytes (§3). |
| 4 local snapshots | **one** `_pmeta-snapshot.bin` (bundle keys + warm hints) | **Delete** `footer-cache-snapshot.go` format, `smartcache.meta.json`, `label-index.json` *snapshot* paths (the in-RAM types live on). |
| 5 dirty mechanisms | `Bundle.dirty` atomic bool/partition | **Delete** `PartitionedIndex.dirty`, LRU-touch-as-dirty, etc. |
| 4 metrics namespaces | `pmeta_facet_{bytes,gets,puts,hits,misses,evictions}{facet=}` | Old metrics kept as aliases one release, then removed. |

Net: `internal/bloomindex` (math), `internal/traceindex` (footer codec), `cache.LabelIndex` (struct) **survive as libraries**; their **lifecycle glue is deleted** and replaced by ~3–4 small `Facet` impls + one `pmeta.Store`.

---

## (3) WHAT STAYS SEPARATE (and why — not everything should merge)

1. **WAL / membuffer** — different *durability class*. WAL is the write path's crash-recovery log (replay gates `/ready`); pmeta is read-optimization metadata that is always *re-derivable from S3*. Merging would couple a correctness-critical subsystem to a rebuildable cache. **Keep separate.**

2. **SmartCache L2 raw bytes (`DiskCache`)** — this is **bulk column-chunk data**, not metadata. It's GBs, watermark-evicted, peer-shardable. pmeta bundles are KBs/partition. Sharing one tier would let cold data evictions thrash hot metadata. **Keep separate**, but align the *eviction watermark constant* (fixes the 80%/100% inconsistency) and share the `pmeta_*` metric verbs for comparability.

3. **PeerCache (HRW ring) + Discovery** — membership/topology, not metadata payload. Orthogonal lifecycle (stabilization, gossip). **Keep separate.** (The map already flags these as "OK as-is" — agreed.)

4. **Token blooms (`_bloom_body_rg_N`)** — row-group-scoped full-text, physically interleaved with row-group data in the footer. Hoisting them into a partition bundle would *break* the row-group-skip locality and duplicate bytes already in the Parquet file. **Stays in footer**, accessed via `FooterCache`. This is a case where the "bloom family overlap" is **justified separation**, not duplication — different granularity (row-group vs file vs partition) is a feature.

5. **FooterCache** — holds *parsed `*parquet.File` objects*, not serializable bytes; it's a CPU-parse cache, not an I/O sidecar. It registers with the unified evictor/metrics but keeps its own snapshot-of-keys (footers are re-fetched, never persisted parsed). **Adapter, not merge.**

6. **`tenant/aliases`, `delete/tombstones`, `stats/registry`** — these are **control-plane / CRDT** state with their own consistency models (alias gossip, soft-delete markers, per-node delta merge). They are not per-partition read-acceleration. **Keep separate.** Tombstones in particular must not be gated behind metadata warmup.

The **logs/traces `main.go` duplication (~1200 LOC, 50+ functions, parity markers)** is a *separate* consolidation problem — real, but it's orchestration duplication, not metadata duplication. pmeta *reduces* it (one `Store` wired identically in both mains shrinks the parity surface) but the main.go merge should be its own track. Don't conflate.

---

## (4) MIGRATION PATH (incremental, each step shippable)

Guardrail for every step: **no VL/VT upstream edits**, **no `.parquet` framing change**, and the tasks 71–83 `/ready` gates (`ServingReady`/`WarmupComplete`/WAL) must pass unchanged — pmeta plugs *into* the existing `runStartup` phases, it doesn't replace them.

- **Step 0 — Scaffold (`internal/pmeta`), no behavior change.** Land `Store`, `Facet`, `Bundle`, registry, bundle codec + golden-file tests. Nothing wired. Shippable: dead code behind no flag.

- **Step 1 — Dual-write bloom facet.** `bloomFacet` writes into the bundle **and** the old `_bloom.bin` (dual-write). Reads still from `_bloom.bin`. Verify byte-identical payload via the existing `bloom_compaction_test.go`. Ship.

- **Step 2 — Flip bloom reads to bundle**, behind `--pmeta.bloom=read` flag; keep `_bloom.bin` write one more release for rollback. Delete `_bloom.bin` write + `partitioned.go` dirty-map in the step that removes the flag. This is the riskiest read-flip — do bloom first because it has the strongest existing test coverage (fuzz + compaction + integration).

- **Step 3 — Fold `_file_metadata.json` → `fileMetaFacet`.** Same dual-write→flip. `LoadSidecars` becomes `Store.WarmPartitions` filtered to `FacetFileMeta`. Manifest now reads file meta from the facet; `m.files` enrichment path unchanged → **cliff-guard and `RefreshFromS3` semantics preserved.**

- **Step 4 — Field/value catalog is born as `fieldCatalogFacet`.** The in-flight work targets the bundle directly — **no 4th sidecar, no new GET loop, no new snapshot, no new dirty bit.** It inherits warmup/eviction/metrics for free. This is the step that pays back the whole investment: a brand-new index ships in ~one facet file instead of replicating the 5-row lifecycle from §1.

- **Step 5 — Fold labels.** Collapse `cache.LabelIndex` S3 path + `manifest.labelIndex` into `labelsFacet`; make manifest's inverted index a derived view. Removes the traces-only `_label_index.json` divergence and the truncation skew (Overlap-2).

- **Step 6 — Virtualize trace-idx + register FooterCache with unified evictor/metrics.** No byte movement; just unify cache/warmup/metrics. `column_stats` folds in here too.

- **Step 7 — One snapshot, one metrics namespace, retire aliases.** Replace the 4 local snapshots with `_pmeta-snapshot.bin`; drop legacy metric names after one release.

Each step: one facet, dual-write, flip, delete-old. Rollback = flip flag back. No big-bang.

---

## (5) BEFORE / AFTER + BLUNT ASSESSMENT

### Component count

| Concern | Before | After |
|---|---|---|
| Resident index structs | 6 | 6 *types* but **1 lifecycle owner** (`pmeta.Store`) + FooterCache + SmartCache |
| S3 sidecar objects / partition | 3 (`_file_metadata.json`, `_bloom.bin`, `{file}.bloom`) + 1 global (`_label_index.json`) | **1** (`_pmeta.bundle`) + footer KV (unchanged) |
| S3 GETs to warm a partition | 3–4 | **1** |
| Local snapshots | 4 | **1** |
| Write-hooks (sidecar) | 5 | **1** (`Store.OnFileFlush` + `PersistDirty`) |
| Parallel-GET loaders | 4 | **1** (`WarmPartitions`) |
| Dirty mechanisms | 5 | **1** |
| Eviction policies/thresholds | 5 (incl. 80%/100% split) | **1** tiered policy (+ SmartCache L2 aligned) |
| Metrics namespaces | 4+ | **1** (`pmeta_*`) |

### Gains (real, not oversold)
- **One GET/partition** is the headline operational win: cold-Jaeger/warmup latency drops with fewer round-trips, and the 16-way loaders collapse to one fan-out — directly helps the cold-start class of bugs this branch is already chasing.
- **One dirty bit + one snapshot** means a single, debuggable answer to "is partition X's metadata current?" Today that's spread across 5 places with no joint view.
- **Adding an index = adding a Facet.** The field/value catalog proves it: no new lifecycle. This is the strongest argument — it changes the *marginal cost of future indexes* from "replicate 9 lifecycle concerns" to "implement `Encode/Decode/Merge`."
- **One metrics namespace** finally lets you compare facet hit-rates/sizes/evictions side-by-side.

### Risks / costs (honest)
- **Bundle = one object** is also a **failure-domain concentration**: a corrupt bundle loses *all* facets for that partition vs. losing one `_bloom.bin`. Mitigation: per-facet `len` framing + CRC so a bad facet is skipped, not fatal; cliff-guard analog at bundle level.
- **Read-flip risk** (Steps 2/3/5): the dual-write→flip dance is where regressions hide. Mandatory byte-identical golden tests per facet before flip; keep old-path write one release.
- **Coupling warmup phases**: pmeta must slot into existing `runStartup` phases without changing `/ready` gate timing (tasks 71–83). If `WarmPartitions` is slower-to-first-serve than the old staggered warmup, `ServeWhileWarming` semantics could shift. Mitigation: facet-priority warmup (file-meta first, bloom/labels lazy).
- **Migration is ~7 PRs of churn** across two modules with parity markers — real engineering cost, mostly mechanical, spread over releases.
- **Don't over-merge**: §3 lists six things that look like overlap but are justified separation. If the team merges WAL or L2-bytes into pmeta to chase a lower component count, that's a regression dressed as consolidation.

### Bottom line
The duplication in §1 rows for **sidecar formats (5→1), GET loaders (4→1), snapshots (4→1), dirty-tracking (5→1)** is genuine and worth collapsing — these differ only in payload, not lifecycle. The **bloom-granularity "overlaps" (row-group/file/partition), WAL, L2 bytes, peercache, and control-plane state are justified separation** and should stay split. The single highest-leverage outcome is making the **in-flight field/value catalog a facet**, which both validates the abstraction and avoids minting a 4th parallel sidecar lifecycle.

Key grounding files: `/Users/slawomirskowron/claude_projects/victoria-lakehouse/internal/manifest/metadata_sidecar.go` (sidecar to fold), `/Users/slawomirskowron/claude_projects/victoria-lakehouse/internal/bloomindex/partitioned.go` (dirty-map to delete), `/Users/slawomirskowron/claude_projects/victoria-lakehouse/internal/storage/parquets3/bloom_build.go` (`_bloom.bin` path + `PersistDirty` to adapter), `/Users/slawomirskowron/claude_projects/victoria-lakehouse/internal/cache/persist.go` (`LabelIndex` struct kept as facet type), `/Users/slawomirskowron/claude_projects/victoria-lakehouse/internal/traceindex/traceindex.go` + `/Users/slawomirskowron/claude_projects/victoria-lakehouse/internal/storage/parquets3/trace_index.go` (footer codec — stays, virtual facet), `/Users/slawomirskowron/claude_projects/victoria-lakehouse/internal/storage/parquets3/footer_cache.go` (adapter, not merge), `/Users/slawomirskowron/claude_projects/victoria-lakehouse/cmd/lakehouse-logs/main.go` + `/Users/slawomirskowron/claude_projects/victoria-lakehouse/lakehouse-traces/main.go` (`runStartup` wiring point; parity duplication is a separate track). New package to create: `internal/pmeta`.

---

## (6) Decision: skip+rebuild self-heal → single flagged PR (supersedes the 7-step migration)

**Key property:** every facet is a *cache of data re-derivable from S3* (Parquet files
+ footers are the source of truth). So a corrupt / missing / unknown-version facet is
**never data loss** — it self-heals:

1. **skip** — a per-facet `len`+CRC mismatch (or unregistered `FacetKind`) is skipped,
   not fatal; the rest of the bundle loads.
2. **mark dirty** — the partition's failed facet is flagged (`DecodeResult.Skipped`).
3. **rebuild** — re-derived from that partition's files via the same `OnFileFlush`
   extraction, on next access or a background sweep.

Worst case is one partition doing one slow rebuild; it self-heals. (If the *length*
header itself is corrupt the stream desyncs and the whole bundle fails decode → the
whole partition rebuilds — coarser, still self-healing.)

**This changes the delivery decision.** Because corruption self-heals and the layer is
re-derivable, the cautious dual-write/flip dance is **not** needed for *data* safety.
The work lands as **one PR behind a master `--pmeta` flag**:

- `--pmeta=off` (default) → today's behavior, untouched = the rollback path.
- `--pmeta=on` → unified layer; missing/corrupt facets rebuild from S3 on access.

What still gates flipping the flag on (the *correctness* risk, which does NOT self-heal):
**per-facet byte-identical golden tests** (reuse `bloom_compaction_test.go`, sidecar
fuzz), **`/ready` timing unchanged** (facet-priority warmup, tasks 71–83 gates pass),
and **the field/value catalog is a facet from day one** (no standalone sidecar).

So: **skip+rebuild = data-safety net · master flag = rollback · golden tests + `/ready`
parity = correctness gate.** One PR, reversible by flag, self-healing on corruption.

### New A1 (this is what's being built now)
`internal/pmeta` scaffold (Facet/Bundle/Store + bundle codec with per-facet len+CRC
framing + skip-on-corruption) **plus** the field/value catalog as the first real facet
(`FacetFieldCatalog`: interned dict + per-partition roaring bitmaps + HLL), behind
`--pmeta`. Subsequent commits on the same PR fold bloom/file-meta/labels facets.

### Bundle codec safeguards (hardened — pmeta is load-bearing)

The bundle wire format is **v2**: a CRC-protected **table of contents (TOC)**
precedes the payloads.

```
magic[4] | version[1]
partLen[2] | partition
facetCount[1]
tocCRC[4]                                  crc32 over the TOC bytes
TOC: facetCount × { kind | flags | len[4] | payloadCRC[4] }  (sorted by kind)
payloads: facetCount × payload[len]        (TOC order)
```

Why the TOC matters: payload lengths live in the **CRC-protected** TOC, not inline
with the (untrusted) payload bytes. So:
- **Payload corruption is isolated to one facet** — its `payloadCRC` fails, that
  facet is skipped (→ rebuild), and the reader still finds the next payload by its
  TOC length. **No desync**, the sibling facets load fine. (The v1 format could
  desync the whole bundle on a corrupt length byte — fixed.)
- **TOC corruption is caught by `tocCRC`** → structural error → rebuild the whole
  partition (rare; the TOC is tiny).
- **Bounded allocation** — `facetCount ≤ 255`, `len ≤ 256 MB/facet`, `≤ 1 GB/bundle`
  validated *before* allocating, so a corrupt count/len can't OOM the process.
- **`Encode` holds one read lock** across all facet serialization, mutually
  exclusive with `OnFileFlush`'s write lock — no Merge-vs-Encode data race.

Test coverage (all green, `-race`): round-trip + deterministic encoding (golden),
**payload-corruption-isolated** (sibling survives), **corrupt-TOC → structural
error**, **truncate-at-every-offset never panics**, garbage-input never panics,
empty bundle, concurrent flush/encode/dirty under `-race`, and two CI fuzzers
(`FuzzDecodeBundle` ~10M execs/0 panics, `FuzzRoundTrip`) wired into
`fuzz-stress-memleak.yaml` alongside the other decoder fuzzers.

## (7) Parity gates — required at every phase

pmeta replaces correctness-critical metadata, so **every phase ships parity
verification at three levels**, and a phase does not flip its `--pmeta` read flag
until all three are green. Parity is not a one-time check; it is a standing gate
per facet.

**Level 1 — unit parity (in `internal/pmeta`, per facet).** Already enforced for
the field/value catalog (`facet_field_catalog_test.go`):
- **facet == ground truth** — the facet's answer equals the exact distinct set
  present in the source contributions (no missing values, no extras).
- **persisted == resident** — encode through the bundle codec, decode into a
  fresh dict, answers are identical.
- **rebuild == original** — a corrupt facet is skipped and rebuilt from the
  partition's files; the rebuilt facet answers identically (self-heal parity).
- **deterministic golden encode** — same content, any merge order → byte-identical
  payload.

**Level 2 — cross-path parity (when a facet is wired, behind `--pmeta`).** Each
facet must match the legacy artifact it replaces, asserted in an integration test
over real Parquet:
| Facet | Must equal |
|---|---|
| field-catalog | `field_values`/`field_names` from the legacy `labelIndex` scan |
| bloom | `_bloom.bin` payload + every file-skip decision |
| file-meta | `_file_metadata.json` Labels/ColumnStats/RowCount per file |
| labels | `_label_index.json` value sets (logs gains parity with the traces-only path) |

**Level 3 — integration / e2e parity (whole system).** With `--pmeta=on` the live
behavior must match `--pmeta=off`:
- `field_values`/`field_names`/Tempo `tag-values` dropdowns return identical sets;
- **cold Jaeger/Tempo trace parity unchanged** (the existing `/loop` parity gate —
  cold vs hot counts, `{nestedSetParent<0}` — must stay green with pmeta on);
- `/ready` timing and the tasks 71–83 gates unchanged (facet-priority warmup);
- **logs AND traces** modules both verified (no parity-marker drift).

The flag flips per facet only after Levels 1–3 pass for that facet; a regression
at any level reverts the flag (data is safe regardless via skip+rebuild).

## (8) Implementation progress (PR #127)

- [x] **Scaffold** — `Facet`/`Bundle`/`Store`, hardened v2 TOC codec (per-facet
  len+CRC, isolated corruption, bounded alloc), 12 tests + 2 fuzzers under `-race`.
- [x] **Field/value catalog facet** — interned `Dict` + per-partition value sets,
  exact typeahead, Level-1 parity tests (ground-truth / persisted==resident /
  rebuild==original / deterministic golden).
- [x] **Persistence + cold-load** — `ObjectStore` interface, `PersistDirty` (one
  PUT/dirty-partition), `WarmPartitions` (one GET/partition, bounded concurrency);
  missing/corrupt bundle → `NeedsRebuild`, per-facet failure → `SkippedFacets`
  (self-heal routing). Round-trip parity + missing/corrupt/skip tests under `-race`.
- [x] **`--pmeta` flag + flush/read wiring** — `config.Pmeta.Enabled` (off by
  default); constructor builds the catalog + `catalogObserver`; the writer feeds it
  at both flush sites with the already-extracted label map; `GetFieldValues` has a
  catalog fast-path that unions values across the range's partitions (nil/empty →
  legacy path unchanged). All builds; existing storage tests green with the flag off.
- [ ] **Level-2 cross-path parity test** — drive the real flush path with `--pmeta`
  on, assert `GetFieldValues` (catalog) == the labelIndex/scan result AND that the
  catalog actually served. The gate before enabling the flag anywhere.
- [ ] **S3 persist/warm wiring** — `ObjectStore` adapter + `PersistDirty` on the
  flush/snapshot cycle + `WarmPartitions` in `runStartup` (one GET/partition).
- [ ] **A2** — HLL high-card layer + `IsHighCard` refusal.
- [ ] **A3** — time-tiered residency + traces `span_attr:*`.
- [ ] **Fold existing facets** — bloom / file-meta / labels (dual-write → flip).