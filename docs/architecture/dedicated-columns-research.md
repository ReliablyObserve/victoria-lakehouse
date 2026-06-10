# Dedicated columns — design research (for review)

**Status: awaiting review.** The last major compression lever (approved scope). Design
researched three ways: the architecture decision evidenced on parquet-go v0.29.0 behavior
tests, the prize measured on real stack data, Tempo's precedent verified from live docs.
Harness preserved at /tmp/dedcol (not committed).

DEDICATED COLUMNS — design research (no implementation; nothing written to the repo; harness lives in /tmp/dedcol).

=== 1. THE CORE DECISION: (a) pre-declared spare columns vs (b) runtime schema vs (c) hybrid ===

RECOMMENDATION: (a)+(c) — Tempo-style PRE-DECLARED spare columns in the typed structs, with a dynamic per-signal key<->slot mapping (config + footer KV + pmeta epoch). This IS the hybrid: physical schema static, semantics dynamic. Evidence:

(a) Pre-declared spares — what makes it cheap here:
- ONE edit point writes them everywhere: ingest maps fields to rows at exactly two switch functions — /internal/vlstorage/insert.go:mapFieldToRow (logs) and /lakehouse-traces/internal/vlstorage/insert.go:268 mapFieldToTraceRow. Slots are filled there; ALL 8 production GenericWriter sites across both modules (internal/storage/parquets3/writer.go:593/647, internal/compaction/compactor.go:580/621, internal/delete/rewriter.go:149/191, lakehouse-traces/.../writer.go:535/589; buffer_export.go feeds typed rows into the same flush writer) inherit filled slots for free because rows stay typed.
- Schema evolution PROVEN both directions on parquet-go v0.29.0 (test run, /tmp/dedcol/evol): FORWARD old file -> new struct reads clean (missing slots = ""), BACKWARD new file -> old struct reads clean (slot columns invisible). So the struct change is a one-time, reversible version bump.
- schemaFingerprint survives mapping changes: writer.go:750 fingerprints (mode, version) — bump version once for the slot columns; the compactor's same-fingerprint grouping (compactor.go:122) keeps old/new files from being merged, exactly the existing invariant. Mapping changes do NOT alter the physical schema, so no new fingerprint per key-set change — only a mapping epoch (see section 3).
- parquet-readback CI gate (scripts/ci/parquet-readback) derives encoding expectations from struct tags via reflection — spare columns propagate automatically.
- Read-path precedent already exists: tracePromotedResourceKeys/tracePromotedSpanKeys in both modules' storage_query.go already implement "typed column wins, map entry suppressed" merging — dedicated slots generalize this with a dynamic table instead of a hardcoded map.
- Tempo precedent (verified from live Grafana docs): vParquet4 = up to 10 string spares per scope (span/resource); vParquet5 = 20 string + 5 int per scope plus event scope, array option, and a "blob" option (zstd-no-dict) for high-cardinality values; candidate rule = "attributes that contribute the most to block size, even if not frequently queried"; tempo-cli analyse blocks = top-N-by-bytes (our catalog can do this online, section 4). Our footer-KV mapping is strictly better than Tempo's: Tempo's mapping lives in external block meta; ours rides standard parquet KV metadata (same slot as the existing _trace_idx precedent), so every file is self-describing for duckdb/pyarrow — satisfies the pure-Parquet portability rule.

(b) Runtime schema construction — feasible in parquet-go (hand-built parquet.NewSchema + untyped writer) but breaks, with cites:
- All 8 GenericWriter[LogRow/TraceRow] sites + GenericReader sites (compactor readLogRows/readTraceRows, query decode into []schema.LogRow, logRowToFields/traceRowToFields, datablock conversion) would move to parquet.Row/map-based codecs — slower (loses the reflection-compiled fast path), loses compile-time safety, touches both modules end to end.
- schemaFingerprint becomes config-derived and EVERY key-set change becomes a physical schema change -> compactor fences every change forever; no remap path without schema-unification logic.
- tests/parity byte-parity suite and the readback CI's struct-tag reflection both break.
- Only real gains: semantic column names visible to external tools (mitigated by footer-KV mapping + trivial duckdb view) and unlimited N (not needed: measurement saturates at ~8 keys/signal).
Verdict: cost vastly exceeds benefit. REJECT (b); adopt (a) with the dynamic-mapping half of (c).

Proposed struct shape (one edit in internal/schema/row.go, both modules inherit): per signal, 6 dict-tagged string slots (ded_str_01..06) + 6 plain string slots (ded_blob_01..06, Tempo's "blob" analog for high-card values where dict bloats). Empty unused slots are included in the measured B totals — overhead immaterial. Int slots: defer (Tempo's own rule: int spares only pay at >=5% prevalence; nothing in the measured corpus demands them — revisit on catalog evidence).

=== 2. MEASURED PRIZE === (see measurements field — headline: logs file size −8.14%, traces −21.37%, projection wins 2x–9,800x, map column itself shrinks −76%/−53–63%)

=== 3. CONFIG SURFACE + KEY-SET CHANGE / DUAL-READ DESIGN ===
- FINDING: the repo already has a documented-but-DEAD config surface: lakehouse.schema.extra_promoted (config.go:779, validated at config.go:1277, documented in docs/configuration.md:185 and open-parquet-format.md:259, ExtraPromoted plumbing in schema/registry.go) — but storage.go:279 constructs NewRegistry(profile) WITHOUT extras and the typed writers cannot write such columns. The docs promise behavior the code cannot deliver. Dedicated columns should SUPERSEDE it: deprecate extra_promoted, introduce per-signal lakehouse.schema.dedicated_columns: [{name, map: log.attributes|resource.attributes|span.attributes|scope.attributes, encoding: dict|plain (default auto from catalog cardinality), bloom: bool}], max 6+6 per signal, validated at load (slot exhaustion = config error). Per-signal per the s3-research review rule; chart already renders per-signal overrides.
- Mapping identity: a mapping EPOCH = hash of the canonical (slot->key) table. Written per file: (i) footer KV lakehouse.dedicated_columns = JSON table (self-describing, portable, _trace_idx precedent), (ii) FileInfo/file-meta facet gets a 4-byte epoch id; the epoch->table dictionary stored ONCE per pmeta bundle, not per file (economy rule: ~tens of bytes per partition, vs R3-style per-file duplication).
- Key-set change (old files have key in map, new in slot) — the DUAL-READ: resolution must be PER FILE, not per query. At plan/open time the file's epoch (from FileInfo, zero extra GETs) resolves key K: if the file's mapping has K->slot, planCols gets the slot column (projected_fetch.go planProjectedRanges keys on PathInSchema[0] — slots are top-level, no change needed) and filter pushdown routes K's PushDownCheck to the slot column's stats/dict/bloom; if not, planCols gets the map column and the existing map-scan path runs. The fields-merge in logRowToFields/traceRowToFields emits the slot value under the semantic name K, suppressing a map duplicate if present — the exact tracePromotedSpanKeys pattern, table-driven. Registry change: FieldMapping already has Origin/MapColumn/MapKey fields modeling map-origin promoted columns — the dynamic table slots straight into the existing Registry shapes.
- Un-promotion / re-promotion: purely a new epoch; old files keep serving from their recorded mapping. No backfill required (same contract the extra-promoted docs already promise).
- Compaction across epochs: v1 = fence (fold epoch into the compaction grouping key next to schemaFingerprint — partitions self-heal as retention ages old epochs out). v2 = REMAP in the compactor: it already decodes full typed rows; move slot values back to maps per source-file footer-KV mapping, then apply the target epoch — compaction becomes the migration engine (Tempo-equivalent). Recommend shipping v1, designing for v2.
- Rollback semantics (honest caveat): an old binary reading a NEW file sees neither the slot (unknown column, ignored) nor the map entry (removed at write) -> promoted attributes invisible under rollback. Options: (i) reversible flag gate like retire-sidecar-writes (recommended: dedicated_columns.enabled flag; disable stops filling slots, files written during the window still need a new-enough binary to read those keys), (ii) a dual-store grace mode (key kept in map AND slot — forfeits the size win, keeps full rollback) for the first release. Decision point.
- Empty-string ambiguity: absent key and explicit "" collapse in slots — identical to ALL existing promoted columns (service.name etc.); documented, not new.

=== 4. INTERPLAY ===
- Catalog as recommendation source: the field/value catalog facet (internal/pmeta/facet_field_catalog.go) knows fields, exact low-card value sets, and high-card flags (threshold/alwaysSketch) — exactly the dict-vs-plain slot router. What it lacks is BYTES per key. Two economy-compliant options: (i) zero-cost offline "analyse" (productize /tmp/dedcol as scripts/bench/dedicated_ab — the tempo-cli analyse equivalent; exact, no standing metadata), (ii) +16 bytes/field in the catalog facet (valueBytes+count accumulators) fed from the writer's EXISTING RawBytes loop (writer.go:689-745 already iterates every map entry — the aggregation is O(1) extra per entry), enabling an auto-suggest admin endpoint ("top keys by bytes; suggested encoding from cardinality"). Both pass the economy rule; (ii) is justified by the measured prize (8-21% of all storage + 10-9,800x projection). Suggest-only — promotion stays an explicit config/review action.
- Planner v2.5 (planned-fetch-v2-research.md Part II): thin columns flip query shapes from S3/S4 (window) into S2 planned-spans. Measured: filter on a promoted low-card key costs 4 KB–183 KB per ~22 MB file (0.02–0.8% of file) vs the 36.6 MB map column (~39%) — sails under the 15%-of-file density gate that the map column always failed. Bundle facet (R0+R1 chunk table) grows by 12 column entries per RG — tens of bytes per file against the 4.3 KB ceiling; the economy gate in the per-slice protocol already measures this.
- Blooms/labels: per-key bloom:true maps to parquet.SplitBlockFilter on the slot column plus the pmeta bloom facet OR-path; promoted keys can also join labels.go extraction and ExtractLogLabelAggregates (schema/label_aggregates.go) under the existing MaxLabelAggregateValues cap — making metadata-only stats-by work for promoted keys. Config-gated extras, not defaults.
- pmeta economy rule check: new standing metadata = per-file 4-byte epoch + per-bundle mapping table + (optional) 16 bytes/field catalog counters. Justified against measured −8/−21% total storage and 10x–9,800x filter byte reductions.

=== 5. DECISION POINTS FOR REVIEW ===
1. Approve option (a/c): spare slots 6 dict + 6 plain per signal, mapping in config + footer KV + pmeta epoch? (Tempo precedent: 10/scope in v4, 20+5 in v5.)
2. Rollback posture: flag-gated with forward-only visibility (retire-sidecar pattern) vs dual-store grace mode (forfeits size win initially)?
3. Compaction across mapping epochs: v1 fence (epoch joins the grouping key) now, v2 compactor remap later — agree to ship fence first?
4. Candidate source: offline analyse harness only, or also the +16B/field catalog byte counters with an auto-suggest endpoint?
5. Supersede the dead extra_promoted config surface (docs currently promise non-working behavior) with dedicated_columns — and fix the docs either way?
6. Traces side-finding: span.attributes carries start_time_unix_nano/end_time_unix_nano as VT-parity string duplicates (~5 MB raw per 2 files, both already/partially typed columns) — promote via slots, or address the duplication at the parity layer directly?

## Measurements (full)

All on REAL live e2e MinIO data (127.0.0.1:29000, obs-archive), decoded and re-encoded with production writer options (zstd SpeedBestCompression, MaxRowsPerRowGroup=20000, split-block blooms on service.name+trace_id), identical rows both arms. Harness: /tmp/dedcol (not in repo), full output /tmp/dedcol/out.txt.

CORPUS: 4 largest compacted-L2 LOG files (of 668 L2; 23.4–23.6 MB each; 490,187 rows) + 2 largest compacted-L2 TRACE files (of 384 L2; ~7.4 MB each; 139,030 rows).

LIVE FOOTER TRUTH (per-top-level-column compressed bytes):
- LOGS: log.attributes = 39.7% of file bytes (inventory said ~35% — confirmed, slightly higher), body 24.8%, _stream 10.4%, _stream_id 8.9%.
- TRACES: span.attributes = 37.9% + resource.attributes = 13.6% -> 51.5% of bytes in maps.

TOP KEYS BY VALUE-BYTES (logs, 490k rows): container.id 29.9 MB raw (100% rows, high-card, 64-char), service.instance.id 10.8 MB (high-card), k8s.cluster.name 7.5 MB (3 distinct), telemetry.sdk.name 6.2 MB (1 distinct), cloud.account.id 5.7 MB (1 distinct), otel.trace_id 3.1 MB (20% rows, high-card), exception.type 3.0 MB (6 distinct), format 2.9 MB (4 distinct). Traces: container.id 8.7 MB, server.address 5.4 MB (14 distinct), service.instance.id 3.1 MB, code.function 2.9 MB (10 distinct), url.full 2.8 MB (100 distinct), start/end_time_unix_nano 2.6 MB each (VT-parity duplicates), k8s.cluster.name 2.1 MB.

A/B RE-ENCODE, 8 keys promoted per signal into spare slots (dict slots for distinct<=2048, plain slots for high-card):
- LOGS: A=94,834,273 B=87,112,071 -> −8.14% total file size (per-file −8.13…−8.17%).
- TRACES: A=11,922,691 B=9,375,132 -> −21.37% (per-file −21.31/−21.42%).

PROJECTION WIN (bytes a filter on the key must fetch, summed over files; A = whole map column, B = thin dedicated column):
- LOGS: telemetry.sdk.name 36.6 MB -> 4.0 KB (9,488x); cloud.account.id -> 3.9 KB (9,827x); k8s.cluster.name -> 125 KB (300x); exception.type -> 182 KB (206x); format -> 183 KB (205x); otel.trace_id -> 1.7 MB (22x); service.instance.id -> 3.1 MB (12x); container.id -> 15.3 MB (2x).
- TRACES: server.address 5.07 MB -> 70 KB (72x); code.function -> 70 KB (72x); k8s.cluster.name 1.81 MB -> 27 KB (67x); url.full -> 124 KB (41x); start/end_time -> 168/212 KB (30x/24x); container.id 1.81 MB -> 399 KB (5x); service.instance.id -> 285 KB (6x).
- Residual map column shrinks too (wins for filters on NON-promoted keys): logs log.attributes 36.6 MB -> 8.9 MB (−76.3%); traces span.attributes −53.4%, resource.attributes −62.9%.
- Planner framing: a promoted low-card filter costs 0.02–0.8% of file bytes vs ~39% today — flips map-key filters from window/dense plans into the planned-spans S2 cell of the v2.5 strategy matrix.

SCHEMA-EVOLUTION PROOF (parquet-go v0.29.0, /tmp/dedcol/evol): old-file->new-struct read OK (slots empty), new-file->old-struct read OK (slots ignored) — the spare-column version bump is reversible at the reader level.

Caveat: e2e datagen corpus — same caveat as every benchmark section in parquet-compression-research.md; distributions (single-value resource attrs, 64-char container.id) are OTel-realistic but absolute percentages should be re-measured on production-shaped data per the per-PR benchmark protocol.
