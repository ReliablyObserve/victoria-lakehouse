package parquets3

import (
	"context"
	"math"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// poolObjectStore adapts s3reader.ClientPool to pmeta.ObjectStore so partition
// bundles can be persisted to / loaded from S3. WarmPartitions treats any GET
// error as "rebuild", so no ErrNotFound translation is needed here.
type poolObjectStore struct{ pool *s3reader.ClientPool }

func (o poolObjectStore) PutObject(ctx context.Context, key string, data []byte) error {
	return o.pool.Upload(ctx, key, data)
}

func (o poolObjectStore) GetObject(ctx context.Context, key string) ([]byte, error) {
	return o.pool.Download(ctx, key)
}

// catalogFileMetaProvider adapts the pmeta fileMetaFacet to manifest.FileMetaProvider,
// so the manifest enriches FileInfo from the in-RAM bundle instead of per-partition
// `_file_metadata.json` S3 GETs (the file-meta read-flip).
type catalogFileMetaProvider struct{ store *pmeta.Store }

func (p catalogFileMetaProvider) FileMeta(partition, fileKey string) (manifest.FileMeta, bool) {
	// The pmeta facet is keyed by the tenant-isolated partition (full key dir),
	// while the manifest keys files by the pure dt=/hour= partition — so derive
	// the facet partition from the file key, not the manifest-supplied one, or
	// EnrichFromProvider's lookup would always miss.
	v, ok := p.store.FileMeta(manifest.ExtractTenantPartition(fileKey), fileKey)
	if !ok {
		return manifest.FileMeta{}, false
	}
	return manifest.FileMeta{
		RowCount:          v.RowCount,
		MinTimeNs:         v.MinTimeNs,
		MaxTimeNs:         v.MaxTimeNs,
		RawBytes:          v.RawBytes,
		SchemaFingerprint: v.SchemaFingerprint,
		Labels:            v.Labels,
	}, true
}

// defaultCardinalityThreshold keeps every human-meaningful facet exact while
// bounding RAM for unbounded id-like fields.
const defaultCardinalityThreshold = 50000

// This file is the (additive) wiring surface between the storage layer and the
// unified partition-metadata layer (internal/pmeta). Everything here is inert
// unless the --pmeta feature is enabled: with it off, no catalogObserver is set
// and no pmeta.Store is constructed, so the hot flush/query paths are unchanged.
// The S3 ObjectStore adapter + persist/warm wiring lands with the cold-load
// commit. See docs/architecture/metadata-consolidation.md.

// newCatalogStore builds a pmeta.Store registered with the field/value catalog
// facet, keyed under the given S3 prefix. Returns nil when pmeta is disabled, so
// callers can `if st := newCatalogStore(...); st != nil { … }` without a flag
// check at every site.
func newCatalogStore(cfg config.PmetaConfig, prefix string) *pmeta.Store {
	if !cfg.Enabled {
		return nil
	}
	threshold := cfg.CardinalityThreshold
	if threshold == 0 {
		threshold = defaultCardinalityThreshold
	}
	s := pmeta.NewStore()
	s.SetPrefix(prefix)
	dict := pmeta.NewDict()
	s.SetDict(dict) // include the interned strings in ResidentBytes
	s.Register(pmeta.FacetFieldCatalog,
		pmeta.NewFieldCatalogFactoryCapped(dict, threshold, effectiveSketchFields(cfg.AlwaysSketchFields)))
	s.Register(pmeta.FacetFileMeta, pmeta.NewFileMetaFactory()) // dual-write of _file_metadata.json
	s.Register(pmeta.FacetBloom, pmeta.NewBloomFactory(0.01))   // dual-write of _bloom.bin (same fpRate)
	return s
}

// catalogObserver feeds flushed file contributions into the pmeta facets: a
// nil-safe observer set on the BatchWriter, invoked at flush with the
// already-extracted label/bloom maps — no extra column scan. Nil (pmeta off,
// the degraded mode) → no-op.
type catalogObserver struct {
	store  *pmeta.Store
	sketch map[string]bool      // always-sketch field names to tap for cardinality
	pool   *s3reader.ClientPool // for persisting dirty bundles to S3 (nil → no persist)
}

// persistDirty writes changed partition bundles to S3 (one PUT per dirty
// partition), so facets that can't be re-derived from the manifest — bloom in
// particular — survive a cold restart. Called on the flush cycle next to the
// legacy bloom sidecar persist. Best-effort: a failure just defers to the next
// flush (the bundle stays dirty).
func (o *catalogObserver) persistDirty(ctx context.Context) {
	if o == nil || o.store == nil || o.pool == nil {
		return
	}
	_, _ = o.store.PersistDirty(ctx, poolObjectStore{o.pool})
}

func (o *catalogObserver) OnFileFlush(partition string, fi manifest.FileInfo, labels, bloomValues map[string][]string) {
	if o == nil || o.store == nil {
		return
	}
	// A label list at the extractor cap may be incomplete: the catalog marks the
	// field high-card (scan answers it exactly) instead of ever serving a
	// silently truncated list as authoritative.
	var truncated []string
	for field, vals := range labels {
		if len(vals) >= maxLabelsPerField {
			truncated = append(truncated, field)
		}
	}
	o.store.OnFileFlush(pmeta.FileContribution{
		Partition:         partition,
		FileKey:           fi.Key,
		RowCount:          fi.RowCount,
		MinTimeNs:         fi.MinTimeNs,
		MaxTimeNs:         fi.MaxTimeNs,
		RawBytes:          fi.RawBytes,
		SchemaFingerprint: fi.SchemaFingerprint,
		Labels:            labels,
		BloomValues:       bloomValues,
		TruncatedFields:   truncated,
	})
	metrics.CatalogResidentBytes.Set(o.store.ResidentBytes())
}

// tapLogRows folds the configured always-sketch id columns (trace_id, span_id)
// from a flushed file's log rows into their per-field HLL, streaming straight off
// the row structs (no slice materialized), then updates the cardinality gauge.
func (o *catalogObserver) tapLogRows(partition string, rows []schema.LogRow) {
	if o == nil || o.store == nil || len(rows) == 0 {
		return
	}
	// Dimensional explorer fields: accurate global per-field HLL fed on every
	// flush so the Cardinality Explorer reports REAL distinct counts (not the
	// lazy, 100-capped LabelIndex). Twin of the root module.
	for _, c := range schema.LogLabelColumns {
		col := c
		o.store.AddCardinality(col.Name, func(yield func(string) bool) {
			for i := range rows {
				if !yield(col.Get(&rows[i])) {
					return
				}
			}
		})
	}
	for _, c := range schema.LogSketchIDColumns {
		col := c
		if !o.sketch[col.Name] {
			continue
		}
		o.sketchID(partition, col.Name, func(yield func(string) bool) {
			for i := range rows {
				if !yield(col.Get(&rows[i])) {
					return
				}
			}
		})
	}
}

// tapTraceRows is tapLogRows for trace rows.
func (o *catalogObserver) tapTraceRows(partition string, rows []schema.TraceRow) {
	if o == nil || o.store == nil || len(rows) == 0 {
		return
	}
	// Dimensional explorer fields — see tapLogRows.
	for _, c := range schema.TraceLabelColumns {
		col := c
		o.store.AddCardinality(col.Name, func(yield func(string) bool) {
			for i := range rows {
				if !yield(col.Get(&rows[i])) {
					return
				}
			}
		})
	}
	for _, c := range schema.TraceSketchIDColumns {
		col := c
		if !o.sketch[col.Name] {
			continue
		}
		o.sketchID(partition, col.Name, func(yield func(string) bool) {
			for i := range rows {
				if !yield(col.Get(&rows[i])) {
					return
				}
			}
		})
	}
}

// sketchID feeds an always-sketch id column's per-file values into BOTH the global
// RAM sketch (the live Cardinality Explorer metric, Store.Cardinality) and the
// partition's PERSISTED catalog HLL (Store.AddPartitionCardinality) so the distinct
// count survives restart via the bundle — the durability gap where trace_id/span_id
// reset on restart (their sketch was RAM-only). vals is replayed once per sink,
// straight off the row structs (no slice materialized).
func (o *catalogObserver) sketchID(partition, field string, vals func(func(string) bool)) {
	o.store.AddCardinality(field, vals)
	o.store.AddPartitionCardinality(partition, field, vals)
	metrics.CatalogFieldCardinality.Set(field, int64(o.store.Cardinality(field)))
}

// effectiveSketchFields unions the operator-configured always-sketch fields with
// the built-in schema.DefaultSketchIDColumns (container.id, service.instance.id,
// the trace ids), so the promoted id columns are sketched + persisted out of the
// box even when an operator's YAML replaces always_sketch_fields with its own
// list. Configured fields first, then any defaults not already present.
func effectiveSketchFields(cfgFields []string) []string {
	out := make([]string, 0, len(cfgFields)+len(schema.DefaultSketchIDColumns))
	seen := make(map[string]bool, len(cfgFields)+len(schema.DefaultSketchIDColumns))
	for _, f := range cfgFields {
		if f != "" && !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	for _, f := range schema.DefaultSketchIDColumns {
		if !seen[f] {
			seen[f] = true
			out = append(out, f)
		}
	}
	return out
}

// sketchSet builds the always-sketch field lookup from config, with the built-in
// id columns unioned in (see effectiveSketchFields).
func sketchSet(fields []string) map[string]bool {
	eff := effectiveSketchFields(fields)
	if len(eff) == 0 {
		return nil
	}
	m := make(map[string]bool, len(eff))
	for _, f := range eff {
		m[f] = true
	}
	return m
}

// catalogFieldValues unions a field's distinct values across the partitions
// overlapping the query's time range, served from the pmeta catalog in RAM.
// Returns nil when the catalog has nothing for the range (cold or field not
// catalogued) so the caller falls through to the legacy labelIndex/scan path.
// Caller guarantees s.catalog != nil.
func (s *Storage) catalogFieldValues(q *logstorage.Query, fieldName string, limit uint64) []logstorage.ValueWithHits {
	startNs, endNs := q.GetFilterTimeRange()
	seen := make(map[string]struct{}, 16)
	valset := make(map[string]struct{})
	// Bounded uint64→int conversion (the facet API takes int; limit can originate
	// from a parsed query param). Clamp to MaxInt32 — the platform-independent int
	// bound — so the conversion is safe even where int is 32-bit (CodeQL
	// go/incorrect-integer-conversion). 0 = no cap; a dropdown never needs 2^31 values.
	catLimit := math.MaxInt32
	if limit < math.MaxInt32 {
		catLimit = int(limit)
	}
	for _, fi := range s.manifest.GetFilesForRange(startNs, endNs) {
		p := manifest.ExtractTenantPartition(fi.Key)
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		for _, v := range s.catalog.FieldValues(p, fieldName, "", catLimit) {
			valset[v] = struct{}{}
		}
	}
	if len(valset) == 0 {
		metrics.CatalogValueLookups.Add("scan", 1) // catalog missed → legacy path
		return nil
	}
	vals := make([]string, 0, len(valset))
	for v := range valset {
		vals = append(vals, v)
	}
	sort.Strings(vals)
	if limit > 0 && uint64(len(vals)) > limit {
		vals = vals[:limit]
	}
	out := make([]logstorage.ValueWithHits, len(vals))
	for i, v := range vals {
		out[i] = logstorage.ValueWithHits{Value: v, Hits: 1}
	}
	metrics.CatalogValueLookups.Add("catalog", 1) // served from RAM
	return out
}

// catalogFieldNames unions the field names across the partitions overlapping the
// query's time range, served from the pmeta catalog in RAM. Returns nil when the
// catalog has nothing for the range so the caller falls through to the legacy
// labelIndex. Caller guarantees s.catalog != nil.
func (s *Storage) catalogFieldNames(q *logstorage.Query) []string {
	startNs, endNs := q.GetFilterTimeRange()
	seen := make(map[string]struct{}, 16)
	nameset := make(map[string]struct{})
	for _, fi := range s.manifest.GetFilesForRange(startNs, endNs) {
		p := manifest.ExtractTenantPartition(fi.Key)
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		for _, n := range s.catalog.FieldNames(p) {
			nameset[n] = struct{}{}
		}
	}
	if len(nameset) == 0 {
		return nil
	}
	names := make([]string, 0, len(nameset))
	for n := range nameset {
		names = append(names, n)
	}
	sort.Strings(names)
	return names
}

// WarmCatalog builds the field/value catalog from the manifest's per-file label
// maps at startup, so dropdowns are fast on the FIRST query after a cold start —
// not just after a local flush. The manifest is already resident (no extra S3
// I/O), and it is the source of truth, so this is also the self-heal path: the
// catalog is fully re-derivable from it. No-op when --pmeta is disabled.
func (s *Storage) WarmCatalog(ctx context.Context) {
	if s.catalog == nil {
		return
	}
	for _, files := range s.manifest.AllFiles() {
		if ctx.Err() != nil {
			return
		}
		for _, fi := range files {
			// Replay, not flush: manifest-derived content is already durable, so
			// it must NOT mark bundles dirty (a dirty mark here re-PUT every
			// partition bundle on the first flush after every restart). Key the
			// facet by the tenant-isolated partition (derived from the file key),
			// matching the live writer flush — NOT the manifest's pure dt=/hour=
			// key, or the warmed facet wouldn't be found by the tenant-scoped reads.
			s.catalog.OnFileReplay(pmeta.FileContribution{
				Partition:         manifest.ExtractTenantPartition(fi.Key),
				FileKey:           fi.Key,
				RowCount:          fi.RowCount,
				MinTimeNs:         fi.MinTimeNs,
				MaxTimeNs:         fi.MaxTimeNs,
				RawBytes:          fi.RawBytes,
				SchemaFingerprint: fi.SchemaFingerprint,
				Labels:            fi.Labels,
			})
		}
	}
	metrics.CatalogResidentBytes.Set(s.catalog.ResidentBytes())
}

// WarmCatalogFromS3 loads any persisted partition bundles from S3 (one GET each),
// restoring facets that WarmCatalog can't rebuild from the manifest — the bloom
// facet in particular. Missing/corrupt bundles are ignored here: WarmCatalog
// covers catalog + file-meta from the manifest, and the legacy bloom sidecar is
// the fallback for bloom during dual-write. Call this BEFORE WarmCatalog so the
// manifest merge re-adds any files newer than the persisted bundle.
func (s *Storage) WarmCatalogFromS3(ctx context.Context) {
	if s.catalog == nil || s.pool == nil {
		return
	}
	// Warm by the TENANT-isolated partition (the full key dir) — bundles are
	// persisted under it (matching the live flush), NOT under the manifest's pure
	// dt=/hour= partition. Group the manifest's files by their tenant partition so
	// both the bundle GETs and the self-heal rebuild key the same way the bundle
	// was written; a mismatch would miss every bundle and silently drop the bloom
	// facet (which is NOT rebuildable from the manifest) on every cold restart.
	filesByTP := make(map[string][]manifest.FileInfo, 64)
	for _, files := range s.manifest.AllFiles() {
		for _, fi := range files {
			tp := manifest.ExtractTenantPartition(fi.Key)
			if tp == "" {
				continue
			}
			filesByTP[tp] = append(filesByTP[tp], fi)
		}
	}
	if len(filesByTP) == 0 {
		return
	}
	parts := make([]string, 0, len(filesByTP))
	for tp := range filesByTP {
		parts = append(parts, tp)
	}
	res := s.catalog.WarmPartitions(ctx, poolObjectStore{s.pool}, parts, 8)

	// Self-heal: a partition whose bundle was missing/corrupt (NeedsRebuild) or
	// partially unusable (SkippedFacets) is rebuilt from the manifest's files —
	// DIRTY, so the repaired bundle persists and replaces the broken S3 object.
	// (Without this the next persistDirty would overwrite the S3 bundle with
	// whatever partial state RAM happened to have.) Bloom content from a lost
	// bundle is not manifest-derivable; new flushes repopulate it.
	rebuild := res.NeedsRebuild
	for p := range res.SkippedFacets {
		rebuild = append(rebuild, p)
	}
	for _, p := range rebuild {
		files := filesByTP[p]
		cs := make([]pmeta.FileContribution, 0, len(files))
		for _, fi := range files {
			cs = append(cs, pmeta.FileContribution{
				FileKey:           fi.Key,
				RowCount:          fi.RowCount,
				MinTimeNs:         fi.MinTimeNs,
				MaxTimeNs:         fi.MaxTimeNs,
				RawBytes:          fi.RawBytes,
				SchemaFingerprint: fi.SchemaFingerprint,
				Labels:            fi.Labels,
			})
		}
		s.catalog.Rebuild(p, cs)
	}
	if len(rebuild) > 0 {
		logger.Infof("pmeta warm: loaded=%d rebuilt=%d (missing/corrupt bundles re-derived from the manifest)", res.Loaded, len(rebuild))
	}
}

// PmetaOnCompacted folds a compaction result into the pmeta facets: the output
// file's catalog/file-meta contribution is added and the merged-away inputs are
// removed (dead keys otherwise accumulate in RAM and in the persisted bundle
// forever). The output gets NO bloom entry — blooms of differently-sized inputs
// cannot be unioned without re-scanning, and an ABSENT bloom key is always kept
// (sound: compacted files are never wrongly excluded, just not bloom-pruned).
func (s *Storage) PmetaOnCompacted(added []manifest.FileInfo, removed []string) {
	if s.catalog == nil {
		return
	}
	for _, fi := range added {
		var truncated []string
		for field, vals := range fi.Labels {
			if len(vals) >= maxLabelsPerField {
				truncated = append(truncated, field)
			}
		}
		s.catalog.OnFileFlush(pmeta.FileContribution{
			Partition:         manifest.ExtractTenantPartition(fi.Key),
			FileKey:           fi.Key,
			RowCount:          fi.RowCount,
			MinTimeNs:         fi.MinTimeNs,
			MaxTimeNs:         fi.MaxTimeNs,
			RawBytes:          fi.RawBytes,
			SchemaFingerprint: fi.SchemaFingerprint,
			Labels:            fi.Labels,
			TruncatedFields:   truncated,
		})
	}
	byPart := make(map[string][]string)
	for _, k := range removed {
		p := manifest.ExtractTenantPartition(k)
		byPart[p] = append(byPart[p], k)
	}
	for p, keys := range byPart {
		s.catalog.RemoveFiles(p, keys)
	}
}

// PmetaOnFileExpired removes an expired file's facet entries; when the whole
// partition has aged out of the manifest, the bundle is evicted from RAM and
// its S3 object deleted (bundles otherwise accumulate forever past retention).
func (s *Storage) PmetaOnFileExpired(partition, key string) {
	if s.catalog == nil {
		return
	}
	// The retention loop hands us the manifest's pure dt=/hour= partition, but the
	// pmeta facet/bundle is keyed by the tenant-isolated partition (full key dir).
	// Derive that from the file key so the facet removal + bundle eviction hit the
	// right bundle; the manifest emptiness check stays on the manifest partition.
	tp := manifest.ExtractTenantPartition(key)
	s.catalog.RemoveFiles(tp, []string{key})
	if len(s.manifest.FilesForPartition(partition)) == 0 {
		s.catalog.Remove(tp)
		if s.pool != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.pool.Delete(ctx, s.catalog.BundleKey(tp)); err != nil {
				logger.Warnf("pmeta: delete expired bundle %s: %v", tp, err)
			}
		}
	}
}

// refuseEnumeration reports whether field_values for a field should return empty
// instead of scanning to enumerate it — true only when refuse_sketch_enumeration
// is on AND the field is a declared always-sketch id column (trace_id, span_id…).
// Threshold-crossers are NOT refused; they still fall through to the scan.
func (s *Storage) refuseEnumeration(field string) bool {
	if s.catalog == nil || !s.cfg.Pmeta.RefuseSketchEnumeration {
		return false
	}
	for _, f := range effectiveSketchFields(s.cfg.Pmeta.AlwaysSketchFields) {
		if f == field {
			return true
		}
	}
	return false
}
