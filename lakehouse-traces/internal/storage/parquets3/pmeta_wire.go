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
	v, ok := p.store.FileMeta(partition, fileKey)
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
// commit.

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
		pmeta.NewFieldCatalogFactoryCapped(dict, threshold, cfg.AlwaysSketchFields))
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
func (o *catalogObserver) tapLogRows(rows []schema.LogRow) {
	if o == nil || o.store == nil || len(o.sketch) == 0 {
		return
	}
	if o.sketch["trace_id"] {
		o.store.AddCardinality("trace_id", func(yield func(string) bool) {
			for i := range rows {
				if !yield(rows[i].TraceID) {
					return
				}
			}
		})
		metrics.CatalogFieldCardinality.Set("trace_id", int64(o.store.Cardinality("trace_id")))
	}
	if o.sketch["span_id"] {
		o.store.AddCardinality("span_id", func(yield func(string) bool) {
			for i := range rows {
				if !yield(rows[i].SpanID) {
					return
				}
			}
		})
		metrics.CatalogFieldCardinality.Set("span_id", int64(o.store.Cardinality("span_id")))
	}
}

// tapTraceRows is tapLogRows for trace rows.
func (o *catalogObserver) tapTraceRows(rows []schema.TraceRow) {
	if o == nil || o.store == nil || len(o.sketch) == 0 {
		return
	}
	if o.sketch["trace_id"] {
		o.store.AddCardinality("trace_id", func(yield func(string) bool) {
			for i := range rows {
				if !yield(rows[i].TraceID) {
					return
				}
			}
		})
		metrics.CatalogFieldCardinality.Set("trace_id", int64(o.store.Cardinality("trace_id")))
	}
	if o.sketch["span_id"] {
		o.store.AddCardinality("span_id", func(yield func(string) bool) {
			for i := range rows {
				if !yield(rows[i].SpanID) {
					return
				}
			}
		})
		metrics.CatalogFieldCardinality.Set("span_id", int64(o.store.Cardinality("span_id")))
	}
}

// sketchSet builds the always-sketch field lookup from config.
func sketchSet(fields []string) map[string]bool {
	if len(fields) == 0 {
		return nil
	}
	m := make(map[string]bool, len(fields))
	for _, f := range fields {
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
		p := manifest.ExtractPartition(fi.Key)
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
		p := manifest.ExtractPartition(fi.Key)
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
	for partition, files := range s.manifest.AllFiles() {
		if ctx.Err() != nil {
			return
		}
		for _, fi := range files {
			// Replay, not flush: manifest-derived content is already durable, so
			// it must NOT mark bundles dirty (a dirty mark here re-PUT every
			// partition bundle on the first flush after every restart).
			s.catalog.OnFileReplay(pmeta.FileContribution{
				Partition:         partition,
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
	parts := make([]string, 0, 64)
	for p := range s.manifest.AllFiles() {
		parts = append(parts, p)
	}
	if len(parts) == 0 {
		return
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
		files := s.manifest.FilesForPartition(p)
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
			Partition:         manifest.ExtractPartition(fi.Key),
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
		p := manifest.ExtractPartition(k)
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
	s.catalog.RemoveFiles(partition, []string{key})
	if len(s.manifest.FilesForPartition(partition)) == 0 {
		s.catalog.Remove(partition)
		if s.pool != nil {
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			if err := s.pool.Delete(ctx, s.catalog.BundleKey(partition)); err != nil {
				logger.Warnf("pmeta: delete expired bundle %s: %v", partition, err)
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
	for _, f := range s.cfg.Pmeta.AlwaysSketchFields {
		if f == field {
			return true
		}
	}
	return false
}
