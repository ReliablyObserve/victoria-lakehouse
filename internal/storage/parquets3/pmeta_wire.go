package parquets3

import (
	"context"
	"sort"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

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
	s.Register(pmeta.FacetFieldCatalog,
		pmeta.NewFieldCatalogFactoryCapped(pmeta.NewDict(), threshold, cfg.AlwaysSketchFields))
	s.Register(pmeta.FacetFileMeta, pmeta.NewFileMetaFactory()) // dual-write of _file_metadata.json
	return s
}

// catalogObserver feeds flushed file labels into the pmeta field/value catalog.
// It mirrors storageBloomObserver: a nil-safe observer set on the BatchWriter and
// invoked at flush with the SAME already-extracted label map the bloom path uses
// — no extra column scan. Nil (pmeta off) → no-op.
type catalogObserver struct {
	store  *pmeta.Store
	sketch map[string]bool // always-sketch field names to tap for cardinality
}

func (o *catalogObserver) OnFileFlush(partition string, fi manifest.FileInfo, labels map[string][]string) {
	if o == nil || o.store == nil {
		return
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
	})
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
	for _, fi := range s.manifest.GetFilesForRange(startNs, endNs) {
		p := manifest.ExtractPartition(fi.Key)
		if _, ok := seen[p]; ok {
			continue
		}
		seen[p] = struct{}{}
		for _, v := range s.catalog.FieldValues(p, fieldName, "", int(limit)) {
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
	if uint64(len(vals)) > limit {
		vals = vals[:limit]
	}
	out := make([]logstorage.ValueWithHits, len(vals))
	for i, v := range vals {
		out[i] = logstorage.ValueWithHits{Value: v, Hits: 1}
	}
	metrics.CatalogValueLookups.Add("catalog", 1) // served from RAM
	return out
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
			s.catalog.OnFileFlush(pmeta.FileContribution{
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
