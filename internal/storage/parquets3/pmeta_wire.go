package parquets3

import (
	"context"
	"sort"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta"
)

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
func newCatalogStore(enabled bool, prefix string) *pmeta.Store {
	if !enabled {
		return nil
	}
	s := pmeta.NewStore()
	s.SetPrefix(prefix)
	s.Register(pmeta.FacetFieldCatalog, pmeta.NewFieldCatalogFactory(pmeta.NewDict()))
	return s
}

// catalogObserver feeds flushed file labels into the pmeta field/value catalog.
// It mirrors storageBloomObserver: a nil-safe observer set on the BatchWriter and
// invoked at flush with the SAME already-extracted label map the bloom path uses
// — no extra column scan. Nil (pmeta off) → no-op.
type catalogObserver struct{ store *pmeta.Store }

func (o *catalogObserver) OnFileFlush(partition, fileKey string, labels map[string][]string) {
	if o == nil || o.store == nil {
		return
	}
	o.store.OnFileFlush(pmeta.FileContribution{
		Partition: partition,
		FileKey:   fileKey,
		Labels:    labels,
	})
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
				Partition: partition,
				FileKey:   fi.Key,
				Labels:    fi.Labels,
			})
		}
	}
}
