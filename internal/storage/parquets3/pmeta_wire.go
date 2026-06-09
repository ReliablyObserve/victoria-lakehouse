package parquets3

import (
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
