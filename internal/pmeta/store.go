package pmeta

import (
	"iter"
	"strings"
	"sync"
)

// Store owns the in-RAM bundles and the facet registry. It is THE single
// lifecycle owner: the one write-hook (OnFileFlush), the one dirty list
// (DirtyPartitions), and — layered on in later commits — the one S3 sidecar
// loader, snapshot manager and eviction policy.
//
// This scaffold is the in-memory + codec core (migration "Step 0"): it is
// unwired into the storage paths and sits behind the --pmeta flag (default off).
type Store struct {
	mu      sync.RWMutex
	reg     map[FacetKind]FacetFactory
	bundles map[string]*Bundle
	prefix  string // S3 key prefix for partition bundles
	// dict, when set, is the shared interning dictionary — included in
	// ResidentBytes so the guardrail metric reflects the true catalog footprint.
	dict *Dict
	// hllByField holds one merged HyperLogLog per high-cardinality field
	// (fed from FileContribution.HighCardValues), giving an approximate
	// distinct-count for fields the catalog does not enumerate. One sketch per
	// field globally (not per partition) keeps it bounded — ~16 KB/field at p=14.
	hllByField map[string]*hll
}

// NewStore returns an empty store with no facets registered.
func NewStore() *Store {
	return &Store{
		reg:        make(map[FacetKind]FacetFactory),
		bundles:    make(map[string]*Bundle),
		hllByField: make(map[string]*hll),
	}
}

// Cardinality returns the approximate distinct-count for a high-cardinality field
// (from its merged HLL sketch), or 0 if the field has no sketch. Used to answer
// "≈ N distinct" for fields the catalog does not enumerate.
func (s *Store) Cardinality(field string) uint64 {
	// estimate() reads the register array that AddCardinality/OnFileFlush mutate
	// under the write lock — hold the read lock ACROSS the estimate, not just the
	// map lookup, or the read races the writers.
	s.mu.RLock()
	defer s.mu.RUnlock()
	h := s.hllByField[field]
	if h == nil {
		return 0
	}
	return h.estimate()
}

// FieldCardinality returns the distinct-value count for a field across the WHOLE
// store — the accurate Cardinality Explorer source, read entirely from pmeta. A
// high-card field (sketched, not enumerable) returns its globally-merged HLL
// estimate; a low/medium-card field returns the size of the UNION of the
// per-partition fieldCatalogFacet's ENUMERATED values (exact). No side map: the
// catalog facets are the persisted, merged, restorable source of truth.
func (s *Store) FieldCardinality(field string) uint64 {
	s.mu.RLock()
	parts := make([]string, 0, len(s.bundles))
	for p := range s.bundles {
		parts = append(parts, p)
	}
	gh := s.hllByField[field] // in-memory feed (e.g. always-sketch ids this run)
	s.mu.RUnlock()

	// Union the PERSISTED per-partition sketches for high-card fields, and
	// enumerate the low-card values — both from the catalog facets, so the count
	// survives restart with the bundle. The in-memory hllByField is folded in too
	// (covers values fed this run not yet flushed to a facet).
	var merged *hll
	seen := make(map[string]struct{})
	for _, p := range parts {
		cf, ok := s.catalog(p)
		if !ok {
			continue
		}
		if h := cf.fieldHLL(field); h != nil {
			if merged == nil {
				merged = newHLL(h.p)
			}
			_ = merged.merge(h)
		} else {
			for _, v := range cf.Values(field, "", 0) {
				seen[v] = struct{}{}
			}
		}
	}
	if merged != nil {
		for v := range seen { // fold any partition that was still low-card
			merged.add(v)
		}
		if gh != nil {
			_ = merged.merge(gh)
		}
		return merged.estimate()
	}
	if gh != nil {
		return gh.estimate()
	}
	return uint64(len(seen))
}

// AddCardinality folds a stream of values into a field's HLL sketch. The values
// are an iterator (iter.Seq) so the caller — typically the flush path over a
// file's rows — feeds them WITHOUT materializing a slice; empty strings are
// skipped. Locks once for the whole stream.
func (s *Store) AddCardinality(field string, values iter.Seq[string]) {
	s.mu.Lock()
	defer s.mu.Unlock()
	h := s.hllByField[field]
	if h == nil {
		h = newHLL(defaultHLLPrecision)
		s.hllByField[field] = h
	}
	for v := range values {
		if v != "" {
			h.add(v)
		}
	}
}

// AddPartitionCardinality folds a value stream into a field's PERSISTED per-partition
// catalog HLL (marking it high-card) so the distinct count survives restart via the
// bundle — for always-sketch id columns (trace_id, span_id) fed from the row tap and
// never enumerated in Labels. Complements AddCardinality, which keeps a global RAM
// sketch for the live metric; THIS is the durable source FieldCardinality unions on
// restart. No-op until the partition's catalog facet exists (the flush that creates
// it runs immediately before the tap), and marks the bundle dirty so it re-persists.
func (s *Store) AddPartitionCardinality(partition, field string, values iter.Seq[string]) {
	cf, ok := s.catalog(partition)
	if !ok {
		return
	}
	cf.addHighCardValues(field, values)
	s.Bundle(partition).markDirty()
}

// Register wires a facet factory for a kind. Call once per kind at startup,
// before OnFileFlush/Decode are used.
func (s *Store) Register(k FacetKind, f FacetFactory) {
	s.mu.Lock()
	s.reg[k] = f
	s.mu.Unlock()
}

// Registry returns a copy of the kind->factory map for DecodeBundle.
func (s *Store) Registry() map[FacetKind]FacetFactory {
	s.mu.RLock()
	defer s.mu.RUnlock()
	m := make(map[FacetKind]FacetFactory, len(s.reg))
	for k, v := range s.reg {
		m[k] = v
	}
	return m
}

// Bundle returns the bundle for a partition, creating an empty one if absent.
func (s *Store) Bundle(partition string) *Bundle {
	s.mu.RLock()
	b, ok := s.bundles[partition]
	s.mu.RUnlock()
	if ok {
		return b
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if b, ok = s.bundles[partition]; ok { // re-check under write lock
		return b
	}
	b = NewBundle(partition)
	s.bundles[partition] = b
	return b
}

// Put installs a fully-loaded bundle (e.g. from DecodeBundle) into the store.
func (s *Store) Put(b *Bundle) {
	s.mu.Lock()
	s.bundles[b.Partition] = b
	s.mu.Unlock()
}

// PutWarm installs a bundle decoded from S3 WITHOUT clobbering live flush
// contributions. With serve-while-warming, a flush can populate a partition's
// bundle before the warm goroutine loads the S3 copy; an unconditional Put would
// replace that bundle, silently dropping the flush's facet contributions and the
// dirty state. PutWarm absorbs the DECODED content into the live bundle instead
// (the union persists on the next flush cycle). When no live bundle exists, this
// is a plain Put.
func (s *Store) PutWarm(decoded *Bundle) {
	s.mu.Lock()
	live, ok := s.bundles[decoded.Partition]
	if !ok {
		s.bundles[decoded.Partition] = decoded
		s.mu.Unlock()
		return
	}
	s.mu.Unlock()

	decoded.mu.RLock()
	facets := make([]Facet, 0, len(decoded.facets))
	for _, f := range decoded.facets {
		facets = append(facets, f)
	}
	decoded.mu.RUnlock()

	for _, df := range facets {
		live.mu.Lock()
		lf, has := live.facets[df.Kind()]
		if !has {
			live.facets[df.Kind()] = df
			live.mu.Unlock()
			continue
		}
		live.mu.Unlock()
		if a, ok := lf.(absorber); ok {
			a.absorbFacet(df)
		}
	}
	live.markDirty() // the union (S3 content + live flushes) must persist
}

// absorber is the optional facet capability PutWarm uses to merge a decoded
// facet into a live one (union semantics, no entry lost from either side).
type absorber interface{ absorbFacet(other Facet) }

// RemoveFiles drops per-file entries (file-meta + bloom) for files that no
// longer exist — the compaction/delete-rewrite hook. The catalog facet is
// untouched: its value sets are a partition-level union that compaction does not
// change. Marks the bundle dirty so the shrunken bundle persists.
func (s *Store) RemoveFiles(partition string, keys []string) {
	if len(keys) == 0 {
		return
	}
	s.mu.RLock()
	b, ok := s.bundles[partition]
	s.mu.RUnlock()
	if !ok {
		return
	}
	if fc, ok := b.Get(FacetFileMeta); ok {
		if fm, ok := fc.(*fileMetaFacet); ok {
			fm.removeFiles(keys)
		}
	}
	if fc, ok := b.Get(FacetBloom); ok {
		if bf, ok := fc.(*bloomFacet); ok {
			bf.removeFiles(keys)
		}
	}
	b.markDirty()
}

// Remove drops a partition's bundle from RAM — the retention/expiry hook. The
// caller is responsible for deleting (or ignoring) the S3 bundle object.
func (s *Store) Remove(partition string) {
	s.mu.Lock()
	delete(s.bundles, partition)
	s.mu.Unlock()
}

// Partitions returns the partitions currently resident in the store.
func (s *Store) Partitions() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0, len(s.bundles))
	for p := range s.bundles {
		out = append(out, p)
	}
	return out
}

// Get returns a facet from a partition's bundle, if present.
func (s *Store) Get(partition string, k FacetKind) (Facet, bool) {
	s.mu.RLock()
	b, ok := s.bundles[partition]
	s.mu.RUnlock()
	if !ok {
		return nil, false
	}
	return b.Get(k)
}

// OnFileFlush folds a newly-flushed file's contribution into every registered
// facet of its partition — THE single write-hook (replaces the five today).
// Facets absent from the bundle are created on demand, so this also drives the
// rebuild-from-files self-heal path.
func (s *Store) OnFileFlush(c FileContribution) {
	s.mergeContribution(c, true)
}

// OnFileReplay is OnFileFlush WITHOUT marking the bundle dirty — the warm path
// (re-deriving catalog/file-meta from the already-durable manifest). Marking
// replays dirty caused a full-manifest bundle PUT storm on the first flush after
// every restart; replayed content is derivable, so it needs no re-persist.
func (s *Store) OnFileReplay(c FileContribution) {
	s.mergeContribution(c, false)
}

func (s *Store) mergeContribution(c FileContribution, markDirty bool) {
	b := s.Bundle(c.Partition)
	reg := s.Registry()
	b.mu.Lock()
	for k, factory := range reg {
		f, ok := b.facets[k]
		if !ok {
			f = factory(c.Partition)
			b.facets[k] = f
		}
		f.Merge(c)
	}
	b.mu.Unlock()
	if markDirty {
		b.markDirty()
	}

	// Fold high-cardinality field values into their per-field HLL sketch (for
	// the "≈ N distinct" readout on fields the catalog does not enumerate).
	if len(c.HighCardValues) > 0 {
		s.mu.Lock()
		for field, vals := range c.HighCardValues {
			h := s.hllByField[field]
			if h == nil {
				h = newHLL(defaultHLLPrecision)
				s.hllByField[field] = h
			}
			for _, v := range vals {
				h.add(v)
			}
		}
		s.mu.Unlock()
	}
}

// Rebuild replays a partition's file contributions through the registered
// facets to reconstruct any that were skipped on load (corrupt/unknown). The
// caller supplies the partition's current files; this is the same path as
// OnFileFlush, run over existing files instead of a fresh one.
func (s *Store) Rebuild(partition string, files []FileContribution) {
	for _, c := range files {
		c.Partition = partition
		s.OnFileFlush(c)
	}
}

// SetDict registers the shared interning dictionary so ResidentBytes accounts
// for it (the dict holds every interned string once, globally).
func (s *Store) SetDict(d *Dict) {
	s.mu.Lock()
	s.dict = d
	s.mu.Unlock()
}

// BundleKey is the S3 object key for a partition's bundle (exported for the
// retention path, which deletes the bundle object alongside the partition).
func (s *Store) BundleKey(partition string) string { return s.bundleKey(partition) }

// ResidentBytes is the approximate RAM held across all partition bundles plus
// the shared dict. Drives the lakehouse_catalog_resident_bytes guardrail.
func (s *Store) ResidentBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	for _, b := range s.bundles {
		n += b.EstimateBytes()
	}
	if s.dict != nil {
		n += s.dict.EstimateBytes()
	}
	return n
}

// PersistedBytes is the cluster's on-S3 metadata footprint — the sum of every
// resident bundle's last-persisted encoded size, tracked incrementally on
// persist / warm-load / compaction (NO S3 LIST). Bundles evicted or removed from
// the store drop out of the sum automatically. Excludes the tiny _meta/ sidecars.
func (s *Store) PersistedBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	for _, b := range s.bundles {
		n += b.PersistedSize()
	}
	return n
}

// PersistedBytesByTenant sums each tenant's bundles' on-S3 encoded size, keyed
// "account:project" parsed from the tenant-isolated partition. Incremental — no
// S3 LIST. Bundles without a numeric tenant prefix are skipped.
func (s *Store) PersistedBytesByTenant() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64)
	for partition, b := range s.bundles {
		if tk := tenantKeyFromPartition(partition); tk != "" {
			out[tk] += b.PersistedSize()
		}
	}
	return out
}

func tenantKeyFromPartition(partition string) string {
	parts := strings.SplitN(partition, "/", 3)
	if len(parts) >= 2 {
		return parts[0] + ":" + parts[1]
	}
	return ""
}

// MetadataBytesByField returns the exact per-field on-RAM metadata footprint,
// keyed by field name: for every resident partition bundle, each field's bloom
// bitset bytes (FacetBloom.BytesByField) plus its catalog-entry / distinct-count
// HLL bytes (FacetFieldCatalog.BytesByField), accumulated across all partitions.
// Incremental — reads the live facets under RLock, no S3 scan. The template is
// PersistedBytesByTenant; this decomposes the same bundles per field instead of
// per tenant. Empty when no bloom/catalog facets are resident.
func (s *Store) MetadataBytesByField() map[string]int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make(map[string]int64)
	for _, b := range s.bundles {
		if fc, ok := b.Get(FacetBloom); ok {
			if bf, ok := fc.(*bloomFacet); ok {
				for field, n := range bf.BytesByField() {
					out[field] += n
				}
			}
		}
		if fc, ok := b.Get(FacetFieldCatalog); ok {
			if cf, ok := fc.(*fieldCatalogFacet); ok {
				for field, n := range cf.BytesByField() {
					out[field] += n
				}
			}
		}
	}
	return out
}

// DirtyPartitions returns partitions with unpersisted changes — THE single
// dirty list (replaces the five per-subsystem mechanisms).
func (s *Store) DirtyPartitions() []string {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]string, 0)
	for p, b := range s.bundles {
		if b.Dirty() {
			out = append(out, p)
		}
	}
	return out
}
