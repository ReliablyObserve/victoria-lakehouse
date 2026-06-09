package pmeta

import "sync"

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
}

// NewStore returns an empty store with no facets registered.
func NewStore() *Store {
	return &Store{
		reg:     make(map[FacetKind]FacetFactory),
		bundles: make(map[string]*Bundle),
	}
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
	b.dirty.Store(true)
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

// ResidentBytes is the approximate RAM held across all partition bundles. Drives
// the lakehouse_catalog_resident_bytes guardrail.
func (s *Store) ResidentBytes() int64 {
	s.mu.RLock()
	defer s.mu.RUnlock()
	var n int64
	for _, b := range s.bundles {
		n += b.EstimateBytes()
	}
	return n
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
