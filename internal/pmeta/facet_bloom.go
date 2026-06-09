package pmeta

import (
	"io"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
)

const defaultBloomFPRate = 0.01

// bloomFacet folds the per-partition bloom index (_bloom.bin) into the unified
// pmeta bundle. It wraps the proven bloomindex.Index (math + codec kept as-is);
// only the lifecycle glue moves into the facet. Blooms are built via the shared
// bloomindex.BuildFileColumns so they are identical to the legacy path — the
// dual-write parity gate (MayContain) depends on that.
type bloomFacet struct {
	partition string
	fpRate    float64
	mu        sync.RWMutex
	idx       *bloomindex.Index
}

// NewBloomFactory returns a FacetFactory at the given false-positive rate (0 → 0.01).
func NewBloomFactory(fpRate float64) FacetFactory {
	if fpRate <= 0 {
		fpRate = defaultBloomFPRate
	}
	return func(partition string) Facet {
		return &bloomFacet{partition: partition, fpRate: fpRate, idx: bloomindex.New()}
	}
}

func (f *bloomFacet) Kind() FacetKind { return FacetBloom }

func (f *bloomFacet) Merge(c FileContribution) {
	if c.FileKey == "" || len(c.BloomValues) == 0 {
		return
	}
	cols := bloomindex.BuildFileColumns(c.BloomValues, f.fpRate)
	if len(cols) == 0 {
		return
	}
	f.mu.Lock()
	f.idx.AddColumns(c.FileKey, cols)
	f.mu.Unlock()
}

func (f *bloomFacet) Encode(w io.Writer) error {
	f.mu.RLock()
	data := f.idx.Marshal()
	f.mu.RUnlock()
	_, err := w.Write(data)
	return err
}

func (f *bloomFacet) Decode(r io.Reader) error {
	data, err := io.ReadAll(r)
	if err != nil {
		return err
	}
	idx, err := bloomindex.Unmarshal(data)
	if err != nil {
		return err
	}
	f.mu.Lock()
	f.idx = idx
	f.mu.Unlock()
	return nil
}

func (f *bloomFacet) EstimateBytes() int64 {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return int64(len(f.idx.Marshal())) // exact; eviction is not on the hot path
}

func (f *bloomFacet) mayContain(keys []string, column, value string) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()
	return f.idx.MayContain(keys, column, value)
}

// BloomMayContain returns the subset of keys whose bloom filter for column may
// contain value, read from the partition's FacetBloom. ok is false when pmeta has
// no bloom facet for the partition (caller falls back to the legacy bloom path).
func (s *Store) BloomMayContain(partition string, keys []string, column, value string) ([]string, bool) {
	fc, ok := s.Get(partition, FacetBloom)
	if !ok {
		return nil, false
	}
	bf, ok := fc.(*bloomFacet)
	if !ok {
		return nil, false
	}
	return bf.mayContain(keys, column, value), true
}
