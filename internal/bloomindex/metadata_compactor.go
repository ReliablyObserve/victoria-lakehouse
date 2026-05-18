package bloomindex

import (
	"context"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// MetadataCompactor handles local bloom tier transitions based on data age.
type MetadataCompactor struct {
	mu    sync.Mutex
	cache *BloomCache
	cfg   TierConfig

	// persistFn uploads bloom data to S3. If nil, tier transitions are local-only.
	persistFn func(ctx context.Context, partition string, data []byte) error
}

// NewMetadataCompactor creates a new local metadata compactor.
func NewMetadataCompactor(cache *BloomCache, cfg TierConfig, persistFn func(ctx context.Context, partition string, data []byte) error) *MetadataCompactor {
	return &MetadataCompactor{
		cache:     cache,
		cfg:       cfg,
		persistFn: persistFn,
	}
}

// CompactPartition checks if a partition needs tier downgrade based on its age
// and performs the transition if needed.
func (mc *MetadataCompactor) CompactPartition(ctx context.Context, partition string, partitionAge time.Duration) error {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	tier := TierForAge(partitionAge, mc.cfg)

	idx, err := mc.cache.Get(ctx, partition)
	if err != nil || idx == nil {
		return err
	}

	switch tier {
	case TierWarm:
		return mc.downgradeToPerFile(ctx, partition, idx)
	case TierCold:
		return mc.downgradeToSummary(ctx, partition, idx)
	case TierArchive:
		mc.cache.Invalidate(partition)
		return nil
	default:
		return nil
	}
}

func (mc *MetadataCompactor) downgradeToPerFile(ctx context.Context, partition string, idx *Index) error {
	if !hasPerRGEntries(idx) {
		return nil
	}

	merged := DowngradeToPerFile(idx)
	mc.cache.Put(partition, merged)

	if mc.persistFn != nil {
		data := merged.Marshal()
		if err := mc.persistFn(ctx, partition, data); err != nil {
			logger.Warnf("bloom persist after per-file downgrade failed for %s: %v", partition, err)
			return err
		}
	}

	return nil
}

func (mc *MetadataCompactor) downgradeToSummary(ctx context.Context, partition string, idx *Index) error {
	if idx.Len() <= 1 {
		return nil
	}

	summary := DowngradeToSummary(idx)
	mc.cache.Put(partition, summary)

	if mc.persistFn != nil {
		data := summary.Marshal()
		if err := mc.persistFn(ctx, partition, data); err != nil {
			logger.Warnf("bloom persist after summary downgrade failed for %s: %v", partition, err)
			return err
		}
	}

	return nil
}

func hasPerRGEntries(idx *Index) bool {
	for key := range idx.Entries() {
		_, _, ok := ParseRGKey(key)
		if ok {
			return true
		}
	}
	return false
}
