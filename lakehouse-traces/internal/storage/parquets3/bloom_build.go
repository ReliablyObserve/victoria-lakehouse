package parquets3

import (
	"context"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// extractLogBloomValues / extractTraceBloomValues are the UNCAPPED bloom feed for
// rows flushed by the traces module (trace_id + service.name). A bloom fed from the
// capped label extractor false-negatives on values past the cap — missing results.
// Both delegate to the shared schema extractor so the pmeta bloom set is identical on
// this flush path and the compaction path in internal/compaction (combined bloom).
func extractLogBloomValues(rows []schema.LogRow) map[string][]string {
	return schema.ExtractLogBloomValues(rows)
}

func extractTraceBloomValues(rows []schema.TraceRow) map[string][]string {
	return schema.ExtractTraceBloomValues(rows)
}

func bloomS3Loader(pool *s3reader.ClientPool, prefix string) func(ctx context.Context, partition string) (*bloomindex.Index, error) {
	return func(ctx context.Context, partition string) (*bloomindex.Index, error) {
		if pool == nil {
			return nil, nil
		}
		key := fmt.Sprintf("%s%s/_bloom.bin", prefix, partition)
		// Singleflight: concurrent queries lazily loading the same cold
		// partition's bloom bundle share one GET (BloomCache.Get has no
		// in-flight dedup of its own).
		data, err := pool.DownloadDedup(ctx, "pmeta_bundle", key)
		if err != nil {
			return nil, nil
		}
		if len(data) == 0 {
			return nil, nil
		}
		idx, err := bloomindex.Unmarshal(data)
		if err != nil {
			logger.Warnf("bloom corrupt for %s, skipping: %v", partition, err)
			return nil, nil
		}
		return idx, nil
	}
}
