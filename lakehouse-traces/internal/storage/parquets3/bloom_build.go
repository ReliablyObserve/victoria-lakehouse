package parquets3

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
)

// writeFileBloom writes a per-file bloom sidecar (.bloom) to S3 for the
// given file key. Reuses bloomindex.Index — no new bloom data structures.
func writeFileBloom(ctx context.Context, pool *s3reader.ClientPool, fileKey string, columnValues map[string][]string) {
	idx := bloomindex.NewFileBloomIndex(columnValues, 0.01)
	if idx.Len() == 0 {
		return
	}
	data := idx.Marshal()
	if len(data) == 0 {
		return
	}
	bloomKey := fileKey + ".bloom"
	if err := pool.Upload(ctx, bloomKey, data); err != nil {
		logger.Warnf("file bloom upload failed: %s; key=%s", err, bloomKey)
		return
	}
	metrics.BloomBuildTotal.Inc("file_bloom")
}
