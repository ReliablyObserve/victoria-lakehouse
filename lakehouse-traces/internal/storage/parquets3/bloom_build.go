package parquets3

import (
	"context"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func (o *storageBloomObserver) writeFileBloom(ctx context.Context, fileKey string, columnValues map[string][]string) {
	idx := bloomindex.NewFileBloomIndex(columnValues, 0.01)
	if idx.Len() == 0 {
		return
	}
	data := idx.Marshal()
	if len(data) == 0 {
		return
	}
	bloomKey := fileKey + ".bloom"
	if err := o.pool.Upload(ctx, bloomKey, data); err != nil {
		logger.Warnf("file bloom upload failed: %s; key=%s", err, bloomKey)
		return
	}
	metrics.BloomBuildTotal.Inc("file_bloom")
}

type storageBloomObserver struct {
	bloom    *bloomindex.PartitionedIndex
	pool     *s3reader.ClientPool
	manifest *manifest.Manifest
}

func (o *storageBloomObserver) OnFileFlush(partition, fileKey string, columnValues map[string][]string) {
	if o.bloom == nil || len(columnValues) == 0 {
		return
	}
	o.bloom.AddFile(partition, fileKey, columnValues)
	metrics.BloomBuildTotal.Inc("flush")
	totalEntries := 0
	for _, vals := range columnValues {
		totalEntries += len(vals)
	}
	metrics.BloomEntriesTotal.Add(totalEntries)

	if o.pool != nil {
		go o.writeFileBloom(context.Background(), fileKey, columnValues)
	}
}

func (o *storageBloomObserver) PersistDirty(ctx context.Context, prefix string) {
	if o.bloom == nil || o.pool == nil {
		return
	}
	for _, partition := range o.bloom.DirtyPartitions() {
		data := o.bloom.MarshalPartition(partition)
		if len(data) == 0 {
			continue
		}
		key := fmt.Sprintf("%s%s/_bloom.bin", prefix, partition)
		if err := o.pool.Upload(ctx, key, data); err != nil {
			metrics.InsertFlushErrorsTotal.Inc()
			metrics.BloomBuildErrors.Inc()
			logger.Warnf("bloom persist failed for %s: %v", partition, err)
			continue
		}
		o.bloom.ClearDirty(partition)
		if o.manifest != nil {
			o.manifest.SetBloomMeta(partition, manifest.PartitionMeta{
				BloomAvailable: true,
				BloomSize:      int64(len(data)),
			})
		}
	}
}

// extractLogBloomValues is the UNCAPPED bloom feed for log rows flushed by the
// traces module (trace_id + service.name). A bloom fed from the capped label
// extractor false-negatives on values past the cap — missing results.
func extractLogBloomValues(rows []schema.LogRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := map[string]map[string]bool{
		"trace_id":     {},
		"service.name": {},
	}
	for i := range rows {
		if rows[i].TraceID != "" {
			sets["trace_id"][rows[i].TraceID] = true
		}
		if rows[i].ServiceName != "" {
			sets["service.name"][rows[i].ServiceName] = true
		}
	}
	return bloomSetsToMap(sets)
}

func extractTraceBloomValues(rows []schema.TraceRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := map[string]map[string]bool{
		"trace_id":     {},
		"service.name": {},
	}
	for i := range rows {
		if rows[i].TraceID != "" {
			sets["trace_id"][rows[i].TraceID] = true
		}
		if rows[i].ServiceName != "" {
			sets["service.name"][rows[i].ServiceName] = true
		}
	}
	return bloomSetsToMap(sets)
}

func bloomSetsToMap(sets map[string]map[string]bool) map[string][]string {
	result := make(map[string][]string, len(sets))
	for col, vs := range sets {
		if len(vs) == 0 {
			continue
		}
		vals := make([]string, 0, len(vs))
		for v := range vs {
			vals = append(vals, v)
		}
		result[col] = vals
	}
	if len(result) == 0 {
		return nil
	}
	return result
}

func bloomS3Loader(pool *s3reader.ClientPool, prefix string) func(ctx context.Context, partition string) (*bloomindex.Index, error) {
	return func(ctx context.Context, partition string) (*bloomindex.Index, error) {
		if pool == nil {
			return nil, nil
		}
		key := fmt.Sprintf("%s%s/_bloom.bin", prefix, partition)
		data, err := pool.Download(ctx, key)
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
