package parquets3

import (
	"context"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// extractLogBloomValues collects each file's distinct values for every bloomed log
// column, driven by the schema SoT (schema.LogBloomValueColumns) so it can never
// drift from the Parquet HasBloom set. Feeds the partition _bloom.bin (file-level
// pruning). span_id is excluded by the SoT (see bloom_value_columns.go).
func extractLogBloomValues(rows []schema.LogRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := make(map[string]map[string]bool, len(schema.LogBloomValueColumns))
	for _, c := range schema.LogBloomValueColumns {
		sets[c.Name] = make(map[string]bool)
	}
	for i := range rows {
		for _, c := range schema.LogBloomValueColumns {
			if v := c.Get(&rows[i]); v != "" {
				sets[c.Name][v] = true
			}
		}
	}
	return bloomSetsToMap(sets)
}

// extractTraceBloomValues is extractLogBloomValues for trace rows (schema.TraceBloomValueColumns).
func extractTraceBloomValues(rows []schema.TraceRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := make(map[string]map[string]bool, len(schema.TraceBloomValueColumns))
	for _, c := range schema.TraceBloomValueColumns {
		sets[c.Name] = make(map[string]bool)
	}
	for i := range rows {
		for _, c := range schema.TraceBloomValueColumns {
			if v := c.Get(&rows[i]); v != "" {
				sets[c.Name][v] = true
			}
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
