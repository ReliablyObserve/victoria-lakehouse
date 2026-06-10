package parquets3

import (
	"context"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/bloomindex"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

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
