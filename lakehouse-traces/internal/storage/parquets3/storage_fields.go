package parquets3

import (
	"bytes"
	"context"
	"fmt"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/parquet-go/parquet-go"
)

func (s *Storage) GetFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	if s.labelIndex.Len() > 0 {
		names := s.labelIndex.GetFieldNames()
		result := make([]logstorage.ValueWithHits, len(names))
		for i, name := range names {
			result[i] = logstorage.ValueWithHits{Value: name, Hits: 1}
		}
		return result, nil
	}

	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	seen := make(map[string]bool)
	var result []logstorage.ValueWithHits

	fi := files[0]
	data, err := s.getFileData(ctx, fi.Key, fi.Size)
	if err != nil {
		return nil, fmt.Errorf("get file data: %w", err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}

	s.updateLabelIndex(f)

	for _, name := range columnNames(f.Root()) {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}
		if !seen[internalName] {
			seen[internalName] = true
			result = append(result, logstorage.ValueWithHits{Value: internalName, Hits: 1})
		}
	}

	return result, nil
}

func (s *Storage) GetFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	if limit > 0 && s.labelIndex.Len() > 0 {
		vals := s.labelIndex.GetFieldValues(fieldName, limit)
		if len(vals) > 0 {
			result := make([]logstorage.ValueWithHits, len(vals))
			for i, v := range vals {
				result[i] = logstorage.ValueWithHits{Value: v, Hits: 1}
			}
			return result, nil
		}
	}

	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	mapping := s.registry.ResolveToParquet(fieldName)
	if mapping == nil {
		mapping = s.registry.ResolveFromParquet(fieldName)
	}
	if mapping == nil {
		return nil, nil
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			logger.Warnf("get file data for field values: %s; key=%s", err, fi.Key)
			continue
		}

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			logger.Warnf("open parquet for field values: %s; key=%s", err, fi.Key)
			continue
		}

		s.updateLabelIndex(f)

		colIdx := findColumnIndex(f.Root(), mapping.ParquetColumn)
		if colIdx < 0 {
			continue
		}

		for _, rg := range f.RowGroups() {
			rows := rg.Rows()
			buf := make([]parquet.Row, 256)
			for {
				n, err := rows.ReadRows(buf)
				for i := 0; i < n; i++ {
					if colIdx < len(buf[i]) {
						val := valueToString(buf[i][colIdx])
						if val != "" {
							seen[val]++
						}
					}
				}
				if err != nil {
					break
				}
			}
			_ = rows.Close()
		}

		if limit > 0 && uint64(len(seen)) >= limit {
			break
		}
	}

	result := make([]logstorage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		result = append(result, logstorage.ValueWithHits{Value: v, Hits: hits})
	}
	if limit > 0 && uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *Storage) GetStreamFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	streamFields := s.registry.StreamFields()
	result := make([]logstorage.ValueWithHits, 0, len(streamFields))
	for _, name := range streamFields {
		result = append(result, logstorage.ValueWithHits{Value: name, Hits: 1})
	}
	return result, nil
}

func (s *Storage) GetStreamFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return s.GetFieldValues(ctx, tenantIDs, q, fieldName, limit)
}

func (s *Storage) GetStreams(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	streamColName := "_stream"
	if m := s.registry.ResolveToParquet(streamColName); m != nil {
		streamColName = m.ParquetColumn
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			logger.Warnf("get file data for streams: %s; key=%s", err, fi.Key)
			continue
		}

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			logger.Warnf("open parquet for streams: %s; key=%s", err, fi.Key)
			continue
		}

		streamIdx := findColumnIndex(f.Root(), streamColName)
		if streamIdx < 0 {
			continue
		}

		for _, rg := range f.RowGroups() {
			rows := rg.Rows()
			buf := make([]parquet.Row, 256)
			for {
				n, err := rows.ReadRows(buf)
				for i := 0; i < n; i++ {
					if streamIdx < len(buf[i]) {
						val := valueToString(buf[i][streamIdx])
						if val != "" {
							seen[val]++
						}
					}
				}
				if err != nil {
					break
				}
			}
			_ = rows.Close()
		}

		if limit > 0 && uint64(len(seen)) >= limit {
			break
		}
	}

	result := make([]logstorage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		result = append(result, logstorage.ValueWithHits{Value: v, Hits: hits})
	}
	if limit > 0 && uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}

func (s *Storage) GetStreamIDs(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	startNs, endNs := q.GetFilterTimeRange()

	files := s.manifest.GetFilesForRange(startNs, endNs)
	if len(files) == 0 {
		return nil, nil
	}

	colName := "_stream_id"
	if m := s.registry.ResolveToParquet(colName); m != nil {
		colName = m.ParquetColumn
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		data, err := s.getFileData(ctx, fi.Key, fi.Size)
		if err != nil {
			logger.Warnf("get file data for stream_ids: %s; key=%s", err, fi.Key)
			continue
		}

		f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			logger.Warnf("open parquet for stream_ids: %s; key=%s", err, fi.Key)
			continue
		}

		colIdx := findColumnIndex(f.Root(), colName)
		if colIdx < 0 {
			continue
		}

		for _, rg := range f.RowGroups() {
			rows := rg.Rows()
			buf := make([]parquet.Row, 256)
			for {
				n, err := rows.ReadRows(buf)
				for i := 0; i < n; i++ {
					if colIdx < len(buf[i]) {
						val := valueToString(buf[i][colIdx])
						if val != "" {
							seen[val]++
						}
					}
				}
				if err != nil {
					break
				}
			}
			_ = rows.Close()
		}

		if limit > 0 && uint64(len(seen)) >= limit {
			break
		}
	}

	result := make([]logstorage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		result = append(result, logstorage.ValueWithHits{Value: v, Hits: hits})
	}
	if limit > 0 && uint64(len(result)) > limit {
		result = result[:limit]
	}
	return result, nil
}
