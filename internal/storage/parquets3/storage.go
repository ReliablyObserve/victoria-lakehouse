package parquets3

import (
	"context"
	"fmt"
	"io"
	"log/slog"
	"strings"
	"time"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type Storage struct {
	cfg      *config.Config
	logger   *slog.Logger
	pool     *s3reader.ClientPool
	manifest *manifest.Manifest
	registry *schema.Registry
}

func New(cfg *config.Config, logger *slog.Logger) (*Storage, error) {
	l := logger.With("component", "parquets3")

	pool, err := s3reader.NewClientPool(context.Background(), &cfg.S3)
	if err != nil {
		return nil, fmt.Errorf("create S3 client pool: %w", err)
	}

	prefix := cfg.AutoPrefix()

	var profile schema.Profile
	if cfg.Mode == config.ModeTraces {
		profile = schema.TracesProfile
	} else {
		profile = schema.LogsProfile
	}

	m := manifest.New(cfg.S3.Bucket, prefix, logger)

	return &Storage{
		cfg:      cfg,
		logger:   l,
		pool:     pool,
		manifest: m,
		registry: schema.NewRegistry(profile),
	}, nil
}

func (s *Storage) RunQuery(ctx context.Context, qctx *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
	if !s.manifest.HasDataForRange(qctx.StartNs, qctx.EndNs) {
		s.logger.Debug("manifest fast path: no data for range",
			"start", time.Unix(0, qctx.StartNs),
			"end", time.Unix(0, qctx.EndNs),
		)
		return nil
	}

	files := s.manifest.GetFilesForRange(qctx.StartNs, qctx.EndNs)
	if len(files) == 0 {
		return nil
	}

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return err
		}
		if err := s.queryFile(ctx, fi, qctx, writeBlock); err != nil {
			s.logger.Warn("query file error", "key", fi.Key, "error", err)
			continue
		}
	}

	return nil
}

func (s *Storage) queryFile(ctx context.Context, fi manifest.FileInfo, qctx *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
	reader := s.pool.NewReaderAt(fi.Key, fi.Size)
	f, err := parquet.OpenFile(reader, fi.Size)
	if err != nil {
		return fmt.Errorf("open parquet file %s: %w", fi.Key, err)
	}

	tsIdx := findColumnIndex(f.Root(), s.registry.TimestampColumn())

	for _, rg := range f.RowGroups() {
		if err := ctx.Err(); err != nil {
			return err
		}

		if tsIdx >= 0 && !rowGroupMatchesTimeRange(rg, tsIdx, qctx.StartNs, qctx.EndNs) {
			continue
		}

		if err := s.readRowGroup(f, rg, qctx, writeBlock); err != nil {
			return err
		}
	}

	return nil
}

func (s *Storage) readRowGroup(f *parquet.File, rg parquet.RowGroup, qctx *storage.QueryContext, writeBlock storage.WriteDataBlockFunc) error {
	schema := f.Root()
	rows := rg.Rows()
	defer func() { _ = rows.Close() }()

	colNames := columnNames(schema)

	buf := make([]parquet.Row, 256)
	for {
		n, err := rows.ReadRows(buf)
		if n > 0 {
			db := s.rowsToDataBlock(buf[:n], colNames, schema, qctx)
			if db != nil && db.RowsCount > 0 {
				writeBlock(0, db)
			}
		}
		if err != nil {
			if err == io.EOF {
				break
			}
			return err
		}
	}

	return nil
}

func (s *Storage) rowsToDataBlock(rows []parquet.Row, colNames []string, root *parquet.Column, qctx *storage.QueryContext) *storage.DataBlock {
	if len(rows) == 0 {
		return nil
	}

	colCount := len(colNames)
	columns := make([][]string, colCount)
	for i := range columns {
		columns[i] = make([]string, 0, len(rows))
	}

	tsColIdx := -1
	for i, name := range colNames {
		if name == "timestamp_unix_nano" {
			tsColIdx = i
			break
		}
	}

	for _, row := range rows {
		if tsColIdx >= 0 && qctx.StartNs != 0 && qctx.EndNs != 0 {
			ts := valueToInt64(row[tsColIdx])
			if ts < qctx.StartNs || ts >= qctx.EndNs {
				continue
			}
		}

		for i := range colNames {
			if i < len(row) {
				columns[i] = append(columns[i], valueToString(row[i]))
			} else {
				columns[i] = append(columns[i], "")
			}
		}
	}

	if len(columns[0]) == 0 {
		return nil
	}

	blockCols := make([]storage.BlockColumn, 0, colCount)
	for i, name := range colNames {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}
		blockCols = append(blockCols, storage.BlockColumn{
			Name:   internalName,
			Values: columns[i],
		})
	}

	return &storage.DataBlock{
		RowsCount: len(columns[0]),
		Columns:   blockCols,
	}
}

func (s *Storage) GetFieldNames(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	files := s.manifest.GetFilesForRange(qctx.StartNs, qctx.EndNs)
	if len(files) == 0 {
		return nil, nil
	}

	seen := make(map[string]bool)
	var result []storage.ValueWithHits

	fi := files[0]
	reader := s.pool.NewReaderAt(fi.Key, fi.Size)
	f, err := parquet.OpenFile(reader, fi.Size)
	if err != nil {
		return nil, fmt.Errorf("open parquet: %w", err)
	}

	for _, name := range columnNames(f.Root()) {
		internalName := name
		if m := s.registry.ResolveFromParquet(name); m != nil {
			internalName = m.InternalName
		}
		if !seen[internalName] {
			seen[internalName] = true
			result = append(result, storage.ValueWithHits{Value: internalName, Hits: 1})
		}
	}

	return result, nil
}

func (s *Storage) GetFieldValues(ctx context.Context, qctx *storage.QueryContext, fieldName string, limit int) ([]storage.ValueWithHits, error) {
	files := s.manifest.GetFilesForRange(qctx.StartNs, qctx.EndNs)
	if len(files) == 0 {
		return nil, nil
	}

	mapping := s.registry.ResolveToParquet(fieldName)
	if mapping == nil {
		return nil, nil
	}

	seen := make(map[string]uint64)

	for _, fi := range files {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		reader := s.pool.NewReaderAt(fi.Key, fi.Size)
		f, err := parquet.OpenFile(reader, fi.Size)
		if err != nil {
			s.logger.Warn("open parquet for field values", "key", fi.Key, "error", err)
			continue
		}

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

		if limit > 0 && len(seen) >= limit {
			break
		}
	}

	result := make([]storage.ValueWithHits, 0, len(seen))
	for v, hits := range seen {
		result = append(result, storage.ValueWithHits{Value: v, Hits: hits})
	}
	return result, nil
}

func (s *Storage) GetStreamFieldNames(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetStreamFieldValues(ctx context.Context, qctx *storage.QueryContext, fieldName string) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetStreams(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetStreamIDs(ctx context.Context, qctx *storage.QueryContext) ([]storage.ValueWithHits, error) {
	return nil, nil
}

func (s *Storage) GetTenantIDs(ctx context.Context, qctx *storage.QueryContext) ([]storage.TenantID, error) {
	return nil, nil
}

func (s *Storage) Manifest() *manifest.Manifest {
	return s.manifest
}

func (s *Storage) Close() error {
	return nil
}

func rowGroupMatchesTimeRange(rg parquet.RowGroup, tsColIdx int, startNs, endNs int64) bool {
	cols := rg.ColumnChunks()
	if tsColIdx >= len(cols) {
		return true
	}

	idx, err := cols[tsColIdx].ColumnIndex()
	if err != nil || idx == nil {
		return true
	}

	numPages := idx.NumPages()
	if numPages == 0 {
		return true
	}

	minVal := idx.MinValue(0)
	maxVal := idx.MaxValue(numPages - 1)

	rgMin := minVal.Int64()
	rgMax := maxVal.Int64()

	return rgMax >= startNs && rgMin < endNs
}

func findColumnIndex(root *parquet.Column, name string) int {
	for i, col := range root.Columns() {
		if col.Name() == name {
			return i
		}
	}
	return -1
}

func columnNames(root *parquet.Column) []string {
	cols := root.Columns()
	names := make([]string, len(cols))
	for i, col := range cols {
		names[i] = col.Name()
	}
	return names
}

func valueToString(v parquet.Value) string {
	if v.IsNull() {
		return ""
	}
	switch v.Kind() {
	case parquet.Int32:
		return fmt.Sprintf("%d", v.Int32())
	case parquet.Int64:
		return fmt.Sprintf("%d", v.Int64())
	case parquet.Int96:
		return v.String()
	case parquet.Float:
		return fmt.Sprintf("%g", v.Float())
	case parquet.Double:
		return fmt.Sprintf("%g", v.Double())
	case parquet.ByteArray, parquet.FixedLenByteArray:
		b := v.ByteArray()
		if isPrintable(b) {
			return string(b)
		}
		return fmt.Sprintf("%x", b)
	case parquet.Boolean:
		if v.Boolean() {
			return "true"
		}
		return "false"
	default:
		return v.String()
	}
}

func valueToInt64(v parquet.Value) int64 {
	if v.IsNull() {
		return 0
	}
	switch v.Kind() {
	case parquet.Int64:
		return v.Int64()
	case parquet.Int32:
		return int64(v.Int32())
	default:
		return 0
	}
}

func isPrintable(b []byte) bool {
	for _, c := range b {
		if c < 0x20 && c != '\t' && c != '\n' && c != '\r' {
			if !strings.ContainsRune("\t\n\r", rune(c)) {
				return false
			}
		}
	}
	return true
}
