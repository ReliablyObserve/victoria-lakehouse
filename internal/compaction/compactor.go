package compaction

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"sort"
	"time"

	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// CompactorPool abstracts S3 operations needed by the compactor.
type CompactorPool interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

// CompactorConfig holds all dependencies for the Compactor.
type CompactorConfig struct {
	Pool             CompactorPool
	Manifest         *manifest.Manifest
	Prefix           string
	Mode             config.Mode
	RowGroupSize     int
	CompressionLevel int
	Logger           *slog.Logger
}

// CompactResult summarises one compaction run.
type CompactResult struct {
	Partition    string
	InputFiles   []string
	OutputFile   string
	RowsMerged   int64
	BytesRead    int64
	BytesWritten int64
	OutputLevel  int
	Duration     time.Duration
}

// Compactor merges small Parquet files into larger ones.
type Compactor struct {
	pool             CompactorPool
	manifest         *manifest.Manifest
	prefix           string
	mode             config.Mode
	rowGroupSize     int
	compressionLevel int
	logger           *slog.Logger
}

// NewCompactor creates a Compactor from the given config.
func NewCompactor(cfg CompactorConfig) *Compactor {
	return &Compactor{
		pool:             cfg.Pool,
		manifest:         cfg.Manifest,
		prefix:           cfg.Prefix,
		mode:             cfg.Mode,
		rowGroupSize:     cfg.RowGroupSize,
		compressionLevel: cfg.CompressionLevel,
		logger:           cfg.Logger.With("component", "compactor"),
	}
}

// Compact reads the given files from S3, merges all rows sorted by timestamp,
// writes a single compacted Parquet file, uploads it, updates the manifest,
// and removes the source files.
func (c *Compactor) Compact(ctx context.Context, partition string, files []manifest.FileInfo, sourceLevel int) (*CompactResult, error) {
	start := time.Now()

	// Verify all files share the same schema fingerprint.
	if len(files) == 0 {
		return nil, fmt.Errorf("no files to compact")
	}
	fp := files[0].SchemaFingerprint
	for _, f := range files[1:] {
		if f.SchemaFingerprint != fp {
			return nil, fmt.Errorf("schema fingerprint mismatch: %s vs %s", fp, f.SchemaFingerprint)
		}
	}

	var (
		result    CompactResult
		allData   [][]byte
		inputKeys []string
	)
	result.Partition = partition
	result.OutputLevel = sourceLevel + 1

	// Download all source files.
	for _, f := range files {
		data, err := c.pool.Download(ctx, f.Key)
		if err != nil {
			return nil, fmt.Errorf("download %s: %w", f.Key, err)
		}
		if data == nil {
			return nil, fmt.Errorf("download %s: file not found", f.Key)
		}
		allData = append(allData, data)
		result.BytesRead += int64(len(data))
		inputKeys = append(inputKeys, f.Key)
	}
	result.InputFiles = inputKeys

	// Merge, write, and upload based on mode.
	var outputData []byte
	var rowsMerged int64

	switch c.mode {
	case config.ModeLogs:
		merged, err := c.mergeLogFiles(allData)
		if err != nil {
			return nil, err
		}
		rowsMerged = int64(len(merged))
		out, err := writeCompactedLogs(merged, c.rowGroupSize, c.compressionLevel)
		if err != nil {
			return nil, fmt.Errorf("write compacted logs: %w", err)
		}
		outputData = out

	case config.ModeTraces:
		merged, err := c.mergeTraceFiles(allData)
		if err != nil {
			return nil, err
		}
		rowsMerged = int64(len(merged))
		out, err := writeCompactedTraces(merged, c.rowGroupSize, c.compressionLevel)
		if err != nil {
			return nil, fmt.Errorf("write compacted traces: %w", err)
		}
		outputData = out

	default:
		return nil, fmt.Errorf("unsupported mode: %s", c.mode)
	}

	result.RowsMerged = rowsMerged
	result.BytesWritten = int64(len(outputData))

	// Build output key.
	short := uuid.New().String()[:8]
	outputKey := fmt.Sprintf("%s%s/compacted-L%d-%s.parquet", c.prefix, partition, result.OutputLevel, short)
	result.OutputFile = outputKey

	// Upload compacted file.
	if err := c.pool.Upload(ctx, outputKey, outputData); err != nil {
		return nil, fmt.Errorf("upload compacted file: %w", err)
	}

	// Determine time bounds from the merged data.
	var minTime, maxTime int64
	if rowsMerged > 0 {
		// We already sorted, so first/last rows give bounds.
		// Re-read from files metadata would be heavier; use the input FileInfo instead.
		minTime = files[0].MinTimeNs
		maxTime = files[0].MaxTimeNs
		for _, f := range files[1:] {
			if f.MinTimeNs < minTime {
				minTime = f.MinTimeNs
			}
			if f.MaxTimeNs > maxTime {
				maxTime = f.MaxTimeNs
			}
		}
	}

	// Add compacted file to manifest.
	c.manifest.AddFile(partition, manifest.FileInfo{
		Key:               outputKey,
		Size:              int64(len(outputData)),
		RowCount:          rowsMerged,
		MinTimeNs:         minTime,
		MaxTimeNs:         maxTime,
		SchemaFingerprint: fp,
		CompactionLevel:   result.OutputLevel,
	})

	// Remove source files from manifest and S3.
	for _, f := range files {
		c.manifest.RemoveFile(partition, f.Key)
		if err := c.pool.Delete(ctx, f.Key); err != nil {
			c.logger.Warn("failed to delete source file", "key", f.Key, "error", err)
		}
	}

	result.Duration = time.Since(start)

	c.logger.Info("compaction complete",
		"partition", partition,
		"input_files", len(files),
		"rows_merged", rowsMerged,
		"bytes_read", result.BytesRead,
		"bytes_written", result.BytesWritten,
		"output_level", result.OutputLevel,
		"duration", result.Duration,
	)

	return &result, nil
}

func (c *Compactor) mergeLogFiles(allData [][]byte) ([]schema.LogRow, error) {
	var merged []schema.LogRow
	for _, data := range allData {
		rows, err := readLogRows(data)
		if err != nil {
			return nil, fmt.Errorf("read log rows: %w", err)
		}
		merged = append(merged, rows...)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].TimestampUnixNano < merged[j].TimestampUnixNano
	})
	return merged, nil
}

func (c *Compactor) mergeTraceFiles(allData [][]byte) ([]schema.TraceRow, error) {
	var merged []schema.TraceRow
	for _, data := range allData {
		rows, err := readTraceRows(data)
		if err != nil {
			return nil, fmt.Errorf("read trace rows: %w", err)
		}
		merged = append(merged, rows...)
	}
	sort.Slice(merged, func(i, j int) bool {
		return merged[i].TimestampUnixNano < merged[j].TimestampUnixNano
	})
	return merged, nil
}

func readLogRows(data []byte) ([]schema.LogRow, error) {
	reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	n := int(reader.NumRows())
	rows := make([]schema.LogRow, n)
	total, err := reader.Read(rows)
	if err != nil && total == 0 {
		return nil, fmt.Errorf("read parquet: %w", err)
	}
	return rows[:total], nil
}

func readTraceRows(data []byte) ([]schema.TraceRow, error) {
	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	n := int(reader.NumRows())
	rows := make([]schema.TraceRow, n)
	total, err := reader.Read(rows)
	if err != nil && total == 0 {
		return nil, fmt.Errorf("read parquet: %w", err)
	}
	return rows[:total], nil
}

func writeCompactedLogs(rows []schema.LogRow, rowGroupSize int, compressionLevel int) ([]byte, error) {
	var buf bytes.Buffer
	codec := &zstd.Codec{Level: zstdLevel(compressionLevel)}
	writer := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(10, "service.name"),
			parquet.SplitBlockFilter(10, "trace_id"),
		),
	)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeCompactedTraces(rows []schema.TraceRow, rowGroupSize int, compressionLevel int) ([]byte, error) {
	var buf bytes.Buffer
	codec := &zstd.Codec{Level: zstdLevel(compressionLevel)}
	writer := parquet.NewGenericWriter[schema.TraceRow](&buf,
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(10, "service.name"),
			parquet.SplitBlockFilter(10, "trace_id"),
		),
	)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func zstdLevel(level int) zstd.Level {
	switch {
	case level <= 1:
		return zstd.SpeedFastest
	case level <= 5:
		return zstd.SpeedDefault
	case level <= 10:
		return zstd.SpeedBetterCompression
	default:
		return zstd.SpeedBestCompression
	}
}
