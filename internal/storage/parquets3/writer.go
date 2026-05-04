package parquets3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"log/slog"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/wal"
)

// BatchWriter buffers incoming rows per partition and flushes them as
// Parquet files to S3 on a configurable interval or size threshold.
type BatchWriter struct {
	cfg      *config.InsertConfig
	pool     *s3reader.ClientPool
	manifest *manifest.Manifest
	prefix   string
	mode     config.Mode
	logger   *slog.Logger

	mu         sync.Mutex
	logBufs    map[string][]schema.LogRow
	traceBufs  map[string][]schema.TraceRow
	totalRows  atomic.Int64
	totalBytes atomic.Int64

	wal *wal.WAL // nil if WAL disabled

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewBatchWriter(cfg *config.InsertConfig, pool *s3reader.ClientPool,
	m *manifest.Manifest, prefix string, mode config.Mode, logger *slog.Logger) *BatchWriter {

	bw := &BatchWriter{
		cfg:       cfg,
		pool:      pool,
		manifest:  m,
		prefix:    prefix,
		mode:      mode,
		logger:    logger.With("component", "writer"),
		logBufs:   make(map[string][]schema.LogRow),
		traceBufs: make(map[string][]schema.TraceRow),
		stopCh:    make(chan struct{}),
	}

	if cfg.WALEnabled && cfg.WALDir != "" {
		walPath := filepath.Join(cfg.WALDir, "lakehouse.wal")
		w, err := wal.Open(walPath, cfg.WALMaxBytesN())
		if err != nil {
			logger.Error("WAL open failed, continuing without WAL", "error", err)
		} else {
			bw.wal = w
		}
	}

	return bw
}

func (w *BatchWriter) Start() {
	w.wg.Add(1)
	go w.flushLoop()
}

func (w *BatchWriter) Stop() {
	close(w.stopCh)
	w.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.FlushAll(ctx); err != nil {
		w.logger.Error("final flush failed", "error", err)
	}
}

func (w *BatchWriter) flushLoop() {
	defer w.wg.Done()
	ticker := time.NewTicker(w.cfg.FlushInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
			if err := w.FlushAll(ctx); err != nil {
				w.logger.Error("periodic flush failed", "error", err)
			}
			cancel()
		case <-w.stopCh:
			return
		}
	}
}

// AddLogRows buffers log rows for later flush. Non-blocking.
func (w *BatchWriter) AddLogRows(rows []schema.LogRow) {
	if len(rows) == 0 {
		return
	}

	if w.wal != nil {
		for i := range rows {
			if err := w.wal.AppendLog(&rows[i]); err != nil {
				w.logger.Error("WAL append failed", "error", err)
				break
			}
		}
	}

	byPartition := make(map[string][]schema.LogRow)
	for i := range rows {
		p := partitionFromNano(rows[i].TimestampUnixNano)
		byPartition[p] = append(byPartition[p], rows[i])
	}

	w.mu.Lock()
	for p, pRows := range byPartition {
		w.logBufs[p] = append(w.logBufs[p], pRows...)
	}
	w.mu.Unlock()

	w.totalRows.Add(int64(len(rows)))

	w.checkSizeThreshold()
}

// AddTraceRows buffers trace rows for later flush. Non-blocking.
func (w *BatchWriter) AddTraceRows(rows []schema.TraceRow) {
	if len(rows) == 0 {
		return
	}

	if w.wal != nil {
		for i := range rows {
			if err := w.wal.AppendTrace(&rows[i]); err != nil {
				w.logger.Error("WAL append failed", "error", err)
				break
			}
		}
	}

	byPartition := make(map[string][]schema.TraceRow)
	for i := range rows {
		p := partitionFromNano(rows[i].TimestampUnixNano)
		byPartition[p] = append(byPartition[p], rows[i])
	}

	w.mu.Lock()
	for p, pRows := range byPartition {
		w.traceBufs[p] = append(w.traceBufs[p], pRows...)
	}
	w.mu.Unlock()

	w.totalRows.Add(int64(len(rows)))

	w.checkSizeThreshold()
}

func (w *BatchWriter) checkSizeThreshold() {
	total := int(w.totalRows.Load())
	if total >= w.cfg.MaxBufferRows {
		w.triggerFlush()
		return
	}

	targetBytes := w.cfg.TargetFileSizeN()
	if targetBytes <= 0 {
		return
	}

	w.mu.Lock()
	var needsFlush bool
	for _, rows := range w.logBufs {
		if estimateRawBytesLogs(rows) >= targetBytes {
			needsFlush = true
			break
		}
	}
	if !needsFlush {
		for _, rows := range w.traceBufs {
			if estimateRawBytesTraces(rows) >= targetBytes {
				needsFlush = true
				break
			}
		}
	}
	w.mu.Unlock()

	if needsFlush {
		w.triggerFlush()
	}
}

func (w *BatchWriter) triggerFlush() {
	ctx, cancel := context.WithTimeout(context.Background(), 60*time.Second)
	defer cancel()
	if err := w.FlushAll(ctx); err != nil {
		w.logger.Error("triggered flush failed", "error", err)
	}
}

// FlushAll snapshots all buffers and flushes them to S3.
func (w *BatchWriter) FlushAll(ctx context.Context) error {
	w.mu.Lock()
	logSnap := w.logBufs
	traceSnap := w.traceBufs
	w.logBufs = make(map[string][]schema.LogRow)
	w.traceBufs = make(map[string][]schema.TraceRow)
	w.totalRows.Store(0)
	w.mu.Unlock()

	var errs []error

	for partition, rows := range logSnap {
		if err := w.flushLogPartition(ctx, partition, rows); err != nil {
			errs = append(errs, fmt.Errorf("flush logs %s: %w", partition, err))
		}
	}

	for partition, rows := range traceSnap {
		if err := w.flushTracePartition(ctx, partition, rows); err != nil {
			errs = append(errs, fmt.Errorf("flush traces %s: %w", partition, err))
		}
	}

	if len(errs) == 0 && w.wal != nil {
		if err := w.wal.Truncate(); err != nil {
			w.logger.Error("WAL truncate failed", "error", err)
		}
	}

	if len(errs) > 0 {
		return fmt.Errorf("flush errors: %v", errs)
	}
	return nil
}

func (w *BatchWriter) flushLogPartition(ctx context.Context, partition string, rows []schema.LogRow) error {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano
	})

	result, err := writeLogsParquet(rows, w.cfg.RowGroupSize, w.cfg.CompressionLevel)
	if err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}

	batchID := randomBatchID()
	key := fmt.Sprintf("%s%s/%s.parquet", w.prefix, partition, batchID)

	if err := w.pool.Upload(ctx, key, result.Data); err != nil {
		return err
	}

	fi := manifest.FileInfo{
		Key:               key,
		Size:              int64(len(result.Data)),
		RowCount:          int64(len(rows)),
		MinTimeNs:         rows[0].TimestampUnixNano,
		MaxTimeNs:         rows[len(rows)-1].TimestampUnixNano,
		RawBytes:          result.RawBytes,
		SchemaFingerprint: schemaFingerprint(w.mode),
		Labels:            extractLogLabels(rows),
	}
	w.manifest.AddFile(partition, fi)

	w.totalBytes.Add(int64(len(result.Data)))

	w.logger.Debug("flushed log partition",
		"partition", partition,
		"rows", len(rows),
		"bytes", len(result.Data),
		"ratio", fi.CompressionRatio(),
		"key", key,
	)

	return nil
}

func (w *BatchWriter) flushTracePartition(ctx context.Context, partition string, rows []schema.TraceRow) error {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano
	})

	result, err := writeTracesParquet(rows, w.cfg.RowGroupSize, w.cfg.CompressionLevel)
	if err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}

	batchID := randomBatchID()
	key := fmt.Sprintf("%s%s/%s.parquet", w.prefix, partition, batchID)

	if err := w.pool.Upload(ctx, key, result.Data); err != nil {
		return err
	}

	fi := manifest.FileInfo{
		Key:               key,
		Size:              int64(len(result.Data)),
		RowCount:          int64(len(rows)),
		MinTimeNs:         rows[0].TimestampUnixNano,
		MaxTimeNs:         rows[len(rows)-1].TimestampUnixNano,
		RawBytes:          result.RawBytes,
		SchemaFingerprint: schemaFingerprint(w.mode),
		Labels:            extractTraceLabels(rows),
	}
	w.manifest.AddFile(partition, fi)

	w.totalBytes.Add(int64(len(result.Data)))

	w.logger.Debug("flushed trace partition",
		"partition", partition,
		"rows", len(rows),
		"bytes", len(result.Data),
		"ratio", fi.CompressionRatio(),
		"key", key,
	)

	return nil
}

type flushResult struct {
	Data     []byte
	RawBytes int64
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

func writeLogsParquet(rows []schema.LogRow, rowGroupSize int, compressionLevel int) (*flushResult, error) {
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
	return &flushResult{
		Data:     buf.Bytes(),
		RawBytes: estimateRawBytesLogs(rows),
	}, nil
}

func writeTracesParquet(rows []schema.TraceRow, rowGroupSize int, compressionLevel int) (*flushResult, error) {
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
	return &flushResult{
		Data:     buf.Bytes(),
		RawBytes: estimateRawBytesTraces(rows),
	}, nil
}

func estimateRawBytesLogs(rows []schema.LogRow) int64 {
	var total int64
	for i := range rows {
		total += int64(unsafe.Sizeof(rows[i]))
		total += int64(len(rows[i].Body))
		total += int64(len(rows[i].ServiceName))
		total += int64(len(rows[i].TraceID))
		for k, v := range rows[i].ResourceAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range rows[i].LogAttributes {
			total += int64(len(k) + len(v))
		}
	}
	return total
}

func estimateRawBytesTraces(rows []schema.TraceRow) int64 {
	var total int64
	for i := range rows {
		total += int64(unsafe.Sizeof(rows[i]))
		total += int64(len(rows[i].TraceID))
		total += int64(len(rows[i].SpanName))
		total += int64(len(rows[i].ServiceName))
		for k, v := range rows[i].ResourceAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range rows[i].SpanAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range rows[i].ScopeAttributes {
			total += int64(len(k) + len(v))
		}
	}
	return total
}

func schemaFingerprint(mode config.Mode) string {
	h := sha256.New()
	h.Write([]byte(string(mode)))
	b := make([]byte, 8)
	binary.LittleEndian.PutUint64(b, 1)
	h.Write(b)
	return fmt.Sprintf("%x", h.Sum(nil)[:8])
}

func partitionFromNano(ns int64) string {
	t := time.Unix(0, ns).UTC()
	return fmt.Sprintf("dt=%s/hour=%02d", t.Format("2006-01-02"), t.Hour())
}

func randomBatchID() string {
	b := make([]byte, 8)
	_, _ = rand.Read(b)
	return fmt.Sprintf("%x", b)
}

// BufferedRows returns the total number of rows currently buffered (unflushed).
func (w *BatchWriter) BufferedRows() int64 {
	return w.totalRows.Load()
}

// TotalBytesUploaded returns the total bytes uploaded to S3 since startup.
func (w *BatchWriter) TotalBytesUploaded() int64 {
	return w.totalBytes.Load()
}

// ReplayWAL reads all entries from the WAL back into memory buffers.
// Call this once at startup for crash recovery.
func (w *BatchWriter) ReplayWAL() (int, int) {
	if w.wal == nil {
		return 0, 0
	}
	logs, traces, err := w.wal.Replay()
	if err != nil {
		w.logger.Error("WAL replay error", "error", err)
	}
	if len(logs) > 0 {
		w.AddLogRows(logs)
	}
	if len(traces) > 0 {
		w.AddTraceRows(traces)
	}
	w.logger.Info("WAL replayed", "logs", len(logs), "traces", len(traces))
	return len(logs), len(traces)
}

// CanWriteData checks if the S3 backend is reachable.
func (w *BatchWriter) CanWriteData(ctx context.Context) error {
	if w.wal != nil && w.wal.IsFull() {
		return fmt.Errorf("WAL full")
	}
	testKey := w.prefix + "_write_check"
	return w.pool.Upload(ctx, testKey, []byte("ok"))
}

// BufferedLogRows returns unflushed log rows matching a time range (for buffer query protocol).
func (w *BatchWriter) BufferedLogRows(startNs, endNs int64) []schema.LogRow {
	w.mu.Lock()
	defer w.mu.Unlock()

	var result []schema.LogRow
	for _, rows := range w.logBufs {
		for _, r := range rows {
			if r.TimestampUnixNano >= startNs && r.TimestampUnixNano < endNs {
				result = append(result, r)
			}
		}
	}
	return result
}

// BufferedTraceRows returns unflushed trace rows matching a time range.
func (w *BatchWriter) BufferedTraceRows(startNs, endNs int64) []schema.TraceRow {
	w.mu.Lock()
	defer w.mu.Unlock()

	var result []schema.TraceRow
	for _, rows := range w.traceBufs {
		for _, r := range rows {
			if r.TimestampUnixNano >= startNs && r.TimestampUnixNano < endNs {
				result = append(result, r)
			}
		}
	}
	return result
}

// PartitionKey builds an S3 key in Hive partition format.
func PartitionKey(prefix, partition, batchID string) string {
	if !strings.HasSuffix(prefix, "/") && prefix != "" {
		prefix += "/"
	}
	return fmt.Sprintf("%s%s/%s.parquet", prefix, partition, batchID)
}
