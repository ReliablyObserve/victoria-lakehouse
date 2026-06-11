package parquets3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// BatchWriter buffers incoming rows per partition and flushes them as
// Parquet files to S3 on a configurable interval or size threshold.
// StatsCallback is called after each successful file flush with the
// compressed size, raw size, row count, and storage class. The flush
// invokes the callback once per distinct tenant in the flushed batch,
// with bytes attributed in proportion to that tenant's row share, so
// the registry can track per-tenant ingest from mixed-tenant batches.
type StatsCallback func(accountID, projectID uint32, compressedBytes, rawBytes, rows int64, storageClass string)

// FlushCacheCallback is called after a successful S3 upload to cache the
// flushed file data locally (write-through cache).
type FlushCacheCallback func(fileKey string, data []byte)

// TenantPrefixFunc returns the S3 key prefix where a row with the given
// (AccountID, ProjectID) tenant identity should land. The returned
// prefix must end in "/". When nil, the writer's default prefix is used
// for every row (single-tenant deployment).
type TenantPrefixFunc func(accountID, projectID uint32) string

// TenantBucketFunc returns the S3 bucket where a tenant's files
// should land. Empty string = default bucket (prefix isolation).
type TenantBucketFunc func(accountID, projectID uint32) string

// TenantPoolFunc returns a PoolWriter for the given bucket name.
type TenantPoolFunc func(bucket string) PoolWriter

// PoolWriter is the subset of s3reader.ClientPool the BatchWriter
// needs for tenant-aware uploads.
type PoolWriter interface {
	Upload(ctx context.Context, key string, data []byte) error
}

type BatchWriter struct {
	cfg      *config.InsertConfig
	pool     *s3reader.ClientPool
	manifest *manifest.Manifest
	prefix   string
	mode     config.Mode

	mu         sync.Mutex
	logBufs    map[string][]schema.LogRow
	traceBufs  map[string][]schema.TraceRow
	totalRows  atomic.Int64
	totalBytes atomic.Int64

	catalogObserver *catalogObserver
	statsCallback   StatsCallback
	flushCacheCb    FlushCacheCallback
	tenantPrefix    TenantPrefixFunc
	tenantBucket    TenantBucketFunc
	tenantPool      TenantPoolFunc

	stopCh chan struct{}
	wg     sync.WaitGroup
}

func NewBatchWriter(cfg *config.InsertConfig, pool *s3reader.ClientPool,
	m *manifest.Manifest, prefix string, mode config.Mode) *BatchWriter {

	bw := &BatchWriter{
		cfg:       cfg,
		pool:      pool,
		manifest:  m,
		prefix:    prefix,
		mode:      mode,
		logBufs:   make(map[string][]schema.LogRow),
		traceBufs: make(map[string][]schema.TraceRow),
		stopCh:    make(chan struct{}),
	}

	return bw
}

func (w *BatchWriter) Start() {
	w.wg.Add(1)
	go w.flushLoop()
}

func (w *BatchWriter) SetStatsCallback(cb StatsCallback) {
	w.statsCallback = cb
}

func (w *BatchWriter) SetFlushCacheCallback(cb FlushCacheCallback) {
	w.flushCacheCb = cb
}

func (w *BatchWriter) SetTenantPrefix(f TenantPrefixFunc) {
	w.tenantPrefix = f
}

func (w *BatchWriter) SetTenantBucket(f TenantBucketFunc) {
	w.tenantBucket = f
}

func (w *BatchWriter) SetTenantPool(f TenantPoolFunc) {
	w.tenantPool = f
}

func (w *BatchWriter) bucketForTenant(accountID, projectID uint32) (string, PoolWriter) {
	if w.tenantBucket == nil || w.tenantPool == nil {
		return "", w.pool
	}
	bucket := w.tenantBucket(accountID, projectID)
	if bucket == "" {
		return "", w.pool
	}
	if p := w.tenantPool(bucket); p != nil {
		return bucket, p
	}
	return "", w.pool
}

func (w *BatchWriter) prefixForTenant(accountID, projectID uint32) string {
	if w.tenantPrefix == nil {
		return w.prefix
	}
	if p := w.tenantPrefix(accountID, projectID); p != "" {
		return p
	}
	return w.prefix
}

func (w *BatchWriter) Stop() {
	close(w.stopCh)
	w.wg.Wait()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := w.FlushAll(ctx); err != nil {
		logger.Errorf("final flush failed: %s", err)
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
				logger.Errorf("periodic flush failed: %s", err)
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
	metrics.InsertRowsTotal.Add(len(rows))

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
	metrics.InsertRowsBuffered.Set(w.totalRows.Load())

	w.checkSizeThreshold()
}

// AddTraceRows buffers trace rows for later flush. Non-blocking.
func (w *BatchWriter) AddTraceRows(rows []schema.TraceRow) {
	if len(rows) == 0 {
		return
	}
	metrics.InsertRowsTotal.Add(len(rows))

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
	metrics.InsertRowsBuffered.Set(w.totalRows.Load())

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
		logger.Errorf("triggered flush failed: %s", err)
	}
}

// FlushAll snapshots all buffers and flushes them to S3.
func (w *BatchWriter) FlushAll(ctx context.Context) error {
	flushStart := time.Now()

	w.mu.Lock()
	logSnap := w.logBufs
	traceSnap := w.traceBufs
	w.logBufs = make(map[string][]schema.LogRow)
	w.traceBufs = make(map[string][]schema.TraceRow)
	w.totalRows.Store(0)
	w.mu.Unlock()

	metrics.InsertRowsBuffered.Set(0)
	metrics.InsertPartitionsActive.Set(int64(len(logSnap) + len(traceSnap)))

	var errs []error

	for partition, rows := range logSnap {
		if err := w.flushLogPartition(ctx, partition, rows); err != nil {
			metrics.InsertFlushErrorsTotal.Inc()
			errs = append(errs, fmt.Errorf("flush logs %s: %w", partition, err))
		}
	}

	for partition, rows := range traceSnap {
		if err := w.flushTracePartition(ctx, partition, rows); err != nil {
			metrics.InsertFlushErrorsTotal.Inc()
			errs = append(errs, fmt.Errorf("flush traces %s: %w", partition, err))
		}
	}

	if len(logSnap) > 0 || len(traceSnap) > 0 {
		metrics.InsertFlushTotal.Inc()
		metrics.InsertFlushDuration.Observe(time.Since(flushStart).Seconds())
	}
	// OUTSIDE the non-empty gate: dirty bundles from a failed prior PUT (or a
	// shutdown-time empty flush) must still persist.
	if w.catalogObserver != nil {
		w.catalogObserver.persistDirty(ctx)
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

	groups := groupLogRowsByTenant(rows)
	for _, g := range groups {
		if err := w.flushLogTenantGroup(ctx, partition, g.AccountID, g.ProjectID, g.Rows); err != nil {
			return err
		}
	}
	return nil
}

func (w *BatchWriter) flushLogTenantGroup(ctx context.Context, partition string, accountID, projectID uint32, rows []schema.LogRow) error {
	result, err := writeLogsParquet(rows, w.cfg.RowGroupSize, w.cfg.CompressionLevel)
	if err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}

	batchID := randomBatchID()
	key := fmt.Sprintf("%s%s/%s.parquet", w.prefixForTenant(accountID, projectID), partition, batchID)

	bucket, uploader := w.bucketForTenant(accountID, projectID)

	metrics.S3RequestsTotal.Inc("PUT")
	if err := uploader.Upload(ctx, key, result.Data); err != nil {
		metrics.S3ErrorsTotal.Inc("PUT")
		return err
	}
	metrics.InsertBytesUploaded.Add(len(result.Data))

	labelStart := time.Now()
	labels := extractLogLabels(rows)
	metrics.WriterLabelExtractionsTotal.Inc("logs")
	metrics.WriterLabelExtractionDuration.Observe(time.Since(labelStart).Seconds())
	var labelValueCount int
	for _, vals := range labels {
		labelValueCount += len(vals)
	}
	metrics.WriterLabelValuesTotal.Add("logs", labelValueCount)

	// True min/max scan — NOT rows[0]/rows[len-1]. The flush input is not
	// guaranteed time-sorted (and the upcoming (stream_id, timestamp) row
	// order makes positional bounds actively wrong); an understated
	// MaxTimeNs breaks manifest range pruning AND the bufferWatermark
	// double-count guard. See schema.LogRowTimeBounds. Twin of the root
	// module's flushLogTenantGroup — keep in sync.
	minTimeNs, maxTimeNs := schema.LogRowTimeBounds(rows)
	fi := manifest.FileInfo{
		Key:               key,
		Bucket:            bucket,
		Size:              int64(len(result.Data)),
		RowCount:          int64(len(rows)),
		MinTimeNs:         minTimeNs,
		MaxTimeNs:         maxTimeNs,
		RawBytes:          result.RawBytes,
		SchemaFingerprint: schemaFingerprint(w.mode),
		Labels:            labels,
		LabelAggregates:   schema.ExtractLogLabelAggregates(rows),
	}
	w.manifest.AddFile(partition, fi)
	if w.catalogObserver != nil {
		// UNCAPPED bloom feed (trace_id + service.name): the capped label map
		// false-negatives on values past maxLabelsPerField — a bloom must see
		// every value present or it wrongly excludes files.
		w.catalogObserver.OnFileFlush(partition, fi, labels, extractLogBloomValues(rows))
		w.catalogObserver.tapLogRows(rows)
	}

	if w.statsCallback != nil {
		w.statsCallback(accountID, projectID, int64(len(result.Data)), result.RawBytes, int64(len(rows)), "STANDARD")
	}

	if w.flushCacheCb != nil {
		w.flushCacheCb(key, result.Data)
	}

	w.totalBytes.Add(int64(len(result.Data)))

	logger.Infof("flushed log partition; partition=%s, tenant=%d:%d, rows=%d, bytes=%d, ratio=%v, key=%s",
		partition, accountID, projectID, len(rows), len(result.Data), fi.CompressionRatio(), key)

	return nil
}

func (w *BatchWriter) flushTracePartition(ctx context.Context, partition string, rows []schema.TraceRow) error {
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano
	})

	groups := groupTraceRowsByTenant(rows)
	for _, g := range groups {
		if err := w.flushTraceTenantGroup(ctx, partition, g.AccountID, g.ProjectID, g.Rows); err != nil {
			return err
		}
	}
	return nil
}

func (w *BatchWriter) flushTraceTenantGroup(ctx context.Context, partition string, accountID, projectID uint32, rows []schema.TraceRow) error {
	result, err := writeTracesParquet(rows, w.cfg.RowGroupSize, w.cfg.CompressionLevel)
	if err != nil {
		return fmt.Errorf("write parquet: %w", err)
	}

	batchID := randomBatchID()
	key := fmt.Sprintf("%s%s/%s.parquet", w.prefixForTenant(accountID, projectID), partition, batchID)

	bucket, uploader := w.bucketForTenant(accountID, projectID)

	metrics.S3RequestsTotal.Inc("PUT")
	if err := uploader.Upload(ctx, key, result.Data); err != nil {
		metrics.S3ErrorsTotal.Inc("PUT")
		return err
	}
	metrics.InsertBytesUploaded.Add(len(result.Data))

	labelStart2 := time.Now()
	labels2 := extractTraceLabels(rows)
	metrics.WriterLabelExtractionsTotal.Inc("traces")
	metrics.WriterLabelExtractionDuration.Observe(time.Since(labelStart2).Seconds())
	var labelValueCount2 int
	for _, vals := range labels2 {
		labelValueCount2 += len(vals)
	}
	metrics.WriterLabelValuesTotal.Add("traces", labelValueCount2)

	// True min/max scan — see the logs flush above and schema.TraceRowTimeBounds.
	minTimeNs, maxTimeNs := schema.TraceRowTimeBounds(rows)
	fi := manifest.FileInfo{
		Key:               key,
		Bucket:            bucket,
		Size:              int64(len(result.Data)),
		RowCount:          int64(len(rows)),
		MinTimeNs:         minTimeNs,
		MaxTimeNs:         maxTimeNs,
		RawBytes:          result.RawBytes,
		SchemaFingerprint: schemaFingerprint(w.mode),
		Labels:            labels2,
		LabelAggregates:   schema.ExtractTraceLabelAggregates(rows),
	}
	w.manifest.AddFile(partition, fi)
	traceBloomValues := extractTraceBloomValues(rows)
	if w.catalogObserver != nil {
		// UNCAPPED bloom feed — same rationale as the logs flush above.
		w.catalogObserver.OnFileFlush(partition, fi, labels2, traceBloomValues)
		w.catalogObserver.tapTraceRows(rows)
	}

	w.totalBytes.Add(int64(len(result.Data)))

	if w.statsCallback != nil {
		w.statsCallback(accountID, projectID, int64(len(result.Data)), result.RawBytes, int64(len(rows)), "STANDARD")
	}

	if w.flushCacheCb != nil {
		w.flushCacheCb(key, result.Data)
	}

	logger.Infof("flushed trace partition; partition=%s, tenant=%d:%d, rows=%d, bytes=%d, ratio=%v, key=%s",
		partition, accountID, projectID, len(rows), len(result.Data), fi.CompressionRatio(), key)

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

	// Pre-compute token bloom metadata for each row group so it can be
	// embedded as file-level key-value metadata in the Parquet footer.
	opts := []parquet.WriterOption{
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
		parquet.BloomFilters(bloomFilters(schema.LogBloomColumns(activeSlotResolver.BloomSlots()...))...),
	}
	if kv := schema.MarshalSlotMapping(activeSlotResolver.Mapping()); kv != nil {
		opts = append(opts, parquet.KeyValueMetadata(schema.DedicatedSlotsMetaKey, string(kv)))
	}
	for rgIdx := 0; rgIdx*rowGroupSize < len(rows); rgIdx++ {
		start := rgIdx * rowGroupSize
		end := start + rowGroupSize
		if end > len(rows) {
			end = len(rows)
		}
		bodies := make([]string, 0, end-start)
		for i := start; i < end; i++ {
			if rows[i].Body != "" {
				bodies = append(bodies, rows[i].Body)
			}
		}
		if len(bodies) > 0 {
			key, value := buildTokenBloomMetadata(bodies, rgIdx)
			opts = append(opts, parquet.KeyValueMetadata(key, string(value)))
		}
	}

	writer := parquet.NewGenericWriter[schema.LogRow](&buf, opts...)
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

	// Pre-compute token bloom metadata for each row group so it can be
	// embedded as file-level key-value metadata in the Parquet footer.
	opts := []parquet.WriterOption{
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
		parquet.BloomFilters(bloomFilters(schema.TraceBloomColumns(activeSlotResolver.BloomSlots()...))...),
	}
	if kv := schema.MarshalSlotMapping(activeSlotResolver.Mapping()); kv != nil {
		opts = append(opts, parquet.KeyValueMetadata(schema.DedicatedSlotsMetaKey, string(kv)))
	}
	for rgIdx := 0; rgIdx*rowGroupSize < len(rows); rgIdx++ {
		start := rgIdx * rowGroupSize
		end := start + rowGroupSize
		if end > len(rows) {
			end = len(rows)
		}
		bodies := make([]string, 0, end-start)
		for i := start; i < end; i++ {
			if rows[i].SpanName != "" {
				bodies = append(bodies, rows[i].SpanName)
			}
		}
		if len(bodies) > 0 {
			key, value := buildTokenBloomMetadata(bodies, rgIdx)
			opts = append(opts, parquet.KeyValueMetadata(key, string(value)))
		}
	}

	tidxStart := time.Now()
	tidxEntries := computeTraceIndex(rows)
	if idxData := marshalTraceIndex(tidxEntries); len(idxData) > 0 {
		opts = append(opts, parquet.KeyValueMetadata(traceIndexMetadataKey, string(idxData)))
		metrics.WriterTraceIdxBuildsTotal.Inc()
		metrics.WriterTraceIdxEntriesTotal.Add(len(tidxEntries))
		metrics.WriterTraceIdxBuildDuration.Observe(time.Since(tidxStart).Seconds())
	}

	writer := parquet.NewGenericWriter[schema.TraceRow](&buf, opts...)
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

const (
	// Fixed scalar sizes per row — see internal/storage/parquets3/writer.go
	// for the rationale.
	fixedLogRowBytes   = 20
	fixedTraceRowBytes = 40
)

func estimateRawBytesLogs(rows []schema.LogRow) int64 {
	var total int64
	for i := range rows {
		r := &rows[i]
		total += fixedLogRowBytes
		total += int64(len(r.Body))
		total += int64(len(r.SeverityText))
		total += int64(len(r.ServiceName))
		total += int64(len(r.TraceID))
		total += int64(len(r.SpanID))
		total += int64(len(r.K8sNamespaceName))
		total += int64(len(r.K8sPodName))
		total += int64(len(r.K8sDeploymentName))
		total += int64(len(r.K8sNodeName))
		total += int64(len(r.DeployEnv))
		total += int64(len(r.CloudRegion))
		total += int64(len(r.HostName))
		total += int64(len(r.Stream))
		total += int64(len(r.StreamID))
		total += int64(len(r.ScopeName))
		for k, v := range r.ResourceAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range r.LogAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range r.ScopeAttributes {
			total += int64(len(k) + len(v))
		}
	}
	return total
}

func estimateRawBytesTraces(rows []schema.TraceRow) int64 {
	var total int64
	for i := range rows {
		r := &rows[i]
		total += fixedTraceRowBytes
		total += int64(len(r.TraceID))
		total += int64(len(r.SpanID))
		total += int64(len(r.ParentSpanID))
		total += int64(len(r.SpanName))
		total += int64(len(r.ServiceName))
		total += int64(len(r.StatusMessage))
		total += int64(len(r.HTTPMethod))
		total += int64(len(r.HTTPStatusCode))
		total += int64(len(r.HTTPUrl))
		total += int64(len(r.DBSystem))
		total += int64(len(r.DBStatement))
		total += int64(len(r.K8sNamespaceName))
		total += int64(len(r.K8sPodName))
		total += int64(len(r.K8sDeploymentName))
		total += int64(len(r.K8sNodeName))
		total += int64(len(r.DeployEnv))
		total += int64(len(r.CloudRegion))
		total += int64(len(r.HostName))
		total += int64(len(r.Stream))
		total += int64(len(r.StreamID))
		total += int64(len(r.ScopeName))
		for k, v := range r.ResourceAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range r.SpanAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range r.ScopeAttributes {
			total += int64(len(k) + len(v))
		}
	}
	return total
}

func schemaFingerprint(mode config.Mode) string {
	h := sha256.New()
	h.Write([]byte(string(mode)))
	b := make([]byte, 8)
	// Schema version. Bumped 1→2 for the dedicated-columns layout (Tier-1 OTel
	// columns + Tier-2 spare slots): old-schema files (v1, attributes in the
	// maps) and new-schema files (v2, promoted columns) get distinct
	// fingerprints so the compactor fences them apart instead of merging
	// incompatible column layouts. Queries still read both (dual-read); only
	// compaction grouping is fenced. Old files migrate forward as they age.
	binary.LittleEndian.PutUint64(b, 2)
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

// CanWriteData checks if the S3 backend is reachable.
func (w *BatchWriter) CanWriteData(ctx context.Context) error {
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

// bloomFilters builds SplitBlockFilter columns (10 bits/value ≈ 1% FPP) from the
// strict per-signal bloom set in internal/schema (cardinality-aligned: high-card
// equality-queried columns only).
var activeSlotResolver *schema.SlotResolver

// SetSlotResolver installs the Tier-2 slot resolver for the traces writer.
func SetSlotResolver(r *schema.SlotResolver) { activeSlotResolver = r }

func bloomFilters(cols []string) []parquet.BloomFilterColumn {
	bf := make([]parquet.BloomFilterColumn, 0, len(cols))
	for _, c := range cols {
		bf = append(bf, parquet.SplitBlockFilter(10, c))
	}
	return bf
}
