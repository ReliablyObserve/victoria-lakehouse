package parquets3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"net/http"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/s3reader"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// StatsCallback is called after each successful file flush with the
// compressed size, raw size, row count, and storage class. The flush
// invokes the callback once per distinct tenant in the flushed batch,
// with bytes attributed in proportion to that tenant's row share, so
// the registry can track per-tenant ingest from mixed-tenant batches.
type StatsCallback func(accountID, projectID uint32, compressedBytes, rawBytes, rows int64, storageClass string)

// FlushCacheCallback is called after a successful S3 upload to cache the
// flushed file data locally (write-through cache). The callback receives the
// S3 key and the raw Parquet bytes.
type FlushCacheCallback func(fileKey string, data []byte)

// TenantPrefixFunc returns the S3 key prefix where a row with the given
// (AccountID, ProjectID) tenant identity should land. The returned
// prefix must end in "/". When nil, the writer's default prefix is used
// for every row (single-tenant deployment).
//
// Per docs/multi-tenancy.md "boundary principle", the resolved prefix
// MUST be integer-keyed (e.g. "{AccountID}/{ProjectID}/<mode>/"); the
// string OrgID is a presentation concern surfaced only at API/UI.
type TenantPrefixFunc func(accountID, projectID uint32) string

// TenantBucketFunc returns the S3 bucket where a tenant's files
// should land. Empty string means "use the writer's default bucket"
// (the common case: prefix isolation, single bucket). Non-empty
// strings trigger bucket-isolation mode, where the writer routes
// writes via the pool registry and stamps FileInfo.Bucket so reads
// land on the same bucket later.
type TenantBucketFunc func(accountID, projectID uint32) string

// TenantPoolFunc returns the s3reader.ClientPool that owns the
// given bucket. Wired together with TenantBucketFunc so the writer
// can look up the right pool per tenant flush. nil means
// single-bucket mode (use the writer's default pool unconditionally).
//
// Declared as a pluggable function rather than an interface on the
// pool registry so this package stays leaf-level — main.go composes
// the two.
type TenantPoolFunc func(bucket string) PoolWriter

// PoolWriter is the subset of s3reader.ClientPool the BatchWriter
// needs. Keeps the dependency narrow and lets tests substitute fakes.
type PoolWriter interface {
	Upload(ctx context.Context, key string, data []byte) error
}

// BatchWriter buffers incoming rows per partition and flushes them as
// Parquet files to S3 on a configurable interval or size threshold.
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

	// inflight holds each FlushAll snapshot while it is written, so its rows
	// stay visible to buffer queries until they are committed to the
	// manifest or put back into the buffers after a failed upload. Guarded
	// by mu.
	inflight    map[uint64]*inflightFlush
	nextFlushID uint64
	// pendingBytes is the estimated raw size of every row not yet committed
	// to object storage: buffered, in flight, or put back after a failure.
	pendingBytes atomic.Int64

	probeMu  sync.Mutex
	probeAt  time.Time
	probeErr error

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
		inflight:  make(map[uint64]*inflightFlush),
		stopCh:    make(chan struct{}),
	}

	return bw
}

func (w *BatchWriter) SetStatsCallback(cb StatsCallback) {
	w.statsCallback = cb
}

// SetFlushCacheCallback sets the write-through cache callback invoked after
// each successful S3 upload. Used by combined nodes (role=all) to cache
// column data locally for immediate query availability.
func (w *BatchWriter) SetFlushCacheCallback(cb FlushCacheCallback) {
	w.flushCacheCb = cb
}

// SetTenantPrefix installs a per-tenant prefix resolver. When set, the
// flush path groups rows by (AccountID, ProjectID) and writes each
// tenant's slice of a partition to its own Parquet file under the
// resolved prefix. When nil, every row lands at the writer's default
// prefix as before — preserving single-tenant deployments.
func (w *BatchWriter) SetTenantPrefix(f TenantPrefixFunc) {
	w.tenantPrefix = f
}

// SetTenantBucket installs a per-tenant bucket resolver. Together
// with SetTenantPool, this routes per-tenant flushes to per-tenant
// S3 buckets. Empty return = use the writer's default bucket so a
// single resolver can mix prefix-only and bucket-isolated tenants.
func (w *BatchWriter) SetTenantBucket(f TenantBucketFunc) {
	w.tenantBucket = f
}

// SetTenantPool installs a bucket-to-pool resolver. Required when
// SetTenantBucket returns non-default buckets — the writer needs
// a pool that talks to that bucket to upload the Parquet file.
func (w *BatchWriter) SetTenantPool(f TenantPoolFunc) {
	w.tenantPool = f
}

// bucketForTenant returns the (bucket, pool) pair to use for the
// tenant's flush. Falls back to (w.pool.Bucket(), w.pool) when no
// per-tenant bucket override is configured for this tenant.
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

// prefixForTenant returns the configured tenant prefix, falling back to
// the writer's default prefix when no resolver is installed.
func (w *BatchWriter) prefixForTenant(accountID, projectID uint32) string {
	if w.tenantPrefix == nil {
		return w.prefix
	}
	if p := w.tenantPrefix(accountID, projectID); p != "" {
		return p
	}
	return w.prefix
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
		// The rows the final flush could not write are back in the buffers,
		// which die with the process: nothing else holds them.
		lost := w.totalRows.Load()
		metrics.InsertRowsLostAtShutdown.Add(int(lost))
		logger.Errorf("final flush failed: %s; %d buffered rows could not be written and are lost", err, lost)
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

	w.pendingBytes.Add(estimateRawBytesLogs(rows))
	metrics.InsertBytesBuffered.Set(w.pendingBytes.Load())
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

	w.pendingBytes.Add(estimateRawBytesTraces(rows))
	metrics.InsertBytesBuffered.Set(w.pendingBytes.Load())
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
//
// A tenant group whose upload fails — or that the flush does not reach before
// ctx ends — is put back into the buffers and written by the next flush; a
// group that was written is never retried. Until then the snapshot stays
// visible to buffer queries (BufferedLogRows / BufferedTraceRows), so a row is
// never missing from reads while it is on its way to object storage.
func (w *BatchWriter) FlushAll(ctx context.Context) error {
	flushStart := time.Now()

	w.mu.Lock()
	logSnap := w.logBufs
	traceSnap := w.traceBufs
	w.logBufs = make(map[string][]schema.LogRow)
	w.traceBufs = make(map[string][]schema.TraceRow)
	w.totalRows.Store(0)
	id := w.nextFlushID
	w.nextFlushID++
	if len(logSnap) > 0 || len(traceSnap) > 0 {
		if w.inflight == nil {
			w.inflight = make(map[uint64]*inflightFlush)
		}
		w.inflight[id] = &inflightFlush{logs: logSnap, traces: traceSnap}
	}
	w.mu.Unlock()

	metrics.InsertRowsBuffered.Set(0)
	metrics.InsertPartitionsActive.Set(int64(len(logSnap) + len(traceSnap)))

	var errs []error
	failedLogs := make(map[string][]schema.LogRow)
	failedTraces := make(map[string][]schema.TraceRow)
	var committedBytes int64

	for partition, rows := range logSnap {
		failed, err := w.flushLogPartition(ctx, partition, rows)
		if err != nil {
			metrics.InsertFlushErrorsTotal.Inc()
			errs = append(errs, fmt.Errorf("flush logs %s: %w", partition, err))
		}
		if len(failed) > 0 {
			failedLogs[partition] = failed
		}
		committedBytes += estimateRawBytesLogs(rows) - estimateRawBytesLogs(failed)
	}

	for partition, rows := range traceSnap {
		failed, err := w.flushTracePartition(ctx, partition, rows)
		if err != nil {
			metrics.InsertFlushErrorsTotal.Inc()
			errs = append(errs, fmt.Errorf("flush traces %s: %w", partition, err))
		}
		if len(failed) > 0 {
			failedTraces[partition] = failed
		}
		committedBytes += estimateRawBytesTraces(rows) - estimateRawBytesTraces(failed)
	}

	w.finishFlush(id, failedLogs, failedTraces, committedBytes)

	if len(logSnap) > 0 || len(traceSnap) > 0 {
		metrics.InsertFlushTotal.Inc()
		metrics.InsertFlushDuration.Observe(time.Since(flushStart).Seconds())
	}
	// OUTSIDE the non-empty gate: bundles left dirty by a FAILED PUT on a prior
	// cycle must retry even when this cycle flushed nothing (and the final
	// shutdown flush is often empty — gating here lost the last bundle state).
	if w.catalogObserver != nil {
		w.catalogObserver.persistDirty(ctx)
	}

	if len(errs) > 0 {
		return fmt.Errorf("flush errors: %v", errs)
	}
	return nil
}

// flushLogPartition writes one partition, one object per tenant, and
// returns the rows it could not write (their tenant group failed, or ctx ended
// before it was reached) with the first error. Rows it did write are committed
// and never returned.
func (w *BatchWriter) flushLogPartition(ctx context.Context, partition string, rows []schema.LogRow) ([]schema.LogRow, error) {
	// The rows may be visible to buffer queries while in flight (FlushAll),
	// so they are reordered under the same lock those queries read under.
	w.mu.Lock()
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano
	})
	w.mu.Unlock()

	// Group rows by tenant so each tenant's slice lands at its own
	// S3 prefix. Fast path: a single-tenant batch (the common case in
	// single-tenant deployments) skips the grouping allocation.
	groups := groupLogRowsByTenant(rows)
	var failed []schema.LogRow
	var firstErr error
	for _, g := range groups {
		err := ctx.Err()
		if err == nil {
			err = w.flushLogTenantGroup(ctx, partition, g.AccountID, g.ProjectID, g.Rows)
		}
		if err != nil {
			failed = append(failed, g.Rows...)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return failed, firstErr
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

	// Wall-clock instrumentation of the two writer-artifact builds
	// that gate query-side speedups (inverted label index + file
	// bloom). Logged as metrics rather than per-line logs because
	// each flush is a hot path.
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
	// double-count guard. See schema.LogRowTimeBounds.
	minTimeNs, maxTimeNs := schema.LogRowTimeBounds(rows)
	fi := manifest.FileInfo{
		Key:               key,
		Bucket:            bucket,
		Size:              int64(len(result.Data)),
		RowCount:          int64(len(rows)),
		MinTimeNs:         minTimeNs,
		MaxTimeNs:         maxTimeNs,
		RawBytes:          result.RawBytes,
		BloomBytes:        footerBloomBytes(result.Data),
		SchemaFingerprint: schemaFingerprint(w.mode),
		Labels:            labels,
		LabelAggregates:   schema.ExtractLogLabelAggregates(rows),
		ColumnBytes:       result.ColumnBytes,
	}
	w.manifest.AddFile(partition, fi)

	// Per-column bloom values for the pmeta bloom facet (uncapped — a capped
	// feed false-negatives). One extraction, no legacy dual-write anymore.
	var bloomValues map[string][]string
	if w.catalogObserver != nil {
		bloomValues = extractLogBloomValues(rows)
	}

	// pmeta field/value catalog + file-meta + bloom facets: fed the same
	// already-extracted maps (no extra scan). Nil unless --pmeta is enabled, so
	// this is a no-op on the hot path by default.
	if w.catalogObserver != nil {
		tp := manifest.ExtractTenantPartition(fi.Key)
		w.catalogObserver.OnFileFlush(tp, fi, labels, bloomValues)
		w.catalogObserver.tapLogRows(tp, rows)
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

// flushTracePartition writes one partition, one object per tenant, and
// returns the rows it could not write (their tenant group failed, or ctx ended
// before it was reached) with the first error. Rows it did write are committed
// and never returned.
func (w *BatchWriter) flushTracePartition(ctx context.Context, partition string, rows []schema.TraceRow) ([]schema.TraceRow, error) {
	// The rows may be visible to buffer queries while in flight (FlushAll),
	// so they are reordered under the same lock those queries read under.
	w.mu.Lock()
	sort.Slice(rows, func(i, j int) bool {
		return rows[i].TimestampUnixNano < rows[j].TimestampUnixNano
	})
	w.mu.Unlock()

	groups := groupTraceRowsByTenant(rows)
	var failed []schema.TraceRow
	var firstErr error
	for _, g := range groups {
		err := ctx.Err()
		if err == nil {
			err = w.flushTraceTenantGroup(ctx, partition, g.AccountID, g.ProjectID, g.Rows)
		}
		if err != nil {
			failed = append(failed, g.Rows...)
			if firstErr == nil {
				firstErr = err
			}
		}
	}
	return failed, firstErr
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

	labelStart := time.Now()
	labels := extractTraceLabels(rows)
	metrics.WriterLabelExtractionsTotal.Inc("traces")
	metrics.WriterLabelExtractionDuration.Observe(time.Since(labelStart).Seconds())
	var labelValueCount int
	for _, vals := range labels {
		labelValueCount += len(vals)
	}
	metrics.WriterLabelValuesTotal.Add("traces", labelValueCount)

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
		BloomBytes:        footerBloomBytes(result.Data),
		SchemaFingerprint: schemaFingerprint(w.mode),
		Labels:            labels,
		LabelAggregates:   schema.ExtractTraceLabelAggregates(rows),
		ColumnBytes:       result.ColumnBytes,
	}
	w.manifest.AddFile(partition, fi)

	var bloomValues map[string][]string
	if w.catalogObserver != nil {
		bloomValues = extractTraceBloomValues(rows)
	}

	// pmeta field/value catalog + file-meta + bloom facets: fed the same
	// already-extracted maps (no extra scan). Nil unless --pmeta is enabled, so
	// this is a no-op on the hot path by default.
	if w.catalogObserver != nil {
		tp := manifest.ExtractTenantPartition(fi.Key)
		w.catalogObserver.OnFileFlush(tp, fi, labels, bloomValues)
		w.catalogObserver.tapTraceRows(tp, rows)
	}

	if w.statsCallback != nil {
		w.statsCallback(accountID, projectID, int64(len(result.Data)), result.RawBytes, int64(len(rows)), "STANDARD")
	}

	if w.flushCacheCb != nil {
		w.flushCacheCb(key, result.Data)
	}

	w.totalBytes.Add(int64(len(result.Data)))

	logger.Infof("flushed trace partition; partition=%s, tenant=%d:%d, rows=%d, bytes=%d, ratio=%v, key=%s",
		partition, accountID, projectID, len(rows), len(result.Data), fi.CompressionRatio(), key)

	return nil
}

type flushResult struct {
	Data     []byte
	RawBytes int64
	// ColumnBytes is per-column compressed bytes from the file footer (column
	// name -> bytes, summed across row groups). Aggregated over the manifest's
	// files it yields the per-field on-S3 storage footprint.
	ColumnBytes map[string]int64
}

// columnBytesFromFooter reads the just-written Parquet footer and returns the
// total compressed bytes each top-level column occupies (summed across row
// groups). Cheap: OpenFile parses only the footer already in memory; no column
// data is read. Returns nil on any error so the flush degrades to "no per-column
// sizes" rather than failing.
func columnBytesFromFooter(data []byte) map[string]int64 {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	md := f.Metadata()
	if md == nil || len(md.RowGroups) == 0 {
		return nil
	}
	out := make(map[string]int64)
	for i := range md.RowGroups {
		for j := range md.RowGroups[i].Columns {
			cm := md.RowGroups[i].Columns[j].MetaData
			if len(cm.PathInSchema) == 0 {
				continue
			}
			out[cm.PathInSchema[0]] += cm.TotalCompressedSize
		}
	}
	if len(out) == 0 {
		return nil
	}
	return out
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
		// Tier-1 strict blooms + operator Tier-2 slot blooms (nil-safe).
		parquet.BloomFilters(bloomFilters(schema.LogBloomColumns(activeSlotResolver.BloomSlots()...))...),
	}
	// Tier-2: record the slot→name binding in the footer KV so the file is
	// self-describing — read-back remaps ded_sNN to the configured attribute
	// name by ITS OWN footer, correct even if the config later changes.
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
	logData := buf.Bytes()
	return &flushResult{
		Data:        logData,
		RawBytes:    estimateRawBytesLogs(rows),
		ColumnBytes: columnBytesFromFooter(logData),
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
	traceData := buf.Bytes()
	return &flushResult{
		Data:        traceData,
		RawBytes:    estimateRawBytesTraces(rows),
		ColumnBytes: columnBytesFromFooter(traceData),
	}, nil
}

// estimateRawBytesLogs / estimateRawBytesTraces delegate to the shared measure
// in internal/schema. The flush writer, the compactor and the delete rewriter
// all record RawBytes into the same manifest field, so they must compute it
// identically — see schema.EstimateRawBytesLogs.
func estimateRawBytesLogs(rows []schema.LogRow) int64 {
	return schema.EstimateRawBytesLogs(rows)
}

func estimateRawBytesTraces(rows []schema.TraceRow) int64 {
	return schema.EstimateRawBytesTraces(rows)
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

// CurrentSchemaFingerprint is the fingerprint files are written with in the given
// mode — exported so the stats / compaction-detection layer can flag stale
// (older-schema) files that still need a re-promotion pass.
func CurrentSchemaFingerprint(mode config.Mode) string { return schemaFingerprint(mode) }

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

// inflightFlush is one FlushAll snapshot while it is being written.
type inflightFlush struct {
	logs   map[string][]schema.LogRow
	traces map[string][]schema.TraceRow
}

// finishFlush retires flush id: the rows it could not write go back into the
// buffers (ahead of rows that arrived meanwhile; a flush sorts them anyway),
// in the same critical section that stops showing them as in flight, so a
// buffer query sees every row exactly once throughout.
func (w *BatchWriter) finishFlush(id uint64, failedLogs map[string][]schema.LogRow, failedTraces map[string][]schema.TraceRow, committedBytes int64) {
	var requeued int
	w.mu.Lock()
	delete(w.inflight, id)
	for p, rows := range failedLogs {
		w.logBufs[p] = append(rows, w.logBufs[p]...)
		requeued += len(rows)
	}
	for p, rows := range failedTraces {
		w.traceBufs[p] = append(rows, w.traceBufs[p]...)
		requeued += len(rows)
	}
	w.mu.Unlock()

	w.pendingBytes.Add(-committedBytes)
	metrics.InsertBytesBuffered.Set(w.pendingBytes.Load())
	if requeued > 0 {
		w.totalRows.Add(int64(requeued))
		metrics.InsertRowsRequeued.Add(requeued)
		logger.Warnf("flush could not write %d rows; they stay buffered and are retried by the next flush", requeued)
	}
	metrics.InsertRowsBuffered.Set(w.totalRows.Load())
}

// writeProbeTTL is how long a write probe's outcome answers CanWriteData.
// Insert handlers ask on every request; probing object storage each time
// was a PUT per insert request.
const writeProbeTTL = 10 * time.Second

// CanWriteData reports whether rows can be accepted now. It answers the way
// VictoriaLogs' own storage does when it cannot take writes — an error the
// insert handlers turn into an HTTP status, so clients back off and retry:
//   - 429 while the rows not yet written to object storage exceed
//     insert.max_buffer_bytes (flushes are failing or cannot keep up), instead
//     of growing the buffer until the process runs out of memory;
//   - 503 while object storage refuses a small write (the probe result is
//     reused for writeProbeTTL).
func (w *BatchWriter) CanWriteData(ctx context.Context) error {
	if limit, pending := w.cfg.MaxBufferBytesN(), w.pendingBytes.Load(); pending > limit {
		metrics.InsertRejected.Inc("buffer_full")
		return &httpserver.ErrorWithStatusCode{
			Err: fmt.Errorf("the insert buffer holds %d bytes not yet written to object storage, over insert.max_buffer_bytes=%d; retry later",
				pending, limit),
			StatusCode: http.StatusTooManyRequests,
		}
	}

	w.probeMu.Lock()
	defer w.probeMu.Unlock()
	if !w.probeAt.IsZero() && time.Since(w.probeAt) < writeProbeTTL {
		if w.probeErr != nil {
			metrics.InsertRejected.Inc("storage_unavailable")
		}
		return w.probeErr
	}
	testKey := w.prefix + "_write_check"
	w.probeErr = nil
	if err := w.pool.Upload(ctx, testKey, []byte("ok")); err != nil {
		metrics.InsertRejected.Inc("storage_unavailable")
		w.probeErr = &httpserver.ErrorWithStatusCode{
			Err:        fmt.Errorf("object storage refuses writes: %w", err),
			StatusCode: http.StatusServiceUnavailable,
		}
	}
	w.probeAt = time.Now()
	return w.probeErr
}

// BufferedLogRows returns unflushed log rows matching a time range (for buffer query protocol).
func (w *BatchWriter) BufferedLogRows(startNs, endNs int64) []schema.LogRow {
	w.mu.Lock()
	defer w.mu.Unlock()

	// [startNs, endNs] inclusive, as the query window it serves. Rows of a
	// flush in progress are included until they are committed.
	var result []schema.LogRow
	collect := func(bufs map[string][]schema.LogRow) {
		for _, rows := range bufs {
			for _, r := range rows {
				if r.TimestampUnixNano >= startNs && r.TimestampUnixNano <= endNs {
					result = append(result, r)
				}
			}
		}
	}
	collect(w.logBufs)
	for _, f := range w.inflight {
		collect(f.logs)
	}
	return result
}

// BufferedTraceRows returns unflushed trace rows matching a time range.
func (w *BatchWriter) BufferedTraceRows(startNs, endNs int64) []schema.TraceRow {
	w.mu.Lock()
	defer w.mu.Unlock()

	// [startNs, endNs] inclusive, as the query window it serves. Rows of a
	// flush in progress are included until they are committed.
	var result []schema.TraceRow
	collect := func(bufs map[string][]schema.TraceRow) {
		for _, rows := range bufs {
			for _, r := range rows {
				if r.TimestampUnixNano >= startNs && r.TimestampUnixNano <= endNs {
					result = append(result, r)
				}
			}
		}
	}
	collect(w.traceBufs)
	for _, f := range w.inflight {
		collect(f.traces)
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
// activeSlotResolver holds the process-wide Tier-2 slot binding (set at startup
// from config). nil = no custom promotions; all SlotResolver methods are nil-safe.
var activeSlotResolver *schema.SlotResolver

// SetSlotResolver installs the Tier-2 slot resolver for the writer (slot blooms
// + footer-KV mapping) and the buffer read-remap.
func SetSlotResolver(r *schema.SlotResolver) { activeSlotResolver = r }

func bloomFilters(cols []string) []parquet.BloomFilterColumn {
	bf := make([]parquet.BloomFilterColumn, 0, len(cols))
	for _, c := range cols {
		bf = append(bf, parquet.SplitBlockFilter(10, c))
	}
	return bf
}
