package parquets3

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/binary"
	"fmt"
	"strings"
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

// BatchWriter writes groups of rows as Parquet objects to S3 and registers each
// with the manifest, the stats and the cache callbacks. It holds no rows of its
// own: the buffer flusher drains the insert buffer through it.
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
	markerPool PoolWriter // segment markers; nil = pool
	cfg        *config.InsertConfig
	pool       *s3reader.ClientPool
	manifest   *manifest.Manifest
	prefix     string
	mode       config.Mode

	totalBytes atomic.Int64

	catalogObserver *catalogObserver
	statsCallback   StatsCallback
	flushCacheCb    FlushCacheCallback
	tenantPrefix    TenantPrefixFunc
	tenantBucket    TenantBucketFunc
	tenantPool      TenantPoolFunc
}

func NewBatchWriter(cfg *config.InsertConfig, pool *s3reader.ClientPool,
	m *manifest.Manifest, prefix string, mode config.Mode) *BatchWriter {

	return &BatchWriter{
		cfg:      cfg,
		pool:     pool,
		manifest: m,
		prefix:   prefix,
		mode:     mode,
	}
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

// logGroupUpload is one tenant's rows of one partition on their way to object
// storage. The object key and bytes are fixed at the first attempt, so a retry
// overwrites the same object: an upload that failed on the client side but
// reached the store (a timeout) is not written a second time under a new
// name, and the manifest keeps one entry per key.
type logGroupUpload struct {
	partition            string
	accountID, projectID uint32
	rows                 []schema.LogRow
	// batchID names the object; empty draws a random one. The buffer flusher
	// derives it from its window so a restarted window names the same objects.
	batchID string
	key     string
	result  *flushResult
	// onStored, when set, is told once the object is stored, or found already
	// settled (live or retired), before the manifest commit. It cannot fail the
	// group: a stored object is always committed at once. The buffer flusher
	// uses it for a best-effort "stored" mark.
	onStored func(key string)
}

// assignLogKey fixes the group's object key (once) and returns it.
func (w *BatchWriter) assignLogKey(up *logGroupUpload) string {
	if up.key == "" {
		if up.batchID == "" {
			up.batchID = randomBatchID()
		}
		up.key = fmt.Sprintf("%s%s/%s.parquet", w.prefixForTenant(up.accountID, up.projectID), up.partition, up.batchID)
	}
	return up.key
}

func (w *BatchWriter) uploadLogGroup(ctx context.Context, up *logGroupUpload) error {
	key := w.assignLogKey(up)
	// An object is never written twice with different content. If the manifest
	// already has this key, an earlier upload of it was stored and adopted by a
	// listing: its rows are live. If the key is retired, the object was
	// compacted, rewritten or removed and its rows live elsewhere. Either way
	// the group is settled; uploading it again would duplicate its rows.
	if w.manifest.HasKey(key) {
		w.settled(up.onStored, key)
		logger.Infof("skipped %s: an earlier upload of it is already live in the manifest; rows=%d", key, len(up.rows))
		return nil
	}
	if w.manifest.IsRetired(key) {
		w.settled(up.onStored, key)
		w.noteSuperseded(key, len(up.rows))
		return nil
	}
	if up.result == nil {
		result, err := writeLogsParquet(up.rows, w.cfg.RowGroupSize, w.cfg.CompressionLevel)
		if err != nil {
			return fmt.Errorf("write parquet: %w", err)
		}
		up.result = result
	}
	partition, accountID, projectID, rows, result := up.partition, up.accountID, up.projectID, up.rows, up.result

	bucket, uploader := w.bucketForTenant(accountID, projectID)

	metrics.S3RequestsTotal.Inc("PUT")
	if err := uploader.Upload(ctx, key, result.Data); err != nil {
		metrics.S3ErrorsTotal.Inc("PUT")
		return err
	}
	metrics.InsertBytesUploaded.Add(len(result.Data))
	if up.onStored != nil {
		up.onStored(key)
	}

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
		BloomBytes:        footerBloomBytes(result.Data),
		ColumnBytes:       result.ColumnBytes,
		SchemaFingerprint: schemaFingerprint(w.mode),
		Labels:            labels,
		LabelAggregates:   schema.ExtractLogLabelAggregates(rows),
	}
	// Atomic with the retired check: a key retired while the PUT was in flight
	// (compaction merged it, a delete rewrite replaced it) must not come back.
	// The PUT may have recreated an already deleted object: it is owed a delete
	// again, and its rows live in whatever superseded it.
	if !w.manifest.AddFileUnlessRetired(partition, fi) {
		w.manifest.Retire(key, "", true)
		w.noteSuperseded(key, len(rows))
		return nil
	}
	if w.catalogObserver != nil {
		// UNCAPPED bloom feed (trace_id + service.name): the capped label map
		// false-negatives on values past maxLabelsPerField — a bloom must see
		// every value present or it wrongly excludes files.
		tp := manifest.ExtractTenantPartition(fi.Key)
		w.catalogObserver.OnFileFlush(tp, fi, labels, extractLogBloomValues(rows))
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

// traceGroupUpload is one tenant's rows of one partition on their way to object
// storage. The object key and bytes are fixed at the first attempt, so a retry
// overwrites the same object: an upload that failed on the client side but
// reached the store (a timeout) is not written a second time under a new
// name, and the manifest keeps one entry per key.
type traceGroupUpload struct {
	partition            string
	accountID, projectID uint32
	rows                 []schema.TraceRow
	// batchID names the object; empty draws a random one. The buffer flusher
	// derives it from its window so a restarted window names the same objects.
	batchID string
	key     string
	result  *flushResult
	// onStored, when set, is told once the object is stored, or found already
	// settled (live or retired), before the manifest commit. It cannot fail the
	// group: a stored object is always committed at once. The buffer flusher
	// uses it for a best-effort "stored" mark.
	onStored func(key string)
}

// assignTraceKey fixes the group's object key (once) and returns it.
func (w *BatchWriter) assignTraceKey(up *traceGroupUpload) string {
	if up.key == "" {
		if up.batchID == "" {
			up.batchID = randomBatchID()
		}
		up.key = fmt.Sprintf("%s%s/%s.parquet", w.prefixForTenant(up.accountID, up.projectID), up.partition, up.batchID)
	}
	return up.key
}

func (w *BatchWriter) uploadTraceGroup(ctx context.Context, up *traceGroupUpload) error {
	key := w.assignTraceKey(up)
	// An object is never written twice with different content. If the manifest
	// already has this key, an earlier upload of it was stored and adopted by a
	// listing: its rows are live. If the key is retired, the object was
	// compacted, rewritten or removed and its rows live elsewhere. Either way
	// the group is settled; uploading it again would duplicate its rows.
	if w.manifest.HasKey(key) {
		w.settled(up.onStored, key)
		logger.Infof("skipped %s: an earlier upload of it is already live in the manifest; rows=%d", key, len(up.rows))
		return nil
	}
	if w.manifest.IsRetired(key) {
		w.settled(up.onStored, key)
		w.noteSuperseded(key, len(up.rows))
		return nil
	}
	if up.result == nil {
		result, err := writeTracesParquet(up.rows, w.cfg.RowGroupSize, w.cfg.CompressionLevel)
		if err != nil {
			return fmt.Errorf("write parquet: %w", err)
		}
		up.result = result
	}
	partition, accountID, projectID, rows, result := up.partition, up.accountID, up.projectID, up.rows, up.result

	bucket, uploader := w.bucketForTenant(accountID, projectID)

	metrics.S3RequestsTotal.Inc("PUT")
	if err := uploader.Upload(ctx, key, result.Data); err != nil {
		metrics.S3ErrorsTotal.Inc("PUT")
		return err
	}
	metrics.InsertBytesUploaded.Add(len(result.Data))
	if up.onStored != nil {
		up.onStored(key)
	}

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
		BloomBytes:        footerBloomBytes(result.Data),
		ColumnBytes:       result.ColumnBytes,
		SchemaFingerprint: schemaFingerprint(w.mode),
		Labels:            labels2,
		LabelAggregates:   schema.ExtractTraceLabelAggregates(rows),
	}
	// Atomic with the retired check: a key retired while the PUT was in flight
	// (compaction merged it, a delete rewrite replaced it) must not come back.
	// The PUT may have recreated an already deleted object: it is owed a delete
	// again, and its rows live in whatever superseded it.
	if !w.manifest.AddFileUnlessRetired(partition, fi) {
		w.manifest.Retire(key, "", true)
		w.noteSuperseded(key, len(rows))
		return nil
	}
	traceBloomValues := extractTraceBloomValues(rows)
	if w.catalogObserver != nil {
		// UNCAPPED bloom feed — same rationale as the logs flush above.
		tp := manifest.ExtractTenantPartition(fi.Key)
		w.catalogObserver.OnFileFlush(tp, fi, labels2, traceBloomValues)
		w.catalogObserver.tapTraceRows(tp, rows)
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
		schema.ParquetCreatedBy(),
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
		schema.ParquetCreatedBy(),
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

// TotalBytesUploaded returns the total bytes uploaded to S3 since startup.
func (w *BatchWriter) TotalBytesUploaded() int64 {
	return w.totalBytes.Load()
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

// settled tells a group's onStored hook that its object needs no upload (it is
// already live or already superseded), so the caller's record of what is done
// includes it.
func (w *BatchWriter) settled(onStored func(key string), key string) {
	if onStored != nil {
		onStored(key)
	}
}

// persistCatalog writes the pmeta bundles changed since the last write. The
// buffer flusher calls it after every drain and on every tick, so a bundle
// left dirty by a failed PUT is retried even when nothing was flushed.
func (w *BatchWriter) persistCatalog(ctx context.Context) {
	if w != nil && w.catalogObserver != nil {
		w.catalogObserver.persistDirty(ctx)
	}
}

// SetMarkerPool overrides where segment markers are written (tests); by
// default they go to the writer's default bucket, where compaction lists them.
func (w *BatchWriter) SetMarkerPool(p PoolWriter) {
	w.markerPool = p
}

// putSegmentMarker writes the "segment committed" marker of a buffer segment
// (manifest.SegmentMarkerKey) to the default bucket. Its time is the object
// store's LastModified; the body is the same on every attempt.
func (w *BatchWriter) putSegmentMarker(ctx context.Context, nonce string, seq uint64) error {
	var p PoolWriter = w.pool
	if w.markerPool != nil {
		p = w.markerPool
	}
	body := fmt.Sprintf(`{"seq":%d}`, seq)
	metrics.S3RequestsTotal.Inc("PUT")
	if err := p.Upload(ctx, manifest.SegmentMarkerKey(w.prefix, nonce), []byte(body)); err != nil {
		metrics.S3ErrorsTotal.Inc("PUT")
		return err
	}
	return nil
}

// objectExists asks the object store, by HEAD, whether key exists in the bucket
// the tenant's objects live in. An error means "unknown", never "absent".
func (w *BatchWriter) objectExists(ctx context.Context, accountID, projectID uint32, key string) (bool, error) {
	_, p := w.bucketForTenant(accountID, projectID)
	c, ok := p.(objectChecker)
	if !ok {
		return false, fmt.Errorf("the object store for %s cannot check existence", key)
	}
	return c.Exists(ctx, key)
}

// objectChecker is the HEAD side of an object store; s3reader.ClientPool has it.
type objectChecker interface {
	Exists(ctx context.Context, key string) (bool, error)
}

// noteSuperseded counts rows a group skipped because its object had been
// retired (compacted, rewritten or removed): whatever superseded it carries
// them.
func (w *BatchWriter) noteSuperseded(key string, rows int) {
	metrics.InsertRowsSuperseded.Add(rows)
	logger.Warnf("flush skipped a retired object %s: it was compacted, rewritten or removed and its rows live elsewhere; rows=%d", key, rows)
}
