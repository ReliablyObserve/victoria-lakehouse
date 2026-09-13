package delete

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// RewriterPool abstracts S3 operations needed by the Rewriter.
type RewriterPool interface {
	Upload(ctx context.Context, key string, data []byte) error
	Download(ctx context.Context, key string) ([]byte, error)
	Delete(ctx context.Context, key string) error
}

// RewriteResult summarises a single file rewrite operation.
//
// Beyond the counters, it carries everything the manifest needs to register the
// rewritten object, computed from the KEPT rows rather than inherited from the
// superseded entry. Row counts and per-value aggregates that are not recomputed
// would keep reporting the deleted rows forever — `stats count() by (field)` is
// answered straight from LabelAggregates without opening a file, so a stale
// aggregate resurrects deleted rows in every dashboard that uses one.
type RewriteResult struct {
	OldKey      string
	NewKey      string
	RowsKept    int64
	RowsRemoved int64
	BytesBefore int64
	BytesAfter  int64

	// MinTimeNs / MaxTimeNs are the true bounds of the kept rows (a full scan,
	// not first/last row — the input is not guaranteed time-sorted, and an
	// understated MaxTimeNs breaks manifest range pruning).
	MinTimeNs int64
	MaxTimeNs int64
	// RawBytes is the uncompressed footprint of the kept rows.
	RawBytes int64
	// BloomBytes / ColumnBytes are read back from the written footer.
	BloomBytes  int64
	ColumnBytes map[string]int64
	// LabelAggregates is field -> value -> row count over the kept rows only.
	LabelAggregates map[string]map[string]int64
	// Labels is the per-field distinct value set over the kept rows — the set
	// the manifest's inverted index is built from and the pmeta field catalog
	// is fed with. It must be recomputed: a value carried only by the removed
	// rows would otherwise go back into the catalog and be served by
	// field_values after the rows are gone.
	Labels map[string][]string
	// BloomValues is column -> distinct values for the bloom columns over the
	// kept rows, handed to the pmeta bloom facet so the replacement stays
	// bloom-prunable (the same feed compaction provides for its outputs).
	BloomValues map[string][]string

	// Published is set by the scheduler once the manifest points at NewKey.
	// Until then the superseded object MUST NOT be deleted: an unpublished
	// rewrite whose source is already gone loses the kept rows outright.
	Published bool

	Duration time.Duration
}

// Rewriter reads Parquet files from S3, removes tombstoned rows, and writes
// the filtered result back.
type Rewriter struct {
	pool         RewriterPool
	prefix       string
	rowGroupSize int
	mode         string
	writers      ParquetWriters
}

// ParquetWriters produce the bytes of a rewritten file.
//
// A replacement must be as prunable as the file it replaces: the SBBF column
// blooms external readers (ClickHouse, DuckDB, Trino) use straight from S3, the
// Tier-2 slot blooms and binding, and for traces the `_trace_idx` footer index.
// The compactor already writes exactly that, so the embedder injects the
// compactor's writers (compaction.WriteLogs / WriteTraces) and a rewritten file
// becomes indistinguishable from a compaction output of the same rows. The
// delete package cannot import the compactor itself — the compactor imports
// this package for the tombstone store — which is why this is injected rather
// than called directly.
//
// Unset writers fall back to a minimal writer (row groups + the source file's
// slot binding, no blooms, no compression). That fallback exists for unit tests;
// production wiring always sets both, and the binaries' tests pin that.
type ParquetWriters struct {
	Logs             func(rows []schema.LogRow, rowGroupSize int, compressionLevel int) ([]byte, error)
	Traces           func(rows []schema.TraceRow, rowGroupSize int, compressionLevel int) ([]byte, error)
	CompressionLevel int
}

// RewriterOption configures a Rewriter.
type RewriterOption func(*Rewriter)

// WithParquetWriters injects the writers a replacement file is produced with.
func WithParquetWriters(w ParquetWriters) RewriterOption {
	return func(r *Rewriter) { r.writers = w }
}

// NewRewriter creates a Rewriter with the given pool, key prefix, row group size, and mode.
// Mode should be "logs" or "traces". If rowGroupSize <= 0 it defaults to 10000.
func NewRewriter(pool RewriterPool, prefix string, rowGroupSize int, mode string, opts ...RewriterOption) *Rewriter {
	if rowGroupSize <= 0 {
		rowGroupSize = 10000
	}
	if mode == "" {
		mode = "logs"
	}
	r := &Rewriter{
		pool:         pool,
		prefix:       prefix,
		rowGroupSize: rowGroupSize,
		mode:         mode,
	}
	for _, o := range opts {
		o(r)
	}
	return r
}

// HasProductionWriters reports whether both format-preserving writers are
// injected. The binaries assert it at construction so a missed wiring cannot
// silently ship replacements without blooms.
func (r *Rewriter) HasProductionWriters() bool {
	return r.writers.Logs != nil && r.writers.Traces != nil
}

// RewriteFile is the PREPARE half of a two-phase rewrite: it downloads the
// Parquet file at key, removes rows matching any of the provided tombstones and
// uploads the filtered result under a new key. It deliberately does NOT touch
// the superseded object — that is Commit's job, and it must not happen until
// the manifest points at the replacement.
//
// The ordering matters for every crash window:
//
//	prepare  — new object exists, unmanifested. A crash here leaves the old
//	           object intact and manifested; the new one is reclaimed by the
//	           orphan sweep and the tombstone is retried. No data moves.
//	publish  — manifest swaps old key for new, atomically (Manifest.ReplaceFile).
//	           A crash here leaves the old object unmanifested; the sweep
//	           reclaims it and the kept rows are served from the new object.
//	commit   — old object deleted. A crash here is identical to the above.
//
// The previous single-phase version deleted the old object inside this function
// while nothing ever updated the manifest, so the manifest pointed at a deleted
// key and the replacement was an unmanaged object the orphan sweep removed
// after its age gate — losing the rows the delete was supposed to KEEP.
//
// If no rows match, the original file is left untouched and RowsRemoved == 0.
func (r *Rewriter) RewriteFile(ctx context.Context, key string, tombstones []Tombstone) (*RewriteResult, error) {
	start := time.Now()

	data, err := r.pool.Download(ctx, key)
	if err != nil {
		return nil, fmt.Errorf("download %s: %w", key, err)
	}

	result := &RewriteResult{
		OldKey:      key,
		BytesBefore: int64(len(data)),
	}

	var newData []byte
	switch r.mode {
	case "traces":
		newData, err = r.filterTraceRows(data, tombstones, result)
	default:
		newData, err = r.filterLogRows(data, tombstones, result)
	}
	if err != nil {
		return nil, err
	}

	if result.RowsRemoved == 0 {
		result.Duration = time.Since(start)
		return result, nil
	}

	if result.RowsKept == 0 {
		// Every row went. There is no replacement object; publishing means
		// dropping the manifest entry, and Commit deletes the file.
		result.BytesAfter = 0
		result.Duration = time.Since(start)
		return result, nil
	}

	result.BytesAfter = int64(len(newData))
	result.BloomBytes = footerBloomBytes(newData)
	result.ColumnBytes = columnBytesFromFooter(newData)

	partition := extractPartition(key)
	short := uuid.New().String()[:8]
	newKey := fmt.Sprintf("%s%s/%s.parquet", r.prefix, partition, short)
	result.NewKey = newKey

	if err := r.pool.Upload(ctx, newKey, newData); err != nil {
		return nil, fmt.Errorf("upload %s: %w", newKey, err)
	}

	result.Duration = time.Since(start)
	return result, nil
}

// Commit is the CLEANUP half: it deletes the superseded object once the
// manifest has been updated to point at the replacement. It refuses to run on
// an unpublished result, which is the single guard standing between a failed
// manifest hand-off and permanent loss of the kept rows.
func (r *Rewriter) Commit(ctx context.Context, result *RewriteResult) error {
	if result == nil || result.RowsRemoved == 0 {
		return nil
	}
	if !result.Published {
		return fmt.Errorf("refusing to delete %s: rewrite not published to the manifest", result.OldKey)
	}
	if err := r.pool.Delete(ctx, result.OldKey); err != nil {
		return fmt.Errorf("delete superseded file %s: %w", result.OldKey, err)
	}
	return nil
}

// Discard deletes a replacement that was written but will never be published —
// the source was merged away concurrently. Best-effort: an object left behind is
// unmanifested, so the orphan sweep reclaims it.
func (r *Rewriter) Discard(ctx context.Context, result *RewriteResult) {
	if result == nil || result.Published || result.NewKey == "" {
		return
	}
	if err := r.pool.Delete(ctx, result.NewKey); err != nil {
		logger.Warnf("discarded rewrite not deleted (orphan sweep will reclaim it); key=%s: %s", result.NewKey, err)
	}
}

// footerBloomBytes sums the encoded size of every column-chunk bloom filter in
// the written file — the same measure the flush and compaction writers record,
// so the manifest's bloom-cost accounting stays comparable across all three
// producers. Best-effort: 0 when the footer can't be parsed.
func footerBloomBytes(data []byte) int64 {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return 0
	}
	var total int64
	for _, rg := range f.RowGroups() {
		for _, cc := range rg.ColumnChunks() {
			if bf := cc.BloomFilter(); bf != nil {
				total += bf.Size()
			}
		}
	}
	return total
}

// columnBytesFromFooter returns column name -> total compressed bytes, read
// from the written footer. Mirrors the flush/compaction writers so per-field
// storage accounting does not go blank the moment a file is rewritten.
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

// sourceSlotMapping lifts the Tier-2 dedicated-slot name binding out of the
// SOURCE file's footer so the rewritten file keeps it. The read path resolves
// ded_sNN columns by each file's OWN footer KV and skips the raw slot column
// when the key is absent — so a rewritten file that dropped the binding would
// serve its kept rows with the promoted attributes missing. Returns nil (and
// the caller writes no KV) when the source carried none.
func sourceSlotMapping(data []byte) []byte {
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		return nil
	}
	md := f.Metadata()
	if md == nil {
		return nil
	}
	for _, kv := range md.KeyValueMetadata {
		if kv.Key == schema.DedicatedSlotsMetaKey {
			return []byte(kv.Value)
		}
	}
	return nil
}

// writerOptions builds the option set for the FALLBACK writer (see
// ParquetWriters): the configured row group size plus the source's slot binding
// when it had one.
//
// Per-row-group token-bloom KV entries are deliberately NOT carried over. They
// are keyed by row group index, and removing rows re-packs the row groups, so
// source row group i's bloom does not describe output row group i. Copying them
// would let the reader skip a row group that does hold matching rows — a false
// NEGATIVE, i.e. silently missing query results. Omitting the key means "no
// bloom information", which costs a scan and returns the right answer.
func (r *Rewriter) writerOptions(src []byte) []parquet.WriterOption {
	opts := []parquet.WriterOption{
		parquet.MaxRowsPerRowGroup(int64(r.rowGroupSize)),
	}
	if kv := sourceSlotMapping(src); len(kv) > 0 {
		opts = append(opts, parquet.KeyValueMetadata(schema.DedicatedSlotsMetaKey, string(kv)))
	}
	return opts
}

func (r *Rewriter) filterLogRows(data []byte, tombstones []Tombstone, result *RewriteResult) ([]byte, error) {
	reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	n := int(reader.NumRows())
	rows := make([]schema.LogRow, n)
	total, err := reader.Read(rows)
	if err != nil && total == 0 {
		return nil, fmt.Errorf("read parquet rows: %w", err)
	}
	rows = rows[:total]

	kept := make([]schema.LogRow, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		fields := logRowToMap(row)
		ts := row.TimestampUnixNano
		if !matchesAny(fields, ts, tombstones) {
			kept = append(kept, *row)
		}
	}

	result.RowsRemoved = int64(len(rows)) - int64(len(kept))
	result.RowsKept = int64(len(kept))

	if result.RowsRemoved == 0 || len(kept) == 0 {
		return nil, nil
	}

	// Manifest metadata recomputed over the KEPT rows. Inheriting either of
	// these from the superseded entry would keep counting the deleted rows.
	result.MinTimeNs, result.MaxTimeNs = schema.LogRowTimeBounds(kept)
	result.LabelAggregates = schema.ExtractLogLabelAggregates(kept)
	result.Labels = schema.ExtractLogLabels(kept)
	result.BloomValues = schema.ExtractLogBloomValues(kept)
	result.RawBytes = schema.EstimateRawBytesLogs(kept)

	if r.writers.Logs != nil {
		out, err := r.writers.Logs(kept, r.rowGroupSize, r.writers.CompressionLevel)
		if err != nil {
			return nil, fmt.Errorf("write parquet: %w", err)
		}
		return out, nil
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.LogRow](&buf, r.writerOptions(data)...)
	if _, err := writer.Write(kept); err != nil {
		return nil, fmt.Errorf("write parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}

func (r *Rewriter) filterTraceRows(data []byte, tombstones []Tombstone, result *RewriteResult) ([]byte, error) {
	reader := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()

	n := int(reader.NumRows())
	rows := make([]schema.TraceRow, n)
	total, err := reader.Read(rows)
	if err != nil && total == 0 {
		return nil, fmt.Errorf("read parquet rows: %w", err)
	}
	rows = rows[:total]

	kept := make([]schema.TraceRow, 0, len(rows))
	for i := range rows {
		row := &rows[i]
		fields := traceRowToMap(row)
		ts := row.TimestampUnixNano
		if !matchesAny(fields, ts, tombstones) {
			kept = append(kept, *row)
		}
	}

	result.RowsRemoved = int64(len(rows)) - int64(len(kept))
	result.RowsKept = int64(len(kept))

	if result.RowsRemoved == 0 || len(kept) == 0 {
		return nil, nil
	}

	// See filterLogRows: recomputed over the kept rows, never inherited.
	result.MinTimeNs, result.MaxTimeNs = schema.TraceRowTimeBounds(kept)
	result.LabelAggregates = schema.ExtractTraceLabelAggregates(kept)
	result.Labels = schema.ExtractTraceLabels(kept)
	result.BloomValues = schema.ExtractTraceBloomValues(kept)
	result.RawBytes = schema.EstimateRawBytesTraces(kept)

	if r.writers.Traces != nil {
		out, err := r.writers.Traces(kept, r.rowGroupSize, r.writers.CompressionLevel)
		if err != nil {
			return nil, fmt.Errorf("write parquet: %w", err)
		}
		return out, nil
	}

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.TraceRow](&buf, r.writerOptions(data)...)
	if _, err := writer.Write(kept); err != nil {
		return nil, fmt.Errorf("write parquet: %w", err)
	}
	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}

func matchesAny(fields map[string]string, ts int64, tombstones []Tombstone) bool {
	for i := range tombstones {
		if tombstones[i].MatchesRow(fields, ts) {
			return true
		}
	}
	return false
}

// extractPartition extracts the partition path (e.g. "dt=2026-01-01/hour=10")
// from a key like "logs/dt=2026-01-01/hour=10/00001.parquet".
func extractPartition(key string) string {
	parts := strings.Split(key, "/")

	// Find partition segments that contain "=" (e.g. dt=..., hour=...).
	var partitionParts []string
	for _, p := range parts {
		if strings.Contains(p, "=") {
			partitionParts = append(partitionParts, p)
		}
	}

	if len(partitionParts) == 0 {
		return "unknown"
	}
	return strings.Join(partitionParts, "/")
}

// logRowToMap converts a LogRow into a map[string]string for tombstone matching.
func logRowToMap(row *schema.LogRow) map[string]string {
	m := map[string]string{
		"body":                   row.Body,
		"severity_text":          row.SeverityText,
		"service.name":           row.ServiceName,
		"k8s.namespace.name":     row.K8sNamespaceName,
		"k8s.pod.name":           row.K8sPodName,
		"k8s.deployment.name":    row.K8sDeploymentName,
		"k8s.node.name":          row.K8sNodeName,
		"deployment.environment": row.DeployEnv,
		"cloud.region":           row.CloudRegion,
		"host.name":              row.HostName,
		"trace_id":               row.TraceID,
		"span_id":                row.SpanID,
		"_stream":                row.Stream,
		"_stream_id":             row.StreamID,
		"scope.name":             row.ScopeName,
	}
	for k, v := range row.ResourceAttributes {
		m[k] = v
	}
	for k, v := range row.LogAttributes {
		m[k] = v
	}
	return m
}

// traceRowToMap converts a TraceRow into a map[string]string for tombstone matching.
func traceRowToMap(row *schema.TraceRow) map[string]string {
	m := map[string]string{
		"body":                   row.SpanName,
		"trace_id":               row.TraceID,
		"span_id":                row.SpanID,
		"parent_span_id":         row.ParentSpanID,
		"span.name":              row.SpanName,
		"service.name":           row.ServiceName,
		"status.message":         row.StatusMessage,
		"scope.name":             row.ScopeName,
		"deployment.environment": row.DeployEnv,
		"cloud.region":           row.CloudRegion,
		"host.name":              row.HostName,
		"k8s.namespace.name":     row.K8sNamespaceName,
		"k8s.deployment.name":    row.K8sDeploymentName,
		"k8s.node.name":          row.K8sNodeName,
		"http.method":            row.HTTPMethod,
		"http.status_code":       row.HTTPStatusCode,
		"http.url":               row.HTTPUrl,
		"db.system":              row.DBSystem,
		"db.statement":           row.DBStatement,
	}
	if row.SpanKind != 0 {
		m["span.kind"] = fmt.Sprintf("%d", row.SpanKind)
	}
	if row.StatusCode != 0 {
		m["status.code"] = fmt.Sprintf("%d", row.StatusCode)
	}
	if row.DurationNs != 0 {
		m["duration_ns"] = fmt.Sprintf("%d", row.DurationNs)
	}
	for k, v := range row.ResourceAttributes {
		m[k] = v
	}
	for k, v := range row.SpanAttributes {
		m[k] = v
	}
	for k, v := range row.ScopeAttributes {
		m[k] = v
	}
	return m
}
