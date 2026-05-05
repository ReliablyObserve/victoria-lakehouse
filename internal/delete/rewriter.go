package delete

import (
	"bytes"
	"context"
	"fmt"
	"strings"
	"time"

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
type RewriteResult struct {
	OldKey      string
	NewKey      string
	RowsKept    int64
	RowsRemoved int64
	BytesBefore int64
	BytesAfter  int64
	Duration    time.Duration
}

// Rewriter reads Parquet files from S3, removes tombstoned rows, and writes
// the filtered result back.
type Rewriter struct {
	pool         RewriterPool
	prefix       string
	rowGroupSize int
	mode         string
}

// NewRewriter creates a Rewriter with the given pool, key prefix, row group size, and mode.
// Mode should be "logs" or "traces". If rowGroupSize <= 0 it defaults to 10000.
func NewRewriter(pool RewriterPool, prefix string, rowGroupSize int, mode string) *Rewriter {
	if rowGroupSize <= 0 {
		rowGroupSize = 10000
	}
	if mode == "" {
		mode = "logs"
	}
	return &Rewriter{
		pool:         pool,
		prefix:       prefix,
		rowGroupSize: rowGroupSize,
		mode:         mode,
	}
}

// RewriteFile downloads the Parquet file at key, removes rows matching any of
// the provided tombstones, and uploads the filtered file. If no rows are
// removed the original file is left untouched and RowsRemoved == 0.
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
		if err := r.pool.Delete(ctx, key); err != nil {
			return nil, fmt.Errorf("delete empty file %s: %w", key, err)
		}
		result.BytesAfter = 0
		result.Duration = time.Since(start)
		return result, nil
	}

	result.BytesAfter = int64(len(newData))

	partition := extractPartition(key)
	short := uuid.New().String()[:8]
	newKey := fmt.Sprintf("%s%s/%s.parquet", r.prefix, partition, short)
	result.NewKey = newKey

	if err := r.pool.Upload(ctx, newKey, newData); err != nil {
		return nil, fmt.Errorf("upload %s: %w", newKey, err)
	}
	if err := r.pool.Delete(ctx, key); err != nil {
		return nil, fmt.Errorf("delete old file %s: %w", key, err)
	}

	result.Duration = time.Since(start)
	return result, nil
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

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.MaxRowsPerRowGroup(int64(r.rowGroupSize)),
	)
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

	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[schema.TraceRow](&buf,
		parquet.MaxRowsPerRowGroup(int64(r.rowGroupSize)),
	)
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
		"severity_text":         row.SeverityText,
		"service.name":          row.ServiceName,
		"k8s.namespace.name":    row.K8sNamespaceName,
		"k8s.pod.name":          row.K8sPodName,
		"k8s.deployment.name":   row.K8sDeploymentName,
		"k8s.node.name":         row.K8sNodeName,
		"deployment.environment": row.DeployEnv,
		"cloud.region":          row.CloudRegion,
		"host.name":             row.HostName,
		"trace_id":              row.TraceID,
		"span_id":               row.SpanID,
		"_stream":               row.Stream,
		"_stream_id":            row.StreamID,
		"scope.name":            row.ScopeName,
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
		"trace_id":              row.TraceID,
		"span_id":               row.SpanID,
		"parent_span_id":        row.ParentSpanID,
		"span.name":             row.SpanName,
		"service.name":          row.ServiceName,
		"status.message":        row.StatusMessage,
		"scope.name":            row.ScopeName,
		"deployment.environment": row.DeployEnv,
		"cloud.region":          row.CloudRegion,
		"host.name":             row.HostName,
		"k8s.namespace.name":    row.K8sNamespaceName,
		"k8s.deployment.name":   row.K8sDeploymentName,
		"k8s.node.name":         row.K8sNodeName,
		"http.method":           row.HTTPMethod,
		"http.status_code":      row.HTTPStatusCode,
		"http.url":              row.HTTPUrl,
		"db.system":             row.DBSystem,
		"db.statement":          row.DBStatement,
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
