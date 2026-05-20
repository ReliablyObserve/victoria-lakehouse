package vlstorage

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
)

// Compile-time check that TracedStorage satisfies storage.Storage.
var _ storage.Storage = (*TracedStorage)(nil)

// TracedStorage wraps a storage.Storage and adds OTEL tracing spans
// to each query method.
type TracedStorage struct {
	inner storage.Storage
}

// NewTracedStorage returns a TracedStorage decorator around s.
func NewTracedStorage(s storage.Storage) *TracedStorage {
	return &TracedStorage{inner: s}
}

func (t *TracedStorage) RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error {
	ctx, span := otel.Tracer("lakehouse-traces").Start(ctx, "storage.run_query")
	defer span.End()
	span.SetAttributes(attribute.Int("tenant_count", len(tenantIDs)))
	return t.inner.RunQuery(ctx, tenantIDs, q, writeBlock)
}

func (t *TracedStorage) GetFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	ctx, span := otel.Tracer("lakehouse-traces").Start(ctx, "storage.get_field_names")
	defer span.End()
	return t.inner.GetFieldNames(ctx, tenantIDs, q)
}

func (t *TracedStorage) GetFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	ctx, span := otel.Tracer("lakehouse-traces").Start(ctx, "storage.get_field_values")
	defer span.End()
	span.SetAttributes(attribute.String("field", fieldName))
	return t.inner.GetFieldValues(ctx, tenantIDs, q, fieldName, limit)
}

func (t *TracedStorage) GetStreamFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error) {
	ctx, span := otel.Tracer("lakehouse-traces").Start(ctx, "storage.get_stream_field_names")
	defer span.End()
	return t.inner.GetStreamFieldNames(ctx, tenantIDs, q)
}

func (t *TracedStorage) GetStreamFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	ctx, span := otel.Tracer("lakehouse-traces").Start(ctx, "storage.get_stream_field_values")
	defer span.End()
	span.SetAttributes(attribute.String("field", fieldName))
	return t.inner.GetStreamFieldValues(ctx, tenantIDs, q, fieldName, limit)
}

func (t *TracedStorage) GetStreams(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	ctx, span := otel.Tracer("lakehouse-traces").Start(ctx, "storage.get_streams")
	defer span.End()
	return t.inner.GetStreams(ctx, tenantIDs, q, limit)
}

func (t *TracedStorage) GetStreamIDs(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error) {
	ctx, span := otel.Tracer("lakehouse-traces").Start(ctx, "storage.get_stream_ids")
	defer span.End()
	return t.inner.GetStreamIDs(ctx, tenantIDs, q, limit)
}

// HasDataForRange delegates without a span (hot path, called constantly).
func (t *TracedStorage) HasDataForRange(startNs, endNs int64) bool {
	return t.inner.HasDataForRange(startNs, endNs)
}

// Close delegates without a span (lifecycle, not performance-relevant).
func (t *TracedStorage) Close() error {
	return t.inner.Close()
}
