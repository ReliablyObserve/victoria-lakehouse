package storage

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type queryHintKey struct{}

func WithTimestampOnlyHint(ctx context.Context) context.Context {
	return context.WithValue(ctx, queryHintKey{}, true)
}

func IsTimestampOnly(ctx context.Context) bool {
	v, _ := ctx.Value(queryHintKey{}).(bool)
	return v
}

type Storage interface {
	RunQuery(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, writeBlock logstorage.WriteDataBlockFunc) error
	GetFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error)
	GetFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error)
	GetStreamFieldNames(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query) ([]logstorage.ValueWithHits, error)
	GetStreamFieldValues(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error)
	GetStreams(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error)
	GetStreamIDs(ctx context.Context, tenantIDs []logstorage.TenantID, q *logstorage.Query, limit uint64) ([]logstorage.ValueWithHits, error)
	HasDataForRange(startNs, endNs int64) bool
	Close() error
}
