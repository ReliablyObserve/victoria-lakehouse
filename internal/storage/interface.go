package storage

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

type queryHintKey struct{}
type countOnlyKey struct{}
type allFieldsKey struct{}

func WithTimestampOnlyHint(ctx context.Context) context.Context {
	return context.WithValue(ctx, queryHintKey{}, true)
}

func IsTimestampOnly(ctx context.Context) bool {
	v, _ := ctx.Value(queryHintKey{}).(bool)
	return v
}

func WithCountOnlyHint(ctx context.Context) context.Context {
	return context.WithValue(ctx, countOnlyKey{}, true)
}

func IsCountOnly(ctx context.Context) bool {
	v, _ := ctx.Value(countOnlyKey{}).(bool)
	return v
}

// WithAllFieldsHint signals to the storage layer that the query has a
// field-enumerating pipe (field_names / field_values / facets /
// block_stats) and projection narrowing must be bypassed — every
// column the rows carry needs to reach the pipe processor, otherwise
// the enumeration reports a truncated schema.
func WithAllFieldsHint(ctx context.Context) context.Context {
	return context.WithValue(ctx, allFieldsKey{}, true)
}

// IsAllFields reports whether the caller asked the storage to bypass
// column-narrowing for field enumeration.
func IsAllFields(ctx context.Context) bool {
	v, _ := ctx.Value(allFieldsKey{}).(bool)
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
