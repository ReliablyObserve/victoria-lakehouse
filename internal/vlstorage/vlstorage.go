package vlstorage

import (
	"context"
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

var (
	mu    sync.RWMutex
	store storage.Storage
)

func SetStorage(s storage.Storage) {
	mu.Lock()
	store = s
	mu.Unlock()
}

func getStorage() storage.Storage {
	mu.RLock()
	s := store
	mu.RUnlock()
	return s
}

func RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	return getStorage().RunQuery(qctx.Context, qctx.TenantIDs, qctx.Query, writeBlock)
}

func GetFieldNames(qctx *logstorage.QueryContext, _ string) ([]logstorage.ValueWithHits, error) {
	return getStorage().GetFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
}

func GetFieldValues(qctx *logstorage.QueryContext, fieldName, _ string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return getStorage().GetFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}

func GetStreamFieldNames(qctx *logstorage.QueryContext, _ string) ([]logstorage.ValueWithHits, error) {
	return getStorage().GetStreamFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
}

func GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName, _ string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return getStorage().GetStreamFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}

func GetStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return getStorage().GetStreams(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func GetStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return getStorage().GetStreamIDs(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func GetTenantIDs(_ context.Context, _, _ int64) ([]logstorage.TenantID, error) {
	return nil, nil
}

func DeleteRunTask(_ context.Context, _ string, _ int64, _ []logstorage.TenantID, _ *logstorage.Filter) error {
	return nil
}

func DeleteStopTask(_ context.Context, _ string) error {
	return nil
}

func DeleteActiveTasks(_ context.Context) ([]*logstorage.DeleteTask, error) {
	return nil, nil
}

func UpdatePerQueryStatsMetrics(_ *logstorage.QueryStats) {}
