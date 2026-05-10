package vlstorage

import (
	"context"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"
)

type adapter struct {
	store storage.Storage
}

func SetStorage(s storage.Storage) {
	vlstorage.SetExternalStorage(&adapter{store: s})
}

func (a *adapter) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	return a.store.RunQuery(qctx.Context, qctx.TenantIDs, qctx.Query, writeBlock)
}

func (a *adapter) GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	return a.store.GetFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
}

func (a *adapter) GetFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}

func (a *adapter) GetStreamFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
}

func (a *adapter) GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}

func (a *adapter) GetStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreams(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func (a *adapter) GetStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamIDs(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func (a *adapter) GetTenantIDs(_ context.Context, _, _ int64) ([]logstorage.TenantID, error) {
	return nil, nil
}

func (a *adapter) DeleteRunTask(_ context.Context, _ string, _ int64, _ []logstorage.TenantID, _ *logstorage.Filter) error {
	return nil
}

func (a *adapter) DeleteStopTask(_ context.Context, _ string) error {
	return nil
}

func (a *adapter) DeleteActiveTasks(_ context.Context) ([]*logstorage.DeleteTask, error) {
	return nil, nil
}

func UpdatePerQueryStatsMetrics(_ *logstorage.QueryStats) {}
