package vlstorage

import (
	"context"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
)

type adapter struct {
	store      storage.Storage
	tombstones *delete.TombstoneStore
}

// SetStorage configures VL's vlstorage dispatch to route all queries
// through the given storage backend via the ExternalStorage interface.
func SetStorage(s storage.Storage, ts *delete.TombstoneStore) {
	vlstorage.SetExternalStorage(&adapter{store: s, tombstones: ts})
}

func (a *adapter) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	return a.store.RunQuery(qctx.Context, qctx.TenantIDs, qctx.Query, writeBlock)
}

func (a *adapter) GetFieldNames(qctx *logstorage.QueryContext, _ string) ([]logstorage.ValueWithHits, error) {
	return a.store.GetFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
}

func (a *adapter) GetFieldValues(qctx *logstorage.QueryContext, fieldName, _ string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}

func (a *adapter) GetStreamFieldNames(qctx *logstorage.QueryContext, _ string) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
}

func (a *adapter) GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName, _ string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}

func (a *adapter) GetStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreams(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func (a *adapter) GetStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamIDs(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func (a *adapter) GetTenantIDs(_ context.Context, _, _ int64) ([]logstorage.TenantID, error) {
	return []logstorage.TenantID{{AccountID: 0, ProjectID: 0}}, nil
}

func (a *adapter) DeleteRunTask(_ context.Context, taskID string, timestamp int64, _ []logstorage.TenantID, f *logstorage.Filter) error {
	if a.tombstones == nil {
		return nil
	}
	a.tombstones.Add(delete.Tombstone{
		ID:        taskID,
		Query:     f.String(),
		StartNs:   0,
		EndNs:     timestamp,
		CreatedAt: time.Now(),
		Mode:      "auto",
	})
	return nil
}

func (a *adapter) DeleteStopTask(_ context.Context, taskID string) error {
	if a.tombstones == nil {
		return nil
	}
	a.tombstones.Remove(taskID)
	return nil
}

func (a *adapter) DeleteActiveTasks(_ context.Context) ([]*logstorage.DeleteTask, error) {
	if a.tombstones == nil {
		return nil, nil
	}
	active := a.tombstones.Active()
	result := make([]*logstorage.DeleteTask, 0, len(active))
	for _, t := range active {
		result = append(result, &logstorage.DeleteTask{
			TaskID: t.ID,
		})
	}
	return result, nil
}

func UpdatePerQueryStatsMetrics(qs *logstorage.QueryStats) {
	if qs == nil {
		return
	}
}
