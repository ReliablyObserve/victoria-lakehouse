package vlstorage

import (
	"context"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/storage"
)

type adapter struct {
	store      storage.Storage
	tombstones *delete.TombstoneStore
}

func SetStorage(s storage.Storage, ts *delete.TombstoneStore) {
	vlstorage.SetExternalStorage(&adapter{store: s, tombstones: ts})
}

func (a *adapter) RunQuery(qctx *logstorage.QueryContext, writeBlock logstorage.WriteDataBlockFunc) error {
	hiddenFilters := qctx.HiddenFieldsFilters

	if logstorage.QueryHasPipes(qctx.Query) {
		searchFn := func(wb logstorage.WriteDataBlockFunc) error {
			return a.store.RunQuery(qctx.Context, qctx.TenantIDs, qctx.Query,
				wrapHiddenFields(wb, hiddenFilters))
		}
		return logstorage.RunQueryExternal(qctx, searchFn, writeBlock)
	}

	return a.store.RunQuery(qctx.Context, qctx.TenantIDs, qctx.Query,
		wrapHiddenFields(writeBlock, hiddenFilters))
}

func (a *adapter) GetFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	return filterHiddenValues(results, qctx.HiddenFieldsFilters), nil
}

func (a *adapter) GetFieldValues(qctx *logstorage.QueryContext, fieldName string, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
}

func (a *adapter) GetStreamFieldNames(qctx *logstorage.QueryContext) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetStreamFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	return filterHiddenValues(results, qctx.HiddenFieldsFilters), nil
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

func (a *adapter) GetTenantIDs(_ context.Context, start, end int64) ([]logstorage.TenantID, error) {
	if !a.store.HasDataForRange(start, end) {
		return nil, nil
	}
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

// wrapHiddenFields wraps writeBlock to strip columns matching HiddenFieldsFilters.
func wrapHiddenFields(writeBlock logstorage.WriteDataBlockFunc, filters []string) logstorage.WriteDataBlockFunc {
	if len(filters) == 0 {
		return writeBlock
	}
	return func(workerID uint, db *logstorage.DataBlock) {
		columns := db.GetColumns(false)
		filtered := make([]logstorage.BlockColumn, 0, len(columns))
		for _, col := range columns {
			if !prefixfilter.MatchFilters(filters, col.Name) {
				filtered = append(filtered, col)
			}
		}
		if len(filtered) == len(columns) {
			writeBlock(workerID, db)
			return
		}
		if len(filtered) == 0 {
			return
		}
		result := &logstorage.DataBlock{}
		result.SetColumns(filtered)
		writeBlock(workerID, result)
	}
}

// filterHiddenValues removes entries whose Value matches any HiddenFieldsFilter pattern.
func filterHiddenValues(results []logstorage.ValueWithHits, filters []string) []logstorage.ValueWithHits {
	if len(filters) == 0 {
		return results
	}
	filtered := make([]logstorage.ValueWithHits, 0, len(results))
	for _, v := range results {
		if !prefixfilter.MatchFilters(filters, v.Value) {
			filtered = append(filtered, v)
		}
	}
	return filtered
}