package vlstorage

import (
	"context"
	"errors"
	"strings"
	"sync/atomic"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/prefixfilter"

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
	hiddenFilters := qctx.HiddenFieldsFilters

	// IMPORTANT: pass the FULL query (with pipes intact) to a.store.RunQuery.
	// Our storage's queryColumns() consults logstorage.GetQueryPipeFields() to
	// expand the parquet column projection to cover fields referenced only by
	// pipes (e.g. `| fields _time, trace_id` or `| partition by (trace_id)`).
	// If we strip pipes here, the projection misses those fields, the emitted
	// DataBlocks don't carry them, and downstream pipes (e.g. `partition by`)
	// silently drop every row. Mirrors the equivalent fix in
	// lakehouse-traces/internal/vtstorage_adapter/adapter.go.
	//
	// Stripping pipes for actual row matching happens inside RunQuery itself
	// via parseFilterFromQuery (Clone + DropAllPipes), so passing the full
	// query here is safe — pipes only inform column projection planning.

	if logstorage.QueryHasPipes(qctx.Query) {
		// Field-enumerating pipes (field_names / field_values / facets /
		// block_stats) need every column on the row — bypass projection
		// narrowing via the all-fields hint so the projection layer
		// reads all Parquet columns. Mirror of the vtstorage_adapter
		// path in lakehouse-traces/internal/vtstorage_adapter/adapter.go.
		ctx := qctx.Context
		if logstorage.QueryNeedsAllFields(qctx.Query) {
			ctx = storage.WithAllFieldsHint(ctx)
		}
		searchFn := func(wb logstorage.WriteDataBlockFunc) error {
			return a.store.RunQuery(ctx, qctx.TenantIDs, qctx.Query,
				wrapHiddenFields(wb, hiddenFilters))
		}
		return logstorage.RunQueryExternalWithSubqueries(qctx, searchFn, a.RunQuery, writeBlock)
	}

	return a.store.RunQuery(qctx.Context, qctx.TenantIDs, qctx.Query,
		wrapHiddenFields(writeBlock, hiddenFilters))
}

func (a *adapter) GetFieldNames(qctx *logstorage.QueryContext, filter string) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	results = filterHiddenValues(results, qctx.HiddenFieldsFilters)
	return filterValuesBySubstring(results, filter), nil
}

func (a *adapter) GetFieldValues(qctx *logstorage.QueryContext, fieldName, filter string, limit uint64) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
	if err != nil {
		return nil, err
	}
	return filterValuesBySubstring(results, filter), nil
}

func (a *adapter) GetStreamFieldNames(qctx *logstorage.QueryContext, filter string) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetStreamFieldNames(qctx.Context, qctx.TenantIDs, qctx.Query)
	if err != nil {
		return nil, err
	}
	results = filterHiddenValues(results, qctx.HiddenFieldsFilters)
	return filterValuesBySubstring(results, filter), nil
}

func (a *adapter) GetStreamFieldValues(qctx *logstorage.QueryContext, fieldName, filter string, limit uint64) ([]logstorage.ValueWithHits, error) {
	results, err := a.store.GetStreamFieldValues(qctx.Context, qctx.TenantIDs, qctx.Query, fieldName, limit)
	if err != nil {
		return nil, err
	}
	return filterValuesBySubstring(results, filter), nil
}

func (a *adapter) GetStreams(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreams(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

func (a *adapter) GetStreamIDs(qctx *logstorage.QueryContext, limit uint64) ([]logstorage.ValueWithHits, error) {
	return a.store.GetStreamIDs(qctx.Context, qctx.TenantIDs, qctx.Query, limit)
}

// tenantLister is implemented by storages that can enumerate the tenants
// holding data in a time range. Kept as an optional interface so the core
// storage.Storage contract (and every test double implementing it) is unchanged.
type tenantLister interface {
	TenantIDsForRange(startNs, endNs int64) []logstorage.TenantID
}

func (a *adapter) GetTenantIDs(_ context.Context, start, end int64) ([]logstorage.TenantID, error) {
	// Report the tenants the cold tier actually holds. The previous hardcoded
	// 0:0 made every multi-tenant deployment look single-tenant to callers that
	// enumerate tenants.
	if tl, ok := a.store.(tenantLister); ok {
		return tl.TenantIDsForRange(start, end), nil
	}
	if !a.store.HasDataForRange(start, end) {
		return nil, nil
	}
	return []logstorage.TenantID{{AccountID: 0, ProjectID: 0}}, nil
}

// deleteTaskMode is the lakehouse delete mode (delete.default_mode) the
// tombstone of a delete task gets; unset, a task hides its rows (the reversible
// mode). Set once at startup by SetDeleteTaskMode.
var deleteTaskMode atomic.Pointer[string]

// SetDeleteTaskMode sets the delete mode for tombstones created by upstream's
// delete API (/delete/run_task, /internal/delete/run_task).
func SetDeleteTaskMode(mode string) {
	deleteTaskMode.Store(&mode)
}

func taskMode() string {
	if p := deleteTaskMode.Load(); p != nil {
		return *p
	}
	return ""
}

// DeleteRunTask registers the task as a tombstone scoped to exactly the task's
// tenant_ids (see delete.RunTask). Upstream's /delete/run_task passes the
// request's tenant; the cluster protocol passes the list the frontend sent.
func (a *adapter) DeleteRunTask(_ context.Context, taskID string, timestamp int64, tenantIDs []logstorage.TenantID, f *logstorage.Filter) error {
	if f == nil {
		return errors.New("missing filter")
	}
	return delete.RunTask(a.tombstones, delete.TaskFilesOf(a.store), delete.AccountOnlyKeys(a.store), taskID, timestamp, delete.TenantRefsOf(tenantIDs), f.String(), taskMode())
}

// DeleteStopTask removes the task's tombstone by id (an un-delete; refused while
// a rewrite of its files is unfinished). Stopping an unknown task is a no-op; so
// is stopping another tenant's task from the public API (delete.StopTask).
func (a *adapter) DeleteStopTask(ctx context.Context, taskID string) error {
	return delete.StopTask(ctx, a.tombstones, taskID)
}

// DeleteActiveTasks lists the active tombstones in upstream's DeleteTask shape,
// only the caller's own for a tenant caller of the public API.
func (a *adapter) DeleteActiveTasks(ctx context.Context) ([]*logstorage.DeleteTask, error) {
	return delete.ActiveTasks(ctx, a.tombstones), nil
}

// filterValuesBySubstring filters results to only include values containing the substring.
// Returns the original slice if filter is empty.
func filterValuesBySubstring(results []logstorage.ValueWithHits, filter string) []logstorage.ValueWithHits {
	if filter == "" {
		return results
	}
	filtered := make([]logstorage.ValueWithHits, 0, len(results))
	for _, v := range results {
		if strings.Contains(v.Value, filter) {
			filtered = append(filtered, v)
		}
	}
	return filtered
}

// wrapHiddenFields wraps writeBlock to strip columns matching HiddenFieldsFilters.
// Uses VL's prefixfilter.MatchFilters for exact and wildcard matching.
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
