package delete

import (
	"errors"
	"fmt"
	"math"
	"sort"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Delete tasks: upstream's delete API (/delete/run_task and the cluster
// protocol's /internal/delete/run_task) mapped onto tombstones.
//
// Upstream registers a task per call — task_id, the tenants it names, a LogsQL
// filter, and the timestamp it was issued at — and deletes the rows of exactly
// those tenants that match the filter and are not newer than the timestamp
// (lib/logstorage/storage.go: DeleteRunTask, processDeleteTask). The lakehouse
// registers the same task as a tombstone with that id, scoped to those tenants,
// covering (-inf, timestamp]. stop_task removes it by id, and active_tasks lists
// tombstones in upstream's DeleteTask shape.

// TaskFiles lists the objects that may hold rows of the given tenants in
// [startNs, endNs] — the storage's tenant-scoped file listing.
type TaskFiles func(tenants []TenantRef, startNs, endNs int64) []string

// TenantFileLister is the storage's tenant-scoped object listing
// (parquets3.Storage.TenantFileKeys).
type TenantFileLister interface {
	TenantFileKeys(tenantIDs []logstorage.TenantID, startNs, endNs int64) []string
}

// TaskFilesOf returns the TaskFiles of a storage that lists its objects by
// tenant, or nil when it does not (the tombstone then starts with no affected
// keys and the rewrite scheduler discovers them).
func TaskFilesOf(storage any) TaskFiles {
	l, ok := storage.(TenantFileLister)
	if !ok {
		return nil
	}
	return func(tenants []TenantRef, startNs, endNs int64) []string {
		return l.TenantFileKeys(TenantIDsOf(tenants), startNs, endNs)
	}
}

// TenantRefsOf converts VL tenant ids to tombstone tenants.
func TenantRefsOf(ids []logstorage.TenantID) []TenantRef {
	if len(ids) == 0 {
		return nil
	}
	out := make([]TenantRef, len(ids))
	for i, id := range ids {
		out[i] = TenantRef{AccountID: id.AccountID, ProjectID: id.ProjectID}
	}
	return out
}

// TenantIDsOf converts tombstone tenants to VL tenant ids.
func TenantIDsOf(refs []TenantRef) []logstorage.TenantID {
	if len(refs) == 0 {
		return nil
	}
	out := make([]logstorage.TenantID, len(refs))
	for i, r := range refs {
		out[i] = logstorage.TenantID{AccountID: r.AccountID, ProjectID: r.ProjectID}
	}
	return out
}

// ErrTaskExists matches upstream's refusal of a task id that is already
// registered (errors.Is); the error's text is upstream's.
var ErrTaskExists = errors.New("the delete task is already registered")

type taskExistsError struct{ id string }

func (e taskExistsError) Error() string {
	return fmt.Sprintf("the delete task with task_id=%q is already registered", e.id)
}

func (e taskExistsError) Is(target error) bool { return target == ErrTaskExists }

// RunTask registers a delete task as a tenant-scoped tombstone. filter is the
// task's LogsQL filter (upstream's Filter.String()), timestamp the task's issue
// time in nanoseconds, mode the lakehouse delete mode (hide, permanent, auto).
//
// A task that names no tenant deletes nothing upstream — its search has no
// tenant to match — and is accepted. It is accepted here too and creates
// nothing: an empty tenant list on a tombstone would mean "every tenant".
func RunTask(store *TombstoneStore, files TaskFiles, taskID string, timestamp int64, tenants []TenantRef, filter, mode string) error {
	if store == nil {
		return fmt.Errorf("the lakehouse delete feature is disabled; set delete.enabled: true in the lakehouse config")
	}
	tenants = NormalizeTenants(tenants)
	if len(tenants) == 0 {
		logger.Infof("delete task names no tenant; nothing to delete; task_id=%q, filter=%q", taskID, filter)
		return nil
	}
	if mode == "" {
		mode = "hide"
	}
	const startNs = math.MinInt64
	var keys []string
	if files != nil {
		keys = files(tenants, startNs, timestamp)
	}
	ts := Tombstone{
		ID:           taskID,
		Query:        filter,
		StartNs:      startNs,
		EndNs:        timestamp,
		AffectedKeys: keys,
		// The un-delete window (rewrite_delay) runs from when this node
		// accepted the task, not from the issuer's clock.
		CreatedAt: time.Now(),
		CreatedBy: "delete_task",
		Mode:      mode,
		Tenants:   tenants,
	}
	if err := ts.Validate(); err != nil {
		return fmt.Errorf("invalid delete task: %w", err)
	}
	if !store.AddIfAbsent(ts) {
		return taskExistsError{id: taskID}
	}
	metrics.DeleteTombstonesTotal.Inc()
	logger.Infof("delete task registered as a tombstone; task_id=%q, tenants=%v, filter=%q, mode=%s, affected_files=%d",
		taskID, tenants, filter, mode, len(keys))
	return nil
}

// ActiveTasks lists every active tombstone as one of upstream's delete tasks
// (task id, tenants, filter, start time), oldest first. A record without tenant
// scope has no tenants to list.
func ActiveTasks(store *TombstoneStore) []*logstorage.DeleteTask {
	if store == nil {
		return nil
	}
	active := store.Active()
	out := make([]*logstorage.DeleteTask, 0, len(active))
	for _, ts := range active {
		out = append(out, &logstorage.DeleteTask{
			TaskID:    ts.ID,
			TenantIDs: TenantIDsOf(ts.Tenants),
			Filter:    ts.Query,
			StartTime: ts.CreatedAt,
		})
	}
	sort.Slice(out, func(i, j int) bool {
		if !out[i].StartTime.Equal(out[j].StartTime) {
			return out[i].StartTime.Before(out[j].StartTime)
		}
		return out[i].TaskID < out[j].TaskID
	})
	return out
}

// StopTask removes the task's tombstone by id: an un-delete, refused while a
// rewrite of its files is unfinished. Stopping an unknown task is a no-op, as
// upstream.
func StopTask(store *TombstoneStore, taskID string) error {
	if store == nil {
		return nil
	}
	if err := store.TryRemove(taskID); err != nil && !errors.Is(err, ErrTombstoneNotFound) {
		return err
	}
	return nil
}
