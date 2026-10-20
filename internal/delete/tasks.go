package delete

import (
	"context"
	"errors"
	"fmt"
	"math"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// Delete tasks: upstream's delete API (/delete/run_task and the cluster
// protocol's /internal/delete/run_task) mapped onto tombstones.
//
// Upstream registers a task per call — task_id, the tenants it names, a LogsQL
// filter, and the timestamp it was issued at — and deletes the rows of exactly
// those tenants that match the filter and are not newer than the timestamp
// (lib/logstorage/storage.go: DeleteRunTask, processDeleteTask). The lakehouse
// registers the same task as a tombstone with that id, scoped to those tenants,
// covering (-inf, timestamp]. A task is complete once it is registered: it is
// durable in S3 and in force on the pod that took it, and every other pod learns
// it within a poll interval. So active_tasks lists nothing (upstream's list holds
// only pending and running tasks, which have finished about a second after
// run_task), and stop_task is a no-op, as stopping a finished upstream task is.
//
// The public API (/delete/*) registers a task for the request's tenant; the
// cluster protocol (/internal/delete/*) carries its tenants in the request, as
// upstream: it is the storage-node side of a vlselect/vtselect fan-out.

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
// The tombstone's definition key is derived from the task id (upstream's id is the
// task's timestamp in nanoseconds) and its bytes hold nothing that differs between
// nodes, so the vlselect/vtselect fan-out of one task to several Lakehouse nodes
// writes the same key with the same bytes: the first create wins and the others
// find it and succeed. A task id that is already registered with different content
// is refused with upstream's error, whichever pod it reaches.
//
// A task that names no tenant deletes nothing upstream — its search has no
// tenant to match — and is accepted. It is accepted here too and creates
// nothing: an empty tenant list on a tombstone would mean "every tenant".
func RunTask(ctx context.Context, store *TombstoneStore, accountOnlyKeys bool, taskID string, timestamp int64, tenants []TenantRef, filter, mode string) error {
	if store == nil {
		return fmt.Errorf("the lakehouse delete feature is disabled; set delete.enabled: true in the lakehouse config")
	}
	if err := CheckTenantsForKeyLayout(tenants, accountOnlyKeys); err != nil {
		return err
	}
	tenants = NormalizeTenants(tenants)
	if len(tenants) == 0 {
		logger.Infof("delete task names no tenant; nothing to delete; task_id=%q, filter=%q", taskID, filter)
		return nil
	}
	if mode == "" {
		mode = "hide"
	}
	ts := Tombstone{
		ID:       taskID,
		Query:    filter,
		StartNs:  math.MinInt64,
		EndNs:    timestamp,
		Mode:     mode,
		Tenants:  tenants,
		FilterAt: timestamp,
	}
	if _, err := store.Create(ctx, ts); err != nil {
		var exists taskExistsError
		if errors.As(err, &exists) {
			return err
		}
		return fmt.Errorf("invalid delete task: %w", err)
	}
	logger.Infof("delete task registered as a tombstone; task_id=%q, tenants=%v, filter=%q, mode=%s",
		taskID, tenants, filter, mode)
	return nil
}

// ActiveTasks lists the delete tasks in progress, as upstream's DeleteTask shape.
// A Lakehouse task is complete once it is registered (see the package comment),
// so there are none: like upstream a second after run_task, the answer is an empty
// list.
func ActiveTasks(ctx context.Context, store *TombstoneStore) []*logstorage.DeleteTask {
	return []*logstorage.DeleteTask{}
}

// StopTask is a no-op that answers ok: a Lakehouse task is complete once it is
// registered, and stopping a finished or unknown upstream task answers ok and
// changes nothing. It never brings rows back (an un-delete is the lakehouse
// API's, and only for hide mode).
func StopTask(ctx context.Context, store *TombstoneStore, taskID string) error {
	return nil
}
