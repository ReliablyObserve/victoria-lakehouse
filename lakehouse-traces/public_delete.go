package main

import (
	"context"
	"flag"
	"fmt"
	"net/http"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/metrics"
)

// upstream-copy: VictoriaTraces app/vtselect/main.go — the -delete.enable flag
// and the "/delete/" branch of RequestHandler — and app/vtselect/logsql.go —
// the /delete/* request counters, deleteHandler and
// processDelete{RunTask,StopTask,ActiveTasks}Request — verbatim
// (public_delete_test.go fails when the vendored source stops matching).
//
// upstream-divergence: the three storage calls go to VictoriaLogs'
// app/vlstorage, which this binary routes into the lakehouse storage (the same
// dispatch /internal/delete/* reaches), instead of VT's app/vtstorage, whose
// delete functions are not routed to external storage; and the response writes
// discard fmt.Fprintf's result explicitly (`_, _ =`) for this repository's
// errcheck. The request handling, flag, answers and metrics are VT's.
//
// The logs binary mounts vlselect.RequestHandler instead of copying anything.
// This binary cannot import vtselect yet (see internal_delete.go); once it
// serves /select/* and /internal/* through vtselect too, this file is replaced
// by vtselect.RequestHandler.
var deleteEnable = flag.Bool("delete.enable", false, "Whether to enable /delete/* HTTP endpoints")

const deleteDisabledMessage = "requests to /delete/* are disabled; pass -delete.enable command-line flag for enabling them"

var (
	deleteRunTaskRequests     = metrics.NewCounter(`vt_http_requests_total{path="/delete/run_task"}`)
	deleteStopTaskRequests    = metrics.NewCounter(`vt_http_requests_total{path="/delete/stop_task"}`)
	deleteActiveTasksRequests = metrics.NewCounter(`vt_http_requests_total{path="/delete/active_tasks"}`)
)

// upstreamPublicDelete is VT's RequestHandler for the /delete/ prefix.
func upstreamPublicDelete(w http.ResponseWriter, r *http.Request) {
	path := strings.ReplaceAll(r.URL.Path, "//", "/")
	if !*deleteEnable {
		httpserver.Errorf(w, r, "%s", deleteDisabledMessage)
		return
	}
	deleteHandler(w, r, path)
}

func deleteHandler(w http.ResponseWriter, r *http.Request, path string) {
	ctx := r.Context()

	switch path {
	case "/delete/run_task":
		deleteRunTaskRequests.Inc()
		processDeleteRunTaskRequest(ctx, w, r)
	case "/delete/stop_task":
		deleteStopTaskRequests.Inc()
		processDeleteStopTaskRequest(ctx, w, r)
	case "/delete/active_tasks":
		deleteActiveTasksRequests.Inc()
		processDeleteActiveTasksRequest(ctx, w, r)
	default:
		httpserver.Errorf(w, r, "unsupported path requested: %q", path)
	}
}

func processDeleteRunTaskRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	tenantID, err := logstorage.GetTenantIDFromRequest(r)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain tenantID: %s", err)
		return
	}

	fStr := r.FormValue("filter")
	f, err := logstorage.ParseFilter(fStr)
	if err != nil {
		httpserver.Errorf(w, r, "cannot parse filter [%s]: %s", fStr, err)
		return
	}

	// Generate taskID from the current timestamp in nanoseconds
	timestamp := time.Now().UnixNano()
	taskID := fmt.Sprintf("%d", timestamp)

	tenantIDs := []logstorage.TenantID{tenantID}
	if err := vlstorage.DeleteRunTask(ctx, taskID, timestamp, tenantIDs, f); err != nil {
		httpserver.Errorf(w, r, "cannot run delete task: %s", err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"task_id":%q}`, taskID)
}

func processDeleteStopTaskRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	taskID := r.FormValue("task_id")
	if taskID == "" {
		httpserver.Errorf(w, r, "missing task_id arg")
		return
	}

	if err := vlstorage.DeleteStopTask(ctx, taskID); err != nil {
		httpserver.Errorf(w, r, "cannot stop task with task_id=%q: %s", taskID, err)
		return
	}

	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, `{"status":"ok"}`)
}

func processDeleteActiveTasksRequest(ctx context.Context, w http.ResponseWriter, r *http.Request) {
	tasks, err := vlstorage.DeleteActiveTasks(ctx)
	if err != nil {
		httpserver.Errorf(w, r, "cannot obtain active delete tasks: %s", err)
		return
	}

	data := logstorage.MarshalDeleteTasksToJSON(tasks)

	w.Header().Set("Content-Type", "application/json")
	_, _ = fmt.Fprintf(w, "%s", data)
}
