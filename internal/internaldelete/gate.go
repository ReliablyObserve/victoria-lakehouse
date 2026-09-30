// Package internaldelete holds the lakehouse part of serving the cluster delete
// protocol (/internal/delete/*).
//
// The protocol itself, and upstream's own gate in front of it
// (-internaldelete.enable, default false), come from upstream: the logs binary
// mounts vlselect.RequestHandler for the prefix, so the flag, its help text, the
// "disabled" answer and the dispatch into internalselect are VictoriaLogs' code.
// The traces binary does the same with a minimal copy until it can mount
// vtselect.RequestHandler (see lakehouse-traces/internal_delete.go).
//
// What the lakehouse adds is one requirement, checked after upstream's flag: the
// delete protocol writes into the lakehouse tombstone store, so it also needs
// the delete feature (delete.enabled in the lakehouse config) to be on.
//
// Upstream's public delete API (/delete/run_task, /delete/stop_task,
// /delete/active_tasks, behind -delete.enable) is served the same way and gets
// the same requirement: see PublicHandler.
package internaldelete

import (
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
	"github.com/VictoriaMetrics/metrics"
)

// FlagName is upstream's flag for the protocol. The flag is registered by
// upstream's vlselect package (logs) or by lakehouse-traces (traces), never here.
const FlagName = "internaldelete.enable"

// DeleteDisabledMessage is the lakehouse's answer when upstream's flag is on but
// the delete feature the protocol writes into is off.
const DeleteDisabledMessage = "requests to /internal/delete/* need the lakehouse delete feature; set delete.enabled: true in the lakehouse config"

// FlagEnabled reports upstream's -internaldelete.enable as registered in this
// binary. A binary that has not registered the flag reports false.
func FlagEnabled() bool {
	return flagIsTrue(FlagName)
}

// PublicFlagName is upstream's flag for the public delete API (/delete/*). It
// is registered by upstream's vlselect package (logs) or by lakehouse-traces
// (traces), never here. It is not the lakehouse delete.enabled setting, which
// governs the lakehouse's own delete API (/delete/logsql/*,
// /delete/tracessql/*) and the tombstone machinery both APIs write into.
const PublicFlagName = "delete.enable"

// PublicDeleteDisabledMessage is the lakehouse's answer when upstream's
// -delete.enable is on but the delete feature is off.
const PublicDeleteDisabledMessage = "requests to /delete/* need the lakehouse delete feature; set delete.enabled: true in the lakehouse config"

// PublicFlagEnabled reports upstream's -delete.enable as registered in this
// binary. A binary that has not registered the flag reports false.
func PublicFlagEnabled() bool {
	return flagIsTrue(PublicFlagName)
}

func flagIsTrue(name string) bool {
	f := flag.Lookup(name)
	return f != nil && f.Value.String() == "true"
}

// Handler wraps upstream, the handler that owns /internal/delete/* (upstream's
// flag check followed by the protocol). flagOn reports upstream's flag and
// deleteEnabled is the lakehouse delete.enabled setting.
//
// Upstream's flag is consulted first: while it is off, the request goes to
// upstream untouched and gets upstream's own "disabled" answer, whatever
// delete.enabled says. Only a request upstream would serve is refused here when
// the delete feature is off.
func Handler(flagOn func() bool, deleteEnabled bool, upstream http.HandlerFunc) http.HandlerFunc {
	return gate(flagOn, deleteEnabled, DeleteDisabledMessage, upstream)
}

// PublicHandler is Handler for upstream's public delete API: upstream, the
// handler owning /delete/* (upstream's -delete.enable check followed by its
// delete handler), is refused only when upstream would serve the request and
// the lakehouse delete feature is off.
func PublicHandler(flagOn func() bool, deleteEnabled bool, upstream http.HandlerFunc) http.HandlerFunc {
	return gate(flagOn, deleteEnabled, PublicDeleteDisabledMessage, upstream)
}

func gate(flagOn func() bool, deleteEnabled bool, message string, upstream http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if flagOn() && !deleteEnabled {
			httpserver.Errorf(w, r, "%s", message)
			return
		}
		upstream(w, r)
	}
}

// RunTaskPath is the public delete API path that starts a delete task.
const RunTaskPath = "/delete/run_task"

// RunTaskRequestsCounter is VictoriaLogs' request counter for RunTaskPath.
const RunTaskRequestsCounter = `vl_http_requests_total{path="/delete/run_task"}`

// RunTaskPOSTOnly wraps next, the handler owning /delete/* (upstream's
// -delete.enable check followed by its delete handler), so that
// /delete/run_task answers 405 unless the request is a POST. A GET, HEAD, PUT
// or DELETE can then never start a delete task, for example one forged through
// SSRF.
//
// VictoriaLogs master (app/vlselect/main.go processDeleteRunTaskRequest) and
// VictoriaTraces v0.12.0 (issue #225) refuse non-POST requests there, with
// exactly this answer, after the -delete.enable check and before parsing the
// tenant or the filter. VictoriaLogs v1.52.0, the pin of the logs binary, has
// no such check, so this wrapper adds it. Once the pin includes the upstream
// check this becomes redundant but harmless
// (TestUpstreamRunTaskStillLacksMethodCheck fails at that point so the
// duplicate gets dropped).
//
// Only run_task is affected, as upstream: stop_task and active_tasks are
// unchanged. flagOn reports upstream's -delete.enable: while it is off the
// request goes to next untouched and gets upstream's own "disabled" answer,
// whatever the method, so the observable order matches upstream.
func RunTaskPOSTOnly(flagOn func() bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if flagOn() && strings.ReplaceAll(r.URL.Path, "//", "/") == RunTaskPath && r.Method != http.MethodPost {
			// VictoriaLogs master counts the request before refusing it. The
			// counter is upstream's own (same name in the default set;
			// GetOrCreateCounter returns the instance vlselect registered).
			metrics.GetOrCreateCounter(RunTaskRequestsCounter).Inc()
			http.Error(w, fmt.Sprintf("Only POST method is allowed; got %s.", r.Method), http.StatusMethodNotAllowed)
			return
		}
		next(w, r)
	}
}

// POSTOnly wraps next, the handler owning a cluster-protocol prefix
// (/internal/select/*, /internal/delete/*), so that any method but POST is
// answered with a bare 405, no body, exactly as upstream does first thing in
// its internalselect.RequestHandler (VictoriaTraces v0.12.0, VictoriaLogs
// master, issues #1635 and #1716). Every upstream client of the protocol
// (netselect) sends POST, so nothing legitimate is refused; a GET can no
// longer run a delete task or read data through a forged request.
//
// gate reports whether upstream's own gate for the prefix is open (for
// /internal/delete/* upstream's -internaldelete.enable): while it is closed the
// request goes to next untouched and gets upstream's own "disabled" answer,
// whatever the method, so the observable order matches upstream. A nil gate is
// always open (/internal/select/* has no enable flag).
//
// The v1.52.0 pin of VictoriaLogs, and the VictoriaLogs revision the traces
// binary embeds, lack the check; once they include it this is redundant but
// harmless (the drift guards fail at that point).
func POSTOnly(gate func() bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost && (gate == nil || gate()) {
			w.WriteHeader(http.StatusMethodNotAllowed)
			return
		}
		next(w, r)
	}
}
