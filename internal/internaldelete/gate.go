// Package internaldelete gates the cluster delete protocol (/internal/delete/*)
// the way VictoriaLogs and VictoriaTraces do.
//
// Upstream serves /internal/delete/* only when -internaldelete.enable is set
// (default false) and answers every other request with a fixed error; see
// app/vlselect/main.go and app/vtselect/main.go (RequestHandler, the
// "/internal/delete/" branch). The lakehouse binaries mount VL's internalselect
// handler directly on their own mux, so that check has to be repeated here, in
// the same order: the flag first, then the protocol handler.
//
// The lakehouse adds one requirement of its own: the delete protocol writes
// into the lakehouse tombstone store, so it also needs the delete feature
// (delete.enabled in the lakehouse config) to be on.
package internaldelete

import (
	"errors"
	"flag"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
)

// Same name, default and help as upstream's flag, so a VL/VT command line
// carries over unchanged.
var enable = flag.Bool("internaldelete.enable", false, "Whether to enable /internal/delete/* HTTP endpoints, which are used by vlselect for deleting logs "+
	"via delete API at vlstorage nodes; see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs")

// DisabledMessage is upstream's answer to /internal/delete/* while the flag is off.
const DisabledMessage = "requests to /internal/delete/* are disabled; pass -internaldelete.enable command-line flag for enabling them; " +
	"see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs"

// DeleteDisabledMessage is the lakehouse's answer when the flag is on but the
// delete feature the protocol writes into is off.
const DeleteDisabledMessage = "requests to /internal/delete/* need the lakehouse delete feature; set delete.enabled: true in the lakehouse config"

// ErrRunTaskNotTenantScoped refuses /internal/delete/run_task. Upstream applies
// a delete task only to the request's tenant_ids, but lakehouse tombstones are
// instance-wide: honouring the task would hide matching rows of every tenant.
// Until tombstones carry a tenant scope the task is refused rather than widened.
var ErrRunTaskNotTenantScoped = errors.New("/internal/delete/run_task is not supported yet: lakehouse tombstones are instance-wide " +
	"and cannot be limited to the requested tenant_ids")

// Enabled reports the -internaldelete.enable flag.
func Enabled() bool {
	return *enable
}

// Handler guards next, the upstream internal protocol handler, for the
// /internal/delete/ prefix. enabled is the -internaldelete.enable flag and
// deleteEnabled the lakehouse delete.enabled setting; both are taken as
// arguments so the gate is testable without touching global flag state.
func Handler(enabled, deleteEnabled bool, next http.HandlerFunc) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		if !enabled {
			httpserver.Errorf(w, r, "%s", DisabledMessage)
			return
		}
		if !deleteEnabled {
			httpserver.Errorf(w, r, "%s", DeleteDisabledMessage)
			return
		}
		next(w, r)
	}
}
