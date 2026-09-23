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
package internaldelete

import (
	"flag"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
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
	f := flag.Lookup(FlagName)
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
	return func(w http.ResponseWriter, r *http.Request) {
		if flagOn() && !deleteEnabled {
			httpserver.Errorf(w, r, "%s", DeleteDisabledMessage)
			return
		}
		upstream(w, r)
	}
}
