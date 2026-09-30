package main

import (
	"flag"
	"net/http"

	"github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/internalselect"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/httpserver"
)

// upstream-copy: VictoriaTraces app/vtselect/main.go — the -internaldelete.enable
// flag and the "/internal/delete/" branch of RequestHandler, verbatim
// (internal_delete_test.go fails when the vendored source stops matching).
//
// The logs binary mounts vlselect.RequestHandler instead of copying anything.
// The traces binary serves /select/logsql/* with VictoriaTraces' own
// app/vtselect/logsql handlers, but not vtselect.RequestHandler: it serves
// /internal/select/* and /internal/delete/* with VictoriaLogs' internalselect,
// because VT's vtstorage does not route delete calls to external storage, and VT's
// and VL's internalselect register the same flag (-internalselect.maxConcurrentRequests),
// so both cannot be linked into one binary. This file is replaced by
// vtselect.RequestHandler if that ever changes.
var internalDeleteEnable = flag.Bool("internaldelete.enable", false, "Whether to enable /internal/delete/* HTTP endpoints, which are used by vtselect for deleting spans "+
	"via delete API at vtstorage nodes")

const internalDeleteDisabledMessage = "requests to /internal/delete/* are disabled; pass -internaldelete.enable command-line flag for enabling them; " +
	"see https://docs.victoriametrics.com/victorialogs/#how-to-delete-logs"

func upstreamInternalDelete(w http.ResponseWriter, r *http.Request) {
	if !*internalDeleteEnable {
		httpserver.Errorf(w, r, "%s", internalDeleteDisabledMessage)
		return
	}
	internalselect.RequestHandler(r.Context(), w, r)
}
