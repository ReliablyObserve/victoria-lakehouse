package main

import (
	"flag"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/selectapi"
)

// upstream-copy: VictoriaTraces app/vtselect/main.go — the -search.maxQueryDuration
// flag, verbatim (search_max_query_duration_test.go fails when the vendored
// source stops matching). VictoriaTraces registers it in vtselect, which this
// binary cannot link (see internal_delete.go), so it is registered here, with
// upstream's name, default and help. It is the deadline of every Jaeger and
// Tempo request, as in vtselect.RequestHandler, lowered per request by the
// 'timeout' arg (selectapi/query_deadline.go). The LogsQL routes of this binary
// keep their own deadline, query.timeout.
var searchMaxQueryDuration = flag.Duration("search.maxQueryDuration", selectapi.DefaultMaxQueryDuration, "The maximum duration for query execution. It can be overridden to a smaller value on a per-query basis via 'timeout' query arg")

// newSelectHandler is the select API of this pod: LogsQL, Jaeger and Tempo, with
// the Jaeger and Tempo deadline taken from -search.maxQueryDuration.
func newSelectHandler(store storage.Storage, cfg *config.Config) *selectapi.Handler {
	return selectapi.NewHandler(store, cfg, selectapi.WithMaxQueryDuration(*searchMaxQueryDuration))
}
