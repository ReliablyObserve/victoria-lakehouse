package vtstorageadapter

import (
	"context"
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
)

// traceIndexLookup is the optional capability we use from the underlying
// storage to bypass span scans on VT's trace-by-ID lookup. The Lakehouse
// parquets3 storage implements this; mock stores in tests do not, and the
// adapter falls back to the existing query-rewrite path in that case.
//
// Kept as a private interface so we don't pollute storage.Storage (shared
// with the logs module) with a trace-specific method.
type traceIndexLookup interface {
	LookupTraceIndex(ctx context.Context, traceID string) (startNs, endNs int64, found bool, err error)
}

// traceIndexLookupTraceID inspects q and returns the trace ID if and only
// if the query has VT's index-lookup shape:
//
//	{trace_id_idx_stream="<bucket>"} AND trace_id_idx:=<traceID> | stats min(_time) ..., min(start_time) ..., max(end_time) ...
//
// The rewriteTraceIndexQuery path further mutates this into a span-scan
// stats query; we want to catch it BEFORE that rewrite so the embedded
// `_trace_idx` footer index can answer without touching span data.
func traceIndexLookupTraceID(q *logstorage.Query) (string, bool) {
	if q == nil {
		return "", false
	}
	queryStr := q.String()
	if !strings.Contains(queryStr, otelpb.TraceIDIndexFieldName+`:=`) {
		return "", false
	}
	// Same parser used by rewriteTraceIndexQuery — keep the two detectors
	// behavior-identical so a query the rewrite would catch is also caught
	// here and vice versa.
	return extractTraceIDFromIndexQuery(queryStr), strings.Contains(queryStr, otelpb.TraceIDIndexFieldName+`:=`)
}

// emitTraceIndexBlock writes a single synthetic DataBlock that mirrors the
// stats-row VT's tempo handler expects from the trace_id_idx_stream
// lookup. The handler reads three columns (`_time`, `start_time`,
// `end_time`) and parses them as RFC3339 / decimal-nanos respectively
// (see VT vtselect/traces/tempo/query.go findTraceIDTimeSplitTimeRange).
//
// Using the same column shape as VT's local index lookup keeps the
// multilevel-select fan-out (vtselect → VT hot + LH cold) able to merge
// our result with VT's hot-tier index hits without special handling.
func emitTraceIndexBlock(writeBlock logstorage.WriteDataBlockFunc, startNs, endNs int64) {
	tsRFC := time.Unix(0, startNs).UTC().Format(time.RFC3339Nano)
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{tsRFC}},
		{Name: otelpb.TraceIDIndexStartTimeFieldName, Values: []string{strconv.FormatInt(startNs, 10)}},
		{Name: otelpb.TraceIDIndexEndTimeFieldName, Values: []string{strconv.FormatInt(endNs, 10)}},
	})
	writeBlock(0, db)
}
