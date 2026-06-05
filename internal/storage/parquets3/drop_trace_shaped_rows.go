package parquets3

import (
	"strings"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// dropTraceShapedRows removes rows from db whose _stream tag is the
// shape VT emits for spans rather than what VL emits for logs. The
// check matches VL upstream's own write-time stream-fields enforcement,
// which guarantees VL hot tier never has these rows. Our cold tier
// historically accumulated some via a now-fixed ingest path (still
// being tracked as data-quality task #70), so this read-side filter
// keeps user-visible query results consistent with VL's behavior
// without surgically rewriting S3.
//
// The two trace-style markers we drop on:
//
//   - `resource_attr:` prefix on a stream tag key — VT's OTLP
//     protoparser builds canonical stream tags as
//     `{resource_attr:service.name="..."}`, which is structurally
//     impossible from VL's `_stream_fields` enforcement (VL uses
//     plain `service.name`, not the prefixed form).
//
//   - `name="<operation>"` as the stream's first tag — VT partitions
//     spans by operation name; logs never use this as a stream
//     dimension.
//
// Both signals are unambiguous: matching either means the row didn't
// come from VL's log-write pipeline, so it shouldn't surface as a log.
//
// If the input db has no _stream column at all (e.g. synthetic
// manifest blocks emitted by handle404Recovery), this is a no-op —
// those rows are by construction logs.
func dropTraceShapedRows(db *logstorage.DataBlock) *logstorage.DataBlock {
	if db == nil || db.RowsCount() == 0 {
		return db
	}

	columns := db.GetColumns(false)
	var streamCol *logstorage.BlockColumn
	for i := range columns {
		if columns[i].Name == "_stream" {
			streamCol = &columns[i]
			break
		}
	}
	if streamCol == nil {
		return db
	}

	// Build a mask of rows to KEEP. Rows whose _stream looks like a
	// VT span-shape are filtered out.
	rowCount := db.RowsCount()
	keep := make([]int, 0, rowCount)
	dropped := 0
	for i, val := range streamCol.Values {
		if isTraceShapedStream(val) {
			dropped++
			continue
		}
		keep = append(keep, i)
	}

	if dropped == 0 {
		return db
	}
	metrics.LogsTraceShapedRowsDropped.Add(dropped)

	if len(keep) == 0 {
		return nil
	}

	// Build a new DataBlock with the surviving rows projected from
	// every column. Allocates O(kept rows) — the dropped tail is
	// abandoned. We can't mutate the input db in place because
	// callers downstream don't expect SetColumns to shrink slices.
	newCols := make([]logstorage.BlockColumn, len(columns))
	for c := range columns {
		src := columns[c].Values
		out := make([]string, len(keep))
		for j, idx := range keep {
			out[j] = src[idx]
		}
		newCols[c] = logstorage.BlockColumn{
			Name:   columns[c].Name,
			Values: out,
		}
	}
	out := &logstorage.DataBlock{}
	out.SetColumns(newCols)
	return out
}

// isTraceShapedStream returns true when the canonical stream tag
// string was emitted by VT's span ingest path rather than VL's log
// ingest path. The two markers:
//
//	- contains `resource_attr:` (VT's prefixed resource attribute)
//	- begins with `{name="..."` (VT's per-operation stream partition)
//
// Both are absent from any legitimate log stream produced by VL —
// VL's _stream_fields enforcement strips prefixed keys and never
// uses `name` as a stream dimension.
func isTraceShapedStream(stream string) bool {
	if stream == "" {
		return false
	}
	if strings.Contains(stream, "resource_attr:") {
		return true
	}
	// Bare `name=` tag at start: `{name="HTTP GET /api/v1/users"`.
	// We don't match every occurrence of "name=" because legitimate
	// k8s.node.name=... would false-positive — we only care about
	// the bare key at the start of the stream brace.
	if strings.HasPrefix(stream, `{name="`) {
		return true
	}
	return false
}
