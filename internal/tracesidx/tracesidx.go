// Package tracesidx stores VictoriaTraces' own trace_id_idx rows in Parquet.
//
// VictoriaTraces writes, for every trace, one row into one of 1,024 log streams
// ({trace_id_idx_stream="N"}, N = xxhash64(trace_id) % 1024): fields trace_id_idx,
// start_time, end_time, duration, _msg "-". Trace-by-ID (Jaeger and Tempo), the
// Tempo duration search, the TraceQL duration join and the VTUI duration heatmap
// read those rows. The traces binary runs upstream's index worker and receives the
// rows; this package is the on-S3 form of them: a small Parquet table of its own,
// one set of objects per tenant and hour partition, next to the span objects:
//
//	<tenant>/traces/dt=D/hour=H/<id>.parquet        spans
//	<tenant>/traces_idx/dt=D/hour=H/<id>.parquet    trace_id_idx rows (this package)
//
// Field names and values are upstream's, row for row (the read side derives _stream,
// _stream_id and _msg with upstream's rules), so an export of the rows equals hot
// VictoriaTraces' export of the same index stream.
//
// The table is sorted by trace_id_idx alone (trace_id_idx_stream is an ordinary column,
// so VictoriaTraces' stream filter is evaluated as a plain filter, and min/max of the id
// per object and per row group are monotonic fences that every engine's zone maps use).
// A lookup of one trace is the footer, then in the one candidate row group the column
// and offset index slices of the id column and the one data page that can hold the id:
// a handful of ranged reads that do not grow with the number of rows (see Locator).
//
// Every object is plain Parquet: any engine reads it, and pushes
// `trace_id_idx = '...'` down to the standard split-block bloom filter, which is
// sized by the distinct values of its row group and is a power of two.
package tracesidx

import (
	"strings"

	"github.com/cespare/xxhash/v2"
)

// Enabled is the spike's build-time switch. true: the traces binary stores the
// trace_id_idx rows upstream writes and serves VictoriaTraces' stage-1 query from
// them. false: the rows are dropped at the flush and the stage-1 query is answered
// from the per-file `_trace_idx` footer entry, as on main. The final design has no
// switch (the footer entry goes away).
const Enabled = true

const (
	// Signal is the key directory of the idx objects, next to "traces".
	Signal = "traces_idx"

	// FormatKey is the footer key/value entry naming the table's layout. ASCII.
	FormatKey = "lh.trace_idx.format"
	// FormatValue is its value; the leading number is the layout version.
	FormatValue = "1 sort=trace_id_idx rows_per_group=250000 page_bytes=16384"

	// Partitions is VictoriaTraces' TraceIDIndexPartitionCount.
	Partitions = 1024

	// RowsPerGroup is the row-group size. Measured: 250k rows with 16 KiB pages
	// is the smallest lookup (about 91 KB read per lookup at 10 M rows).
	RowsPerGroup = 250_000
	// PageBytes is the data page size.
	PageBytes = 16 << 10
	// ColumnIndexLimit bounds the min/max values the column index keeps per page.
	// 32 bytes holds a whole 32-hex-character trace id.
	ColumnIndexLimit = 32

	// BloomBitsPerValue is the split-block filter's bits per distinct value
	// (10.5 gives about 1 % false positives).
	BloomBitsPerValue = 10.5
)

// Upstream's field names (VictoriaTraces lib/protoparser/opentelemetry/pb
// internal_fields.go); duplicated as constants so this package stays free of the
// VictoriaTraces and VictoriaLogs imports, which differ per module pin. A test in
// the traces module pins them to the upstream constants.
const (
	StreamField    = "trace_id_idx_stream"
	IDField        = "trace_id_idx"
	StartField     = "start_time"
	EndField       = "end_time"
	DurationField  = "duration"
	TimeField      = "_time"
	MessageValue   = "-"
	StreamTagsFmt  = `{trace_id_idx_stream="%d"}`
	CanonicalIDLen = 32
)

// Row is one trace_id_idx row. The Parquet column names are upstream's field
// names, so an engine reading the table sees VictoriaTraces' own fields.
type Row struct {
	// Time is the row's _time in Unix nanoseconds: the trace's start time.
	Time int64 `parquet:"_time,delta"`
	// Stream is the value of the trace_id_idx_stream stream tag: xxhash64(id) % 1024.
	Stream int32 `parquet:"trace_id_idx_stream"`
	// TraceID is the trace_id_idx field.
	TraceID string `parquet:"trace_id_idx"`
	// Start, End and Duration are the start_time, end_time and duration fields
	// (decimal nanoseconds in the row export).
	Start    int64 `parquet:"start_time,delta"`
	End      int64 `parquet:"end_time,delta"`
	Duration int64 `parquet:"duration,delta"`
}

// StreamOf returns upstream's stream value for a trace id.
func StreamOf(traceID string) int32 {
	return int32(xxhash.Sum64String(traceID) % Partitions)
}

// IsKey reports whether an object key names an idx object. The kind is derived
// from the key alone, so objects adopted from a listing or after a restart need
// no metadata.
func IsKey(key string) bool {
	return strings.Contains(key, "/"+Signal+"/") || strings.HasPrefix(key, Signal+"/")
}

// PrefixFor turns a span key prefix ("<tenant>/traces/") into the idx prefix
// ("<tenant>/traces_idx/").
func PrefixFor(spanPrefix string) string {
	if p, ok := strings.CutSuffix(spanPrefix, "traces/"); ok {
		return p + Signal + "/"
	}
	if spanPrefix != "" && !strings.HasSuffix(spanPrefix, "/") {
		spanPrefix += "/"
	}
	return spanPrefix + Signal + "/"
}

// RawBytesPerRow is the raw (decoded) size the stats count per row: the 32-byte id
// and four 8-byte numbers plus the stream.
const RawBytesPerRow = 32 + 4*8 + 4
