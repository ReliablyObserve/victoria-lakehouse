package vlstorage

import (
	"strconv"
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

// kindIdxMalformed is the lakehouse_vt_internal_rows_dropped_total kind of a
// trace_id_idx row that cannot be stored: no trace id or no usable time.
const kindIdxMalformed = "trace_id_idx_malformed"

func init() { metrics.VTInternalRowsDropped.Init(kindIdxMalformed) }

// IdxRowsOf returns the trace_id_idx rows of db: the rows of VictoriaTraces' index
// stream, as the table stores them. Every other row is ignored (DataBlockToTraceRows
// converts those). The values are copied out of the block.
//
// The field names are upstream's (otelpb.TraceIDIndex*); a test pins tracesidx's own
// copies to them.
func IdxRowsOf(db *logstorage.DataBlock) []tracesidx.Row {
	if db == nil || db.RowsCount() == 0 {
		return nil
	}
	cols := db.GetColumns(false)
	var stream, tm, id, start, end, dur []string
	for _, c := range cols {
		switch c.Name {
		case "_stream":
			stream = c.Values
		case "_time":
			tm = c.Values
		case otelpb.TraceIDIndexFieldName:
			id = c.Values
		case otelpb.TraceIDIndexStartTimeFieldName:
			start = c.Values
		case otelpb.TraceIDIndexEndTimeFieldName:
			end = c.Values
		case otelpb.TraceIDIndexDuration:
			dur = c.Values
		}
	}
	if stream == nil || id == nil {
		return nil
	}
	var out []tracesidx.Row
	for i := range stream {
		if !isTraceIDIndexRow(stream[i]) {
			continue
		}
		r, ok := idxRowAt(i, stream, tm, id, start, end, dur)
		if !ok {
			metrics.VTInternalRowsDropped.Inc(kindIdxMalformed)
			continue
		}
		out = append(out, r)
	}
	return out
}

func at(vs []string, i int) string {
	if i < len(vs) {
		return vs[i]
	}
	return ""
}

func idxRowAt(i int, stream, tm, id, start, end, dur []string) (tracesidx.Row, bool) {
	traceID := strings.Clone(at(id, i))
	if traceID == "" {
		return tracesidx.Row{}, false
	}
	s, err := strconv.ParseInt(at(start, i), 10, 64)
	if err != nil {
		return tracesidx.Row{}, false
	}
	e, err := strconv.ParseInt(at(end, i), 10, 64)
	if err != nil {
		return tracesidx.Row{}, false
	}
	d, err := strconv.ParseInt(at(dur, i), 10, 64)
	if err != nil {
		d = e - s // upstream writes end-start; absent in rows of older writers
	}
	// _time is the start time upstream stamps the row with; the block's own _time is
	// the same instant (VictoriaLogs prints it at nanosecond precision).
	t := s
	if v := at(tm, i); v != "" {
		if p, err := time.Parse(time.RFC3339Nano, v); err == nil {
			t = p.UnixNano()
		}
	}
	return tracesidx.Row{Time: t, Stream: streamValueOf(at(stream, i), traceID), TraceID: traceID, Start: s, End: e, Duration: d}, true
}

// streamValueOf reads N out of {trace_id_idx_stream="N"}; when the stream text is
// not of that form it computes upstream's value from the id.
func streamValueOf(streamText, traceID string) int32 {
	const marker = otelpb.TraceIDIndexStreamName + `="`
	if i := strings.Index(streamText, marker); i >= 0 {
		rest := streamText[i+len(marker):]
		if j := strings.IndexByte(rest, '"'); j > 0 {
			if n, err := strconv.ParseInt(rest[:j], 10, 32); err == nil && n >= 0 {
				return int32(n)
			}
		}
	}
	return tracesidx.StreamOf(traceID)
}
