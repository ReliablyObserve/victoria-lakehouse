package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

// idxRowsOfSpans derives one index row per trace the way upstream's index worker does:
// min start and max end of the trace's spans.
func idxRowsOfSpans(rows []schema.TraceRow) []tracesidx.Row {
	type bound struct{ s, e int64 }
	m := map[string]*bound{}
	for i := range rows {
		s := schema.Int64Value(rows[i].StartTimeUnixNano)
		e := s + schema.Int64Value(rows[i].DurationNs)
		if b, ok := m[rows[i].TraceID]; ok {
			b.s, b.e = min(b.s, s), max(b.e, e)
		} else {
			m[rows[i].TraceID] = &bound{s, e}
		}
	}
	out := make([]tracesidx.Row, 0, len(m))
	for id, b := range m {
		out = append(out, tracesidx.Row{Time: b.s, Stream: tracesidx.StreamOf(id), TraceID: id, Start: b.s, End: b.e, Duration: b.e - b.s})
	}
	return out
}

// BenchmarkFlushGroupWrite is the CPU, allocations and bytes of writing the objects of
// one flush group of 30,000 spans (5,000 traces): on main the span object (with the
// per-file _trace_idx footer entry); here the span object without it, plus the
// trace_id_idx object of the same group (index rows are derived outside the timer: the
// upstream index worker produces them).
func BenchmarkFlushGroupWrite(b *testing.B) {
	rows := traceRows(30000, rwHour)
	idx := idxRowsOfSpans(rows)
	b.ReportAllocs()
	b.ResetTimer()
	var total, idxTotal int
	for i := 0; i < b.N; i++ {
		r, err := writeTracesParquet(rows, 10000, 3)
		if err != nil {
			b.Fatal(err)
		}
		total = len(r.Data)
		if tracesidx.Enabled {
			// The flush hands Marshal a slice of its own (the rows of the group), so the copy is part of the
			// work. (Stopping and starting the timer per iteration reads the memory statistics each time,
			// a stop-the-world that skews the timing of a 40 ms operation.)
			data, _, err := tracesidx.Marshal(append([]tracesidx.Row(nil), idx...), 3)
			if err != nil {
				b.Fatal(err)
			}
			idxTotal = len(data)
		}
	}
	b.ReportMetric(float64(total+idxTotal), "obj-bytes/op")
	b.ReportMetric(float64(idxTotal), "idx-bytes/op")
}
