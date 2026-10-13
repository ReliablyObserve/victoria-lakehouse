package delete

import (
	"fmt"
	"strconv"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

// LogRowFields renders a LogRow as the []logstorage.Field shape VL filters
// evaluate against. Exported so the compactor can apply the SAME tombstone
// predicate the rewriter and the query path use — three places deciding
// independently which rows a delete covers is how a row ends up hidden by one
// path and copied forward by another.
func LogRowFields(row *schema.LogRow) []logstorage.Field {
	m := logRowToMap(row)
	return fieldsFromMap(m)
}

// TraceRowFields is LogRowFields for spans.
func TraceRowFields(row *schema.TraceRow) []logstorage.Field {
	m := traceRowToMap(row)
	return fieldsFromMap(m)
}

// IdxRowFields renders a trace_id_idx row as the fields VictoriaLogs sees for it:
// upstream's field names, _stream and _msg. _stream_id is left out: it depends on the
// tenant, and a tombstone is applied to objects of its own tenant only.
func IdxRowFields(row *tracesidx.Row) []logstorage.Field {
	return []logstorage.Field{
		{Name: "_time", Value: time.Unix(0, row.Time).UTC().Format(time.RFC3339Nano)},
		{Name: "_stream", Value: fmt.Sprintf(tracesidx.StreamTagsFmt, row.Stream)},
		{Name: "_msg", Value: tracesidx.MessageValue},
		{Name: tracesidx.StreamField, Value: strconv.Itoa(int(row.Stream))},
		{Name: tracesidx.IDField, Value: row.TraceID},
		{Name: tracesidx.StartField, Value: strconv.FormatInt(row.Start, 10)},
		{Name: tracesidx.EndField, Value: strconv.FormatInt(row.End, 10)},
		{Name: tracesidx.DurationField, Value: strconv.FormatInt(row.Duration, 10)},
	}
}

func fieldsFromMap(m map[string]string) []logstorage.Field {
	fields := make([]logstorage.Field, 0, len(m))
	for k, v := range m {
		fields = append(fields, logstorage.Field{Name: k, Value: v})
	}
	return fields
}
