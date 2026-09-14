package delete

import (
	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
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

func fieldsFromMap(m map[string]string) []logstorage.Field {
	fields := make([]logstorage.Field, 0, len(m))
	for k, v := range m {
		fields = append(fields, logstorage.Field{Name: k, Value: v})
	}
	return fields
}
