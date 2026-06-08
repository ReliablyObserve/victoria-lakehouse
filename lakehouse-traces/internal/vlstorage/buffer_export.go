package vlstorage

import (
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// DataBlockToTraceRows reconstructs schema.TraceRow values from a DataBlock
// emitted by the Option B buffer's RunQuery. It is the read side of the future
// "Parquet from the buffer via RunQuery export" (P5): the buffer is queried with
// `*` over a flush window, and the resulting DataBlocks are turned back into the
// exact TraceRow shape the legacy insert path produced — so the Parquet a buffer
// flush writes is byte-for-byte what the legacy []TraceRow path would have
// written.
//
// Parity is achieved by REUSING the insert field-mapping (mapFieldToTraceRow)
// for every non-special column, exactly as logRowsToTraceRows does, plus the
// special VL columns:
//   - _stream     → row.Stream    (already the human-readable StreamTags string)
//   - _stream_id  → row.StreamID  (VL's native id == computeStreamID, verified)
//   - tenant      → AccountID/ProjectID (the query is per-tenant; no tenant col)
//
// TimestampUnixNano comes from start_time_unix_nano (full nanoseconds) because
// VL's _time column is formatted at microsecond precision; for OTLP spans the
// row timestamp equals the span start, so this preserves the legacy value
// without precision loss. Falls back to _time only when start is absent.
func DataBlockToTraceRows(db *logstorage.DataBlock, tenant logstorage.TenantID) []schema.TraceRow {
	if db == nil {
		return nil
	}
	cols := db.GetColumns(false)
	n := db.RowsCount()
	if n == 0 {
		return nil
	}

	rows := make([]schema.TraceRow, 0, n)
	for i := 0; i < n; i++ {
		row := schema.TraceRow{
			AccountID: tenant.AccountID,
			ProjectID: tenant.ProjectID,
		}
		for _, c := range cols {
			if i >= len(c.Values) {
				continue
			}
			// strings.Clone: c.Values aliases the DataBlock's pooled column memory,
			// which logstorage reuses across blocks. Without cloning, a row's string
			// fields read another row's bytes once the block is recycled — silently
			// corrupting flushed field values.
			v := strings.Clone(c.Values[i])
			switch c.Name {
			case "_msg":
				// empty for traces.
			case "_time":
				// _time IS the row's event timestamp (VL stores the span END
				// time here) and is what the legacy path (r.Timestamp) and hot VT
				// use for partitioning + the Parquet _time column. It MUST drive
				// TimestampUnixNano — NOT start_time_unix_nano, which is the span
				// START and differs by the span duration (and is absent on some
				// rows → 0 → dt=1970). Using start_time here scattered rows out of
				// the window collectWindow selected them by (selected by _time),
				// producing the per-window under-count.
				if v != "" {
					if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
						row.TimestampUnixNano = t.UnixNano()
					}
				}
			case "_stream":
				row.Stream = v
			case "_stream_id":
				row.StreamID = v
			default:
				// trace_id, span_id, name, service.name, k8s.*, duration_ns,
				// start_time_unix_nano (→ StartTimeUnixNano field, kept separate),
				// resource_attr:*, span_attr:*, … all go through the SAME mapper
				// the insert path uses.
				mapFieldToTraceRow(&row, c.Name, v)
			}
		}
		rows = append(rows, row)
	}
	return rows
}
