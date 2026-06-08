package vlstorage

import (
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// DataBlockToLogRows reconstructs schema.LogRow values from a DataBlock emitted
// by the Option B buffer's RunQuery. It is the read side of the WAL-cutover:
// the buffer is queried with `*` over a flush window, and the resulting
// DataBlocks are turned back into the exact LogRow shape the legacy insert path
// produced — so the Parquet a buffer flush writes is byte-for-byte what the
// legacy []LogRow path would have written.
//
// Parity is achieved by REUSING the insert field-mapping (mapFieldToRow) for
// every non-special column, exactly as logRowsToSchemaRows does, plus the
// special VL columns:
//   - _msg        → row.Body
//   - _stream     → row.Stream     (human-readable StreamTags string)
//   - _stream_id  → row.StreamID   (VL's native id == computeStreamID, verified)
//   - _time       → row.TimestampUnixNano
//   - tenant      → AccountID/ProjectID (the query is per-tenant; no tenant col)
//
// Unlike traces (which recover full nanoseconds from start_time_unix_nano), logs
// have no separate nanosecond field — the timestamp comes from VL's _time
// column. VL formats _time at microsecond precision, so a sub-microsecond
// ingest timestamp is truncated here. For OTLP/syslog logs the source timestamp
// is microsecond-or-coarser in practice, so this matches; the parity harness
// quantifies any residual delta.
func DataBlockToLogRows(db *logstorage.DataBlock, tenant logstorage.TenantID) []schema.LogRow {
	if db == nil {
		return nil
	}
	cols := db.GetColumns(false)
	n := db.RowsCount()
	if n == 0 {
		return nil
	}

	rows := make([]schema.LogRow, 0, n)
	for i := 0; i < n; i++ {
		row := schema.LogRow{
			AccountID: tenant.AccountID,
			ProjectID: tenant.ProjectID,
		}
		for _, c := range cols {
			if i >= len(c.Values) {
				continue
			}
			v := c.Values[i]
			switch c.Name {
			case "_msg":
				// VL stores the message under the empty field name internally,
				// but the DataBlock surfaces it as the "_msg" column. The insert
				// mapper (mapFieldToRow) keys Body off the empty name, so map it
				// here explicitly rather than routing through the mapper.
				row.Body = v
			case "_stream":
				row.Stream = v
			case "_stream_id":
				row.StreamID = v
			case "_time":
				if v != "" {
					if t, err := time.Parse(time.RFC3339Nano, v); err == nil {
						row.TimestampUnixNano = t.UnixNano()
					}
				}
			default:
				// level, service.name, k8s.*, trace_id, span_id, resource_attr:*,
				// log_attr:*, … all go through the SAME mapper the insert path
				// uses.
				mapFieldToRow(&row, c.Name, v)
			}
		}
		rows = append(rows, row)
	}
	return rows
}
