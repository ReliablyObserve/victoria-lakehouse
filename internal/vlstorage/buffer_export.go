package vlstorage

import (
	"strings"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// DataBlockToLogRows reconstructs schema.LogRow values from a DataBlock emitted
// by the insert buffer's RunQuery. The flusher queries a buffer segment with `*`
// over one group's time slice and turns the resulting DataBlocks into the
// LogRows its Parquet file holds.
//
// Every non-special column goes through the insert field mapping
// (mapFieldToRow); the special VictoriaLogs columns are handled here:
//   - _msg        → row.Body
//   - _stream     → row.Stream     (human-readable StreamTags string)
//   - _stream_id  → row.StreamID   (VL's native id == computeStreamID, verified)
//   - _time       → row.TimestampUnixNano
//   - tenant      → AccountID/ProjectID (the query is per-tenant; no tenant col)
//
// The severity text is derived last (explicit text, else from severity_number,
// else the stream's level tag), as the compactor does when it backfills.
//
// Unlike traces (which recover full nanoseconds from start_time_unix_nano), logs
// have no separate nanosecond field — the timestamp comes from VL's _time
// column. VL formats _time at microsecond precision, so a sub-microsecond
// ingest timestamp is truncated here. For OTLP/syslog logs the source timestamp
// is microsecond-or-coarser in practice.
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
			// strings.Clone: c.Values points into the DataBlock's pooled column
			// memory, which logstorage REUSES across blocks. Without cloning, a
			// reconstructed row's string fields alias that buffer and read another
			// row's bytes once the block is recycled — a heisenbug that silently
			// corrupts flushed field values (and flaked the gate-filter test).
			v := strings.Clone(c.Values[i])
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
				if v == "" {
					// A block lists every column any of its rows has; a row without
					// the field reads "" there. Upstream ignores empty-valued fields
					// at ingest, so "" is an absent field, never a stored one.
					continue
				}
				mapFieldToRow(&row, c.Name, v)
			}
		}
		// Same derivation step as the insert path had and the compactor's backfill
		// has: an explicit severity text, else the one derived from
		// severity_number, else the stream's `level` tag. OTLP rows often carry
		// only the number.
		var st *logstorage.StreamTags
		if row.Stream != "" {
			st = logstorage.GetStreamTags()
			if err := st.UnmarshalString(row.Stream); err != nil {
				logstorage.PutStreamTags(st)
				st = nil
			}
		}
		row.SeverityText = schema.DeriveSeverityText(row.SeverityText, row.SeverityNumber, st)
		if st != nil {
			logstorage.PutStreamTags(st)
		}
		rows = append(rows, row)
	}
	return rows
}
