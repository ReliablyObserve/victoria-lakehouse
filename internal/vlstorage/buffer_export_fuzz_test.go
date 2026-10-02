package vlstorage

import (
	"fmt"
	"strings"
	"testing"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// FuzzDataBlockToLogRows fuzzes the conversion every Parquet row goes through:
// a block with arbitrary column names and values must convert without
// panicking, and the rows must not alias the block's memory (the engine reuses
// it) — overwriting the block afterwards must not change a row.
func FuzzDataBlockToLogRows(f *testing.F) {
	f.Add("level", "info", "service.name", "svc")
	f.Add("severity_number", "9", "_msg", "hello")
	f.Add("_time", "2026-01-02T03:04:05.123456789Z", "_stream", `{service.name="x",level="warn"}`)
	f.Add("_stream", `{broken`, "severity_number", "13")
	f.Add("", "ignored", "custom.tag", "val")
	f.Add("trace_id", "", "span_id", "")
	f.Add(strings.Repeat("x", 256), strings.Repeat("y", 1024), "_msg", "m")

	f.Fuzz(func(t *testing.T, n1, v1, n2, v2 string) {
		arena := []byte(v1 + v2)
		vals1 := unsafe.String(unsafe.SliceData(arena), len(v1))
		vals2 := unsafe.String(unsafe.SliceData(arena[len(v1):]), len(v2))
		db := &logstorage.DataBlock{}
		db.SetColumns([]logstorage.BlockColumn{
			{Name: n1, Values: []string{vals1}},
			{Name: n2, Values: []string{vals2}},
		})
		rows := DataBlockToLogRows(db, logstorage.TenantID{AccountID: 1, ProjectID: 2})
		before := fmt.Sprintf("%+v", rows)
		for i := range arena {
			arena[i] = 'X'
		}
		if after := fmt.Sprintf("%+v", rows); after != before {
			t.Fatalf("a converted row aliases the block's memory:\nbefore %s\nafter  %s", before, after)
		}
	})
}
