package vtstorageadapter

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/tracesidx"
)

// skipWhenIdxTable skips a test of the shortcuts the adapter takes without the
// trace_id_idx table (the `_trace_idx` footer fast path, the span-scan rewrite, the
// stripped index stream). With the table on (tracesidx.Enabled) VictoriaTraces' own
// queries run unchanged and TestIdxTable_* in idx_table_test.go pin that instead;
// the shortcuts and their tests stay for the switch's other position.
func skipWhenIdxTable(t *testing.T) {
	t.Helper()
	if tracesidx.Enabled {
		t.Skip("replaced by TestIdxTable_*: the trace_id_idx table is on, the adapter takes no shortcut")
	}
}
