package parquets3

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestDropTraceShapedRows_KeepsLogStreams verifies that legitimate log
// stream tags (the shape VL upstream's _stream_fields enforcement
// produces) survive the filter unchanged. If this test fails, a
// recent edit broke the "log row" classification heuristic and
// real log data is about to be dropped from query results.
func TestDropTraceShapedRows_KeepsLogStreams(t *testing.T) {
	cases := []string{
		// Canonical log stream — flat tag names, no prefix.
		`{service.name="api-gateway",k8s.namespace.name="prod",level="INFO"}`,
		// k8s.node.name contains the substring "name=" but is NOT a
		// trace stream — false-positive guard test.
		`{cloud.region="us-east-1",host.name="ip-10-0-1-42",k8s.node.name="node-pool-a-1",service.name="api-gateway"}`,
		// Empty stream — synthetic blocks from handle404Recovery and
		// similar. Must pass through unchanged.
		``,
	}
	for _, streamVal := range cases {
		t.Run(streamVal, func(t *testing.T) {
			db := blockWithStream([]string{streamVal})
			out := dropTraceShapedRows(db)
			if out == nil || out.RowsCount() != 1 {
				t.Errorf("legitimate log stream %q was dropped; want kept", streamVal)
			}
		})
	}
}

// TestDropTraceShapedRows_DropsTraceStreams verifies that VT-style
// stream tags get filtered out. Both markers should be caught:
//   - `resource_attr:` prefix anywhere in the stream
//   - `name="..."` as the stream's first tag
//
// These can never come from VL's log-write path; if a query result
// surfaces them, they were written via VT's OTLP protoparser to a
// path that landed in the logs storage — a pre-existing data quality
// issue (task #70) we mask at read time to keep tier-output parity.
func TestDropTraceShapedRows_DropsTraceStreams(t *testing.T) {
	cases := []string{
		// VT's prefixed resource attribute.
		`{resource_attr:service.name="api-gateway"}`,
		// VT's per-operation partition.
		`{name="HTTP GET /api/v1/users",resource_attr:service.name="user-service"}`,
		// Even just `name=...` at start without resource_attr is
		// still trace-shaped — VL never partitions logs on
		// operation name.
		`{name="DB INSERT orders"}`,
	}
	for _, streamVal := range cases {
		t.Run(streamVal, func(t *testing.T) {
			db := blockWithStream([]string{streamVal})
			out := dropTraceShapedRows(db)
			if out != nil && out.RowsCount() > 0 {
				t.Errorf("trace-shaped stream %q was kept; want dropped", streamVal)
			}
		})
	}
}

// TestDropTraceShapedRows_MixedBlock checks the partial-keep path —
// a block with both log and trace-shaped streams should emerge with
// only the log rows intact AND all other columns reduced to the same
// row count. A miscount here would corrupt downstream pipes.
func TestDropTraceShapedRows_MixedBlock(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{
			Name: "_stream",
			Values: []string{
				`{service.name="api-gateway",level="INFO"}`,          // keep
				`{name="HTTP GET",resource_attr:service.name="svc"}`, // drop
				`{service.name="user-service",level="WARN"}`,         // keep
				`{resource_attr:service.name="payment-service"}`,     // drop
			},
		},
		{
			Name:   "_msg",
			Values: []string{"hello", "", "world", ""},
		},
		{
			Name:   "trace_id",
			Values: []string{"abc", "def", "ghi", "jkl"},
		},
	})

	out := dropTraceShapedRows(db)
	if out == nil {
		t.Fatalf("output block is nil; expected 2 surviving rows")
	}
	if out.RowsCount() != 2 {
		t.Errorf("RowsCount = %d, want 2", out.RowsCount())
	}

	cols := out.GetColumns(false)
	if len(cols) != 3 {
		t.Fatalf("column count = %d, want 3", len(cols))
	}

	// Every column must have exactly RowsCount() values — any
	// mismatch corrupts downstream block decoding.
	for _, c := range cols {
		if len(c.Values) != out.RowsCount() {
			t.Errorf("column %q has %d values, want %d", c.Name, len(c.Values), out.RowsCount())
		}
	}

	// The two surviving rows are at original indices 0 and 2.
	var msgCol *logstorage.BlockColumn
	for i := range cols {
		if cols[i].Name == "_msg" {
			msgCol = &cols[i]
			break
		}
	}
	if msgCol == nil {
		t.Fatalf("missing _msg column after filter")
	}
	if msgCol.Values[0] != "hello" || msgCol.Values[1] != "world" {
		t.Errorf("surviving _msg values = %v, want [hello world]", msgCol.Values)
	}
}

// TestDropTraceShapedRows_NoStreamColumn handles synthetic blocks
// (e.g. handle404Recovery's manifest-only fallback) that don't have
// a _stream column at all. Must be a no-op — without a stream we
// can't classify, and these synthetic rows are by construction log
// data.
func TestDropTraceShapedRows_NoStreamColumn(t *testing.T) {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_time", Values: []string{"2026-06-05T13:00:00Z"}},
		{Name: "_msg", Values: []string{"a log line"}},
	})
	out := dropTraceShapedRows(db)
	if out == nil || out.RowsCount() != 1 {
		t.Errorf("block with no _stream was filtered; want pass-through")
	}
}

// TestIsTraceShapedStream pins the classifier directly so the regex
// edits don't drift away from the documented contract.
func TestIsTraceShapedStream(t *testing.T) {
	cases := []struct {
		in   string
		want bool
	}{
		// Trace-shaped.
		{`{resource_attr:service.name="x"}`, true},
		{`{name="HTTP GET",resource_attr:service.name="x"}`, true},
		{`{name="DB QUERY"}`, true},
		// Log-shaped.
		{`{service.name="api"}`, false},
		{`{k8s.node.name="node-1",service.name="api"}`, false},
		{`{level="INFO",service.name="api"}`, false},
		// Edge cases.
		{``, false},
		{`{}`, false},
		// `host.name` and `k8s.node.name` look superficially like
		// `name=` but are NOT trace-shaped — they don't appear at
		// `{name="...` start.
		{`{host.name="ip-10-0-0-1",service.name="api"}`, false},
	}
	for _, c := range cases {
		got := isTraceShapedStream(c.in)
		if got != c.want {
			t.Errorf("isTraceShapedStream(%q) = %v, want %v", c.in, got, c.want)
		}
	}
}

func blockWithStream(values []string) *logstorage.DataBlock {
	db := &logstorage.DataBlock{}
	db.SetColumns([]logstorage.BlockColumn{
		{Name: "_stream", Values: values},
		{Name: "_msg", Values: make([]string, len(values))},
	})
	return db
}
