package vlstorage

import (
	"testing"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func TestTraceMessageCanonicalLogRowsOwnsNativeAndCustomerValues(t *testing.T) {
	lr := logstorage.GetLogRows(nil, nil, nil, nil, "")
	defer logstorage.PutLogRows(lr)
	tenant := logstorage.TenantID{AccountID: 77341, ProjectID: 88}
	lr.MustAdd(tenant, 123, []logstorage.Field{
		{Name: "_msg", Value: "native-message"},
		{Name: "trace_id", Value: "trace"},
		{Name: "span_id", Value: "span"},
		{Name: "span_attr:_msg", Value: "customer-message"},
		{Name: "span_attr:span_attr:_msg", Value: "literal-message"},
	}, -1)
	canonical := false
	var canonicalBacking []byte
	lr.ForEachRow(func(_ uint64, r *logstorage.InsertRow) {
		for _, f := range r.Fields {
			if f.Name == "" && f.Value == "native-message" {
				canonical = true
				// Upstream owns this mutable arena; expose recycling deterministically.
				canonicalBacking = unsafe.Slice(unsafe.StringData(f.Value), len(f.Value))
			}
		}
	})
	if !canonical {
		t.Fatal("fixture did not exercise upstream's canonical native message")
	}
	// The spans take the path every Parquet row takes: into the insert buffer
	// (upstream storage), read back and converted by DataBlockToTraceRows.
	rows := rowsViaBuffer(t, lr)
	for i := range canonicalBacking {
		canonicalBacking[i] = 'x'
	}
	lr.ResetKeepSettings()
	lr.MustAdd(tenant, 456, []logstorage.Field{{Name: "_msg", Value: "recycled-value"}}, -1)
	if len(rows) != 1 {
		t.Fatalf("converted rows=%d", len(rows))
	}
	r := rows[0]
	if r.Body != "native-message" || r.SpanAttributes["_msg"] != "customer-message" || r.SpanAttributes["span_attr:_msg"] != "literal-message" {
		t.Fatalf("canonical message values lost or aliased after recycle: %+v", r)
	}
	if r.AccountID != tenant.AccountID || r.ProjectID != tenant.ProjectID || r.TimestampUnixNano != 123 {
		t.Fatalf("canonical conversion changed tenant/time: %+v", r)
	}
}
