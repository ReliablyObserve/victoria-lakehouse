package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestExtractTraceBloomValues_CoversSchemaSet: every bloom-value column with a
// non-empty row value must appear in the extracted map (driven by the schema SoT),
// and span_id (the documented exclusion) must NOT — locking schema-driven extraction
// against the old hardcoded {trace_id, service.name} list.
func TestExtractTraceBloomValues_CoversSchemaSet(t *testing.T) {
	rows := []schema.TraceRow{{
		TraceID: "tid-1", SpanID: "sid-1", SpanName: "GET /x",
		ServiceName: "api", ServiceInstanceID: "inst-1", ContainerID: "ctr-1",
		HostName: "h1", K8sPodName: "pod-1", K8sNodeName: "node-1",
		URLFull: "http://x/y", ClientAddress: "1.1.1.1", ServerAddress: "2.2.2.2",
		NetworkPeerAddress: "3.3.3.3", DBCollectionName: "coll", DBOperationName: "find",
		RPCMethod: "Get", MessagingDestination: "queue", CodeFunctionName: "fn",
		ExceptionType: "Err",
	}}
	got := extractTraceBloomValues(rows)
	for _, c := range schema.TraceBloomValueColumns {
		if _, ok := got[c.Name]; !ok {
			t.Errorf("bloom column %q missing from extracted values", c.Name)
		}
	}
	if _, ok := got["span_id"]; ok {
		t.Error("span_id must be excluded from bloom values (cost), but was extracted")
	}
}

// TestExtractLogBloomValues_CoversSchemaSet is the log twin.
func TestExtractLogBloomValues_CoversSchemaSet(t *testing.T) {
	rows := []schema.LogRow{{
		TraceID: "tid-1", SpanID: "sid-1",
		ContainerID: "ctr-1", ServiceInstanceID: "inst-1", ServiceName: "api",
		ServiceVersion: "1.0", HostName: "h1", K8sPodName: "pod-1", K8sNodeName: "node-1",
		ExceptionType: "Err",
	}}
	got := extractLogBloomValues(rows)
	for _, c := range schema.LogBloomValueColumns {
		if _, ok := got[c.Name]; !ok {
			t.Errorf("bloom column %q missing from extracted values", c.Name)
		}
	}
	if _, ok := got["span_id"]; ok {
		t.Error("span_id must be excluded from log bloom values, but was extracted")
	}
}

// TestExtractTraceBloomValues_Dedups: repeated values collapse to one.
func TestExtractTraceBloomValues_Dedups(t *testing.T) {
	rows := make([]schema.TraceRow, 50)
	for i := range rows {
		rows[i] = schema.TraceRow{TraceID: "same", ServiceName: "api"}
	}
	got := extractTraceBloomValues(rows)
	if len(got["trace_id"]) != 1 {
		t.Errorf("trace_id dedup: got %d values, want 1", len(got["trace_id"]))
	}
}

// TestExtractBloomValues_EmptyAndNil: no rows → nil; all-empty values → no keys.
func TestExtractBloomValues_EmptyAndNil(t *testing.T) {
	if extractTraceBloomValues(nil) != nil {
		t.Error("nil trace rows should yield nil")
	}
	if extractLogBloomValues(nil) != nil {
		t.Error("nil log rows should yield nil")
	}
	if got := extractTraceBloomValues([]schema.TraceRow{{}}); len(got) != 0 {
		t.Errorf("all-empty row should yield no bloom keys, got %v", got)
	}
}

// FuzzExtractTraceBloomValues: arbitrary field values must not panic, every output
// key must be a declared bloom-value column, span_id never appears, and each value
// list is deduplicated.
func FuzzExtractTraceBloomValues(f *testing.F) {
	f.Add("a", "b", "c", "d")
	f.Add("", "", "", "")
	f.Fuzz(func(t *testing.T, tid, sid, svc, host string) {
		rows := []schema.TraceRow{
			{TraceID: tid, SpanID: sid, ServiceName: svc, HostName: host},
			{TraceID: tid, SpanID: sid, ServiceName: svc, HostName: host}, // dup
		}
		got := extractTraceBloomValues(rows)
		allowed := map[string]bool{}
		for _, c := range schema.TraceBloomValueColumns {
			allowed[c.Name] = true
		}
		for k, vs := range got {
			if !allowed[k] {
				t.Errorf("unexpected bloom key %q (not in SoT)", k)
			}
			if k == "span_id" {
				t.Error("span_id must never be bloom-extracted")
			}
			seen := map[string]bool{}
			for _, v := range vs {
				if seen[v] {
					t.Errorf("duplicate value %q under %q", v, k)
				}
				seen[v] = true
			}
		}
	})
}
