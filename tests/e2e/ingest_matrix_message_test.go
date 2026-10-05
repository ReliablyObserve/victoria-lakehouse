//go:build e2e

package e2e

import (
	"encoding/json"
	im "github.com/ReliablyObserve/victoria-lakehouse/tests/ingestmatrix"
	"testing"
)

func TestIngestMatrix_RawDefaultMessageGapIsExact(t *testing.T) {
	for _, mutation := range []string{"none", "undeclared", "unobserved", "different-hot-message", "absent-body", "wrong-body", "wrong-name"} {
		t.Run(mutation, func(t *testing.T) {
			hot := map[string]string{"_msg": "-", "trace_id": "fixture", "span_id": "one", "name": "correct"}
			raw := im.RawParquetRow{"account_id": {"4401"}, "project_id": {"1"}, "trace_id": {"fixture"}, "span_id": {"one"}, "span.name": {"correct"}, "body": {defaultMsgVL}}
			s := &caseState{c: im.Case{Rows: 1, Gaps: []string{"traces-default-msg-value"}}, p: im.Params{Tenant: im.NumericTenant, Marker: "fixture"}, hotRows: []map[string]string{hot}, gapHits: map[string]int{"traces-default-msg-value": 1}, preflushDefaultMessageGap: true}
			switch mutation {
			case "undeclared":
				s.c.Gaps = nil
			case "unobserved":
				s.gapHits = map[string]int{}
				s.preflushDefaultMessageGap = false
			case "different-hot-message":
				hot["_msg"] = "customer message"
			case "absent-body":
				delete(raw, "body")
			case "wrong-body":
				raw["body"] = []string{"arbitrary replacement"}
			case "wrong-name":
				raw["span.name"] = []string{"wrong"}
			}
			cell := &pqCell{rows: 1, spanRows: 1, spans: map[string]int{"one": 1}, raw: []im.RawParquetRow{raw}}
			r := &matrixRun{sig: im.Traces, pq: map[string]*pqObject{"4401/1/traces/test.parquet": {cells: map[string]*pqCell{"fixture": cell}}}}
			before := hot["_msg"]
			ok, why := r.parquetExact(s)
			if hot["_msg"] != before {
				t.Fatal("default gap mutated captured hot oracle")
			}
			if mutation == "none" && !ok {
				t.Fatalf("positive exact default-body gap: %s", why)
			}
			if mutation != "none" && ok {
				t.Fatalf("raw default-body gap accepted %s", mutation)
			}
		})
	}
}

func TestIngestMatrix_ColdOnlyDefaultMessageIsNotIngestEvidence(t *testing.T) {
	hot := map[string]string{"_msg": "-", "trace_id": "fixture", "span_id": "one", "name": "correct"}
	lh := map[string]string{"_msg": defaultMsgVL, "trace_id": "fixture", "span_id": "one", "name": "correct"}
	raw := im.RawParquetRow{"account_id": {"4401"}, "project_id": {"1"}, "trace_id": {"fixture"}, "span_id": {"one"}, "span.name": {"correct"}, "body": {defaultMsgVL}}
	s := &caseState{c: im.Case{Rows: 1, Gaps: []string{"traces-default-msg-value"}}, p: im.Params{Tenant: im.NumericTenant, Marker: "fixture"}, hotRows: []map[string]string{hot}, gapHits: map[string]int{}, flushed: true}
	cell := &pqCell{rows: 1, spanRows: 1, spans: map[string]int{"one": 1}, raw: []im.RawParquetRow{raw}}
	r := &matrixRun{sig: im.Traces, pq: map[string]*pqObject{"4401/1/traces/test.parquet": {cells: map[string]*pqCell{"fixture": cell}}}}
	hotJSON, _ := json.Marshal(hot)
	lhJSON, _ := json.Marshal(lh)
	r.applyKnownGaps(s, []string{string(hotJSON)}, []string{string(lhJSON)})
	if s.gapHits["traces-default-msg-value"] == 0 {
		t.Fatal("fixture did not record cold-only default gap")
	}
	if ok, why := r.parquetExact(s); ok {
		t.Fatalf("cold-only message difference accepted as preflush ingest evidence: %s", why)
	}
}
