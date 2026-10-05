//go:build e2e

package e2e

import (
	im "github.com/ReliablyObserve/victoria-lakehouse/tests/ingestmatrix"
	"testing"
)

func TestIngestMatrix_RawIndexTenantTruth(t *testing.T) {
	const marker = "trace-fixture"
	s := &caseState{c: im.Case{Rows: 1}, p: im.Params{Tenant: im.NumericTenant, Marker: marker}, hotRows: []map[string]string{{"trace_id": marker, "span_id": "one", "_msg": "-"}}}
	span := im.RawParquetRow{"account_id": {"4401"}, "project_id": {"1"}, "trace_id": {marker}, "span_id": {"one"}, "body": {"-"}}
	index := im.RawParquetRow{"account_id": {"4401"}, "project_id": {"1"}, "trace_id": {marker}}
	cell := &pqCell{rows: 2, spanRows: 1, spans: map[string]int{"one": 1}, raw: []im.RawParquetRow{span, index}}
	r := &matrixRun{sig: im.Traces, pq: map[string]*pqObject{"4401/1/traces/test.parquet": {cells: map[string]*pqCell{marker: cell}}}}
	if ok, why := r.parquetExact(s); !ok {
		t.Fatalf("positive control: %s", why)
	}
	index["account_id"] = []string{"9999"}
	if ok, why := r.parquetExact(s); ok {
		t.Fatalf("wrong-tenant trace-index physical row accepted: %s", why)
	}
}
