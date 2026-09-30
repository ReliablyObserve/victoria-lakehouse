package vlstorage

import (
	"fmt"
	"strings"
	"testing"
)

// Proof tests for the registry rows vl.pipe.union.basic and vl.filter.in.subquery:
// each runs the row's exact query through the logs adapter over a fixture whose
// answer is known, so the row's `pass` is backed by a test that fails without the
// subquery routing (the routing is QueryHasPipes || QueryHasFilterSubqueries).

func TestRow_VLPipeUnionBasic(t *testing.T) {
	s := &filterStore{rows: []map[string]string{
		{"_msg": "a", "level": "ERROR"},
		{"_msg": "b", "level": "WARN"},
		{"_msg": "c", "level": "ERROR"},
		{"_msg": "d", "level": "INFO"},
	}}
	got := runAdapter(t, s, `level:="ERROR" | union (level:="WARN") | stats count() as n`)
	if strings.Join(got, ";") != "n=3" {
		t.Fatalf("union row query: got %v, want [n=3] (2 ERROR + 1 WARN); the union subquery was not resolved", got)
	}
}

func TestRow_VLFilterInSubquery(t *testing.T) {
	var rows []map[string]string
	for i := 0; i < 130; i++ {
		rows = append(rows, map[string]string{"_msg": fmt.Sprintf("m%d", i), "service.name": "busy"})
	}
	for i := 0; i < 5; i++ {
		rows = append(rows, map[string]string{"_msg": fmt.Sprintf("q%d", i), "service.name": "quiet"})
	}
	s := &filterStore{rows: rows}
	got := runAdapter(t, s, `service.name:in(* | stats by (service.name) count() as n | filter n:>100 | fields service.name) | limit 100`)
	if len(got) != 100 {
		t.Fatalf("in() row query returned %d rows, want 100 (limit of the 130 'busy' rows)", len(got))
	}
	for _, r := range got {
		if !strings.Contains(r, "service.name=busy") {
			t.Fatalf("row from a service under the threshold leaked through the in() subquery: %s", r)
		}
	}
}

// A query whose ONLY subquery is an in() filter (no pipes at all) also has to
// be resolved, and so does one nested inside a union branch: the branch reaches
// the adapter's RunQuery as a pipe-less query of its own.
func TestRunQuery_FilterSubqueriesWithoutPipes(t *testing.T) {
	s := &filterStore{rows: []map[string]string{
		{"_msg": "a", "level": "error"},
		{"_msg": "b", "level": "warn"},
		{"_msg": "c", "level": "error"},
		{"_msg": "warn", "level": "info"},
	}}

	// Plain, no pipes.
	got := runAdapter(t, s, `_msg:in(level:warn | fields _msg)`)
	if strings.Join(got, ";") != "_msg=b,level=warn" {
		t.Errorf("no-pipes in(): got %v, want only the row whose _msg the subquery returned (b)", got)
	}

	// In a union branch: level:error (2 rows) + the branch's rows, where the
	// branch is `_msg:in(level:warn | fields _msg)` (resolves to _msg=b).
	got = runAdapter(t, s, `level:error | union (_msg:in(level:warn | fields _msg)) | stats count() n`)
	if strings.Join(got, ";") != "n=3" {
		t.Errorf("in() inside a union branch: got %v, want [n=3] (2 error + 1 from the branch)", got)
	}
}
