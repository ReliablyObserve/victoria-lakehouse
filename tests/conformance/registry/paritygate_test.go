package registry

import (
	"strings"
	"testing"
)

func TestParityParsers(t *testing.T) {
	al := ParseAllowlist([]byte("# c\n\nTestA/sub#01  # B1: why\nTestB # B2\n"))
	if len(al) != 2 || !al["TestA/sub#01"] || !al["TestB"] {
		t.Fatalf("allowlist = %v", al)
	}
	res := ParseResolved([]byte("| Id | D |\n|---|---|\n| **B1** | open |\n| **Tie (B8)** | **Resolved** | x |\n| Plain | Resolved | y |\n"))
	if len(res) != 2 || !res["Tie (B8)"] || !res["Plain"] {
		t.Fatalf("resolved = %v", res)
	}
	rows := map[string]RowLite{}
	if err := ParseRowsLenient([]byte("- {id: a, expect: pass, compare: {type: exact-json}, refs: {tests: [x_test.go#T]}}\n- {id: b, expect: differ, compare: {type: count}}\n"), rows); err != nil {
		t.Fatal(err)
	}
	if !rows["a"].Exact() || rows["b"].Exact() || rows["a"].Tests[0] != "x_test.go#T" {
		t.Fatalf("rows = %+v", rows)
	}
}

func snapshot(al []string, res []string, rows ...RowLite) ParitySnapshot {
	s := ParitySnapshot{Allowlist: map[string]bool{}, Resolved: map[string]bool{}, Rows: map[string]RowLite{}}
	for _, a := range al {
		s.Allowlist[a] = true
	}
	for _, r := range res {
		s.Resolved[r] = true
	}
	for _, r := range rows {
		if r.Whole == nil {
			r.Whole = map[string]any{"id": r.ID, "expect": r.Expect, "compare": r.Compare, "tests": r.Tests}
		}
		s.Rows[r.ID] = r
	}
	return s
}

func TestParityCheck(t *testing.T) {
	lock := RowLite{ID: "lock", Expect: "pass", Compare: ParityExactCompare, Tests: []string{"tests/parity/p_test.go#TestP"}}
	base := snapshot([]string{"TestP"}, nil, RowLite{ID: "gap", Expect: "differ"})
	// fix without locks
	v := ParityCheck(base, snapshot(nil, nil, RowLite{ID: "gap", Expect: "differ"}), nil)
	if !v.Fix || len(v.Problems) != 2 {
		t.Fatalf("want 2 problems, got %+v", v)
	}
	// fix with locks
	v = ParityCheck(base, snapshot(nil, nil, RowLite{ID: "gap", Expect: "differ"}, lock), []string{"tests/parity/p_test.go"})
	if !v.Fix || len(v.Problems) != 0 || len(v.Weakenings) != 0 {
		t.Fatalf("want clean fix, got %+v", v)
	}
	// weakenings
	b2 := snapshot(nil, nil, lock)
	for name, head := range map[string]ParitySnapshot{
		"allowlist added": snapshot([]string{"TestNew"}, nil, lock),
		"differ":          snapshot(nil, nil, RowLite{ID: "lock", Expect: "differ", Compare: ParityExactCompare}),
		"loose":           snapshot(nil, nil, RowLite{ID: "lock", Expect: "pass", Compare: "count"}),
		"deleted":         snapshot(nil, nil),
	} {
		v := ParityCheck(b2, head, nil)
		if len(v.Weakenings) != 1 || v.Fix {
			t.Errorf("%s: %+v", name, v)
		}
	}
	// rename of an allowlist entry: still a weakening, with a pointed message
	v = ParityCheck(snapshot([]string{"TestP/logs"}, nil), snapshot([]string{"TestP/logs/parquet"}, nil), nil)
	if len(v.Weakenings) != 1 || !strings.Contains(v.Weakenings[0], "looks like a rename") {
		t.Errorf("rename message: %+v", v)
	}
	v = ParityCheck(snapshot(nil, nil), snapshot([]string{"TestQ"}, nil), nil)
	if len(v.Weakenings) != 1 || strings.Contains(v.Weakenings[0], "rename") {
		t.Errorf("plain add must not claim rename: %+v", v)
	}
	// docs flip + unchanged exact row is not a lock
	v = ParityCheck(snapshot(nil, nil, lock), snapshot(nil, []string{"B1"}, lock), []string{"tests/parity/p_test.go"})
	if !v.Fix || len(v.Problems) != 1 || !strings.Contains(v.Problems[0], "exact-json") {
		t.Fatalf("unchanged row must not count: %+v", v)
	}
	if !IsParityTestFile("tests/parity/x_test.go") || IsParityTestFile("tests/parity/sub/x_test.go") || IsParityTestFile("tests/parity/known_failures.txt") {
		t.Error("IsParityTestFile")
	}
}
