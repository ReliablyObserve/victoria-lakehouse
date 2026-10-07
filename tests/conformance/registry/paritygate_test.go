package registry

import (
	"reflect"
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
	if err := ParseRowsLenient([]byte("- {id: a, expect: pass, compare: {type: exact-json}, request: {path: /x}, refs: {tests: [x_test.go#T]}}\n- {id: b, expect: differ, compare: {type: count}}\n"), rows); err != nil {
		t.Fatal(err)
	}
	if !rows["a"].Exact() || rows["b"].Exact() || rows["a"].Tests[0] != "x_test.go#T" {
		t.Fatalf("rows = %+v", rows)
	}
	if got := AllowlistPath([]byte("run: |\n  python x.py \\\n    --allowlist tests/parity/known_failures.txt \\\n    --summary-file y\n")); got != "tests/parity/known_failures.txt" {
		t.Errorf("AllowlistPath = %q", got)
	}
	if AllowlistPath([]byte("nothing")) != "" {
		t.Error("no argument is the empty path")
	}
}

func TestExactEquivalent(t *testing.T) {
	cases := []struct {
		yaml string
		want bool
	}{
		{"{type: exact-json}", true},
		{"{type: count}", true},
		{"{type: trace}", true},
		{"{type: ndjson-multiset, project: [a]}", true},
		{`{type: values-with-hits, options: {hits_tolerance: "0"}}`, true},
		{`{type: values-with-hits, options: {hits_tolerance: "0.0"}}`, true},
		{`{type: values-with-hits, options: {hits_tolerance: "0.5"}}`, false},
		{`{type: values-with-hits}`, false},
		{`{type: series, options: {rel_tolerance: "0"}}`, true},
		{`{type: series, options: {rel_tolerance: "0.02"}}`, false},
		{`{type: series}`, false},
		{`{type: status}`, false},
		{`{type: schema}`, false},
	}
	for _, c := range cases {
		rows := map[string]RowLite{}
		if err := ParseRowsLenient([]byte("- {id: r, expect: pass, compare: "+c.yaml+"}\n"), rows); err != nil {
			t.Fatal(err)
		}
		if got := rows["r"].Exact(); got != c.want {
			t.Errorf("%s: Exact = %v, want %v", c.yaml, got, c.want)
		}
	}
	rows := map[string]RowLite{}
	_ = ParseRowsLenient([]byte("- {id: r, expect: differ, compare: {type: exact-json}}\n"), rows)
	if rows["r"].Exact() {
		t.Error("a differ row is never a lock")
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
		if r.CompareMap == nil && r.Compare != "" {
			r.CompareMap = map[string]any{"type": r.Compare}
		}
		if r.Whole == nil {
			r.Whole = map[string]any{"id": r.ID, "expect": r.Expect, "compare": r.CompareMap, "request": r.Request, "tests": r.Tests}
		}
		s.Rows[r.ID] = r
	}
	return s
}

const lockTest = "tests/parity/p_test.go#TestP"

func lockRow() RowLite {
	return RowLite{ID: "lock", Expect: "pass", Compare: "exact-json", Request: "r1", Tests: []string{lockTest}}
}

func mod(refs ...string) map[string]bool {
	m := map[string]bool{}
	for _, r := range refs {
		m[r] = true
	}
	return m
}

func TestParityCheck_FixNeedsLocks(t *testing.T) {
	gap := RowLite{ID: "gap", Expect: "differ"}
	base := snapshot([]string{"TestP"}, nil, gap)
	// allowlist removal with nothing else: two problems.
	v := ParityCheck(base, snapshot(nil, nil, gap), nil)
	if !v.Fix || len(v.Problems) != 2 {
		t.Fatalf("want 2 problems, got %+v", v)
	}
	// with a new lock referencing a modified parity test: clean.
	v = ParityCheck(base, snapshot(nil, nil, gap, lockRow()), mod(lockTest))
	if !v.Fix || len(v.Problems) != 0 || len(v.Weakenings) != 0 {
		t.Fatalf("want clean fix, got %+v", v)
	}
	// B10: the parity test file was only touched (no function modified).
	v = ParityCheck(base, snapshot(nil, nil, gap, lockRow()), mod())
	if len(v.Problems) != 2 {
		t.Fatalf("a lock that references an unmodified test must not count: %+v", v)
	}
	// a bare file reference is not enough.
	bare := lockRow()
	bare.Tests = []string{"tests/parity/p_test.go"}
	v = ParityCheck(base, snapshot(nil, nil, gap, bare), mod(lockTest))
	if len(v.Problems) != 1 || !strings.Contains(v.Problems[0], "no new or changed registry row is a lock") {
		t.Fatalf("a bare file ref is not a named lock: %+v", v)
	}
	// a loose compare is not a lock.
	loose := lockRow()
	loose.Compare, loose.CompareMap = "status", map[string]any{"type": "status"}
	v = ParityCheck(base, snapshot(nil, nil, gap, loose), mod(lockTest))
	if len(v.Problems) != 1 {
		t.Fatalf("a status compare is not exact: %+v", v)
	}
	// an UNCHANGED exact row that already references the test is not this PR's lock.
	v = ParityCheck(snapshot([]string{"TestP"}, nil, lockRow()), snapshot(nil, nil, lockRow()), mod(lockTest))
	if len(v.Problems) != 1 {
		t.Fatalf("an unchanged row must not count: %+v", v)
	}
}

func TestParityCheck_FlipIsTheOnlyTrigger(t *testing.T) {
	// A differ->pass flip alone makes a parity-fix PR, and the flipped row itself must be the lock.
	gap := RowLite{ID: "gap", Expect: "differ", Compare: "exact-json", Request: "r", Tests: []string{lockTest}}
	flipped := gap
	flipped.Expect = "pass"
	base, head := snapshot(nil, nil, gap), snapshot(nil, nil, flipped)
	v := ParityCheck(base, head, mod(lockTest))
	if !v.Fix || len(v.Problems) != 0 {
		t.Fatalf("a flipped, exact, referencing row is a lock: %+v", v)
	}
	if v = ParityCheck(base, head, nil); !v.Fix || len(v.Problems) != 2 {
		t.Fatalf("flip without a modified parity test: %+v", v)
	}
	// another changed lock row elsewhere does not excuse the flipped row.
	other := RowLite{ID: "other", Expect: "pass", Compare: "exact-json", Tests: []string{lockTest}}
	loose := flipped
	loose.Compare, loose.CompareMap = "status", map[string]any{"type": "status"}
	v = ParityCheck(base, snapshot(nil, nil, loose, other), mod(lockTest))
	if !v.Fix || len(v.Problems) != 1 || !strings.Contains(v.Problems[0], "the flipped row gap must itself be a lock") {
		t.Fatalf("the flipped row must be the lock: %+v", v)
	}
}

func TestParityCheck_NonFixPRsReportNoLockProblems(t *testing.T) {
	// A PR that is not a parity fix is never asked for locks, whatever it changes.
	base := snapshot([]string{"TestP"}, nil, lockRow())
	head := snapshot([]string{"TestP"}, nil, lockRow(), RowLite{ID: "new", Expect: "pass", Compare: "status"})
	v := ParityCheck(base, head, nil)
	if v.Fix || len(v.Problems) != 0 || len(v.Weakenings) != 0 {
		t.Fatalf("not a fix, nothing weakened: %+v", v)
	}
}

func TestParityCheck_Weakenings(t *testing.T) {
	pass := func(id string, mut func(*RowLite)) RowLite {
		r := RowLite{ID: id, Expect: "pass", Compare: "ndjson-multiset", Request: "r1"}
		r.CompareMap = map[string]any{"type": "ndjson-multiset", "project": []any{"a"}}
		if mut != nil {
			mut(&r)
		}
		return r
	}
	cases := map[string]struct {
		head ParitySnapshot
		want string
	}{
		"allowlist added":       {snapshot([]string{"TestNew"}, nil, pass("r", nil)), "allowlist entry added: TestNew"},
		"deleted pass row":      {snapshot(nil, nil), "pass row deleted: r"},
		"pass to differ":        {snapshot(nil, nil, pass("r", func(r *RowLite) { r.Expect = "differ" })), "weakened to expect=differ"},
		"compare type loosened": {snapshot(nil, nil, pass("r", func(r *RowLite) { r.CompareMap = map[string]any{"type": "count"} })), "compare (type, options or project) changed"},
		"project narrowed (B7)": {snapshot(nil, nil, pass("r", func(r *RowLite) { r.CompareMap = map[string]any{"type": "ndjson-multiset", "project": []any{}} })), "compare (type, options or project) changed"},
		"tolerance raised (B7)": {snapshot(nil, nil, pass("r", func(r *RowLite) {
			r.CompareMap = map[string]any{"type": "ndjson-multiset", "project": []any{"a"}, "options": map[string]any{"hits_tolerance": "0.5"}}
		})), "compare (type, options or project) changed"},
		"request re-pointed (B8)": {snapshot(nil, nil, pass("r", func(r *RowLite) { r.Request = "/health" })), "request changed"},
	}
	base := snapshot(nil, nil, pass("r", nil))
	for name, c := range cases {
		v := ParityCheck(base, c.head, nil)
		if len(v.Weakenings) != 1 || !strings.Contains(v.Weakenings[0], c.want) || v.Fix {
			t.Errorf("%s: %+v", name, v)
		}
	}
	// a differ row is free to change.
	v := ParityCheck(snapshot(nil, nil, RowLite{ID: "g", Expect: "differ"}), snapshot(nil, nil), nil)
	if len(v.Weakenings) != 0 {
		t.Errorf("only pass rows are protected: %+v", v)
	}
	// rename hint
	v = ParityCheck(snapshot([]string{"TestP/logs"}, nil), snapshot([]string{"TestP/logs/parquet"}, nil), nil)
	if len(v.Weakenings) != 1 || !strings.Contains(v.Weakenings[0], "looks like a rename") {
		t.Errorf("rename message: %+v", v)
	}
	v = ParityCheck(snapshot(nil, nil), snapshot([]string{"TestQ"}, nil), nil)
	if len(v.Weakenings) != 1 || strings.Contains(v.Weakenings[0], "rename") {
		t.Errorf("plain add must not claim rename: %+v", v)
	}
	// docs flip is a trigger, and an unchanged row is no lock
	v = ParityCheck(snapshot(nil, nil, lockRow()), snapshot(nil, []string{"B1"}, lockRow()), mod(lockTest))
	if !v.Fix || len(v.Problems) != 1 || !strings.Contains(v.Problems[0], "no new or changed registry row is a lock") {
		t.Fatalf("unchanged row must not count: %+v", v)
	}
	if !IsParityTestFile("tests/parity/x_test.go") || IsParityTestFile("tests/parity/sub/x_test.go") || IsParityTestFile("tests/parity/known_failures.txt") {
		t.Error("IsParityTestFile")
	}
}

func TestChangedEntries(t *testing.T) {
	b := map[string]map[string]any{"a": {"id": "a", "x": 1}, "b": {"id": "b"}, "c": {"id": "c"}}
	h := map[string]map[string]any{"a": {"id": "a", "x": 2}, "b": {"id": "b"}, "d": {"id": "d"}}
	added, changed, removed := ChangedEntries(b, h)
	if !reflect.DeepEqual(added, []string{"d"}) || !reflect.DeepEqual(changed, []string{"a"}) || !reflect.DeepEqual(removed, []string{"c"}) {
		t.Fatalf("%v %v %v", added, changed, removed)
	}
}
