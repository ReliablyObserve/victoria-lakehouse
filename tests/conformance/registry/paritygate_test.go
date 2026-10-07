package registry

import (
	"fmt"
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
	for _, w := range []string{"--allowlist=tests/a.txt", "--allowlist   tests/a.txt", "python x --allowlist tests/a.txt --summary-file y"} {
		if got := AllowlistPath([]byte(w)); got != "tests/a.txt" {
			t.Errorf("AllowlistPath(%q) = %q", w, got)
		}
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
	if !v.Fix || len(v.Problems) != 3 {
		t.Fatalf("want 3 problems (no parity test, no lock, no lock for TestP), got %+v", v)
	}
	// with a new lock referencing a modified parity test: clean.
	v = ParityCheck(base, snapshot(nil, nil, gap, lockRow()), mod(lockTest))
	if !v.Fix || len(v.Problems) != 0 || len(v.Weakenings) != 0 {
		t.Fatalf("want clean fix, got %+v", v)
	}
	// B10: the parity test file was only touched (no function modified).
	v = ParityCheck(base, snapshot(nil, nil, gap, lockRow()), mod())
	if len(v.Problems) != 3 {
		t.Fatalf("a lock that references an unmodified test must not count: %+v", v)
	}
	// a bare file reference is not enough.
	bare := lockRow()
	bare.Tests = []string{"tests/parity/p_test.go"}
	v = ParityCheck(base, snapshot(nil, nil, gap, bare), mod(lockTest))
	if len(v.Problems) != 2 || !strings.Contains(v.Problems[0], "no new or changed registry row is a lock") {
		t.Fatalf("a bare file ref is not a named lock: %+v", v)
	}
	// a loose compare is not a lock.
	loose := lockRow()
	loose.Compare, loose.CompareMap = "status", map[string]any{"type": "status"}
	v = ParityCheck(base, snapshot(nil, nil, gap, loose), mod(lockTest))
	if len(v.Problems) != 2 {
		t.Fatalf("a status compare is not exact: %+v", v)
	}
	// an UNCHANGED exact row that already references the test is not this PR's lock.
	v = ParityCheck(snapshot([]string{"TestP"}, nil, lockRow()), snapshot(nil, nil, lockRow()), mod(lockTest))
	if len(v.Problems) != 2 {
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

func wholeRow(t *testing.T, yaml string) RowLite {
	t.Helper()
	rows := map[string]RowLite{}
	if err := ParseRowsLenient([]byte(yaml), rows); err != nil {
		t.Fatal(err)
	}
	for _, r := range rows {
		return r
	}
	t.Fatal("no row")
	return RowLite{}
}

const passRow = `- id: lh.r
  title: t
  surface: lh
  expect: pass
  targets: [hot, cold]
  seed: [logs.base]
  layers: [api]
  upstream: {route: /x}
  request: {method: GET, path: /x}
  compare: {type: exact-json}
  pending: true
  refs: {doc: docs/a.md, tests: [tests/parity/p_test.go#TestP]}
`

func TestPassRowChanges_WholeRow(t *testing.T) {
	base := wholeRow(t, passRow)
	edit := func(old, new string) RowLite { return wholeRow(t, strings.Replace(passRow, old, new, 1)) }
	weak := map[string]RowLite{
		"targets (N2)":  edit("targets: [hot, cold]", "targets: [hot]"),
		"seed (N3)":     edit("seed: [logs.base]", "seed: [logs.empty]"),
		"layers":        edit("layers: [api]", "layers: [ui]"),
		"upstream":      edit("upstream: {route: /x}", "upstream: {route: /y}"),
		"surface":       edit("surface: lh", "surface: vl"),
		"request":       edit("path: /x}", "path: /health}"),
		"compare":       edit("compare: {type: exact-json}", "compare: {type: count}"),
		"a new key":     edit("pending: true\n", "pending: true\n  since: {vl: \"1.0\"}\n"),
		"lost test ref": edit("refs: {doc: docs/a.md, tests: [tests/parity/p_test.go#TestP]}", "refs: {doc: docs/a.md, tests: []}"),
	}
	for name, h := range weak {
		if got := passRowChanges(base, h, nil); len(got) == 0 {
			t.Errorf("%s: a change to a pass row must be a weakening", name)
		}
	}
	// N1: pending set on a row that was executing.
	exec := wholeRow(t, strings.Replace(passRow, "  pending: true\n", "", 1))
	pend := wholeRow(t, passRow)
	if got := passRowChanges(exec, pend, nil); len(got) != 1 || !strings.Contains(got[0], "set to pending") {
		t.Errorf("N1: pending set must be a weakening, got %v", got)
	}
	allowed := map[string]RowLite{
		"title":              edit("title: t", "title: a better title"),
		"notes":              wholeRow(t, passRow+"  notes: extra\n"),
		"refs.doc":           edit("docs/a.md", "docs/b.md"),
		"more test refs":     edit("TestP]}", "TestP, tests/parity/p_test.go#TestQ]}"),
		"pending turned off": edit("  pending: true\n", ""),
		"nothing":            base,
	}
	for name, h := range allowed {
		if got := passRowChanges(base, h, nil); len(got) != 0 {
			t.Errorf("%s must not be a weakening, got %v", name, got)
		}
	}
	// a renamed test: the old reference no longer resolves and the row keeps as many references
	renamed := wholeRow(t, strings.Replace(passRow, "#TestP]", "#TestRenamed]", 1))
	gone := func(ref string) bool { return !strings.HasSuffix(ref, "#TestP") }
	if got := passRowChanges(base, renamed, gone); len(got) != 0 {
		t.Errorf("a replaced reference to a renamed test is fine, got %v", got)
	}
	if got := passRowChanges(base, renamed, nil); len(got) != 1 {
		t.Errorf("without proof the old test is gone the replacement is a loss, got %v", got)
	}
	still := func(string) bool { return true }
	if got := passRowChanges(base, renamed, still); len(got) != 1 {
		t.Errorf("a reference swapped while its test still exists is a loss, got %v", got)
	}
	// deleting the lock's test and dropping its reference is a weakening
	dropped := wholeRow(t, strings.Replace(passRow, "tests: [tests/parity/p_test.go#TestP]", "tests: []", 1))
	if got := passRowChanges(base, dropped, gone); len(got) != 1 {
		t.Errorf("a dropped reference is a weakening even when its test is gone, got %v", got)
	}
	// differ rows are not protected
	if v := ParityCheck(snapshot(nil, nil, RowLite{ID: "g", Expect: "differ"}), snapshot(nil, nil), nil); len(v.Weakenings) != 0 {
		t.Errorf("%+v", v)
	}
}

func TestParityCheck_LockMustBeRelatedAndExecuting(t *testing.T) {
	gap := RowLite{ID: "gap", Expect: "differ"}
	base := snapshot([]string{"TestFixed/logs", "TestOther"}, nil, gap)
	own := RowLite{ID: "own", Expect: "pass", Compare: "exact-json", Tests: []string{"tests/parity/p_test.go#TestFixed"}}
	other := RowLite{ID: "other", Expect: "pass", Compare: "exact-json", Tests: []string{"tests/parity/p_test.go#TestOther"}}
	mods := mod("tests/parity/p_test.go#TestFixed", "tests/parity/p_test.go#TestOther")
	// each removed entry's top-level test is named by a lock
	v := ParityCheck(base, snapshot(nil, nil, gap, own, other), mods)
	if len(v.Problems) != 0 {
		t.Fatalf("both locked: %+v", v)
	}
	// N6: a lock for an UNRELATED test does not lock the removed entry
	v = ParityCheck(base, snapshot(nil, nil, gap, other), mods)
	if len(v.Problems) != 1 || !strings.Contains(v.Problems[0], "TestFixed") {
		t.Fatalf("an unrelated lock must not lock TestFixed: %+v", v)
	}
	// a pending row is never a lock
	pend := own
	pend.Pending = true
	v = ParityCheck(base, snapshot(nil, nil, gap, pend, other), mods)
	if len(v.Problems) != 1 || !strings.Contains(v.Problems[0], "TestFixed") {
		t.Fatalf("a pending row is not a lock: %+v", v)
	}
	// a flipped row must also not be pending
	flipBase := snapshot(nil, nil, RowLite{ID: "f", Expect: "differ", Compare: "exact-json", Tests: []string{lockTest}})
	flipHead := RowLite{ID: "f", Expect: "pass", Compare: "exact-json", Tests: []string{lockTest}, Pending: true}
	if v := ParityCheck(flipBase, snapshot(nil, nil, flipHead), mod(lockTest)); len(v.Problems) != 1 {
		t.Fatalf("a pending flipped row is no lock: %+v", v)
	}
}

func TestModifiedParityTests_OnlyTheParitySuite(t *testing.T) {
	src := func(m map[string]string) func(string) []byte {
		return func(p string) []byte {
			if s, ok := m[p]; ok {
				return []byte("package p\n\n" + s)
			}
			return nil
		}
	}
	base := map[string]string{"tests/parity/a_test.go": "func TestA() {}\n"}
	head := map[string]string{
		"tests/parity/a_test.go":     "func TestA() { _ = 1 }\n",
		"tests/parity/sub/b_test.go": "func TestSub() {}\n",
		"tests/e2e/c_test.go":        "func TestE2E() {}\n",
		"internal/x/d_test.go":       "func TestD() {}\n",
	}
	got, err := ModifiedParityTests([]string{"tests/parity/a_test.go", "tests/parity/sub/b_test.go", "tests/e2e/c_test.go", "internal/x/d_test.go"}, src(base), src(head))
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || !got["tests/parity/a_test.go#TestA"] {
		t.Fatalf("only tests/parity/*_test.go count: %v", got)
	}
	if _, err := ModifiedParityTests([]string{"tests/parity/a_test.go"}, src(base), func(string) []byte { return []byte("package p\nfunc (") }); err == nil {
		t.Error("a parity test file that does not parse is an error")
	}
}

func TestSubstantiveChanges_ProseDoesNotCount(t *testing.T) {
	row := map[string]any{"id": "r", "title": "t", "notes": "n", "description": "d", "highlight": "h", "differ_note": "x",
		"expect": "pass", "refs": map[string]any{"doc": "a.md", "tests": []any{"a_test.go#T"}}, "targets": []any{"hot"}}
	with := func(k string, v any) map[string]any {
		c := map[string]any{}
		for kk, vv := range row {
			c[kk] = vv
		}
		c[k] = v
		return c
	}
	base := map[string]map[string]any{"r": row}
	for _, k := range []string{"title", "notes", "description", "highlight", "differ_note"} {
		if got := SubstantiveChanges(base, map[string]map[string]any{"r": with(k, "edited")}); len(got) != 0 {
			t.Errorf("%s is prose, got %v", k, got)
		}
	}
	if got := SubstantiveChanges(base, map[string]map[string]any{"r": with("refs", map[string]any{"doc": "b.md", "tests": []any{"a_test.go#T"}})}); len(got) != 0 {
		t.Errorf("refs.doc is prose, got %v", got)
	}
	for name, h := range map[string]map[string]any{
		"targets": with("targets", []any{"hot", "cold"}),
		"refs":    with("refs", map[string]any{"doc": "a.md", "tests": []any{"a_test.go#T", "a_test.go#U"}}),
		"pending": with("pending", true),
		"expect":  with("expect", "differ"),
		"new key": with("covers", []any{"internal/x/**"}),
	} {
		if got := SubstantiveChanges(base, map[string]map[string]any{"r": h}); !reflect.DeepEqual(got, []string{"r"}) {
			t.Errorf("%s is a substantive change, got %v", name, got)
		}
	}
	if got := SubstantiveChanges(base, map[string]map[string]any{"r": row, "n": {"id": "n"}}); !reflect.DeepEqual(got, []string{"n"}) {
		t.Errorf("an added entry is substantive: %v", got)
	}
	if got := SubstantiveChanges(base, map[string]map[string]any{}); !reflect.DeepEqual(got, []string{"r"}) {
		t.Errorf("a removed entry is substantive: %v", got)
	}
}

func TestSummarizeChanges_CollapsesLongLists(t *testing.T) {
	ids := func(n int) []string {
		var out []string
		for i := 0; i < n; i++ {
			out = append(out, fmt.Sprintf("id%02d", i))
		}
		return out
	}
	short := SummarizeChanges([]ChangeSet{{Kind: "rows", Added: ids(3)}})
	if !strings.Contains(short, "- rows added (3): id00, id01, id02\n") || strings.Contains(short, "<details>") {
		t.Errorf("short list stays inline:\n%s", short)
	}
	long := SummarizeChanges([]ChangeSet{{Kind: "rows", Changed: ids(30)}, {Kind: "features", Removed: ids(1)}})
	for _, want := range []string{"- rows changed (30): id00, id01, id02, id03, id04, id05, id06, id07, … and 22 more\n", "<details><summary>all 30 rows changed</summary>", "  - id29\n", "- features removed (1): id00\n"} {
		if !strings.Contains(long, want) {
			t.Errorf("missing %q in:\n%s", want, long)
		}
	}
	if strings.Contains(long, "id08, id09") {
		t.Errorf("only the first %d ids are inline", maxInlineIDs)
	}
	if got := SummarizeChanges(nil); !strings.Contains(got, "none") {
		t.Errorf("empty: %q", got)
	}
}
