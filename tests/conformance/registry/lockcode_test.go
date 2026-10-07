package registry

import (
	"sort"
	"strings"
	"testing"
)

type tree map[string]string

func (tr tree) src(p string) []byte {
	if s, ok := tr[p]; ok {
		return []byte(s)
	}
	return nil
}

func (tr tree) list(dir string) []string {
	var out []string
	for p := range tr {
		if strings.HasSuffix(p, ".go") && strings.TrimSuffix(p, "/"+p[strings.LastIndex(p, "/")+1:]) == strings.TrimSuffix(dir, "/") {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func lockRows(tests ...string) map[string]RowLite {
	return map[string]RowLite{"r": {ID: "r", Expect: "pass", Compare: "exact-json", CompareMap: map[string]any{"type": "exact-json"}, Tests: tests}}
}

const (
	parityTest = "package parity\n\nfunc TestLock(t *testing.T) {\n\tRunParity(t)\n\tmine()\n}\n\nfunc TestOther(t *testing.T) { onlyOther() }\n\nfunc mine() {}\n"
	parityHelp = "package parity\n\nfunc RunParity(t *testing.T) { judge(t) }\n"
	parityJudg = "package parity\n\nfunc judge(t *testing.T) { deep(t) }\n\nfunc onlyOther() {}\n"
	parityDeep = "package parity\n\nfunc deep(t *testing.T) { deeper(t) }\n"
	parityDpr  = "package parity\n\nfunc deeper(t *testing.T) { deepest(t) }\n"
	parityDst  = "package parity\n\nfunc deepest(t *testing.T) {}\n"
)

func parityTree() tree {
	return tree{
		"tests/parity/lock_test.go":  parityTest,
		"tests/parity/helpers.go":    parityHelp,
		"tests/parity/judge.go":      parityJudg,
		"tests/parity/deep.go":       parityDeep,
		"tests/parity/deeper.go":     parityDpr,
		"tests/parity/deepest.go":    parityDst,
		"tests/parity/unrelated.go":  "package parity\n\nfunc unrelated() {}\n",
		"tests/parity/other_test.go": "package parity\n\nfunc TestNotALock(t *testing.T) {}\n",
	}
}

func TestLockCodeFiles_CallGraph(t *testing.T) {
	tr := parityTree()
	refs := lockTestRefs(lockRows("tests/parity/lock_test.go#TestLock"))
	got, err := LockCodeFiles(refs, tr.src, tr.list)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"tests/parity/lock_test.go", "tests/parity/helpers.go", "tests/parity/judge.go", "tests/parity/deep.go", "tests/parity/deeper.go"} {
		if _, ok := got[want]; !ok {
			t.Errorf("%s must be lock code: %v", want, got)
		}
	}
	for _, not := range []string{"tests/parity/deepest.go", "tests/parity/unrelated.go", "tests/parity/other_test.go"} {
		if _, ok := got[not]; ok {
			t.Errorf("%s is beyond the call depth or unrelated: %v", not, got)
		}
	}
	// onlyOther() is called by TestOther, which is no lock: it adds nothing to the set by itself
	if _, ok := got["tests/parity/judge.go"]; !ok {
		t.Error("judge.go is reached through RunParity")
	}
	// a bare file reference makes every test of the file a lock
	bare, _ := LockCodeFiles(lockTestRefs(lockRows("tests/parity/lock_test.go")), tr.src, tr.list)
	if _, ok := bare["tests/parity/judge.go"]; !ok {
		t.Error("a bare reference holds the helpers too")
	}
}

func TestLockCodeFiles_ProductPackagesKeepProductCodeOut(t *testing.T) {
	tr := tree{
		"internal/x/lock_test.go":   "package x\n\nfunc TestLock(t *testing.T) { Product(); helper() }\n",
		"internal/x/product.go":     "package x\n\nfunc Product() {}\n",
		"internal/x/helper_test.go": "package x\n\nfunc helper() {}\n",
	}
	got, err := LockCodeFiles(lockTestRefs(lockRows("internal/x/lock_test.go#TestLock")), tr.src, tr.list)
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := got["internal/x/product.go"]; ok {
		t.Errorf("product code is the code under test, not lock code: %v", got)
	}
	if _, ok := got["internal/x/helper_test.go"]; !ok {
		t.Errorf("a test helper is lock code: %v", got)
	}
}

func TestLockCodeFiles_ParseError(t *testing.T) {
	tr := tree{"tests/parity/lock_test.go": "package parity\nfunc ("}
	if _, err := LockCodeFiles(lockTestRefs(lockRows("tests/parity/lock_test.go#TestLock")), tr.src, tr.list); err == nil {
		t.Error("a package the gate cannot read is an error")
	}
}

func TestLockCodeChanges(t *testing.T) {
	base := parityTree()
	refs := lockTestRefs(lockRows("tests/parity/lock_test.go#TestLock"))
	edit := func(f func(tree)) tree {
		h := tree{}
		for k, v := range base {
			h[k] = v
		}
		f(h)
		return h
	}
	check := func(name string, head tree, want int, frag string) {
		t.Helper()
		got, err := LockCodeChanges(refs, refs, base.src, head.src, base.list, head.list)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != want || (frag != "" && (want == 0 || !strings.Contains(got[0], frag))) {
			t.Errorf("%s: %v", name, got)
		}
	}
	check("nothing", base, 0, "")
	check("LC1: the lock test returns early", edit(func(h tree) {
		h["tests/parity/lock_test.go"] = strings.Replace(parityTest, "RunParity(t)", "reportLockCells(t, 1000)\n\tif true {\n\t\treturn\n\t}\n\tRunParity(t)", 1)
	}), 1, "tests/parity/lock_test.go: lock code changed — owner review")
	check("LC4: the judging helper is gutted", edit(func(h tree) {
		h["tests/parity/judge.go"] = "package parity\n\nfunc judge(t *testing.T) {}\n\nfunc onlyOther() {}\n"
	}), 1, "tests/parity/judge.go: lock code changed — owner review")
	check("a helper deleted", edit(func(h tree) { delete(h, "tests/parity/deep.go") }), 1, "deep.go")
	check("a comment in a helper still counts", edit(func(h tree) { h["tests/parity/helpers.go"] += "// x\n" }), 1, "helpers.go")
	check("an unrelated file is free", edit(func(h tree) { h["tests/parity/unrelated.go"] += "// x\n" }), 0, "")
	check("a file beyond the call depth is free", edit(func(h tree) { h["tests/parity/deepest.go"] += "// x\n" }), 0, "")
	check("a non-lock test file is free", edit(func(h tree) { h["tests/parity/other_test.go"] += "// x\n" }), 0, "")
	check("a NEW test in a NEW file is free", edit(func(h tree) {
		h["tests/parity/new_test.go"] = "package parity\n\nfunc TestNew(t *testing.T) { RunParity(t) }\n"
	}), 0, "")
	check("a new non-test file in the lock's package", edit(func(h tree) { h["tests/parity/zz.go"] = "package parity\n\nfunc zz() {}\n" }), 1, "a new non-test file")
	check("the init() goroutine trick: a new test file with init()", edit(func(h tree) {
		h["tests/parity/zz_test.go"] = "package parity\n\nfunc init() { go func() {}() }\n"
	}), 1, "a new file with init()")
	check("a new test file without init() is free", edit(func(h tree) { h["tests/parity/zz_test.go"] = "package parity\n\nfunc helperOnly() {}\n" }), 0, "")
	// a file that is lock code at head only (a row newly referencing it) is judged by its change too
	headRefs := lockTestRefs(lockRows("tests/parity/lock_test.go#TestLock", "tests/parity/other_test.go#TestNotALock"))
	got, err := LockCodeChanges(refs, headRefs, base.src, edit(func(h tree) { h["tests/parity/other_test.go"] += "// x\n" }).src, base.list, base.list)
	if err != nil || len(got) != 1 || !strings.Contains(got[0], "other_test.go") {
		t.Errorf("a newly referenced file edited in the same PR: %v %v", got, err)
	}
}

func TestLockTestRefs_OnlyExactRowsAndTestFiles(t *testing.T) {
	rows := map[string]RowLite{
		"a": {ID: "a", Expect: "pass", Compare: "exact-json", CompareMap: map[string]any{"type": "exact-json"}, Tests: []string{"x/a_test.go#TestA", "x/b_test.go", "docs/x.md#anchor", "scripts/t.py"}},
		"b": {ID: "b", Expect: "pass", Compare: "status", CompareMap: map[string]any{"type": "status"}, Tests: []string{"x/c_test.go#TestC"}},
		"c": {ID: "c", Expect: "differ", Compare: "exact-json", CompareMap: map[string]any{"type": "exact-json"}, Tests: []string{"x/d_test.go#TestD"}},
	}
	got := lockTestRefs(rows)
	if len(got) != 2 || !got["x/a_test.go"]["TestA"] || !got["x/b_test.go"][""] {
		t.Errorf("%v", got)
	}
}
