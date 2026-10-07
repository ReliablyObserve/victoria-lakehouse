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
	refs := LockTestRefs(lockRows("tests/parity/lock_test.go#TestLock"))
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
	bare, _ := LockCodeFiles(LockTestRefs(lockRows("tests/parity/lock_test.go")), tr.src, tr.list)
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
	got, err := LockCodeFiles(LockTestRefs(lockRows("internal/x/lock_test.go#TestLock")), tr.src, tr.list)
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
	if _, err := LockCodeFiles(LockTestRefs(lockRows("tests/parity/lock_test.go#TestLock")), tr.src, tr.list); err == nil {
		t.Error("a package the gate cannot read is an error")
	}
}

func (tr tree) files(prefix string) []string {
	var out []string
	for p := range tr {
		if p == prefix || strings.HasSuffix(prefix, "/") && strings.HasPrefix(p, prefix) {
			out = append(out, p)
		}
	}
	sort.Strings(out)
	return out
}

func TestLockCodeChanges(t *testing.T) {
	base := parityTree()
	base["tests/parity/docker-compose.yml"] = "LH_BASE_URL: lh\n"
	base["tests/parity/testdata/seed.json"] = "{}"
	base["tests/parity/lock_cells.txt"] = "TestLock 1\n"
	base["deployment/docker/docker-compose-e2e.yml"] = "x: 1\n"
	base["cmd/datagen/main.go"] = "package main\n"
	refs := LockTestRefs(lockRows("tests/parity/lock_test.go#TestLock"))
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
		got, err := LockCodeChanges(refs, refs, base.src, head.src, base.list, head.list, base.files, head.files)
		if err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		if len(got) != want || (frag != "" && (want == 0 || !strings.Contains(got[0], frag))) {
			t.Errorf("%s: %v", name, got)
		}
	}
	check("nothing", base, 0, "")
	check("LC1: the lock test returns early", edit(func(h tree) {
		h["tests/parity/lock_test.go"] = strings.Replace(parityTest, "RunParity(t)", "if true {\n\t\treturn\n\t}\n\tRunParity(t)", 1)
	}), 1, "tests/parity/lock_test.go: lock code changed — owner review")
	check("the judging helper is gutted", edit(func(h tree) { h["tests/parity/judge.go"] = "package parity\n\nfunc judge(t *testing.T) {}\n" }), 1, "judge.go")
	check("a helper deleted", edit(func(h tree) { delete(h, "tests/parity/deep.go") }), 1, "deep.go")
	check("G2: an init() appended to an existing non-lock test file", edit(func(h tree) {
		h["tests/parity/other_test.go"] += "\nfunc init() { lhBaseURL = vlBaseURL }\n"
	}), 1, "other_test.go")
	check("a comment in an unrelated file of the package still counts", edit(func(h tree) { h["tests/parity/unrelated.go"] += "// x\n" }), 1, "unrelated.go")
	check("a file beyond the call depth still counts: the whole package is gated", edit(func(h tree) { h["tests/parity/deepest.go"] += "// x\n" }), 1, "deepest.go")
	check("a NEW test in a NEW file is free", edit(func(h tree) {
		h["tests/parity/new_test.go"] = "package parity\n\nfunc TestNew(t *testing.T) { RunParity(t) }\n\nfunc helperNew() {}\n\nconst k = 1\n\ntype T struct{}\n"
	}), 0, "")
	check("G1: a new file with a package-level var initializer", edit(func(h tree) {
		h["tests/parity/zz_env_test.go"] = "package parity\n\nvar _ = func() int { lhBaseURL = vlBaseURL; return 0 }()\n"
	}), 1, "declares a package-level var")
	check("a new file with a plain var", edit(func(h tree) { h["tests/parity/zz.go"] = "package parity\n\nvar x = 1\n" }), 1, "zz.go")
	check("a new file with init()", edit(func(h tree) { h["tests/parity/zz_test.go"] = "package parity\n\nfunc init() { go func() {}() }\n" }), 1, "declares func init")
	check("a new file with TestMain", edit(func(h tree) { h["tests/parity/zz_test.go"] = "package parity\n\nfunc TestMain(m *testing.M) {}\n" }), 1, "declares func TestMain")
	check("a new file that does not parse", edit(func(h tree) { h["tests/parity/zz_test.go"] = "package parity\nfunc (" }), 1, "does not parse")
	check("a new non-lock-package test file is free", edit(func(h tree) { h["internal/x/new_test.go"] = "package x\n\nvar v = 1\nfunc init() {}\n" }), 0, "")
	check("G3: the parity compose file", edit(func(h tree) { h["tests/parity/docker-compose.yml"] = "LH_BASE_URL: vl\n" }), 1, "docker-compose.yml")
	check("a parity seed file deleted", edit(func(h tree) { delete(h, "tests/parity/testdata/seed.json") }), 1, "seed.json")
	check("the e2e compose file", edit(func(h tree) { h["deployment/docker/docker-compose-e2e.yml"] = "x: 2\n" }), 1, "docker-compose-e2e.yml")
	check("the data generator", edit(func(h tree) { h["cmd/datagen/main.go"] += "// x\n" }), 1, "cmd/datagen/main.go")
	check("a floor may grow without the owner", edit(func(h tree) { h["tests/parity/lock_cells.txt"] = "TestLock 5\n" }), 0, "")
	// a lock added by this PR is not protected yet: editing the test it names is how a parity fix is written
	headRefs := LockTestRefs(lockRows("tests/parity/lock_test.go#TestLock", "tests/other/x_test.go#TestX"))
	h := edit(func(h tree) { h["tests/other/x_test.go"] = "package other\n" })
	b2 := tree{}
	for k, v := range base {
		b2[k] = v
	}
	b2["tests/other/x_test.go"] = "package other\n"
	h["tests/other/x_test.go"] = "package other\n\n// edited\n"
	got, err := LockCodeChanges(refs, headRefs, b2.src, h.src, b2.list, h.list, b2.files, h.files)
	if err != nil || len(got) != 0 {
		t.Errorf("a newly added lock does not protect its package yet: %v %v", got, err)
	}
}

func TestLockTestRefs_OnlyExactRowsAndTestFiles(t *testing.T) {
	rows := map[string]RowLite{
		"a": {ID: "a", Expect: "pass", Compare: "exact-json", CompareMap: map[string]any{"type": "exact-json"}, Tests: []string{"x/a_test.go#TestA", "x/b_test.go", "docs/x.md#anchor", "scripts/t.py"}},
		"b": {ID: "b", Expect: "pass", Compare: "status", CompareMap: map[string]any{"type": "status"}, Tests: []string{"x/c_test.go#TestC"}},
		"c": {ID: "c", Expect: "differ", Compare: "exact-json", CompareMap: map[string]any{"type": "exact-json"}, Tests: []string{"x/d_test.go#TestD"}},
	}
	got := LockTestRefs(rows)
	if len(got) != 2 || !got["x/a_test.go"]["TestA"] || !got["x/b_test.go"][""] {
		t.Errorf("%v", got)
	}
}

func TestLockCodeChanges_ProductLockPackage(t *testing.T) {
	base := tree{"internal/p/p_test.go": "package p\n\nimport \"testing\"\n\nfunc TestLock(t *testing.T) {}\n", "internal/p/impl.go": "package p\n\nfunc F() {}\n"}
	refs := LockTestRefs(lockRows("internal/p/p_test.go#TestLock"))
	run := func(head tree) []string {
		got, err := LockCodeChanges(refs, refs, base.src, head.src, base.list, head.list, base.files, head.files)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	cp := func() tree {
		h := tree{}
		for k, v := range base {
			h[k] = v
		}
		return h
	}
	head := cp()
	head["internal/p/impl.go"] = "package p\n\nfunc F() { println() }\n"
	if got := run(head); len(got) != 0 {
		t.Errorf("a product file edit in a lock package is free: %v", got)
	}
	head = cp()
	head["internal/p/p_test.go"] += "// x\n"
	if got := run(head); len(got) != 1 || !strings.Contains(got[0], "p_test.go") {
		t.Errorf("an existing test file of a lock package is gated: %v", got)
	}
	head = cp()
	delete(head, "internal/p/p_test.go")
	if got := run(head); len(got) != 1 {
		t.Errorf("deleting a lock package test file is gated: %v", got)
	}
	head = cp()
	head["internal/p/new.go"] = "package p\n\nvar V = 1\n\nfunc init() {}\n"
	if got := run(head); len(got) != 0 {
		t.Errorf("a new non-test product file is free, whatever it declares: %v", got)
	}
	head = cp()
	head["internal/p/new_test.go"] = "package p\n\nfunc TestNew(t *testing.T) {}\n\nfunc helper() {}\n"
	if got := run(head); len(got) != 0 {
		t.Errorf("a new func-only test file is free: %v", got)
	}
	head["internal/p/new_test.go"] = "package p\n\nvar V = 1\n"
	if got := run(head); len(got) != 1 {
		t.Errorf("a new test file with a package var is gated: %v", got)
	}
	head["internal/p/new_test.go"] = "package p\n\nfunc TestMain(m *testing.M) {}\n"
	if got := run(head); len(got) != 1 {
		t.Errorf("a new test file with TestMain is gated: %v", got)
	}
}
