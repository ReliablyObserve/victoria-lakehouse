package registry

import (
	"encoding/json"
	"os"
	"reflect"
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
	}), 1, "tests/parity/lock_test.go: it holds a lock test")
	check("the judging helper is gutted", edit(func(h tree) { h["tests/parity/judge.go"] = "package parity\n\nfunc judge(t *testing.T) {}\n" }), 1, "judge.go")
	check("a helper deleted", edit(func(h tree) { delete(h, "tests/parity/deep.go") }), 1, "deep.go")
	check("G2: an init() appended to an existing non-lock test file", edit(func(h tree) {
		h["tests/parity/other_test.go"] += "\nfunc init() { lhBaseURL = vlBaseURL }\n"
	}), 1, "other_test.go")
	check("a comment in an unrelated file of the package still counts", edit(func(h tree) { h["tests/parity/unrelated.go"] += "// x\n" }), 1, "unrelated.go")
	check("a file beyond the call depth still counts: the whole package is gated", edit(func(h tree) { h["tests/parity/deepest.go"] += "// x\n" }), 1, "deepest.go")
	check("a NEW test in a NEW file is listed: a func-only test file still runs inside the lock package", edit(func(h tree) {
		h["tests/parity/new_test.go"] = "package parity\n\nfunc TestNew(t *testing.T) { RunParity(t) }\n\nfunc helperNew() {}\n\nconst k = 1\n\ntype T struct{}\n"
	}), 1, "tests/parity/new_test.go: a new Go file in a package that holds a registry lock")
	// M-A of the second review: shapes that were neither failed nor listed
	check("M-A1: a warm-up test that repoints the shared base URLs", edit(func(h tree) {
		h["tests/parity/aaa_warmup_test.go"] = "package parity\n\nfunc TestAAAWarmup(t *testing.T) {\n\tvlBaseURL = lhBaseURL\n\tvtBaseURL = lhtBaseURL\n}\n"
	}), 1, "tests/parity/aaa_warmup_test.go: a new Go file")
	check("M-A2: a test that prints a forged transcript and exits with success", edit(func(h tree) {
		h["tests/parity/zz_forged_test.go"] = "package parity\n\nimport (\n\t\"fmt\"\n\t\"os\"\n\t\"syscall\"\n)\n\nfunc TestForged(t *testing.T) {\n\tfmt.Println(\"lock-cells\", os.Getenv(\"PARITY_LOCK_NONCE\"), \"TestLock 50\")\n\tsyscall.Exit(0)\n}\n"
	}), 1, "tests/parity/zz_forged_test.go")
	check("M-A3: a blank import of a new package is listed on the file and names the import", edit(func(h tree) {
		h["tests/parity/zz_hook_test.go"] = "package parity\n\nimport _ \"github.com/ReliablyObserve/victoria-lakehouse/tests/parityhook\"\n"
	}), 1, `blank-imports "github.com/ReliablyObserve/victoria-lakehouse/tests/parityhook"`)
	check("M-A3b: a new non-stdlib import added to an EXISTING file is named, next to the existing-file reason", edit(func(h tree) {
		h["tests/parity/unrelated.go"] = "package parity\n\nimport \"example.com/evil/hook\"\n\nfunc unrelated() { hook.Run() }\n"
	}), 1, `imports "example.com/evil/hook"`)
	check("M-A3c: an import some file of the package already had at the merge base is not new", edit(func(h tree) {
		base["tests/parity/imports_base.go"] = "package parity\n\nimport \"example.com/known/lib\"\n\nvar _ = lib.X\n"
		h["tests/parity/imports_base.go"] = base["tests/parity/imports_base.go"]
		h["tests/parity/new_use_test.go"] = "package parity\n\nimport \"example.com/known/lib\"\n\nfunc TestUse(t *testing.T) { _ = lib.X }\n"
	}), 1, "a new Go file")
	delete(base, "tests/parity/imports_base.go")
	check("M-A4: a new C source file in the lock package directory", edit(func(h tree) { h["tests/parity/ctor.c"] = "void __attribute__((constructor)) f(void) {}\n" }), 1, "ctor.c: a new native source file")
	check("M-A4b: a prebuilt .syso object", edit(func(h tree) { h["tests/parity/hook.syso"] = "\x00" }), 1, "hook.syso")
	check("a new testdata file is not a native source", edit(func(h tree) { h["tests/parity/testdata/data.json"] = "{}" }), 0, "")
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
	if got := run(head); len(got) != 1 || !strings.Contains(got[0], "internal/p/new.go: a new Go file in a package that holds a registry lock: it is linked into the package's test binary") {
		t.Errorf("a new non-test product file of a lock package is listed (with the neutral reason): %v", got)
	}
	head = cp()
	head["internal/p/new_test.go"] = "package p\n\nfunc TestNew(t *testing.T) {}\n\nfunc helper() {}\n"
	if got := run(head); len(got) != 1 || !strings.Contains(got[0], "internal/p/new_test.go") {
		t.Errorf("a new func-only test file of a lock package is listed: %v", got)
	}
	head = cp()
	head["internal/p/new.go"] = "package p\n\nimport _ \"example.com/evil/hook\"\n\nfunc G() {}\n"
	if got := run(head); len(got) != 1 || !strings.Contains(got[0], `blank-imports "example.com/evil/hook"`) {
		t.Errorf("a product file of a lock package that blank-imports a package nobody imported before is listed: %v", got)
	}
	head = cp()
	head["internal/p/impl.go"] = "package p\n\nimport _ \"example.com/evil/hook\"\n\nfunc F() {}\n"
	if got := run(head); len(got) != 1 || !strings.Contains(got[0], `internal/p/impl.go: blank-imports "example.com/evil/hook"`) {
		t.Errorf("an EXISTING product file of a lock package that starts blank-importing a new package is listed: %v", got)
	}
	head = cp()
	head["internal/p/impl.go"] = "package p\n\nimport \"fmt\"\n\nfunc F() { fmt.Println() }\n"
	if got := run(head); len(got) != 0 {
		t.Errorf("editing a product file to import only the standard library is free: %v", got)
	}
	head = cp()
	head["internal/p/new_test.go"] = "package p\n\nfunc TestNew(t *testing.T) {}\n\nfunc helper() {}\n"
	head["internal/p/new_test.go"] = "package p\n\nvar V = 1\n"
	if got := run(head); len(got) != 1 {
		t.Errorf("a new test file with a package var is gated: %v", got)
	}
	head["internal/p/new_test.go"] = "package p\n\nfunc TestMain(m *testing.M) {}\n"
	if got := run(head); len(got) != 1 {
		t.Errorf("a new test file with TestMain is gated: %v", got)
	}
}

// The #491 compose diff: a Go toolchain image tag moved and nothing else.
const composeBase = "services:\n  build:\n    image: golang:1.27.1\n    environment:\n      LH_BASE_URL: lh\n"

func TestGolangImageBumpOnly(t *testing.T) {
	cases := []struct {
		name string
		head string
		want bool
	}{
		{"the toolchain image tag moved", strings.Replace(composeBase, "golang:1.27.1", "golang:1.27.2", 1), true},
		{"unchanged", composeBase, false},
		{"another image family", strings.Replace(composeBase, "golang:1.27.1", "node:1.27.2", 1), false},
		{"a suffix appears", strings.Replace(composeBase, "golang:1.27.1", "golang:1.27.2-alpine", 1), false},
		{"a flag line is added", composeBase + "    command: x\n", false},
		{"the environment changed too", strings.Replace(strings.Replace(composeBase, "golang:1.27.1", "golang:1.27.2", 1), "LH_BASE_URL: lh", "LH_BASE_URL: vl", 1), false},
		{"a digest instead of a version", strings.Replace(composeBase, "golang:1.27.1", "golang:1.27.2@sha256:abcd", 1), false},
		{"the indentation changed", strings.Replace(strings.Replace(composeBase, "golang:1.27.1", "golang:1.27.2", 1), "    image:", "  image:", 1), false},
	}
	for _, c := range cases {
		if got := GolangImageBumpOnly([]byte(composeBase), []byte(c.head)); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
	alpBase := "    image: golang:1.27.1-alpine3.23\n"
	if !GolangImageBumpOnly([]byte(alpBase), []byte("    image: golang:1.27.2-alpine3.24\n")) {
		t.Error("an alpine release may move together with the Go version")
	}
	if GolangImageBumpOnly([]byte(alpBase), []byte("    image: golang:1.27.2-alpine\n")) {
		t.Error("the suffix shape may not change")
	}
}

func TestLockCodeChanges_ParityComposeToolchainImageBumpIsNotLockCode(t *testing.T) {
	base := tree{"tests/parity/lock_test.go": parityTest, "tests/parity/docker-compose.yml": composeBase}
	refs := LockTestRefs(lockRows("tests/parity/lock_test.go#TestLock"))
	run := func(compose string) []string {
		head := tree{"tests/parity/lock_test.go": parityTest, "tests/parity/docker-compose.yml": compose}
		got, err := LockCodeChanges(refs, refs, base.src, head.src, base.list, head.list, base.files, head.files)
		if err != nil {
			t.Fatal(err)
		}
		return got
	}
	if got := run(strings.Replace(composeBase, "golang:1.27.1", "golang:1.27.2", 1)); len(got) != 0 {
		t.Errorf("a toolchain image tag bump of the parity compose file is not lock code: %v", got)
	}
	if got := run(strings.Replace(composeBase, "LH_BASE_URL: lh", "LH_BASE_URL: vl", 1)); len(got) != 1 {
		t.Errorf("any other edit of the parity compose file is lock code: %v", got)
	}
}

// The compose cases of the fixture that scripts/ci/pr_classify.py is tested against too:
// the two twins of the rule cannot drift apart without one of the tests failing.
func TestGolangImageBumpOnly_SharedFixture(t *testing.T) {
	raw, err := os.ReadFile("../../../scripts/ci/tests/testdata/golang_image_cases.json")
	if err != nil {
		t.Fatal(err)
	}
	var fx struct {
		Cases []struct {
			Name, Kind, Base, Head string
			Want                   bool
		}
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, c := range fx.Cases {
		if c.Kind != "compose" {
			continue
		}
		n++
		if got := GolangImageBumpOnly([]byte(c.Base), []byte(c.Head)); got != c.Want {
			t.Errorf("%s: got %v, want %v", c.Name, got, c.Want)
		}
	}
	if n < 12 {
		t.Errorf("the shared fixture has only %d compose cases", n)
	}
}

// The reviewer's smuggles: a lone CR, a Unicode digit, a Unicode separator and invalid UTF-8 are
// never a verified image bump, whatever the rest of the file says.
func TestGolangImageBumpOnly_UnreadableBytesAreNeverABump(t *testing.T) {
	for name, head := range map[string]string{
		"a CR after the bumped line":           "    image: golang:1.27.2\r\n    command: x\n",
		"a CR instead of the LF":               "    image: golang:1.27.2\r    command: x\n",
		"an Arabic-Indic digit in the version": "    image: golang:1.27.\u0663\n    command: x\n",
		"a Unicode line separator":             "    image: golang:1.27.2\u2028    command: x\n",
		"invalid UTF-8":                        "    image: golang:1.27.2\n    command: x\xff\n",
		"a BOM":                                "\ufeff    image: golang:1.27.2\n    command: x\n",
	} {
		if GolangImageBumpOnly([]byte("    image: golang:1.27.1\n    command: x\n"), []byte(head)) {
			t.Errorf("%s must not be a verified bump", name)
		}
	}
	// the same file with CR in BOTH revisions is still not read as LF text
	crlf := "    image: golang:1.27.1\r\n"
	if GolangImageBumpOnly([]byte(crlf), []byte(strings.Replace(crlf, "1.27.1", "1.27.2", 1))) {
		t.Error("a CRLF file is not verified")
	}
	// an em dash in an untouched comment line is fine (the compose file of this repository holds some)
	base := "    # a \u2014 b\n    image: golang:1.27.1\n"
	if !GolangImageBumpOnly([]byte(base), []byte(strings.Replace(base, "1.27.1", "1.27.2", 1))) {
		t.Error("an unchanged UTF-8 comment must not block a bump")
	}
}

func lockInitVar(baseTree, headTree tree) []string {
	refs := LockTestRefs(lockRows("tests/parity/lock_test.go#TestLock", "internal/p/p_test.go#TestP"))
	return LockInitVarAdditions(refs, baseTree.src, headTree.src, baseTree.list, headTree.list)
}

func TestLockInitVarAdditions(t *testing.T) {
	base := func() tree {
		tr := parityTree()
		tr["tests/parity/vars.go"] = "package parity\n\nvar cases = []int{1}\n\nvar _ = compute()\n\nfunc init() {}\n"
		tr["internal/p/p_test.go"] = "package p\n\nfunc TestP(t *testing.T) {}\n"
		tr["internal/p/p.go"] = "package p\n\nfunc F() {}\n"
		return tr
	}
	cp := func(mod func(tr tree)) tree { tr := base(); mod(tr); return tr }
	newFile := func(src string) tree {
		return cp(func(tr tree) { tr["tests/parity/zz.go"] = "package parity\n\n" + src })
	}
	cases := []struct {
		name string
		head tree
		want int // number of flagged files
		frag string
	}{
		{"nothing changed", base(), 0, ""},
		{"init_exit: a new test file whose init() exits when the nonce is set", cp(func(tr tree) {
			tr["tests/parity/zz_env_test.go"] = "package parity\n\nimport \"os\"\n\nfunc init() {\n\tif os.Getenv(\"PARITY_LOCK_NONCE\") != \"\" {\n\t\tos.Exit(0)\n\t}\n}\n"
		}), 1, "adds 1 new init() function(s)"},
		{"a new file with a var initialized by a called func literal", newFile("var _ = func() int { return 0 }()\n"), 1, "package-level var whose initializer calls a function: _ = func() int { return 0 }()"},
		{"a var initialized by a package func", newFile("var x = compute()\n"), 1, "x = compute()"},
		{"a var initialized by a stdlib constructor", newFile("var re = regexp.MustCompile(\"x\")\nvar e = errors.New(\"x\")\n"), 1, "re = regexp.MustCompile"},
		{"a parenthesised call of os.Exit", newFile("var _ = (os.Exit)(0)\n"), 1, "(os.Exit)(0)"},
		{"regexp.MustCompile and errors.New of the standard library are pure constructors", newFile("import (\n\t\"errors\"\n\t\"regexp\"\n)\n\nvar re = regexp.MustCompile(`^a+$`)\nvar e = errors.New(\"x\")\n"), 0, ""},
		{"a regexp.MustCompile whose argument calls a function is not", newFile("import \"regexp\"\n\nvar re = regexp.MustCompile(pattern())\n"), 1, "re = regexp.MustCompile(pattern())"},
		{"a package that is not the standard library regexp gets no exemption", newFile("import regexp \"example.com/evil/regexp\"\n\nvar re = regexp.MustCompile(\"x\")\n"), 1, "re = regexp.MustCompile"},
		{"the stdlib name without its import is a call like any other", newFile("var re = regexp.MustCompile(\"x\")\n"), 1, "re = regexp.MustCompile"},
		{"an aliased stdlib import keeps the exemption", newFile("import rx \"regexp\"\n\nvar re = rx.MustCompile(\"x\")\n"), 0, ""},
		{"a call buried in a composite literal", newFile("var tbl = []int{1, f(), 3}\n"), 1, "tbl = []int{1, f(), 3}"},
		{"a call to a user type name looks like a function call and is flagged", newFile("var v = T(3)\n"), 1, "v = T(3)"},
		{"an init() added to an existing lock file", cp(func(tr tree) { tr["tests/parity/lock_test.go"] += "\nfunc init() {}\n" }), 1, "tests/parity/lock_test.go"},
		{"a called var added to an existing helper file", cp(func(tr tree) { tr["tests/parity/helpers.go"] += "\nvar x = compute()\n" }), 1, "x = compute()"},
		{"a second identical blank call-var in a file that had one", cp(func(tr tree) { tr["tests/parity/vars.go"] += "\nvar _ = compute()\n" }), 1, "_ = compute()"},
		{"a second init() in a file that had one", cp(func(tr tree) { tr["tests/parity/vars.go"] += "\nfunc init() {}\n" }), 1, "1 new init()"},
		{"a call var inside a grouped declaration", newFile("var (\n\ta = 1\n\tb = f()\n)\n"), 1, "b = f()"},
		{"a literal var initializer edited into a call", cp(func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n\nvar cases = load()\n\nvar _ = compute()\n\nfunc init() {}\n"
		}), 1, "cases = load()"},
		{"a func named like a builtin makes a later `var _ = len(x)` a call of that func: the declaration is refused", cp(func(tr tree) {
			tr["tests/parity/zz.go"] = "package parity\n\nfunc len(x any) int { os.Exit(0); return 0 }\n"
		}), 1, "declares len, which shadows a predeclared identifier"},
		{"a type, var or const named like a predeclared type is refused too", newFile("type string struct{}\n"), 1, "declares string"},
		{"shadowing from a NON-test file of a product package is refused as well", cp(func(tr tree) { tr["internal/p/shadow.go"] = "package p\n\nfunc make(n int) int { return n }\n" }), 1, "internal/p/shadow.go: declares make"},
		// free
		{"a literal table is not load-time code (#438's var fvPropMaps)", newFile("var fvPropMaps = []fvPropMap{{\"resource.attributes\", \"\"}, {\"log.attributes\", \"\"}}\n"), 0, ""},
		{"scalars, maps, conversions, builtins and a bare declaration are free", newFile("var n = 3\nvar m = map[string]int{\"a\": 1}\nvar b = []byte(\"x\")\nvar l = len(m)\nvar f64 = float64(n)\nvar x int\nvar d = 5 * time.Second\nvar mk = make([]int, 3)\n"), 0, ""},
		{"a func value that is never called at load is free", newFile("var hook = func() { os.Exit(0) }\n"), 0, ""},
		{"an existing literal table edited in place is only listed as lock-affecting, not failed", cp(func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n\nvar cases = []int{1, 2, 3}\n\nvar _ = compute()\n\nfunc init() {}\n"
		}), 0, ""},
		{"removing a var and an init() is free", cp(func(tr tree) { tr["tests/parity/vars.go"] = "package parity\n" }), 0, ""},
		{"funcs, consts and types are free", newFile("const K = 1\n\ntype T struct{}\n\nfunc Helper() {}\n\nfunc TestNew(t *testing.T) { var local = f(); _ = local }\n"), 0, ""},
		{"a local var inside a func is free", newFile("func f() { var x = g(); _ = x }\n"), 0, ""},
		{"a method named init is not init()", newFile("type T struct{}\n\nfunc (T) init() {}\n"), 0, ""},
		{"a new file that does not parse", newFile("func (\n"), 1, "does not parse"},
		// minor 5: load-time declarations are a multiset per package, so a pure move or rename is not an addition
		{"a call-var moved to a NEW file is a pure move", cp(func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n\nvar cases = []int{1}\n\nfunc init() {}\n"
			tr["tests/parity/vars_moved.go"] = "package parity\n\nvar _ = compute()\n"
		}), 0, ""},
		{"the file holding the load-time declarations is renamed", cp(func(tr tree) {
			tr["tests/parity/renamed.go"] = tr["tests/parity/vars.go"]
			delete(tr, "tests/parity/vars.go")
		}), 0, ""},
		{"a var and an init() moved to a new file together", cp(func(tr tree) {
			tr["tests/parity/moved.go"] = "package parity\n\nvar _ = compute()\n\nfunc init() {}\n"
			tr["tests/parity/vars.go"] = "package parity\n\nvar cases = []int{1}\n"
		}), 0, ""},
		{"an init() deleted from one file and a DIFFERENT init() added in a new one is an addition", cp(func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n\nvar cases = []int{1}\n\nvar _ = compute()\n"
			tr["tests/parity/zz_env_test.go"] = "package parity\n\nfunc init() { os.Exit(0) }\n"
		}), 1, "tests/parity/zz_env_test.go: adds 1 new init() function(s)"},
		{"a call-var moved AND changed on the way is an addition", cp(func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n\nvar cases = []int{1}\n\nfunc init() {}\n"
			tr["tests/parity/vars_moved.go"] = "package parity\n\nvar _ = compute2()\n"
		}), 1, "_ = compute2()"},
		{"one more copy of an existing call-var in a new file is an addition", cp(func(tr tree) {
			tr["tests/parity/vars_copy.go"] = "package parity\n\nvar _ = compute()\n"
		}), 1, "tests/parity/vars_copy.go"},
		{"an init() with a different body in the same file is an addition", cp(func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n\nvar cases = []int{1}\n\nvar _ = compute()\n\nfunc init() { os.Exit(0) }\n"
		}), 1, "1 new init()"},
		{"a product package: a new non-test file with init and call-var is product code", cp(func(tr tree) {
			tr["internal/p/new.go"] = "package p\n\nvar V = f()\n\nfunc init() {}\n"
		}), 0, ""},
		{"a product package: a new _test.go with a call-var is not free", cp(func(tr tree) { tr["internal/p/v_test.go"] = "package p\n\nvar V = f()\n" }), 1, "internal/p/v_test.go"},
		{"a product package: a new _test.go with a literal table is free", cp(func(tr tree) { tr["internal/p/v_test.go"] = "package p\n\nvar V = []int{1}\n" }), 0, ""},
		{"a product package: an init() added to an existing _test.go is not free", cp(func(tr tree) { tr["internal/p/p_test.go"] += "\nfunc init() {}\n" }), 1, "internal/p/p_test.go"},
		{"a package without a lock is free", cp(func(tr tree) { tr["tests/other/zz_test.go"] = "package other\n\nvar V = f()\n\nfunc init() {}\n" }), 0, ""},
	}
	for _, c := range cases {
		got := lockInitVar(base(), c.head)
		if len(got) != c.want {
			t.Errorf("%s: got %d flagged %v, want %d", c.name, len(got), got, c.want)
			continue
		}
		if c.frag != "" && !strings.Contains(strings.Join(got, "\n"), c.frag) {
			t.Errorf("%s: %v lacks %q", c.name, got, c.frag)
		}
	}
}

// A shadow the merge base already had is not an addition: only new declarations are refused.
func TestLockInitVarAdditions_ShadowAlreadyAtBase(t *testing.T) {
	tr := parityTree()
	tr["tests/parity/old_shadow.go"] = "package parity\n\nfunc len(x any) int { return 0 }\n"
	if got := lockInitVar(tr, tr); len(got) != 0 {
		t.Errorf("unchanged tree: %v", got)
	}
}

func lockCgo(baseTree, headTree tree) []string {
	refs := LockTestRefs(lockRows("tests/parity/lock_test.go#TestLock", "internal/p/p_test.go#TestP"))
	return LockCgoAdditions(refs, baseTree.src, headTree.src, baseTree.list, headTree.list)
}

// import "C" in a package that holds a lock is load-time code a parser-level rule cannot see (a C
// constructor runs before main): a hard failure, for any .go file of the package, product files included.
func TestLockCgoAdditions(t *testing.T) {
	base := func() tree {
		tr := parityTree()
		tr["internal/p/p_test.go"] = "package p\n\nfunc TestP(t *testing.T) {}\n"
		tr["internal/p/p.go"] = "package p\n\nfunc F() {}\n"
		return tr
	}
	cp := func(mod func(tr tree)) tree { tr := base(); mod(tr); return tr }
	cgo := "package parity\n\n/*\nvoid __attribute__((constructor)) boot(void) { _exit(0); }\n*/\nimport \"C\"\n"
	cases := []struct {
		name string
		head tree
		want int
	}{
		{"nothing changed", base(), 0},
		{"a new non-test file with import C in a pure test package", cp(func(tr tree) { tr["tests/parity/zz_cgo.go"] = cgo }), 1},
		{"a new test file with import C", cp(func(tr tree) { tr["tests/parity/zz_cgo_test.go"] = cgo }), 1},
		{"import C added to an existing file", cp(func(tr tree) { tr["tests/parity/unrelated.go"] = cgo }), 1},
		{"import C in a grouped import", cp(func(tr tree) {
			tr["tests/parity/zz.go"] = "package parity\n\n// #include <stdlib.h>\nimport (\n\t\"C\"\n\t\"fmt\"\n)\n"
		}), 1},
		{"a product file of a lock package", cp(func(tr tree) { tr["internal/p/cgo.go"] = "package p\n\nimport \"C\"\n" }), 1},
		{"a package that holds no lock", cp(func(tr tree) { tr["tests/other/cgo.go"] = "package other\n\nimport \"C\"\n" }), 0},
		{"a file that already imported C at the merge base is not new cgo", nil, 0},
		{"an alias is no way around it", cp(func(tr tree) { tr["tests/parity/zz.go"] = "package parity\n\nimport c \"C\"\n" }), 1},
		{"a string mentioning import C is not an import", cp(func(tr tree) { tr["tests/parity/zz.go"] = "package parity\n\nvar s = `import \"C\"`\n" }), 0},
	}
	for _, c := range cases {
		b, h := base(), c.head
		if h == nil { // a base that already had cgo
			b["tests/parity/legacy_cgo.go"] = "package parity\n\nimport \"C\"\n"
			h = tree{}
			for k, v := range b {
				h[k] = v
			}
			h["tests/parity/legacy_cgo.go"] += "\nfunc more() {}\n"
		}
		if got := lockCgo(b, h); len(got) != c.want {
			t.Errorf("%s: got %v, want %d", c.name, got, c.want)
		}
	}
}

// ---- third review of the gate ----------------------------------------------------------------------

func cloneTree(tr tree, mod func(tree)) tree {
	h := tree{}
	for k, v := range tr {
		h[k] = v
	}
	if mod != nil {
		mod(h)
	}
	return h
}

// MAJOR-2: the key of a moved load-time declaration holds what decides what it runs.
func TestLoadTimeKey_ResolvesWhatADeclarationMeans(t *testing.T) {
	const real = "package parity\n\nimport schema \"example.com/real/schema\"\n\nvar fixed = schema.Estimate(1)\n"
	const bare = "package parity\n\nimport \"example.com/real/schema\"\n\nvar fixed = schema.Estimate(1)\n"
	mk := func(vars string) tree {
		tr := parityTree()
		tr["tests/parity/vars.go"] = vars
		// a repository package whose clause (schema) is not the last element of its path
		tr["tests/realpkg/r.go"] = "package schema\n\nfunc Estimate(int) int { return 1 }\n"
		return tr
	}
	cases := []struct {
		name string
		base string
		head func(tr tree)
		want int
		frag string
	}{
		{"a pure move with identical imports is free", real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = real
		}, 0, ""},
		{"a pure move with reformatting and a comment is free", "package parity\n\nvar x = f(1, 2)\n", func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "package parity\n\n// moved\nvar x = f(1,\n\t2) // note\n"
		}, 0, ""},
		{"alias swap: the alias now names another package", real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "package parity\n\nimport schema \"example.com/evil/schema\"\n\nvar fixed = schema.Estimate(1)\n"
		}, 1, "fixed = schema.Estimate(1)"},
		{"alias swap with no alias: the package CLAUSE of a repository package is chosen by the author", bare, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/evilpkg/x.go"] = "package schema\n\nfunc Estimate(int) int { return 0 }\n"
			tr["tests/parity/moved.go"] = "package parity\n\nimport \"github.com/ReliablyObserve/victoria-lakehouse/tests/evilpkg\"\n\nvar fixed = schema.Estimate(1)\n"
		}, 1, "fixed = schema.Estimate(1)"},
		{"two repository packages that both declare `package schema` are not the same package (the last path element names neither)", "package parity\n\nimport \"github.com/ReliablyObserve/victoria-lakehouse/tests/realpkg\"\n\nvar fixed = schema.Estimate(1)\n", func(tr tree) {
			tr["tests/evilpkg/e.go"] = "package schema\n\nfunc Estimate(int) int { return 0 }\n"
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "package parity\n\nimport \"github.com/ReliablyObserve/victoria-lakehouse/tests/evilpkg\"\n\nvar fixed = schema.Estimate(1)\n"
		}, 1, "fixed = schema.Estimate(1)"},
		{"the same import path under another alias is a different spelling and is refused", real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "package parity\n\nimport s \"example.com/real/schema\"\n\nvar fixed = s.Estimate(1)\n"
		}, 1, "fixed = s.Estimate(1)"},
		{"a dot import that names another package", "package parity\n\nimport . \"example.com/real/schema\"\n\nvar fixed = Estimate(1)\n", func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "package parity\n\nimport . \"example.com/evil/schema\"\n\nvar fixed = Estimate(1)\n"
		}, 1, "fixed = Estimate(1)"},
		{"white space INSIDE a string literal is part of the key (strings.Fields used to collapse it)", "package parity\n\nvar s = strings.ToUpper(\"a b\")\n", func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "package parity\n\nvar s = strings.ToUpper(\"a  b\")\n"
		}, 1, "strings.ToUpper"},
		{"a raw string with a tab is not a space", "package parity\n\nvar s = f(`a b`)\n", func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "package parity\n\nvar s = f(`a\tb`)\n"
		}, 1, "f("},
		{"moved into a file with a //go:build line", real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "//go:build linux\n\n" + real
		}, 1, "fixed"},
		{"moved out of a constrained file into an unconstrained one", "//go:build e2e\n\n" + real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = real
		}, 1, "fixed"},
		{"moved between two files with the same constraint is free", "//go:build e2e\n\n" + real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "//go:build e2e\n\n" + real
		}, 0, ""},
		{"an equivalent //go:build spelling is the same constraint", "//go:build a && b\n\n" + real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "//go:build (a && b)\n\n" + real
		}, 0, ""},
		{"moved into a GOOS-suffixed file", real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved_windows.go"] = real
		}, 1, "fixed"},
		{"moved into a GOARCH-suffixed test file", real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved_linux_arm64_test.go"] = real
		}, 1, "fixed"},
		{"an init() whose body calls another package under the same alias", "package parity\n\nimport s \"example.com/real/schema\"\n\nfunc init() { s.Do() }\n", func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "package parity\n\nimport s \"example.com/evil/schema\"\n\nfunc init() { s.Do() }\n"
		}, 1, "1 new init()"},
		{"a moved declaration that was changed on the way (the name-only mutant)", real, func(tr tree) {
			tr["tests/parity/vars.go"] = "package parity\n"
			tr["tests/parity/moved.go"] = "package parity\n\nimport schema \"example.com/real/schema\"\n\nvar fixed = schema.Estimate(2)\n"
		}, 1, "fixed = schema.Estimate(2)"},
	}
	for _, c := range cases {
		base := mk(c.base)
		got := lockInitVar(base, cloneTree(base, c.head))
		if len(got) != c.want {
			t.Errorf("%s: got %d flagged %v, want %d", c.name, len(got), got, c.want)
			continue
		}
		if c.frag != "" && !strings.Contains(strings.Join(got, "\n"), c.frag) {
			t.Errorf("%s: %v lacks %q", c.name, got, c.frag)
		}
	}
}

func TestFileNameConstraint(t *testing.T) {
	for path, want := range map[string]string{
		"a/x.go": "", "a/x_test.go": "", "a/linux.go": "", "a/x_linux.go": "linux", "a/x_linux_test.go": "linux",
		"a/x_linux_amd64.go": "linux_amd64", "a/x_amd64.go": "amd64", "a/x_windows_test.go": "windows",
		"a/x_unknownos.go": "", "a/my_linuxish.go": "", "a/_linux.go": "linux",
	} {
		if got := fileNameConstraint(path); got != want {
			t.Errorf("%s: got %q, want %q", path, got, want)
		}
	}
}

// MAJOR-1: a symlink, or a submodule, where a lock or the go tool would read it.
func TestSymlinkProblems(t *testing.T) {
	raw := func(recs ...string) []byte {
		var b strings.Builder
		for _, r := range recs {
			i := strings.LastIndex(r, " ")
			meta, path := r[:i], r[i+1:]
			b.WriteString(":" + meta + "\x00" + path + "\x00")
		}
		return []byte(b.String())
	}
	const z = "0000000000000000000000000000000000000000"
	rec := func(old, new, status, path string) string {
		return old + " " + new + " " + z + " " + z + " " + status + " " + path
	}
	dirs := append(RuntimeConfigPaths(), "tests/e2e", "tests/parity", "internal/storage/parquets3", "tests/ingestmatrix")
	cases := []struct {
		name string
		recs []string
		want int
		frag string
	}{
		{"a new .go symlink in tests/e2e", []string{rec("000000", "120000", "A", "tests/e2e/zz_link.go")}, 1, "tests/e2e/zz_link.go: a symbolic link added"},
		{"a new .go symlink in tests/parity", []string{rec("000000", "120000", "A", "tests/parity/zz_link.go")}, 1, "tests/parity/zz_link.go"},
		{"a new .go symlink in a product lock package", []string{rec("000000", "120000", "A", "internal/storage/parquets3/zz_link.go")}, 1, "the go tool compiles a linked .go file"},
		{"a new symlink in a lock-adjacent package", []string{rec("000000", "120000", "A", "tests/ingestmatrix/zz.txt")}, 1, "tests/ingestmatrix/zz.txt"},
		{"a regular lock file replaced by a symlink", []string{rec("100644", "120000", "T", "tests/parity/aaa_test.go")}, 1, "a regular file replaced by a symbolic link"},
		{"a symlinked _test.go", []string{rec("000000", "120000", "A", "tests/parity/zz_link_test.go")}, 1, "zz_link_test.go"},
		{"a symlink to a directory under a lock dir", []string{rec("000000", "120000", "A", "tests/e2e/zzdir")}, 1, "tests/e2e/zzdir"},
		{"a symlink in a subdirectory of a lock dir", []string{rec("000000", "120000", "A", "tests/e2e/testdata/link.json")}, 1, "testdata/link.json"},
		{"a retargeted symlink", []string{rec("120000", "120000", "M", "tests/parity/l")}, 1, "retargeted"},
		{"a submodule in a lock dir", []string{rec("000000", "160000", "A", "tests/e2e/sub")}, 1, "a submodule"},
		{"a .go symlink anywhere", []string{rec("000000", "120000", "A", "docs/zz.go")}, 1, "docs/zz.go"},
		{"a native-source symlink anywhere", []string{rec("000000", "120000", "A", "docs/zz.c"), rec("000000", "120000", "A", "docs/zz.F")}, 2, "native source"},
		{"the lock suites' compose file replaced by a symlink", []string{rec("100644", "120000", "T", "deployment/docker/docker-compose-e2e.yml")}, 1, "docker-compose-e2e.yml"},
		{"a docs-only symlink outside every lock dir is free", []string{rec("000000", "120000", "A", "docs/zz_link.md")}, 0, ""},
		{"a symlink that is DELETED is free", []string{rec("120000", "000000", "D", "tests/e2e/old")}, 0, ""},
		{"a symlink turned into a regular file is free here (the file is listed as a new one)", []string{rec("120000", "100644", "T", "tests/e2e/old.txt")}, 0, ""},
		{"a regular file edit is not a link", []string{rec("100644", "100644", "M", "tests/e2e/a_test.go")}, 0, ""},
		{"a path that only shares a prefix with a lock dir is free", []string{rec("000000", "120000", "A", "tests/e2e2/zz.txt")}, 0, ""},
	}
	for _, c := range cases {
		entries, err := ParseDiffRaw(raw(c.recs...))
		if err != nil {
			t.Fatalf("%s: %v", c.name, err)
		}
		got := SymlinkProblems(entries, dirs)
		if len(got) != c.want {
			t.Errorf("%s: got %d %v, want %d", c.name, len(got), got, c.want)
			continue
		}
		if c.frag != "" && !strings.Contains(strings.Join(got, "\n"), c.frag) {
			t.Errorf("%s: %v lacks %q", c.name, got, c.frag)
		}
	}
	if _, err := ParseDiffRaw([]byte("garbage\x00")); err == nil {
		t.Error("an unparseable record is an error")
	}
	if _, err := ParseDiffRaw([]byte(":100644 100644 a b M\x00")); err == nil {
		t.Error("a record without a path is an error")
	}
}

// MAJOR-3: packages a lock package imports.
func TestLockAdjacent(t *testing.T) {
	base := tree{
		"tests/e2e/e2e_test.go":        "package e2e\n\nimport (\n\t\"testing\"\n\n\t\"github.com/ReliablyObserve/victoria-lakehouse/tests/ingestmatrix\"\n\t\"github.com/ReliablyObserve/victoria-lakehouse/internal/pmeta\"\n)\n\nfunc TestLock(t *testing.T) {}\n",
		"tests/ingestmatrix/a.go":      "package ingestmatrix\n\nfunc A() {}\n",
		"tests/ingestmatrix/b_test.go": "package ingestmatrix\n\nimport \"github.com/ReliablyObserve/victoria-lakehouse/internal/neverlinked\"\n",
		"internal/pmeta/p.go":          "package pmeta\n\nimport \"github.com/ReliablyObserve/victoria-lakehouse/internal/deep\"\n\nfunc P() { deep.D() }\n",
		"internal/deep/d.go":           "package deep\n\nimport \"github.com/ReliablyObserve/victoria-lakehouse/internal/deeper\"\n\nfunc D() { deeper.D() }\n",
		"internal/deeper/d.go":         "package deeper\n\nimport \"github.com/ReliablyObserve/victoria-lakehouse/internal/deepest\"\n\nfunc D() { deepest.D() }\n",
		"internal/deepest/d.go":        "package deepest\n\nfunc D() {}\n",
		"internal/neverlinked/n.go":    "package neverlinked\n",
		"internal/x/x_test.go":         "package x\n\nimport \"github.com/ReliablyObserve/victoria-lakehouse/internal/y\"\n\nfunc TestX(t *testing.T) {}\n",
		"internal/x/impl.go":           "package x\n\nimport \"github.com/ReliablyObserve/victoria-lakehouse/internal/z\"\n",
		"internal/y/y.go":              "package y\n",
		"internal/z/z.go":              "package z\n",
	}
	refs := LockTestRefs(lockRows("tests/e2e/e2e_test.go#TestLock", "internal/x/x_test.go#TestX"))
	lockDirs := LockDirs(refs)
	got := LockAdjacentDirs(lockDirs, base.src, base.list)
	want := "internal/deep internal/deeper internal/pmeta internal/y tests/ingestmatrix"
	if strings.Join(got, " ") != want {
		t.Errorf("adjacent dirs: got %v, want %s (depth 3 from the test layer; _test.go of an imported package and a lock package's own product imports are out)", got, want)
	}
	run := func(mod func(tree)) (hard []string, listed []LockChange) {
		head := cloneTree(base, mod)
		adj := LockAdjacentUnion(lockDirs, base.src, head.src, base.list, head.list)
		return LockAdjacentChanges(adj, base.src, head.src, base.list, head.list, base.files, head.files)
	}
	cases := []struct {
		name       string
		mod        func(tree)
		hard       string // fragment of the one hard failure ("" none)
		listedFrag string // fragment of a listed reason ("" none)
	}{
		{"nothing changed", func(tree) {}, "", ""},
		{"x_ingestmatrix_init: an init() added to an existing file", func(tr tree) { tr["tests/ingestmatrix/a.go"] += "\nfunc init() { os.Exit(0) }\n" }, "tests/ingestmatrix/a.go: adds 1 new init() function(s)", ""},
		{"x_ingestmatrix_newfile: a new file there is listed", func(tr tree) { tr["tests/ingestmatrix/new.go"] = "package ingestmatrix\n\nfunc N() {}\n" }, "", "a new Go file in a test-support package"},
		{"a new file with an init() there", func(tr tree) { tr["tests/ingestmatrix/new.go"] = "package ingestmatrix\n\nfunc init() {}\n" }, "tests/ingestmatrix/new.go: adds 1 new init()", "a new Go file"},
		{"an init() in an imported PRODUCT package", func(tr tree) { tr["internal/pmeta/p.go"] += "\nfunc init() {}\n" }, "internal/pmeta/p.go: adds 1 new init()", ""},
		{"an init() three imports deep", func(tr tree) { tr["internal/deeper/d.go"] += "\nfunc init() {}\n" }, "internal/deeper/d.go", ""},
		{"an init() four imports deep is out of reach", func(tr tree) { tr["internal/deepest/d.go"] += "\nfunc init() {}\n" }, "", ""},
		{"an init() in the _test.go of an imported package is never linked", func(tr tree) { tr["tests/ingestmatrix/b_test.go"] += "\nfunc init() {}\n" }, "", ""},
		{"a new func-only file in an imported product package is free", func(tr tree) { tr["internal/pmeta/new.go"] = "package pmeta\n\nfunc Q() {}\n" }, "", ""},
		{"a called var in an imported product package is listed, not failed", func(tr tree) { tr["internal/pmeta/new.go"] = "package pmeta\n\nvar V = f()\n" }, "", "initializer calls a function"},
		{"a blank import in an imported package is listed", func(tr tree) {
			tr["internal/pmeta/p.go"] = "package pmeta\n\nimport _ \"example.com/evil/hook\"\n\nfunc P() {}\n"
		}, "", `blank-imports "example.com/evil/hook"`},
		{"an ordinary import between product packages is free", func(tr tree) {
			tr["internal/pmeta/p.go"] = "package pmeta\n\nimport (\n\t\"github.com/ReliablyObserve/victoria-lakehouse/internal/deep\"\n\t\"github.com/ReliablyObserve/victoria-lakehouse/internal/y\"\n)\n\nfunc P() { deep.D(); y.Y() }\n"
		}, "", ""},
		{"cgo in an imported package", func(tr tree) {
			tr["tests/ingestmatrix/c.go"] = "package ingestmatrix\n\n/*\nvoid f(void) {}\n*/\nimport \"C\"\n"
		}, `tests/ingestmatrix/c.go: imports "C"`, ""},
		{"a new native source file in an imported package", func(tr tree) { tr["tests/ingestmatrix/c.s"] = "TEXT ·f(SB),0,$0\n" }, "c.s: a native source file", ""},
		{"a changed native source file in an imported package", func(tr tree) { tr["tests/ingestmatrix/c.c"] = "int x;\n" }, "c.c: a native source file", ""},
		{"a new package a lock test imports (blank import hook) is adjacent from its first commit", func(tr tree) {
			tr["tests/hook/hook.go"] = "package hook\n\nimport \"os\"\n\nfunc init() { os.Exit(0) }\n"
			tr["tests/e2e/zz_hook_test.go"] = "package e2e\n\nimport _ \"github.com/ReliablyObserve/victoria-lakehouse/tests/hook\"\n"
		}, "tests/hook/hook.go: adds 1 new init()", "a new Go file in a test-support package"},
		{"a file of an imported package that stops parsing is listed", func(tr tree) { tr["tests/ingestmatrix/a.go"] = "package ingestmatrix\n\nfunc (\n" }, "", "no longer parses"},
		{"a package no lock package imports is free", func(tr tree) { tr["internal/neverlinked/n.go"] += "\nfunc init() {}\n" }, "", ""},
		{"a lock package's own product import is out of reach (Rule 1 covers it)", func(tr tree) { tr["internal/z/z.go"] += "\nfunc init() {}\n" }, "", ""},
	}
	// native sources need a tree listing that includes non-Go files
	for _, c := range cases {
		hard, listed := run(c.mod)
		if c.name == "a changed native source file in an imported package" {
			// the base holds the file, the head changes it
			b2 := cloneTree(base, func(tr tree) { tr["tests/ingestmatrix/c.c"] = "int y;\n" })
			head := cloneTree(base, c.mod)
			adj := LockAdjacentUnion(lockDirs, b2.src, head.src, b2.list, head.list)
			hard, listed = LockAdjacentChanges(adj, b2.src, head.src, b2.list, head.list, b2.files, head.files)
		}
		if c.hard == "" && len(hard) != 0 {
			t.Errorf("%s: unexpected hard failures %v", c.name, hard)
		}
		if c.hard != "" && !strings.Contains(strings.Join(hard, "\n"), c.hard) {
			t.Errorf("%s: hard %v lacks %q", c.name, hard, c.hard)
		}
		var all []string
		for _, l := range listed {
			all = append(all, l.String())
		}
		if c.listedFrag == "" && c.hard == "" && len(listed) != 0 {
			t.Errorf("%s: unexpected listing %v", c.name, all)
		}
		if c.listedFrag != "" && !strings.Contains(strings.Join(all, "\n"), c.listedFrag) {
			t.Errorf("%s: listing %v lacks %q", c.name, all, c.listedFrag)
		}
	}
}

func TestLockCodeChangeList_KeepsPathAndReasonApart(t *testing.T) {
	base := tree{"tests/parity/lock_test.go": parityTest, "tests/parity/a: b.go": "package parity\n"}
	head := cloneTree(base, func(tr tree) { tr["tests/parity/a: b.go"] += "// x\n" })
	refs := LockTestRefs(lockRows("tests/parity/lock_test.go#TestLock"))
	got, err := LockCodeChangeList(refs, refs, base.src, head.src, base.list, head.list, base.files, head.files)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 1 || got[0].Path != "tests/parity/a: b.go" || strings.Contains(got[0].Reason, "b.go") {
		t.Errorf("a path with \": \" must stay whole: %+v", got)
	}
}

func TestMinPassProblems(t *testing.T) {
	cases := []struct {
		name       string
		base, head string
		want       string // fragment of the one problem, "" none
	}{
		{"unchanged", "# min-pass: 341\nTestA  # B1: r\n", "# min-pass: 341\nTestA  # B1: r\n", ""},
		{"raised", "# min-pass: 341\n", "# min-pass: 342\n", ""},
		{"lowered", "# min-pass: 341\n", "# min-pass: 340\n", "lowered from 341 to 340"},
		{"lowered to zero", "# min-pass: 341\n", "# min-pass: 0\n", "lowered from 341 to 0"},
		{"removed", "# min-pass: 341\nTestA  # B1: r\n", "TestA  # B1: r\n", "was removed"},
		{"hidden behind more #", "# min-pass: 341\n", "### min-pass: 3\n", "lowered"},
		{"written without a space", "# min-pass: 341\n", "#min-pass:3\n", "lowered"},
		{"an Arabic-Indic digit value is unreadable", "# min-pass: 341\n", "# min-pass: ٣\n", "unreadable"},
		{"a signed value is unreadable", "# min-pass: 341\n", "# min-pass: +400\n", "unreadable"},
		{"an underscore value is unreadable", "# min-pass: 341\n", "# min-pass: 3_41\n", "unreadable"},
		{"a duplicate directive", "# min-pass: 341\n", "# min-pass: 341\n# min-pass: 1\n", "duplicate"},
		{"a mention in prose is not the directive", "# min-pass: 341\n", "# min-pass: 341\n# fewer than min-pass: 3 tests\n", ""},
		{"an entry line is not a directive", "# min-pass: 341\n", "# min-pass: 341\nTestA  # min-pass: 3\n", ""},
		{"no directive at the base: nothing to compare", "TestA  # B1: r\n", "# min-pass: 1\nTestA  # B1: r\n", ""},
		{"an unreadable base: nothing to compare", "# min-pass: many\n", "# min-pass: 1\n", ""},
		{"Unicode white space around the value is white space (as in Python)", "# min-pass: 341\n", "# min-pass: 340　\n", "lowered"},
	}
	for _, c := range cases {
		got := MinPassProblems("tests/parity/known_failures.txt", []byte(c.base), []byte(c.head))
		if c.want == "" && len(got) != 0 {
			t.Errorf("%s: unexpected %v", c.name, got)
		}
		if c.want != "" && !strings.Contains(strings.Join(got, "\n"), c.want) {
			t.Errorf("%s: %v lacks %q", c.name, got, c.want)
		}
	}
	if got := MinPassProblems("f", []byte("# min-pass: 3\n"), nil); got != nil {
		t.Errorf("a missing file is reported elsewhere: %v", got)
	}
}

func TestVersionBefore_Overflow(t *testing.T) {
	for _, c := range []struct {
		a, b           string
		before, wantOK bool
	}{
		{"1.27.2", "1.27.3", true, true}, {"1.27.3", "1.27.2", false, true}, {"1.27", "1.27.1", true, true}, {"", "1", false, true},
		{"1.27.99999999999999999999", "1.27.2", false, false}, {"1.27.2", "1.27.99999999999999999999", false, false},
		{"1.27.2147483648", "1.27.2", false, false}, {"1.27.2147483647", "1.27.2", false, true},
	} {
		before, ok := versionBefore(c.a, c.b)
		if before != c.before || ok != c.wantOK {
			t.Errorf("versionBefore(%q, %q) = %v, %v; want %v, %v", c.a, c.b, before, ok, c.before, c.wantOK)
		}
	}
}

// The Go column of scripts/ci/tests/testdata/parity_data_parse.json: what the gate reads from the parity
// allowlist and the lock floors. The Python ratchet asserts its own column of the same file.
func TestParityParse_SharedFixture(t *testing.T) {
	raw, err := os.ReadFile("../../../scripts/ci/tests/testdata/parity_data_parse.json")
	if err != nil {
		t.Fatal(err)
	}
	type parseCase struct {
		Name, Text string
		Python, Go any
	}
	var fx struct {
		Allowlist []parseCase `json:"allowlist"`
		LockCells []parseCase `json:"lock_cells"`
	}
	if err := json.Unmarshal(raw, &fx); err != nil {
		t.Fatal(err)
	}
	fx2 := fx
	if len(fx.Allowlist) < 20 || len(fx2.LockCells) < 15 {
		t.Fatalf("the parse fixture has only %d allowlist and %d lock-cell cases", len(fx.Allowlist), len(fx2.LockCells))
	}
	asNames := func(v any) []string {
		var out []string
		for _, x := range v.([]any) {
			out = append(out, x.(string))
		}
		sort.Strings(out)
		return out
	}
	asCells := func(v any) map[string]int {
		out := map[string]int{}
		for k, x := range v.(map[string]any) {
			out[k] = int(x.(float64))
		}
		return out
	}
	for _, c := range fx.Allowlist {
		// the gate's reading of an allowlist: refuse unreadable bytes, then the first word of every entry line
		var got []string
		reject := len(ParityDataProblems([]string{"f"}, func(string) []byte { return nil }, func(string) []byte { return []byte(c.Text) })) > 0
		if !reject {
			for k := range ParseAllowlist([]byte(c.Text)) {
				got = append(got, k)
			}
			sort.Strings(got)
		}
		if c.Go == "reject" {
			if !reject {
				t.Errorf("allowlist %q: the gate must refuse this input, it read %v", c.Name, got)
			}
		} else if reject || strings.Join(got, ",") != strings.Join(asNames(c.Go), ",") {
			t.Errorf("allowlist %q: gate read %v (rejected: %v), fixture says %v", c.Name, got, reject, c.Go)
		}
		if c.Python != "reject" && c.Go != "reject" && strings.Join(asNames(c.Python), ",") != strings.Join(asNames(c.Go), ",") {
			t.Errorf("allowlist %q: both readers accept this input and disagree", c.Name)
		}
	}
	for _, c := range fx2.LockCells {
		cells, perr := ParseLockCells([]byte(c.Text))
		reject := perr != nil || len(ParityDataProblems([]string{"f"}, func(string) []byte { return nil }, func(string) []byte { return []byte(c.Text) })) > 0
		if c.Go == "reject" {
			if !reject {
				t.Errorf("lock cells %q: the gate must refuse this input, it read %v", c.Name, cells)
			}
		} else if reject || !reflect.DeepEqual(cells, asCells(c.Go)) {
			t.Errorf("lock cells %q: gate read %v (rejected: %v), fixture says %v", c.Name, cells, reject, c.Go)
		}
		if c.Python != "reject" && c.Go != "reject" && !reflect.DeepEqual(asCells(c.Python), asCells(c.Go)) {
			t.Errorf("lock cells %q: both readers accept this input and disagree", c.Name)
		}
	}
}
