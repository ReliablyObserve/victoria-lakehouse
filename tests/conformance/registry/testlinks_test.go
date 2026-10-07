package registry

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestTestFuncs_OnlyRealTests(t *testing.T) {
	src := []byte("package p\n\nfunc TestA(t *testing.T) {}\nfunc Test_b(t *testing.T) {}\nfunc FuzzC(f *testing.F) {}\n" +
		"func TestMain(m *testing.M) {}\nfunc Testify(t *testing.T) {}\nfunc (s *S) TestMethod() {}\nfunc helper() {}\n  func TestIndented() {}\nfunc Test(t *testing.T) {}\n")
	got, err := TestFuncs(src)
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"FuzzC", "Test", "TestA", "TestIndented", "Test_b"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TestFuncs = %v, want %v", got, want)
	}
}

func TestTestFuncs_NonASCIIAndSyntax(t *testing.T) {
	got, err := TestFuncs([]byte("package p\n\nfunc TestÄé(t *testing.T) {}\nfunc Test日本(t *testing.T) {}\nfunc TestÀ_ok(t *testing.T) {}\nfunc Testà(t *testing.T) {}\n"))
	if err != nil {
		t.Fatal(err)
	}
	want := []string{"TestÀ_ok", "TestÄé", "Test日本"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TestFuncs = %v, want %v (Testà has a lowercase letter after Test: a helper)", got, want)
	}
	if _, err := TestFuncs([]byte("package p\nfunc (")); err == nil {
		t.Error("a file that does not parse must be an error, not zero tests")
	}
	if got, err := TestFuncs(nil); err != nil || len(got) != 0 {
		t.Errorf("a missing file has no tests: %v %v", got, err)
	}
}

func TestModifiedTests_IgnoresComments(t *testing.T) {
	base := []byte("package p\n\nfunc TestA(t *testing.T) { x := 1; _ = x }\nfunc TestB(t *testing.T) {}\n")
	head := []byte("package p\n\n// a new comment\nfunc TestA(t *testing.T) {\n\t// inside\n\tx := 1; _ = x }\nfunc TestB(t *testing.T) { _ = 2 }\nfunc TestC(t *testing.T) {}\n")
	got, err := ModifiedTests(base, head)
	if err != nil {
		t.Fatal(err)
	}
	if want := []string{"TestB", "TestC"}; !reflect.DeepEqual(got, want) {
		t.Fatalf("ModifiedTests = %v, want %v (a comment-only edit is not a modification)", got, want)
	}
}

func TestIsLinkedTestFile(t *testing.T) {
	cases := map[string]bool{
		"internal/x/a_test.go":                  true,
		"cmd/lakehouse-logs/a_test.go":          true,
		"lakehouse-traces/internal/y/a_test.go": true,
		"tests/parity/a_test.go":                true,
		"tests/ingestmatrix/a_test.go":          true,
		"tests/conformance/registry/a_test.go":  true,
		"internal/x/a.go":                       false,
		"internal/x/testdata/a_test.go":         false,
		"lakehouse-traces/deps/V/a_test.go":     false,
		"tests/s3compat/a_test.go":              true,
		"tests/playwright/a_test.go":            false,
		"scripts/x/a_test.go":                   false,
	}
	for p, want := range cases {
		if got := IsLinkedTestFile(p); got != want {
			t.Errorf("IsLinkedTestFile(%q) = %v, want %v", p, got, want)
		}
	}
}

func TestDiffAddedTests_MovesAreNotAdds(t *testing.T) {
	base := map[string]string{
		"internal/x/a_test.go": "func TestMoved() {}\nfunc TestKept() {}\nfunc TestRenamedOld() {}\n",
		"internal/y/a_test.go": "func TestSameNameOtherPkg() {}\n",
	}
	head := map[string]string{
		"internal/x/a_test.go": "func TestKept() {}\nfunc TestRenamedNew() {}\n",
		"internal/x/b_test.go": "func TestMoved() {}\n",
		"internal/z/a_test.go": "func TestSameNameOtherPkg() {}\n", // moved across packages: an add
		"internal/x/c.go":      "func TestNotATestFile() {}\n",
	}
	src := func(m map[string]string) func(string) []byte {
		return func(p string) []byte {
			if s, ok := m[p]; ok {
				return []byte("package x\n\n" + s)
			}
			return nil
		}
	}
	changed := []string{"internal/x/a_test.go", "internal/x/b_test.go", "internal/y/a_test.go", "internal/z/a_test.go", "internal/x/c.go"}
	var got []string
	diff, err := DiffAddedTests(changed, src(base), src(head))
	if err != nil {
		t.Fatal(err)
	}
	for _, a := range diff {
		got = append(got, a.Pkg+"#"+a.Name)
	}
	want := []string{"internal/x#TestRenamedNew", "internal/z#TestSameNameOtherPkg"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("added = %v, want %v", got, want)
	}
}

func TestCollectTestRefs_LinkedAndStale(t *testing.T) {
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, rel)
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("registry/rows/r.yaml", "- id: lh.r\n  unknown_future_key: 1\n  refs:\n    tests:\n      - internal/x/a_test.go#TestA\n")
	write("registry/features/f.yaml", "- id: lh.feature.f\n  tests:\n    - internal/x/b_test.go\n    - internal/x/a_test.go#TestGone\n")
	write("internal/x/a_test.go", "package x\nfunc TestA() {}\nfunc TestNew() {}\n")
	write("internal/x/b_test.go", "package x\nfunc TestB() {}\n")
	write("internal/.scratch/c_test.go", "package x\nfunc TestScratch() {}\n")
	write("deps/V/internal/x/d_test.go", "package x\nfunc TestVendored() {}\n")
	refs, err := CollectTestRefs(filepath.Join(root, "registry/rows"), filepath.Join(root, "registry/features"))
	if err != nil || len(refs) != 3 {
		t.Fatalf("refs = %v, err = %v", refs, err)
	}
	a := AddedTest{File: "internal/x/a_test.go", Name: "TestNew"}
	if Linked(a, refs) {
		t.Error("TestNew must not be linked")
	}
	if !Linked(AddedTest{File: "internal/x/a_test.go", Name: "TestA"}, refs) {
		t.Error("TestA is linked by name")
	}
	if Linked(AddedTest{File: "internal/x/b_test.go", Name: "TestAnything"}, refs) {
		t.Error("a file-level reference must not link a NEW test: it must be named file#Test")
	}
	if !Linked(AddedTest{File: "internal/x/a_test.go", Name: "TestA"}, refs) {
		t.Error("a named reference links its test")
	}
	stale := StaleRefs(root, refs, []string{"internal/x/a_test.go"})
	if len(stale) != 1 || !strings.Contains(stale[0], "TestGone") {
		t.Errorf("stale = %v, want exactly the TestGone reference", stale)
	}
	if got := StaleRefs(root, refs, []string{"internal/other/x_test.go"}); len(got) != 0 {
		t.Errorf("references into untouched files must not be examined, got %v", got)
	}
	total, un, err := UnlinkedTests(root, refs)
	if err != nil || total != 3 || len(un) != 2 {
		t.Errorf("UnlinkedTests = %d %v %v", total, un, err)
	}
}

func TestLockFileWeakenings(t *testing.T) {
	base := map[string]string{
		"tests/parity/a_test.go": "//go:build parity\n\npackage parity\n\nfunc TestA(t *testing.T) { t.Skip(\"known\") }\n",
		"tests/parity/b_test.go": "// +build parity\n\npackage parity\n\nfunc TestB(t *testing.T) {}\n",
		"tests/parity/c_test.go": "package parity\n\nfunc TestC(t *testing.T) {}\n",
	}
	src := func(m map[string]string) func(string) []byte {
		return func(p string) []byte {
			if s, ok := m[p]; ok {
				return []byte(s)
			}
			return nil
		}
	}
	files := []string{"tests/parity/a_test.go", "tests/parity/b_test.go", "tests/parity/c_test.go", "tests/parity/new_test.go", "tests/parity/gone_test.go", "tests/parity/x.go", "tests/parity/c_test.go"}
	cases := map[string]struct {
		head map[string]string
		want []string
	}{
		"nothing changed": {base, nil},
		"build tag replaced (N4)": {map[string]string{
			"tests/parity/a_test.go": "//go:build parity && ignore\n\npackage parity\n\nfunc TestA(t *testing.T) { t.Skip(\"known\") }\n",
			"tests/parity/b_test.go": base["tests/parity/b_test.go"], "tests/parity/c_test.go": base["tests/parity/c_test.go"]},
			[]string{"tests/parity/a_test.go: the build constraint header changed in a file a lock references"}},
		"comment-only header edit counts": {map[string]string{
			"tests/parity/a_test.go": base["tests/parity/a_test.go"], "tests/parity/c_test.go": base["tests/parity/c_test.go"],
			"tests/parity/b_test.go": "// +build parity\n// harmless comment\n\npackage parity\n\nfunc TestB(t *testing.T) {}\n"},
			[]string{"tests/parity/b_test.go: the build constraint header changed in a file a lock references"}},
		"a build tag added to a file with none": {map[string]string{
			"tests/parity/a_test.go": base["tests/parity/a_test.go"], "tests/parity/b_test.go": base["tests/parity/b_test.go"],
			"tests/parity/c_test.go": "//go:build never\n\npackage parity\n\nfunc TestC(t *testing.T) {}\n"},
			[]string{"tests/parity/c_test.go: the build constraint header changed in a file a lock references"}},
		"t.Skip added (N5)": {map[string]string{
			"tests/parity/a_test.go": base["tests/parity/a_test.go"], "tests/parity/b_test.go": base["tests/parity/b_test.go"],
			"tests/parity/c_test.go": "package parity\n\nfunc TestC(t *testing.T) { t.Skip(\"flaky\") }\n"},
			[]string{"tests/parity/c_test.go: a Skip call was added to a file a lock references"}},
		"SkipNow and Skipf count": {map[string]string{
			"tests/parity/a_test.go": base["tests/parity/a_test.go"], "tests/parity/b_test.go": "// +build parity\n\npackage parity\n\nfunc TestB(t *testing.T) { t.Skipf(\"x\") }\n",
			"tests/parity/c_test.go": "package parity\n\nfunc TestC(t *testing.T) { t.SkipNow() }\n"},
			[]string{"tests/parity/b_test.go: a Skip call was added to a file a lock references", "tests/parity/c_test.go: a Skip call was added to a file a lock references"}},
		"an existing Skip kept or removed is fine": {map[string]string{
			"tests/parity/a_test.go": "//go:build parity\n\npackage parity\n\nfunc TestA(t *testing.T) {}\n",
			"tests/parity/b_test.go": base["tests/parity/b_test.go"], "tests/parity/c_test.go": base["tests/parity/c_test.go"]}, nil},
		"a new file and a deleted file are not weakenings": {map[string]string{
			"tests/parity/a_test.go": base["tests/parity/a_test.go"], "tests/parity/b_test.go": base["tests/parity/b_test.go"],
			"tests/parity/c_test.go": base["tests/parity/c_test.go"], "tests/parity/new_test.go": "package parity\nfunc TestN(t *testing.T) { t.Skip() }\n"}, nil},
	}
	for name, c := range cases {
		got := LockFileWeakenings(files, src(base), src(c.head))
		if !reflect.DeepEqual(got, c.want) {
			t.Errorf("%s: got %v, want %v", name, got, c.want)
		}
	}
}

func TestStaleRefsBlobAndRefResolves(t *testing.T) {
	src := func(path string) []byte {
		switch path {
		case "internal/x/a_test.go":
			return []byte("package x\n\nfunc TestA(t *testing.T) {}\n")
		case "internal/x/broken_test.go":
			return []byte("package x\nfunc (")
		}
		return nil
	}
	for ref, want := range map[string]bool{
		"internal/x/a_test.go#TestA": true, "internal/x/a_test.go": true, "internal/x/a_test.go#TestGone": false,
		"internal/x/missing_test.go#TestA": false, "internal/x/broken_test.go#TestA": false,
	} {
		if got := RefResolves(src, ref); got != want {
			t.Errorf("RefResolves(%q) = %v, want %v", ref, got, want)
		}
	}
	refs := []TestRef{
		{Owner: "r1", Ref: "internal/x/a_test.go#TestGone", Path: "internal/x/a_test.go", Name: "TestGone"},
		{Owner: "r2", Ref: "internal/x/a_test.go#TestA", Path: "internal/x/a_test.go", Name: "TestA"},
		{Owner: "r3", Ref: "internal/y/untouched_test.go#TestZ", Path: "internal/y/untouched_test.go", Name: "TestZ"},
		{Owner: "r4", Ref: "docs/x.md", Path: "docs/x.md"},
	}
	got := StaleRefsBlob(src, refs, []string{"internal/x/a_test.go", "docs/x.md"})
	if len(got) != 1 || !strings.Contains(got[0], "r1") {
		t.Errorf("only the touched, unresolved Go test reference is stale: %v", got)
	}
}

func TestTestMainChanges(t *testing.T) {
	mk := func(body string) []byte { return []byte("package p\n\nimport (\"os\"; \"testing\")\n\n" + body) }
	base := map[string][]byte{
		"d/a_test.go": mk("func TestMain(m *testing.M) { os.Exit(m.Run()) }\n"),
		"d/b_test.go": mk("func TestB(t *testing.T) {}\n"),
		"d/c_test.go": mk("func TestC(t *testing.T) {}\n"),
	}
	src := func(m map[string][]byte) func(string) []byte {
		return func(p string) []byte { return m[p] }
	}
	files := []string{"d/a_test.go", "d/b_test.go", "d/c_test.go", "d/new_test.go", "d/a_test.go"}
	cases := map[string]struct {
		head map[string][]byte
		want int
	}{
		"nothing":                               {base, 0},
		"os.Exit(0) (X6)":                       {map[string][]byte{"d/a_test.go": mk("func TestMain(m *testing.M) { os.Exit(0) }\n"), "d/b_test.go": base["d/b_test.go"], "d/c_test.go": base["d/c_test.go"]}, 1},
		"early return in the existing TestMain": {map[string][]byte{"d/a_test.go": mk("func TestMain(m *testing.M) { if os.Getenv(\"CI\") != \"\" { os.Exit(0) }; os.Exit(m.Run()) }\n"), "d/b_test.go": base["d/b_test.go"], "d/c_test.go": base["d/c_test.go"]}, 1},
		"a TestMain added in a new file":        {map[string][]byte{"d/a_test.go": base["d/a_test.go"], "d/b_test.go": base["d/b_test.go"], "d/c_test.go": base["d/c_test.go"], "d/new_test.go": mk("func TestMain(m *testing.M) { os.Exit(0) }\n")}, 1},
		"TestMain removed":                      {map[string][]byte{"d/a_test.go": mk("func TestX(t *testing.T) {}\n"), "d/b_test.go": base["d/b_test.go"], "d/c_test.go": base["d/c_test.go"]}, 1},
		"a comment or whitespace only":          {map[string][]byte{"d/a_test.go": mk("// comment\nfunc TestMain(m *testing.M) {\n\tos.Exit(m.Run())\n}\n"), "d/b_test.go": base["d/b_test.go"], "d/c_test.go": base["d/c_test.go"]}, 0},
		"a test elsewhere in the package":       {map[string][]byte{"d/a_test.go": base["d/a_test.go"], "d/b_test.go": mk("func TestB(t *testing.T) { _ = 1 }\n"), "d/c_test.go": base["d/c_test.go"]}, 0},
		"a file that stops parsing":             {map[string][]byte{"d/a_test.go": []byte("package p\nfunc ("), "d/b_test.go": base["d/b_test.go"], "d/c_test.go": base["d/c_test.go"]}, 1},
	}
	for name, c := range cases {
		if got := TestMainChanges(files, src(base), src(c.head)); len(got) != c.want {
			t.Errorf("%s: %v", name, got)
		}
	}
}

func TestFilenameConstrained(t *testing.T) {
	for path, want := range map[string]bool{
		"tests/parity/a_test.go":                 false,
		"tests/parity/ingest_matrix_test.go":     false,
		"tests/parity/a_windows_test.go":         true,
		"tests/parity/a_linux_test.go":           true,
		"tests/parity/a_amd64_test.go":           true,
		"tests/parity/a_linux_arm64_test.go":     true,
		"tests/parity/_a_test.go":                true,
		"tests/parity/.a_test.go":                true,
		"tests/parity/windows_test.go":           false, // a single word is not a suffix
		"tests/parity/matrix_js_helpers_test.go": false,
	} {
		if got := filenameConstrained(path); got != want {
			t.Errorf("filenameConstrained(%q) = %v, want %v", path, got, want)
		}
	}
	src := func(m map[string]string) func(string) []byte {
		return func(p string) []byte {
			if s, ok := m[p]; ok {
				return []byte(s)
			}
			return nil
		}
	}
	got := LockFileWeakenings([]string{"tests/parity/a_windows_test.go", "tests/parity/ok_test.go"}, src(nil),
		src(map[string]string{"tests/parity/a_windows_test.go": "package p\n", "tests/parity/ok_test.go": "package p\n"}))
	if len(got) != 1 || !strings.Contains(got[0], "a_windows_test.go") {
		t.Errorf("a new lock file with a constraining name: %v", got)
	}
	// a file that already existed under that name is judged by its content only
	same := map[string]string{"tests/parity/a_windows_test.go": "package p\n"}
	if got := LockFileWeakenings([]string{"tests/parity/a_windows_test.go"}, src(same), src(same)); len(got) != 0 {
		t.Errorf("%v", got)
	}
}
