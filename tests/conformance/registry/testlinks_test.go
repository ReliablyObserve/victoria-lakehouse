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
	got := TestFuncs(src)
	want := []string{"TestA", "Test_b", "FuzzC", "Test"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("TestFuncs = %v, want %v", got, want)
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
		"tests/s3compat/a_test.go":              false,
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
				return []byte(s)
			}
			return nil
		}
	}
	changed := []string{"internal/x/a_test.go", "internal/x/b_test.go", "internal/y/a_test.go", "internal/z/a_test.go", "internal/x/c.go"}
	var got []string
	for _, a := range DiffAddedTests(changed, src(base), src(head)) {
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
	if !Linked(AddedTest{File: "internal/x/b_test.go", Name: "TestAnything"}, refs) {
		t.Error("a file-level reference links every test in the file")
	}
	stale := StaleRefs(root, refs, []string{"internal/x/a_test.go"})
	if len(stale) != 1 || !strings.Contains(stale[0], "TestGone") {
		t.Errorf("stale = %v, want exactly the TestGone reference", stale)
	}
	if got := StaleRefs(root, refs, []string{"internal/other/x_test.go"}); len(got) != 0 {
		t.Errorf("references into untouched files must not be examined, got %v", got)
	}
	total, un, err := UnlinkedTests(root, refs)
	if err != nil || total != 3 || len(un) != 1 || un[0].Name != "TestNew" {
		t.Errorf("UnlinkedTests = %d %v %v", total, un, err)
	}
}
