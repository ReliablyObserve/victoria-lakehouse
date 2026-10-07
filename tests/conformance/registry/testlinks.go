package registry

import (
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"

	"gopkg.in/yaml.v3"
)

// This file is the engine of the "tests must be linked from the registry" PR
// gate (cmd/testlinks, run by scripts/ci/check_registry_touch.sh): every
// top-level Go Test or Fuzz function a PR adds, in the packages that carry
// behaviour, must be named by a row's `refs.tests` or a feature's `tests:`, and
// a reference to a test the PR removed or renamed must not be left behind.

// testFuncRe matches a top-level Go test or fuzz function. Methods, helpers
// and TestMain are not tests: `Test` must be followed by a non-lowercase rune
// (the go tool's own rule), so `Testify` is a helper, not a test.
var testFuncRe = regexp.MustCompile(`(?m)^func +((?:Test|Fuzz)(?:[^a-z\s(][A-Za-z0-9_]*)?)\(`)

// linkedTestPrefixes are the trees whose tests must be linked. The product
// packages carry behaviour; the tests/ suites are the cross-cutting proofs.
var linkedTestPrefixes = []string{
	"internal/", "cmd/", "lakehouse-traces/",
	"tests/parity/", "tests/e2e/", "tests/conformance/", "tests/ingestmatrix/",
}

// IsLinkedTestFile reports whether path is a Go test file whose Test and Fuzz
// functions must be linked from the registry.
func IsLinkedTestFile(path string) bool {
	if !strings.HasSuffix(path, "_test.go") {
		return false
	}
	for _, seg := range []string{"/deps/", "/testdata/", "/vendor/"} {
		if strings.Contains("/"+path, seg) {
			return false
		}
	}
	for _, p := range linkedTestPrefixes {
		if strings.HasPrefix(path, p) {
			return true
		}
	}
	return false
}

// TestFuncs returns the names of the top-level Test and Fuzz functions in a Go
// test file's source, excluding TestMain.
func TestFuncs(src []byte) []string {
	var out []string
	for _, m := range testFuncRe.FindAllSubmatch(src, -1) {
		if name := string(m[1]); name != "TestMain" {
			out = append(out, name)
		}
	}
	return out
}

// TestRef is one `path#Name` (or bare `path`) test reference of a row or feature.
type TestRef struct {
	Owner string // row or feature id
	Ref   string
	Path  string
	Name  string // "" for a file-level reference
}

// CollectTestRefs reads every row's `refs.tests` and every feature's `tests`
// under the registry directories. Decoding is deliberately lenient (unknown
// keys ignored): the strict schema checks belong to LoadDir and LoadFeatures,
// and this gate must keep working while a PR is changing the schema.
func CollectTestRefs(dirs ...string) ([]TestRef, error) {
	type doc struct {
		ID    string   `yaml:"id"`
		Tests []string `yaml:"tests"`
		Refs  *Refs    `yaml:"refs"`
	}
	var out []TestRef
	for _, dir := range dirs {
		err := filepath.WalkDir(dir, func(p string, d fs.DirEntry, werr error) error {
			if werr != nil {
				return werr
			}
			if d.IsDir() || !strings.HasSuffix(p, ".yaml") {
				return nil
			}
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			var docs []doc
			if err := yaml.Unmarshal(data, &docs); err != nil {
				return fmt.Errorf("%s: %w", p, err)
			}
			for _, e := range docs {
				refs := append([]string(nil), e.Tests...)
				if e.Refs != nil {
					refs = append(refs, e.Refs.Tests...)
				}
				for _, r := range refs {
					path, name := splitRef(r)
					out = append(out, TestRef{Owner: e.ID, Ref: r, Path: path, Name: name})
				}
			}
			return nil
		})
		if err != nil {
			return nil, err
		}
	}
	return out, nil
}

// AddedTest is a Test or Fuzz function a PR introduced to a package.
type AddedTest struct {
	Pkg  string // directory
	Name string
	File string // file at HEAD that defines it
}

// DiffAddedTests compares the test functions of the changed test files at the
// merge base and at HEAD, per package, and returns those only HEAD has. A
// function that moved between files of one package, or whose file was renamed,
// is in both sets and so is not added; a renamed function is removed plus
// added. baseSrc and headSrc return a file's source at that revision (nil when
// the file does not exist there).
func DiffAddedTests(changed []string, baseSrc, headSrc func(path string) []byte) []AddedTest {
	type key struct{ pkg, name string }
	base := map[key]bool{}
	head := map[key]string{}
	for _, f := range changed {
		if !IsLinkedTestFile(f) {
			continue
		}
		pkg := filepath.ToSlash(filepath.Dir(f))
		for _, n := range TestFuncs(baseSrc(f)) {
			base[key{pkg, n}] = true
		}
		for _, n := range TestFuncs(headSrc(f)) {
			head[key{pkg, n}] = f
		}
	}
	var out []AddedTest
	for k, f := range head {
		if !base[k] {
			out = append(out, AddedTest{Pkg: k.pkg, Name: k.name, File: f})
		}
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].File != out[j].File {
			return out[i].File < out[j].File
		}
		return out[i].Name < out[j].Name
	})
	return out
}

// Linked reports whether some reference names the test: `file#Name`, or the
// bare `file` (a file-level link covers every test in that file).
func Linked(t AddedTest, refs []TestRef) bool {
	for _, r := range refs {
		if r.Path == t.File && (r.Name == "" || r.Name == t.Name) {
			return true
		}
	}
	return false
}

// StaleRefs returns the Go-test references that touch a file the PR changed
// and no longer resolve: the file was deleted, or the named function is gone
// (removed or renamed). References into files the PR did not touch are not
// examined, so the gate asks for no backfill of older drift.
func StaleRefs(repoRoot string, refs []TestRef, changed []string) []string {
	touched := map[string]bool{}
	for _, c := range changed {
		touched[c] = true
	}
	var out []string
	for _, r := range refs {
		if !strings.HasSuffix(r.Path, "_test.go") || !touched[r.Path] {
			continue
		}
		if err := CheckTestRef(repoRoot, r.Ref); err != nil {
			out = append(out, fmt.Sprintf("%s: %v", r.Owner, err))
		}
	}
	sort.Strings(out)
	return out
}

// UnlinkedTests walks repoRoot and returns every in-scope Test or Fuzz
// function no reference names. It is the size of the backfill the gate does
// not demand, reported for calibration.
func UnlinkedTests(repoRoot string, refs []TestRef) (total int, unlinked []AddedTest, err error) {
	err = filepath.WalkDir(repoRoot, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		rel, rerr := filepath.Rel(repoRoot, p)
		if rerr != nil {
			return rerr
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if d.Name() == ".git" || d.Name() == "deps" || d.Name() == ".claude" {
				return fs.SkipDir
			}
			return nil
		}
		if !IsLinkedTestFile(rel) {
			return nil
		}
		data, rerr := os.ReadFile(p)
		if rerr != nil {
			return rerr
		}
		for _, n := range TestFuncs(data) {
			total++
			t := AddedTest{Pkg: filepath.ToSlash(filepath.Dir(rel)), Name: n, File: rel}
			if !Linked(t, refs) {
				unlinked = append(unlinked, t)
			}
		}
		return nil
	})
	return total, unlinked, err
}
