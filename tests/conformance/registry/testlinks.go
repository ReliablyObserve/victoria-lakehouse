package registry

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/scanner"
	"go/token"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode"
	"unicode/utf8"

	"gopkg.in/yaml.v3"
)

// This file is the engine of the "tests must be linked from the registry" PR
// gate (cmd/testlinks, run by scripts/ci/check_registry_touch.sh): every
// top-level Go Test or Fuzz function a PR adds, in the packages that carry
// behaviour, must be named by a row's `refs.tests` or a feature's `tests:`, and
// a reference to a test the PR removed or renamed must not be left behind.

// linkedTestPrefixes are the trees whose tests must be linked. The product
// packages carry behaviour; the tests/ suites are the cross-cutting proofs.
var linkedTestPrefixes = []string{
	"internal/", "cmd/", "lakehouse-traces/",
	"tests/parity/", "tests/e2e/", "tests/conformance/", "tests/ingestmatrix/", "tests/s3compat/",
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

// isTestName reports whether a function name is a Go test or fuzz name by the
// go tool's rule: the prefix must not be followed by a lowercase letter. The
// name may hold any Unicode letter, which a byte-oriented regexp would miss.
func isTestName(name string) bool {
	for _, prefix := range []string{"Test", "Fuzz"} {
		if rest, ok := strings.CutPrefix(name, prefix); ok {
			r, _ := utf8.DecodeRuneInString(rest)
			return rest == "" || !unicode.IsLower(r)
		}
	}
	return false
}

// testDecls parses a Go test file and returns its top-level Test and Fuzz
// functions (no receiver, TestMain excluded) with their comment-free printed
// source, so a function counts as modified only when its code changed.
func testDecls(src []byte) (map[string]string, error) {
	out := map[string]string{}
	if len(bytes.TrimSpace(src)) == 0 {
		return out, nil
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, "", src, parser.SkipObjectResolution)
	if err != nil {
		return nil, err
	}
	for _, d := range f.Decls {
		fn, ok := d.(*ast.FuncDecl)
		if !ok || fn.Recv != nil || !isTestName(fn.Name.Name) || fn.Name.Name == "TestMain" {
			continue
		}
		var buf bytes.Buffer
		if err := format.Node(&buf, fset, fn); err != nil {
			return nil, err
		}
		out[fn.Name.Name] = tokenText(buf.Bytes())
	}
	return out, nil
}

// tokenText renders Go source as its token sequence without semicolons, so
// whitespace, line breaks and `;` versus newline never count as a change of
// code (a string literal keeps its exact text).
func tokenText(src []byte) string {
	var sc scanner.Scanner
	fset := token.NewFileSet()
	file := fset.AddFile("", fset.Base(), len(src))
	sc.Init(file, src, nil, 0)
	var parts []string
	for {
		_, tok, lit := sc.Scan()
		if tok == token.EOF {
			break
		}
		if tok == token.SEMICOLON {
			continue
		}
		if lit == "" {
			lit = tok.String()
		}
		parts = append(parts, lit)
	}
	return strings.Join(parts, " ")
}

// TestFuncs returns the names of the top-level Test and Fuzz functions in a Go
// test file's source, excluding TestMain, sorted. Found with go/parser.
func TestFuncs(src []byte) ([]string, error) {
	decls, err := testDecls(src)
	if err != nil {
		return nil, err
	}
	names := make([]string, 0, len(decls))
	for n := range decls {
		names = append(names, n)
	}
	sort.Strings(names)
	return names, nil
}

// ModifiedTests returns the Test and Fuzz functions of a file that exist at
// head and are new or whose code (comments ignored) differs from base.
func ModifiedTests(baseSrc, headSrc []byte) ([]string, error) {
	b, err := testDecls(baseSrc)
	if err != nil {
		return nil, err
	}
	h, err := testDecls(headSrc)
	if err != nil {
		return nil, err
	}
	var out []string
	for n, text := range h {
		if old, ok := b[n]; !ok || old != text {
			out = append(out, n)
		}
	}
	sort.Strings(out)
	return out, nil
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
func DiffAddedTests(changed []string, baseSrc, headSrc func(path string) []byte) ([]AddedTest, error) {
	type key struct{ pkg, name string }
	base := map[key]bool{}
	head := map[key]string{}
	for _, f := range changed {
		if !IsLinkedTestFile(f) {
			continue
		}
		pkg := filepath.ToSlash(filepath.Dir(f))
		bn, err := TestFuncs(baseSrc(f))
		if err != nil {
			return nil, fmt.Errorf("%s at the merge base: %w", f, err)
		}
		hn, err := TestFuncs(headSrc(f))
		if err != nil {
			return nil, fmt.Errorf("%s at head: %w", f, err)
		}
		for _, n := range bn {
			base[key{pkg, n}] = true
		}
		for _, n := range hn {
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
	return out, nil
}

// Linked reports whether some reference names the test as `file#Name`. A bare
// file reference does not link a NEW test: it was written before the test
// existed and says nothing about it.
func Linked(t AddedTest, refs []TestRef) bool {
	for _, r := range refs {
		if r.Path == t.File && r.Name == t.Name {
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
			// Every dot-directory (.git, scratch worktrees, tooling) is
			// outside the repository's tests, as is the vendored deps tree.
			if p != repoRoot && (strings.HasPrefix(d.Name(), ".") || d.Name() == "deps") {
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
		names, perr := TestFuncs(data)
		if perr != nil {
			return fmt.Errorf("%s: %w", rel, perr)
		}
		for _, n := range names {
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

var skipCallRe = regexp.MustCompile(`\.(?:Skip|SkipNow|Skipf)\(`)

// testFileHeader is everything before the package clause: build constraints and
// the comments around them. Any edit of it, comments included, counts as
// changing a constraint.
func testFileHeader(src []byte) string {
	if m := packageClauseRe.FindIndex(src); m != nil {
		return string(src[:m[0]])
	}
	return string(src)
}

var packageClauseRe = regexp.MustCompile(`(?m)^package\s`)

// LockFileWeakenings reports, for each test file a lock row references, a
// changed build constraint (header before the package clause) or a newly added
// Skip, SkipNow or Skipf call: either stops a lock from running without
// touching the registry.
func LockFileWeakenings(files []string, baseSrc, headSrc func(string) []byte) []string {
	var out []string
	seen := map[string]bool{}
	for _, f := range files {
		if seen[f] || !strings.HasSuffix(f, "_test.go") {
			continue
		}
		seen[f] = true
		b, h := baseSrc(f), headSrc(f)
		if b == nil {
			continue // a new file has nothing to weaken
		}
		if h == nil {
			continue // a deleted file is the stale-reference check's business
		}
		if testFileHeader(b) != testFileHeader(h) {
			out = append(out, fmt.Sprintf("%s: the build constraint header changed in a file a lock references", f))
		}
		if len(skipCallRe.FindAll(h, -1)) > len(skipCallRe.FindAll(b, -1)) {
			out = append(out, fmt.Sprintf("%s: a Skip call was added to a file a lock references", f))
		}
	}
	sort.Strings(out)
	return out
}
