package registry

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"unicode/utf8"
)

// Lock code is every file whose edit could make a lock pass without comparing
// what it claims to compare: the files of the lock tests, and the files that
// define the helpers those tests call for their comparison. Runtime floors
// (lock_cells.txt) only protect tests/parity. The gate does not block an edit of
// lock code: it lists the files in the report, so the reviewer whose approval the
// pull request requires sees them. Whatever can be checked from content is Rule 3
// (a lost reference, a lowered floor, a new Skip) and stays automatic.

// lockCallDepth bounds how many calls deep the helper search follows a lock
// test's body inside its package. Four reaches the comparison and judging
// helpers of the parity suite (RunParity, compareParityAt, compareCountEqual,
// judgeCounts); the files that hold them are listed by file, so a callee in the
// same file as a nearer one costs nothing extra.
const lockCallDepth = 4

// PackageFiles lists the .go files of one package directory at a revision.
type PackageFiles func(dir string) []string

// LockTestRefs groups the lock rows' test references by file: the named tests
// of each file, or nil for a bare file reference (every test of the file).
func LockTestRefs(rows ...map[string]RowLite) map[string]map[string]bool {
	out := map[string]map[string]bool{}
	for _, rs := range rows {
		for _, r := range rs {
			if !r.Exact() {
				continue
			}
			for _, t := range r.Tests {
				path, name := splitRef(t)
				if !strings.HasSuffix(path, "_test.go") {
					continue
				}
				if out[path] == nil {
					out[path] = map[string]bool{}
				}
				if name == "" {
					out[path][""] = true
				} else {
					out[path][name] = true
				}
			}
		}
	}
	return out
}

// callGraphScope reports whether a package file may carry test support that a
// lock test depends on: any _test.go file, and any file under tests/ (the
// parity suite keeps its helpers in plain .go files). Product code in a product
// package is the code under test, not lock code.
func callGraphScope(path string) bool {
	return strings.HasSuffix(path, "_test.go") || strings.HasPrefix(path, "tests/")
}

type parsedFile struct {
	path  string
	funcs []*ast.FuncDecl
	err   error
}

func parseGo(path string, src []byte) parsedFile {
	if src == nil {
		return parsedFile{path: path}
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
	if err != nil {
		return parsedFile{path: path, err: err}
	}
	pf := parsedFile{path: path}
	for _, d := range f.Decls {
		if fn, ok := d.(*ast.FuncDecl); ok {
			pf.funcs = append(pf.funcs, fn)
		}
	}
	return pf
}

func calledNames(fn *ast.FuncDecl) map[string]bool {
	out := map[string]bool{}
	if fn.Body == nil {
		return out
	}
	ast.Inspect(fn.Body, func(n ast.Node) bool {
		call, ok := n.(*ast.CallExpr)
		if !ok {
			return true
		}
		switch f := call.Fun.(type) {
		case *ast.Ident:
			out[f.Name] = true
		case *ast.SelectorExpr:
			out[f.Sel.Name] = true
		}
		return true
	})
	return out
}

// LockCodeFiles returns the files that are lock code at one revision, with the
// reason each is. src reads a file's blob, list lists a package directory's
// .go files. A package that does not parse is an error: the gate cannot judge
// what it cannot read.
func LockCodeFiles(refs map[string]map[string]bool, src func(string) []byte, list PackageFiles) (map[string]string, error) {
	out := map[string]string{}
	byDir := map[string][]string{}
	for f := range refs {
		d := filepath.ToSlash(filepath.Dir(f))
		byDir[d] = append(byDir[d], f)
	}
	for dir, lockFiles := range byDir {
		sort.Strings(lockFiles)
		var files []parsedFile
		byName := map[string][]*ast.FuncDecl{}
		fileOf := map[*ast.FuncDecl]string{}
		for _, p := range list(dir) {
			pf := parseGo(p, src(p))
			if pf.err != nil {
				return nil, fmt.Errorf("%s: %w", p, pf.err)
			}
			files = append(files, pf)
			for _, fn := range pf.funcs {
				byName[fn.Name.Name] = append(byName[fn.Name.Name], fn)
				fileOf[fn] = p
			}
		}
		parsed := map[string]parsedFile{}
		for _, pf := range files {
			parsed[pf.path] = pf
		}
		type item struct {
			fn    *ast.FuncDecl
			depth int
			why   string
		}
		var queue []item
		seen := map[*ast.FuncDecl]bool{}
		for _, lf := range lockFiles {
			out[lf] = "it holds a lock test"
			for _, fn := range parsed[lf].funcs {
				if fn.Recv != nil || !isTestName(fn.Name.Name) || fn.Name.Name == "TestMain" {
					continue
				}
				if refs[lf][""] || refs[lf][fn.Name.Name] {
					queue = append(queue, item{fn, 0, fn.Name.Name})
					seen[fn] = true
				}
			}
		}
		for len(queue) > 0 {
			it := queue[0]
			queue = queue[1:]
			if it.depth >= lockCallDepth {
				continue
			}
			names := make([]string, 0)
			for n := range calledNames(it.fn) {
				names = append(names, n)
			}
			sort.Strings(names)
			for _, n := range names {
				for _, callee := range byName[n] {
					cf := fileOf[callee]
					if seen[callee] || !callGraphScope(cf) {
						continue
					}
					seen[callee] = true
					if _, ok := out[cf]; !ok {
						out[cf] = fmt.Sprintf("a lock test (%s) calls %s from it", it.why, n)
					}
					queue = append(queue, item{callee, it.depth + 1, it.why})
				}
			}
		}
	}
	return out, nil
}

// TreeFiles lists every regular file under a path prefix (a directory or one
// file) at a revision, recursively.
type TreeFiles func(prefix string) []string

// lockRuntimeConfig is the runtime configuration the lock suites run against: the
// parity stack and its seed data, the e2e stack, and the data generator that
// seeds them. Editing or deleting an existing file there changes what every lock
// compares, so it is lock code like the tests themselves. lock_cells.txt and
// known_failures.txt are excluded: they have their own ratchets (a floor may only
// grow, the allowlist may only shrink), and README.md is prose.
var lockRuntimeConfig = []string{
	"tests/parity/",
	"deployment/docker/docker-compose-e2e.yml",
	"deployment/docker/lakehouse-e2e-config.yml",
	"cmd/datagen/",
}

var lockRuntimeExempt = map[string]bool{
	"tests/parity/lock_cells.txt":     true,
	"tests/parity/known_failures.txt": true,
	"tests/parity/README.md":          true,
}

// declaresOnlyTests reports whether a new Go file in a lock package declares
// nothing but funcs (tests, fuzz targets, benchmarks, helpers), consts, types
// and imports. A package-level var (whose initializer runs at load and can
// repoint shared state), init() and TestMain are not allowed: the returned
// string names the first offender, "" if there is none.
func declaresOnlyTests(path string, src []byte) (string, error) {
	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
	if err != nil {
		return "", err
	}
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil && (d.Name.Name == "init" || d.Name.Name == "TestMain") {
				return "func " + d.Name.Name, nil
			}
		case *ast.GenDecl:
			if d.Tok == token.VAR {
				return "a package-level var", nil
			}
		}
	}
	return "", nil
}

// LockCodeChanges lists the lock-affecting files a PR changed, judged against the
// locks that exist at the merge base (a lock the PR adds itself is not protected
// yet). The result is informational: the caller reports it, it does not fail.
//
//  1. A lock package is a directory that holds a lock-referenced test file. In a pure
//     test package (anything under tests/) every existing .go file that the PR edits
//     or deletes is lock code: the package shares state, so any file of it can make a
//     lock compare something else. In a product package (internal/..., lakehouse-traces/...,
//     cmd/...) only the existing _test.go files count; product code is what the locks
//     verify, so editing it is free.
//  2. A NEW .go file in a pure test package, or a NEW _test.go file in a product package,
//     is free when it declares only funcs, consts and types. A package-level var, init()
//     or TestMain, or a file that does not parse, is lock code. New non-test files in a
//     product package are free.
//  3. Existing files of the lock suites' runtime configuration (lockRuntimeConfig)
//     that the PR edits or deletes.
//  4. The call-graph helper search (LockCodeFiles) names the reason for the files it
//     reaches; it stays inside the lock test's package, so rule 1 covers everything
//     it finds. Residual risk: a lock that compares through a func value, through
//     another package, or deeper than lockCallDepth is only covered by rule 1 for
//     its own package.
func LockCodeChanges(baseRefs, headRefs map[string]map[string]bool, baseSrc, headSrc func(string) []byte, baseList, headList PackageFiles, baseTree, headTree TreeFiles) ([]string, error) {
	why, err := LockCodeFiles(baseRefs, baseSrc, baseList)
	if err != nil {
		return nil, fmt.Errorf("at the merge base: %w", err)
	}
	var out []string
	flagged := map[string]bool{}
	flag := func(f, reason string) {
		if !flagged[f] {
			flagged[f] = true
			out = append(out, fmt.Sprintf("%s: %s", f, reason))
		}
	}
	dirs := map[string]bool{}
	for f := range baseRefs {
		dirs[filepath.ToSlash(filepath.Dir(f))] = true
	}
	for d := range dirs {
		pureTest := strings.HasPrefix(d+"/", "tests/")
		inBase := map[string]bool{}
		for _, p := range baseList(d) {
			inBase[p] = true
			// In a pure test package every file is lock support. In a product package only the
			// test files are: product code is what the locks verify, not a way to game them.
			if !pureTest && !strings.HasSuffix(p, "_test.go") {
				continue
			}
			bs, hs := baseSrc(p), headSrc(p)
			if hs == nil || !bytes.Equal(bs, hs) {
				reason := "an existing file of a package that holds a lock test"
				if !pureTest {
					reason = "an existing test file of a package that holds a lock test"
				}
				if r, ok := why[p]; ok {
					reason = r + "; any file of that package can change what it compares"
					if !pureTest {
						reason = r
					}
				}
				flag(p, reason)
			}
		}
		for _, p := range headList(d) {
			if inBase[p] || (!pureTest && !strings.HasSuffix(p, "_test.go")) {
				continue
			}
			bad, perr := declaresOnlyTests(p, headSrc(p))
			switch {
			case perr != nil:
				flag(p, "a new file in the package of a lock that does not parse")
			case bad != "":
				flag(p, "a new file in the package of a lock declares "+bad)
			}
		}
	}
	for _, prefix := range lockRuntimeConfig {
		for _, p := range baseTree(prefix) {
			if lockRuntimeExempt[p] {
				continue
			}
			if hs := headSrc(p); hs == nil || !bytes.Equal(baseSrc(p), hs) {
				if hs != nil && GolangImageBumpOnly(baseSrc(p), hs) {
					continue // the Go toolchain image of a build container moved; nothing a lock compares changed
				}
				flag(p, "runtime configuration of the lock suites")
			}
		}
	}
	sort.Strings(out)
	return out, nil
}

// loadTimeDecls returns what a Go file declares that runs at package load or on every test binary
// start: the count of init() functions and the multiset of package-level var names. A package-level
// var initializer and init() run before any test, can call os.Exit, and can repoint shared state, so
// a lock package may not gain one (TestMain is checked by TestMainChanges).
func loadTimeDecls(path string, src []byte) (inits int, vars map[string]int, err error) {
	vars = map[string]int{}
	if src == nil {
		return 0, vars, nil
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
	if err != nil {
		return 0, nil, err
	}
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil && d.Name.Name == "init" {
				inits++
			}
		case *ast.GenDecl:
			if d.Tok != token.VAR {
				continue
			}
			for _, sp := range d.Specs {
				if vs, ok := sp.(*ast.ValueSpec); ok {
					for _, n := range vs.Names {
						vars[n.Name]++
					}
				}
			}
		}
	}
	return inits, vars, nil
}

// LockInitVarAdditions lists the files of a lock package that gain an init() function or a
// package-level var at head: a new file that declares one, or an existing file whose declarations
// now include one the merge base did not have (per name, `_` counted). Lock packages are judged
// against the locks that exist at the merge base. In a pure test package (under tests/) every .go
// file is in scope; in a product package only the _test.go files are. A file that no longer parses
// is listed too: the package would not build and its locks would not run. Removing a var or an
// init() is free; so is a func, const or type.
//
// This is a content rule (hard, Rule 3): the owner's approval is not what stops a lock from being
// silenced by `func init() { if os.Getenv(...) != "" { os.Exit(0) } }`.
func LockInitVarAdditions(baseRefs map[string]map[string]bool, baseSrc, headSrc func(string) []byte, headList PackageFiles) []string {
	dirs := map[string]bool{}
	for f := range baseRefs {
		dirs[filepath.ToSlash(filepath.Dir(f))] = true
	}
	var out []string
	for d := range dirs {
		pureTest := strings.HasPrefix(d+"/", "tests/")
		for _, p := range headList(d) {
			if !pureTest && !strings.HasSuffix(p, "_test.go") {
				continue
			}
			hs := headSrc(p)
			if hs == nil || bytes.Equal(baseSrc(p), hs) {
				continue
			}
			hi, hv, err := loadTimeDecls(p, hs)
			if err != nil {
				out = append(out, p+": does not parse, so the locks of its package would not run")
				continue
			}
			bi, bv, berr := loadTimeDecls(p, baseSrc(p))
			if berr != nil {
				bi, bv = 0, map[string]int{} // a base that did not parse has nothing to compare: all of it is new
			}
			var added []string
			if hi > bi {
				added = append(added, fmt.Sprintf("%d new init() function(s)", hi-bi))
			}
			var names []string
			for n, c := range hv {
				if c > bv[n] {
					names = append(names, n)
				}
			}
			sort.Strings(names)
			if len(names) > 0 {
				added = append(added, "package-level var "+strings.Join(names, ", "))
			}
			if len(added) > 0 {
				out = append(out, fmt.Sprintf("%s: adds %s to a package that holds a registry lock", p, strings.Join(added, " and ")))
			}
		}
	}
	sort.Strings(out)
	return out
}

// golangImageLine matches a compose `image: golang:<version>[-alpine[<ver>]]` line.
// pre is everything before the version (indentation and key), so the two lines of a
// pair must agree on it exactly.
var golangImageLine = regexp.MustCompile(`^(?P<pre>[ \t]+image:[ \t]+golang:)(?P<ver>[0-9]+\.[0-9]+(?:\.[0-9]+)?)(?P<alp>-alpine(?:[0-9]+\.[0-9]+)?)?[ \t]*$`)

// GolangImageBumpOnly reports whether head differs from base only in the version of
// `image: golang:<version>` lines: same line count, every differing line pair is such a
// line in both revisions with the same indentation, the same image family and the same
// suffix shape (`-alpine` stays `-alpine`, its optional version may change), and
// every other line byte-identical. The Python classifier (scripts/ci/pr_classify.py)
// holds the same rule; both are tested against the same fixture.
func GolangImageBumpOnly(base, head []byte) bool {
	// A control byte (CR included) or a non-UTF-8 byte means the file is not read the way the
	// Python twin and Docker read it: nothing is verified. Only "\n" separates lines; \d and \S
	// below are ASCII in RE2, like the Python twin's [0-9] and [!-~].
	if hasUnreadableBytes(base) || hasUnreadableBytes(head) {
		return false
	}
	bl := strings.Split(string(base), "\n")
	hl := strings.Split(string(head), "\n")
	if len(bl) != len(hl) {
		return false
	}
	changed := false
	for i := range bl {
		if bl[i] == hl[i] {
			continue
		}
		bm, hm := golangImageLine.FindStringSubmatch(bl[i]), golangImageLine.FindStringSubmatch(hl[i])
		if bm == nil || hm == nil || bm[1] != hm[1] {
			return false
		}
		if imageSuffixShape(bm[3]) != imageSuffixShape(hm[3]) {
			return false
		}
		changed = true
	}
	return changed
}

// hasUnreadableBytes reports whether b is not clean text: invalid UTF-8, a control byte other than
// TAB and LF (CR, VT, FF, FS, GS, RS, DEL ...), or one of U+0085, U+2028, U+2029 and a BOM. The
// Python classifier's clean_text rejects the same set.
func hasUnreadableBytes(b []byte) bool {
	if !utf8.Valid(b) {
		return true
	}
	for _, r := range string(b) {
		if r == '\n' || r == '\t' {
			continue
		}
		if r < 0x20 || r == 0x7f || r == 0x85 || r == 0x2028 || r == 0x2029 || r == 0xfeff {
			return true
		}
	}
	return false
}

// imageSuffixShape reduces an image tag suffix to its shape: none, bare `-alpine`, or
// `-alpine` with a version. A bump may change the version, never the shape.
func imageSuffixShape(suffix string) string {
	switch suffix {
	case "":
		return ""
	case "-alpine":
		return "-alpine"
	default:
		return "-alpineV"
	}
}
