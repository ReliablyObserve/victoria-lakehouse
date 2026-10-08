package registry

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"path/filepath"
	"sort"
	"strings"
)

// Lock code is every file whose edit could make a lock pass without comparing
// what it claims to compare: the files of the lock tests, and the files that
// define the helpers those tests call for their comparison. Runtime floors
// (lock_cells.txt) only protect tests/parity; this gate protects all of it by
// making any change to lock code the owner's decision.

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

// LockCodeChanges lists the lock code a PR changed, judged against the locks that
// exist at the merge base (a lock the PR adds itself is not protected yet).
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
			out = append(out, fmt.Sprintf("%s: lock code changed — owner review (%s)", f, reason))
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
				flag(p, "runtime configuration of the lock suites")
			}
		}
	}
	sort.Strings(out)
	return out, nil
}
