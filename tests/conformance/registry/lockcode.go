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

// hasInit reports whether a Go source declares func init().
func hasInit(src []byte) bool {
	pf := parseGo("", src)
	for _, fn := range pf.funcs {
		if fn.Recv == nil && fn.Name.Name == "init" {
			return true
		}
	}
	return false
}

// LockCodeChanges lists the lock code a PR changed: a file that is lock code at
// the merge base or at head and differs between them (edited, or deleted), and
// in the package of every lock a NEW file that declares init() or TestMain, and,
// in a package under tests/ (never in product code), a NEW non-test file. New test functions in new _test.go files are free.
func LockCodeChanges(baseRefs, headRefs map[string]map[string]bool, baseSrc, headSrc func(string) []byte, baseList, headList PackageFiles) ([]string, error) {
	b, err := LockCodeFiles(baseRefs, baseSrc, baseList)
	if err != nil {
		return nil, fmt.Errorf("at the merge base: %w", err)
	}
	// Only locks that already exist at the merge base are protected: a lock a PR adds is
	// judged by the lock rules (named test, floor, owner review of the row itself), and
	// editing the test it names is how a parity fix is written.
	var out []string
	reasons := b
	for f, why := range reasons {
		bs, hs := baseSrc(f), headSrc(f)
		if bs == nil {
			continue // a file new to the tree is judged below
		}
		if hs == nil || !bytes.Equal(bs, hs) {
			out = append(out, fmt.Sprintf("%s: lock code changed — owner review (%s)", f, why))
		}
	}
	dirs := map[string]bool{}
	for f := range baseRefs {
		dirs[filepath.ToSlash(filepath.Dir(f))] = true
	}
	for d := range dirs {
		inBase := map[string]bool{}
		for _, p := range baseList(d) {
			inBase[p] = true
		}
		for _, p := range headList(d) {
			if inBase[p] {
				continue
			}
			data := headSrc(p)
			switch {
			case !strings.HasSuffix(p, "_test.go"):
				if !strings.HasPrefix(d+"/", "tests/") {
					continue // product code: a new product file is a product change, judged by rule 1
				}
				out = append(out, fmt.Sprintf("%s: lock code changed — owner review (a new non-test file in the package of a lock)", p))
			case hasInit(data):
				out = append(out, fmt.Sprintf("%s: lock code changed — owner review (a new file with init() in the package of a lock)", p))
			}
		}
	}
	sort.Strings(out)
	return out, nil
}
