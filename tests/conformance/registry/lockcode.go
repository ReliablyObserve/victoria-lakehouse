package registry

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/build/constraint"
	"go/parser"
	"go/scanner"
	"go/token"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
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

// nativeSourceExt are the non-Go source files the go tool compiles into a package: cgo's C/C++/Objective-C
// sources and headers, assembly, Fortran and SWIG files, and prebuilt .syso objects. None of them is
// a .go file, so packageFiles never sees them, yet a constructor in one runs before any test.
var nativeSourceExt = map[string]bool{
	".c": true, ".cc": true, ".cpp": true, ".cxx": true, ".h": true, ".hh": true, ".hpp": true, ".hxx": true,
	".m": true, ".mm": true, ".s": true, ".S": true, ".sx": true, ".syso": true,
	".f": true, ".F": true, ".f90": true, ".for": true, ".swig": true, ".swigcxx": true,
}

// goImport is one import of a Go file: its path, and whether it is a blank (`_`) or dot (`.`) import.
type goImport struct {
	path  string
	blank bool
}

// importsOf returns the imports of a Go file; ok is false when the file does not parse.
func importsOf(path string, src []byte) (imps []goImport, ok bool) {
	if src == nil {
		return nil, true
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.ImportsOnly)
	if err != nil {
		return nil, false
	}
	for _, im := range f.Imports {
		p := strings.Trim(im.Path.Value, "\"`")
		imps = append(imps, goImport{path: p, blank: im.Name != nil && (im.Name.Name == "_" || im.Name.Name == ".")})
	}
	return imps, true
}

// stdlibImport reports whether an import path is a standard-library package: the first element of
// its path has no dot. "C" is not a package but cgo's pseudo-import; it is not standard library for
// this purpose and has a rule of its own (LockCgoAdditions).
func stdlibImport(p string) bool {
	if p == "C" {
		return false
	}
	first, _, _ := strings.Cut(p, "/")
	return !strings.Contains(first, ".")
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
//  2. EVERY NEW .go file of a lock package (a pure test package or a product package, test file
//     or not) is listed, whatever it declares. A test file that declares only funcs still
//     runs inside the lock package: `func TestAAA(t *testing.T) { vlBaseURL = lhBaseURL }`
//     sorts first and every later lock then compares Lakehouse with itself; a test that
//     prints a forged transcript and exits does the same. No syntactic rule can tell that
//     from a legitimate new test, so the reviewer reads it. A file that does not parse is
//     listed with that reason, and one that declares a var, init() or TestMain says so.
//  3. Any .go file of a lock package (new or edited, test or product) that gains an import
//     of a non-standard-library package no file of the package imported at the merge base
//     (a blank import runs that package's init() before any test), and every new native
//     source file (.c, .s, .syso ...) in a lock package directory.
//  4. Existing files of the lock suites' runtime configuration (lockRuntimeConfig)
//     that the PR edits or deletes.
//  5. The call-graph helper search (LockCodeFiles) names the reason for the files it
//     reaches; it stays inside the lock test's package, so rule 1 covers everything
//     it finds. Residual risk: a lock that compares through a func value, through
//     another package, or deeper than lockCallDepth is only covered by rule 1 for
//     its own package.
//
// Load-time code (init, called package-level vars) and cgo are hard failures elsewhere
// (LockInitVarAdditions, LockCgoAdditions); this function only makes sure a person sees the files.
func LockCodeChanges(baseRefs, headRefs map[string]map[string]bool, baseSrc, headSrc func(string) []byte, baseList, headList PackageFiles, baseTree, headTree TreeFiles) ([]string, error) {
	list, err := LockCodeChangeList(baseRefs, headRefs, baseSrc, headSrc, baseList, headList, baseTree, headTree)
	if err != nil {
		return nil, err
	}
	out := make([]string, 0, len(list))
	for _, c := range list {
		out = append(out, c.String())
	}
	return out, nil
}

// LockChange is one file the gate lists for the reviewer, with the reason. A path may hold ": ", so the
// two parts stay separate until they are printed.
type LockChange struct{ Path, Reason string }

func (c LockChange) String() string { return c.Path + ": " + c.Reason }

// LockCodeChangeList is LockCodeChanges with the path and the reason kept apart.
func LockCodeChangeList(baseRefs, headRefs map[string]map[string]bool, baseSrc, headSrc func(string) []byte, baseList, headList PackageFiles, baseTree, headTree TreeFiles) ([]LockChange, error) {
	why, err := LockCodeFiles(baseRefs, baseSrc, baseList)
	if err != nil {
		return nil, fmt.Errorf("at the merge base: %w", err)
	}
	reasons := map[string][]string{}
	var order []string
	flag := func(f, reason string) {
		if _, ok := reasons[f]; !ok {
			order = append(order, f)
		}
		for _, r := range reasons[f] {
			if r == reason {
				return
			}
		}
		reasons[f] = append(reasons[f], reason)
	}
	dirs := map[string]bool{}
	for f := range baseRefs {
		dirs[filepath.ToSlash(filepath.Dir(f))] = true
	}
	for d := range dirs {
		pureTest := strings.HasPrefix(d+"/", "tests/")
		inBase := map[string]bool{}
		baseImports := map[string]bool{}
		for _, p := range baseList(d) {
			inBase[p] = true
			if imps, ok := importsOf(p, baseSrc(p)); ok {
				for _, im := range imps {
					baseImports[im.path] = true
				}
			}
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
			hs := headSrc(p)
			if !inBase[p] {
				bad, perr := declaresOnlyTests(p, hs)
				switch {
				case perr != nil:
					flag(p, "a new file in the package of a lock that does not parse")
				case !pureTest && !strings.HasSuffix(p, "_test.go"):
					// product code: it declares vars and init() as a matter of course, so the reason says only
					// what is true of every such file
					flag(p, "a new Go file in a package that holds a registry lock: it is linked into the package's test binary")
				case bad != "":
					flag(p, "a new file in the package of a lock declares "+bad)
				default:
					flag(p, "a new Go file in a package that holds a registry lock: it runs inside the lock package")
				}
			}
			if hs == nil || (inBase[p] && bytes.Equal(baseSrc(p), hs)) {
				continue
			}
			imps, ok := importsOf(p, hs)
			if !ok {
				continue
			}
			var fresh []string
			for _, im := range imps {
				if im.path == "C" || stdlibImport(im.path) || baseImports[im.path] {
					continue
				}
				kind := "imports"
				if im.blank {
					kind = "blank-imports"
				}
				fresh = append(fresh, kind+" "+strconv.Quote(im.path))
			}
			if len(fresh) > 0 {
				sort.Strings(fresh)
				flag(p, strings.Join(fresh, ", ")+", which no file of the package imported at the merge base (a package's init() runs before any test)")
			}
		}
		// new native sources next to the Go files
		baseNative := map[string]bool{}
		for _, p := range baseTree(d + "/") {
			baseNative[p] = true
		}
		for _, p := range headTree(d + "/") {
			if filepath.ToSlash(filepath.Dir(p)) == d && nativeSourceExt[filepath.Ext(p)] && !baseNative[p] {
				flag(p, "a new native source file (cgo, assembly or an object file) in a package that holds a registry lock: it is compiled into the lock package")
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
	sort.Strings(order)
	out := make([]LockChange, 0, len(order))
	for _, f := range order {
		out = append(out, LockChange{Path: f, Reason: strings.Join(reasons[f], "; ")})
	}
	return out, nil
}

// LockCgoAdditions lists the files of a lock package that start importing "C" at head: cgo is
// load-time code the parser-level init()/var rules cannot see (an `__attribute__((constructor))` in the
// preamble runs before main, and a test can end the binary with success from it). Every .go file of the
// package counts, new or edited, test or product, because all of them are linked into the package's test
// binary. A file that already imported "C" at the merge base is not new cgo. The repository has no cgo
// in a lock package, so this stays quiet in practice.
func LockCgoAdditions(baseRefs map[string]map[string]bool, baseSrc, headSrc func(string) []byte, baseList, headList PackageFiles) []string {
	var out []string
	for d := range LockDirs(baseRefs) {
		for _, p := range cgoAdded(d, baseSrc, headSrc, headList, func(string) bool { return true }) {
			out = append(out, p+`: imports "C" (cgo) in a package that holds a registry lock`)
		}
	}
	sort.Strings(out)
	return out
}

// cgoAdded lists the paths of the files of dir d (inScope picks them) that import "C" at head and did not
// at the merge base.
func cgoAdded(d string, baseSrc, headSrc func(string) []byte, headList PackageFiles, inScope func(string) bool) []string {
	hasC := func(path string, src []byte) bool {
		imps, _ := importsOf(path, src)
		for _, im := range imps {
			if im.path == "C" {
				return true
			}
		}
		return false
	}
	var out []string
	for _, p := range headList(d) {
		hs := headSrc(p)
		if !inScope(p) || hs == nil || bytes.Equal(baseSrc(p), hs) || !hasC(p, hs) || hasC(p, baseSrc(p)) {
			continue
		}
		out = append(out, p)
	}
	return out
}

// LockDirs returns the directories that hold a lock test file.
func LockDirs(refs ...map[string]map[string]bool) map[string]bool {
	dirs := map[string]bool{}
	for _, r := range refs {
		for f := range r {
			dirs[filepath.ToSlash(filepath.Dir(f))] = true
		}
	}
	return dirs
}

// predeclaredTypes are the identifiers a call expression can name without calling a function: a
// conversion such as int64(x) or string(b). Everything else spelled `name(...)` is a function call.
var predeclaredTypes = map[string]bool{
	"bool": true, "byte": true, "rune": true, "string": true, "error": true, "any": true,
	"int": true, "int8": true, "int16": true, "int32": true, "int64": true,
	"uint": true, "uint8": true, "uint16": true, "uint32": true, "uint64": true, "uintptr": true,
	"float32": true, "float64": true, "complex64": true, "complex128": true,
}

// pureBuiltins are the builtin functions that run no user code.
var pureBuiltins = map[string]bool{
	"len": true, "cap": true, "make": true, "new": true, "append": true, "min": true, "max": true,
	"complex": true, "real": true, "imag": true,
}

// pureStdlibCalls are the standard-library constructors a test file may call in a package-level var
// initializer: they cannot end the test binary with success (regexp.MustCompile panics on a bad
// pattern, which fails the run). Keyed by import path; a file that imports another package under the
// name `regexp` gets no exemption.
var pureStdlibCalls = map[string]map[string]bool{
	"regexp": {"MustCompile": true, "MustCompilePOSIX": true},
	"errors": {"New": true},
}

// runsCodeAtLoad reports whether evaluating the expression at package load can run user code: it
// contains a function call that is not a conversion to a predeclared or composite type, not a pure
// builtin and not one of the pure standard-library constructors (pureStdlibCalls). A function literal is only code that CAN run later; its body is not run at load, so it is
// not descended into (calling it, `func() int {...}()`, is itself a call and is flagged).
func runsCodeAtLoad(e ast.Expr, imports map[string]string) bool {
	// isConversion reports whether fun names a type that is converted to, not a function that is called.
	// Only syntactically certain cases count; a user-defined type name or a qualified one looks like a
	// function and is treated as a call (conservative).
	var isConversion func(fun ast.Expr) bool
	isConversion = func(fun ast.Expr) bool {
		switch f := fun.(type) {
		case *ast.ArrayType, *ast.MapType, *ast.ChanType, *ast.FuncType, *ast.InterfaceType, *ast.StructType:
			return true
		case *ast.ParenExpr:
			return isConversion(f.X)
		case *ast.Ident:
			return predeclaredTypes[f.Name] || pureBuiltins[f.Name]
		}
		return false
	}
	found := false
	ast.Inspect(e, func(n ast.Node) bool {
		switch n := n.(type) {
		case *ast.FuncLit:
			return false
		case *ast.CallExpr:
			if sel, ok := n.Fun.(*ast.SelectorExpr); ok {
				if id, ok := sel.X.(*ast.Ident); ok && pureStdlibCalls[imports[id.Name]][sel.Sel.Name] {
					return true // pure: its arguments are still inspected
				}
			}
			if !isConversion(n.Fun) {
				found = true
			}
		}
		return !found
	})
	return found
}

// declKey identifies one load-time declaration. display is what the report shows (the name and the
// whitespace-normalised source); sig is everything that decides WHAT RUNS when the package loads:
//
//   - the exact token stream of the declaration (go/scanner: string and rune literals keep their exact
//     bytes, so "a b" and "a  b" differ; only insignificant white space, comments and the automatic
//     semicolons' spelling are normalised away);
//   - the RESOLVED import path of every package qualifier the declaration uses (`schema.F` is
//     `schema` the alias or package clause of which import path: moving the declaration into a file
//     where `schema` names another package is a different declaration), and the dot-imported paths of
//     the file (a dot import brings names in without a qualifier);
//   - the file's build constraints (`//go:build`, `// +build`, and the GOOS/GOARCH file-name suffix):
//     moving a declaration into or out of a constrained file changes whether it runs at all.
//
// A pure move or rename of the file therefore keeps the key; a move that changes what the code means
// does not.
type declKey struct{ display, sig string }

// fileScope is what a file contributes to the key of every declaration in it.
type fileScope struct {
	imports     []fileImport
	dotImports  []string
	constraints string
}

type fileImport struct {
	alias string // "" when the import has no explicit name
	path  string
}

// pkgNames resolves the default name of an imported package. For a package of this repository the
// name is its `package` clause, read from the revision's blobs: the author of the change chooses it, so
// the last path element proves nothing (`tests/evil` may declare `package schema`). For any other
// package the candidates are derived from the path (the name the go tool itself guesses for it).
type pkgNames struct {
	root  string // module path of the repository
	src   func(string) []byte
	list  PackageFiles
	cache map[string][]string
}

// repoModule is the module path of this repository, used when go.mod cannot be read (the unit-test
// trees have none). lakehouse-traces/ has its own go.mod but its module path extends the root one, so
// one prefix maps both.
const repoModule = "github.com/ReliablyObserve/victoria-lakehouse"

func newPkgNames(src func(string) []byte, list PackageFiles) *pkgNames {
	root := repoModule
	for _, line := range strings.Split(string(src("go.mod")), "\n") {
		if f := strings.Fields(line); len(f) == 2 && f[0] == "module" {
			root = strings.Trim(f[1], "\"")
			break
		}
	}
	return &pkgNames{root: root, src: src, list: list, cache: map[string][]string{}}
}

// repoDir maps an import path of this repository to its directory ("" when it is not one).
func (n *pkgNames) repoDir(importPath string) string {
	if importPath == n.root {
		return "."
	}
	if strings.HasPrefix(importPath, n.root+"/") {
		return strings.TrimPrefix(importPath, n.root+"/")
	}
	return ""
}

// candidates are the local names an unaliased import of importPath can have.
func (n *pkgNames) candidates(importPath string) []string {
	if c, ok := n.cache[importPath]; ok {
		return c
	}
	var out []string
	if dir := n.repoDir(importPath); dir != "" {
		for _, p := range n.list(dir) {
			if strings.HasSuffix(p, "_test.go") {
				continue
			}
			if f, err := parser.ParseFile(token.NewFileSet(), p, n.src(p), parser.PackageClauseOnly); err == nil {
				out = append(out, f.Name.Name)
				break
			}
		}
	}
	if len(out) == 0 {
		out = pathNames(importPath)
	}
	n.cache[importPath] = out
	return out
}

// pathNames are the names a package path suggests: its last element, without a version suffix
// element (/v2), a "go-" prefix, a "-go" suffix or a ".vN" / ".go" tail.
func pathNames(importPath string) []string {
	parts := strings.Split(importPath, "/")
	last := parts[len(parts)-1]
	if len(parts) > 1 && len(last) > 1 && last[0] == 'v' && strings.Trim(last[1:], "0123456789") == "" {
		last = parts[len(parts)-2]
	}
	out := []string{last}
	if i := strings.Index(last, "."); i > 0 {
		out = append(out, last[:i])
	}
	out = append(out, strings.TrimSuffix(strings.TrimPrefix(last, "go-"), "-go"), strings.ReplaceAll(last, "-", "_"))
	return out
}

// buildConstraints returns the normalised build constraints of a file: its //go:build and // +build
// lines (before the package clause) and the implicit GOOS/GOARCH constraint of its name.
func buildConstraints(path string, f *ast.File) string {
	var out []string
	for _, cg := range f.Comments {
		if cg.Pos() >= f.Package {
			break
		}
		for _, c := range cg.List {
			if !constraint.IsGoBuild(c.Text) && !constraint.IsPlusBuild(c.Text) {
				continue
			}
			if x, err := constraint.Parse(c.Text); err == nil {
				out = append(out, x.String())
			} else {
				out = append(out, strings.TrimSpace(c.Text))
			}
		}
	}
	if sfx := fileNameConstraint(path); sfx != "" {
		out = append(out, "file:"+sfx)
	}
	sort.Strings(out)
	return strings.Join(out, ";")
}

var knownOS = map[string]bool{"aix": true, "android": true, "darwin": true, "dragonfly": true, "freebsd": true, "hurd": true, "illumos": true, "ios": true, "js": true, "linux": true, "nacl": true, "netbsd": true, "openbsd": true, "plan9": true, "solaris": true, "wasip1": true, "windows": true, "zos": true}

var knownArch = map[string]bool{"386": true, "amd64": true, "amd64p32": true, "arm": true, "armbe": true, "arm64": true, "arm64be": true, "loong64": true, "mips": true, "mipsle": true, "mips64": true, "mips64le": true, "mips64p32": true, "mips64p32le": true, "ppc": true, "ppc64": true, "ppc64le": true, "riscv": true, "riscv64": true, "s390": true, "s390x": true, "sparc": true, "sparc64": true, "wasm": true}

// fileNameConstraint is the GOOS/GOARCH suffix of a file name the go tool treats as a build constraint
// (`x_windows.go`, `x_linux_amd64_test.go`), "" when there is none. It follows go/build's rule: the part
// before the first underscore is never a constraint.
func fileNameConstraint(path string) string {
	name := filepath.Base(path)
	name, _, _ = strings.Cut(name, ".")
	i := strings.Index(name, "_")
	if i < 0 {
		return ""
	}
	l := strings.Split(name[i:], "_")
	if n := len(l); n > 0 && l[n-1] == "test" {
		l = l[:n-1]
	}
	n := len(l)
	switch {
	case n >= 2 && knownOS[l[n-2]] && knownArch[l[n-1]]:
		return l[n-2] + "_" + l[n-1]
	case n >= 1 && (knownOS[l[n-1]] || knownArch[l[n-1]]):
		return l[n-1]
	}
	return ""
}

// scopeOf collects the imports and the constraints of a parsed file.
func scopeOf(path string, f *ast.File) fileScope {
	sc := fileScope{constraints: buildConstraints(path, f)}
	for _, im := range f.Imports {
		p := strings.Trim(im.Path.Value, "\"`")
		alias := ""
		if im.Name != nil {
			alias = im.Name.Name
		}
		if alias == "." {
			sc.dotImports = append(sc.dotImports, p)
			continue
		}
		sc.imports = append(sc.imports, fileImport{alias: alias, path: p})
	}
	sort.Strings(sc.dotImports)
	return sc
}

// resolveQualifiers maps every package qualifier the node uses (`q` in `q.Name`) to the sorted list of
// import paths of the file it can name, and spells the result as one string. A qualifier no import
// names (a local variable, a field, an import this heuristic cannot name) is written `?q`.
func (sc fileScope) resolveQualifiers(n ast.Node, names *pkgNames) string {
	quals := map[string]bool{}
	ast.Inspect(n, func(x ast.Node) bool {
		if sel, ok := x.(*ast.SelectorExpr); ok {
			if id, ok := sel.X.(*ast.Ident); ok {
				quals[id.Name] = true
			}
		}
		return true
	})
	var out []string
	for q := range quals {
		var paths []string
		for _, im := range sc.imports {
			if im.alias == "_" {
				continue
			}
			if im.alias != "" {
				if im.alias == q {
					paths = append(paths, im.path)
				}
				continue
			}
			for _, c := range names.candidates(im.path) {
				if c == q {
					paths = append(paths, im.path)
					break
				}
			}
		}
		sort.Strings(paths)
		if len(paths) == 0 {
			out = append(out, "?"+q)
		} else {
			out = append(out, q+"=>"+strings.Join(paths, ","))
		}
	}
	sort.Strings(out)
	return strings.Join(out, " ")
}

// tokenStream spells src as its Go token stream: identifiers and literals keep their exact text, every
// other token its canonical spelling, comments and the white space between tokens are dropped, and an
// automatic semicolon equals an explicit one.
func tokenStream(src []byte) string {
	fset := token.NewFileSet()
	file := fset.AddFile("", fset.Base(), len(src))
	var s scanner.Scanner
	s.Init(file, src, func(token.Position, string) {}, 0)
	var sb strings.Builder
	for {
		_, tok, lit := s.Scan()
		if tok == token.EOF {
			break
		}
		switch tok {
		case token.IDENT, token.INT, token.FLOAT, token.IMAG, token.CHAR, token.STRING, token.ILLEGAL:
			sb.WriteString(lit)
		case token.SEMICOLON:
			sb.WriteString(";")
		default:
			sb.WriteString(tok.String())
		}
		sb.WriteByte(0x01)
	}
	return sb.String()
}

// loadTimeDecls returns what a Go file declares that runs code at package load, before any test:
// the multiset of init() functions and the multiset of package-level vars whose initializer calls a
// function. Both are keyed by declKey (see there): a literal table (`var cases = []T{{...}}`), a
// conversion or a var without an initializer runs nothing and is not counted. TestMain is checked by
// TestMainChanges.
func loadTimeDecls(path string, src []byte, names *pkgNames) (inits, vars map[declKey]int, err error) {
	inits, vars = map[declKey]int{}, map[declKey]int{}
	if src == nil {
		return inits, vars, nil
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.ParseComments|parser.SkipObjectResolution)
	if err != nil {
		return nil, nil, err
	}
	imports := map[string]string{} // local name -> import path, for the pure standard-library constructors
	for _, im := range f.Imports {
		ip := strings.Trim(im.Path.Value, `"`+"`")
		name := ip[strings.LastIndex(ip, "/")+1:]
		if im.Name != nil {
			name = im.Name.Name
		}
		imports[name] = ip
	}
	sc := scopeOf(path, f)
	sigOf := func(n ast.Node, text []byte) string {
		return tokenStream(text) + "\x00" + sc.resolveQualifiers(n, names) + "\x00" + strings.Join(sc.dotImports, ",") + "\x00" + sc.constraints
	}
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil && d.Name.Name == "init" {
				body := ""
				var sig string
				if d.Body != nil {
					raw := src[fset.Position(d.Body.Pos()).Offset:fset.Position(d.Body.End()).Offset]
					body = string(raw)
					sig = sigOf(d.Body, raw)
				}
				inits[declKey{"init() " + strings.Join(strings.Fields(body), " "), sig}]++
			}
		case *ast.GenDecl:
			if d.Tok != token.VAR {
				continue
			}
			for _, sp := range d.Specs {
				vs, ok := sp.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for i, v := range vs.Values {
					if !runsCodeAtLoad(v, imports) {
						continue
					}
					name := "_"
					if i < len(vs.Names) {
						name = vs.Names[i].Name
					} else if len(vs.Names) > 0 {
						name = vs.Names[0].Name // var a, b = f(): one call, several names
					}
					raw := src[fset.Position(v.Pos()).Offset:fset.Position(v.End()).Offset]
					vars[declKey{name + " = " + strings.Join(strings.Fields(string(raw)), " "), name + "\x02" + sigOf(v, raw)}]++
				}
			}
		}
	}
	return inits, vars, nil
}

// shadowedBuiltins lists the top-level names (functions, types, vars, consts) a Go file declares that
// shadow a predeclared type or a builtin function the load-time check trusts: `func len(...)` in a
// package would turn `var _ = len(x)` into a call of that func.
func shadowedBuiltins(path string, src []byte) []string {
	if src == nil {
		return nil
	}
	f, err := parser.ParseFile(token.NewFileSet(), path, src, parser.SkipObjectResolution)
	if err != nil {
		return nil
	}
	var out []string
	note := func(name string) {
		if predeclaredTypes[name] || pureBuiltins[name] {
			out = append(out, name)
		}
	}
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil {
				note(d.Name.Name)
			}
		case *ast.GenDecl:
			for _, sp := range d.Specs {
				switch sp := sp.(type) {
				case *ast.TypeSpec:
					note(sp.Name.Name)
				case *ast.ValueSpec:
					for _, n := range sp.Names {
						note(n.Name)
					}
				}
			}
		}
	}
	sort.Strings(out)
	return out
}

// LockInitVarAdditions lists the files of a lock package that gain load-time code at head: an
// init() function, or a package-level var whose initializer calls a function (it runs before any
// test). Load-time declarations are compared as a MULTISET PER PACKAGE, not per file: an init() is
// keyed by its normalised body and a var by its name and initializer, so moving a declaration to
// another file of the package, or renaming the file that holds it, adds nothing, while a new
// declaration (another body, another initializer, one more copy) does. The files reported are the
// files at head that gained a declaration the package has more of than at the merge base. Lock
// packages are judged against the locks that exist at the merge base. In a pure test package (under
// tests/) every .go file is in scope; in a product package only the _test.go files are. A file that
// no longer parses is listed too: the package would not build and its locks would not run. Removing
// a var or an init() is free; so is a func, const or type, and so is a var whose initializer runs no
// code: a literal table, a conversion, a var with no initializer (a table cannot end the test binary;
// #438's `var fvPropMaps = []fvPropMap{{...}}` is one). Editing a var's initializer to call a
// function is an addition.
//
// This is a content rule (hard, Rule 3): the owner's approval is not what stops a lock from being
// silenced by `func init() { if os.Getenv(...) != "" { os.Exit(0) } }`.
func LockInitVarAdditions(baseRefs map[string]map[string]bool, baseSrc, headSrc func(string) []byte, baseList, headList PackageFiles) []string {
	dirs := map[string]bool{}
	for f := range baseRefs {
		dirs[filepath.ToSlash(filepath.Dir(f))] = true
	}
	var out []string
	for d := range dirs {
		pureTest := strings.HasPrefix(d+"/", "tests/")
		// A package that newly declares a name shadowing a predeclared type or builtin (len, string, ...)
		// would make the "pure" calls the check trusts into calls of its own code. Every file of the
		// package counts here, test or not: product code shares the package scope with its tests.
		baseShadow := map[string]bool{}
		for _, p := range baseList(d) {
			for _, n := range shadowedBuiltins(p, baseSrc(p)) {
				baseShadow[n] = true
			}
		}
		for _, p := range headList(d) {
			var fresh []string
			for _, n := range shadowedBuiltins(p, headSrc(p)) {
				if !baseShadow[n] {
					fresh = append(fresh, n)
				}
			}
			if len(fresh) > 0 {
				out = append(out, fmt.Sprintf("%s: declares %s, which shadows a predeclared identifier in a package that holds a registry lock", p, strings.Join(fresh, ", ")))
			}
		}
		inScope := func(p string) bool { return pureTest || strings.HasSuffix(p, "_test.go") }
		for _, a := range loadTimeAdditions(d, inScope, baseSrc, headSrc, baseList, headList) {
			var added []string
			if a.inits > 0 {
				added = append(added, fmt.Sprintf("%d new init() function(s)", a.inits))
			}
			if len(a.vars) > 0 {
				added = append(added, "package-level var whose initializer calls a function: "+strings.Join(a.vars, ", "))
			}
			if a.broken {
				out = append(out, a.path+": does not parse, so the locks of its package would not run")
				continue
			}
			out = append(out, fmt.Sprintf("%s: adds %s to a package that holds a registry lock", a.path, strings.Join(added, " and ")))
		}
	}
	sort.Strings(out)
	return out
}

// loadAddition is one file of a package that gains load-time declarations at head.
type loadAddition struct {
	path   string
	inits  int      // new init() functions
	vars   []string // new package-level vars whose initializer calls a function (display text)
	broken bool     // the file changed and no longer parses
}

// loadTimeAdditions compares the load-time declarations of the files of one package directory
// (inScope picks the files that count) as a MULTISET PER PACKAGE: a declaration moved to another file
// or a renamed file adds nothing, a declaration with another key (another body, another initializer,
// other import paths behind its qualifiers, another build constraint, one more copy) does. The files
// reported are the files at head that gained a declaration the package has more of than at the merge
// base.
func loadTimeAdditions(d string, inScope func(p string) bool, baseSrc, headSrc func(string) []byte, baseList, headList PackageFiles) []loadAddition {
	baseNames, headNames := newPkgNames(baseSrc, baseList), newPkgNames(headSrc, headList)
	type decls struct{ inits, vars map[declKey]int }
	baseFile := map[string]decls{}
	basePkgInits, basePkgVars := map[declKey]int{}, map[declKey]int{}
	for _, p := range baseList(d) {
		if !inScope(p) {
			continue
		}
		bi, bv, err := loadTimeDecls(p, baseSrc(p), baseNames)
		if err != nil {
			continue // a base that did not parse has nothing to compare: all of it is new
		}
		baseFile[p] = decls{bi, bv}
		for k, c := range bi {
			basePkgInits[k] += c
		}
		for k, c := range bv {
			basePkgVars[k] += c
		}
	}
	type headDecl struct {
		path         string
		inits, vars  map[declKey]int
		changedBytes bool
	}
	var heads []headDecl
	var out []loadAddition
	headPkgInits, headPkgVars := map[declKey]int{}, map[declKey]int{}
	for _, p := range headList(d) {
		if !inScope(p) {
			continue
		}
		hs := headSrc(p)
		if hs == nil {
			continue
		}
		hi, hv, err := loadTimeDecls(p, hs, headNames)
		if err != nil {
			if !bytes.Equal(baseSrc(p), hs) {
				out = append(out, loadAddition{path: p, broken: true})
			}
			continue
		}
		for k, c := range hi {
			headPkgInits[k] += c
		}
		for k, c := range hv {
			headPkgVars[k] += c
		}
		heads = append(heads, headDecl{p, hi, hv, !bytes.Equal(baseSrc(p), hs)})
	}
	for _, h := range heads {
		if !h.changedBytes {
			continue
		}
		bf := baseFile[h.path]
		a := loadAddition{path: h.path}
		for k, c := range h.inits {
			if gain := c - bf.inits[k]; gain > 0 && headPkgInits[k] > basePkgInits[k] {
				a.inits += min(gain, headPkgInits[k]-basePkgInits[k])
			}
		}
		seen := map[string]bool{}
		for k, c := range h.vars {
			if c > bf.vars[k] && headPkgVars[k] > basePkgVars[k] && !seen[k.display] {
				seen[k.display] = true
				a.vars = append(a.vars, k.display)
			}
		}
		sort.Strings(a.vars)
		if a.inits > 0 || len(a.vars) > 0 {
			out = append(out, a)
		}
	}
	return out
}

// golangImageLine matches a compose `image: golang:<version>[-alpine[<ver>]][@sha256:<64 hex>]` line.
// pre is everything before the version (indentation and key), so the two lines of a
// pair must agree on it exactly. The digest group does not capture: which digest a line
// carries (or none) is not compared, and anything but 64 lowercase hex characters leaves
// the line unmatched.
var golangImageLine = regexp.MustCompile(`^(?P<pre>[ \t]+image:[ \t]+golang:)(?P<ver>[0-9]+\.[0-9]+(?:\.[0-9]+)?)(?P<alp>-alpine(?:[0-9]+\.[0-9]+)?)?(?:@sha256:[0-9a-f]{64})?[ \t]*$`)

// GolangImageBumpOnly reports whether head differs from base only in the version of
// `image: golang:<version>` lines: same line count, every differing line pair is such a
// line in both revisions with the same indentation, the same image family and the same
// suffix shape (`-alpine` stays `-alpine`, its optional version may change; either side may
// carry a digest, and the digests may differ), and every other line byte-identical. The Python classifier (scripts/ci/pr_classify.py)
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
		// a bump moves forward: the Go version and the alpine release may not go back
		goBack, ok1 := versionBefore(hm[2], bm[2])
		alpBack, ok2 := versionBefore(strings.TrimPrefix(hm[3], "-alpine"), strings.TrimPrefix(bm[3], "-alpine"))
		if !ok1 || !ok2 || goBack || alpBack {
			return false
		}
		changed = true
	}
	return changed
}

// versionBefore reports whether dotted version a is numerically older than b ("3.9" is older than
// "3.23"; "1.27" is older than "1.27.1"). Both are digits and dots, as the line regexp guarantees; an
// empty string (no alpine release) is never older than anything. ok is false when a component does not
// fit 31 bits: the Python twin refuses the same (_vtuple), so an overflowing number is never a bump.
func versionBefore(a, b string) (before, ok bool) {
	if a == "" || b == "" {
		return false, true
	}
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) || i < len(bs); i++ {
		var x, y uint64
		var err error
		if i < len(as) {
			if x, err = strconv.ParseUint(as[i], 10, 31); err != nil {
				return false, false
			}
		}
		if i < len(bs) {
			if y, err = strconv.ParseUint(bs[i], 10, 31); err != nil {
				return false, false
			}
		}
		if x != y {
			return x < y, true
		}
	}
	return false, true
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

// ---------------------------------------------------------------------------------------------------
// Symbolic links.
//
// The gate reads blobs, and `packageFiles` / `treeFiles` keep regular files only (git mode 100*). A
// symlink is invisible to every lock rule above, yet `go test` compiles a symlinked .go file: a
// `tests/e2e/zz_link.go -> ../../docs/zz.txt` whose target holds an init() that ends the binary with
// success silences the locks and no rule would have listed it. So a symlink (or a submodule) that a PR
// adds or changes is a hard failure wherever the go tool or a lock could read it.

// DiffEntry is one path of `git diff --raw -z --no-renames`: its status letter and the git modes at the
// two revisions (000000 for an absent side).
type DiffEntry struct{ Path, Status, OldMode, NewMode string }

// ParseDiffRaw parses the output of `git diff --raw -z --no-renames --no-abbrev`.
func ParseDiffRaw(raw []byte) ([]DiffEntry, error) {
	toks := strings.Split(string(raw), "\x00")
	var out []DiffEntry
	for i := 0; i < len(toks); i++ {
		t := toks[i]
		if t == "" {
			continue
		}
		if !strings.HasPrefix(t, ":") {
			return nil, fmt.Errorf("unexpected diff record %q", t)
		}
		f := strings.Fields(t[1:])
		if len(f) != 5 || i+1 >= len(toks) || toks[i+1] == "" {
			return nil, fmt.Errorf("unexpected diff record %q", t)
		}
		i++
		out = append(out, DiffEntry{Path: toks[i], Status: f[4], OldMode: f[0], NewMode: f[1]})
	}
	return out, nil
}

// IsLinkEntry reports whether the entry puts a symbolic link or a submodule into the tree.
func (e DiffEntry) IsLinkEntry() bool {
	return e.Status != "D" && (e.NewMode == "120000" || e.NewMode == "160000")
}

// underAny reports whether path is one of dirs or lies below one. A runtime-config entry ending in "/"
// names a directory, one without it a single file.
func underAny(path string, dirs []string) bool {
	for _, d := range dirs {
		d = strings.TrimSuffix(d, "/")
		if path == d || strings.HasPrefix(path, d+"/") {
			return true
		}
	}
	return false
}

// SymlinkProblems lists the symbolic links and submodules a PR adds or changes that the gate refuses:
// any whose name is a .go file or a native source file (the go tool compiles those), and any under a
// directory of dirs (the packages that hold a lock, the packages a lock package imports, the lock
// suites' runtime configuration), whatever its name or target, because a lock rule reads blobs of
// regular files and cannot see through a link. A link elsewhere (docs/) is free.
func SymlinkProblems(entries []DiffEntry, dirs []string) []string {
	var out []string
	for _, e := range entries {
		if !e.IsLinkEntry() {
			continue
		}
		kind := "a symbolic link"
		if e.NewMode == "160000" {
			kind = "a submodule"
		}
		what := kind + " added"
		switch {
		case e.OldMode == "120000" && e.NewMode == "120000":
			what = "a symbolic link retargeted"
		case strings.HasPrefix(e.OldMode, "100"):
			what = "a regular file replaced by " + kind
		case e.OldMode != "000000":
			what = kind + " replacing another entry"
		}
		switch {
		case strings.HasSuffix(e.Path, ".go"):
			out = append(out, fmt.Sprintf("%s: %s: the go tool compiles a linked .go file and no lock rule can read it", e.Path, what))
		case nativeSourceExt[filepath.Ext(e.Path)]:
			out = append(out, fmt.Sprintf("%s: %s: the go tool compiles a linked native source file and no lock rule can read it", e.Path, what))
		case underAny(e.Path, dirs):
			out = append(out, fmt.Sprintf("%s: %s in a package that holds a registry lock, a package one imports, or the lock suites' configuration", e.Path, what))
		}
	}
	sort.Strings(out)
	return out
}

// RuntimeConfigPaths are the lock suites' runtime configuration entries (see lockRuntimeConfig).
func RuntimeConfigPaths() []string { return append([]string(nil), lockRuntimeConfig...) }

// ---------------------------------------------------------------------------------------------------
// Lock-adjacent packages.
//
// A lock package's test binary also links every package its test layer imports, and an init() or a
// cgo constructor in one of them runs before any test: `tests/ingestmatrix` is imported by tests/e2e,
// is no lock package and no product code, so none of the rules above looked at it. A lock-adjacent
// package is an in-repo package whose NON-test files are linked into a lock package's test binary
// through the lock package's test layer (its _test.go files; every .go file of a package under
// tests/), followed through non-test files to lockAdjacentDepth. The set is computed at the merge base
// AND at head, so a package a PR adds and imports from a lock test is covered from its first commit.
//
// Not covered, by design (tests/conformance/README.md lists them): the product files of the lock
// packages themselves and what they import (the code under test; a registry content change is the only
// control there), dynamic loading (plugins, go:linkname), and anything outside the repository's own
// module path.

// lockAdjacentDepth bounds how far the import search follows non-test files.
const lockAdjacentDepth = 3

// LockAdjacentDirs returns the lock-adjacent package directories at one revision, sorted. A lock
// package itself is never adjacent (it has its own rules).
func LockAdjacentDirs(lockDirs map[string]bool, src func(string) []byte, list PackageFiles) []string {
	names := newPkgNames(src, list)
	adj := map[string]bool{}
	var frontier []string
	visit := func(files []string, next *[]string) {
		for _, p := range files {
			imps, ok := importsOf(p, src(p))
			if !ok {
				continue
			}
			for _, im := range imps {
				dir := names.repoDir(im.path)
				if dir == "" || dir == "." || lockDirs[dir] || adj[dir] || len(list(dir)) == 0 {
					continue
				}
				adj[dir] = true
				*next = append(*next, dir)
			}
		}
	}
	for d := range lockDirs {
		var files []string
		for _, p := range list(d) {
			if strings.HasPrefix(d+"/", "tests/") || strings.HasSuffix(p, "_test.go") {
				files = append(files, p)
			}
		}
		visit(files, &frontier)
	}
	for depth := 2; depth <= lockAdjacentDepth && len(frontier) > 0; depth++ {
		var next []string
		for _, d := range frontier {
			var files []string
			for _, p := range list(d) {
				if !strings.HasSuffix(p, "_test.go") {
					files = append(files, p)
				}
			}
			visit(files, &next)
		}
		frontier = next
	}
	out := make([]string, 0, len(adj))
	for d := range adj {
		out = append(out, d)
	}
	sort.Strings(out)
	return out
}

// LockAdjacentUnion is the lock-adjacent packages at the merge base together with those at head.
func LockAdjacentUnion(lockDirs map[string]bool, baseSrc, headSrc func(string) []byte, baseList, headList PackageFiles) []string {
	seen := map[string]bool{}
	var out []string
	for _, set := range [][]string{LockAdjacentDirs(lockDirs, baseSrc, baseList), LockAdjacentDirs(lockDirs, headSrc, headList)} {
		for _, d := range set {
			if !seen[d] {
				seen[d] = true
				out = append(out, d)
			}
		}
	}
	sort.Strings(out)
	return out
}

// LockAdjacentChanges judges the lock-adjacent packages of a PR. hard are Rule 3 failures: an added
// init() (it runs before any test and can end the test binary), a new import "C", and a new or changed
// native source file. listed are files the reviewer must see: a new Go file in a test-support package
// (under tests/), a package-level var whose initializer calls a function, a blank or dot import, an
// import of a module outside this repository, and a changed file that no longer parses. The 15
// product packages among them have no init() today (git history: never), so the init() rule costs no
// legitimate change.
func LockAdjacentChanges(adjacent []string, baseSrc, headSrc func(string) []byte, baseList, headList PackageFiles, baseTree, headTree TreeFiles) (hard []string, listed []LockChange) {
	nonTest := func(p string) bool { return !strings.HasSuffix(p, "_test.go") }
	headNames := newPkgNames(headSrc, headList)
	for _, d := range adjacent {
		support := strings.HasPrefix(d+"/", "tests/")
		for _, a := range loadTimeAdditions(d, nonTest, baseSrc, headSrc, baseList, headList) {
			switch {
			case a.broken:
				listed = append(listed, LockChange{a.path, "a file of a package a lock package imports no longer parses"})
			default:
				if a.inits > 0 {
					hard = append(hard, fmt.Sprintf("%s: adds %d new init() function(s) to a package that a lock package imports", a.path, a.inits))
				}
				if len(a.vars) > 0 {
					listed = append(listed, LockChange{a.path, "adds a package-level var whose initializer calls a function to a package that a lock package imports: " + strings.Join(a.vars, ", ")})
				}
			}
		}
		for _, p := range cgoAdded(d, baseSrc, headSrc, headList, nonTest) {
			hard = append(hard, p+`: imports "C" (cgo) in a package that a lock package imports`)
		}
		inBase := map[string]bool{}
		baseImports := map[string]bool{}
		for _, p := range baseList(d) {
			if !nonTest(p) {
				continue
			}
			inBase[p] = true
			if imps, ok := importsOf(p, baseSrc(p)); ok {
				for _, im := range imps {
					baseImports[im.path] = true
				}
			}
		}
		for _, p := range headList(d) {
			hs := headSrc(p)
			if !nonTest(p) || hs == nil {
				continue
			}
			if support && !inBase[p] {
				listed = append(listed, LockChange{p, "a new Go file in a test-support package that a lock package imports: it is linked into the lock package's test binary"})
			}
			if inBase[p] && bytes.Equal(baseSrc(p), hs) {
				continue
			}
			imps, ok := importsOf(p, hs)
			if !ok {
				continue
			}
			var fresh []string
			for _, im := range imps {
				if im.path == "C" || stdlibImport(im.path) || baseImports[im.path] {
					continue
				}
				dir := headNames.repoDir(im.path)
				// an import between product packages is routine and its new package, if any, is itself
				// adjacent; the shapes that hide code are a blank/dot import, a module outside the repository,
				// and a test-support package
				if !im.blank && dir != "" && !strings.HasPrefix(dir+"/", "tests/") {
					continue
				}
				kind := "imports"
				if im.blank {
					kind = "blank-imports"
				}
				fresh = append(fresh, kind+" "+strconv.Quote(im.path))
			}
			if len(fresh) > 0 {
				sort.Strings(fresh)
				listed = append(listed, LockChange{p, strings.Join(fresh, ", ") + ", which no file of the package imported at the merge base, in a package that a lock package imports"})
			}
		}
		baseNative := map[string]bool{}
		for _, p := range baseTree(d + "/") {
			baseNative[p] = true
		}
		for _, p := range headTree(d + "/") {
			if filepath.ToSlash(filepath.Dir(p)) != d || !nativeSourceExt[filepath.Ext(p)] {
				continue
			}
			if !baseNative[p] || !bytes.Equal(baseSrc(p), headSrc(p)) {
				hard = append(hard, p+": a native source file (cgo, assembly or an object file) added or changed in a package that a lock package imports")
			}
		}
	}
	sort.Strings(hard)
	sort.Slice(listed, func(i, j int) bool { return listed[i].Path < listed[j].Path })
	return hard, listed
}

// MergeLockChanges unions two lists of listed files by path, joining the reasons.
func MergeLockChanges(lists ...[]LockChange) []LockChange {
	reasons := map[string][]string{}
	var order []string
	for _, l := range lists {
		for _, c := range l {
			if _, ok := reasons[c.Path]; !ok {
				order = append(order, c.Path)
			}
			reasons[c.Path] = append(reasons[c.Path], c.Reason)
		}
	}
	sort.Strings(order)
	out := make([]LockChange, 0, len(order))
	for _, p := range order {
		out = append(out, LockChange{p, strings.Join(reasons[p], "; ")})
	}
	return out
}

// ---------------------------------------------------------------------------------------------------
// min-pass.

const minPassDirective = "min-pass:"

// floorDirectives are the expected-pass floors of the parity allowlist: the combined one and one per
// signal. `min-pass-logs:` does not start with `min-pass:`, so each directive is found by its own name.
var floorDirectives = []string{minPassDirective, "min-pass-logs:", "min-pass-traces:"}

// expectedSkipDirective names a skip the parity ratchet accepts: `# expected-skip: <test path>  # <reason>`.
const expectedSkipDirective = "expected-skip:"

// parseFloor reads one `# <name> N` directive of the parity allowlist the way the Python ratchet
// does (parity_ratchet.parse_allowlist: a comment line, leading '#' characters stripped, then white
// space), except that the value must be ASCII digits only, which the ratchet requires too. present is
// false when there is no directive; err is set for a duplicate or an unreadable value.
func parseFloor(src []byte, name string) (n uint64, present bool, err error) {
	for i, raw := range strings.Split(string(src), "\n") {
		line := strings.TrimSpace(raw)
		if !strings.HasPrefix(line, "#") {
			continue
		}
		directive := strings.TrimSpace(strings.TrimLeft(line, "#"))
		if !strings.HasPrefix(directive, name) {
			continue
		}
		if present {
			return 0, true, fmt.Errorf("line %d: duplicate %s directive", i+1, strings.TrimSuffix(name, ":"))
		}
		value := strings.TrimSpace(strings.TrimPrefix(directive, name))
		ok := value != "" && len(value) <= 15
		for _, r := range value {
			if r < '0' || r > '9' {
				ok = false
			}
		}
		if !ok {
			return 0, true, fmt.Errorf("line %d: unreadable %s value %q", i+1, strings.TrimSuffix(name, ":"), value)
		}
		n, _ = strconv.ParseUint(value, 10, 64)
		present = true
	}
	return n, present, nil
}

// parseMinPass reads the combined `# min-pass: N` directive.
func parseMinPass(src []byte) (n uint64, present bool, err error) {
	return parseFloor(src, minPassDirective)
}

// MinPassProblems is the ratchet of the expected number of passing parity tests: the value of
// `# min-pass: N` in the allowlist, and of the per-signal `# min-pass-logs: N` and
// `# min-pass-traces: N`, may grow, never shrink, and a directive may not disappear or become
// unreadable. A base whose directive is missing or unreadable has nothing to compare against for it.
func MinPassProblems(allowlistPath string, baseSrc, headSrc []byte) []string {
	if headSrc == nil {
		return nil // a missing allowlist is reported elsewhere
	}
	var out []string
	for _, name := range floorDirectives {
		b, bPresent, bErr := parseFloor(baseSrc, name)
		h, hPresent, hErr := parseFloor(headSrc, name)
		switch {
		case hErr != nil:
			out = append(out, fmt.Sprintf("%s: %v", allowlistPath, hErr))
		case bErr != nil || !bPresent:
		case !hPresent:
			out = append(out, fmt.Sprintf("%s: the `# %s %d` directive was removed (that lowers the expected number of passing parity tests to none)", allowlistPath, name, b))
		case h < b:
			out = append(out, fmt.Sprintf("%s: `# %s` lowered from %d to %d; the expected number of passing parity tests may only grow", allowlistPath, name, b, h))
		}
	}
	return out
}

// expectedSkips returns the test paths the allowlist lists as `# expected-skip: <path>  # <reason>`.
func expectedSkips(src []byte) map[string]bool {
	out := map[string]bool{}
	for _, raw := range strings.Split(string(src), "\n") {
		line := strings.TrimSpace(raw)
		if !strings.HasPrefix(line, "#") {
			continue
		}
		directive := strings.TrimSpace(strings.TrimLeft(line, "#"))
		if !strings.HasPrefix(directive, expectedSkipDirective) {
			continue
		}
		if f := strings.Fields(strings.TrimPrefix(directive, expectedSkipDirective)); len(f) > 0 {
			out[f[0]] = true
		}
	}
	return out
}

// ExpectedSkipProblems lists the expected skips a PR adds to the parity allowlist. A skip counts as a
// failure unless the allowlist names it, so each added entry loosens the parity ratchet and needs the
// owner, like an added allowlist entry. Removing one is free.
func ExpectedSkipProblems(allowlistPath string, baseSrc, headSrc []byte) []string {
	if headSrc == nil {
		return nil
	}
	base := expectedSkips(baseSrc)
	var out []string
	for name := range expectedSkips(headSrc) {
		if !base[name] {
			out = append(out, fmt.Sprintf("%s: expected skip added: %s", allowlistPath, name))
		}
	}
	sort.Strings(out)
	return out
}
