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
	".f": true, ".f90": true, ".for": true, ".swig": true, ".swigcxx": true,
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
	out := make([]string, 0, len(order))
	for _, f := range order {
		out = append(out, fmt.Sprintf("%s: %s", f, strings.Join(reasons[f], "; ")))
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
	dirs := map[string]bool{}
	for f := range baseRefs {
		dirs[filepath.ToSlash(filepath.Dir(f))] = true
	}
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
	for d := range dirs {
		for _, p := range headList(d) {
			hs := headSrc(p)
			if hs == nil || bytes.Equal(baseSrc(p), hs) || !hasC(p, hs) || hasC(p, baseSrc(p)) {
				continue
			}
			out = append(out, p+`: imports "C" (cgo) in a package that holds a registry lock`)
		}
	}
	sort.Strings(out)
	return out
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

// loadTimeDecls returns what a Go file declares that runs code at package load, before any test:
// the multiset of init() functions (keyed by the whitespace-normalised source of the body) and the
// multiset of package-level vars whose initializer calls a function (keyed `name = initializer
// source`). A literal table (`var cases = []T{{...}}`), a conversion or a var without an
// initializer runs nothing and is not counted. TestMain is checked by TestMainChanges.
func loadTimeDecls(path string, src []byte) (inits, vars map[string]int, err error) {
	inits, vars = map[string]int{}, map[string]int{}
	if src == nil {
		return inits, vars, nil
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, src, parser.SkipObjectResolution)
	if err != nil {
		return nil, nil, err
	}
	imports := map[string]string{} // local name -> import path
	for _, im := range f.Imports {
		path := strings.Trim(im.Path.Value, `"`+"`")
		name := path[strings.LastIndex(path, "/")+1:]
		if im.Name != nil {
			name = im.Name.Name
		}
		imports[name] = path
	}
	for _, d := range f.Decls {
		switch d := d.(type) {
		case *ast.FuncDecl:
			if d.Recv == nil && d.Name.Name == "init" {
				body := ""
				if d.Body != nil {
					body = string(src[fset.Position(d.Body.Pos()).Offset:fset.Position(d.Body.End()).Offset])
				}
				inits["init() "+strings.Join(strings.Fields(body), " ")]++
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
					text := string(src[fset.Position(v.Pos()).Offset:fset.Position(v.End()).Offset])
					vars[name+" = "+strings.Join(strings.Fields(text), " ")]++
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
		// the package's load-time declarations at the merge base, and the base file's own (for attribution)
		type decls struct{ inits, vars map[string]int }
		baseFile := map[string]decls{}
		basePkgInits, basePkgVars := map[string]int{}, map[string]int{}
		for _, p := range baseList(d) {
			if !inScope(p) {
				continue
			}
			bi, bv, err := loadTimeDecls(p, baseSrc(p))
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
			inits, vars  map[string]int
			changedBytes bool
		}
		var heads []headDecl
		headPkgInits, headPkgVars := map[string]int{}, map[string]int{}
		for _, p := range headList(d) {
			if !inScope(p) {
				continue
			}
			hs := headSrc(p)
			if hs == nil {
				continue
			}
			hi, hv, err := loadTimeDecls(p, hs)
			if err != nil {
				if !bytes.Equal(baseSrc(p), hs) {
					out = append(out, p+": does not parse, so the locks of its package would not run")
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
			var added []string
			n := 0
			for k, c := range h.inits {
				if gain := c - bf.inits[k]; gain > 0 && headPkgInits[k] > basePkgInits[k] {
					n += min(gain, headPkgInits[k]-basePkgInits[k])
				}
			}
			if n > 0 {
				added = append(added, fmt.Sprintf("%d new init() function(s)", n))
			}
			var names []string
			for k, c := range h.vars {
				if c > bf.vars[k] && headPkgVars[k] > basePkgVars[k] {
					names = append(names, k)
				}
			}
			sort.Strings(names)
			if len(names) > 0 {
				added = append(added, "package-level var whose initializer calls a function: "+strings.Join(names, ", "))
			}
			if len(added) > 0 {
				out = append(out, fmt.Sprintf("%s: adds %s to a package that holds a registry lock", h.path, strings.Join(added, " and ")))
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
		// a bump moves forward: the Go version and the alpine release may not go back
		if versionBefore(hm[2], bm[2]) || versionBefore(strings.TrimPrefix(hm[3], "-alpine"), strings.TrimPrefix(bm[3], "-alpine")) {
			return false
		}
		changed = true
	}
	return changed
}

// versionBefore reports whether dotted version a is numerically older than b ("3.9" is older than
// "3.23"; "1.27" is older than "1.27.1"). Both are digits and dots, as the line regexp guarantees; an
// empty string (no alpine release) is never older than anything.
func versionBefore(a, b string) bool {
	if a == "" || b == "" {
		return false
	}
	as, bs := strings.Split(a, "."), strings.Split(b, ".")
	for i := 0; i < len(as) || i < len(bs); i++ {
		var x, y int
		if i < len(as) {
			x, _ = strconv.Atoi(as[i])
		}
		if i < len(bs) {
			y, _ = strconv.Atoi(bs[i])
		}
		if x != y {
			return x < y
		}
	}
	return false
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
