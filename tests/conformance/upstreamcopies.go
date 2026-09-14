package conformance

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// The files under patches/*/ ending in .src are copied verbatim into the
// cloned upstream tree by the Makefile's deps-* targets. They compile as part
// of VictoriaLogs / VictoriaTraces, inside upstream's own packages, which is
// what lets them reach unexported machinery (runPipes, initJoinMaps, q.pipes,
// q.f, the pipe* types).
//
// That same property makes them the one place in this repository where
// upstream code can be *duplicated* instead of *reused* without anything
// noticing. A .src file may contain:
//
//   - a function derived from an upstream function (RunQueryExternal is
//     (*Storage).runQuery with the local search swapped out),
//   - a function that stands in for an upstream one and deliberately covers
//     less of it (GetQueryPipeFields vs getNeededColumns — parity divergence
//     B4),
//   - a switch over a set of upstream types that upstream may grow
//     (QueryNeedsAllFields).
//
// None of these break the build when upstream changes: the copy keeps
// compiling against the new tree while quietly no longer matching it. The
// patches under patches/*.patch cannot drift like this — `git apply` fails
// loudly when their context moves — so the .src files were the blind spot.
//
// The guard below closes it. Every top-level function in every .src file must
// carry one of three markers in its doc comment:
//
//	// upstream-copy: <file inside the upstream tree> <func>
//	// upstream-divergence: <why this is not a plain call into upstream>
//
//	// upstream-typeset: <kind>
//	// upstream-divergence: <what the enumeration is for>
//
//	// upstream-original: <why this has no upstream counterpart>
//
// For every marked copy and typeset a reviewed baseline is committed under
// patches/upstream-copies/<patch dir>/, holding the upstream text exactly as
// it read at that tree's pin. The test re-extracts it from the vendored tree
// and fails on any difference, naming the pin and printing the diff — so an
// upstream bump that changes a copied function's behaviour stops the bump
// instead of shipping inside it.
//
// The pins live in the baselines, not in the markers, on purpose: the
// patches/vl-logs/ and patches/vl-traces/ trees are byte-equal by
// scripts/ci/check_patches_equal.sh (the two VictoriaLogs pins differ, so the
// same .src compiles against both), and an inline `@ v1.52.0` would make that
// gate unsatisfiable. Each patch directory gets its own baseline directory
// instead, and both are checked.

// SrcPatchDirs maps each patch directory that holds .src files to the
// vendored tree those files are copied into.
var SrcPatchDirs = []SrcPatchDir{
	{Name: "vl-logs", Tree: "deps/VictoriaLogs", PinVar: "VL_VERSION_LOGS"},
	{Name: "vl-traces", Tree: "lakehouse-traces/deps/VictoriaLogs", PinVar: "VL_COMMIT_TRACES"},
	{Name: "vt-traces", Tree: "lakehouse-traces/deps/VictoriaTraces", PinVar: "VT_VERSION"},
}

// SrcPatchDir is one patches/<name>/ directory and the vendored upstream tree
// its .src files are copied into.
type SrcPatchDir struct {
	Name   string
	Tree   string
	PinVar string
}

// Marker kinds. Every top-level func in a .src file carries exactly one.
const (
	MarkerCopy     = "upstream-copy"
	MarkerTypeSet  = "upstream-typeset"
	MarkerOriginal = "upstream-original"
)

// MarkerDivergence is the mandatory companion of upstream-copy and
// upstream-typeset: the reasoned note that says why the copy exists and what
// it does not carry over. A copy without one is a copy nobody reviewed.
const MarkerDivergence = "upstream-divergence"

// SrcFunc is one top-level function declaration in a .src file together with
// the marker its doc comment carries.
type SrcFunc struct {
	// File is the .src file's base name, e.g. "external_query.go.src".
	File string
	// Name is the function name. Methods are not used in .src files today;
	// a method would be recorded as "(recv).Name".
	Name string
	// Marker is one of MarkerCopy / MarkerTypeSet / MarkerOriginal, or "" when
	// the doc comment carries none.
	Marker string
	// Arg is the marker's argument: for MarkerCopy "<upstream file> <func>",
	// for MarkerTypeSet the typeset kind, for MarkerOriginal the reason.
	Arg string
	// Divergence is the upstream-divergence note, joined into one line.
	Divergence string
	// Line is the func declaration's line in the .src file, for messages.
	Line int
}

// UpstreamFile returns the upstream-relative path an upstream-copy marker
// names, and the function spec inside it.
func (f SrcFunc) UpstreamFile() (file, fn string, err error) {
	if f.Marker != MarkerCopy {
		return "", "", fmt.Errorf("%s: %s is not an %s", f.File, f.Name, MarkerCopy)
	}
	fields := strings.Fields(f.Arg)
	if len(fields) != 2 {
		return "", "", fmt.Errorf("%s: %s: %s wants `<upstream file> <func>`, got %q", f.File, f.Name, MarkerCopy, f.Arg)
	}
	return fields[0], fields[1], nil
}

var markerRe = regexp.MustCompile(`^(upstream-copy|upstream-typeset|upstream-original|upstream-divergence):\s*(.*)$`)

// ParseSrcFuncs parses one .src file and returns its top-level functions with
// the markers their doc comments carry. The file is Go source with a .src
// extension, so go/parser reads it unchanged.
func ParseSrcFuncs(path string) ([]SrcFunc, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, data, parser.ParseComments)
	if err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	base := filepath.Base(path)

	var out []SrcFunc
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		sf := SrcFunc{File: base, Name: funcDeclName(fd), Line: fset.Position(fd.Pos()).Line}
		var divergence []string
		if fd.Doc != nil {
			for _, c := range fd.Doc.List {
				text := strings.TrimSpace(strings.TrimPrefix(c.Text, "//"))
				m := markerRe.FindStringSubmatch(text)
				if m == nil {
					// A continuation line of a multi-line divergence note.
					if len(divergence) > 0 && text != "" {
						divergence = append(divergence, text)
					}
					continue
				}
				if m[1] == MarkerDivergence {
					divergence = append(divergence, strings.TrimSpace(m[2]))
					continue
				}
				if sf.Marker != "" {
					return nil, fmt.Errorf("%s: %s carries both %s and %s", base, sf.Name, sf.Marker, m[1])
				}
				sf.Marker = m[1]
				sf.Arg = strings.TrimSpace(m[2])
				divergence = nil
			}
		}
		sf.Divergence = strings.TrimSpace(strings.Join(divergence, " "))
		out = append(out, sf)
	}
	return out, nil
}

func funcDeclName(fd *ast.FuncDecl) string {
	if fd.Recv == nil || len(fd.Recv.List) == 0 {
		return fd.Name.Name
	}
	return "(" + exprString(fd.Recv.List[0].Type) + ")." + fd.Name.Name
}

func exprString(e ast.Expr) string {
	switch t := e.(type) {
	case *ast.Ident:
		return t.Name
	case *ast.StarExpr:
		return "*" + exprString(t.X)
	default:
		return fmt.Sprintf("%T", e)
	}
}

// SrcFilesIn lists the .src files in patches/<dir>/, sorted.
func SrcFilesIn(repoRoot, patchDir string) ([]string, error) {
	glob := filepath.Join(repoRoot, "patches", patchDir, "*.src")
	paths, err := filepath.Glob(glob)
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

// ExtractUpstreamFunc returns the verbatim source text of funcSpec as it is
// declared in file relFile of the vendored tree at treeDir. funcSpec is either
// a plain name ("getNeededColumns") or a method ("(*Storage).runQuery").
//
// The doc comment is excluded: a reworded comment is not a behaviour change,
// and a guard that fires on prose trains reviewers to re-record without
// reading. Everything from `func` to the closing brace is compared.
func ExtractUpstreamFunc(treeDir, relFile, funcSpec string) (string, error) {
	path := filepath.Join(treeDir, filepath.FromSlash(relFile))
	data, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, path, data, parser.SkipObjectResolution)
	if err != nil {
		return "", fmt.Errorf("parse %s: %w", path, err)
	}
	for _, decl := range f.Decls {
		fd, ok := decl.(*ast.FuncDecl)
		if !ok || funcDeclName(fd) != funcSpec {
			continue
		}
		start := fset.Position(fd.Pos()).Offset
		end := fset.Position(fd.End()).Offset
		return strings.ReplaceAll(string(data[start:end]), "\r\n", "\n"), nil
	}
	return "", fmt.Errorf("%s: no func %s in %s", treeDir, funcSpec, relFile)
}

// pipeUpdateNeededFieldsRe matches the per-pipe implementations of the `pipe`
// interface method that declares which input fields a pipe reads.
var pipeUpdateNeededFieldsRe = regexp.MustCompile(`(?m)^func \(\w+ \*(pipe\w+)\) updateNeededFields\(`)

// ExtractTypeSet computes one of the upstream type sets a .src file switches
// over. The only kind today is "pipes-implementing-updateNeededFields": every
// pipe type in lib/logstorage that declares which fields it needs, which is
// exactly the set QueryNeedsAllFields has to classify.
//
// Upstream grows this set on most releases (v1.52.0 added json_array_concat).
// Growth is not a build error and not a query error — the new pipe simply
// falls through QueryNeedsAllFields' switch into the "project a subset"
// branch, which is right for most pipes and wrong for any that enumerate
// fields. Baselining the set turns "upstream added a pipe" into a review
// question instead of a silent default.
func ExtractTypeSet(treeDir, kind string) ([]string, error) {
	if kind != "pipes-implementing-updateNeededFields" {
		return nil, fmt.Errorf("unknown %s kind %q", MarkerTypeSet, kind)
	}
	dir := filepath.Join(treeDir, "lib", "logstorage")
	entries, err := os.ReadDir(dir)
	if err != nil {
		return nil, err
	}
	seen := map[string]bool{}
	for _, e := range entries {
		name := e.Name()
		if e.IsDir() || !strings.HasSuffix(name, ".go") || strings.HasSuffix(name, "_test.go") {
			continue
		}
		data, err := os.ReadFile(filepath.Join(dir, name))
		if err != nil {
			return nil, err
		}
		for _, m := range pipeUpdateNeededFieldsRe.FindAllSubmatch(data, -1) {
			seen[string(m[1])] = true
		}
	}
	if len(seen) == 0 {
		return nil, fmt.Errorf("%s: no pipe types found under lib/logstorage", treeDir)
	}
	out := make([]string, 0, len(seen))
	for k := range seen {
		out = append(out, k)
	}
	sort.Strings(out)
	return out, nil
}

// BaselinePath is where the reviewed upstream text for one marked function in
// one patch directory is committed.
func BaselinePath(repoRoot, patchDir, srcFile, funcName string) string {
	stem := strings.TrimSuffix(srcFile, ".src") // external_query.go
	stem = strings.TrimSuffix(stem, ".go")      // external_query
	return filepath.Join(repoRoot, "patches", "upstream-copies", patchDir, stem+"."+funcName+".txt")
}

// Baseline is a committed snapshot of upstream text at one pin.
type Baseline struct {
	Upstream string // "lib/logstorage/storage_search.go (*Storage).runQuery", or a typeset kind
	Tree     string // "deps/VictoriaLogs"
	Pin      string // "v1.52.0"
	Body     string // the upstream text, verbatim
}

// FormatBaseline renders a baseline file: three `#` header lines naming what
// was recorded and at which pin, a blank line, then the upstream text.
func FormatBaseline(b Baseline) string {
	return fmt.Sprintf("# upstream: %s\n# tree: %s\n# pin: %s\n\n%s\n", b.Upstream, b.Tree, b.Pin, strings.TrimRight(b.Body, "\n"))
}

// ParseBaseline reads a baseline file back.
func ParseBaseline(path string) (Baseline, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Baseline{}, err
	}
	var b Baseline
	lines := strings.Split(strings.ReplaceAll(string(data), "\r\n", "\n"), "\n")
	i := 0
	for ; i < len(lines) && strings.HasPrefix(lines[i], "#"); i++ {
		field := strings.TrimSpace(strings.TrimPrefix(lines[i], "#"))
		key, value, ok := strings.Cut(field, ":")
		if !ok {
			return Baseline{}, fmt.Errorf("%s: header line %q is not `# key: value`", path, lines[i])
		}
		switch strings.TrimSpace(key) {
		case "upstream":
			b.Upstream = strings.TrimSpace(value)
		case "tree":
			b.Tree = strings.TrimSpace(value)
		case "pin":
			b.Pin = strings.TrimSpace(value)
		default:
			return Baseline{}, fmt.Errorf("%s: unknown header key %q", path, key)
		}
	}
	if b.Upstream == "" || b.Tree == "" || b.Pin == "" {
		return Baseline{}, fmt.Errorf("%s: header must set upstream, tree and pin", path)
	}
	if i < len(lines) && lines[i] == "" {
		i++
	}
	b.Body = strings.TrimRight(strings.Join(lines[i:], "\n"), "\n")
	return b, nil
}

// LineDiff renders the first differing region of two texts, with up to three
// lines of context, so a failure names what upstream changed instead of
// printing two whole functions.
func LineDiff(want, got string) string {
	w := strings.Split(want, "\n")
	g := strings.Split(got, "\n")
	n := len(w)
	if len(g) < n {
		n = len(g)
	}
	first := -1
	for i := 0; i < n; i++ {
		if w[i] != g[i] {
			first = i
			break
		}
	}
	if first < 0 {
		if len(w) == len(g) {
			return "(texts are equal)"
		}
		first = n
	}
	lo := first - 3
	if lo < 0 {
		lo = 0
	}
	var sb strings.Builder
	fmt.Fprintf(&sb, "first difference at line %d\n", first+1)
	for i := lo; i < first; i++ {
		fmt.Fprintf(&sb, "  %s\n", w[i])
	}
	for i := first; i < first+4; i++ {
		if i < len(w) {
			fmt.Fprintf(&sb, "- baseline: %s\n", w[i])
		}
		if i < len(g) {
			fmt.Fprintf(&sb, "+ upstream: %s\n", g[i])
		}
	}
	return sb.String()
}
