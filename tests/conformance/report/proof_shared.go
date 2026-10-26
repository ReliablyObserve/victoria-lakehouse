package report

import (
	"fmt"
	"io/fs"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
)

// This file is the "shared upstream code" half of the proof coverage report:
// the pins, the patches and the imported VictoriaLogs / VictoriaTraces
// packages, each with the tests or gates that name it. The scan reads the
// repository tree; the report is computed from the plain facts it returns
// (SharedCodeInput), so the computation is testable without a tree.

// GoModDep is one upstream module line of a go.mod: the required version and
// the replace target, if any.
type GoModDep struct {
	File    string `json:"file"`
	Module  string `json:"module"`
	Version string `json:"version"`
	Replace string `json:"replace,omitempty"`
}

// PinRef is one pin variable of the Makefile and the files that name it.
type PinRef struct {
	Name   string
	Value  string
	Guards []string
}

// PatchRef is one file under patches/ (a .patch or a .src) with the files that
// name it by path or file name (Guards) and the files that name its directory
// (DirGuards, for gates that take the directory as a default argument).
type PatchRef struct {
	Path      string
	Guards    []string
	DirGuards []string
}

// Import kinds of an ImportUse.
const (
	ImportSrc  = "src"  // a non-test Go file
	ImportTest = "test" // a Go test file
	ImportGate = "gate" // a gate script under scripts/ci
)

// ImportUse is one file that names one upstream package by its quoted import
// path. Module is "logs" for the root module and "traces" for lakehouse-traces.
type ImportUse struct {
	Path   string
	Module string
	File   string
	Kind   string
}

// SharedCodeInput is the raw facts of the shared-code scan.
type SharedCodeInput struct {
	GoMods  []GoModDep
	Pins    []PinRef
	Patches []PatchRef
	Imports []ImportUse
}

// SharedItem is one pin or patch of the shared-code table.
type SharedItem struct {
	Kind      string   `json:"kind"`
	Name      string   `json:"name"`
	Detail    string   `json:"detail,omitempty"`
	Guards    []string `json:"guards"`
	DirGuards []string `json:"dir_guards,omitempty"`
	Guarded   bool     `json:"guarded"`
}

// PackageItem is one imported upstream package.
type PackageItem struct {
	Package     string   `json:"package"`
	SrcLogs     int      `json:"src_files_logs_module"`
	SrcTraces   int      `json:"src_files_traces_module"`
	TestFiles   int      `json:"guard_files"`
	Guards      []string `json:"guards"`
	Guarded     bool     `json:"guarded"`
	UpstreamLib bool     `json:"-"`
}

// SharedCoverage is the computed shared-code section.
type SharedCoverage struct {
	Items     int           `json:"items"`
	Guarded   int           `json:"guarded"`
	Unguarded int           `json:"unguarded"`
	Modules   []GoModDep    `json:"go_mod_upstream_modules"`
	Pins      []SharedItem  `json:"pins"`
	Patches   []SharedItem  `json:"patches"`
	Packages  []PackageItem `json:"imported_packages"`
	// UnguardedItems names every item with no guard, "<kind> <name>".
	UnguardedItems []string `json:"unguarded_items"`
	// LibPackages counts the distinct VictoriaMetrics/lib packages imported
	// (the library module VL and VT build on); they are counted, not listed.
	LibPackages int `json:"victoriametrics_lib_packages_imported"`
}

const upstreamModulePrefix = "github.com/VictoriaMetrics/"

func computeShared(in *SharedCodeInput) SharedCoverage {
	sc := SharedCoverage{
		Modules:        []GoModDep{},
		Pins:           []SharedItem{},
		Patches:        []SharedItem{},
		Packages:       []PackageItem{},
		UnguardedItems: []string{},
	}
	if in == nil {
		return sc
	}
	sc.Modules = append(sc.Modules, in.GoMods...)
	sort.Slice(sc.Modules, func(i, j int) bool {
		if sc.Modules[i].File != sc.Modules[j].File {
			return sc.Modules[i].File < sc.Modules[j].File
		}
		return sc.Modules[i].Module < sc.Modules[j].Module
	})

	add := func(it SharedItem) SharedItem {
		it.Guarded = len(it.Guards) > 0 || len(it.DirGuards) > 0
		sc.Items++
		if it.Guarded {
			sc.Guarded++
		} else {
			sc.Unguarded++
			sc.UnguardedItems = append(sc.UnguardedItems, it.Kind+" "+it.Name)
		}
		return it
	}

	for _, p := range in.Pins {
		it := SharedItem{Kind: "pin", Name: p.Name, Detail: p.Value, Guards: sortedCopy(p.Guards)}
		sc.Pins = append(sc.Pins, add(it))
	}
	sort.Slice(sc.Pins, func(i, j int) bool { return sc.Pins[i].Name < sc.Pins[j].Name })

	for _, p := range in.Patches {
		kind := "patch"
		if strings.HasSuffix(p.Path, ".src") {
			kind = "src"
		}
		it := SharedItem{Kind: kind, Name: p.Path, Guards: sortedCopy(p.Guards), DirGuards: sortedCopy(p.DirGuards)}
		sc.Patches = append(sc.Patches, add(it))
	}
	sort.Slice(sc.Patches, func(i, j int) bool { return sc.Patches[i].Name < sc.Patches[j].Name })

	type pkgAcc struct {
		logsSrc, tracesSrc map[string]bool
		guards             map[string]bool
	}
	pkgs := map[string]*pkgAcc{}
	libs := map[string]bool{}
	for _, u := range in.Imports {
		rel := strings.TrimPrefix(u.Path, upstreamModulePrefix)
		if strings.HasPrefix(rel, "VictoriaMetrics/") {
			if u.Kind == ImportSrc {
				libs[rel] = true
			}
			continue
		}
		a := pkgs[rel]
		if a == nil {
			a = &pkgAcc{logsSrc: map[string]bool{}, tracesSrc: map[string]bool{}, guards: map[string]bool{}}
			pkgs[rel] = a
		}
		switch u.Kind {
		case ImportSrc:
			if u.Module == "traces" {
				a.tracesSrc[u.File] = true
			} else {
				a.logsSrc[u.File] = true
			}
		default:
			a.guards[u.File] = true
		}
	}
	sc.LibPackages = len(libs)
	for rel, a := range pkgs {
		if len(a.logsSrc)+len(a.tracesSrc) == 0 {
			continue // only named by tests: not imported by shipped code
		}
		var g []string
		for f := range a.guards {
			g = append(g, f)
		}
		sort.Strings(g)
		pi := PackageItem{Package: rel, SrcLogs: len(a.logsSrc), SrcTraces: len(a.tracesSrc), TestFiles: len(g), Guards: g, Guarded: len(g) > 0}
		sc.Packages = append(sc.Packages, pi)
		sc.Items++
		if pi.Guarded {
			sc.Guarded++
		} else {
			sc.Unguarded++
			sc.UnguardedItems = append(sc.UnguardedItems, "package "+rel)
		}
	}
	sort.Slice(sc.Packages, func(i, j int) bool { return sc.Packages[i].Package < sc.Packages[j].Package })
	sort.Strings(sc.UnguardedItems)
	return sc
}

func sortedCopy(in []string) []string {
	out := append([]string{}, in...)
	sort.Strings(out)
	return out
}

// shortList renders at most max file names and the count of the rest.
func shortList(files []string, max int) string {
	if len(files) == 0 {
		return "—"
	}
	shown := files
	if len(shown) > max {
		shown = shown[:max]
	}
	parts := make([]string, len(shown))
	for i, f := range shown {
		parts[i] = "`" + f + "`"
	}
	s := strings.Join(parts, ", ")
	if len(files) > max {
		s += fmt.Sprintf(" (+%d)", len(files)-max)
	}
	return s
}

func (pc *ProofCoverage) writeShared(b *strings.Builder) {
	s := pc.Shared
	b.WriteString("## 4. Shared upstream code\n\n")
	fmt.Fprintf(b, "%d items shared with, patched into or imported from VictoriaLogs / VictoriaTraces; %d have a guard, **%d are unguarded**. A guard is a Go test file or a `scripts/ci` gate that names the item; the build also fails on a patch that no longer applies, which is not counted here. Not derivable and so not listed: look-alike re-implementations under `internal/`.\n\n", s.Items, s.Guarded, s.Unguarded)

	if len(s.Modules) > 0 {
		b.WriteString("### Upstream modules (go.mod)\n\n| go.mod | Module | Version | Replaced by |\n|---|---|---|---|\n")
		for _, m := range s.Modules {
			rep := m.Replace
			if rep == "" {
				rep = "—"
			}
			fmt.Fprintf(b, "| `%s` | `%s` | `%s` | `%s` |\n", m.File, strings.TrimPrefix(m.Module, upstreamModulePrefix), m.Version, rep)
		}
		b.WriteString("\n")
	}

	b.WriteString("### Pins (Makefile)\n\n| Pin | Value | Guard |\n|---|---|---|\n")
	for _, p := range s.Pins {
		fmt.Fprintf(b, "| `%s` | `%s` | %s |\n", p.Name, p.Detail, shortList(p.Guards, 2))
	}
	b.WriteString("\n")

	b.WriteString("### Patches and compiled-in sources (`patches/`)\n\n| File | Kind | Named by | Directory named by |\n|---|---|---|---|\n")
	for _, p := range s.Patches {
		fmt.Fprintf(b, "| `%s` | %s | %s | %s |\n", p.Name, p.Kind, shortList(p.Guards, 2), shortList(p.DirGuards, 2))
	}
	b.WriteString("\n")

	fmt.Fprintf(b, "### Imported upstream packages\n\n%d VictoriaLogs / VictoriaTraces packages are imported by shipped code (plus %d `VictoriaMetrics/lib` packages, not listed). *Logs* and *traces* count the non-test Go files of each module that import the package; *Guard files* count the Go tests and gates that name its quoted import path.\n\n", len(s.Packages), s.LibPackages)
	b.WriteString("| Package | Logs | Traces | Guard files | Guards |\n|---|--:|--:|--:|---|\n")
	for _, p := range s.Packages {
		fmt.Fprintf(b, "| `%s` | %d | %d | %d | %s |\n", p.Package, p.SrcLogs, p.SrcTraces, p.TestFiles, shortList(p.Guards, 2))
	}
	b.WriteString("\n")

	b.WriteString("### Unguarded\n\n")
	if len(s.UnguardedItems) == 0 {
		b.WriteString("None: every item above is named by at least one test or gate.\n\n")
		return
	}
	for _, u := range s.UnguardedItems {
		fmt.Fprintf(b, "- %s\n", u)
	}
	b.WriteString("\n")
}

// --- the scan ---------------------------------------------------------------

var (
	upstreamImportRe = regexp.MustCompile(`"(github\.com/VictoriaMetrics/(?:VictoriaLogs|VictoriaTraces|VictoriaMetrics)/[^"\s]+)"`)
	goModRequireRe   = regexp.MustCompile(`(?m)^\s*(?:require\s+)?(github\.com/VictoriaMetrics/(?:VictoriaLogs|VictoriaTraces|VictoriaMetrics))\s+(v\S+)`)
	goModReplaceRe   = regexp.MustCompile(`(?m)^\s*replace\s+(github\.com/VictoriaMetrics/(?:VictoriaLogs|VictoriaTraces|VictoriaMetrics))(?:\s+v\S+)?\s+=>\s+(\S+)`)
)

// pinNames are the Makefile variables that pin the upstream versions.
var pinNames = []string{"VL_VERSION_LOGS", "VL_COMMIT_TRACES", "VT_VERSION"}

// skippedDirs are directory names the scan never enters: vendored upstream
// trees, test data, and anything hidden (.git, .claude worktrees, .github).
func skipDir(name string) bool {
	return strings.HasPrefix(name, ".") || name == "deps" || name == "vendor" || name == "node_modules" || name == "testdata"
}

// selfFixturePrefix is the generator's own test files: they spell upstream
// import paths, patch names and pin names as fixtures, which would make every
// item look guarded by the very code that reports on it.
const selfFixturePrefix = "tests/conformance/report/proof_"

type guardFile struct {
	rel     string
	content string
}

// ScanSharedCode reads the repository at root and returns the facts of the
// shared-code section. It is deterministic: every list is sorted, nothing
// depends on directory order or the clock.
func ScanSharedCode(root string) (*SharedCodeInput, error) {
	in := &SharedCodeInput{}
	var guards []guardFile
	var patchFiles []string

	err := filepath.WalkDir(root, func(p string, d fs.DirEntry, werr error) error {
		if werr != nil {
			return werr
		}
		rel, err := filepath.Rel(root, p)
		if err != nil {
			return err
		}
		rel = filepath.ToSlash(rel)
		if d.IsDir() {
			if rel != "." && skipDir(d.Name()) {
				return filepath.SkipDir
			}
			return nil
		}
		switch {
		case strings.HasSuffix(rel, ".go"):
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			kind := ImportSrc
			if strings.HasSuffix(rel, "_test.go") {
				kind = ImportTest
				if !strings.HasPrefix(rel, selfFixturePrefix) {
					guards = append(guards, guardFile{rel, string(data)})
				}
			}
			mod := "logs"
			if strings.HasPrefix(rel, "lakehouse-traces/") {
				mod = "traces"
			}
			if kind == ImportTest && strings.HasPrefix(rel, selfFixturePrefix) {
				return nil
			}
			for _, imp := range uniqueMatches(upstreamImportRe, string(data)) {
				in.Imports = append(in.Imports, ImportUse{Path: imp, Module: mod, File: rel, Kind: kind})
			}
		case path.Dir(rel) == "scripts/ci" && (strings.HasSuffix(rel, ".sh") || strings.HasSuffix(rel, ".py")):
			data, err := os.ReadFile(p)
			if err != nil {
				return err
			}
			guards = append(guards, guardFile{rel, string(data)})
			for _, imp := range uniqueMatches(upstreamImportRe, string(data)) {
				in.Imports = append(in.Imports, ImportUse{Path: imp, Module: "logs", File: rel, Kind: ImportGate})
			}
		case strings.HasPrefix(rel, "patches/") && (strings.HasSuffix(rel, ".patch") || strings.HasSuffix(rel, ".src")):
			patchFiles = append(patchFiles, rel)
		}
		return nil
	})
	if err != nil {
		return nil, err
	}
	sort.Slice(guards, func(i, j int) bool { return guards[i].rel < guards[j].rel })

	named := func(token string) []string {
		var out []string
		for _, g := range guards {
			if strings.Contains(g.content, token) {
				out = append(out, g.rel)
			}
		}
		return out
	}

	sort.Strings(patchFiles)
	for _, pf := range patchFiles {
		ref := PatchRef{Path: pf}
		seen := map[string]bool{}
		for _, tok := range []string{pf, path.Base(pf)} {
			for _, g := range named(tok) {
				if !seen[g] {
					seen[g] = true
					ref.Guards = append(ref.Guards, g)
				}
			}
		}
		sort.Strings(ref.Guards)
		// A file that already names the patch itself is not listed again as a
		// directory-level guard.
		for _, g := range named(path.Dir(pf)) {
			if !seen[g] {
				ref.DirGuards = append(ref.DirGuards, g)
			}
		}
		in.Patches = append(in.Patches, ref)
	}

	if mk, err := os.ReadFile(filepath.Join(root, "Makefile")); err == nil {
		for _, name := range pinNames {
			re := regexp.MustCompile(`(?m)^` + regexp.QuoteMeta(name) + `\s*:=\s*(\S+)`)
			if m := re.FindSubmatch(mk); m != nil {
				in.Pins = append(in.Pins, PinRef{Name: name, Value: string(m[1]), Guards: named(name)})
			}
		}
	}

	for _, gm := range []string{"go.mod", "lakehouse-traces/go.mod"} {
		data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(gm)))
		if err != nil {
			if os.IsNotExist(err) {
				continue
			}
			return nil, err
		}
		in.GoMods = append(in.GoMods, parseGoModUpstream(gm, string(data))...)
	}

	sort.Slice(in.Imports, func(i, j int) bool {
		a, b := in.Imports[i], in.Imports[j]
		if a.Path != b.Path {
			return a.Path < b.Path
		}
		if a.File != b.File {
			return a.File < b.File
		}
		return a.Kind < b.Kind
	})
	return in, nil
}

func uniqueMatches(re *regexp.Regexp, s string) []string {
	seen := map[string]bool{}
	var out []string
	for _, m := range re.FindAllStringSubmatch(s, -1) {
		if !seen[m[1]] {
			seen[m[1]] = true
			out = append(out, m[1])
		}
	}
	sort.Strings(out)
	return out
}

// parseGoModUpstream extracts the VictoriaLogs / VictoriaTraces /
// VictoriaMetrics requires and replaces of one go.mod, sorted by module.
func parseGoModUpstream(file, content string) []GoModDep {
	deps := map[string]*GoModDep{}
	for _, m := range goModRequireRe.FindAllStringSubmatch(content, -1) {
		deps[m[1]] = &GoModDep{File: file, Module: m[1], Version: m[2]}
	}
	for _, m := range goModReplaceRe.FindAllStringSubmatch(content, -1) {
		if d := deps[m[1]]; d != nil {
			d.Replace = m[2]
		} else {
			deps[m[1]] = &GoModDep{File: file, Module: m[1], Replace: m[2]}
		}
	}
	out := make([]GoModDep, 0, len(deps))
	for _, d := range deps {
		out = append(out, *d)
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Module < out[j].Module })
	return out
}
