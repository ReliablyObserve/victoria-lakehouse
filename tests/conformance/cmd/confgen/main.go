// confgen regenerates the documents derived from the vendored upstream
// sources, the conformance registry and the feature catalog:
// tests/conformance/inventory.generated.yaml, UPSTREAM_COVERAGE.md,
// docs/features.md, and README.md's generated "Key Features" block.
//
//	confgen -write   # regenerate every file
//	confgen -check   # exit 1 when any file is out of date, or when the
//	                 # feature-catalog gate fails
//
// Passing both flags writes first, then checks the freshly written files
// (exit 0 when that second pass is clean).
//
// "Out of date" is byte inequality, with one exception: a release reference in
// docs/features.md that an older changelog state rendered and that is still
// true (see report.Document) is current for -check, so the release workflow's
// changelog-only PR stays green. -write always writes the exact rendering.
package main

import (
	"bytes"
	"flag"
	"fmt"
	"io"
	"os"
	"path/filepath"

	conformance "github.com/ReliablyObserve/victoria-lakehouse/tests/conformance"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/report"
)

type genFile struct {
	path string
	want []byte
	// accepts, when set, decides whether the bytes on disk are current even
	// though they differ from want (see report.Document.Accepts); nil means
	// only want itself is current.
	accepts func(have []byte) bool
}

// fileState is how a generated file on disk compares with its plan.
type fileState int

const (
	fileExact    fileState = iota // identical to the planned bytes
	fileAccepted                  // differs, but only in release references that are still true
	fileStale                     // out of date: regenerate
)

// state compares have (the bytes on disk) with the plan for f.
func (f genFile) state(have []byte) fileState {
	switch {
	case bytes.Equal(have, f.want):
		return fileExact
	case f.accepts != nil && f.accepts(have):
		return fileAccepted
	default:
		return fileStale
	}
}

// plan is everything a run needs: the documents to write or compare, and the
// feature-catalog verdict. It is built in one place so `-write` and `-check`
// can never disagree about what "current" means, and so the whole derivation
// is testable without running the command.
type plan struct {
	files           []genFile
	featureCount    int
	featureFailures []string
	featureSummary  string
}

// buildPlan derives every generated document and the feature-gate verdict for
// the repository at root.
func buildPlan(root string) (*plan, error) {
	dirs := inventory.DefaultDirs(root)
	if dirs.VLVersion == "" || dirs.VTVersion == "" {
		return nil, fmt.Errorf("VLVersion/VTVersion not read from Makefile (VL_VERSION_LOGS=%q VT_VERSION=%q) — an empty pin is never a valid basis to write or check the generated inventory", dirs.VLVersion, dirs.VTVersion)
	}
	inv, err := inventory.Extract(dirs)
	if err != nil {
		return nil, fmt.Errorf("extract (run make deps-logs deps-traces deps-vt first): %w", err)
	}
	reg, err := registry.LoadDir(filepath.Join(root, "tests", "conformance", "registry", "rows"))
	if err != nil {
		return nil, err
	}
	features, err := registry.LoadFeatures(filepath.Join(root, "tests", "conformance", "registry", "features"), root)
	if err != nil {
		return nil, err
	}
	changelog, err := registry.ParseChangelog(filepath.Join(root, "CHANGELOG.md"))
	if err != nil {
		return nil, err
	}

	wantInv, err := inv.Bytes()
	if err != nil {
		return nil, err
	}

	readmePath := filepath.Join(root, "README.md")
	readme, err := os.ReadFile(readmePath)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", readmePath, err)
	}
	wantReadme, err := report.ReplaceMarkedBlock(string(readme), report.ReadmeFeaturesBegin, report.ReadmeFeaturesEnd, report.RenderReadmeFeatures(features))
	if err != nil {
		return nil, fmt.Errorf("README.md feature block: %w", err)
	}

	fd := conformance.CheckFeatures(features, reg, changelog)
	featuresDoc := report.RenderFeatures(features, reg, changelog, report.FeaturesDocDir)
	return &plan{
		files: []genFile{
			{path: filepath.Join(root, "tests", "conformance", "inventory.generated.yaml"), want: wantInv},
			{path: filepath.Join(root, "UPSTREAM_COVERAGE.md"), want: []byte(report.RenderCoverage(inv, reg))},
			{path: filepath.Join(root, report.FeaturesDocDir, "features.md"), want: []byte(featuresDoc.String()), accepts: featuresDoc.Accepts},
			{path: readmePath, want: []byte(wantReadme)},
		},
		featureCount:    len(features.Features),
		featureFailures: fd.HardFailures(),
		featureSummary:  fd.Summary(),
	}, nil
}

func main() {
	write := flag.Bool("write", false, "regenerate files")
	check := flag.Bool("check", false, "fail when files are stale")
	root := flag.String("root", "", "repo root (default: auto-detect)")
	flag.Parse()

	if !*write && !*check {
		fmt.Fprintln(os.Stderr, "usage: confgen -write | -check [-root <repo>]")
		flag.PrintDefaults()
		os.Exit(2)
	}

	if *root == "" {
		r, err := inventory.RepoRoot()
		if err != nil {
			fatal(err)
		}
		*root = r
	}
	p, err := buildPlan(*root)
	if err != nil {
		fatal(err)
	}
	files := p.files

	if *write {
		wroteAny := false
		for _, f := range files {
			have, err := readExisting(f.path)
			if err != nil {
				fatal(fmt.Errorf("read %s: %w", f.path, err))
			}
			if bytes.Equal(have, f.want) {
				continue
			}
			if err := os.WriteFile(f.path, f.want, 0o644); err != nil {
				fatal(err)
			}
			fmt.Println("wrote", f.path)
			wroteAny = true
		}
		if !wroteAny {
			fmt.Println("up to date")
		}
	}

	if *check {
		stale, err := checkFiles(os.Stdout, files)
		if err != nil {
			fatal(err)
		}

		// The feature-catalog gate runs here as well as in the conformance
		// tests, so `confgen -check` alone is enough to tell a PR author
		// that a feature is missing, unverified or undocumented.
		for _, msg := range p.featureFailures {
			fmt.Println("feature catalog:", msg)
		}
		fmt.Printf("feature catalog: %d features, %s\n", p.featureCount, p.featureSummary)

		if stale {
			fmt.Println("generated files are stale — run: make conformance-gen")
		}
		if stale || len(p.featureFailures) > 0 {
			os.Exit(1)
		}
	}
}

// checkFiles compares every planned file with the one on disk, reporting each
// out-of-date file as "stale:" and each file that is current only through
// still-true release references as "current:", and returns whether any file
// is stale.
func checkFiles(w io.Writer, files []genFile) (bool, error) {
	stale := false
	for _, f := range files {
		have, err := readExisting(f.path)
		if err != nil {
			return false, fmt.Errorf("read %s: %w", f.path, err)
		}
		switch f.state(have) {
		case fileStale:
			stale = true
			fmt.Fprintln(w, "stale:", f.path)
		case fileAccepted:
			fmt.Fprintln(w, "current:", f.path, "— its release references predate the newest CHANGELOG.md release but are still true; 'make conformance-gen' renders the exact versions")
		}
	}
	return stale, nil
}

// readExisting reads path, treating a missing file as legitimately empty (so
// it still compares unequal to any non-empty want, and so is reported as
// stale — the existing, correct behavior for a file that has never been
// generated) while surfacing every other error (permission denied, EISDIR,
// ...) to the caller instead of silently discarding it: a permission error
// must never be misreported as "stale", since -write would then try to
// overwrite a file it could not even read, and -check would print a
// misleading diagnosis.
func readExisting(path string) ([]byte, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		if os.IsNotExist(err) {
			return nil, nil
		}
		return nil, err
	}
	return data, nil
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "confgen:", err)
	os.Exit(2)
}
