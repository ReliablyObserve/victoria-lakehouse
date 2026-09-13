// confgen regenerates the documents derived from the vendored upstream
// sources, the conformance registry and the feature catalog:
// tests/conformance/inventory.generated.yaml, UPSTREAM_COVERAGE.md,
// docs/features.md, and README.md's generated "Key Features" block.
//
//	confgen -write   # regenerate every file
//	confgen -check   # exit 1 when a regeneration would change any file,
//	                 # or when the feature-catalog gate fails
//
// Passing both flags writes first, then checks the freshly written files
// (exit 0 when that second pass is clean).
package main

import (
	"bytes"
	"flag"
	"fmt"
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
	bullets, err := registry.ParseChangelogAdded(filepath.Join(root, "CHANGELOG.md"))
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

	fd := conformance.CheckFeatures(features, reg, bullets)
	return &plan{
		files: []genFile{
			{filepath.Join(root, "tests", "conformance", "inventory.generated.yaml"), wantInv},
			{filepath.Join(root, "UPSTREAM_COVERAGE.md"), []byte(report.RenderCoverage(inv, reg))},
			{filepath.Join(root, report.FeaturesDocDir, "features.md"), []byte(report.RenderFeatures(features, reg, report.FeaturesDocDir))},
			{readmePath, []byte(wantReadme)},
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
		stale := false
		for _, f := range files {
			have, err := readExisting(f.path)
			if err != nil {
				fatal(fmt.Errorf("read %s: %w", f.path, err))
			}
			if !bytes.Equal(have, f.want) {
				stale = true
				fmt.Println("stale:", f.path)
			}
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
