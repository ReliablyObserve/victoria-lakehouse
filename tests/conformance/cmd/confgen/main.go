// confgen regenerates tests/conformance/inventory.generated.yaml and
// UPSTREAM_COVERAGE.md from the vendored upstream sources and the registry.
//
//	confgen -write   # regenerate both files
//	confgen -check   # exit 1 when a regeneration would change either file
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

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/report"
)

type genFile struct {
	path string
	want []byte
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
	dirs := inventory.DefaultDirs(*root)
	if dirs.VLVersion == "" || dirs.VTVersion == "" {
		fatal(fmt.Errorf("VLVersion/VTVersion not read from Makefile (VL_VERSION_LOGS=%q VT_VERSION=%q) — an empty pin is never a valid basis to write or check the generated inventory", dirs.VLVersion, dirs.VTVersion))
	}
	inv, err := inventory.Extract(dirs)
	if err != nil {
		fatal(fmt.Errorf("extract (run make deps-logs deps-traces deps-vt first): %w", err))
	}
	reg, err := registry.LoadDir(filepath.Join(*root, "tests", "conformance", "registry", "rows"))
	if err != nil {
		fatal(err)
	}

	wantInv, err := inv.Bytes()
	if err != nil {
		fatal(err)
	}
	wantCov := []byte(report.RenderCoverage(inv, reg))
	files := []genFile{
		{filepath.Join(*root, "tests", "conformance", "inventory.generated.yaml"), wantInv},
		{filepath.Join(*root, "UPSTREAM_COVERAGE.md"), wantCov},
	}

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
		if stale {
			fmt.Println("generated files are stale — run: make conformance-gen")
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
