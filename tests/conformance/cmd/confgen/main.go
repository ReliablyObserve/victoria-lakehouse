// confgen regenerates tests/conformance/inventory.generated.yaml and
// UPSTREAM_COVERAGE.md from the vendored upstream sources and the registry.
//
//	confgen -write   # regenerate both files
//	confgen -check   # exit 1 when a regeneration would change either file
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

func main() {
	write := flag.Bool("write", false, "regenerate files")
	check := flag.Bool("check", false, "fail when files are stale")
	root := flag.String("root", "", "repo root (default: auto-detect)")
	flag.Parse()
	if *root == "" {
		r, err := inventory.RepoRoot()
		if err != nil {
			fatal(err)
		}
		*root = r
	}
	dirs := inventory.DefaultDirs(*root)
	if *write && (dirs.VLVersion == "" || dirs.VTVersion == "") {
		fatal(fmt.Errorf("VLVersion/VTVersion not read from Makefile (VL_VERSION_LOGS=%q VT_VERSION=%q) — refusing to write an inventory with an empty version", dirs.VLVersion, dirs.VTVersion))
	}
	inv, err := inventory.Extract(dirs)
	if err != nil {
		fatal(fmt.Errorf("extract (run make deps-logs deps-traces deps-vt first): %w", err))
	}
	reg, err := registry.LoadDir(filepath.Join(*root, "tests", "conformance", "registry", "rows"))
	if err != nil {
		fatal(err)
	}
	invPath := filepath.Join(*root, "tests", "conformance", "inventory.generated.yaml")
	covPath := filepath.Join(*root, "UPSTREAM_COVERAGE.md")
	tmp := filepath.Join(os.TempDir(), "inventory.generated.yaml")
	if err := inv.Write(tmp); err != nil {
		fatal(err)
	}
	wantInv, _ := os.ReadFile(tmp)
	wantCov := []byte(report.RenderCoverage(inv, reg))
	stale := false
	for _, f := range []struct {
		path string
		want []byte
	}{{invPath, wantInv}, {covPath, wantCov}} {
		have, _ := os.ReadFile(f.path)
		if !bytes.Equal(have, f.want) {
			stale = true
			if *write {
				if err := os.WriteFile(f.path, f.want, 0o644); err != nil {
					fatal(err)
				}
				fmt.Println("wrote", f.path)
			} else {
				fmt.Println("stale:", f.path)
			}
		}
	}
	if *check && stale {
		fmt.Println("generated files are stale — run: make conformance-gen")
		os.Exit(1)
	}
	if !*write && !*check {
		fmt.Println("nothing to do: pass -write or -check")
	}
}

func fatal(err error) {
	fmt.Fprintln(os.Stderr, "confgen:", err)
	os.Exit(2)
}
