package report

import (
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for rel, content := range files {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(content), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

const (
	scanLogstorage = `"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"`
	scanVTStorage  = `"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"`
	scanVMLogger   = `"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"`
)

func scanFixture() map[string]string {
	return map[string]string{
		"go.mod": `module example.com/lh

require (
	github.com/VictoriaMetrics/VictoriaLogs v1.53.0
	github.com/VictoriaMetrics/VictoriaMetrics v1.152.1 // indirect
	github.com/other/thing v1.0.0
)

replace github.com/VictoriaMetrics/VictoriaLogs => ./deps/VictoriaLogs
`,
		"lakehouse-traces/go.mod": `module example.com/lh/traces

require github.com/VictoriaMetrics/VictoriaTraces v0.12.0

replace github.com/VictoriaMetrics/VictoriaTraces => ./deps/VictoriaTraces
`,
		"Makefile": "VL_VERSION_LOGS := v1.53.0\nVL_COMMIT_TRACES := abc123\nVT_VERSION := v0.12.0\nOTHER := x\n",

		"internal/a/a.go":         "package a\nimport (\n\t" + scanLogstorage + "\n\t" + scanVMLogger + "\n)\n",
		"internal/a/a_test.go":    "package a\nimport " + scanLogstorage + "\n",
		"lakehouse-traces/b/b.go": "package b\nimport (\n\t" + scanLogstorage + "\n\t" + scanVTStorage + "\n)\n",

		// Never scanned: vendored upstream trees, hidden dirs, testdata.
		"deps/VictoriaLogs/x/x.go":                  "package x\nimport " + scanVTStorage + "\n",
		"lakehouse-traces/deps/VictoriaLogs/y/y.go": "package y\nimport " + scanVTStorage + "\n",
		".claude/worktrees/w/w.go":                  "package w\nimport " + scanVTStorage + "\n",
		"internal/a/testdata/t.go":                  "package t\nimport " + scanVTStorage + "\n",

		// The generator's own fixtures are never guards.
		"tests/conformance/report/proof_x_test.go": "package report\nvar _ = " + scanVTStorage + " + \"patches/vl-logs/one.patch\" + \"VT_VERSION\"\n",

		"patches/vl-logs/one.patch":     "diff\n",
		"patches/vl-logs/two.go.src":    "package x\n",
		"patches/vl-logs/README.md":     "not a patch\n",
		"patches/vt-traces/three.patch": "diff\n",

		"internal/g/g_test.go":     "package g\n// guards patches/vl-logs/one.patch and VL_COMMIT_TRACES\n",
		"internal/h/h_test.go":     "package h\n// two.go.src by its file name only\n",
		"scripts/ci/gate.sh":       "#!/bin/sh\ncheck patches/vl-logs\n",
		"scripts/ci/tests/deep.sh": "patches/vt-traces/three.patch\n",
		"docs/notes.md":            "patches/vt-traces/three.patch VT_VERSION\n",
	}
}

func TestScanSharedCode(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, scanFixture())
	in, err := ScanSharedCode(root)
	if err != nil {
		t.Fatal(err)
	}

	wantPatches := []PatchRef{
		{Path: "patches/vl-logs/one.patch", Guards: []string{"internal/g/g_test.go"}, DirGuards: []string{"scripts/ci/gate.sh"}},
		{Path: "patches/vl-logs/two.go.src", Guards: []string{"internal/h/h_test.go"}, DirGuards: []string{"internal/g/g_test.go", "scripts/ci/gate.sh"}},
		{Path: "patches/vt-traces/three.patch"}, // only a docs file and a nested test script name it
	}
	if !reflect.DeepEqual(in.Patches, wantPatches) {
		t.Errorf("patches:\n got %+v\nwant %+v", in.Patches, wantPatches)
	}

	wantPins := []PinRef{
		{Name: "VL_VERSION_LOGS", Value: "v1.53.0"},
		{Name: "VL_COMMIT_TRACES", Value: "abc123", Guards: []string{"internal/g/g_test.go"}},
		{Name: "VT_VERSION", Value: "v0.12.0"},
	}
	if !reflect.DeepEqual(in.Pins, wantPins) {
		t.Errorf("pins:\n got %+v\nwant %+v", in.Pins, wantPins)
	}

	wantMods := []GoModDep{
		{File: "go.mod", Module: "github.com/VictoriaMetrics/VictoriaLogs", Version: "v1.53.0", Replace: "./deps/VictoriaLogs"},
		{File: "go.mod", Module: "github.com/VictoriaMetrics/VictoriaMetrics", Version: "v1.152.1"},
		{File: "lakehouse-traces/go.mod", Module: "github.com/VictoriaMetrics/VictoriaTraces", Version: "v0.12.0", Replace: "./deps/VictoriaTraces"},
	}
	if !reflect.DeepEqual(in.GoMods, wantMods) {
		t.Errorf("go.mod deps:\n got %+v\nwant %+v", in.GoMods, wantMods)
	}

	var imports []string
	for _, u := range in.Imports {
		imports = append(imports, strings.TrimPrefix(u.Path, upstreamModulePrefix)+" "+u.Module+" "+u.Kind+" "+u.File)
	}
	wantImports := []string{
		"VictoriaLogs/lib/logstorage logs src internal/a/a.go",
		"VictoriaLogs/lib/logstorage logs test internal/a/a_test.go",
		"VictoriaLogs/lib/logstorage traces src lakehouse-traces/b/b.go",
		"VictoriaMetrics/lib/logger logs src internal/a/a.go",
		"VictoriaTraces/app/vtstorage traces src lakehouse-traces/b/b.go",
	}
	if !reflect.DeepEqual(imports, wantImports) {
		t.Errorf("imports:\n got %q\nwant %q", imports, wantImports)
	}

	// The scan feeds the computation end to end.
	sc := computeShared(in)
	if sc.Items != 3+3+2 || sc.LibPackages != 1 {
		t.Errorf("shared = %d items, %d libs", sc.Items, sc.LibPackages)
	}
}

func TestScanSharedCode_Deterministic(t *testing.T) {
	root := t.TempDir()
	writeTree(t, root, scanFixture())
	a, err := ScanSharedCode(root)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ScanSharedCode(root)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(a, b) {
		t.Error("two scans of one tree differ")
	}
}

func TestScanSharedCode_EmptyTree(t *testing.T) {
	in, err := ScanSharedCode(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	if len(in.Pins)+len(in.Patches)+len(in.Imports)+len(in.GoMods) != 0 {
		t.Errorf("an empty tree has no shared code: %+v", in)
	}
}

func TestParseGoModUpstream(t *testing.T) {
	got := parseGoModUpstream("go.mod", `module m

require github.com/VictoriaMetrics/VictoriaTraces v0.12.0
require (
	github.com/VictoriaMetrics/metrics v1.44.0
)
replace github.com/VictoriaMetrics/VictoriaLogs v1.0.0 => ../vl
`)
	want := []GoModDep{
		{File: "go.mod", Module: "github.com/VictoriaMetrics/VictoriaLogs", Replace: "../vl"},
		{File: "go.mod", Module: "github.com/VictoriaMetrics/VictoriaTraces", Version: "v0.12.0"},
	}
	if !reflect.DeepEqual(got, want) {
		t.Errorf("got %+v, want %+v (a replace without a require is still listed; the metrics module is not an upstream app)", got, want)
	}
}

func TestShortList(t *testing.T) {
	if got := shortList(nil, 2); got != "—" {
		t.Errorf("empty = %q", got)
	}
	if got := shortList([]string{"a", "b"}, 2); got != "`a`, `b`" {
		t.Errorf("two = %q", got)
	}
	if got := shortList([]string{"a", "b", "c", "d"}, 2); got != "`a`, `b` (+2)" {
		t.Errorf("four = %q", got)
	}
}
