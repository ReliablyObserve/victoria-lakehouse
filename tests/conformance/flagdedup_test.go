package conformance

import (
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
)

// The lakehouse-traces binary links VictoriaLogs' vlstorage/vlinsert packages
// and VictoriaTraces' vtstorage/vtinsert packages into ONE process. Both
// upstreams register their own -retentionPeriod, -storageDataPath,
// -insert.maxFieldsPerLine and so on with the same global flag.CommandLine, and
// Go's flag package panics at init on a duplicate name — so every name both
// sides register has to be deduplicated. That is what
// patches/vt-traces/vtstorage-flag-dedup.patch (via the safe* helpers in
// flag_dedup.go.src) and patches/vt-traces/vtinsert-flag-dedup.patch do: the
// VictoriaTraces side checks flag.Lookup first and reuses VictoriaLogs'
// already-registered Value instead of registering a second one.
//
// The failure mode this file guards is silent and total: an upstream bump that
// adds one new VictoriaTraces flag colliding with an existing VictoriaLogs flag
// makes the traces binary panic on startup. It still COMPILES, so build, vet
// and every unit test stay green — only the binary is dead. The tests below
// recompute the collision set from the two vendored trees and require the
// dedup list to match it exactly, in both directions.

// safeRegisterRe matches a registration that goes through the dedup helpers
// (safeInt, safeBool, safeRetentionDuration, ...) added by flag_dedup.go.src.
var safeRegisterRe = regexp.MustCompile(`safe[A-Z][A-Za-z]*\(\s*"([a-zA-Z0-9_.\-]+)"`)

// lookupGuardRe matches the open-coded form the vtinsert patches use, where a
// package-level var is initialised from an existing flag.Lookup("name") before
// falling back to registering it.
var lookupGuardRe = regexp.MustCompile(`flag\.Lookup\(\s*"([a-zA-Z0-9_.\-]+)"\s*\)`)

const (
	vlModulePrefix = "github.com/VictoriaMetrics/VictoriaLogs/"
	vtModulePrefix = "github.com/VictoriaMetrics/VictoriaTraces/"
)

// tracesLinkedPackages returns the upstream VictoriaLogs and VictoriaTraces
// packages actually linked into the lakehouse-traces binary, as directories in
// the two vendored trees. `go list -deps` is the ground truth: the static
// package lists in inventory (VLFlagPackages / LinkedIntoLH) are coarser —
// they mark app/vlinsert linked as a whole, which would invent a collision on
// nativeinsert.maxRequestSize even though the traces binary links only
// vlinsert/insertutil and vlinsert/opentelemetry.
func tracesLinkedPackages(t *testing.T, root string) (vlDirs, vtDirs []string) {
	t.Helper()
	tracesDir := filepath.Join(root, "lakehouse-traces")
	cmd := exec.Command("go", "list", "-deps", ".")
	cmd.Dir = tracesDir
	cmd.Env = append(os.Environ(), "GOWORK=off")
	out, err := cmd.Output()
	if err != nil {
		t.Fatalf("go list -deps in %s: %v", tracesDir, err)
	}
	vlRoot := filepath.Join(tracesDir, "deps", "VictoriaLogs")
	vtRoot := filepath.Join(tracesDir, "deps", "VictoriaTraces")
	for _, pkg := range strings.Fields(string(out)) {
		switch {
		case strings.HasPrefix(pkg, vlModulePrefix+"app/"):
			vlDirs = append(vlDirs, filepath.Join(vlRoot, filepath.FromSlash(strings.TrimPrefix(pkg, vlModulePrefix))))
		case strings.HasPrefix(pkg, vtModulePrefix+"app/"):
			vtDirs = append(vtDirs, filepath.Join(vtRoot, filepath.FromSlash(strings.TrimPrefix(pkg, vtModulePrefix))))
		}
	}
	if len(vlDirs) == 0 || len(vtDirs) == 0 {
		t.Fatalf("go list -deps found no upstream app packages (VictoriaLogs=%d VictoriaTraces=%d) — the module paths changed", len(vlDirs), len(vtDirs))
	}
	return vlDirs, vtDirs
}

// scanFlags collects every flag name registered by the given package
// directories, mapped to the directory that registers it (relative to root).
func scanFlags(t *testing.T, root string, dirs []string) map[string]string {
	t.Helper()
	out := map[string]string{}
	for _, d := range dirs {
		names, err := inventory.ScanFlagNames(d)
		if err != nil {
			t.Fatalf("scan %s: %v", d, err)
		}
		for name, file := range names {
			if _, ok := out[name]; ok {
				continue
			}
			rel, _ := filepath.Rel(root, filepath.Join(d, file))
			out[name] = filepath.ToSlash(rel)
		}
	}
	return out
}

// requireTracesDeps skips only when the traces module's vendored trees are
// absent and skipping is allowed, matching the rest of this package.
func requireTracesDeps(t *testing.T, root string) {
	t.Helper()
	for _, dir := range []string{
		filepath.Join(root, "lakehouse-traces", "deps", "VictoriaLogs"),
		filepath.Join(root, "lakehouse-traces", "deps", "VictoriaTraces"),
	} {
		if _, err := os.Stat(dir); err != nil {
			if os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" {
				t.Fatalf("deps missing (%s): run make deps-traces deps-vt", dir)
			}
			t.Skip("vendored deps not present locally")
		}
	}
}

// dedupGuardedFlags returns the flag names the VictoriaTraces side registers
// through a dedup guard — either a safe* helper or an open-coded
// flag.Lookup("name") fallback.
func dedupGuardedFlags(t *testing.T, vtDirs []string) map[string]bool {
	t.Helper()
	guarded := map[string]bool{}
	for _, d := range vtDirs {
		ents, err := os.ReadDir(d)
		if err != nil {
			t.Fatalf("read %s: %v", d, err)
		}
		for _, e := range ents {
			if e.IsDir() || !strings.HasSuffix(e.Name(), ".go") || strings.HasSuffix(e.Name(), "_test.go") {
				continue
			}
			data, err := os.ReadFile(filepath.Join(d, e.Name()))
			if err != nil {
				t.Fatalf("read %s: %v", filepath.Join(d, e.Name()), err)
			}
			for _, re := range []*regexp.Regexp{safeRegisterRe, lookupGuardRe} {
				for _, m := range re.FindAllStringSubmatch(string(data), -1) {
					guarded[m[1]] = true
				}
			}
		}
	}
	return guarded
}

// TestVTFlagDedupCoversEveryCollision is the gate: every flag name registered
// by both a linked VictoriaLogs package and a linked VictoriaTraces package
// must be deduplicated on the VictoriaTraces side, and nothing else may be.
//
//   - A collision that is NOT guarded means the traces binary panics on
//     startup with "flag redefined". Fix: extend
//     patches/vt-traces/vtstorage-flag-dedup.patch (or the vtinsert one) to
//     route that registration through the safe* helper.
//   - A guard with NO collision behind it means the dedup list outlived the
//     collision it was written for (upstream renamed or dropped the
//     VictoriaLogs flag). Fix: drop it from the patch, so the traces binary
//     registers the flag normally instead of silently inheriting a value
//     from nowhere.
func TestVTFlagDedupCoversEveryCollision(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	requireTracesDeps(t, root)

	vlDirs, vtDirs := tracesLinkedPackages(t, root)
	vlFlags := scanFlags(t, root, vlDirs)
	vtFlags := scanFlags(t, root, vtDirs)

	guarded := dedupGuardedFlags(t, vtDirs)
	collisions, unguarded, dead := diffDedup(vlFlags, vtFlags, guarded)

	if len(collisions) == 0 {
		t.Fatal("no VictoriaLogs/VictoriaTraces flag collisions found at all — the scan is not seeing the vendored sources, so this gate would pass vacuously")
	}
	for _, name := range unguarded {
		t.Errorf("flag -%s is registered by BOTH %s (VictoriaLogs) and %s (VictoriaTraces) but is not deduplicated: lakehouse-traces will panic at startup with \"flag redefined\".\n"+
			"Add it to patches/vt-traces/vtstorage-flag-dedup.patch (safe* helper from flag_dedup.go.src) or patches/vt-traces/vtinsert-flag-dedup.patch.",
			name, vlFlags[name], vtFlags[name])
	}
	for _, name := range dead {
		t.Errorf("flag -%s is deduplicated on the VictoriaTraces side (%s) but VictoriaLogs no longer registers it, so there is nothing to collide with: the guard makes VictoriaTraces silently skip its own registration. Drop it from patches/vt-traces/*-flag-dedup*.patch.",
			name, vtFlags[name])
	}

	t.Logf("VictoriaLogs/VictoriaTraces flag collisions in lakehouse-traces: %d, all deduplicated: %s", len(collisions), strings.Join(collisions, " "))
}

// diffDedup is the whole decision, separated from the filesystem so it can be
// exercised in both failing directions without touching the vendored trees
// (which are never modified, not even in a test):
//
//	collisions — names registered by both linked upstreams
//	unguarded  — collisions with no dedup guard (startup panic)
//	dead       — guards whose VictoriaLogs counterpart is gone (silent skip).
//	             Guards for flags the traces binary does not link at all are
//	             ignored: they cannot affect this binary either way.
func diffDedup(vlFlags, vtFlags map[string]string, guarded map[string]bool) (collisions, unguarded, dead []string) {
	for name := range vtFlags {
		if _, ok := vlFlags[name]; ok {
			collisions = append(collisions, name)
			if !guarded[name] {
				unguarded = append(unguarded, name)
			}
		}
	}
	for name := range guarded {
		if _, ok := vtFlags[name]; !ok {
			continue
		}
		if _, ok := vlFlags[name]; !ok {
			dead = append(dead, name)
		}
	}
	sort.Strings(collisions)
	sort.Strings(unguarded)
	sort.Strings(dead)
	return collisions, unguarded, dead
}

// TestDiffDedup_Cases pins both failure directions and the passing shape.
func TestDiffDedup_Cases(t *testing.T) {
	vl := map[string]string{
		"retentionPeriod":     "app/vlstorage/main.go",
		"storageDataPath":     "app/vlstorage/main.go",
		"loki.maxRequestSize": "app/vlinsert/loki/loki_json.go",
	}
	vt := map[string]string{
		"retentionPeriod":             "app/vtstorage/main.go", // guarded collision
		"storageDataPath":             "app/vtstorage/main.go", // UNGUARDED collision
		"servicegraph.taskInterval":   "app/victoria-traces/servicegraph/servicegraph.go",
		"nativeinsert.maxRequestSize": "app/vtinsert/nativeinsert/nativeinsert.go", // guard with no VL counterpart
	}
	guarded := map[string]bool{
		"retentionPeriod":             true,
		"nativeinsert.maxRequestSize": true,
		"someFlagInAnUnlinkedPackage": true, // not in vt: ignored
	}

	collisions, unguarded, dead := diffDedup(vl, vt, guarded)
	if want := []string{"retentionPeriod", "storageDataPath"}; !reflect.DeepEqual(collisions, want) {
		t.Errorf("collisions = %v, want %v", collisions, want)
	}
	if want := []string{"storageDataPath"}; !reflect.DeepEqual(unguarded, want) {
		t.Errorf("unguarded = %v, want %v", unguarded, want)
	}
	if want := []string{"nativeinsert.maxRequestSize"}; !reflect.DeepEqual(dead, want) {
		t.Errorf("dead = %v, want %v", dead, want)
	}

	// Fully deduplicated: no findings in either direction.
	_, unguarded, dead = diffDedup(
		map[string]string{"retentionPeriod": "a"},
		map[string]string{"retentionPeriod": "b"},
		map[string]bool{"retentionPeriod": true},
	)
	if len(unguarded) != 0 || len(dead) != 0 {
		t.Errorf("clean case reported unguarded=%v dead=%v", unguarded, dead)
	}
}

// TestVTFlagDedupPatchesPresent keeps the mechanism itself from disappearing:
// the collision test above reads the PATCHED vendored tree, so deleting the
// patches while leaving an already-patched checkout on disk would let it pass.
func TestVTFlagDedupPatchesPresent(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range []string{
		"patches/vt-traces/flag_dedup.go.src",
		"patches/vt-traces/vtstorage-flag-dedup.patch",
		"patches/vt-traces/vtinsert-flag-dedup.patch",
	} {
		if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(p))); err != nil {
			t.Errorf("%s is missing — the lakehouse-traces flag dedup has no mechanism behind it: %v", p, err)
		}
	}
}
