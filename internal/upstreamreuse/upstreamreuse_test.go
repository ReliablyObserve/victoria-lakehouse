// Package upstreamreuse holds regression tests that pin the
// "extend VL/VT, don't duplicate" policy documented in
// patches/README.md. The tests are pure file-existence / regex
// checks against the repo tree — they don't run any VL/VT code,
// so they stay fast and can be enforced in CI without re-cloning
// upstream deps.
//
// Two classes of guard:
//
//  1. Patch presence. Each patch in patches/{vl-logs,vl-traces,vt-traces}/
//     must exist. Deleting one without updating this list is a
//     deliberate act and forces the contributor to remove the
//     guard, which lands in the same PR.
//
//  2. Import presence. Every VL/VT symbol enumerated in
//     patches/README.md's "Imported VL/VT symbols" table must be
//     present somewhere under internal/ or lakehouse-traces/, and
//     must NOT have been replaced with a local re-implementation.
//     The forbidden patterns are also enumerated below.
package upstreamreuse

import (
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"testing"
)

// repoRoot resolves the repository root from the test's working
// directory (internal/upstreamreuse). Anchored to go.mod so the
// guards can't be silently bypassed by relocating the test file.
func repoRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatalf("Getwd: %v", err)
	}
	dir := wd
	for {
		if _, err := os.Stat(filepath.Join(dir, "go.mod")); err == nil {
			// Verify it's the lakehouse go.mod, not the test's
			// internal go.sum or similar.
			if data, _ := os.ReadFile(filepath.Join(dir, "go.mod")); strings.Contains(string(data), "victoria-lakehouse") {
				return dir
			}
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("could not locate repo root from %s", wd)
		}
		dir = parent
	}
}

// TestRequiredPatchesExist pins every patch file that LH relies on.
// If a contributor deletes one of these (e.g. because they think LH
// no longer needs the extension and want to revert to a local copy),
// the test fails and forces them to update both the policy doc
// AND this list in the same PR.
func TestRequiredPatchesExist(t *testing.T) {
	required := []string{
		// VL — applied to deps/VictoriaLogs/
		"patches/vl-logs/external.go.src",
		"patches/vl-logs/external_query.go.src",
		"patches/vl-logs/vlstorage-dispatch.patch",
		"patches/vl-logs/vl-export-severity.patch",
		// VL — applied to lakehouse-traces/deps/VictoriaLogs/ (mirror of vl-logs)
		"patches/vl-traces/external.go.src",
		"patches/vl-traces/external_query.go.src",
		"patches/vl-traces/vlstorage-dispatch.patch",
		"patches/vl-traces/vl-export-severity.patch",
		// VT — applied to lakehouse-traces/deps/VictoriaTraces/
		"patches/vt-traces/external.go.src",
		"patches/vt-traces/flag_dedup.go.src",
		"patches/vt-traces/vtstorage-dispatch.patch",
		"patches/vt-traces/vtstorage-flag-dedup.patch",
		"patches/vt-traces/go-mod-replace.patch",
	}
	root := repoRoot(t)
	for _, rel := range required {
		t.Run(rel, func(t *testing.T) {
			path := filepath.Join(root, rel)
			info, err := os.Stat(path)
			if err != nil {
				t.Fatalf("required patch missing: %s (%v) — see patches/README.md for the policy", rel, err)
			}
			if info.Size() == 0 {
				t.Errorf("required patch empty: %s — likely a botched edit", rel)
			}
		})
	}
}

// TestVLLogsPatchesMirrorTraces guards rule "if you add a patch to
// vl-logs, mirror it to vl-traces". The two modules share the VL
// dependency, so any patched symbol exposed in one must be exposed
// in the other or the traces build breaks at runtime when it tries
// to call the symbol.
func TestVLLogsPatchesMirrorTraces(t *testing.T) {
	root := repoRoot(t)
	logsDir := filepath.Join(root, "patches/vl-logs")
	tracesDir := filepath.Join(root, "patches/vl-traces")
	entries, err := os.ReadDir(logsDir)
	if err != nil {
		t.Fatalf("ReadDir %s: %v", logsDir, err)
	}
	for _, e := range entries {
		if e.IsDir() {
			continue
		}
		name := e.Name()
		// Patches and the full-file replacements both count.
		if !strings.HasSuffix(name, ".patch") && !strings.HasSuffix(name, ".go.src") {
			continue
		}
		mirror := filepath.Join(tracesDir, name)
		if _, err := os.Stat(mirror); err != nil {
			t.Errorf("patches/vl-logs/%s has no mirror in patches/vl-traces/ — both VL clones must carry the same patches", name)
		}
	}
}

// TestVLVTSymbolsAreImported guards rule "every documented VL/VT
// import stays imported". If a contributor "cleans up" the import
// and pastes a local copy of the upstream symbol, the grep here
// fails and surfaces the policy violation.
//
// Each row pairs the upstream Go package path with the source file
// that imports it. Use a representative caller per upstream package
// rather than an exhaustive list — exhaustive checks here would
// become a refactor tax, while the representative one is enough to
// catch a "delete the import" regression.
func TestVLVTSymbolsAreImported(t *testing.T) {
	cases := []struct {
		callerRel    string
		upstreamPath string
	}{
		{
			callerRel:    "internal/vlstorage/insert.go",
			upstreamPath: "github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/opentelemetry",
		},
		{
			callerRel:    "internal/vlstorage/insert.go",
			upstreamPath: "github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage",
		},
		{
			callerRel:    "internal/vlstorage/insert.go",
			upstreamPath: "github.com/VictoriaMetrics/VictoriaLogs/app/vlinsert/insertutil",
		},
		{
			callerRel:    "internal/selectapi/handler.go",
			upstreamPath: "github.com/VictoriaMetrics/VictoriaLogs/app/vlselect/logsql",
		},
		{
			callerRel:    "lakehouse-traces/internal/selectapi/handler.go",
			upstreamPath: "github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/tempo",
		},
		{
			callerRel:    "lakehouse-traces/internal/selectapi/handler.go",
			upstreamPath: "github.com/VictoriaMetrics/VictoriaTraces/app/vtselect/traces/jaeger",
		},
	}
	root := repoRoot(t)
	for _, tc := range cases {
		t.Run(tc.callerRel+"->"+tc.upstreamPath, func(t *testing.T) {
			body, err := os.ReadFile(filepath.Join(root, tc.callerRel))
			if err != nil {
				t.Fatalf("read %s: %v", tc.callerRel, err)
			}
			if !strings.Contains(string(body), tc.upstreamPath) {
				t.Errorf("%s no longer imports %s — per patches/README.md the upstream symbol is reused, not re-implemented. If you really want to drop the import, also update patches/README.md and this test in the same PR.", tc.callerRel, tc.upstreamPath)
			}
		})
	}
}

// TestForbiddenLocalCopiesOfUpstreamSymbols guards against the most
// common policy violation: someone reads upstream, copies the
// implementation into LH, and silently diverges. The forbidden
// patterns below are signatures of upstream code we already import.
// A hit means "stop, you're duplicating — use the upstream import."
func TestForbiddenLocalCopiesOfUpstreamSymbols(t *testing.T) {
	cases := []struct {
		name     string
		pattern  *regexp.Regexp
		dirRoots []string
		excluded []string
		reason   string
	}{
		{
			name:    "logSeverities table re-defined locally",
			pattern: regexp.MustCompile(`var\s+\w*[lL]ogSeverities\s*=\s*\[\]string\{`),
			dirRoots: []string{"internal", "lakehouse-traces/internal",
				"cmd", "lakehouse-traces/cmd"},
			reason: "VL upstream owns this table; reuse via otelpb.FormatSeverity (see patches/vl-logs/vl-export-severity.patch)",
		},
		{
			name:    "OTel severity number → text table re-defined",
			pattern: regexp.MustCompile(`"Trace4",\s*"Debug"`),
			dirRoots: []string{"internal", "lakehouse-traces/internal",
				"cmd", "lakehouse-traces/cmd"},
			reason: "Same as logSeverities — call FormatSeverity instead.",
		},
	}

	root := repoRoot(t)
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var hits []string
			for _, sub := range tc.dirRoots {
				dir := filepath.Join(root, sub)
				err := filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
					if err != nil || d.IsDir() {
						return nil
					}
					// Test files OK — they may contain the pattern as expected output assertions.
					if strings.HasSuffix(path, "_test.go") {
						return nil
					}
					if !strings.HasSuffix(path, ".go") {
						return nil
					}
					// Skip patched upstream copies (these live under deps/, not internal/).
					if strings.Contains(path, "/deps/") {
						return nil
					}
					for _, ex := range tc.excluded {
						if strings.Contains(path, ex) {
							return nil
						}
					}
					data, err := os.ReadFile(path)
					if err != nil {
						return nil
					}
					if tc.pattern.Match(data) {
						hits = append(hits, path)
					}
					return nil
				})
				if err != nil {
					t.Logf("walk %s: %v", dir, err)
				}
			}
			if len(hits) > 0 {
				t.Errorf("forbidden pattern %q found in %d file(s):\n  %s\nreason: %s",
					tc.pattern, len(hits), strings.Join(hits, "\n  "), tc.reason)
			}
		})
	}
}
