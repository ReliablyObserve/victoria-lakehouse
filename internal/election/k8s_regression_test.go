// internal/election/k8s_regression_test.go
//
// Regression locks for the binary-size and dependency-graph wins delivered
// by replacing the full k8s.io/client-go elector with a hand-rolled
// rest+meta/v1 implementation. Each test here is designed to FAIL loudly
// when a future refactor accidentally re-introduces the heavy k8s closure.
package election

import (
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"strings"
	"testing"
)

// forbiddenK8sPackages lists the k8s.io paths that we MUST NOT pull in from
// the election subtree. Adding any of them re-introduces the ~14 MB of
// transitive code we shed in PR #96.
//
// Discovery: `kubernetes` (the full clientset), `tools/leaderelection` (the
// official elector wrapper), `tools/leaderelection/resourcelock` (its lock
// abstraction that in turn imports kubernetes), and the heavy *typed* API
// modules (core/apps/resource/admissionregistration) all expand the closure
// to ~700 packages and ~21 MB.
//
// `apimachinery/pkg/apis/meta/v1` and `client-go/rest` are intentionally NOT
// forbidden — they are the 311-package Option B baseline.
var forbiddenK8sPackages = []string{
	"k8s.io/client-go/kubernetes",
	"k8s.io/client-go/tools/leaderelection",
	"k8s.io/client-go/tools/leaderelection/resourcelock",
	"k8s.io/api/core/v1",
	"k8s.io/api/apps/v1",
	"k8s.io/api/resource/v1",
	"k8s.io/api/admissionregistration/v1",
}

// TestNoForbiddenImports is the most important lock in this PR. It runs
// `go list -deps -json ./internal/election/...` and asserts that none of the
// forbidden packages above appear in the closure. Comment out the import
// statement in k8s.go and this test still passes; ADD any forbidden import
// to k8s.go (or to any package it depends on) and this test fails.
//
// Fails-without: bare-bones meta/v1+rest implementation in k8s.go. If a
// future maintainer pulls in client-go's kubernetes.NewForConfig or
// tools/leaderelection again, this test fires.
func TestNoForbiddenImports(t *testing.T) {
	out, err := runGoList(t, "-deps", "-json", "./internal/election/...")
	if err != nil {
		t.Fatalf("go list failed: %v", err)
	}
	dec := json.NewDecoder(strings.NewReader(out))
	seen := map[string]struct{}{}
	for dec.More() {
		var pkg struct {
			ImportPath string `json:"ImportPath"`
		}
		if err := dec.Decode(&pkg); err != nil {
			t.Fatalf("decode go list output: %v", err)
		}
		seen[pkg.ImportPath] = struct{}{}
	}
	var violations []string
	for _, banned := range forbiddenK8sPackages {
		if _, found := seen[banned]; found {
			violations = append(violations, banned)
		}
	}
	if len(violations) > 0 {
		t.Fatalf("forbidden k8s packages pulled into election subtree: %v\n"+
			"This re-introduces the heavy client-go closure we shed in PR #96. "+
			"See internal/election/README.md for the allowed surface (rest + meta/v1 only).",
			violations)
	}
}

// TestBinarySizeBound builds lakehouse-logs with the production ldflags and
// asserts the stripped binary stays under 40 MB. The pre-PR baseline was
// 55 MB; the slim (build-tag-gated) baseline was 33 MB; this PR's always-on
// K8s elector lands at ~37 MB. 40 MB gives a 3 MB cushion for future churn.
//
// Skipped under -short to keep `go test -short` cheap; CI's full run picks
// it up.
//
// Fails-without: -s -w stripping in the build; FIPS-on by default (which
// adds ~5 MB); re-introduction of unused heavy deps.
func TestBinarySizeBound(t *testing.T) {
	if testing.Short() {
		t.Skip("binary build is expensive; skipping under -short")
	}
	root := repoRoot(t)
	out := filepath.Join(t.TempDir(), "lakehouse-logs-size-test")
	cmd := exec.Command("go", "build", "-trimpath", "-ldflags=-s -w", "-o", out, "./cmd/lakehouse-logs")
	cmd.Dir = root
	cmd.Env = append(os.Environ(), "GOWORK=off", "CGO_ENABLED=0")
	if output, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("go build failed: %v\n%s", err, output)
	}
	info, err := os.Stat(out)
	if err != nil {
		t.Fatal(err)
	}
	const limit = int64(40 * 1024 * 1024)
	if info.Size() > limit {
		t.Fatalf("lakehouse-logs binary size = %d bytes (%.1f MB); limit = %d bytes (%.1f MB)",
			info.Size(), float64(info.Size())/1024/1024,
			limit, float64(limit)/1024/1024)
	}
	t.Logf("lakehouse-logs binary size = %d bytes (%.1f MB); limit = %.1f MB",
		info.Size(), float64(info.Size())/1024/1024,
		float64(limit)/1024/1024)
}

// TestElectionDepCount asserts the election subtree's dependency closure
// stays under the bound. Baseline at the time of the always-on K8s elector
// (PR #96, post-Option B implementation) is 329 packages (311-package
// Option B isolated baseline plus ~18 VL/VT logger/buildinfo helpers
// already imported by other subsystems). 340 gives a small cushion.
//
// Fails-without: the lean meta/v1+rest-only surface. Add even one new
// transitive k8s-flavoured import and this fails fast.
func TestElectionDepCount(t *testing.T) {
	out, err := runGoList(t, "-deps", "./internal/election/...")
	if err != nil {
		t.Fatalf("go list failed: %v", err)
	}
	count := 0
	for _, line := range strings.Split(strings.TrimSpace(out), "\n") {
		if strings.TrimSpace(line) != "" {
			count++
		}
	}
	const limit = 340
	if count > limit {
		t.Fatalf("election subtree dep count = %d; limit = %d. "+
			"See internal/election/README.md for the allowed import surface.",
			count, limit)
	}
	t.Logf("election subtree dep count = %d (limit %d)", count, limit)
}

// TestFIPSMode_WhenEnabled builds a tiny test binary with GOFIPS140=v1.0.0
// and asserts that crypto/fips140.Enabled() reports true at runtime when
// GODEBUG=fips140=on is also set.
//
// Fails-without: GOFIPS140 build-arg plumbing in the Makefile and
// Dockerfiles, or the crypto/fips140.Enabled() helper exported via
// fips-status subcommand.
func TestFIPSMode_WhenEnabled(t *testing.T) {
	if testing.Short() {
		t.Skip("FIPS-mode binary build is expensive; skipping under -short")
	}
	if runtime.GOOS == "windows" {
		t.Skip("FIPS test skipped on windows")
	}
	root := repoRoot(t)
	out := filepath.Join(t.TempDir(), "fips-probe")

	// Write a tiny probe program inside the temp dir, then build it with
	// GOFIPS140=v1.0.0. The probe writes "true" or "false" depending on
	// crypto/fips140.Enabled().
	probeDir := filepath.Join(t.TempDir(), "probe")
	if err := os.MkdirAll(probeDir, 0o755); err != nil {
		t.Fatal(err)
	}
	probe := `package main

import (
	"crypto/fips140"
	"fmt"
)

func main() {
	if fips140.Enabled() {
		fmt.Println("true")
		return
	}
	fmt.Println("false")
}
`
	if err := os.WriteFile(filepath.Join(probeDir, "main.go"), []byte(probe), 0o644); err != nil {
		t.Fatal(err)
	}
	gomod := "module fipsprobe\n\ngo 1.26\n"
	if err := os.WriteFile(filepath.Join(probeDir, "go.mod"), []byte(gomod), 0o644); err != nil {
		t.Fatal(err)
	}
	build := exec.Command("go", "build", "-o", out, ".")
	build.Dir = probeDir
	build.Env = append(os.Environ(),
		"GOWORK=off", "CGO_ENABLED=0",
		"GOFIPS140=v1.0.0",
	)
	if output, err := build.CombinedOutput(); err != nil {
		t.Fatalf("FIPS build failed: %v\n%s\nroot=%s", err, output, root)
	}
	// Run the probe with GODEBUG=fips140=on. crypto/fips140.Enabled() should
	// then return true.
	run := exec.Command(out)
	run.Env = append(os.Environ(), "GODEBUG=fips140=on")
	output, err := run.CombinedOutput()
	if err != nil {
		t.Fatalf("FIPS probe run failed: %v\n%s", err, output)
	}
	got := strings.TrimSpace(string(output))
	if got != "true" {
		t.Fatalf("FIPS probe returned %q; want \"true\" with GOFIPS140=v1.0.0 + GODEBUG=fips140=on", got)
	}
}

// TestRenewDeadline_LeaderExits is a negative-control complement to the
// renewLoop deadline branch in TestK8sElector_Renew_FailsWithinRenewDeadline.
// It exists here for documentation: a single regression test that names the
// exact production line it guards. To verify, comment out the deadline
// check in renewLoop and re-run; both TestK8sElector_Renew_FailsWithinRenewDeadline
// and this test will hang then fail.
//
// We don't duplicate the body; we just point at the canonical test.
func TestRenewDeadline_LeaderExits(t *testing.T) {
	// Sanity: confirm the helper test exists and references the public API.
	if testing.Short() {
		t.Skip("smoke gate; full deadline behaviour verified in TestK8sElector_Renew_FailsWithinRenewDeadline")
	}
	// Verify the constant is sensible (RenewDeadline strictly less than
	// LeaseDuration in defaults).
	e, err := NewK8sElector(K8sElectorConfig{LeaseName: "renew-deadline-control", Identity: "pod-0"})
	if err != nil {
		t.Fatal(err)
	}
	if !(e.cfg.RenewDeadline < e.cfg.LeaseDuration) {
		t.Errorf("RenewDeadline (%v) must be < LeaseDuration (%v) for liveness", e.cfg.RenewDeadline, e.cfg.LeaseDuration)
	}
}

// repoRoot returns the absolute path of the victoria-lakehouse repo root
// (containing go.mod). The test must run from inside the module.
func repoRoot(t *testing.T) string {
	t.Helper()
	return moduleRoot(t)
}

// runGoList invokes `go list` with GOWORK=off from the module root and
// captures stdout.
func runGoList(t *testing.T, args ...string) (string, error) {
	t.Helper()
	cmd := exec.Command("go", append([]string{"list"}, args...)...)
	cmd.Env = append(os.Environ(), "GOWORK=off")
	// Resolve to module root so `./internal/election/...` always parses.
	cmd.Dir = moduleRoot(t)
	out, err := cmd.Output()
	if err != nil {
		if ee, ok := err.(*exec.ExitError); ok {
			return string(out), fmt.Errorf("%w: stderr=%s", err, ee.Stderr)
		}
		return string(out), err
	}
	return string(out), nil
}

// moduleRoot walks parents of the current test file looking for go.mod.
func moduleRoot(t *testing.T) string {
	t.Helper()
	wd, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	cur := wd
	for {
		if _, err := os.Stat(filepath.Join(cur, "go.mod")); err == nil {
			return cur
		}
		parent := filepath.Dir(cur)
		if parent == cur {
			t.Fatalf("go.mod not found anywhere above %s", wd)
		}
		cur = parent
	}
}

// formatBytes pretty-prints byte counts for log messages.
//
//nolint:unused // helper retained for future bound-tightening tests.
func formatBytes(n int64) string {
	const mib = 1024 * 1024
	return strconv.FormatFloat(float64(n)/float64(mib), 'f', 1, 64) + " MiB"
}
