package conformance

import (
	"fmt"
	"os"
	"path"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
)

// The Makefile is the single source of truth for the three upstream pins
// (VL_VERSION_LOGS, VL_COMMIT_TRACES, VT_VERSION), but the Docker builds do
// not use it: each Dockerfile clones VictoriaLogs / VictoriaTraces itself from
// its own ARG default, and the Compose files pull the hot-tier images by tag.
//
// A pin that drifts from the Makefile is not a warning, it is a hard build
// failure with a misleading message: the patches under patches/ are generated
// against one exact upstream tree, so an older clone fails with
// "error: patch failed: app/vlstorage/main.go:569" — which reads like a broken
// patch rather than a stale version ARG. On the Compose side a stale tag
// silently benchmarks and parity-tests the wrong upstream release.
//
// These tests make every pin outside the Makefile equal to the Makefile's.

var (
	argVLVersionRe = regexp.MustCompile(`(?m)^ARG VL_VERSION=(\S+)`)
	argVLCommitRe  = regexp.MustCompile(`(?m)^ARG VL_COMMIT=(\S+)`)
	argVTVersionRe = regexp.MustCompile(`(?m)^ARG VT_VERSION=(\S+)`)

	// imageTagRe matches the upstream images by tag wherever Compose pulls or
	// wraps them: `image: victoriametrics/victoria-logs:v1.52.0`,
	// `UPSTREAM: victoriametrics/victoria-traces:v0.11.0`, ...
	imageTagRe = regexp.MustCompile(`victoriametrics/(victoria-logs|victoria-traces):(\S+)`)

	// dockerPatchRefRe matches a patch path a Dockerfile applies from the
	// copied patches/ tree.
	dockerPatchRefRe = regexp.MustCompile(`/tmp/patches/([A-Za-z0-9._/-]+\.patch)`)

	// dockerPatchCopyRe matches the COPY that stages the patches, whose source
	// decides what a /tmp/patches/... reference resolves to: Dockerfile.logs
	// copies only patches/vl-logs/, Dockerfile.traces copies patches/ whole.
	dockerPatchCopyRe = regexp.MustCompile(`(?m)^COPY\s+patches/(\S*)\s+/tmp/patches/`)
)

// dockerfiles are the image builds that clone upstream themselves.
var dockerfiles = []string{"Dockerfile.logs", "Dockerfile.traces", "Dockerfile.datagen"}

func readRepoFile(t *testing.T, root, rel string) string {
	t.Helper()
	data, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(rel)))
	if err != nil {
		t.Fatalf("read %s: %v", rel, err)
	}
	return string(data)
}

// TestDockerfilePinsMatchMakefile keeps every Dockerfile's upstream ARG
// defaults equal to the Makefile pins.
func TestDockerfilePinsMatchMakefile(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	d := inventory.DefaultDirs(root)
	if d.VLVersion == "" || d.VTVersion == "" || d.VLCommitTraces == "" {
		t.Fatalf("Makefile pins not readable: VL_VERSION_LOGS=%q VL_COMMIT_TRACES=%q VT_VERSION=%q", d.VLVersion, d.VLCommitTraces, d.VTVersion)
	}

	checks := []struct {
		file, arg, want string
		re              *regexp.Regexp
		optional        bool
	}{
		{"Dockerfile.logs", "VL_VERSION", d.VLVersion, argVLVersionRe, false},
		{"Dockerfile.datagen", "VL_VERSION", d.VLVersion, argVLVersionRe, false},
		{"Dockerfile.traces", "VL_VERSION", d.VLVersion, argVLVersionRe, false},
		{"Dockerfile.traces", "VL_COMMIT", d.VLCommitTraces, argVLCommitRe, false},
		{"Dockerfile.traces", "VT_VERSION", d.VTVersion, argVTVersionRe, false},
	}
	for _, c := range checks {
		body := readRepoFile(t, root, c.file)
		m := c.re.FindStringSubmatch(body)
		if m == nil {
			if c.optional {
				continue
			}
			t.Errorf("%s has no `ARG %s=` line — the Docker build no longer takes the pin as an argument, so it cannot be kept in step with the Makefile", c.file, c.arg)
			continue
		}
		if m[1] != c.want {
			t.Errorf("%s: ARG %s=%s but the Makefile pins %s.\n"+
				"The patches under patches/ are generated against the Makefile's tree; a stale ARG fails the image build with a \"patch failed\" hunk error that looks like a broken patch.",
				c.file, c.arg, m[1], c.want)
		}
	}
}

// TestDockerfilePatchRefsExist catches the other half of the same class of
// breakage: a Dockerfile applying a patch that was deleted from patches/ (as
// vt-traces/go-mod-replace.patch was, in favour of `go mod edit -replace`).
// The Makefile stops referencing it in the same commit; the Dockerfiles are
// easy to forget, and the failure only shows up in the image build.
func TestDockerfilePatchRefsExist(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	checked := 0
	for _, f := range dockerfiles {
		body := readRepoFile(t, root, f)
		refs := dockerPatchRefRe.FindAllStringSubmatch(body, -1)
		if len(refs) == 0 {
			continue
		}
		copyMatch := dockerPatchCopyRe.FindStringSubmatch(body)
		if copyMatch == nil {
			t.Errorf("%s applies patches from /tmp/patches/ but has no `COPY patches/... /tmp/patches/` line — the reference cannot be resolved", f)
			continue
		}
		prefix := copyMatch[1] // "" when the whole patches/ tree is copied
		for _, m := range refs {
			checked++
			rel := path.Join("patches", prefix, m[1])
			if _, err := os.Stat(filepath.Join(root, filepath.FromSlash(rel))); err != nil {
				t.Errorf("%s applies %s, which does not exist: the image build fails at that step. Remove the step or restore the patch.", f, rel)
			}
		}
	}
	if checked == 0 {
		t.Fatal("no patch references found in any Dockerfile — the regexp no longer matches how patches are applied, so this gate passes vacuously")
	}
}

// composeFiles are every Compose file that pins an upstream hot-tier image.
var composeFiles = []string{
	"deployment/docker/docker-compose-e2e.yml",
	"deployment/docker/docker-compose-benchmark.yml",
	"deployment/docker/docker-compose-benchmark.gp3.yml",
	"tests/parity/docker-compose.yml",
}

// TestComposeImagePinsMatchMakefile keeps the hot tier every Compose stack runs
// on the same release the binaries embed. A stale tag does not fail anything —
// it silently measures and parity-tests against the previous upstream version.
func TestComposeImagePinsMatchMakefile(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	d := inventory.DefaultDirs(root)
	want := map[string]string{"victoria-logs": d.VLVersion, "victoria-traces": d.VTVersion}

	found := map[string][]string{}
	for _, f := range composeFiles {
		body := readRepoFile(t, root, f)
		for i, line := range strings.Split(body, "\n") {
			for _, m := range imageTagRe.FindAllStringSubmatch(line, -1) {
				product, tag := m[1], m[2]
				found[product] = append(found[product], fmt.Sprintf("%s:%d", f, i+1))
				if tag != want[product] {
					t.Errorf("%s:%d pins victoriametrics/%s:%s but the Makefile pins %s — the stack would run against the previous upstream release.", f, i+1, product, tag, want[product])
				}
			}
		}
	}
	for product, refs := range found {
		sort.Strings(refs)
		t.Logf("%s pinned at %s in %d place(s): %s", product, want[product], len(refs), strings.Join(refs, " "))
	}
	if len(found["victoria-logs"]) == 0 || len(found["victoria-traces"]) == 0 {
		t.Fatalf("found no upstream image pins (victoria-logs=%d victoria-traces=%d) — the file list or the regexp is stale, so this gate passes vacuously", len(found["victoria-logs"]), len(found["victoria-traces"]))
	}
}

// TestComposeHealthchecksDoNotAssumeAShell guards the upgrade trap that broke
// the e2e and parity stacks: from VictoriaLogs v1.51.0 / VictoriaTraces v0.10.0
// the published images are distroless. A `test: ["CMD", "wget", ...]` on one of
// them can never run, the container never turns healthy, and every dependant
// wired with `condition: service_healthy` hangs until the job times out.
// Services built from Dockerfile.upstream-probe carry a static busybox for
// exactly this, and must probe through it.
func TestComposeHealthchecksDoNotAssumeAShell(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	probed := 0
	for _, f := range composeFiles {
		body := readRepoFile(t, root, f)
		lines := strings.Split(body, "\n")
		for i, line := range lines {
			if !strings.Contains(line, "Dockerfile.upstream-probe") {
				continue
			}
			probed++
			// The healthcheck belongs to the same service block: scan forward
			// to the next service (a line indented by exactly two spaces that
			// ends in ':').
			for j := i + 1; j < len(lines); j++ {
				l := lines[j]
				if regexp.MustCompile(`^  \S.*:$`).MatchString(l) {
					break
				}
				if !strings.Contains(l, `"CMD"`) {
					continue
				}
				if strings.Contains(l, `"wget"`) && !strings.Contains(l, "/probe/busybox") {
					t.Errorf("%s:%d probes a distroless upstream image with bare wget; use [\"CMD\", \"/probe/busybox\", \"wget\", ...] (the image has no shell and no wget of its own).", f, j+1)
				}
			}
		}
	}
	if probed == 0 {
		t.Fatal("no service builds from Dockerfile.upstream-probe — either the distroless workaround was removed (then remove this test too) or the file list is stale")
	}
}

// workflowPinRe matches a workflow env entry for one of the three pins, with
// or without quotes: `  VL_VERSION_LOGS: v1.52.0`, `  VT_VERSION: "v0.11.0"`.
var workflowPinRe = regexp.MustCompile(`(?m)^\s*(VL_VERSION_LOGS|VL_COMMIT_TRACES|VT_VERSION):\s*"?([^"\s#]+)"?`)

// TestWorkflowPinsMatchMakefile closes the last place the pins are duplicated:
// the workflow `env:` blocks. They feed the deps cache key and, for the image
// builds, the --build-arg values, so a stale one either restores a cache built
// from a different upstream tree or builds a release image against it.
func TestWorkflowPinsMatchMakefile(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	d := inventory.DefaultDirs(root)
	want := map[string]string{
		"VL_VERSION_LOGS":  d.VLVersion,
		"VL_COMMIT_TRACES": d.VLCommitTraces,
		"VT_VERSION":       d.VTVersion,
	}

	dir := filepath.Join(root, ".github", "workflows")
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatalf("read %s: %v", dir, err)
	}
	checked := 0
	for _, e := range entries {
		if e.IsDir() || !strings.HasSuffix(e.Name(), ".yaml") {
			continue
		}
		body := readRepoFile(t, root, path.Join(".github", "workflows", e.Name()))
		for _, line := range strings.Split(body, "\n") {
			m := workflowPinRe.FindStringSubmatch(line)
			if m == nil {
				continue
			}
			// `${{ env.X }}` references and `$(...)` expansions are uses, not
			// declarations.
			if strings.Contains(m[2], "$") {
				continue
			}
			checked++
			if m[2] != want[m[1]] {
				t.Errorf(".github/workflows/%s declares %s: %s but the Makefile pins %s", e.Name(), m[1], m[2], want[m[1]])
			}
		}
	}
	if checked == 0 {
		t.Fatal("no workflow pin declarations found — the regexp is stale, so this gate passes vacuously")
	}
	t.Logf("checked %d workflow pin declarations against the Makefile", checked)
}
