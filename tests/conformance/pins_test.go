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
	// `UPSTREAM: victoriametrics/victoria-traces:v0.12.0`, ...
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

var (
	// makefilePatchRe matches a `git apply` line of a Makefile deps target and
	// captures the patch directory (vl-logs, vl-traces, vt-traces) and the patch.
	makefilePatchRe = regexp.MustCompile(`(?m)^\s*cd\s.*&&\s*git apply (?:\.\./)+patches/([A-Za-z0-9._-]+)/([A-Za-z0-9._-]+\.patch)\s*$`)

	// dockerApplyRe matches a `git apply` of a staged patch in a Dockerfile.
	dockerApplyRe = regexp.MustCompile(`git apply /tmp/patches/([A-Za-z0-9._/-]+\.patch)`)
)

// makefilePatchLists returns, per patches/ subdirectory, the patches `make deps`
// applies, in order.
func makefilePatchLists(makefile string) map[string][]string {
	out := map[string][]string{}
	for _, m := range makefilePatchRe.FindAllStringSubmatch(makefile, -1) {
		out[m[1]] = append(out[m[1]], m[2])
	}
	return out
}

// dockerfilePatchLists returns, per patches/ subdirectory, the patches a
// Dockerfile applies, in order. A Dockerfile that copies a single directory
// (`COPY patches/vl-logs/ /tmp/patches/`) refers to its patches without the
// directory, so the COPY source is put back in front.
func dockerfilePatchLists(dockerfile string) map[string][]string {
	prefix := ""
	if m := dockerPatchCopyRe.FindStringSubmatch(dockerfile); m != nil {
		prefix = m[1]
	}
	out := map[string][]string{}
	for _, m := range dockerApplyRe.FindAllStringSubmatch(dockerfile, -1) {
		rel := path.Join(prefix, m[1])
		dir, name := path.Dir(rel), path.Base(rel)
		out[dir] = append(out[dir], name)
	}
	return out
}

// patchListProblems compares what a Dockerfile applies with what the Makefile
// applies for the directories it is expected to build, and returns one line per
// difference: a patch missing, a patch the Makefile does not apply, or the same
// patches in another order. want is the Makefile's lists; dirs are the
// directories the Dockerfile must cover (it must apply nothing outside them).
func patchListProblems(file string, want, got map[string][]string, dirs []string) []string {
	var problems []string
	covered := map[string]bool{}
	for _, dir := range dirs {
		covered[dir] = true
		w, g := want[dir], got[dir]
		if len(w) == 0 {
			problems = append(problems, fmt.Sprintf("the Makefile applies no patch from patches/%s: the parse no longer matches its layout, so this gate would pass vacuously", dir))
			continue
		}
		inW, inG := map[string]bool{}, map[string]bool{}
		for _, n := range w {
			inW[n] = true
		}
		for _, n := range g {
			inG[n] = true
		}
		missing, extra := false, false
		for _, n := range w {
			if !inG[n] {
				missing = true
				problems = append(problems, fmt.Sprintf("%s does not apply patches/%s/%s, which `make deps` applies: the released image is built without it", file, dir, n))
			}
		}
		for _, n := range g {
			if !inW[n] {
				extra = true
				problems = append(problems, fmt.Sprintf("%s applies patches/%s/%s, which `make deps` does not apply", file, dir, n))
			}
		}
		if !missing && !extra && strings.Join(w, "\n") != strings.Join(g, "\n") {
			problems = append(problems, fmt.Sprintf("%s applies patches/%s in the order %v but `make deps` applies %v", file, dir, g, w))
		}
	}
	var stray []string
	for dir := range got {
		if !covered[dir] {
			stray = append(stray, dir)
		}
	}
	sort.Strings(stray)
	for _, dir := range stray {
		problems = append(problems, fmt.Sprintf("%s applies patches from patches/%s, which it does not build", file, dir))
	}
	return problems
}

// TestDockerfilePatchesMatchMakefile keeps what the release images apply equal
// to what `make deps` applies, per patch directory and in the same order. The
// patch exists-check above cannot see a patch a Dockerfile LEAVES OUT: the
// Dockerfiles listed vl-const-timestamps-parse.patch nowhere, so every image
// was built without it while every local and CI test ran with it.
func TestDockerfilePatchesMatchMakefile(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	want := makefilePatchLists(readRepoFile(t, root, "Makefile"))
	for _, c := range []struct {
		file string
		dirs []string
	}{
		{"Dockerfile.logs", []string{"vl-logs"}},
		{"Dockerfile.traces", []string{"vl-logs", "vl-traces", "vt-traces"}},
	} {
		for _, p := range patchListProblems(c.file, want, dockerfilePatchLists(readRepoFile(t, root, c.file)), c.dirs) {
			t.Error(p)
		}
	}
}

// TestPatchListProblems proves the comparison above: it fails on the defect it
// exists for (a patch left out of one tree), on an extra patch, a reordered
// list, a Dockerfile building a directory it should not, and a Makefile it can
// no longer read.
func TestPatchListProblems(t *testing.T) {
	makefile := "deps-logs:\n" +
		"\tcd $(VL_DIR_LOGS) && git apply ../../patches/vl-logs/a.patch\n" +
		"\tcd $(VL_DIR_LOGS) && git apply ../../patches/vl-logs/b.patch\n" +
		"deps-traces:\n" +
		"\tcd $(VL_DIR_TRACES) && git apply ../../../patches/vl-traces/a.patch\n" +
		"\tcd $(VL_DIR_TRACES) && git apply ../../../patches/vl-traces/c.patch\n"
	logsDocker := "COPY patches/vl-logs/ /tmp/patches/\n" +
		"RUN cd deps/VictoriaLogs && git apply /tmp/patches/a.patch && \\\n    git apply /tmp/patches/b.patch\n"
	tracesDocker := "COPY patches/ /tmp/patches/\n" +
		"RUN cd deps/VictoriaLogs && git apply /tmp/patches/vl-logs/a.patch && \\\n    git apply /tmp/patches/vl-logs/b.patch\n" +
		"RUN cd lakehouse-traces/deps/VictoriaLogs && git apply /tmp/patches/vl-traces/a.patch && \\\n    git apply /tmp/patches/vl-traces/c.patch\n"

	want := makefilePatchLists(makefile)
	if got := strings.Join(want["vl-logs"], ","); got != "a.patch,b.patch" {
		t.Fatalf("Makefile vl-logs list = %q", got)
	}
	if got := strings.Join(want["vl-traces"], ","); got != "a.patch,c.patch" {
		t.Fatalf("Makefile vl-traces list = %q", got)
	}
	if got := strings.Join(dockerfilePatchLists(logsDocker)["vl-logs"], ","); got != "a.patch,b.patch" {
		t.Fatalf("Dockerfile.logs list (single-directory COPY) = %q", got)
	}

	logsDirs, tracesDirs := []string{"vl-logs"}, []string{"vl-logs", "vl-traces"}
	cases := []struct {
		name    string
		file    string
		body    string
		dirs    []string
		wantSub string // "" = no problem expected
	}{
		{"matching logs image", "Dockerfile.logs", logsDocker, logsDirs, ""},
		{"matching traces image", "Dockerfile.traces", tracesDocker, tracesDirs, ""},
		{"patch left out of the logs image", "Dockerfile.logs",
			strings.Replace(logsDocker, " && \\\n    git apply /tmp/patches/b.patch", "", 1), logsDirs,
			"does not apply patches/vl-logs/b.patch"},
		{"patch left out of the traces VictoriaLogs tree", "Dockerfile.traces",
			strings.Replace(tracesDocker, " && \\\n    git apply /tmp/patches/vl-traces/c.patch", "", 1), tracesDirs,
			"does not apply patches/vl-traces/c.patch"},
		{"patch only the Dockerfile applies", "Dockerfile.logs",
			logsDocker + "RUN git apply /tmp/patches/z.patch\n", logsDirs,
			"applies patches/vl-logs/z.patch, which `make deps` does not apply"},
		{"patches in another order", "Dockerfile.traces",
			strings.NewReplacer("vl-logs/a.patch", "vl-logs/b.patch", "vl-logs/b.patch", "vl-logs/a.patch").Replace(tracesDocker), tracesDirs,
			"in the order"},
		{"directory the image does not build", "Dockerfile.logs",
			tracesDocker, logsDirs,
			"applies patches from patches/vl-traces, which it does not build"},
		{"nothing applied", "Dockerfile.logs", "FROM scratch\n", logsDirs,
			"does not apply patches/vl-logs/a.patch"},
	}
	for _, c := range cases {
		problems := patchListProblems(c.file, want, dockerfilePatchLists(c.body), c.dirs)
		joined := strings.Join(problems, "\n")
		if c.wantSub == "" {
			if len(problems) != 0 {
				t.Errorf("%s: unexpected problems: %s", c.name, joined)
			}
			continue
		}
		if !strings.Contains(joined, c.wantSub) {
			t.Errorf("%s: want a problem containing %q, got %q", c.name, c.wantSub, joined)
		}
	}

	// A Makefile the parse cannot read must not pass vacuously.
	if p := patchListProblems("Dockerfile.logs", makefilePatchLists("all:\n\ttrue\n"), dockerfilePatchLists(logsDocker), logsDirs); len(p) == 0 ||
		!strings.Contains(p[0], "vacuously") {
		t.Errorf("an unreadable Makefile must be reported, got %v", p)
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
// or without quotes: `  VL_VERSION_LOGS: v1.52.0`, `  VT_VERSION: "v0.12.0"`.
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

// upstreamProbeDockerfile wraps the distroless upstream images with one static
// busybox, so the Compose healthchecks have a binary to execute.
const upstreamProbeDockerfile = "deployment/docker/Dockerfile.upstream-probe"

// upstreamProbeBase is the image that busybox is copied from, pinned by tag AND
// digest. The tag names a release a reader can look up; the digest makes the
// bytes immutable, so a re-pushed tag cannot swap the binary that runs inside
// every hot-tier container's healthcheck. To move it, resolve the new index
// digest (`docker buildx imagetools inspect busybox:<tag>`, the "Digest:" line
// of the index, not a per-platform manifest) and change the Dockerfile and this
// constant in the same commit.
const upstreamProbeBase = "busybox:1.37.0-uclibc@sha256:8d7b1636e974e0adfd8d945955fca609304f0a56c18799dfd032d6e661382d84"

var (
	dockerFromRe = regexp.MustCompile(`(?m)^FROM\s+(\S+)`)

	// digestPinnedRe accepts `[registry/][path/]name:tag@sha256:<64 hex>`. The
	// tag must sit on the last path component, so a registry port
	// (`registry:5000/busybox@sha256:...`) is not mistaken for one.
	digestPinnedRe = regexp.MustCompile(`^(?:[^@\s/]+/)*[^@\s/:]+:[^@\s/:]+@sha256:[0-9a-f]{64}$`)
)

// TestUpstreamProbeBaseImagePinnedByDigest keeps the healthcheck wrapper's own
// base image as fixed as the upstream releases it wraps. `FROM ${UPSTREAM}` is
// the release under test and is pinned through the Compose build arguments
// (TestComposeImagePinsMatchMakefile); every other FROM must be pinned by tag and
// digest, and must be exactly upstreamProbeBase.
func TestUpstreamProbeBaseImagePinnedByDigest(t *testing.T) {
	root, err := inventory.RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	body := readRepoFile(t, root, upstreamProbeDockerfile)

	var bases []string
	for _, m := range dockerFromRe.FindAllStringSubmatch(body, -1) {
		ref := m[1]
		if strings.HasPrefix(ref, "$") {
			continue
		}
		bases = append(bases, ref)
		if !digestPinnedRe.MatchString(ref) {
			t.Errorf("%s: FROM %s is not pinned by tag and digest (want name:tag@sha256:<64 hex>) — a re-pushed tag would change the healthcheck binary without a diff here", upstreamProbeDockerfile, ref)
		}
	}
	if len(bases) == 0 {
		t.Fatalf("%s has no fixed base image — either the busybox stage was removed (then remove this test too) or the FROM regexp is stale", upstreamProbeDockerfile)
	}
	if len(bases) != 1 || bases[0] != upstreamProbeBase {
		t.Errorf("%s copies its probe binary from %v, want exactly [%s] — move the Dockerfile and upstreamProbeBase together", upstreamProbeDockerfile, bases, upstreamProbeBase)
	}
}

// TestDigestPinnedRe pins the shape the digest gate accepts, so loosening the
// regexp cannot quietly let a tag-only or digest-only reference through.
func TestDigestPinnedRe(t *testing.T) {
	const digest = "sha256:8d7b1636e974e0adfd8d945955fca609304f0a56c18799dfd032d6e661382d84"
	cases := []struct {
		ref  string
		want bool
	}{
		{"busybox:1.37.0-uclibc@" + digest, true},
		{"docker.io/library/busybox:1.37.0-uclibc@" + digest, true},
		{"registry.example:5000/team/busybox:1.37.0@" + digest, true},
		{"busybox:1.37.0-uclibc", false},                                                                  // tag only: mutable
		{"busybox@" + digest, false},                                                                      // digest only: unreadable
		{"registry.example:5000/busybox@" + digest, false},                                                // a port is not a tag
		{"busybox:1.37.0@sha256:8d7b1636", false},                                                         // truncated digest
		{"busybox:1.37.0@sha256:8D7B1636E974E0ADFD8D945955FCA609304F0A56C18799DFD032D6E661382D84", false}, // digests are lowercase hex
		{"busybox:1.37.0@sha512:" + digest[len("sha256:"):], false},
	}
	for _, c := range cases {
		if got := digestPinnedRe.MatchString(c.ref); got != c.want {
			t.Errorf("digestPinnedRe.MatchString(%q) = %v, want %v", c.ref, got, c.want)
		}
	}
}
