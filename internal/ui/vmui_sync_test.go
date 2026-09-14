package ui

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"
)

// vmui is VictoriaLogs' own build output, copied into internal/ui/vmui/ by
// `make sync-vmui` (and by the same inline copy in Dockerfile.logs:30 /
// Dockerfile.traces:50) and served from there through go:embed. Only
// index.html is tracked in git; it names the content-hashed asset filenames,
// which makes it the drift marker for the whole bundle. When an upstream bump
// rebuilds vmui, every hash in index.html changes, and the tests below fail
// until `make sync-vmui` is re-run and index.html re-committed — so a stale
// embedded UI can never ship silently alongside a newer VictoriaLogs.

// vmuiAssetRefRe matches the asset paths index.html references, e.g.
//
//	<script type="module" crossorigin src="./assets/index-Ds1HKnSd.js"></script>
//	<link rel="stylesheet" crossorigin href="./assets/index-I_gKSIT2.css">
var vmuiAssetRefRe = regexp.MustCompile(`\./(assets/[A-Za-z0-9._\-]+)`)

// moduleLineRe matches the lakehouse module's own module declaration exactly,
// so walking up never stops at the traces module's go.mod (a different module
// further down the same tree). Same rule as
// tests/conformance/inventory.RepoRoot.
var moduleLineRe = regexp.MustCompile(`(?m)^module github\.com/ReliablyObserve/victoria-lakehouse$`)

// repoRoot walks up from the working directory to the lakehouse go.mod.
func repoRoot(t *testing.T) string {
	t.Helper()
	dir, err := os.Getwd()
	if err != nil {
		t.Fatal(err)
	}
	for {
		if data, err := os.ReadFile(filepath.Join(dir, "go.mod")); err == nil && moduleLineRe.Match(data) {
			return dir
		}
		parent := filepath.Dir(dir)
		if parent == dir {
			t.Fatalf("lakehouse go.mod not found above %s", dir)
		}
		dir = parent
	}
}

// depsRequired reports whether a missing deps/ tree must fail rather than
// skip. Mirrors tests/conformance (CONFORMANCE_REQUIRE_DEPS=1, set by
// `make conformance-check`) and additionally requires the vendored tree on
// any CI runner, where `make deps-logs` always runs before the tests: a
// silent skip there would let a stale vmui through the gate.
func depsRequired() bool {
	return os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" || os.Getenv("CI") != ""
}

// vendoredVMUIDir returns deps/VictoriaLogs/app/vlselect/vmui, skipping the
// test when the vendored tree is absent and skipping is allowed.
func vendoredVMUIDir(t *testing.T) string {
	t.Helper()
	dir := filepath.Join(repoRoot(t), "deps", "VictoriaLogs", "app", "vlselect", "vmui")
	if _, err := os.Stat(dir); err != nil {
		if os.IsNotExist(err) && !depsRequired() {
			t.Skipf("%s missing — run make deps-logs (set CONFORMANCE_REQUIRE_DEPS=1 to fail instead)", dir)
		}
		t.Fatalf("vendored vmui tree unusable: %v", err)
	}
	return dir
}

// TestVMUIIndexMatchesVendoredVL is the drift gate: the index.html compiled
// into the binary must be byte-identical to the one in the vendored
// VictoriaLogs tree the Makefile pins. Any upstream vmui rebuild (new asset
// hashes, new meta tags, a restructured <head>) fails here first, before it
// can reach the injector or the e2e stack.
func TestVMUIIndexMatchesVendoredVL(t *testing.T) {
	vendored := vendoredVMUIDir(t)

	want, err := os.ReadFile(filepath.Join(vendored, "index.html"))
	if err != nil {
		t.Fatalf("read vendored index.html: %v", err)
	}
	got, err := vmuiFiles.ReadFile("vmui/index.html")
	if err != nil {
		t.Fatalf("read embedded vmui/index.html: %v", err)
	}
	if string(got) != string(want) {
		t.Fatalf("embedded internal/ui/vmui/index.html differs from %s/index.html\n"+
			"the vendored VictoriaLogs shipped a new vmui build — run `make sync-vmui` and commit internal/ui/vmui/index.html\n"+
			"embedded sha256=%s vendored sha256=%s", vendored, sum(got), sum(want))
	}
}

// TestVMUIAssetsMatchVendoredVL checks the other half of the bundle: every
// content-hashed asset index.html references must exist in the vendored tree,
// and — when the working copy has been synced (the assets are .gitignore'd, so
// they are only present after `make sync-vmui` or inside the Docker builder) —
// must be byte-identical to it. A referenced asset that is missing upstream
// means index.html and the asset directory came from different VL builds.
func TestVMUIAssetsMatchVendoredVL(t *testing.T) {
	vendored := vendoredVMUIDir(t)

	index, err := vmuiFiles.ReadFile("vmui/index.html")
	if err != nil {
		t.Fatalf("read embedded vmui/index.html: %v", err)
	}
	refs := map[string]bool{}
	for _, m := range vmuiAssetRefRe.FindAllStringSubmatch(string(index), -1) {
		refs[m[1]] = true
	}
	if len(refs) == 0 {
		t.Fatal("index.html references no ./assets/* files — the reference regexp or upstream's HTML layout changed")
	}

	names := make([]string, 0, len(refs))
	for n := range refs {
		names = append(names, n)
	}
	sort.Strings(names)

	comparedEmbedded := 0
	for _, name := range names {
		vendoredBytes, err := os.ReadFile(filepath.Join(vendored, filepath.FromSlash(name)))
		if err != nil {
			t.Errorf("index.html references %s but the vendored tree does not have it (%v) — index.html and assets/ are from different VictoriaLogs builds; run `make sync-vmui`", name, err)
			continue
		}
		embedded, err := vmuiFiles.ReadFile("vmui/" + name)
		if err != nil {
			// Assets are .gitignore'd; absent means "not synced in this
			// working copy", which is a legitimate state for `go test` on a
			// fresh checkout. The Docker builds and `make build-logs` both
			// sync first, so the shipped binary always has them.
			continue
		}
		comparedEmbedded++
		if string(embedded) != string(vendoredBytes) {
			t.Errorf("embedded vmui asset %s differs from the vendored tree (embedded sha256=%s vendored sha256=%s) — run `make sync-vmui`", name, sum(embedded), sum(vendoredBytes))
		}
	}

	if comparedEmbedded == 0 {
		t.Logf("vmui assets not present in this working copy (they are .gitignore'd); checked %d references against %s only. Run `make sync-vmui` to compare content too.", len(names), vendored)
	}
}

// TestVMUIEmbeddedTreeHasNoStaleAssets guards the copy direction the Makefile
// target and the Dockerfiles use: internal/ui/vmui/ must never accumulate
// files the vendored tree no longer ships. Without the wipe in `sync-vmui`,
// every VictoriaLogs bump would leave the previous build's hashed bundle
// behind and the embedded FS (and the image) would grow forever.
func TestVMUIEmbeddedTreeHasNoStaleAssets(t *testing.T) {
	vendored := vendoredVMUIDir(t)

	var embedded []string
	err := fs.WalkDir(vmuiFiles, "vmui", func(p string, d fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		embedded = append(embedded, strings.TrimPrefix(p, "vmui/"))
		return nil
	})
	if err != nil {
		t.Fatalf("walk embedded vmui: %v", err)
	}

	for _, name := range embedded {
		if _, err := os.Stat(filepath.Join(vendored, filepath.FromSlash(name))); err != nil {
			t.Errorf("embedded vmui carries %s, which the vendored VictoriaLogs tree does not ship — a stale file from an older VL build; run `make sync-vmui`", name)
		}
	}
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8])
}
