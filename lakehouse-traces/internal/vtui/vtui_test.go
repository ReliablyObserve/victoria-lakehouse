package vtui

import (
	"crypto/sha256"
	"encoding/hex"
	"io/fs"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/ui"
)

// VTUI is VictoriaTraces' own build output (VictoriaTraces v0.12.0 replaced the
// log-based UI with it), copied into internal/vtui/vmui/ by `make
// sync-vmui-traces` (and by the inline copy in Dockerfile.traces). Only
// index.html is tracked; it names the content-hashed asset files, which makes it
// the drift marker for the whole bundle. Twin of internal/ui/vmui_sync_test.go,
// against the traces module's vendored VictoriaTraces instead of the logs
// module's vendored VictoriaLogs.

// vendoredDir is lakehouse-traces/deps/VictoriaTraces/app/vtselect/vmui.
var vendoredDir = filepath.Join("..", "..", "deps", "VictoriaTraces", "app", "vtselect", "vmui")

var assetRefRe = regexp.MustCompile(`\./(assets/[A-Za-z0-9._\-]+)`)

// depsRequired mirrors internal/ui and tests/conformance: a missing vendored tree
// fails on CI (where `make deps-vt` always runs first) instead of skipping.
func depsRequired() bool {
	return os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" || os.Getenv("CI") != ""
}

func requireVendored(t *testing.T) string {
	t.Helper()
	if _, err := os.Stat(vendoredDir); err != nil {
		if os.IsNotExist(err) && !depsRequired() {
			t.Skipf("%s missing — run make deps-vt (set CONFORMANCE_REQUIRE_DEPS=1 to fail instead)", vendoredDir)
		}
		t.Fatalf("vendored VTUI tree unusable: %v", err)
	}
	return vendoredDir
}

func sum(b []byte) string {
	h := sha256.Sum256(b)
	return hex.EncodeToString(h[:8])
}

func TestVTUIIndexMatchesVendoredVT(t *testing.T) {
	dir := requireVendored(t)
	want, err := os.ReadFile(filepath.Join(dir, "index.html"))
	if err != nil {
		t.Fatal(err)
	}
	got, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatal(err)
	}
	if string(got) != string(want) {
		t.Fatalf("embedded internal/vtui/vmui/index.html differs from %s/index.html\n"+
			"the vendored VictoriaTraces shipped a new UI build — run `make sync-vmui-traces` and commit internal/vtui/vmui/index.html\n"+
			"embedded sha256=%s vendored sha256=%s", dir, sum(got), sum(want))
	}
}

func TestVTUIAssetsMatchVendoredVT(t *testing.T) {
	dir := requireVendored(t)
	index, err := fs.ReadFile(FS(), "index.html")
	if err != nil {
		t.Fatal(err)
	}
	refs := map[string]bool{}
	for _, m := range assetRefRe.FindAllStringSubmatch(string(index), -1) {
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

	compared := 0
	for _, name := range names {
		vendored, err := os.ReadFile(filepath.Join(dir, filepath.FromSlash(name)))
		if err != nil {
			t.Errorf("index.html references %s but the vendored tree does not have it (%v) — index.html and assets/ are from different VictoriaTraces builds; run `make sync-vmui-traces`", name, err)
			continue
		}
		embedded, err := fs.ReadFile(FS(), name)
		if err != nil {
			continue // assets are .gitignore'd: absent means "not synced in this working copy"
		}
		compared++
		if string(embedded) != string(vendored) {
			t.Errorf("embedded VTUI asset %s differs from the vendored tree (embedded sha256=%s vendored sha256=%s) — run `make sync-vmui-traces`", name, sum(embedded), sum(vendored))
		}
	}
	if compared == 0 {
		t.Logf("VTUI assets not present in this working copy (they are .gitignore'd); checked %d references against %s only. Run `make sync-vmui-traces` to compare content too.", len(names), dir)
	}
}

// The embedded tree must never accumulate files the vendored tree no longer
// ships: without the wipe in `sync-vmui-traces`, every bump would leave the
// previous build's hashed bundle behind.
func TestVTUIEmbeddedTreeHasNoStaleAssets(t *testing.T) {
	dir := requireVendored(t)
	err := fs.WalkDir(FS(), ".", func(p string, d fs.DirEntry, err error) error {
		if err != nil || d.IsDir() {
			return err
		}
		if _, err := os.Stat(filepath.Join(dir, filepath.FromSlash(p))); err != nil {
			t.Errorf("embedded VTUI carries %s, which the vendored VictoriaTraces tree does not ship — a stale file from an older build; run `make sync-vmui-traces`", p)
		}
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
}

// The traces binary serves VictoriaTraces' UI, not VictoriaLogs', at
// /select/vmui/ — with the Lakehouse tab injected and the bundle untouched.
func TestServesVTUIWithLakehouseTab(t *testing.T) {
	mux := http.NewServeMux()
	ui.RegisterVMUIFS(mux, true, FS())

	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/select/vmui/", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /select/vmui/ = %d, want 200", rec.Code)
	}
	body := rec.Body.String()
	if !strings.Contains(body, "UI for VictoriaTraces") {
		t.Errorf("/select/vmui/ does not serve VictoriaTraces' UI: %.300s", body)
	}
	if strings.Contains(body, "UI for VictoriaLogs") {
		t.Errorf("/select/vmui/ still serves the log-based UI VictoriaTraces v0.12.0 replaced")
	}
	if !strings.Contains(body, `<script src="/lakehouse/ui/vmui-tab.js"></script>`) {
		t.Errorf("the Lakehouse tab script is not injected into the VTUI index: %.300s", body)
	}

	// The no-slash form redirects to the bundle.
	rec = httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/select/vmui", nil))
	if rec.Code != http.StatusMovedPermanently || rec.Header().Get("Location") != "/select/vmui/" {
		t.Errorf("GET /select/vmui = %d Location=%q, want a redirect to /select/vmui/", rec.Code, rec.Header().Get("Location"))
	}

	// Every asset the index names is served unmodified (skipped when the
	// working copy has not been synced: assets are .gitignore'd).
	index, _ := fs.ReadFile(FS(), "index.html")
	for _, m := range assetRefRe.FindAllStringSubmatch(string(index), -1) {
		want, err := fs.ReadFile(FS(), m[1])
		if err != nil {
			continue
		}
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/select/vmui/"+m[1], nil))
		if rec.Code != http.StatusOK || rec.Body.String() != string(want) {
			t.Errorf("GET /select/vmui/%s = %d, want the bundle file unmodified", m[1], rec.Code)
		}
	}
}
