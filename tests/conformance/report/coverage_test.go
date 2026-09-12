package report

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

func TestRenderCoverage(t *testing.T) {
	inv := &inventory.Inventory{VLVersion: "v1.50.0", VTVersion: "v0.9.2", Items: []inventory.Item{
		{Kind: "route", Name: "/select/logsql/query", Source: "app/vlselect/main.go"},
		{Kind: "flag", Name: "search.maxQueueDuration", Source: "app/vlselect/main.go", Linked: false},
	}}
	reg, err := registry.LoadDir("../registry/testdata/valid")
	if err != nil {
		t.Fatal(err)
	}
	md := RenderCoverage(inv, reg)
	for _, w := range []string{"GENERATED", "v1.50.0", "/select/logsql/query", "vl.select.query.wildcard", "✅", "search.maxQueueDuration", "not linked", "lh.stats.overview.schema", "🧩"} {
		if !strings.Contains(md, w) {
			t.Fatalf("coverage doc missing %q:\n%s", w, md)
		}
	}
}

// TestRenderCoverage_HeaderShowsTracesPin covers the header branch that
// prints the traces module's separate VictoriaLogs commit pin.
func TestRenderCoverage_HeaderShowsTracesPin(t *testing.T) {
	inv := &inventory.Inventory{VLVersion: "v1.50.0", VTVersion: "v0.9.2", VLCommitTraces: "77df0c04d532"}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	md := RenderCoverage(inv, reg)
	if !strings.Contains(md, "77df0c04d532") {
		t.Fatalf("expected the traces VL commit pin in the header:\n%s", md)
	}
}

// TestIcon exercises every branch of icon() directly, including the ones the
// hand-crafted testdata/valid fixture used by TestRenderCoverage does not
// naturally reach (absent, unsupported, differ, pending).
func TestIcon(t *testing.T) {
	cases := []struct {
		name string
		row  *registry.Row
		want string
	}{
		{"nil row", nil, "⚪ no row"},
		{"absent", &registry.Row{Expect: registry.ExpectAbsent}, "⛔ absent upstream and on LH"},
		{"unsupported", &registry.Row{Expect: registry.ExpectUnsupported}, "⛔ unsupported on cold (documented)"},
		{"differ", &registry.Row{Expect: registry.ExpectDiffer, DifferNote: "cold lacks tail"}, "🔁 differs: cold lacks tail"},
		{"pending", &registry.Row{Expect: registry.ExpectPass, Pending: true}, "🟡 declared, not yet executed"},
		{"lh addition", &registry.Row{Expect: registry.ExpectPass, Origin: registry.OriginLHAddition}, "🧩 LH addition"},
		{"native verified", &registry.Row{Expect: registry.ExpectPass, Origin: registry.OriginNative}, "✅ native, verified"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if got := icon(c.row); got != c.want {
				t.Fatalf("icon() = %q, want %q", got, c.want)
			}
		})
	}
}

// TestRenderCoverage_LinkedFlagNoRow covers the flag-linked branch of
// RenderCoverage (a linked flag without a row keeps the plain icon, no
// "not linked" suffix) and the multi-row Rows column (comma-joined ids).
func TestRenderCoverage_LinkedFlagNoRow(t *testing.T) {
	inv := &inventory.Inventory{VLVersion: "v1.50.0", VTVersion: "v0.9.2", Items: []inventory.Item{
		{Kind: "flag", Name: "search.maxConcurrentRequests", Source: "app/vlselect/main.go", Linked: true},
	}}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	md := RenderCoverage(inv, reg)
	if strings.Contains(md, "not linked") {
		t.Fatalf("linked flag should not be marked 'not linked':\n%s", md)
	}
	if !strings.Contains(md, "⚪ no row") {
		t.Fatalf("linked flag with no row should show the plain no-row icon:\n%s", md)
	}
}

// TestRenderCoverage_EmptyInventory covers the empty-inventory path (no
// per-kind sections rendered) and an inventory with no LH additions.
func TestRenderCoverage_EmptyInventory(t *testing.T) {
	inv := &inventory.Inventory{VLVersion: "v1.50.0", VTVersion: "v0.9.2"}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	md := RenderCoverage(inv, reg)
	if !strings.Contains(md, "Lakehouse additions") {
		t.Fatalf("expected the Lakehouse additions header even with no rows:\n%s", md)
	}
	for _, kind := range []string{"## Upstream route", "## Upstream pipe", "## Upstream filter", "## Upstream stats", "## Upstream traceql", "## Upstream flag"} {
		if strings.Contains(md, kind) {
			t.Fatalf("empty inventory should not render a %q section:\n%s", kind, md)
		}
	}
}
