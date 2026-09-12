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

// TestAggregateStatus_WorstOfPassAndUnsupported proves a passing row can
// never hide an unsupported sibling that covers the same upstream item:
// the aggregate status must be the worse of the two, not whichever row
// happens to be first.
func TestAggregateStatus_WorstOfPassAndUnsupported(t *testing.T) {
	pass := &registry.Row{ID: "vl.select.tail.hot", Expect: registry.ExpectPass}
	unsupported := &registry.Row{ID: "vl.select.tail.unsupported", Expect: registry.ExpectUnsupported}

	for _, rows := range [][]*registry.Row{{pass, unsupported}, {unsupported, pass}} {
		got := aggregateStatus(rows)
		if !strings.Contains(got, "⛔ unsupported on cold (documented)") {
			t.Fatalf("aggregateStatus(%v) = %q, want the unsupported icon regardless of row order", rows, got)
		}
		if strings.Contains(got, "✅") {
			t.Fatalf("aggregateStatus(%v) = %q must not also claim verified", rows, got)
		}
	}
}

// TestAggregateStatus_PendingOnlyAnnotatesWithoutHiding proves that when
// every covering row is still Pending, the note is appended to the
// worst-of status rather than replacing it — so a differ/unsupported
// status is not disguised as "declared, not yet executed" with no
// indication of what it actually declares.
func TestAggregateStatus_PendingOnlyAnnotatesWithoutHiding(t *testing.T) {
	rows := []*registry.Row{
		{ID: "vl.select.tail.hot", Expect: registry.ExpectPass, Pending: true},
		{ID: "vl.select.tail.hot2", Expect: registry.ExpectPass, Pending: true},
	}
	got := aggregateStatus(rows)
	if !strings.Contains(got, "✅") || !strings.Contains(got, "(declared, not yet executed)") {
		t.Fatalf("aggregateStatus(all pending) = %q, want the verified icon annotated with the pending note", got)
	}

	// One executed row among the covering rows must suppress the note.
	mixed := []*registry.Row{
		{ID: "vl.select.tail.hot", Expect: registry.ExpectPass, Pending: true},
		{ID: "vl.select.tail.hot2", Expect: registry.ExpectPass, Pending: false},
	}
	got = aggregateStatus(mixed)
	if strings.Contains(got, "declared, not yet executed") {
		t.Fatalf("aggregateStatus(mixed pending) = %q, must not append the note when one row already ran", got)
	}
}

// TestAggregateStatus_AllExpectRanks exercises every expectRank/expectIcon
// branch (differ and absent) via aggregateStatus, and confirms absent
// outranks differ as the worst status.
func TestAggregateStatus_AllExpectRanks(t *testing.T) {
	differ := &registry.Row{ID: "vt.tempo.tags.differ", Expect: registry.ExpectDiffer, DifferNote: "scope=intrinsic missing"}
	if got := aggregateStatus([]*registry.Row{differ}); !strings.Contains(got, "🔁 differs: scope=intrinsic missing") {
		t.Fatalf("aggregateStatus(differ) = %q", got)
	}
	absent := &registry.Row{ID: "vt.tempo.metrics_instant.absent", Expect: registry.ExpectAbsent}
	if got := aggregateStatus([]*registry.Row{absent}); !strings.Contains(got, "⛔ absent upstream and on LH") {
		t.Fatalf("aggregateStatus(absent) = %q", got)
	}
	if got := aggregateStatus([]*registry.Row{differ, absent}); !strings.Contains(got, "⛔ absent upstream and on LH") {
		t.Fatalf("aggregateStatus(differ, absent) = %q, want absent to outrank differ", got)
	}
}

// TestCoveredKeys_RoutePrefixExpansion covers the four routes the higher-tier
// review flagged as disagreeing between the drift check (which treats them
// as prefix-covered) and the coverage doc (which showed "no row"):
// /insert/{datadog,journald,loki,splunk}/.
func TestCoveredKeys_RoutePrefixExpansion(t *testing.T) {
	inv := &inventory.Inventory{Items: []inventory.Item{
		{Kind: "route", Name: "/insert/datadog/"},
		{Kind: "route", Name: "/insert/journald/"},
		{Kind: "route", Name: "/insert/loki/"},
		{Kind: "route", Name: "/insert/splunk/"},
	}}
	reg := &registry.Registry{Rows: []registry.Row{
		{ID: "vl.insert.datadog_logs.count", Upstream: &registry.Upstream{Route: "/insert/datadog/api/v2/logs"}},
		{ID: "vl.insert.journald.count", Upstream: &registry.Upstream{Route: "/insert/journald/upload"}},
		{ID: "vl.insert.loki_push_json.count", Upstream: &registry.Upstream{Route: "/insert/loki/api/v1/push"}},
		{ID: "vl.insert.splunk_event.count", Upstream: &registry.Upstream{Route: "/insert/splunk/services/collector/event"}},
	}}
	reg.ByID = map[string]*registry.Row{}
	for i := range reg.Rows {
		reg.ByID[reg.Rows[i].ID] = &reg.Rows[i]
	}
	covered := CoveredKeys(inv, reg)
	for _, prefix := range []string{"/insert/datadog/", "/insert/journald/", "/insert/loki/", "/insert/splunk/"} {
		if len(covered["route:"+prefix]) == 0 {
			t.Fatalf("route:%s should be covered by prefix expansion, got: %v", prefix, covered)
		}
	}

	md := RenderCoverage(inv, reg)
	if strings.Contains(md, "⚪ no row") {
		t.Fatalf("all four prefix routes should be covered, found an uncovered row:\n%s", md)
	}
	if !strings.Contains(md, "route: 4/4 covered") {
		t.Fatalf("expected all 4 prefix routes counted as covered in the totals line:\n%s", md)
	}
}

// TestCoveredKeys_NoSelfMatchDuplicate proves a row whose own upstream
// route IS a trailing-slash inventory route (e.g. a row citing
// /select/tempo/api/v2/search/tag/ when that exact route, with the
// trailing slash, is also the inventory item) is listed once, not twice
// (once as an exact match, once again via prefix self-expansion).
func TestCoveredKeys_NoSelfMatchDuplicate(t *testing.T) {
	inv := &inventory.Inventory{Items: []inventory.Item{
		{Kind: "route", Name: "/select/tempo/api/v2/search/tag/"},
	}}
	reg := &registry.Registry{Rows: []registry.Row{
		{ID: "vt.tempo.v2_search_tag_values.basic", Upstream: &registry.Upstream{Route: "/select/tempo/api/v2/search/tag/"}},
	}}
	ids := CoveredKeys(inv, reg)["route:/select/tempo/api/v2/search/tag/"]
	if len(ids) != 1 {
		t.Fatalf("expected exactly one covering id, got %v", ids)
	}
}

// TestRenderCoverage_GatedSection covers the trailing "gated on a later
// upstream version / absent by design" section: a since-gated differ row
// and an absent row, neither of whose upstream key is in the inventory.
func TestRenderCoverage_GatedSection(t *testing.T) {
	inv := &inventory.Inventory{VLVersion: "v1.50.0", VTVersion: "v0.9.2"}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	reg.Rows = []registry.Row{
		{
			ID: "vl.pipe.json_array_concat.basic", Expect: registry.ExpectDiffer,
			DifferNote: "ships in VL 1.52.0", Since: map[string]string{"vl": "1.52.0"},
			Upstream: &registry.Upstream{Pipe: "json_array_concat"},
		},
		{
			ID: "vt.tempo.metrics_instant.absent", Expect: registry.ExpectAbsent,
			Upstream: &registry.Upstream{Route: "/select/tempo/api/metrics/instant"},
		},
	}
	md := RenderCoverage(inv, reg)
	if !strings.Contains(md, "Rows gated on a later upstream version") {
		t.Fatalf("expected the gated/absent section header:\n%s", md)
	}
	for _, want := range []string{"vl.pipe.json_array_concat.basic", "vl 1.52.0", "ships in VL 1.52.0", "vt.tempo.metrics_instant.absent", "absent"} {
		if !strings.Contains(md, want) {
			t.Fatalf("gated/absent section missing %q:\n%s", want, md)
		}
	}
}

// TestRenderCoverage_PreambleAndLegend covers the static preamble and icon
// legend printed at the top of the generated doc.
func TestRenderCoverage_PreambleAndLegend(t *testing.T) {
	inv := &inventory.Inventory{VLVersion: "v1.50.0", VTVersion: "v0.9.2"}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	md := RenderCoverage(inv, reg)
	for _, want := range []string{"never re-implements", "patches/README.md", "Legend:", "✅", "🟡", "🔁", "⛔", "⚪", "🧩"} {
		if !strings.Contains(md, want) {
			t.Fatalf("preamble/legend missing %q:\n%s", want, md)
		}
	}
}

// TestRenderCoverage_PerKindTotals covers the per-kind "N/M covered" totals
// line printed under each table heading.
func TestRenderCoverage_PerKindTotals(t *testing.T) {
	inv := &inventory.Inventory{Items: []inventory.Item{
		{Kind: "route", Name: "/select/logsql/query"},
		{Kind: "route", Name: "/select/logsql/new_thing"},
	}}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	reg.Rows = []registry.Row{{ID: "vl.select.query.basic", Upstream: &registry.Upstream{Route: "/select/logsql/query"}}}
	md := RenderCoverage(inv, reg)
	if !strings.Contains(md, "route: 1/2 covered") {
		t.Fatalf("expected a '1/2 covered' totals line:\n%s", md)
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
