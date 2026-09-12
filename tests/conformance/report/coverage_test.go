package report

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

func TestRenderCoverage(t *testing.T) {
	inv := &inventory.Inventory{VLVersion: "v1.50.0", VTVersion: "v0.9.2", Items: []inventory.Item{
		{Kind: "route", Surface: "vl", Name: "/select/logsql/query", Source: "app/vlselect/main.go"},
		{Kind: "flag", Surface: "vl", Name: "search.maxQueueDuration", Source: "app/vlselect/main.go", Linked: false},
	}}
	reg, err := registry.LoadDir("../registry/testdata/valid")
	if err != nil {
		t.Fatal(err)
	}
	md := RenderCoverage(inv, reg)
	for _, w := range []string{"GENERATED", "v1.50.0", "/select/logsql/query", "vl.select.query.wildcard", "✅", "search.maxQueueDuration", "not linked", "lh.stats.overview.schema", "🧩", "route: 1/1 covered"} {
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
		{Kind: "flag", Surface: "vl", Name: "search.maxConcurrentRequests", Source: "app/vlselect/main.go", Linked: true},
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
// every covering row is still Pending and the worst expectation is pass,
// the status is exactly the plain "declared, not yet executed" note (there
// is no verified/differ/unsupported/absent claim to make yet); when the
// worst expectation is worse than pass, the note is appended to that
// expectation's status instead of replacing it, so a differ/unsupported
// status is not disguised as "declared, not yet executed" with no
// indication of what it actually declares.
func TestAggregateStatus_PendingOnlyAnnotatesWithoutHiding(t *testing.T) {
	rows := []*registry.Row{
		{ID: "vl.select.tail.hot", Expect: registry.ExpectPass, Pending: true},
		{ID: "vl.select.tail.hot2", Expect: registry.ExpectPass, Pending: true},
	}
	got := aggregateStatus(rows)
	if got != "🟡 declared, not yet executed" {
		t.Fatalf("aggregateStatus(all pending, pass) = %q, want the plain pending note", got)
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

	// All pending with a worse-than-pass worst expectation still appends
	// the note rather than replacing the status.
	pendingUnsupported := []*registry.Row{
		{ID: "vl.select.tail.unsupported", Expect: registry.ExpectUnsupported, Pending: true},
	}
	got = aggregateStatus(pendingUnsupported)
	if !strings.Contains(got, "⛔ unsupported on cold (documented)") || !strings.Contains(got, "(declared, not yet executed)") {
		t.Fatalf("aggregateStatus(pending unsupported) = %q, want the unsupported icon annotated with the pending note", got)
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
		{Kind: "route", Surface: "vl", Name: "/insert/datadog/"},
		{Kind: "route", Surface: "vl", Name: "/insert/journald/"},
		{Kind: "route", Surface: "vl", Name: "/insert/loki/"},
		{Kind: "route", Surface: "vl", Name: "/insert/splunk/"},
	}}
	reg := &registry.Registry{Rows: []registry.Row{
		{ID: "vl.insert.datadog_logs.count", Surface: registry.SurfaceVL, Upstream: &registry.Upstream{Route: "/insert/datadog/api/v2/logs"}},
		{ID: "vl.insert.journald.count", Surface: registry.SurfaceVL, Upstream: &registry.Upstream{Route: "/insert/journald/upload"}},
		{ID: "vl.insert.loki_push_json.count", Surface: registry.SurfaceVL, Upstream: &registry.Upstream{Route: "/insert/loki/api/v1/push"}},
		{ID: "vl.insert.splunk_event.count", Surface: registry.SurfaceVL, Upstream: &registry.Upstream{Route: "/insert/splunk/services/collector/event"}},
	}}
	reg.ByID = map[string]*registry.Row{}
	for i := range reg.Rows {
		reg.ByID[reg.Rows[i].ID] = &reg.Rows[i]
	}
	covered := CoveredKeys(inv, reg)
	for _, prefix := range []string{"/insert/datadog/", "/insert/journald/", "/insert/loki/", "/insert/splunk/"} {
		if len(covered["vl:route:"+prefix]) == 0 {
			t.Fatalf("vl:route:%s should be covered by prefix expansion, got: %v", prefix, covered)
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
		{Kind: "route", Surface: "vt", Name: "/select/tempo/api/v2/search/tag/"},
	}}
	reg := &registry.Registry{Rows: []registry.Row{
		{ID: "vt.tempo.v2_search_tag_values.basic", Surface: registry.SurfaceVT, Upstream: &registry.Upstream{Route: "/select/tempo/api/v2/search/tag/"}},
	}}
	ids := CoveredKeys(inv, reg)["vt:route:/select/tempo/api/v2/search/tag/"]
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
		{Kind: "route", Surface: "vl", Name: "/select/logsql/query"},
		{Kind: "route", Surface: "vl", Name: "/select/logsql/new_thing"},
	}}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	reg.Rows = []registry.Row{{ID: "vl.select.query.basic", Surface: registry.SurfaceVL, Upstream: &registry.Upstream{Route: "/select/logsql/query"}}}
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

// TestRowSurfaces covers every branch: vl and vt rows map to their own
// single surface, an lh row maps to both, and an unrecognized/zero-value
// Surface maps to none (nil) rather than guessing.
func TestRowSurfaces(t *testing.T) {
	cases := []struct {
		name    string
		surface registry.Surface
		want    []string
	}{
		{"vl", registry.SurfaceVL, []string{"vl"}},
		{"vt", registry.SurfaceVT, []string{"vt"}},
		{"lh", registry.SurfaceLH, []string{"vl", "vt"}},
		{"unrecognized", registry.Surface("bogus"), nil},
		{"zero value", registry.Surface(""), nil},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := &registry.Row{Surface: c.surface}
			got := RowSurfaces(r)
			if len(got) != len(c.want) {
				t.Fatalf("RowSurfaces(surface=%q) = %v, want %v", c.surface, got, c.want)
			}
			for i := range got {
				if got[i] != c.want[i] {
					t.Fatalf("RowSurfaces(surface=%q) = %v, want %v", c.surface, got, c.want)
				}
			}
		})
	}
}

// TestPresentOnAnySurface_NoUpstream proves a row with no Upstream
// reference (nil, or a non-nil zero value) is never "present" — there is
// nothing to look up.
func TestPresentOnAnySurface_NoUpstream(t *testing.T) {
	present := map[string]bool{"vl:route:/select/logsql/query": true}
	if PresentOnAnySurface(present, &registry.Row{Surface: registry.SurfaceVL, Upstream: nil}) {
		t.Fatal("a row with a nil Upstream must never be present")
	}
	if PresentOnAnySurface(present, &registry.Row{Surface: registry.SurfaceVL, Upstream: &registry.Upstream{}}) {
		t.Fatal("a row with a zero-value Upstream must never be present")
	}
}

// TestPresentOnAnySurface_LHChecksEitherSurface proves an lh row is
// "present" when its upstream key is present on either surface it covers
// (not only the first one checked), and "not present" only when it is on
// neither.
func TestPresentOnAnySurface_LHChecksEitherSurface(t *testing.T) {
	row := &registry.Row{Surface: registry.SurfaceLH, Upstream: &registry.Upstream{Route: "/select/logsql/query"}}

	presentOnVT := map[string]bool{"vt:route:/select/logsql/query": true}
	if !PresentOnAnySurface(presentOnVT, row) {
		t.Fatal("lh row should be present via the vt surface even though vl is absent")
	}

	presentOnVL := map[string]bool{"vl:route:/select/logsql/query": true}
	if !PresentOnAnySurface(presentOnVL, row) {
		t.Fatal("lh row should be present via the vl surface even though vt is absent")
	}

	neither := map[string]bool{"vl:route:/select/tempo/api/search": true}
	if PresentOnAnySurface(neither, row) {
		t.Fatal("lh row should not be present when neither surface has its upstream key")
	}
}

// TestRenderCoverage_LHRowGatedOnNeitherSurface proves an lh row is only
// listed in the "gated" section when its upstream key is present on
// neither vl nor vt — here it is present on vt, so it must be covered
// normally (not gated) even though the row's own Surface is lh.
func TestRenderCoverage_LHRowGatedOnNeitherSurface(t *testing.T) {
	inv := &inventory.Inventory{VLVersion: "v1.50.0", VTVersion: "v0.9.2", Items: []inventory.Item{
		{Kind: "route", Surface: "vt", Name: "/select/tempo/api/search"},
	}}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	reg.Rows = []registry.Row{{
		ID: "lh.shim.tempo_search_empty_q", Surface: registry.SurfaceLH, Expect: registry.ExpectPass,
		Upstream: &registry.Upstream{Route: "/select/tempo/api/search"},
	}}
	for i := range reg.Rows {
		reg.ByID[reg.Rows[i].ID] = &reg.Rows[i]
	}
	md := RenderCoverage(inv, reg)
	gatedIdx := strings.Index(md, "Rows gated on a later upstream version")
	if gatedIdx < 0 {
		t.Fatalf("expected the gated/absent section header:\n%s", md)
	}
	if strings.Contains(md[gatedIdx:], "lh.shim.tempo_search_empty_q") {
		t.Fatalf("lh row present on the vt surface must not appear in the gated section:\n%s", md)
	}
	if !strings.Contains(md, "route: 1/1 covered") {
		t.Fatalf("expected the vt route to be covered by the lh row:\n%s", md)
	}
	if !strings.Contains(md, "✅ native, verified") {
		t.Fatalf("expected the lh row to render as covered/verified in the main table:\n%s", md)
	}
}

// TestRenderCoverage_SameNameSortsBySurface proves that when VL and VT
// independently register an item with the same Name (e.g. a shared route
// or flag name), the per-kind table orders them deterministically by
// Surface as a tie-break — not by whatever order sort.Slice's unstable
// comparator happens to leave equal-Name items in, which could otherwise
// change confgen's output between runs.
func TestRenderCoverage_SameNameSortsBySurface(t *testing.T) {
	inv := &inventory.Inventory{Items: []inventory.Item{
		{Kind: "route", Surface: "vt", Name: "/insert/native", Source: "app/vtinsert/main.go"},
		{Kind: "route", Surface: "vl", Name: "/insert/native", Source: "app/vlinsert/main.go"},
	}}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	md := RenderCoverage(inv, reg)
	vlIdx := strings.Index(md, "app/vlinsert/main.go")
	vtIdx := strings.Index(md, "app/vtinsert/main.go")
	if vlIdx < 0 || vtIdx < 0 {
		t.Fatalf("expected both vl and vt /insert/native rows in the table:\n%s", md)
	}
	if vlIdx > vtIdx {
		t.Fatalf("expected the vl row (Surface tie-break) before the vt row for the same Name:\n%s", md)
	}

	// Reversed input order must produce the same output order (proves the
	// tie-break, not accidental input-order preservation).
	inv.Items[0], inv.Items[1] = inv.Items[1], inv.Items[0]
	md2 := RenderCoverage(inv, reg)
	if md2 != md {
		t.Fatalf("same-name sort must be order-independent:\nfirst:  %s\nsecond: %s", md, md2)
	}
}

// TestRenderCoverage_PerSurfacePreamble proves the doc states that items
// are counted per surface, so a route/flag registered by both VL and VT
// isn't misread as a unique-name count.
func TestRenderCoverage_PerSurfacePreamble(t *testing.T) {
	inv := &inventory.Inventory{VLVersion: "v1.50.0", VTVersion: "v0.9.2"}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	md := RenderCoverage(inv, reg)
	if !strings.Contains(md, "PER SURFACE") {
		t.Fatalf("expected the per-surface counting note in the preamble:\n%s", md)
	}
}
