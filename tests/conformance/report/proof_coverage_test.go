package report

import (
	"encoding/json"
	"math/rand"
	"regexp"
	"sort"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

var dateRe = regexp.MustCompile(`\b(19|20)\d\d-\d\d-\d\d`)

// proofFixtureRegistry is a registry whose every count is known by hand; the
// comments next to each row say which cell of the report it lands in.
func proofFixtureRegistry() *registry.Registry {
	hot := []registry.Target{registry.TargetHot}
	cold := []registry.Target{registry.TargetCold}
	refs := func(tests ...string) *registry.Refs { return &registry.Refs{Tests: tests} }
	up := func(route string) *registry.Upstream { return &registry.Upstream{Route: route} }
	api := []string{"api"}
	req := &registry.Request{Method: "GET", Path: "/x"}

	rows := []registry.Row{
		// vl route /a: executed, hot, parity test -> verified, differential.
		{ID: "vl.select.a.basic", Surface: registry.SurfaceVL, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Upstream: up("/a"), Layers: api, Refs: refs("tests/parity/a_test.go#TestA")},
		// vl route /a perf cell: never counts as coverage of /a.
		{ID: "vl.perf.a.cell", Surface: registry.SurfaceVL, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Upstream: up("/a"), Layers: []string{"perf"}, Pending: true, Refs: refs("tests/parity/a_test.go#TestA"), Perf: &registry.Perf{Cell: "a"}},
		// vl route /b: pending pass -> declared.
		{ID: "vl.select.b.basic", Surface: registry.SurfaceVL, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Upstream: up("/b"), Layers: api, Pending: true, Request: req},
		// vl route /c: differ.
		{ID: "vl.select.c.differ", Surface: registry.SurfaceVL, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectDiffer, DifferNote: "n", Targets: hot, Upstream: up("/c"), Layers: api, Pending: true, Request: req, Refs: refs("tests/e2e/c_test.go#TestC")},
		// vl route /d: unsupported + pending -> absent/unsupported.
		{ID: "vl.select.d.unsupported", Surface: registry.SurfaceVL, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectUnsupported, Targets: cold, Upstream: up("/d"), Layers: api, Pending: true, Request: req},
		// vl route /e: no row at all.
		// vt route /f: one pending and one executed row -> verified; the
		// executed one links a non-differential test, so not diff-linked.
		{ID: "vt.select.f.pending", Surface: registry.SurfaceVT, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Upstream: up("/f"), Layers: api, Pending: true, Request: req},
		{ID: "vt.select.f.done", Surface: registry.SurfaceVT, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: cold, Upstream: up("/f"), Layers: api, Refs: refs("internal/x/f_test.go#TestF")},
		// vl pipe: pass pending + differ sibling -> the worst (differ) wins.
		{ID: "vl.pipe.g.pass", Surface: registry.SurfaceVL, Kind: registry.KindPipe, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Upstream: &registry.Upstream{Pipe: "g"}, Layers: api, Pending: true, Request: req},
		{ID: "vl.pipe.g.differ", Surface: registry.SurfaceVL, Kind: registry.KindPipe, Origin: registry.OriginNative, Expect: registry.ExpectDiffer, DifferNote: "n", Targets: hot, Upstream: &registry.Upstream{Pipe: "g"}, Layers: api, Pending: true, Request: req},
		// vt traceql: pending pass -> declared.
		{ID: "vt.traceql.i.basic", Surface: registry.SurfaceVT, Kind: registry.KindTraceQL, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Upstream: &registry.Upstream{TraceQL: "i"}, Layers: api, Pending: true, Request: req},
		// Ingest matrix cells.
		{ID: "vl.ingest.loki.numeric", Surface: registry.SurfaceVL, Kind: registry.KindInsert, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Upstream: up("/insert/loki"), Layers: api, Refs: refs("tests/e2e/ingest_test.go#TestIngest")},
		{ID: "vt.ingest.otlp.numeric", Surface: registry.SurfaceVT, Kind: registry.KindInsert, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Upstream: up("/insert/otlp"), Layers: api, Pending: true, Request: req},
		{ID: "vt.ingest.otlp.alias", Surface: registry.SurfaceVT, Kind: registry.KindInsert, Origin: registry.OriginNative, Expect: registry.ExpectDiffer, DifferNote: "n", Targets: hot, Upstream: up("/insert/otlp"), Layers: api, Request: req},
		// Pass rows for the test-link split (vl and vt, non-perf).
		{ID: "vl.select.p1.diff", Surface: registry.SurfaceVL, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Layers: api, Refs: refs("tests/e2e/p_test.go#TestP")},
		{ID: "vl.select.p2.hot_unit", Surface: registry.SurfaceVL, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Layers: api, Refs: refs("internal/x/p_test.go#TestP")},
		{ID: "vl.select.p3.cold_parity", Surface: registry.SurfaceVL, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: cold, Layers: api, Refs: refs("tests/parity/p_test.go#TestP")},
		{ID: "vl.select.p4.none", Surface: registry.SurfaceVL, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: hot, Layers: api, Pending: true, Request: req},
		{ID: "vt.select.p5.none", Surface: registry.SurfaceVT, Kind: registry.KindSelect, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: cold, Layers: api},
		// A differ row and an lh row are not upstream pass rows.
		{ID: "lh.stats.q.pass", Surface: registry.SurfaceLH, Kind: registry.KindAdmin, Origin: registry.OriginLHAddition, Expect: registry.ExpectPass, Targets: hot, Layers: api, Refs: refs("tests/parity/lh_test.go#TestLH")},
		// One pending row per pending reason.
		{ID: "lh.durability.d.one", Surface: registry.SurfaceLH, Kind: registry.KindDurability, Origin: registry.OriginLHAddition, Expect: registry.ExpectPass, Targets: cold, Layers: api, Pending: true},
		{ID: "lh.chaos.h.one", Surface: registry.SurfaceLH, Kind: registry.KindAdmin, Origin: registry.OriginLHAddition, Expect: registry.ExpectPass, Targets: cold, Layers: []string{"chaos"}, Pending: true},
		{ID: "vl.flag.f.one", Surface: registry.SurfaceVL, Kind: registry.KindFlag, Origin: registry.OriginNative, Expect: registry.ExpectPass, Targets: cold, Upstream: &registry.Upstream{Flag: "f"}, Layers: api, Pending: true},
		{ID: "lh.admin.r.linked", Surface: registry.SurfaceLH, Kind: registry.KindAdmin, Origin: registry.OriginLHAddition, Expect: registry.ExpectPass, Targets: cold, Layers: api, Pending: true, Request: req, Refs: refs("internal/x/r_test.go#TestR")},
		{ID: "lh.admin.u.nothing", Surface: registry.SurfaceLH, Kind: registry.KindAdmin, Origin: registry.OriginLHAddition, Expect: registry.ExpectPass, Targets: cold, Layers: api, Pending: true},
	}
	reg := &registry.Registry{Rows: rows, ByID: map[string]*registry.Row{}}
	for i := range reg.Rows {
		reg.ByID[reg.Rows[i].ID] = &reg.Rows[i]
	}
	return reg
}

func proofFixtureInventory() *inventory.Inventory {
	return &inventory.Inventory{VLVersion: "v1", VTVersion: "v1", Items: []inventory.Item{
		{Kind: "route", Surface: "vl", Name: "/a"},
		{Kind: "route", Surface: "vl", Name: "/b"},
		{Kind: "route", Surface: "vl", Name: "/c"},
		{Kind: "route", Surface: "vl", Name: "/d"},
		{Kind: "route", Surface: "vl", Name: "/e"},
		{Kind: "route", Surface: "vt", Name: "/f"},
		{Kind: "pipe", Surface: "vl", Name: "g"},
		{Kind: "flag", Surface: "vl", Name: "h"},
		{Kind: "traceql", Surface: "vt", Name: "i"},
	}}
}

func proofFixtureFeatures() *registry.FeatureSet {
	mk := func(id string, area registry.Area, status registry.Status, rows []string, bench []string, tests ...string) registry.Feature {
		return registry.Feature{ID: id, Area: area, Status: status, Rows: rows, Bench: bench, Tests: tests}
	}
	fs := &registry.FeatureSet{Features: []registry.Feature{
		mk("lh.feature.ingest.exact", "ingest", registry.StatusShipped, []string{"vl.select.a.basic"}, nil, "x_test.go"),
		mk("lh.feature.ingest.ext", "ingest", registry.StatusShipped, []string{"vl.select.p2.hot_unit"}, []string{"count_total"}),
		mk("lh.feature.query.perfonly", "query", registry.StatusShipped, nil, []string{"scan"}, "y_test.go"),
		mk("lh.feature.query.bare", "query", registry.StatusShipped, nil, nil, "y_test.go", "z_test.go"),
		mk("lh.feature.query.perfcell", "query", registry.StatusShipped, []string{"vl.perf.a.cell"}, nil),
		mk("lh.feature.query.differ", "query", registry.StatusShipped, []string{"vl.select.c.differ"}, nil),
		mk("lh.feature.query.lhrow", "query", registry.StatusShipped, []string{"lh.stats.q.pass"}, nil),
		mk("lh.feature.query.planned", "query", registry.StatusPlanned, nil, nil),
		mk("lh.feature.query.wip", "query", registry.StatusInProgress, nil, nil),
	}}
	fs.ByID = map[string]*registry.Feature{}
	for i := range fs.Features {
		fs.ByID[fs.Features[i].ID] = &fs.Features[i]
	}
	return fs
}

func proofFixtureShared() *SharedCodeInput {
	const pkgA = "github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	const pkgB = "github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"
	const pkgC = "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	return &SharedCodeInput{
		GoMods: []GoModDep{{File: "go.mod", Module: "github.com/VictoriaMetrics/VictoriaLogs", Version: "v1", Replace: "./deps/VictoriaLogs"}},
		Pins: []PinRef{
			{Name: "PIN_GUARDED", Value: "v1", Guards: []string{"a_test.go"}},
			{Name: "PIN_BARE", Value: "v2"},
		},
		Patches: []PatchRef{
			{Path: "patches/x/file.patch", Guards: []string{"b_test.go"}},
			{Path: "patches/x/dir.patch", DirGuards: []string{"gate.sh"}},
			{Path: "patches/x/bare.src"},
		},
		Imports: []ImportUse{
			{Path: pkgA, Module: "logs", File: "internal/a.go", Kind: ImportSrc},
			{Path: pkgA, Module: "traces", File: "lakehouse-traces/a.go", Kind: ImportSrc},
			{Path: pkgA, Module: "logs", File: "internal/a_test.go", Kind: ImportTest},
			{Path: pkgB, Module: "traces", File: "lakehouse-traces/b.go", Kind: ImportSrc},
			{Path: pkgC, Module: "logs", File: "internal/c_test.go", Kind: ImportTest}, // named by a test only: not imported
			{Path: "github.com/VictoriaMetrics/VictoriaMetrics/lib/logger", Module: "logs", File: "internal/l.go", Kind: ImportSrc},
		},
	}
}

func proofFixture() ProofInputs {
	return ProofInputs{Inventory: proofFixtureInventory(), Registry: proofFixtureRegistry(), Features: proofFixtureFeatures(), Shared: proofFixtureShared()}
}

func findKind(t *testing.T, ks []KindCoverage, kind, signal string) KindCoverage {
	t.Helper()
	for _, k := range ks {
		if k.Kind == kind && k.Signal == signal {
			return k
		}
	}
	t.Fatalf("no %s/%s line in %+v", kind, signal, ks)
	return KindCoverage{}
}

func TestProofCoverage_KindCounts(t *testing.T) {
	pc := ComputeProofCoverage(proofFixture())

	route := findKind(t, pc.Upstream, "route", "all")
	want := KindCoverage{Kind: "route", Signal: "all", Items: 6, WithRow: 5, Verified: 2, Pending: 1, Differs: 1, AbsentUnsupported: 1, NoRow: 1, DifferentiallyLinked: 2}
	if route != want {
		t.Errorf("route/all = %+v, want %+v", route, want)
	}
	if got := findKind(t, pc.Upstream, "route", "vl"); got.Items != 5 || got.Verified != 1 || got.NoRow != 1 || got.DifferentiallyLinked != 2 {
		t.Errorf("route/vl = %+v", got)
	}
	if got := findKind(t, pc.Upstream, "route", "vt"); got.Items != 1 || got.Verified != 1 || got.DifferentiallyLinked != 0 {
		t.Errorf("route/vt = %+v (mixed pending/executed rows verify; the executed row is not differential)", got)
	}
	if got := findKind(t, pc.Upstream, "pipe", "all"); got.Differs != 1 || got.Pending != 0 || got.Verified != 0 {
		t.Errorf("pipe/all = %+v: the worst covering row (differ) must win over a pending pass", got)
	}
	if got := findKind(t, pc.Upstream, "flag", "all"); got.Items != 1 || got.NoRow != 1 {
		t.Errorf("flag/all = %+v", got)
	}
	if got := findKind(t, pc.Upstream, "traceql", "all"); got.Pending != 1 {
		t.Errorf("traceql/all = %+v", got)
	}
	ing := findKind(t, pc.Upstream, "ingest", "all")
	if ing.Items != 3 || ing.Verified != 1 || ing.Pending != 1 || ing.Differs != 1 || ing.DifferentiallyLinked != 1 {
		t.Errorf("ingest/all = %+v", ing)
	}
	// Signals with no items get no line (pipes exist on logs only).
	for _, k := range pc.Upstream {
		if k.Kind == "pipe" && k.Signal == "vt" {
			t.Errorf("an empty per-signal line must be omitted: %+v", k)
		}
	}
}

// TestProofCoverage_AgreesWithUpstreamCoverageDoc ties the per-kind status
// counts to UPSTREAM_COVERAGE.md: the report and the doc are two views of one
// classification and must never disagree.
func TestProofCoverage_AgreesWithUpstreamCoverageDoc(t *testing.T) {
	in := proofFixture()
	md := RenderCoverage(in.Inventory, in.Registry)
	pc := ComputeProofCoverage(in)
	for _, kind := range inventoryKinds {
		section := ""
		if i := strings.Index(md, "## Upstream "+kind+" ("); i >= 0 {
			section = md[i:]
			if j := strings.Index(section[3:], "\n## "); j >= 0 {
				section = section[:j+3]
			}
		}
		var verified, declared, differ, absent, norow int
		for _, line := range strings.Split(section, "\n") {
			switch {
			case strings.Contains(line, "| ✅"):
				verified++
			case strings.Contains(line, "| 🟡"):
				declared++
			case strings.Contains(line, "| 🔁"):
				differ++
			case strings.Contains(line, "| ⛔"):
				absent++
			case strings.Contains(line, "| ⚪"):
				norow++
			}
		}
		got := KindCoverage{Kind: kind, Signal: "all"}
		for _, k := range pc.Upstream {
			if k.Kind == kind && k.Signal == "all" {
				got = k
			}
		}
		if got.Verified != verified || got.Pending != declared || got.Differs != differ || got.AbsentUnsupported != absent || got.NoRow != norow {
			t.Errorf("%s: report %+v disagrees with UPSTREAM_COVERAGE.md (✅%d 🟡%d 🔁%d ⛔%d ⚪%d)", kind, got, verified, declared, differ, absent, norow)
		}
	}
}

func TestProofCoverage_PassRowSplit(t *testing.T) {
	pc := ComputeProofCoverage(proofFixture())
	byS := map[string]PassRowSplit{}
	for _, s := range pc.PassRows {
		byS[s.Signal] = s
	}
	// vl pass rows (non-perf): a, b, pipe g (pass), flag f, ingest loki, p1..p4 = 9; the c differ
	// row, the d unsupported row, the perf cell and the lh rows are not upstream pass rows.
	// Differential: a, ingest loki, p1. Other test: p2 (unit test), p3 (cold target). None: b, g, flag, p4.
	wantVL := PassRowSplit{Signal: "vl", Rows: 9, TargetHot: 7, Differential: 3, OtherTest: 2, NoTest: 4}
	if byS["vl"] != wantVL {
		t.Errorf("vl = %+v, want %+v", byS["vl"], wantVL)
	}
	// vt pass rows: f.pending, f.done, traceql i, ingest otlp numeric, p5 = 5 (the alias cell differs).
	wantVT := PassRowSplit{Signal: "vt", Rows: 5, TargetHot: 3, Differential: 0, OtherTest: 1, NoTest: 4}
	if byS["vt"] != wantVT {
		t.Errorf("vt = %+v, want %+v", byS["vt"], wantVT)
	}
	all := byS["all"]
	if all.Rows != 14 || all.Differential != 3 || all.OtherTest != 3 || all.NoTest != 8 || all.Differential+all.OtherTest+all.NoTest != all.Rows {
		t.Errorf("all = %+v", all)
	}
	var ids []string
	for _, r := range pc.PassRowsNone {
		ids = append(ids, r.ID)
	}
	if !sort.StringsAreSorted(ids) || len(ids) != all.NoTest {
		t.Errorf("the no-test list must be sorted and match the count: %v", ids)
	}
	if strings.Contains(strings.Join(ids, " "), "vl.perf.a.cell") {
		t.Error("a perf cell is not an upstream pass row")
	}
}

func TestProofCoverage_FeatureClasses(t *testing.T) {
	pc := ComputeProofCoverage(proofFixture())
	f := pc.Features
	if f.Total != 9 || f.Shipped != 7 || f.ByStatus["planned"] != 1 || f.ByStatus["in-progress"] != 1 {
		t.Fatalf("totals = %+v", f)
	}
	class := map[string]FeatureProof{}
	for _, fp := range f.ShippedAll {
		class[fp.ID] = fp
	}
	cases := map[string]string{
		"lh.feature.ingest.exact":   ClassUpstreamExact,
		"lh.feature.ingest.ext":     ClassLHExtension, // a hot row with an internal test only
		"lh.feature.query.perfonly": ClassPerfProven,
		"lh.feature.query.bare":     ClassUnproven,
		"lh.feature.query.perfcell": ClassLHExtension, // a perf cell never makes a feature upstream-exact
		"lh.feature.query.differ":   ClassLHExtension, // a differ row is not exact
		"lh.feature.query.lhrow":    ClassLHExtension, // an lh row is not an upstream surface
	}
	for id, want := range cases {
		if got := class[id].ProofClass; got != want {
			t.Errorf("%s class = %q, want %q", id, got, want)
		}
	}
	if f.ByClass[ClassUpstreamExact] != 1 || f.ByClass[ClassLHExtension] != 4 || f.ByClass[ClassPerfProven] != 1 || f.ByClass[ClassUnproven] != 1 {
		t.Errorf("by class = %v", f.ByClass)
	}
	// Rows, perf scenario, tests.
	if f.NoRows != 2 { // perfonly, bare
		t.Errorf("no rows = %d, want 2", f.NoRows)
	}
	if f.NoPerf != 4 { // exact, bare, differ, lhrow have neither bench nor perf cell
		t.Errorf("no perf scenario = %d, want 4", f.NoPerf)
	}
	if f.NoTests != 4 { // ext, perfcell, differ, lhrow
		t.Errorf("no tests = %d, want 4", f.NoTests)
	}
	if got := class["lh.feature.query.perfcell"]; got.PerfCells != 1 || !got.PerfScenario || got.RowsPending != 1 {
		t.Errorf("perfcell feature = %+v", got)
	}
	if got := class["lh.feature.query.bare"]; got.Tests != 2 || got.Bench == nil {
		t.Errorf("bare feature = %+v (Bench must be an empty list for stable JSON)", got)
	}
	if f.HotRows != 5 { // exact, ext, perfcell, differ and lhrow all cite a row that targets hot
		t.Errorf("features citing a hot row = %d, want 5", f.HotRows)
	}
}

func TestProofCoverage_PendingReasons(t *testing.T) {
	pc := ComputeProofCoverage(proofFixture())
	got := map[string]int{}
	for _, p := range pc.Pending {
		got[p.Reason] = p.Rows
	}
	// 15 pending rows: one perf cell, durability, chaos and flag row each; two with a
	// request and a test (c.differ, r.linked); eight with a request and no test (b, d,
	// f.pending, pipe g pass and differ, traceql i, vt ingest otlp numeric, p4); and
	// u.nothing with neither -> unclassified.
	want := map[string]int{
		ReasonPerfCell: 1, ReasonDurabilityCell: 1, ReasonHarness: 1, ReasonFlagRow: 1,
		ReasonRunnerLinked: 2, ReasonRunnerBare: 8, ReasonUnclassified: 1,
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("reason %s = %d, want %d (all: %v)", k, got[k], v, got)
		}
	}
	total := 0
	for _, n := range got {
		total += n
	}
	if total != pc.Registry.Pending {
		t.Errorf("pending reasons sum to %d, registry pending is %d", total, pc.Registry.Pending)
	}
	v := pc.Verdict
	if v.PendingElsewhere != 2 || v.PendingUnclassified != 1 || v.PendingNoRunner != 12 {
		t.Errorf("verdict pending split = %+v", v)
	}
	if order := []string{pc.Pending[0].Reason, pc.Pending[len(pc.Pending)-1].Reason}; order[0] != ReasonPerfCell || order[1] != ReasonUnclassified {
		t.Errorf("reasons must keep the fixed display order, got %v", order)
	}
}

func TestProofCoverage_SharedCode(t *testing.T) {
	pc := ComputeProofCoverage(proofFixture())
	s := pc.Shared
	// 2 pins + 3 patches + 2 imported VL/VT packages (the test-only one is dropped).
	if s.Items != 7 || s.Guarded != 4 || s.Unguarded != 3 {
		t.Fatalf("shared = items %d guarded %d unguarded %d, want 7/4/3", s.Items, s.Guarded, s.Unguarded)
	}
	wantUnguarded := []string{"package VictoriaTraces/app/vtstorage", "pin PIN_BARE", "src patches/x/bare.src"}
	if strings.Join(s.UnguardedItems, "|") != strings.Join(wantUnguarded, "|") {
		t.Errorf("unguarded = %v, want %v", s.UnguardedItems, wantUnguarded)
	}
	if len(s.Packages) != 2 || s.LibPackages != 1 {
		t.Errorf("packages = %+v libs = %d", s.Packages, s.LibPackages)
	}
	a := s.Packages[0]
	if a.Package != "VictoriaLogs/lib/logstorage" || !a.InLogs || !a.InTraces || !a.Guarded || len(a.Guards) != 1 {
		t.Errorf("logstorage = %+v", a)
	}
	var dirOnly *SharedItem
	for i := range s.Patches {
		if s.Patches[i].Name == "patches/x/dir.patch" {
			dirOnly = &s.Patches[i]
		}
	}
	if dirOnly == nil || !dirOnly.Guarded {
		t.Errorf("a patch whose directory a gate names is guarded: %+v", dirOnly)
	}
	if pc.Verdict.SharedCodeItems != 7 || pc.Verdict.SharedCodeUnguarded != 3 {
		t.Errorf("verdict shared = %+v", pc.Verdict)
	}
}

func TestProofCoverage_Verdict(t *testing.T) {
	v := ComputeProofCoverage(proofFixture()).Verdict
	want := Verdict{
		UpstreamItems: 9, UpstreamItemsWithRow: 7, UpstreamItemsNoRow: 2, UpstreamItemsVerified: 2,
		UpstreamItemsDeclaredOnly: 2, UpstreamItemsDifferentiallyLinked: 2,
		IngestCells: 3, IngestCellsVerified: 1,
		UpstreamPassRows: 14, UpstreamPassRowsDifferential: 3, UpstreamPassRowsOtherTest: 3, UpstreamPassRowsNoTest: 8,
		RowsTotal: 24, RowsPending: 15, PendingNoRunner: 12, PendingElsewhere: 2, PendingUnclassified: 1,
		ShippedFeatures: 7, ShippedFeaturesNoRows: 2, ShippedFeaturesNoPerf: 4, ShippedFeaturesUnproven: 1,
		SharedCodeItems: 7, SharedCodeUnguarded: 3,
	}
	if v != want {
		t.Errorf("verdict:\n got %+v\nwant %+v", v, want)
	}
}

// TestProofCoverage_Deterministic proves the report is a pure function of the
// registry's content, not of row order: shuffled rows give the same bytes.
func TestProofCoverage_Deterministic(t *testing.T) {
	base := proofFixture()
	wantMD := ComputeProofCoverage(base).Markdown()
	wantJSON, err := ComputeProofCoverage(base).JSON()
	if err != nil {
		t.Fatal(err)
	}
	rng := rand.New(rand.NewSource(7))
	for i := 0; i < 5; i++ {
		in := proofFixture()
		rng.Shuffle(len(in.Registry.Rows), func(a, b int) { in.Registry.Rows[a], in.Registry.Rows[b] = in.Registry.Rows[b], in.Registry.Rows[a] })
		in.Registry.ByID = map[string]*registry.Row{}
		for j := range in.Registry.Rows {
			in.Registry.ByID[in.Registry.Rows[j].ID] = &in.Registry.Rows[j]
		}
		in.Shared.Imports = append([]ImportUse(nil), in.Shared.Imports...)
		rng.Shuffle(len(in.Shared.Imports), func(a, b int) {
			in.Shared.Imports[a], in.Shared.Imports[b] = in.Shared.Imports[b], in.Shared.Imports[a]
		})
		pc := ComputeProofCoverage(in)
		if got := pc.Markdown(); got != wantMD {
			t.Fatalf("markdown depends on input order (round %d)", i)
		}
		gotJSON, err := pc.JSON()
		if err != nil {
			t.Fatal(err)
		}
		if string(gotJSON) != string(wantJSON) {
			t.Fatalf("json depends on input order (round %d)", i)
		}
	}
	if dateRe.MatchString(wantMD) || dateRe.Match(wantJSON) {
		t.Error("the report must carry no date or timestamp")
	}
}

func TestProofCoverage_JSONShape(t *testing.T) {
	b, err := ComputeProofCoverage(proofFixture()).JSON()
	if err != nil {
		t.Fatal(err)
	}
	if !strings.HasSuffix(string(b), "}\n") {
		t.Error("JSON must end with one newline")
	}
	var doc map[string]any
	if err := json.Unmarshal(b, &doc); err != nil {
		t.Fatal(err)
	}
	for _, k := range []string{"schema_version", "verdict", "upstream_by_kind", "upstream_pass_rows", "upstream_pass_rows_without_test", "registry", "features", "shared_code", "pending_by_reason", "not_inventoried"} {
		if _, ok := doc[k]; !ok {
			t.Errorf("JSON is missing %q", k)
		}
	}
	verdict := doc["verdict"].(map[string]any)
	for _, k := range []string{"upstream_pass_rows_no_test", "shipped_features_no_rows", "shipped_features_no_perf_scenario", "shared_code_unguarded", "rows_pending_unclassified"} {
		if _, ok := verdict[k]; !ok {
			t.Errorf("verdict is missing %q", k)
		}
	}
}

func TestProofCoverage_MarkdownHeadlines(t *testing.T) {
	md := ComputeProofCoverage(proofFixture()).Markdown()
	for _, want := range []string{
		"# Proof coverage (GENERATED by `make conformance-gen`",
		"## Verdict", "## 1. Upstream surfaces by kind", "## 2. Upstream pass rows by test link",
		"## 3. Shipped features", "## 4. Shared upstream code", "## 5. Pending rows by reason", "## 6. Registry totals",
		"| ... linking no test at all | 8 |",
		"<details><summary>8 upstream pass rows link no test</summary>",
		"| `vl.select.p4.none` | vl | select | yes | yes |",
		"- package VictoriaTraces/app/vtstorage",
		"| `lh.feature.ingest.exact` | ingest | 1 (0) | 1 | 1 | none | upstream-exact |",
	} {
		if !strings.Contains(md, want) {
			t.Errorf("markdown is missing %q", want)
		}
	}
}

// TestProofCoverage_NilInputs: a missing inventory, feature set or shared scan
// yields an empty section, not a panic.
func TestProofCoverage_NilInputs(t *testing.T) {
	pc := ComputeProofCoverage(ProofInputs{})
	if pc.Verdict.RowsTotal != 0 || pc.Shared.Items != 0 || pc.Features.Shipped != 0 {
		t.Errorf("empty inputs gave %+v", pc.Verdict)
	}
	if md := pc.Markdown(); !strings.Contains(md, "None: every item above") {
		t.Errorf("an empty shared section reports no unguarded items:\n%s", md)
	}
	if _, err := pc.JSON(); err != nil {
		t.Fatal(err)
	}
}

func TestIsDifferentiallyLinked(t *testing.T) {
	hot, cold := []registry.Target{registry.TargetHot}, []registry.Target{registry.TargetCold}
	mk := func(targets []registry.Target, tests ...string) *registry.Row {
		return &registry.Row{Targets: targets, Refs: &registry.Refs{Tests: tests}}
	}
	cases := []struct {
		name string
		row  *registry.Row
		want bool
	}{
		{"hot + parity", mk(hot, "tests/parity/a_test.go#T"), true},
		{"hot + e2e", mk(hot, "tests/e2e/a_test.go#T"), true},
		{"hot + several, one differential", mk(hot, "internal/x_test.go", "tests/parity/a_test.go#T"), true},
		{"cold + parity", mk(cold, "tests/parity/a_test.go#T"), false},
		{"hot + unit test", mk(hot, "internal/x_test.go#T"), false},
		{"hot + conformance test", mk(hot, "tests/conformance/x_test.go#T"), false},
		{"hot, no tests", mk(hot), false},
		{"hot, nil refs", &registry.Row{Targets: hot}, false},
	}
	for _, c := range cases {
		if got := IsDifferentiallyLinked(c.row); got != c.want {
			t.Errorf("%s: got %v, want %v", c.name, got, c.want)
		}
	}
}

func TestClassifyItem(t *testing.T) {
	row := func(e registry.Expect, pending bool) *registry.Row { return &registry.Row{Expect: e, Pending: pending} }
	cases := []struct {
		name string
		rows []*registry.Row
		want itemClass
	}{
		{"no rows", nil, classNoRow},
		{"executed pass", []*registry.Row{row(registry.ExpectPass, false)}, classVerified},
		{"pending pass", []*registry.Row{row(registry.ExpectPass, true)}, classDeclared},
		{"pending and executed pass", []*registry.Row{row(registry.ExpectPass, true), row(registry.ExpectPass, false)}, classVerified},
		{"differ, pending", []*registry.Row{row(registry.ExpectDiffer, true)}, classDiffer},
		{"differ, executed", []*registry.Row{row(registry.ExpectDiffer, false)}, classDiffer},
		{"unsupported", []*registry.Row{row(registry.ExpectUnsupported, true)}, classAbsent},
		{"absent", []*registry.Row{row(registry.ExpectAbsent, false)}, classAbsent},
		{"pass + unsupported: the worse wins", []*registry.Row{row(registry.ExpectPass, false), row(registry.ExpectUnsupported, false)}, classAbsent},
		{"differ + absent: absent is worse", []*registry.Row{row(registry.ExpectDiffer, false), row(registry.ExpectAbsent, false)}, classAbsent},
	}
	for _, c := range cases {
		if got := classifyItem(c.rows); got != c.want {
			t.Errorf("%s: class %d, want %d", c.name, got, c.want)
		}
	}
}

func TestPendingReason(t *testing.T) {
	req := &registry.Request{Method: "GET", Path: "/x"}
	tests := &registry.Refs{Tests: []string{"internal/x_test.go"}}
	cases := []struct {
		name string
		row  registry.Row
		want string
	}{
		{"perf layer wins over everything", registry.Row{Layers: []string{"perf", "chaos"}, Kind: registry.KindDurability, Request: req}, ReasonPerfCell},
		{"durability kind", registry.Row{Kind: registry.KindDurability, Layers: []string{"api"}, Request: req}, ReasonDurabilityCell},
		{"chaos layer", registry.Row{Layers: []string{"api", "chaos"}, Kind: registry.KindAdmin, Request: req}, ReasonHarness},
		{"ui layer", registry.Row{Layers: []string{"ui"}, Kind: registry.KindUI}, ReasonHarness},
		{"flag kind", registry.Row{Layers: []string{"api"}, Kind: registry.KindFlag, Request: req}, ReasonFlagRow},
		{"request with tests", registry.Row{Layers: []string{"api"}, Kind: registry.KindSelect, Request: req, Refs: tests}, ReasonRunnerLinked},
		{"request alone", registry.Row{Layers: []string{"api"}, Kind: registry.KindSelect, Request: req}, ReasonRunnerBare},
		{"neither", registry.Row{Layers: []string{"api"}, Kind: registry.KindSelect}, ReasonUnclassified},
		{"tests but no request", registry.Row{Layers: []string{"api"}, Kind: registry.KindSelect, Refs: tests}, ReasonUnclassified},
	}
	for _, c := range cases {
		if got := pendingReason(&c.row); got != c.want {
			t.Errorf("%s: %q, want %q", c.name, got, c.want)
		}
	}
}

// TestComputeShared_GuardListsAreStable: the report must not move each time a
// test is added to a heavily guarded item, so an item with more than three
// guards says "many" and lists none; sparse items name their guards; an item
// with no guard lists an empty (non-nil) list, for stable JSON.
func TestComputeShared_GuardListsAreStable(t *testing.T) {
	const pkg = "github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	const sparse = "github.com/VictoriaMetrics/VictoriaLogs/app/vlstorage"
	in := &SharedCodeInput{
		Pins:    []PinRef{{Name: "P", Guards: []string{"e_test.go", "a_test.go", "d_test.go", "b_test.go", "c_test.go"}}, {Name: "Q", Guards: []string{"c.go", "a.go", "b.go"}}},
		Patches: []PatchRef{{Path: "patches/x/p.patch", DirGuards: []string{"1", "2", "3", "4"}}, {Path: "patches/x/q.patch"}},
		Imports: []ImportUse{
			{Path: pkg, Module: "logs", File: "a.go", Kind: ImportSrc},
			{Path: sparse, Module: "logs", File: "a.go", Kind: ImportSrc},
			{Path: sparse, Module: "logs", File: "s_test.go", Kind: ImportTest},
		},
	}
	for _, f := range []string{"z_test.go", "y_test.go", "x_test.go", "w_test.go"} {
		in.Imports = append(in.Imports, ImportUse{Path: pkg, Module: "logs", File: f, Kind: ImportTest})
	}
	sc := computeShared(in)
	pins := map[string]SharedItem{}
	for _, p := range sc.Pins {
		pins[p.Name] = p
	}
	if p := pins["P"]; !p.Guarded || !p.GuardsMany || len(p.Guards) != 0 || p.Guards == nil {
		t.Errorf("five guards must read as many, with an empty non-nil list: %+v", p)
	}
	if q := pins["Q"]; !q.Guarded || q.GuardsMany || strings.Join(q.Guards, ",") != "a.go,b.go,c.go" {
		t.Errorf("three guards are named, sorted: %+v", q)
	}
	if p := sc.Patches[0]; !p.Guarded || !p.DirGuardsMany || len(p.DirGuards) != 0 || p.GuardsMany {
		t.Errorf("four directory guards read as many: %+v", p)
	}
	if q := sc.Patches[1]; q.Guarded || q.GuardsMany || q.Guards == nil || len(q.Guards) != 0 {
		t.Errorf("an unguarded patch lists an empty non-nil list: %+v", q)
	}
	pk := map[string]PackageItem{}
	for _, p := range sc.Packages {
		pk[p.Package] = p
	}
	if p := pk["VictoriaLogs/lib/logstorage"]; !p.Guarded || !p.GuardsMany || len(p.Guards) != 0 {
		t.Errorf("a package named by four tests reads as many: %+v", p)
	}
	if p := pk["VictoriaLogs/app/vlstorage"]; !p.Guarded || p.GuardsMany || strings.Join(p.Guards, ",") != "s_test.go" {
		t.Errorf("a package named by one test names it: %+v", p)
	}
	if sc.Unguarded != 1 || sc.UnguardedItems[0] != "patch patches/x/q.patch" {
		t.Errorf("unguarded = %d %v", sc.Unguarded, sc.UnguardedItems)
	}
	md := (&ProofCoverage{Shared: sc}).Markdown()
	if !strings.Contains(md, "many (more than 3 files)") {
		t.Errorf("the table must say many:\n%s", md)
	}
}
