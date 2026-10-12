package registry

import (
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func durabilityRow() Row {
	return Row{
		ID: "lh.durability.pre1970_rows_logs", Title: "t", Surface: SurfaceLH, Kind: KindDurability,
		Origin: OriginLHAddition, Expect: ExpectPass, Targets: []Target{TargetCold},
		Layers: []string{"chaos"}, Pending: true,
		Durability: &Durability{Fault: "pre1970_rows", Mode: "buffer-flush", Signal: "logs",
			Guarantees: []string{"no-loss", "loss-counted"}, Metric: "lakehouse_insert_rows_dropped_total"},
		Refs: &Refs{Tests: []string{"internal/storage/parquets3/x_test.go#TestX"}},
	}
}

func TestDurability_ValidCells(t *testing.T) {
	r := durabilityRow()
	if err := r.Validate(); err != nil {
		t.Fatalf("a pass cell: %v", err)
	}
	g := durabilityRow()
	g.ID = "lh.durability.pre1970_stored_logs"
	g.Expect = ExpectDiffer
	g.DifferNote = "known gap"
	g.Durability.Gap = "https://github.com/ReliablyObserve/victoria-lakehouse/issues/484"
	if err := g.Validate(); err != nil {
		t.Fatalf("a known-gap cell: %v", err)
	}
	n := durabilityRow()
	n.Durability.Guarantees = []string{"no-loss", "no-duplicate", "rejected-429"}
	n.Durability.Metric = ""
	if err := n.Validate(); err != nil {
		t.Fatalf("a cell with no loss-counted guarantee and no metric: %v", err)
	}
}

func TestDurability_InvalidCells(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Row)
		want string
	}{
		{"no block", func(r *Row) { r.Durability = nil }, "durability block required"},
		{"block on another kind", func(r *Row) {
			r.Kind = KindSelect
			r.Request = &Request{Method: "GET", Path: "/x"}
			r.Compare = &Compare{Type: "status"}
			r.Seed = []string{"logs.base"}
			r.Origin = OriginLHAddition
		}, "only allowed for kind=durability"},
		{"bad fault", func(r *Row) { r.Durability.Fault = "Pre 1970" }, "durability.fault"},
		{"bad mode", func(r *Row) { r.Durability.Mode = "wal" }, "durability.mode"},
		{"bad signal", func(r *Row) { r.Durability.Signal = "metrics" }, "durability.signal"},
		{"no guarantees", func(r *Row) { r.Durability.Guarantees = nil; r.Durability.Metric = "" }, "durability.guarantees required"},
		{"bad guarantee", func(r *Row) { r.Durability.Guarantees = []string{"best-effort"} }, `durability.guarantees: "best-effort" invalid`},
		{"the old refused-counted value", func(r *Row) { r.Durability.Guarantees = []string{"refused-counted"} }, "invalid"},
		{"repeated guarantee", func(r *Row) { r.Durability.Guarantees = []string{"no-loss", "no-loss", "loss-counted"} }, "repeated"},
		{"loss-counted without a metric", func(r *Row) { r.Durability.Metric = "" }, "durability.metric must name the counter"},
		{"metric without loss-counted", func(r *Row) { r.Durability.Guarantees = []string{"no-loss"} }, "only allowed with the guarantee loss-counted"},
		{"signal not in id", func(r *Row) { r.Durability.Signal = "traces" }, "must end in _traces"},
		{"not an addition", func(r *Row) {
			r.Origin = OriginNative
			r.Upstream = &Upstream{Route: "/x"}
		}, "must be lh-addition"},
		{"absent", func(r *Row) { r.Expect = ExpectAbsent }, "expect must be pass or differ"},
		{"gap without issue", func(r *Row) { r.Expect = ExpectDiffer; r.DifferNote = "n" }, "issue link"},
		{"gap on a pass cell", func(r *Row) { r.Durability.Gap = "https://github.com/o/r/issues/1" }, "only allowed when expect=differ"},
		{"no tests", func(r *Row) { r.Refs = nil }, "refs.tests required"},
		{"has a request", func(r *Row) { r.Request = &Request{Method: "GET", Path: "/x"} }, "request not allowed"},
		{"has a compare", func(r *Row) { r.Compare = &Compare{Type: "status"} }, "compare not allowed"},
	}
	for _, c := range cases {
		r := durabilityRow()
		c.mut(&r)
		err := r.Validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: error %v, want it to contain %q", c.name, err, c.want)
		}
	}
}

// durabilityRepo writes a small repository: the tests the cells cite, a metrics
// file, and docs/durability.md with the given text.
func durabilityRepo(t *testing.T, doc string) string {
	t.Helper()
	root := t.TempDir()
	write := func(rel, body string) {
		p := filepath.Join(root, filepath.FromSlash(rel))
		if err := os.MkdirAll(filepath.Dir(p), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(p, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write("internal/metrics/m.go", `package metrics
var X = NewCounterVec("lakehouse_dropped_total", "reason")`)
	write("internal/pkg/a_test.go", "package pkg\nfunc TestA(t *testing.T) {}\n")
	write("lakehouse-traces/internal/pkg/a_test.go", "package pkg\nfunc TestA(t *testing.T) {}\n")
	write("tests/e2e/chaos_x_test.go", "package e2e\nfunc TestChaosX(t *testing.T) {}\n")
	write("tests/e2e/chaos_x_traces_test.go", "package e2e\nfunc TestChaosXTraces(t *testing.T) {}\n")
	write("scripts/ci/tests/compare_test.sh", "#!/bin/bash\n# case hot-100y\n")
	write("docs/durability.md", doc)
	return root
}

func twinRows() *Registry {
	mk := func(sig, test string) Row {
		r := durabilityRow()
		r.ID = "lh.durability.fault_one_" + sig
		r.Durability.Signal = sig
		r.Durability.Metric = "lakehouse_dropped_total"
		r.Refs = &Refs{Tests: []string{test}}
		return r
	}
	return &Registry{Rows: []Row{mk("logs", "internal/pkg/a_test.go#TestA"), mk("traces", "lakehouse-traces/internal/pkg/a_test.go#TestA")}}
}

const goodDoc = "The cells are lh.durability.fault_one_* and nothing else.\n"

func TestCheckDurability_Consistent(t *testing.T) {
	if errs := CheckDurability(twinRows(), durabilityRepo(t, goodDoc)); len(errs) != 0 {
		t.Fatalf("a consistent registry reported %v", errs)
	}
	// A cell can also be named exactly.
	doc := "lh.durability.fault_one_logs and lh.durability.fault_one_traces\n"
	if errs := CheckDurability(twinRows(), durabilityRepo(t, doc)); len(errs) != 0 {
		t.Fatalf("exact names reported %v", errs)
	}
	// An end-to-end test serves a cell when its file name says the signal.
	reg := twinRows()
	reg.Rows[0].Refs.Tests = []string{"tests/e2e/chaos_x_test.go#TestChaosX"}
	reg.Rows[1].Refs.Tests = []string{"tests/e2e/chaos_x_traces_test.go#TestChaosXTraces"}
	if errs := CheckDurability(reg, durabilityRepo(t, goodDoc)); len(errs) != 0 {
		t.Fatalf("end-to-end refs reported %v", errs)
	}
	// A comparison script under scripts/ci/tests serves both signals.
	reg = twinRows()
	reg.Rows[0].Refs.Tests = append(reg.Rows[0].Refs.Tests, "scripts/ci/tests/compare_test.sh#hot-100y")
	reg.Rows[1].Refs.Tests = append(reg.Rows[1].Refs.Tests, "scripts/ci/tests/compare_test.sh#hot-100y")
	if errs := CheckDurability(reg, durabilityRepo(t, goodDoc)); len(errs) != 0 {
		t.Fatalf("script refs reported %v", errs)
	}
}

func TestCheckDurability_Rejects(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Registry) (doc string)
		want string
	}{
		{"a test that does not exist", func(r *Registry) string {
			r.Rows[0].Refs.Tests = []string{"internal/pkg/a_test.go#TestGone"}
			return goodDoc
		}, "no `func TestGone(`"},
		{"a file that does not exist", func(r *Registry) string {
			r.Rows[0].Refs.Tests = []string{"internal/pkg/nope_test.go#TestA"}
			return goodDoc
		}, "no such file"},
		{"a ref without a test name", func(r *Registry) string {
			r.Rows[0].Refs.Tests = []string{"internal/pkg/a_test.go"}
			return goodDoc
		}, "must name a test"},
		{"a traces cell citing a logs test", func(r *Registry) string {
			r.Rows[1].Refs.Tests = []string{"internal/pkg/a_test.go#TestA"}
			return goodDoc
		}, "unit tests live under lakehouse-traces/"},
		{"a logs cell citing a traces test", func(r *Registry) string {
			r.Rows[0].Refs.Tests = []string{"lakehouse-traces/internal/pkg/a_test.go#TestA"}
			return goodDoc
		}, "live outside lakehouse-traces/"},
		{"a logs cell citing the traces end-to-end test", func(r *Registry) string {
			r.Rows[0].Refs.Tests = []string{"tests/e2e/chaos_x_traces_test.go#TestChaosXTraces"}
			return goodDoc
		}, "cites an end-to-end test of the traces binary"},
		{"a missing twin", func(r *Registry) string {
			r.Rows = r.Rows[:1]
			return goodDoc
		}, "no traces twin"},
		{"twins with different expect", func(r *Registry) string {
			r.Rows[1].Expect = ExpectDiffer
			return goodDoc
		}, "but its twin"},
		{"twins with different guarantees", func(r *Registry) string {
			r.Rows[1].Durability.Guarantees = []string{"no-loss"}
			r.Rows[1].Durability.Metric = ""
			return goodDoc
		}, "differ from its twin"},
		{"a metric that is not registered", func(r *Registry) string {
			r.Rows[0].Durability.Metric = "lakehouse_nowhere_total"
			return goodDoc
		}, "not registered in internal/metrics"},
		{"a cell the doc does not mention", func(r *Registry) string { return "no cells here\n" }, "does not mention the cell"},
		{"a cell id in the doc that does not exist", func(r *Registry) string {
			return goodDoc + "and lh.durability.ghost_cell_logs\n"
		}, "which is no durability cell"},
	}
	for _, c := range cases {
		reg := twinRows()
		doc := c.mut(reg)
		errs := CheckDurability(reg, durabilityRepo(t, doc))
		if !strings.Contains(strings.Join(errs, "\n"), c.want) {
			t.Errorf("%s: got %v, want an error containing %q", c.name, errs, c.want)
		}
	}
}

// The real registry: every durability cell cites tests that exist, has its twin,
// registers its metric and is named in docs/durability.md. This is the check that
// makes the cells more than labels; `confgen -check` runs it too.
func TestDurability_RealRegistry(t *testing.T) {
	reg, err := LoadDir("rows")
	if err != nil {
		t.Fatal(err)
	}
	n := 0
	for _, r := range reg.Rows {
		if r.Kind == KindDurability {
			n++
		}
	}
	if n == 0 {
		t.Fatal("the registry has no durability cells")
	}
	if errs := CheckDurability(reg, "../../.."); len(errs) != 0 {
		t.Fatalf("durability cells are inconsistent:\n  %s", strings.Join(errs, "\n  "))
	}
}
