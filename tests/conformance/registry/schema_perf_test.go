package registry

import (
	"strings"
	"testing"
)

func perfRow(expect Expect) Row {
	r := Row{
		ID: "vl.perf.field_values_level.x", Title: "t", Surface: SurfaceVL, Kind: KindSelect,
		Origin: OriginNative, Expect: expect, Targets: []Target{TargetCold},
		Seed:     []string{"logs.fieldmeta"},
		Upstream: &Upstream{Route: "/select/logsql/field_values"},
		Request:  &Request{Method: "GET", Path: "/select/logsql/field_values", Params: map[string]string{"query": "*", "field": "level"}},
		Compare:  &Compare{Type: "values-with-hits"},
		Layers:   []string{"perf"},
		Pending:  true,
		Perf: &Perf{
			Cell:     "fv_level/pmeta=false/layout=flushed/window=whole/filter=none/s3=0ms",
			Counters: &PerfCounters{S3Gets: 24, S3Bytes: 1_720_000, RowGroups: 24, Pages: 200, Path: "scan"},
		},
	}
	if expect == ExpectPass {
		r.Perf.Budget = &PerfBudget{P50Ms: 7.6, P90Ms: 8.1, Valid: "10/10"}
	} else {
		r.DifferNote = "not exact yet"
	}
	return r
}

func TestPerf_ValidRows(t *testing.T) {
	for _, e := range []Expect{ExpectPass, ExpectDiffer} {
		r := perfRow(e)
		if err := r.Validate(); err != nil {
			t.Errorf("expect=%s: %v", e, err)
		}
	}
}

func TestPerf_Invalid(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Row)
		want string
	}{
		{"perf layer without block", func(r *Row) { r.Perf = nil }, "perf block required"},
		{"block without perf layer", func(r *Row) { r.Layers = []string{"api"} }, "only allowed on a perf-layer row"},
		{"no cell", func(r *Row) { r.Perf.Cell = " " }, "perf.cell required"},
		{"no counters", func(r *Row) { r.Perf.Counters = nil }, "perf.counters required"},
		{"negative counter", func(r *Row) { r.Perf.Counters.S3Gets = -1 }, "must not be negative"},
		{"no path", func(r *Row) { r.Perf.Counters.Path = "" }, "path required"},
		{"pass without budget", func(r *Row) { r.Perf.Budget = nil }, "budget required when expect=pass"},
		{"p90 below p50", func(r *Row) { r.Perf.Budget.P90Ms = 1 }, "0 < p50_ms <= p90_ms"},
		{"zero p50", func(r *Row) { r.Perf.Budget.P50Ms = 0 }, "0 < p50_ms <= p90_ms"},
		{"bad valid", func(r *Row) { r.Perf.Budget.Valid = "all" }, "must be k/N"},
		{"valid over zero", func(r *Row) { r.Perf.Budget.Valid = "0/0" }, "must be k/N"},
	}
	for _, c := range cases {
		r := perfRow(ExpectPass)
		c.mut(&r)
		err := r.Validate()
		if err == nil || !strings.Contains(err.Error(), c.want) {
			t.Errorf("%s: err = %v, want it to mention %q", c.name, err, c.want)
		}
	}
}

// A wrong answer is never timed, so a differ row must not carry a budget.
func TestPerf_DifferRowRejectsBudget(t *testing.T) {
	r := perfRow(ExpectDiffer)
	r.Perf.Budget = &PerfBudget{P50Ms: 1, P90Ms: 2, Valid: "0/10"}
	err := r.Validate()
	if err == nil || !strings.Contains(err.Error(), "a wrong answer gets no budget") {
		t.Fatalf("err = %v, want the no-budget rule", err)
	}
}

func TestPerf_FieldMetaSeedsAreKnown(t *testing.T) {
	for _, s := range []string{"logs.fieldmeta", "traces.fieldmeta"} {
		if !Seeds[s] {
			t.Errorf("seed %s is not registered", s)
		}
	}
}
