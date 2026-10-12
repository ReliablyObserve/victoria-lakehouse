package registry

import (
	"strings"
	"testing"
)

func durabilityRow() Row {
	return Row{
		ID: "lh.durability.pre1970_rows_logs", Title: "t", Surface: SurfaceLH, Kind: KindDurability,
		Origin: OriginLHAddition, Expect: ExpectPass, Targets: []Target{TargetCold},
		Layers: []string{"chaos"}, Pending: true,
		Durability: &Durability{Fault: "pre1970_rows", Mode: "buffer-flush", Signal: "logs", Guarantee: "refused-counted"},
		Refs:       &Refs{Tests: []string{"internal/storage/parquets3/x_test.go#TestX"}},
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
		{"bad guarantee", func(r *Row) { r.Durability.Guarantee = "best-effort" }, "durability.guarantee"},
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
