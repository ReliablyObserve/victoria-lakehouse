package registry

import (
	"strings"
	"testing"
)

func validRow() Row {
	return Row{
		ID: "vl.select.query.exact_filter", Title: "query exact filter",
		Surface: SurfaceVL, Kind: KindSelect, Origin: OriginNative, Expect: ExpectPass,
		Targets: []Target{TargetHot, TargetCold}, Seed: []string{"logs.base"},
		Upstream: &Upstream{Route: "/select/logsql/query"},
		Request:  &Request{Method: "GET", Path: "/select/logsql/query", Params: map[string]string{"query": `level:="ERROR"`}},
		Compare:  &Compare{Type: "ndjson-multiset"},
		Layers:   []string{"api"},
		Pending:  true,
	}
}

func TestRow_Validate_OK(t *testing.T) {
	r := validRow()
	if err := r.Validate(); err != nil {
		t.Fatalf("valid row rejected: %v", err)
	}
}

func TestRow_Validate_Rejects(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Row)
		want string
	}{
		{"bad id", func(r *Row) { r.ID = "Query Exact" }, "id"},
		{"bad surface", func(r *Row) { r.Surface = "vx" }, "surface"},
		{"differ needs note", func(r *Row) { r.Expect = ExpectDiffer; r.DifferNote = "" }, "differ_note"},
		{"pass needs seed", func(r *Row) { r.Seed = nil }, "seed"},
		{"no targets", func(r *Row) { r.Targets = nil }, "targets"},
		{"native needs upstream", func(r *Row) { r.Upstream = nil }, "upstream"},
		{"lh-addition must not cite upstream", func(r *Row) { r.Origin = OriginLHAddition }, "upstream"},
		{"non-ui needs request", func(r *Row) { r.Request = nil }, "request"},
		{"unknown comparator", func(r *Row) { r.Compare.Type = "magic" }, "compare"},
		{"absent must not have seed", func(r *Row) { r.Expect = ExpectAbsent }, "seed"},
		{"empty title", func(r *Row) { r.Title = "" }, "title"},
		{"bad kind", func(r *Row) { r.Kind = "bad" }, "kind"},
		{"bad origin", func(r *Row) { r.Origin = "bad" }, "origin"},
		{"bad expect", func(r *Row) { r.Expect = "bad" }, "expect"},
		{"invalid target", func(r *Row) { r.Targets = []Target{"bad"} }, "targets"},
		{"no compare", func(r *Row) { r.Compare = nil }, "compare"},
		{"no layers", func(r *Row) { r.Layers = nil }, "layers"},
		{"differ note whitespace only", func(r *Row) { r.Expect = ExpectDiffer; r.DifferNote = "   " }, "differ_note"},
		{"shim needs upstream", func(r *Row) { r.Origin = OriginLHShim; r.Upstream = nil }, "upstream"},
		{"shim with empty upstream", func(r *Row) { r.Origin = OriginLHShim; r.Upstream = &Upstream{} }, "upstream"},
		{"lh-addition must not cite upstream", func(r *Row) { r.Origin = OriginLHAddition; r.Upstream = &Upstream{Route: "/x"} }, "upstream"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := validRow()
			c.mut(&r)
			err := r.Validate()
			if err == nil || !strings.Contains(err.Error(), c.want) {
				t.Fatalf("want error mentioning %q, got %v", c.want, err)
			}
		})
	}
}

func TestRow_Validate_FlagKind_NoSeed(t *testing.T) {
	r := validRow()
	r.Kind = KindFlag
	r.Seed = nil
	if err := r.Validate(); err != nil {
		t.Fatalf("flag kind should not require seed, but got: %v", err)
	}
}

func TestRow_Validate_UIKind_NoRequest(t *testing.T) {
	r := validRow()
	r.Kind = KindUI
	r.Request = nil
	if err := r.Validate(); err != nil {
		t.Fatalf("ui kind should not require request, but got: %v", err)
	}
}

func TestRow_Validate_ExpectUnsupported(t *testing.T) {
	r := validRow()
	r.Expect = ExpectUnsupported
	r.Seed = nil
	if err := r.Validate(); err != nil {
		t.Fatalf("unsupported expect should be valid, but got: %v", err)
	}
}

func TestRow_Validate_TargetGlobal(t *testing.T) {
	r := validRow()
	r.Targets = []Target{TargetGlobal}
	if err := r.Validate(); err != nil {
		t.Fatalf("valid row with TargetGlobal rejected: %v", err)
	}
}

func TestRow_Validate_AllSurfaces(t *testing.T) {
	for _, surface := range []Surface{SurfaceVL, SurfaceVT, SurfaceLH} {
		t.Run(string(surface), func(t *testing.T) {
			r := validRow()
			r.Surface = surface
			if err := r.Validate(); err != nil {
				t.Fatalf("valid row with surface %s rejected: %v", surface, err)
			}
		})
	}
}

func TestRow_Validate_AllKinds(t *testing.T) {
	kinds := []Kind{KindSelect, KindInsert, KindPipe, KindFilter, KindStats, KindTraceQL, KindFlag, KindUI, KindAdmin, KindInternal}
	for _, kind := range kinds {
		t.Run(string(kind), func(t *testing.T) {
			r := validRow()
			r.Kind = kind
			if kind == KindUI || kind == KindFlag {
				r.Request = nil
				if kind == KindFlag {
					r.Seed = nil
				}
			}
			if err := r.Validate(); err != nil {
				t.Fatalf("valid row with kind %s rejected: %v", kind, err)
			}
		})
	}
}

func TestRow_Validate_AllExpectValues(t *testing.T) {
	for _, expect := range []Expect{ExpectPass, ExpectDiffer, ExpectAbsent, ExpectUnsupported} {
		t.Run(string(expect), func(t *testing.T) {
			r := validRow()
			r.Expect = expect
			if expect == ExpectDiffer {
				r.DifferNote = "some note"
			}
			if expect == ExpectAbsent || expect == ExpectUnsupported {
				r.Seed = nil
			}
			if err := r.Validate(); err != nil {
				t.Fatalf("valid row with expect %s rejected: %v", expect, err)
			}
		})
	}
}

func TestRow_Validate_AllOrigins(t *testing.T) {
	for _, origin := range []Origin{OriginNative, OriginLHShim, OriginLHAddition} {
		t.Run(string(origin), func(t *testing.T) {
			r := validRow()
			r.Origin = origin
			if origin == OriginLHAddition {
				r.Upstream = nil
			}
			if err := r.Validate(); err != nil {
				t.Fatalf("valid row with origin %s rejected: %v", origin, err)
			}
		})
	}
}

func TestUpstream_Key(t *testing.T) {
	cases := []struct {
		name     string
		upstream *Upstream
		expected string
	}{
		{"route", &Upstream{Route: "/select/logsql/query"}, "route:/select/logsql/query"},
		{"pipe", &Upstream{Pipe: "coalesce"}, "pipe:coalesce"},
		{"filter", &Upstream{Filter: "range"}, "filter:range"},
		{"stats", &Upstream{Stats: "quantile"}, "stats:quantile"},
		{"traceql", &Upstream{TraceQL: "histogram_over_time"}, "traceql:histogram_over_time"},
		{"flag", &Upstream{Flag: "search.maxTraces"}, "flag:search.maxTraces"},
		{"empty", &Upstream{}, ":"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			key := c.upstream.Key()
			if key != c.expected {
				t.Fatalf("want %q, got %q", c.expected, key)
			}
		})
	}
}
