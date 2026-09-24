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
		// ID validation
		{"bad id", func(r *Row) { r.ID = "Query Exact" }, "must match"},
		{"id only 2 segments", func(r *Row) { r.ID = "vl.select" }, "must match"},
		{"id with slash", func(r *Row) { r.ID = "vl.select/query.x" }, "must match"},
		{"id prefix vl mismatch surface", func(r *Row) { r.ID = "vl.test.x"; r.Surface = SurfaceVT }, "does not match surface"},
		{"id prefix vt mismatch surface", func(r *Row) { r.ID = "vt.test.x"; r.Surface = SurfaceVL }, "does not match surface"},
		{"id prefix lh mismatch surface", func(r *Row) { r.ID = "lh.test.x"; r.Surface = SurfaceVT }, "does not match surface"},

		// Title validation
		{"empty title", func(r *Row) { r.Title = "" }, "title"},
		{"whitespace title", func(r *Row) { r.Title = "   " }, "title"},

		// Surface validation
		{"bad surface", func(r *Row) { r.Surface = "vx" }, "surface"},

		// Kind validation
		{"bad kind", func(r *Row) { r.Kind = "bad" }, "kind"},

		// Origin validation
		{"bad origin", func(r *Row) { r.Origin = "bad" }, "origin"},

		// Expect validation
		{"bad expect", func(r *Row) { r.Expect = "bad" }, "expect"},

		// DifferNote validation
		{"differ needs note", func(r *Row) { r.Expect = ExpectDiffer; r.DifferNote = "" }, "differ_note"},
		{"differ note whitespace only", func(r *Row) { r.Expect = ExpectDiffer; r.DifferNote = "   " }, "differ_note"},

		// Targets validation
		{"no targets", func(r *Row) { r.Targets = nil }, "targets"},
		{"invalid target", func(r *Row) { r.Targets = []Target{"bad"} }, "targets:"},
		{"duplicate targets", func(r *Row) { r.Targets = []Target{TargetHot, TargetHot} }, "targets: duplicate"},

		// Seed validation
		{"pass needs seed", func(r *Row) { r.Seed = nil }, "seed"},
		{"unknown seed", func(r *Row) { r.Seed = []string{"nope"} }, `seed "nope" unknown`},
		{"absent must not have seed", func(r *Row) { r.Expect = ExpectAbsent }, "seed"},
		// The allow-list check applies to every row with a seed, not just
		// pass/differ: an expect=unsupported row with an unknown seed name
		// must still be rejected (there is no Expect case that skips it).
		{"unsupported with unknown seed", func(r *Row) { r.Expect = ExpectUnsupported; r.Seed = []string{"nope"} }, `seed "nope" unknown`},

		// Since validation
		{"since invalid key", func(r *Row) { r.Since = map[string]string{"xx": "1.0"} }, "since: key"},
		{"since empty value", func(r *Row) { r.Since = map[string]string{"vl": ""} }, "since:"},

		// Upstream validation
		{"native needs upstream", func(r *Row) { r.Upstream = nil }, "upstream"},
		{"shim needs upstream", func(r *Row) { r.Origin = OriginLHShim; r.Upstream = nil }, "upstream"},
		{"shim with empty upstream", func(r *Row) { r.Origin = OriginLHShim; r.Upstream = &Upstream{} }, "upstream"},
		{"upstream multiple fields set", func(r *Row) { r.Upstream = &Upstream{Route: "/x", Flag: "y"} }, "exactly one"},
		{"lh-addition with upstream field set", func(r *Row) { r.Origin = OriginLHAddition; r.Upstream = &Upstream{Route: "/x"} }, "upstream"},

		// Request validation
		{"non-ui needs request", func(r *Row) { r.Request = nil }, "request"},
		{"request invalid method", func(r *Row) { r.Request.Method = "INVALID" }, "request.method"},
		{"request empty path", func(r *Row) { r.Request.Path = "" }, "request.path"},
		{"request path no slash", func(r *Row) { r.Request.Path = "query" }, "request.path"},

		// Compare validation
		{"no compare", func(r *Row) { r.Compare = nil }, "compare"},
		{"unknown comparator", func(r *Row) { r.Compare.Type = "magic" }, "compare"},
		{"ui kind requires ui comparator", func(r *Row) { r.Kind = KindUI; r.Request = nil; r.Compare.Type = "ndjson-multiset" }, "incompatible with kind"},
		{"non-ui kind cannot use ui comparator", func(r *Row) { r.Compare.Type = "ui" }, "incompatible with kind"},
		{"absent expect requires absent or status", func(r *Row) { r.Expect = ExpectAbsent; r.Seed = nil; r.Compare.Type = "ndjson-multiset" }, "incompatible with expect=absent"},

		// Layers validation
		{"no layers", func(r *Row) { r.Layers = nil }, "layers"},
		{"invalid layer", func(r *Row) { r.Layers = []string{"apis"} }, "layers:"},
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

func TestRow_Validate_MultipleViolations(t *testing.T) {
	// Test two simultaneous violations with proper error format
	r := validRow()
	r.Title = ""             // missing title
	r.Layers = []string{"x"} // invalid layer
	err := r.Validate()
	if err == nil {
		t.Fatalf("expected validation error, got nil")
	}
	errMsg := err.Error()
	// Should have row id prefix
	if !strings.HasPrefix(errMsg, "row vl.select.query.exact_filter:") {
		t.Fatalf("error should start with row id prefix, got: %v", errMsg)
	}
	// Should have both violations in sorted order separated by "; "
	if !strings.Contains(errMsg, "layers:") || !strings.Contains(errMsg, "title") {
		t.Fatalf("error should contain both violations, got: %v", errMsg)
	}
	parts := strings.Split(errMsg[34:], "; ") // skip "row vl.select.query.exact_filter: "
	if len(parts) < 2 {
		t.Fatalf("error should have at least 2 violations separated by '; ', got: %v", errMsg)
	}
	// Verify sorted order (layers: < title)
	if parts[0] > parts[1] {
		t.Fatalf("violations should be in sorted order, got: %v, %v", parts[0], parts[1])
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
	r.Compare.Type = "ui"
	if err := r.Validate(); err != nil {
		t.Fatalf("ui kind should not require request, but got: %v", err)
	}
}

func TestRow_Validate_UIKindRequiresUIComparator(t *testing.T) {
	r := validRow()
	r.Kind = KindUI
	r.Request = nil
	r.Compare.Type = "ui"
	if err := r.Validate(); err != nil {
		t.Fatalf("ui kind with ui comparator should be valid, but got: %v", err)
	}
}

func TestRow_Validate_ExpectAbsentRequiresAbsentOrStatusComparator(t *testing.T) {
	for _, ct := range []string{"absent", "status"} {
		t.Run(ct, func(t *testing.T) {
			r := validRow()
			r.Expect = ExpectAbsent
			r.Seed = nil
			r.Compare.Type = ct
			if err := r.Validate(); err != nil {
				t.Fatalf("absent expect with %s comparator should be valid, but got: %v", ct, err)
			}
		})
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
			// Adjust id to match surface
			prefix := string(surface)
			r.ID = prefix + ".select.query.exact_filter"
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
			if kind == KindUI {
				r.Request = nil
				r.Compare.Type = "ui"
			} else if kind == KindFlag {
				r.Request = nil
				r.Seed = nil
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
			if expect == ExpectAbsent {
				r.Seed = nil
				r.Compare.Type = "absent"
			} else if expect == ExpectUnsupported {
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

func TestRow_Validate_RequestMethods(t *testing.T) {
	validMethods := []string{"GET", "POST", "PUT", "DELETE", "HEAD", "PATCH"}
	for _, method := range validMethods {
		t.Run(method, func(t *testing.T) {
			r := validRow()
			r.Request.Method = method
			if err := r.Validate(); err != nil {
				t.Fatalf("valid request method %s rejected: %v", method, err)
			}
		})
	}
}

func TestRow_Validate_ValidLayers(t *testing.T) {
	validLayers := []string{"api", "ui", "perf", "chaos"}
	for _, layer := range validLayers {
		t.Run(layer, func(t *testing.T) {
			r := validRow()
			r.Layers = []string{layer}
			if layer == "perf" {
				// A perf-layer row must carry its perf block (schema_perf_test.go).
				r.Perf = &Perf{Cell: "c", Counters: &PerfCounters{Path: "scan"}}
				if r.Expect == ExpectPass {
					r.Perf.Budget = &PerfBudget{P50Ms: 1, P90Ms: 1, Valid: "1/1"}
				}
			}
			if err := r.Validate(); err != nil {
				t.Fatalf("valid layer %s rejected: %v", layer, err)
			}
		})
	}
}

func TestRow_Validate_ValidSeeds(t *testing.T) {
	validSeeds := []string{"logs.base", "logs.edge", "logs.streams", "traces.base", "traces.sg", "tenants.iso", "logs.fieldmeta", "traces.fieldmeta"}
	for _, seed := range validSeeds {
		t.Run(seed, func(t *testing.T) {
			r := validRow()
			r.Seed = []string{seed}
			if err := r.Validate(); err != nil {
				t.Fatalf("valid seed %s rejected: %v", seed, err)
			}
		})
	}
}

func TestRow_Validate_ValidSinceKeys(t *testing.T) {
	validKeys := []string{"vl", "vt"}
	for _, key := range validKeys {
		t.Run(key, func(t *testing.T) {
			r := validRow()
			r.Since = map[string]string{key: "1.51.0"}
			if err := r.Validate(); err != nil {
				t.Fatalf("valid since key %s rejected: %v", key, err)
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
		{"nil", nil, ":"},
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

func TestUpstream_IsZero(t *testing.T) {
	cases := []struct {
		name     string
		upstream *Upstream
		expected bool
	}{
		{"nil", nil, true},
		{"empty", &Upstream{}, true},
		{"route", &Upstream{Route: "/x"}, false},
		{"pipe", &Upstream{Pipe: "x"}, false},
		{"filter", &Upstream{Filter: "x"}, false},
		{"stats", &Upstream{Stats: "x"}, false},
		{"traceql", &Upstream{TraceQL: "x"}, false},
		{"flag", &Upstream{Flag: "x"}, false},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			result := c.upstream.IsZero()
			if result != c.expected {
				t.Fatalf("want %v, got %v", c.expected, result)
			}
		})
	}
}
