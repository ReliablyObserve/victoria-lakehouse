package conformance

import (
	"os"
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/inventory"
	"github.com/ReliablyObserve/victoria-lakehouse/tests/conformance/registry"
)

func TestCheckDrift_UnmappedAndStale(t *testing.T) {
	inv := &inventory.Inventory{
		VLVersion: "v1.50.0",
		VTVersion: "v0.9.0",
		Items: []inventory.Item{
			{Kind: "route", Name: "/select/logsql/query", Source: "app/vlselect/main.go"},
			{Kind: "route", Name: "/select/logsql/new_thing", Source: "app/vlselect/main.go"},
			{Kind: "flag", Name: "search.brandNew", Source: "app/vlselect/main.go"},
		},
	}
	reg, err := registry.LoadDir("registry/testdata/valid")
	if err != nil {
		t.Fatal(err)
	}
	stale := registry.Row{
		ID:       "vl.select.gone.basic",
		Title:    "gone",
		Surface:  registry.SurfaceVL,
		Kind:     registry.KindSelect,
		Origin:   registry.OriginNative,
		Expect:   registry.ExpectPass,
		Targets:  []registry.Target{registry.TargetHot},
		Seed:     []string{"logs.base"},
		Upstream: &registry.Upstream{Route: "/select/logsql/gone"},
		Request:  &registry.Request{Method: "GET", Path: "/select/logsql/gone"},
		Compare:  &registry.Compare{Type: "status"},
		Layers:   []string{"api"},
		Pending:  true,
	}
	reg.Rows = append(reg.Rows, stale)
	rep := CheckDrift(inv, reg)
	if len(rep.Unmapped) != 1 || rep.Unmapped[0].Name != "/select/logsql/new_thing" {
		t.Fatalf("unmapped: %+v", rep.Unmapped)
	}
	if len(rep.Stale) != 1 || rep.Stale[0] != "vl.select.gone.basic" {
		t.Fatalf("stale: %v", rep.Stale)
	}
	if len(rep.FlagWarnings) != 1 || rep.FlagWarnings[0].Name != "search.brandNew" {
		t.Fatalf("flag warnings: %+v", rep.FlagWarnings)
	}
	hard := rep.HardFailures()
	if len(hard) != 2 {
		t.Fatalf("expected 2 hard failures, got %d: %v", len(hard), hard)
	}
	hardStr := strings.Join(hard, "\n")
	if !strings.Contains(hardStr, "upstream: { route: /select/logsql/new_thing }") {
		t.Fatalf("hard failures should list the unmapped route with a row stub: %v", hard)
	}
}

func TestCheckDrift_AbsentRowsMayCiteNonInventoryRoutes(t *testing.T) {
	inv := &inventory.Inventory{}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	reg.Rows = []registry.Row{{
		ID:       "vt.tempo.metrics_instant.absent",
		Title:    "absent",
		Surface:  registry.SurfaceVT,
		Kind:     registry.KindSelect,
		Origin:   registry.OriginNative,
		Expect:   registry.ExpectAbsent,
		Targets:  []registry.Target{registry.TargetHot},
		Upstream: &registry.Upstream{Route: "/select/tempo/api/metrics/instant"},
		Request:  &registry.Request{Method: "GET", Path: "/select/tempo/api/metrics/instant"},
		Compare:  &registry.Compare{Type: "absent"},
		Layers:   []string{"api"},
	}}
	if rep := CheckDrift(inv, reg); len(rep.Stale) != 0 {
		t.Fatalf("absent rows are allowed to cite routes upstream lacks: %v", rep.Stale)
	}
}

// TestCheckDrift_PendingBumpRows ensures rows with a newer Since version than
// the inventory are not marked stale.
func TestCheckDrift_PendingBumpRows(t *testing.T) {
	inv := &inventory.Inventory{
		VLVersion: "v1.50.0",
		VTVersion: "v0.9.2",
	}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	// Row with since.vt = "0.9.3" > inventory VTVersion "v0.9.2"
	reg.Rows = []registry.Row{{
		ID:       "vt.select.traces.routes.pending",
		Title:    "pending bump",
		Surface:  registry.SurfaceVT,
		Kind:     registry.KindSelect,
		Origin:   registry.OriginNative,
		Expect:   registry.ExpectPass,
		Targets:  []registry.Target{registry.TargetHot},
		Seed:     []string{"traces.base"},
		Upstream: &registry.Upstream{Route: "/select/tempo/api/traces/"},
		Request:  &registry.Request{Method: "GET", Path: "/select/tempo/api/traces/"},
		Compare:  &registry.Compare{Type: "status"},
		Layers:   []string{"api"},
		Since:    map[string]string{"vt": "0.9.3"},
	}}
	rep := CheckDrift(inv, reg)
	if len(rep.Stale) != 0 {
		t.Fatalf("pending-bump rows should not be stale, got: %v", rep.Stale)
	}
	if len(rep.PendingBump) != 1 || rep.PendingBump[0] != "vt.select.traces.routes.pending" {
		t.Fatalf("expected 1 pending bump, got: %v", rep.PendingBump)
	}
	hard := rep.HardFailures()
	if len(hard) != 0 {
		t.Fatalf("pending bump rows should not cause hard failures, got: %v", hard)
	}
}

// TestCheckDrift_PrefixCoverage ensures a route item with a trailing slash
// is covered by any row citing a route with that prefix.
func TestCheckDrift_PrefixCoverage(t *testing.T) {
	inv := &inventory.Inventory{
		VLVersion: "v1.50.0",
		VTVersion: "v0.9.0",
		Items: []inventory.Item{
			{Kind: "route", Name: "/select/jaeger/", Source: "app/vlselect/jaeger.go"},
		},
	}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	// Row citing /select/jaeger/api/services which starts with /select/jaeger/
	reg.Rows = []registry.Row{{
		ID:       "vl.select.jaeger.services",
		Title:    "jaeger services",
		Surface:  registry.SurfaceVL,
		Kind:     registry.KindSelect,
		Origin:   registry.OriginNative,
		Expect:   registry.ExpectPass,
		Targets:  []registry.Target{registry.TargetHot},
		Seed:     []string{"logs.base"},
		Upstream: &registry.Upstream{Route: "/select/jaeger/api/services"},
		Request:  &registry.Request{Method: "GET", Path: "/select/jaeger/api/services"},
		Compare:  &registry.Compare{Type: "status"},
		Layers:   []string{"api"},
	}}
	rep := CheckDrift(inv, reg)
	if len(rep.Unmapped) != 0 {
		t.Fatalf("prefix-covered item should not be unmapped: %v", rep.Unmapped)
	}
}

// TestDrift_RealRegistry checks against the generated inventory.
// It skips gracefully if the file is missing unless CONFORMANCE_REQUIRE_DEPS=1.
func TestDrift_RealRegistry(t *testing.T) {
	inv, err := inventory.Read("inventory.generated.yaml")
	if err != nil {
		// Only skip if the file does not exist; other errors (permission, EISDIR) must fail.
		if os.IsNotExist(err) && os.Getenv("CONFORMANCE_REQUIRE_DEPS") != "1" {
			t.Skip("inventory.generated.yaml missing — run make conformance-gen (or set CONFORMANCE_REQUIRE_DEPS=1 to fail)")
		}
		t.Fatalf("inventory.generated.yaml missing — run make conformance-gen: %v", err)
	}
	reg, err := registry.LoadDir("registry/rows")
	if err != nil {
		t.Fatal(err)
	}
	rep := CheckDrift(inv, reg)
	if hard := rep.HardFailures(); len(hard) > 0 {
		t.Fatalf("registry drift:\n%s", strings.Join(hard, "\n"))
	}
	for _, w := range rep.FlagWarnings {
		t.Logf("WARNING: upstream flag %s (%s) has no registry row — add one in rows/flags/matters.yaml if it changes behavior or compatibility", w.Name, w.Source)
	}
}

func TestCheckDrift_Summary(t *testing.T) {
	rep := DriftReport{
		Unmapped:     []inventory.Item{{Kind: "route", Name: "/foo"}},
		Stale:        []string{"vl.foo.bar", "vl.foo.baz"},
		FlagWarnings: []inventory.Item{{Kind: "flag", Name: "search.x"}},
	}
	rep.PendingBump = []string{"vt.foo.bar.pending"}
	summary := rep.Summary()
	if !strings.Contains(summary, "1 unmapped") || !strings.Contains(summary, "2 stale") ||
		!strings.Contains(summary, "1 pending-bump") || !strings.Contains(summary, "1 flag warning") {
		t.Fatalf("unexpected summary: %s", summary)
	}
}

// TestCheckDrift_NonRouteCoverage tests pipe, filter, stats, traceql kinds
// are covered only with exact matches (no prefix).
func TestCheckDrift_NonRouteCoverage(t *testing.T) {
	inv := &inventory.Inventory{
		VLVersion: "v1.50.0",
		VTVersion: "v0.9.0",
		Items: []inventory.Item{
			{Kind: "pipe", Name: "coalesce", Source: "lib/logstorage/pipe_coalesce.go"},
			{Kind: "filter", Name: "range", Source: "lib/logstorage/filter_range.go"},
			{Kind: "stats", Name: "quantile", Source: "lib/logstorage/stats_quantile.go"},
			{Kind: "traceql", Name: "histogram_over_time", Source: "lib/traceql/func.go"},
		},
	}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	reg.Rows = []registry.Row{
		{
			ID:       "vl.logstorage.pipe.coalesce",
			Title:    "coalesce",
			Surface:  registry.SurfaceVL,
			Kind:     registry.KindPipe,
			Origin:   registry.OriginNative,
			Expect:   registry.ExpectPass,
			Targets:  []registry.Target{registry.TargetHot},
			Seed:     []string{"logs.base"},
			Upstream: &registry.Upstream{Pipe: "coalesce"},
			Request:  &registry.Request{Method: "GET", Path: "/"},
			Compare:  &registry.Compare{Type: "status"},
			Layers:   []string{"api"},
		},
		{
			ID:       "vl.logstorage.filter.range",
			Title:    "range",
			Surface:  registry.SurfaceVL,
			Kind:     registry.KindFilter,
			Origin:   registry.OriginNative,
			Expect:   registry.ExpectPass,
			Targets:  []registry.Target{registry.TargetHot},
			Seed:     []string{"logs.base"},
			Upstream: &registry.Upstream{Filter: "range"},
			Request:  &registry.Request{Method: "GET", Path: "/"},
			Compare:  &registry.Compare{Type: "status"},
			Layers:   []string{"api"},
		},
		{
			ID:       "vl.logstorage.stats.quantile",
			Title:    "quantile",
			Surface:  registry.SurfaceVL,
			Kind:     registry.KindStats,
			Origin:   registry.OriginNative,
			Expect:   registry.ExpectPass,
			Targets:  []registry.Target{registry.TargetHot},
			Seed:     []string{"logs.base"},
			Upstream: &registry.Upstream{Stats: "quantile"},
			Request:  &registry.Request{Method: "GET", Path: "/"},
			Compare:  &registry.Compare{Type: "status"},
			Layers:   []string{"api"},
		},
		{
			ID:       "vt.traceql.func.histogram_over_time",
			Title:    "histogram_over_time",
			Surface:  registry.SurfaceVT,
			Kind:     registry.KindTraceQL,
			Origin:   registry.OriginNative,
			Expect:   registry.ExpectPass,
			Targets:  []registry.Target{registry.TargetHot},
			Seed:     []string{"traces.base"},
			Upstream: &registry.Upstream{TraceQL: "histogram_over_time"},
			Request:  &registry.Request{Method: "GET", Path: "/"},
			Compare:  &registry.Compare{Type: "status"},
			Layers:   []string{"api"},
		},
	}
	rep := CheckDrift(inv, reg)
	if len(rep.Unmapped) != 0 {
		t.Fatalf("non-route kinds should be covered by exact match, got unmapped: %v", rep.Unmapped)
	}
}

// TestCheckDrift_PendingBumpVL tests pending-bump with VL version.
func TestCheckDrift_PendingBumpVL(t *testing.T) {
	inv := &inventory.Inventory{
		VLVersion: "v1.50.0",
		VTVersion: "v0.9.0",
	}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	reg.Rows = []registry.Row{{
		ID:       "vl.new.feature.basic",
		Title:    "new feature",
		Surface:  registry.SurfaceVL,
		Kind:     registry.KindSelect,
		Origin:   registry.OriginNative,
		Expect:   registry.ExpectPass,
		Targets:  []registry.Target{registry.TargetHot},
		Seed:     []string{"logs.base"},
		Upstream: &registry.Upstream{Route: "/select/new"},
		Request:  &registry.Request{Method: "GET", Path: "/select/new"},
		Compare:  &registry.Compare{Type: "status"},
		Layers:   []string{"api"},
		Since:    map[string]string{"vl": "1.51.0"},
	}}
	rep := CheckDrift(inv, reg)
	if len(rep.PendingBump) != 1 {
		t.Fatalf("expected 1 pending bump, got: %v", rep.PendingBump)
	}
}

// TestCheckDrift_VersionCompare tests version comparison with different formats.
func TestCheckDrift_VersionCompare(t *testing.T) {
	tests := []struct {
		name     string
		inv      *inventory.Inventory
		since    map[string]string
		expectPB bool
	}{
		{
			name:     "equal versions",
			inv:      &inventory.Inventory{VTVersion: "v0.9.2"},
			since:    map[string]string{"vt": "0.9.2"},
			expectPB: false,
		},
		{
			name:     "older version",
			inv:      &inventory.Inventory{VTVersion: "v0.9.2"},
			since:    map[string]string{"vt": "0.9.1"},
			expectPB: false,
		},
		{
			name:     "newer patch",
			inv:      &inventory.Inventory{VTVersion: "v0.9.2"},
			since:    map[string]string{"vt": "0.9.3"},
			expectPB: true,
		},
		{
			name:     "newer minor",
			inv:      &inventory.Inventory{VTVersion: "v0.9.2"},
			since:    map[string]string{"vt": "0.10.0"},
			expectPB: true,
		},
		{
			name:     "newer major",
			inv:      &inventory.Inventory{VTVersion: "v1.0.0"},
			since:    map[string]string{"vt": "2.0.0"},
			expectPB: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			reg := &registry.Registry{ByID: map[string]*registry.Row{}}
			reg.Rows = []registry.Row{{
				ID:       "vt.test.feature",
				Title:    "test",
				Surface:  registry.SurfaceVT,
				Kind:     registry.KindSelect,
				Origin:   registry.OriginNative,
				Expect:   registry.ExpectPass,
				Targets:  []registry.Target{registry.TargetHot},
				Seed:     []string{"traces.base"},
				Upstream: &registry.Upstream{Route: "/select/test"},
				Request:  &registry.Request{Method: "GET", Path: "/select/test"},
				Compare:  &registry.Compare{Type: "status"},
				Layers:   []string{"api"},
				Since:    tt.since,
			}}
			rep := CheckDrift(tt.inv, reg)
			if tt.expectPB && len(rep.PendingBump) != 1 {
				t.Fatalf("expected pending bump, got: %v", rep.PendingBump)
			}
			if !tt.expectPB && len(rep.PendingBump) != 0 {
				t.Fatalf("expected no pending bump, got: %v", rep.PendingBump)
			}
		})
	}
}

// TestCheckDrift_SummaryMultipleFlagWarnings tests summary with multiple flag warnings.
func TestCheckDrift_SummaryMultipleFlagWarnings(t *testing.T) {
	rep := DriftReport{
		FlagWarnings: []inventory.Item{
			{Kind: "flag", Name: "search.x"},
			{Kind: "flag", Name: "search.y"},
		},
	}
	summary := rep.Summary()
	if !strings.Contains(summary, "2 flag warnings") {
		t.Fatalf("expected '2 flag warnings', got: %s", summary)
	}
}

// TestCheckDrift_NegativePrefixCoverage ensures non-trailing-slash routes
// are not covered by prefix matching. /select/logsql/query_time_range must
// be unmapped when only /select/logsql/query (without /) is cited.
func TestCheckDrift_NegativePrefixCoverage(t *testing.T) {
	inv := &inventory.Inventory{
		VLVersion: "v1.50.0",
		VTVersion: "v0.9.0",
		Items: []inventory.Item{
			{Kind: "route", Name: "/select/logsql/query", Source: "app/vlselect/main.go"},
			{Kind: "route", Name: "/select/logsql/query_time_range", Source: "app/vlselect/main.go"},
		},
	}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	// Only cite /select/logsql/query (no prefix semantics since no trailing /)
	reg.Rows = []registry.Row{{
		ID:       "vl.select.logsql.query",
		Title:    "query",
		Surface:  registry.SurfaceVL,
		Kind:     registry.KindSelect,
		Origin:   registry.OriginNative,
		Expect:   registry.ExpectPass,
		Targets:  []registry.Target{registry.TargetHot},
		Seed:     []string{"logs.base"},
		Upstream: &registry.Upstream{Route: "/select/logsql/query"},
		Request:  &registry.Request{Method: "GET", Path: "/select/logsql/query"},
		Compare:  &registry.Compare{Type: "status"},
		Layers:   []string{"api"},
	}}
	rep := CheckDrift(inv, reg)
	if len(rep.Unmapped) != 1 || rep.Unmapped[0].Name != "/select/logsql/query_time_range" {
		t.Fatalf("query_time_range should be unmapped (no prefix match without trailing /): %v", rep.Unmapped)
	}
}

// TestCheckDrift_AbsentButPresentRows ensures rows with expect=absent whose
// upstream key IS in the inventory are tracked separately and not marked stale.
func TestCheckDrift_AbsentButPresentRows(t *testing.T) {
	inv := &inventory.Inventory{
		VLVersion: "v1.50.0",
		VTVersion: "v0.9.0",
		Items: []inventory.Item{
			{Kind: "route", Name: "/select/logsql/old_endpoint", Source: "app/vlselect/main.go"},
		},
	}
	reg := &registry.Registry{ByID: map[string]*registry.Row{}}
	reg.Rows = []registry.Row{{
		ID:       "vl.select.logsql.old_endpoint.absent",
		Title:    "old endpoint absent",
		Surface:  registry.SurfaceVL,
		Kind:     registry.KindSelect,
		Origin:   registry.OriginNative,
		Expect:   registry.ExpectAbsent,
		Targets:  []registry.Target{registry.TargetHot},
		Upstream: &registry.Upstream{Route: "/select/logsql/old_endpoint"},
		Request:  &registry.Request{Method: "GET", Path: "/select/logsql/old_endpoint"},
		Compare:  &registry.Compare{Type: "absent"},
		Layers:   []string{"api"},
	}}
	rep := CheckDrift(inv, reg)
	if len(rep.Stale) != 0 {
		t.Fatalf("absent-but-present rows should not be marked stale: %v", rep.Stale)
	}
	if len(rep.AbsentButPresent) != 1 || rep.AbsentButPresent[0] != "vl.select.logsql.old_endpoint.absent" {
		t.Fatalf("expected 1 absent-but-present, got: %v", rep.AbsentButPresent)
	}
}

// TestCheckDrift_SummaryWithAbsentButPresent tests summary includes absent-but-present count.
func TestCheckDrift_SummaryWithAbsentButPresent(t *testing.T) {
	rep := DriftReport{
		Unmapped:         []inventory.Item{{Kind: "route", Name: "/foo"}},
		Stale:            []string{"vl.foo.bar"},
		FlagWarnings:     []inventory.Item{{Kind: "flag", Name: "search.x"}},
		PendingBump:      []string{"vt.foo.bar.pending"},
		AbsentButPresent: []string{"vl.foo.absent.present"},
	}
	summary := rep.Summary()
	if !strings.Contains(summary, "1 unmapped") || !strings.Contains(summary, "1 stale") ||
		!strings.Contains(summary, "1 pending-bump") || !strings.Contains(summary, "1 flag warning") ||
		!strings.Contains(summary, "1 absent-but-present") {
		t.Fatalf("unexpected summary: %s", summary)
	}
}
