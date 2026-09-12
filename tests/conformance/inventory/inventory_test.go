package inventory

import (
	"os"
	"path/filepath"
	"reflect"
	"testing"
)

func TestExtract_FixtureRoundTrip(t *testing.T) {
	inv, err := Extract(Dirs{VL: "testdata/mini-vl", VT: "testdata/mini-vt", VLVersion: "x", VTVersion: "y"})
	if err != nil {
		t.Fatal(err)
	}
	if !has(inv.Items, "route", "/select/logsql/query") || !has(inv.Items, "pipe", "coalesce") || !has(inv.Items, "traceql", "rate") || !has(inv.Items, "flag", "search.maxQueueDuration") {
		t.Fatalf("inventory incomplete: %+v", inv.Items)
	}
	p := filepath.Join(t.TempDir(), "inv.yaml")
	if err := inv.Write(p); err != nil {
		t.Fatal(err)
	}
	back, err := Read(p)
	if err != nil || len(back.Items) != len(inv.Items) || back.VLVersion != "x" {
		t.Fatalf("round trip: %v %+v", err, back)
	}
}

// Real vendored trees: required in CI (CONFORMANCE_REQUIRE_DEPS=1), optional locally.
func TestExtract_RealDeps(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	d := DefaultDirs(root)
	for _, dir := range []string{d.VL, d.VT} {
		if _, err := os.Stat(dir); err != nil {
			if os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" {
				t.Fatalf("deps missing (%s): run make deps-logs deps-traces deps-vt", dir)
			}
			t.Skip("vendored deps not present locally")
		}
	}
	inv, err := Extract(d)
	if err != nil {
		t.Fatal(err)
	}
	for _, w := range [][2]string{{"route", "/select/logsql/query"}, {"route", "/select/jaeger/api/traces/"}, {"route", "/select/tempo/api/search"},
		{"pipe", "stats"}, {"filter", "range"}, {"stats", "quantile"}, {"traceql", "histogram_over_time"}, {"flag", "search.maxConcurrentRequests"}} {
		if !has(inv.Items, w[0], w[1]) {
			t.Fatalf("real deps: missing %v", w)
		}
	}
	if d.VLVersion == "" || d.VTVersion == "" {
		t.Fatalf("versions not read from Makefile: %+v", d)
	}

	// Log counts per kind for transparency
	counts := make(map[string]int)
	for _, it := range inv.Items {
		counts[it.Kind]++
	}
	t.Logf("real deps inventory: routes=%d pipes=%d filters=%d stats=%d traceql=%d flags=%d total=%d",
		counts["route"], counts["pipe"], counts["filter"], counts["stats"], counts["traceql"], counts["flag"], len(inv.Items))
}

// TestVLSurface_TracesPinEqualsLogsPin guards the claim in DefaultDirs' doc
// comment: the traces module vendors its own separate copy of VictoriaLogs
// (pinned to VL_COMMIT_TRACES), and DefaultDirs only extracts the logs-pin
// copy (deps/VictoriaLogs) for the shared VL surface (routes, engine tables,
// flags). This test proves that shortcut is safe by extracting the same
// surface from both copies and asserting item-for-item equality. If the two
// pins ever diverge, this test fails and DefaultDirs must extract both.
func TestVLSurface_TracesPinEqualsLogsPin(t *testing.T) {
	root, err := RepoRoot()
	if err != nil {
		t.Fatal(err)
	}
	d := DefaultDirs(root)
	tracesVL := filepath.Join(root, "lakehouse-traces", "deps", "VictoriaLogs")
	for _, dir := range []string{d.VL, tracesVL} {
		if _, err := os.Stat(dir); err != nil {
			if os.Getenv("CONFORMANCE_REQUIRE_DEPS") == "1" {
				t.Fatalf("deps missing (%s): run make deps-logs deps-traces", dir)
			}
			t.Skip("vendored deps not present locally")
		}
	}

	logsRoutes, err := ExtractVLRoutes(d.VL)
	if err != nil {
		t.Fatal(err)
	}
	tracesRoutes, err := ExtractVLRoutes(tracesVL)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(logsRoutes, tracesRoutes) {
		t.Fatalf("VL routes differ between logs pin and traces pin:\nlogs:   %+v\ntraces: %+v", logsRoutes, tracesRoutes)
	}

	logsEngine, err := ExtractEngine(d.VL)
	if err != nil {
		t.Fatal(err)
	}
	tracesEngine, err := ExtractEngine(tracesVL)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(logsEngine, tracesEngine) {
		t.Fatalf("VL engine tables differ between logs pin and traces pin:\nlogs:   %+v\ntraces: %+v", logsEngine, tracesEngine)
	}

	logsFlags, err := ExtractFlags(d.VL, VLFlagPackages, LinkedIntoLH)
	if err != nil {
		t.Fatal(err)
	}
	tracesFlags, err := ExtractFlags(tracesVL, VLFlagPackages, LinkedIntoLH)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(logsFlags, tracesFlags) {
		t.Fatalf("VL flags differ between logs pin and traces pin:\nlogs:   %+v\ntraces: %+v", logsFlags, tracesFlags)
	}
}
