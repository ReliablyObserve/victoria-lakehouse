package registry

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

const (
	extDir      = "testdata/extensions/valid"
	extFeatures = "testdata/extensions/features"
	extRepo     = "testdata/extensions/repo"
)

// TestProofExtensions is the one entry point of the proof-extension tests (the registry links this single name).
func TestProofExtensions(t *testing.T) {
	for _, c := range []struct {
		name string
		fn   func(*testing.T)
	}{
		{"ExampleRowsLoad", proofExt_ExampleRowsLoad},
		{"RoundTrip", proofExt_RoundTrip},
		{"UnusedFieldsAreOmitted", proofExt_UnusedFieldsAreOmitted},
		{"LiveRegistryPointers", proofExt_LiveRegistryPointers},
		{"RowRejects", proofExt_RowRejects},
		{"LoaderRejectsTypos", proofExt_LoaderRejectsTypos},
		{"UnknownKindStillRejected", proofExt_UnknownKindStillRejected},
		{"DataLayersMatchShapes", proofExt_DataLayersMatchShapes},
		{"ExampleFeaturesLoad", proofExt_ExampleFeaturesLoad},
		{"FeatureRejects", proofExt_FeatureRejects},
		{"FeatureValidWithNA", proofExt_FeatureValidWithNA},
		{"RefsResolveInFixtureRepo", proofExt_RefsResolveInFixtureRepo},
		{"RefsDangling", proofExt_RefsDangling},
		{"RefsBrokenFixtureFiles", proofExt_RefsBrokenFixtureFiles},
		{"NASeeResolves", proofExt_NASeeResolves},
		{"NASeePerfRow", proofExt_NASeePerfRow},
		{"PerfScenarioIsLocked", proofExt_PerfScenarioIsLocked},
	} {
		t.Run(c.name, c.fn)
	}
}

func loadExt(t *testing.T) *Registry {
	t.Helper()
	reg, err := LoadDir(extDir)
	if err != nil {
		t.Fatalf("the example rows must load: %v", err)
	}
	return reg
}

func mustContain(t *testing.T, name string, err error, want string) {
	t.Helper()
	if err == nil || !strings.Contains(err.Error(), want) {
		t.Fatalf("%s: err = %v, want it to mention %q", name, err, want)
	}
}

// The example rows of every new field and kind load through the strict loader.
func proofExt_ExampleRowsLoad(t *testing.T) {
	reg := loadExt(t)
	count := map[Kind]int{}
	for _, r := range reg.Rows {
		count[r.Kind]++
	}
	// 3 perf rows (kind select with the scenario fields), 3 visual, 3 contract rows (kind admin, with the other admin example), 4 shared-code
	// (one per type), and 3 rows that carry data_layers, tenant_forms and tier.
	if count[KindVisual] != 3 || count[KindAdmin] != 4 || count[KindSharedCode] != 4 {
		t.Fatalf("kinds in the examples = %v", count)
	}
	scen := 0
	for _, r := range reg.Rows {
		if r.Perf != nil && r.Perf.Scenario != nil {
			scen++
		}
	}
	if scen != 3 {
		t.Fatalf("perf rows with a scenario = %d, want 3", scen)
	}
	h := reg.ByID["vl.select.hits.layers_example"]
	if h == nil || len(h.DataLayers) != 4 || len(h.TenantForms) != 2 || h.Tier != "core" {
		t.Fatalf("layers example = %+v", h)
	}
}

// Every example survives marshal -> strict decode -> validate unchanged: omitempty on every new field and
// the yaml tags match the loader's.
func proofExt_RoundTrip(t *testing.T) {
	reg := loadExt(t)
	for _, r := range reg.Rows {
		out, err := yaml.Marshal([]Row{r})
		if err != nil {
			t.Fatalf("%s: marshal: %v", r.ID, err)
		}
		dec := yaml.NewDecoder(bytes.NewReader(out))
		dec.KnownFields(true)
		var back []Row
		if err := dec.Decode(&back); err != nil {
			t.Fatalf("%s: decode own output: %v\n%s", r.ID, err, out)
		}
		if err := back[0].Validate(); err != nil {
			t.Fatalf("%s: round-tripped row invalid: %v", r.ID, err)
		}
		out2, _ := yaml.Marshal(back)
		if !bytes.Equal(out, out2) {
			t.Fatalf("%s: round trip changed the row:\n%s\n--\n%s", r.ID, out, out2)
		}
	}
}

// A row that uses none of the new fields marshals without them: existing rows stay byte-for-byte what they were.
func proofExt_UnusedFieldsAreOmitted(t *testing.T) {
	r := perfRow(ExpectPass)
	out, err := yaml.Marshal(r)
	if err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"data_layers", "tenant_forms", "tier", "contract", "visual", "shared_code", "scenario", "baseline", "gate", "tolerance"} {
		if strings.Contains(string(out), key) {
			t.Errorf("an unused %s is written:\n%s", key, out)
		}
	}
}

// The live registry and feature catalog still load, and every proof pointer they make resolves in the repository
// (none today; a row migrated later to a new field is checked here without touching this test).
func proofExt_LiveRegistryPointers(t *testing.T) {
	reg, err := LoadDir("rows")
	if err != nil {
		t.Fatal(err)
	}
	feats, err := LoadFeatures("features", "../../..")
	if err != nil {
		t.Fatal(err)
	}
	if p := CheckProofRefs(reg, feats, "../../.."); len(p) > 0 {
		t.Errorf("live registry proof pointers:\n  %s", strings.Join(p, "\n  "))
	}
}

func extRow(t *testing.T, id string) Row {
	t.Helper()
	r := loadExt(t).ByID[id]
	if r == nil {
		t.Fatalf("example row %s missing", id)
	}
	// a deep copy through YAML so a mutation never leaks into another case
	out, err := yaml.Marshal(*r)
	if err != nil {
		t.Fatal(err)
	}
	var c Row
	if err := yaml.Unmarshal(out, &c); err != nil {
		t.Fatal(err)
	}
	return c
}

func proofExt_RowRejects(t *testing.T) {
	cases := []struct {
		name string
		id   string
		mut  func(*Row)
		want string
	}{
		// data_layers, tenant_forms, tier
		{"unknown data layer", "vl.select.hits.layers_example", func(r *Row) { r.DataLayers = []string{"hot"} }, `data_layers: "hot" invalid (after-restart|buffer|cold|compacted)`},
		{"data layer repeated", "vl.select.hits.layers_example", func(r *Row) { r.DataLayers = []string{"cold", "cold"} }, `data_layers: "cold" repeated`},
		{"api is a test layer, not a data layer", "vl.select.hits.layers_example", func(r *Row) { r.DataLayers = []string{"api"} }, "data_layers"},
		{"unknown tenant form", "vl.select.hits.layers_example", func(r *Row) { r.TenantForms = []string{"string"} }, `tenant_forms: "string" invalid (alias|numeric)`},
		{"tenant form repeated", "vl.select.hits.layers_example", func(r *Row) { r.TenantForms = []string{"alias", "alias"} }, `tenant_forms: "alias" repeated`},
		{"unknown tier", "vl.select.hits.layers_example", func(r *Row) { r.Tier = "nightly" }, `tier "nightly" invalid (core|focused|daily)`},

		// perf scenario
		{"scenario without baseline", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Baseline = "" }, `perf.baseline "" invalid`},
		{"unknown baseline", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Baseline = "loki" }, `perf.baseline "loki" invalid (base|hot-vl|hot-vt|release)`},
		{"unknown gate", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Gate = "p99" }, `perf.gate "p99" invalid (counters|latency|none)`},
		{"gate without scenario", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Scenario = nil }, "perf.scenario required"},
		{"baseline without scenario", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Scenario = nil; r.Perf.Gate = "" }, "perf.scenario required"},
		{"scenario set not an id", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Scenario.Set = "Control Logs" }, "perf.scenario.set"},
		{"scenario shape padded", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Scenario.Shape = " logs count_total" }, "perf.scenario.shape"},
		{"latency gate without tolerance", "vl.perf.field_names.hot_latency", func(r *Row) { r.Perf.Tolerance = nil }, "perf.tolerance required when perf.gate=latency"},
		{"tolerance on a counters gate", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Tolerance = &PerfTolerance{Pct: 5} }, "perf.tolerance only allowed when perf.gate=latency"},
		{"tolerance zero", "vl.perf.field_names.hot_latency", func(r *Row) { r.Perf.Tolerance = &PerfTolerance{} }, "not both zero"},
		{"tolerance negative", "vl.perf.field_names.hot_latency", func(r *Row) { r.Perf.Tolerance = &PerfTolerance{Pct: -1, Ms: 3} }, "pct >= 0 and ms >= 0"},
		{"tolerance over 100", "vl.perf.field_names.hot_latency", func(r *Row) { r.Perf.Tolerance = &PerfTolerance{Pct: 101} }, "over 100"},
		{"scenario keeps the perf block rules: counters", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Counters = nil }, "perf.counters required"},
		{"scenario keeps the perf block rules: layer", "vl.perf.count_total.base_counters", func(r *Row) { r.Layers = []string{"api"} }, "only allowed on a perf-layer row"},

		// visual
		{"visual without block", "lh.visual.explore_logs_volume", func(r *Row) { r.Visual = nil }, "visual block required"},
		{"visual page not an id", "lh.visual.explore_logs_volume", func(r *Row) { r.Visual.Page = "Explore Logs" }, "visual.page"},
		{"visual without signals", "lh.visual.explore_logs_volume", func(r *Row) { r.Visual.Signals = nil }, "visual.signals required"},
		{"visual unknown signal", "lh.visual.explore_logs_volume", func(r *Row) { r.Visual.Signals = []string{"metrics"} }, `visual.signals: "metrics" invalid`},
		{"visual repeated signal", "lh.visual.explore_logs_volume", func(r *Row) { r.Visual.Signals = []string{"logs", "logs"} }, `"logs" repeated`},
		{"visual unknown reference", "lh.visual.explore_logs_volume", func(r *Row) { r.Visual.Reference = "grafana" }, `visual.reference "grafana" invalid (hot-vl|hot-vt|loki|release|tempo)`},
		{"visual repeated range", "lh.visual.jaeger_trace_view", func(r *Row) { r.Visual.Ranges = []string{"cold", "cold"} }, `visual.ranges: "cold" repeated`},
		{"visual range not a word", "lh.visual.jaeger_trace_view", func(r *Row) { r.Visual.Ranges = []string{"Last 7 days"} }, "visual.ranges"},
		{"visual needs the ui comparator", "lh.visual.explore_logs_volume", func(r *Row) { r.Compare = &Compare{Type: "exact-json"} }, `compare.type "exact-json" incompatible with kind "visual"`},
		{"visual has no compare", "lh.visual.explore_logs_volume", func(r *Row) { r.Compare = nil }, "compare required"},
		{"visual has no request", "lh.visual.explore_logs_volume", func(r *Row) { r.Request = &Request{Method: "GET", Path: "/x"} }, "request not allowed for kind=visual"},
		{"visual block on another kind", "lh.api.info.contract", func(r *Row) { r.Visual = &Visual{Page: "p"} }, "visual block only allowed for kind=visual"},
		{"ui comparator on a non-ui kind still rejected", "lh.api.info.contract", func(r *Row) { r.Compare = &Compare{Type: "ui"} }, "incompatible with kind"},

		// contract on a schema-compare row
		{"contract wrong directory", "lh.api.info.contract", func(r *Row) { r.Contract = "docs/info.json" }, "must be tests/conformance/contracts/<row id>.json"},
		{"contract named after another row", "lh.api.info.contract", func(r *Row) { r.Contract = "tests/conformance/contracts/lh.api.other.contract.json" }, "must be named after the row id"},
		{"contract path escape", "lh.api.info.contract", func(r *Row) { r.Contract = "tests/conformance/contracts/../../x.json" }, "contract"},
		{"contract of an upstream API", "lh.api.info.contract", func(r *Row) { r.Request.Path = "/select/logsql/query" }, "request.path under /lakehouse/"},
		{"contract on a row without request", "lh.api.info.contract", func(r *Row) { r.Request = nil }, "request.path under /lakehouse/"},
		{"contract not on the lh surface", "lh.api.info.contract", func(r *Row) {
			r.Surface = SurfaceVL
			r.ID = "vl.api.info.contract"
			r.Contract = "tests/conformance/contracts/vl.api.info.contract.json"
		}, "surface must be lh"},
		{"contract needs the schema comparator", "lh.api.info.contract", func(r *Row) { r.Compare.Type = "exact-json" }, "contract needs compare.type schema"},

		// shared-code
		{"shared-code without block", "lh.shared.example_export.patch", func(r *Row) { r.SharedCode = nil }, "shared_code block required"},
		{"shared-code block on another kind", "lh.api.info.contract", func(r *Row) { r.SharedCode = &SharedCode{} }, "shared_code block only allowed for kind=shared-code"},
		{"shared-code unknown type", "lh.shared.example_export.patch", func(r *Row) { r.SharedCode.Type = "fork" }, `shared_code.type "fork" invalid (package|patch|pin|src)`},
		{"patch name not a patch", "lh.shared.example_export.patch", func(r *Row) { r.SharedCode.Name = "deps/x.go" }, "patches/**/*.patch"},
		{"src name not a src", "lh.shared.example_query.src", func(r *Row) { r.SharedCode.Name = "patches/example/x.patch" }, "patches/**/*.src"},
		{"package name not an import path", "lh.shared.logstorage.package", func(r *Row) { r.SharedCode.Name = "logstorage" }, "Go import path"},
		{"pin name unknown", "lh.shared.vl_commit_traces.pin", func(r *Row) { r.SharedCode.Name = "GO_VERSION" }, "must be a Makefile pin"},
		{"no binaries", "lh.shared.example_export.patch", func(r *Row) { r.SharedCode.Binaries = nil }, "shared_code.binaries required"},
		{"unknown binary", "lh.shared.example_export.patch", func(r *Row) { r.SharedCode.Binaries = []string{"select"} }, `shared_code.binaries: "select" invalid (logs|traces)`},
		{"no guard tests", "lh.shared.example_export.patch", func(r *Row) { r.Refs = nil }, "refs.tests required for kind=shared-code"},
		{"shared-code is not native", "lh.shared.example_export.patch", func(r *Row) { r.Origin = OriginNative }, "origin must be lh-addition"},
		{"shared-code has no compare", "lh.shared.example_export.patch", func(r *Row) { r.Compare = &Compare{Type: "exact-json"} }, "compare not allowed for kind=shared-code"},
		{"shared-code has no request", "lh.shared.example_export.patch", func(r *Row) { r.Request = &Request{Method: "GET", Path: "/x"} }, "request not allowed for kind=shared-code"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := extRow(t, c.id)
			if err := r.Validate(); err != nil {
				t.Fatalf("the example row must be valid before the mutation: %v", err)
			}
			c.mut(&r)
			mustContain(t, c.name, r.Validate(), c.want)
		})
	}
}

// The strict loader names an unknown key of a new block, not just "invalid".
func proofExt_LoaderRejectsTypos(t *testing.T) {
	dir := t.TempDir()
	src := `- id: lh.visual.typo
  title: t
  surface: lh
  kind: visual
  origin: lh-addition
  expect: pass
  targets: [cold]
  compare: { type: ui }
  layers: [ui]
  data_layer: [cold]
  visual: { page: p, signals: [logs], reference: hot-vl }
`
	if err := os.WriteFile(filepath.Join(dir, "a.yaml"), []byte(src), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := LoadDir(dir)
	mustContain(t, "typo", err, "field data_layer not found")
}

// kinds that did not exist are still rejected by an old loader's vocabulary: the kind list is closed.
func proofExt_UnknownKindStillRejected(t *testing.T) {
	r := extRow(t, "lh.visual.explore_logs_volume")
	r.Kind = "screenshot"
	mustContain(t, "kind", r.Validate(), `kind "screenshot" invalid`)
}

// The vocabularies line up with the files they join on.
func proofExt_DataLayersMatchShapes(t *testing.T) {
	var s struct {
		Layers map[string]any `json:"layers"`
	}
	if err := readJSON("../../../scripts/bench/ab/shapes.json", &s); err != nil {
		t.Fatal(err)
	}
	if len(s.Layers) != len(DataLayers) {
		t.Errorf("shapes.json layers = %d, DataLayers = %d", len(s.Layers), len(DataLayers))
	}
	for l := range s.Layers {
		if !DataLayers[l] {
			t.Errorf("shapes.json layer %q is not a registry data layer", l)
		}
	}
}

// --- features ---

func proofExt_ExampleFeaturesLoad(t *testing.T) {
	set, err := LoadFeatures(extFeatures, "")
	if err != nil {
		t.Fatalf("the example features must load: %v", err)
	}
	na := 0
	for _, f := range set.Features {
		if f.PerfNA != nil && f.ProofNA != nil {
			na++
		}
	}
	if len(set.Features) != 4 || na != 3 {
		t.Fatalf("features = %d, with both n/a statements = %d", len(set.Features), na)
	}
}

func proofExt_FeatureRejects(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*Feature)
		want string
	}{
		{"unknown perf class", func(f *Feature) { f.PerfNA = &NotApplicable{Class: "later", Reason: "r"} }, `perf_na.class "later" invalid (constant-cost|covered-by-feature|no-runtime-path|ui-only|upstream-unchanged)`},
		{"unknown proof class", func(f *Feature) { f.ProofNA = &NotApplicable{Class: "ui-only", Reason: "r"} }, `proof_na.class "ui-only" invalid (cli-only|covered-by-feature|deploy-only|no-user-surface)`},
		{"perf reason empty", func(f *Feature) { f.PerfNA = &NotApplicable{Class: "ui-only", Reason: "  "} }, "perf_na.reason required"},
		{"proof reason empty", func(f *Feature) { f.ProofNA = &NotApplicable{Class: "cli-only"} }, "proof_na.reason required"},
		{"covered without see", func(f *Feature) { f.PerfNA = &NotApplicable{Class: "covered-by-feature", Reason: "r"} }, "perf_na.see required"},
		{"see without covered", func(f *Feature) {
			f.ProofNA = &NotApplicable{Class: "cli-only", Reason: "r", See: "lh.feature.query.other"}
		}, "proof_na.see only allowed with class covered-by-feature"},
		{"see not a feature id", func(f *Feature) {
			f.PerfNA = &NotApplicable{Class: "covered-by-feature", Reason: "r", See: "query.other"}
		}, "perf_na.see"},
		{"see itself", func(f *Feature) {
			f.PerfNA = &NotApplicable{Class: "covered-by-feature", Reason: "r", See: "lh.feature.storage.example"}
		}, "names the feature itself"},
		{"perf n/a with a bench scenario", func(f *Feature) {
			f.Bench = []string{"count_total"}
			f.PerfNA = &NotApplicable{Class: "ui-only", Reason: "r"}
		}, "perf_na contradicts bench"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			f := validFeature()
			c.mut(&f)
			mustContain(t, c.name, f.Validate(), c.want)
		})
	}
}

func proofExt_FeatureValidWithNA(t *testing.T) {
	f := validFeature()
	f.PerfNA = &NotApplicable{Class: "constant-cost", Reason: "one lookup"}
	f.ProofNA = &NotApplicable{Class: "covered-by-feature", Reason: "r", See: "lh.feature.query.other"}
	if err := f.Validate(); err != nil {
		t.Fatal(err)
	}
}

// --- pointers into the repository ---

func proofExt_RefsResolveInFixtureRepo(t *testing.T) {
	reg := loadExt(t)
	set, err := LoadFeatures(extFeatures, "")
	if err != nil {
		t.Fatal(err)
	}
	// the covering feature cites a row of the main registry in real use; the fixture cites an example row
	reg2 := &Registry{Rows: reg.Rows, ByID: reg.ByID}
	if p := CheckProofRefs(reg2, set, extRepo); len(p) > 0 {
		t.Fatalf("example pointers must resolve:\n  %s", strings.Join(p, "\n  "))
	}
}

func proofExt_RefsDangling(t *testing.T) {
	cases := []struct {
		name string
		id   string
		mut  func(*Row)
		want string
	}{
		{"scenario set missing", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Scenario.Set = "no-such-set" }, `perf.scenario.set "no-such-set" is not a shape set`},
		{"scenario shape missing", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Scenario.Shape = "logs nope" }, `perf.scenario.shape "logs nope" is not a shape of set control-logs`},
		{"hot-vt cannot judge a logs shape", "vl.perf.count_total.base_counters", func(r *Row) { r.Perf.Baseline = "hot-vt" }, "perf.baseline hot-vt cannot judge shape"},
		{"hot-vl cannot judge a traces shape", "vt.perf.trace_by_id.report_only", func(r *Row) { r.Perf.Baseline = "hot-vl" }, "perf.baseline hot-vl cannot judge shape"},
		{"page missing", "lh.visual.explore_logs_volume", func(r *Row) { r.Visual.Page = "no-such-page" }, `visual.page "no-such-page" is not a page`},
		{"range not captured by the page", "lh.visual.jaeger_trace_view", func(r *Row) { r.Visual.Ranges = []string{"buffer"} }, `visual.ranges "buffer" is not a capture range of page jaeger-ui-trace (cold)`},
		{"range not in spec", "lh.visual.explore_logs_volume", func(r *Row) { r.Visual.Ranges = []string{"7d"} }, `visual.ranges "7d"`},
		{"contract file missing", "lh.api.info.contract", func(r *Row) {
			r.ID = "lh.api.gone.contract"
			r.Contract = "tests/conformance/contracts/lh.api.gone.contract.json"
		}, "contract:"},
		{"patch missing", "lh.shared.example_export.patch", func(r *Row) { r.SharedCode.Name = "patches/example/gone.patch" }, "shared_code.name:"},
		{"src missing", "lh.shared.example_query.src", func(r *Row) { r.SharedCode.Name = "patches/example/gone.go.src" }, "shared_code.name:"},
		{"pin missing in the Makefile", "lh.shared.vl_commit_traces.pin", func(r *Row) { r.SharedCode.Name = "VT_VERSION" }, "is not set in the Makefile"},
		{"gate missing", "lh.shared.example_export.patch", func(r *Row) { r.SharedCode.Gate = "scripts/ci/gone.sh" }, "shared_code.gate:"},
		{"gate escapes the repo", "lh.shared.example_export.patch", func(r *Row) { r.SharedCode.Gate = "../x.sh" }, "must stay inside the repository"},
		{"guard test missing", "lh.shared.example_export.patch", func(r *Row) { r.Refs.Tests = []string{"internal/example/example_test.go#TestGone"} }, "refs.tests"},
		{"guard is not a test", "lh.shared.example_export.patch", func(r *Row) { r.Refs.Tests = []string{"scripts/ci/check_patches_equal.sh"} }, "not a test"},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := extRow(t, c.id)
			c.mut(&r)
			if err := r.Validate(); err != nil {
				t.Fatalf("the mutated row must still be valid, only its pointer dangles: %v", err)
			}
			reg := &Registry{Rows: []Row{r}, ByID: map[string]*Row{}}
			reg.ByID[r.ID] = &reg.Rows[0]
			p := CheckProofRefs(reg, nil, extRepo)
			if !strings.Contains(strings.Join(p, "\n"), c.want) {
				t.Fatalf("problems = %q, want one mentioning %q", p, c.want)
			}
		})
	}
}

func proofExt_RefsBrokenFixtureFiles(t *testing.T) {
	r := extRow(t, "lh.api.info.contract")
	repo := t.TempDir()
	path := filepath.Join(repo, "tests/conformance/contracts")
	if err := os.MkdirAll(path, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(path, "lh.api.info.contract.json"), []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	reg := &Registry{Rows: []Row{r}, ByID: map[string]*Row{r.ID: &r}}
	p := CheckProofRefs(reg, nil, repo)
	if len(p) != 1 || !strings.Contains(p[0], "contract:") {
		t.Fatalf("a contract that is not JSON must be reported, got %q", p)
	}

	// shapes.json and spec.json missing: every scenario and page row reports it
	pr, vr := extRow(t, "vl.perf.count_total.base_counters"), extRow(t, "lh.visual.explore_logs_volume")
	reg = &Registry{Rows: []Row{pr, vr}, ByID: map[string]*Row{}}
	p = CheckProofRefs(reg, nil, t.TempDir())
	if len(p) != 2 {
		t.Fatalf("want one problem per row, got %q", p)
	}
}

func proofExt_NASeeResolves(t *testing.T) {
	set, err := LoadFeatures(extFeatures, "")
	if err != nil {
		t.Fatal(err)
	}
	reg := loadExt(t)
	byID := set.ByID

	// the target of a `see` must exist
	byID["lh.feature.cache.warm_example"].PerfNA.See = "lh.feature.query.gone"
	p := CheckProofRefs(reg, set, extRepo)
	if !strings.Contains(strings.Join(p, "\n"), "perf_na.see lh.feature.query.gone is not a feature") {
		t.Fatalf("problems = %q", p)
	}
	byID["lh.feature.cache.warm_example"].PerfNA.See = "lh.feature.query.count_pushdown_example"

	// no chains: the target must not be n/a itself
	byID["lh.feature.cache.warm_example"].PerfNA.See = "lh.feature.deploy.chart_example"
	p = CheckProofRefs(reg, set, extRepo)
	if !strings.Contains(strings.Join(p, "\n"), "is itself perf_na") {
		t.Fatalf("problems = %q", p)
	}
	byID["lh.feature.cache.warm_example"].PerfNA.See = "lh.feature.query.count_pushdown_example"

	// the target must have what it is said to cover: a scenario / a row
	target := byID["lh.feature.query.count_pushdown_example"]
	target.Bench = nil
	p = CheckProofRefs(reg, set, extRepo)
	if !strings.Contains(strings.Join(p, "\n"), "has no bench scenario or perf row") {
		t.Fatalf("problems = %q", p)
	}
	target.Bench = []string{"count_total"}
	target.Rows = nil
	p = CheckProofRefs(reg, set, extRepo)
	if !strings.Contains(strings.Join(p, "\n"), "cites no registry row") {
		t.Fatalf("problems = %q", p)
	}
}

// A feature that has a perf row (not a bench scenario) is a valid `see` target.
func proofExt_NASeePerfRow(t *testing.T) {
	reg := loadExt(t)
	set, err := LoadFeatures(extFeatures, "")
	if err != nil {
		t.Fatal(err)
	}
	target := set.ByID["lh.feature.query.count_pushdown_example"]
	target.Bench = nil
	target.Rows = []string{"vl.perf.count_total.base_counters"}
	if p := CheckProofRefs(reg, set, extRepo); len(p) != 0 {
		t.Fatalf("a perf row is a perf proof: %q", p)
	}
}

// --- the lock gate treats the new perf keys as a different measurement ---

func proofExt_PerfScenarioIsLocked(t *testing.T) {
	type outcome struct{ raises, perfFails, weak int }
	withScenario := func(s perfSpec, extra string) RowLite {
		y := strings.Replace(s.yaml("r"), "perf: { ", "perf: { "+extra+", ", 1)
		r := wholeRow(t, y)
		r.File = "tests/conformance/registry/rows/perf/field_metadata.yaml"
		return r
	}
	const scen = "scenario: { set: control-logs }, baseline: base, gate: counters"
	cases := []struct {
		name       string
		base, head string
		want       outcome
	}{
		{"unchanged", scen, scen, outcome{}},
		{"added to a row without (three keys)", "", scen, outcome{weak: 3}},
		{"removed (three keys)", scen, "", outcome{weak: 3}},
		{"another scenario", scen, "scenario: { set: stats }, baseline: base, gate: counters", outcome{weak: 1}},
		{"another baseline", scen, "scenario: { set: control-logs }, baseline: release, gate: counters", outcome{weak: 1}},
		{"gate loosened to none", scen, "scenario: { set: control-logs }, baseline: base, gate: none", outcome{weak: 1}},
		{"tolerance widened", "scenario: { set: control-logs }, baseline: hot-vl, gate: latency, tolerance: { pct: 10, ms: 1 }",
			"scenario: { set: control-logs }, baseline: hot-vl, gate: latency, tolerance: { pct: 50, ms: 1 }", outcome{weak: 1}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			mk := func(extra string) RowLite {
				if extra == "" {
					return newSpec().row(t, "r")
				}
				return withScenario(newSpec(), extra)
			}
			v := ParityCheck(perfSnap(mk(c.base)), perfSnap(mk(c.head)), nil)
			got := outcome{len(v.PerfRaises), len(v.PerfFailures), len(v.Weakenings)}
			if got != c.want {
				t.Fatalf("raises/perfFails/weak = %+v, want %+v\nverdict: %+v", got, c.want, v)
			}
		})
	}
}
