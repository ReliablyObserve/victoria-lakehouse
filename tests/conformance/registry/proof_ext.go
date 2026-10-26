package registry

import (
	"regexp"
	"sort"
	"strings"
)

// Proof-harness schema extensions. Every field here is optional: a registry
// that uses none of them loads and validates exactly as before. The fields
// say, in the registry and not in workflow YAML, which data layer a cell
// proves, which tier runs it, how a perf scenario is judged, which UI page
// is a row, where the contract of a Lakehouse-only API lives, and which
// upstream-shared code a guard protects.
//
// Validation here is lexical (a closed vocabulary, a shape, a combination
// that cannot be true). That a named scenario, page, contract or patch really
// exists is decided by CheckProofRefs, which reads the repository.

const (
	// KindVisual is a Grafana, VMUI, VTUI or Jaeger UI page proven by the
	// Playwright capture (tests/playwright/proof): a row names the page.
	KindVisual Kind = "visual"
	// KindAPISchema is a Lakehouse-only API (/lakehouse/api/v1/*,
	// /lakehouse/info) whose JSON contract is a committed file.
	KindAPISchema Kind = "api-schema"
	// KindSharedCode is code that Lakehouse shares with, imports from or
	// patches into VictoriaLogs/VictoriaTraces. It is backed by guard tests
	// (refs.tests), like a durability cell, not by an HTTP request.
	KindSharedCode Kind = "shared-code"
)

// DataLayers are the data layers a cell proves, spelled as the `layers` of
// scripts/bench/ab/shapes.json so the perf smoke and the registry join on
// them. They are not the `layers` field of a row (api, ui, perf, chaos: the
// kind of test) and `cold` here is not the `targets: cold` value (the engine
// that answers): a cell reads "this answer is proven while the data sits in
// the buffer / flushed cold / compacted / after a restart".
var DataLayers = map[string]bool{
	"buffer": true, "cold": true, "compacted": true, "after-restart": true,
}

// TenantForms are the tenant spellings a cell proves: the numeric AccountID
// and ProjectID of VictoriaLogs and VictoriaTraces, and Lakehouse's optional
// string aliases and OrgIDs.
var TenantForms = map[string]bool{"numeric": true, "alias": true}

// Tiers select where a row runs in the PR proof: `core` always, `focused`
// when the files a PR changes reach the row, `daily` only in the daily run.
var Tiers = map[string]bool{"core": true, "focused": true, "daily": true}

// PerfBaselines are the systems a perf scenario is judged against: the main
// build (`base`), hot VictoriaLogs, hot VictoriaTraces, or the previous release.
var PerfBaselines = map[string]bool{"base": true, "hot-vl": true, "hot-vt": true, "release": true}

// PerfGates say what fails a PR: a deterministic counter (S3 requests and
// bytes, row groups, pages) that rose, a latency beyond the tolerance, or
// nothing (the scenario is reported only).
var PerfGates = map[string]bool{"counters": true, "latency": true, "none": true}

// VisualReferences are the systems a UI page is compared with, next to the
// base and PR builds: hot VictoriaLogs or VictoriaTraces, Loki or Tempo
// through loki-vl-proxy, or the previous release (the Lakehouse UI).
var VisualReferences = map[string]bool{"hot-vl": true, "hot-vt": true, "loki": true, "tempo": true, "release": true}

// SharedCodeTypes say what a shared-code row names: a `.patch` file, a `.src`
// file compiled into an upstream package, an imported upstream package, or an
// upstream pin.
var SharedCodeTypes = map[string]bool{"patch": true, "src": true, "package": true, "pin": true}

// SharedCodePins are the pins of the Makefile a shared-code row may name.
var SharedCodePins = map[string]bool{"VL_VERSION_LOGS": true, "VL_COMMIT_TRACES": true, "VT_VERSION": true}

// SharedCodeBinaries are the binaries a shared-code row protects: the logs
// binary builds against the latest VL pin and the traces binary against the
// VL commit that VictoriaTraces requires.
var SharedCodeBinaries = map[string]bool{"logs": true, "traces": true}

// classCovered is the n/a class that points at another feature.
const classCovered = "covered-by-feature"

// PerfNAClasses are the accepted reasons for `perf_na`: why a feature has no
// perf scenario. The list is closed; a reason outside it is a new class to
// propose to the owner.
var PerfNAClasses = map[string]bool{
	"no-runtime-path":    true, // configuration, documentation, deploy or CLI tooling: no query or ingest path
	"upstream-unchanged": true, // behaviour and cost are upstream's: no Lakehouse code on the path
	"constant-cost":      true, // O(1) in files, tenants and days by design; the note says how
	"ui-only":            true, // a screen: page-load time is a visual-proof concern
	"covered-by-feature": true, // proven by another feature's scenario; `see` names it
}

// ProofNAClasses are the accepted reasons for `proof_na`.
var ProofNAClasses = map[string]bool{
	"no-user-surface":    true, // internal; nothing a user sees through an API or a UI
	"deploy-only":        true, // chart, Dockerfile, CI or release machinery
	"cli-only":           true, // a command-line tool: its tests are its proof
	"covered-by-feature": true, // proven by another feature's rows or pages; `see` names it
}

// NotApplicable states that a feature has no perf scenario (`perf_na`) or no
// API/visual proof (`proof_na`), in a closed class plus the reason in words.
type NotApplicable struct {
	Class  string `yaml:"class"`
	Reason string `yaml:"reason"`
	// See is the feature that covers this one; required with, and only
	// allowed with, the class covered-by-feature.
	See string `yaml:"see,omitempty"`
}

// PerfScenario names the scenario of scripts/bench/ab/shapes.json a perf row
// is judged on, against which baseline, and what fails.
type PerfScenario struct {
	// Set is a shape set of shapes.json (e.g. trace-by-id); Shape optionally
	// narrows it to one shape (by its `name`).
	Set   string `yaml:"set"`
	Shape string `yaml:"shape,omitempty"`
}

// PerfTolerance is the latency a scenario may lose against its baseline: a
// relative part and an absolute part, in effect the larger of the two.
type PerfTolerance struct {
	Pct float64 `yaml:"pct"`
	Ms  float64 `yaml:"ms"`
}

// Visual makes a UI page a row: the page of tests/playwright/proof/spec.json,
// the capture ranges, the signals it shows and the reference system.
type Visual struct {
	Page      string   `yaml:"page"`
	Ranges    []string `yaml:"ranges,omitempty"` // default: the ranges the page declares in spec.json
	Signals   []string `yaml:"signals"`
	Reference string   `yaml:"reference"`
}

// SharedCode describes upstream-shared code (kind=shared-code): what it is,
// which binaries it protects, and the CI gate beside the tests in refs.tests.
type SharedCode struct {
	Type string `yaml:"type"`
	// Name is a repo-relative patch or .src path (types patch, src), a Go
	// package import path (package), or a Makefile pin (pin).
	Name     string   `yaml:"name"`
	Binaries []string `yaml:"binaries"`
	// Gate names a CI gate or script that guards the code besides the tests
	// of refs.tests (e.g. scripts/ci/check_patches_equal.sh).
	Gate string `yaml:"gate,omitempty"`
}

var (
	perfSetRe     = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	pageIDRe      = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	visualRangeRe = regexp.MustCompile(`^[a-z0-9]+(-[a-z0-9]+)*$`)
	contractRe    = regexp.MustCompile(`^tests/conformance/contracts/([a-z0-9_.]+)\.json$`)
	packagePathRe = regexp.MustCompile(`^[a-z0-9.-]+(/[A-Za-z0-9._-]+)+$`)
)

// validateProofExtensions applies the rules of the optional proof fields of a
// row. It is a separate function so that Row.Validate keeps one flat check per
// base field.
func (r *Row) validateProofExtensions(add func(string, ...any)) {
	validateClosedSet("data_layers", r.DataLayers, DataLayers, add)
	validateClosedSet("tenant_forms", r.TenantForms, TenantForms, add)
	if r.Tier != "" && !Tiers[r.Tier] {
		add("tier %q invalid (core|focused|daily)", r.Tier)
	}

	r.validatePerfScenario(add)

	// contract: the committed JSON contract of a Lakehouse-only API.
	if r.Kind == KindAPISchema {
		r.validateAPISchema(add)
	} else if r.Contract != "" {
		add("contract only allowed for kind=api-schema")
	}

	if r.Kind == KindVisual {
		r.validateVisual(add)
	} else if r.Visual != nil {
		add("visual block only allowed for kind=visual")
	}

	if r.Kind == KindSharedCode {
		r.validateSharedCode(add)
	} else if r.SharedCode != nil {
		add("shared_code block only allowed for kind=shared-code")
	}
}

// validateClosedSet checks a list field against a closed vocabulary and
// rejects repeated entries.
func validateClosedSet(field string, got []string, allowed map[string]bool, add func(string, ...any)) {
	seen := map[string]bool{}
	for _, v := range got {
		switch {
		case !allowed[v]:
			add("%s: %q invalid (%s)", field, v, vocab(allowed))
		case seen[v]:
			add("%s: %q repeated", field, v)
		}
		seen[v] = true
	}
}

func vocab(m map[string]bool) string {
	keys := make([]string, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return strings.Join(keys, "|")
}

// validatePerfScenario checks the optional scenario fields of the perf block.
// They only exist next to the block's counters, so every rule of the perf
// block (perf layer, cell, counters, budget on a pass row) still applies.
func (r *Row) validatePerfScenario(add func(string, ...any)) {
	p := r.Perf
	if p == nil {
		return
	}
	set := p.Scenario != nil || p.Baseline != "" || p.Gate != "" || p.Tolerance != nil
	if !set {
		return
	}
	switch {
	case p.Scenario == nil:
		add("perf.scenario required with perf.baseline, perf.gate or perf.tolerance")
	case !perfSetRe.MatchString(p.Scenario.Set):
		add("perf.scenario.set %q must be a shape set id of scripts/bench/ab/shapes.json (lower-case words joined by -)", p.Scenario.Set)
	case p.Scenario.Shape != "" && strings.TrimSpace(p.Scenario.Shape) != p.Scenario.Shape:
		add("perf.scenario.shape %q has surrounding spaces", p.Scenario.Shape)
	}
	if !PerfBaselines[p.Baseline] {
		add("perf.baseline %q invalid (%s)", p.Baseline, vocab(PerfBaselines))
	}
	if !PerfGates[p.Gate] {
		add("perf.gate %q invalid (%s)", p.Gate, vocab(PerfGates))
	}
	switch {
	case p.Gate == "latency" && p.Tolerance == nil:
		add("perf.tolerance required when perf.gate=latency")
	case p.Gate != "latency" && p.Tolerance != nil:
		add("perf.tolerance only allowed when perf.gate=latency (counters are exact: any increase fails)")
	case p.Tolerance != nil:
		t := p.Tolerance
		if t.Pct < 0 || t.Ms < 0 || (t.Pct == 0 && t.Ms == 0) {
			add("perf.tolerance needs pct >= 0 and ms >= 0, not both zero")
		}
		if t.Pct > 100 {
			add("perf.tolerance.pct %g is over 100", t.Pct)
		}
	}
}

func (r *Row) validateAPISchema(add func(string, ...any)) {
	if r.Surface != SurfaceLH || r.Origin != OriginLHAddition {
		add("api-schema covers a Lakehouse-only API: surface must be lh and origin lh-addition")
	}
	if r.Request != nil && !strings.HasPrefix(r.Request.Path, "/lakehouse/") {
		add("api-schema request.path %q must be a Lakehouse-only API (/lakehouse/...)", r.Request.Path)
	}
	if r.Compare != nil && r.Compare.Type != "schema" {
		add("api-schema compare.type must be schema, got %q", r.Compare.Type)
	}
	m := contractRe.FindStringSubmatch(r.Contract)
	switch {
	case r.Contract == "":
		add("contract required for kind=api-schema (tests/conformance/contracts/<row id>.json)")
	case m == nil:
		add("contract %q must be tests/conformance/contracts/<row id>.json", r.Contract)
	case m[1] != r.ID:
		add("contract %q must be named after the row id %q", r.Contract, r.ID)
	}
}

func (r *Row) validateVisual(add func(string, ...any)) {
	v := r.Visual
	if v == nil {
		add("visual block required for kind=visual")
		return
	}
	if !pageIDRe.MatchString(v.Page) {
		add("visual.page %q must be a page id of tests/playwright/proof/spec.json", v.Page)
	}
	seen := map[string]bool{}
	for _, rng := range v.Ranges {
		switch {
		case !visualRangeRe.MatchString(rng):
			add("visual.ranges: %q invalid", rng)
		case seen[rng]:
			add("visual.ranges: %q repeated", rng)
		}
		seen[rng] = true
	}
	if len(v.Signals) == 0 {
		add("visual.signals required")
	}
	seen = map[string]bool{}
	for _, s := range v.Signals {
		switch {
		case !DurabilitySignals[s]:
			add("visual.signals: %q invalid (logs|traces)", s)
		case seen[s]:
			add("visual.signals: %q repeated", s)
		}
		seen[s] = true
	}
	if !VisualReferences[v.Reference] {
		add("visual.reference %q invalid (%s)", v.Reference, vocab(VisualReferences))
	}
	if r.Request != nil {
		add("request not allowed for kind=visual (the page is the request)")
	}
}

func (r *Row) validateSharedCode(add func(string, ...any)) {
	s := r.SharedCode
	if s == nil {
		add("shared_code block required for kind=shared-code")
		return
	}
	if !SharedCodeTypes[s.Type] {
		add("shared_code.type %q invalid (%s)", s.Type, vocab(SharedCodeTypes))
	}
	switch s.Type {
	case "patch":
		if !strings.HasPrefix(s.Name, "patches/") || !strings.HasSuffix(s.Name, ".patch") {
			add("shared_code.name %q must be a patches/**/*.patch path for type patch", s.Name)
		}
	case "src":
		if !strings.HasPrefix(s.Name, "patches/") || !strings.HasSuffix(s.Name, ".src") {
			add("shared_code.name %q must be a patches/**/*.src path for type src", s.Name)
		}
	case "package":
		if !packagePathRe.MatchString(s.Name) {
			add("shared_code.name %q must be a Go import path for type package", s.Name)
		}
	case "pin":
		if !SharedCodePins[s.Name] {
			add("shared_code.name %q must be a Makefile pin (%s) for type pin", s.Name, vocab(SharedCodePins))
		}
	}
	if len(s.Binaries) == 0 {
		add("shared_code.binaries required")
	}
	validateClosedSet("shared_code.binaries", s.Binaries, SharedCodeBinaries, add)
	if strings.TrimSpace(s.Gate) != s.Gate {
		add("shared_code.gate %q has surrounding spaces", s.Gate)
	}
	if r.Origin != OriginLHAddition {
		add("shared-code rows describe Lakehouse's use of upstream code: origin must be lh-addition")
	}
	if r.Compare != nil {
		add("compare not allowed for kind=shared-code (the guard tests prove it)")
	}
	if r.Request != nil {
		add("request not allowed for kind=shared-code")
	}
	if r.Refs == nil || len(r.Refs.Tests) == 0 {
		add("refs.tests required for kind=shared-code (the guard tests)")
	}
}

// validate checks a not-applicable statement of a feature; field is its YAML
// name and classes the closed list.
func (n *NotApplicable) validate(field string, classes map[string]bool, add func(string, ...any)) {
	if n == nil {
		return
	}
	if !classes[n.Class] {
		add("%s.class %q invalid (%s)", field, n.Class, vocab(classes))
	}
	if strings.TrimSpace(n.Reason) == "" {
		add("%s.reason required: say in words why it does not apply", field)
	}
	switch {
	case n.Class == classCovered && n.See == "":
		add("%s.see required with class covered-by-feature", field)
	case n.Class != classCovered && n.See != "":
		add("%s.see only allowed with class covered-by-feature", field)
	case n.See != "" && !featureIDRe.MatchString(n.See):
		add("%s.see %q must be a feature id", field, n.See)
	}
}
