// Package registry defines the conformance registry: one Row per endpoint or
// feature that the verification machine must check, native VL/VT rows first,
// Lakehouse additions second. Rows are declarative; a future runner executes them.
package registry

import (
	"fmt"
	"regexp"
	"sort"
	"strings"
)

type Surface string
type Kind string
type Origin string
type Expect string
type Target string

const (
	SurfaceVL Surface = "vl"
	SurfaceVT Surface = "vt"
	SurfaceLH Surface = "lh"

	KindSelect   Kind = "select"
	KindInsert   Kind = "insert"
	KindPipe     Kind = "pipe"
	KindFilter   Kind = "filter"
	KindStats    Kind = "stats"
	KindTraceQL  Kind = "traceql"
	KindFlag     Kind = "flag"
	KindUI       Kind = "ui"
	KindAdmin    Kind = "admin"
	KindInternal Kind = "internal"
	// KindDurability is a cell of the durability matrix: one fault shape, one
	// write mode, one signal, with the guarantee that holds and the Go tests that
	// prove it. It is backed by tests, not by an HTTP request and compare.
	KindDurability Kind = "durability"
	// KindVisual and KindSharedCode are declared in proof_ext.go.

	OriginNative     Origin = "native"
	OriginLHShim     Origin = "lh-shim"
	OriginLHAddition Origin = "lh-addition"

	ExpectPass        Expect = "pass"
	ExpectDiffer      Expect = "differ"
	ExpectAbsent      Expect = "absent"
	ExpectUnsupported Expect = "unsupported"

	TargetHot    Target = "hot"
	TargetCold   Target = "cold"
	TargetGlobal Target = "global"
)

// Comparators a future runner implements. Kept here so lint rejects typos early.
var Comparators = map[string]bool{
	"exact-json": true, "ndjson-multiset": true, "values-with-hits": true, "count": true,
	"series": true, "trace": true, "status": true, "error": true, "schema": true,
	"golden": true, "absent": true, "ui": true,
}

// Layers defines valid layer names for categorizing tests by infrastructure level.
var Layers = map[string]bool{
	"api": true, "ui": true, "perf": true, "chaos": true,
}

// Seeds defines valid dataset names for seeding conformance tests.
// Interim until the seed manifest exists; keep the list in sync with the datasets documented for the runner.
var Seeds = map[string]bool{
	"logs.base":    true,
	"logs.edge":    true,
	"logs.streams": true,
	"traces.base":  true,
	"traces.sg":    true,
	"tenants.iso":  true,
	// The ingest parity matrix writes its own rows (unique markers per run) and
	// reads them back; there is no pre-seeded dataset behind its rows.
	"ingest.matrix": true,
	// Field-metadata perf cells: the deterministic generators of
	// internal/storage/parquets3/field_values_bench_test.go (fmSlotRows) and its
	// traces twin (fmtSlotRows) — a quiet and a busy hour with known truth.
	"logs.fieldmeta":   true,
	"traces.fieldmeta": true,
}

var idRe = regexp.MustCompile(`^(vl|vt|lh|ui|fuzz)\.[a-z0-9_]+(\.[a-z0-9_]+)+$`)

// upstreamField represents one field in the Upstream struct.
type upstreamField struct {
	name string
	get  func(*Upstream) string
}

// upstreamFields lists all possible upstream field names and accessors in order.
var upstreamFields = []upstreamField{
	{"route", func(u *Upstream) string { return u.Route }},
	{"pipe", func(u *Upstream) string { return u.Pipe }},
	{"filter", func(u *Upstream) string { return u.Filter }},
	{"stats", func(u *Upstream) string { return u.Stats }},
	{"traceql", func(u *Upstream) string { return u.TraceQL }},
	{"flag", func(u *Upstream) string { return u.Flag }},
}

// Upstream links a native/shim row to the inventory item it covers.
// Exactly one field is set.
type Upstream struct {
	Route   string `yaml:"route,omitempty"`   // e.g. /select/logsql/query
	Pipe    string `yaml:"pipe,omitempty"`    // e.g. coalesce
	Filter  string `yaml:"filter,omitempty"`  // e.g. range
	Stats   string `yaml:"stats,omitempty"`   // e.g. quantile
	TraceQL string `yaml:"traceql,omitempty"` // e.g. histogram_over_time
	Flag    string `yaml:"flag,omitempty"`    // e.g. search.maxTraces
}

func (u *Upstream) key() (kind, name string) {
	if u == nil {
		return "", ""
	}
	for _, field := range upstreamFields {
		if val := field.get(u); val != "" {
			return field.name, val
		}
	}
	return "", ""
}

// Key returns "<kind>:<name>" used to join with inventory items.
// Returns ":" if the receiver is nil or has no fields set.
func (u *Upstream) Key() string {
	k, n := u.key()
	return k + ":" + n
}

// IsZero reports whether the receiver is nil or has no fields set.
func (u *Upstream) IsZero() bool {
	if u == nil {
		return true
	}
	for _, field := range upstreamFields {
		if field.get(u) != "" {
			return false
		}
	}
	return true
}

type Request struct {
	Method  string            `yaml:"method"`
	Path    string            `yaml:"path"`
	Params  map[string]string `yaml:"params,omitempty"`
	Headers map[string]string `yaml:"headers,omitempty"`
	Body    string            `yaml:"body,omitempty"`
}

type Compare struct {
	Type    string            `yaml:"type"`
	Project []string          `yaml:"project,omitempty"`
	Options map[string]string `yaml:"options,omitempty"`
}

type Refs struct {
	Doc   string   `yaml:"doc,omitempty"`
	Tests []string `yaml:"tests,omitempty"`
}

// PerfBudget is a perf row's latency budget: the measured p50/p90 of the cell
// and the exact-iteration count it must keep (e.g. "10/10"). Only rows whose
// answer is exact carry one — a wrong answer gets no budget.
type PerfBudget struct {
	P50Ms float64 `yaml:"p50_ms"`
	P90Ms float64 `yaml:"p90_ms"`
	Valid string  `yaml:"valid"`
}

// PerfCounters are the deterministic costs of one answer: S3 requests and
// bytes, Parquet row groups and pages touched, and the path that answered
// (catalog, scan, ...). Unlike latency they do not depend on the host, so a
// gate can hold them exactly.
type PerfCounters struct {
	S3Gets    int    `yaml:"s3_gets"`
	S3Bytes   int64  `yaml:"s3_bytes"`
	RowGroups int    `yaml:"row_groups,omitempty"`
	Pages     int    `yaml:"pages,omitempty"`
	Path      string `yaml:"path"`
}

// Perf ties a row to one cell of a measurement harness: Cell is the harness
// cell name the runner joins its results on.
type Perf struct {
	Cell     string        `yaml:"cell"`
	Budget   *PerfBudget   `yaml:"budget,omitempty"`
	Counters *PerfCounters `yaml:"counters,omitempty"`

	// Scenario, Baseline, Gate and Tolerance are optional (proof_ext.go): they
	// tie the cell to a shape set of scripts/bench/ab/shapes.json, say which
	// system it is judged against and what fails a PR. Set together.
	Scenario  *PerfScenario  `yaml:"scenario,omitempty"`
	Baseline  string         `yaml:"baseline,omitempty"` // base, hot-vl, hot-vt, release
	Gate      string         `yaml:"gate,omitempty"`     // counters, latency, none
	Tolerance *PerfTolerance `yaml:"tolerance,omitempty"`
}

// Durability describes one cell of the durability matrix (kind=durability): the
// fault, the write mode, the signal and the guarantees that hold after the
// fault. A cell that is a known gap names its issue.
type Durability struct {
	Fault  string `yaml:"fault"`
	Mode   string `yaml:"mode"`
	Signal string `yaml:"signal"`
	// Guarantees are values of DurabilityGuarantees.
	Guarantees []string `yaml:"guarantees"`
	// Metric is the counter that makes a loss visible; required with the
	// guarantee loss-counted, and a name registered in internal/metrics.
	Metric string `yaml:"metric,omitempty"`
	// Gap is the issue link of a known gap; required when expect=differ.
	Gap string `yaml:"gap,omitempty"`
}

// DurabilityModes, DurabilitySignals and DurabilityGuarantees are the values a
// durability cell may take. The guarantees are the approved set of the design
// (no-loss, no-duplicate, loss-counted, rejected-429, rejected-503): a row that
// the insert path drops and answers as a success is loss-counted, with the
// counter that counts it.
var (
	DurabilityModes      = map[string]bool{"buffer-flush": true}
	DurabilitySignals    = map[string]bool{"logs": true, "traces": true}
	DurabilityGuarantees = map[string]bool{"no-loss": true, "no-duplicate": true, "loss-counted": true, "rejected-429": true, "rejected-503": true}
)

var durabilityMetricRe = regexp.MustCompile(`^lakehouse_[a-z0-9_]+$`)

// durabilityLossMetricRe is what the counter of a loss-counted cell is called: a
// name that says it counts drops, losses or failures. A loss-counted cell naming
// a general counter (lakehouse_insert_rows_total) would claim a loss is visible
// when the counter says nothing about losses. The name is a lexical check: that
// the code increments the counter on the loss is proved by the tests the cell
// cites, not here.
var durabilityLossMetricRe = regexp.MustCompile(`^lakehouse_[a-z0-9_]*(dropped|lost|errors)[a-z0-9_]*$`)

var durabilityFaultRe = regexp.MustCompile(`^[a-z0-9_]+$`)

var perfValidRe = regexp.MustCompile(`^[0-9]+/[1-9][0-9]*$`)

type Row struct {
	ID         string            `yaml:"id"`
	Title      string            `yaml:"title"`
	Surface    Surface           `yaml:"surface"`
	Kind       Kind              `yaml:"kind"`
	Origin     Origin            `yaml:"origin"`
	Expect     Expect            `yaml:"expect"`
	DifferNote string            `yaml:"differ_note,omitempty"`
	Since      map[string]string `yaml:"since,omitempty"` // {"vl": "1.51.0"}
	Targets    []Target          `yaml:"targets"`
	Seed       []string          `yaml:"seed,omitempty"`
	Upstream   *Upstream         `yaml:"upstream,omitempty"`
	Request    *Request          `yaml:"request,omitempty"`
	Compare    *Compare          `yaml:"compare,omitempty"`
	Layers     []string          `yaml:"layers"`
	Pending    bool              `yaml:"pending,omitempty"` // declared but not yet executed by the runner
	Refs       *Refs             `yaml:"refs,omitempty"`
	Notes      string            `yaml:"notes,omitempty"`
	Perf       *Perf             `yaml:"perf,omitempty"`
	Durability *Durability       `yaml:"durability,omitempty"`

	// Proof-harness extensions (proof_ext.go). All optional.
	DataLayers  []string    `yaml:"data_layers,omitempty"`  // buffer, cold, compacted, after-restart
	TenantForms []string    `yaml:"tenant_forms,omitempty"` // numeric, alias
	Tier        string      `yaml:"tier,omitempty"`         // core, focused, daily
	Contract    string      `yaml:"contract,omitempty"`     // schema-compare rows: tests/conformance/contracts/<row id>.json
	Visual      *Visual     `yaml:"visual,omitempty"`       // kind=visual
	SharedCode  *SharedCode `yaml:"shared_code,omitempty"`  // kind=shared-code
}

type Registry struct {
	Rows []Row
	// ByID is built after Rows is final; never append to Rows afterwards (pointers into the slice).
	ByID map[string]*Row
}

//nolint:gocyclo // one flat check per row field and rule, in field order; splitting it would scatter the rules a row must satisfy
func (r *Row) Validate() error {
	var errs []string
	add := func(f string, a ...any) { errs = append(errs, fmt.Sprintf(f, a...)) }

	// ID validation
	if !idRe.MatchString(r.ID) {
		add("id %q must match %s", r.ID, idRe)
	} else {
		// Cross-check id prefix with surface
		if strings.HasPrefix(r.ID, "vl.") && r.Surface != SurfaceVL {
			add("id prefix %q does not match surface %q", "vl", r.Surface)
		} else if strings.HasPrefix(r.ID, "vt.") && r.Surface != SurfaceVT {
			add("id prefix %q does not match surface %q", "vt", r.Surface)
		} else if strings.HasPrefix(r.ID, "lh.") && r.Surface != SurfaceLH {
			add("id prefix %q does not match surface %q", "lh", r.Surface)
		}
	}

	// Title validation
	if strings.TrimSpace(r.Title) == "" {
		add("title required")
	}

	// Surface validation
	switch r.Surface {
	case SurfaceVL, SurfaceVT, SurfaceLH:
	default:
		add("surface %q invalid", r.Surface)
	}

	// Kind validation
	switch r.Kind {
	case KindSelect, KindInsert, KindPipe, KindFilter, KindStats, KindTraceQL, KindFlag, KindUI, KindAdmin, KindInternal, KindDurability, KindVisual, KindSharedCode:
	default:
		add("kind %q invalid", r.Kind)
	}

	// Origin validation
	switch r.Origin {
	case OriginNative, OriginLHShim, OriginLHAddition:
	default:
		add("origin %q invalid", r.Origin)
	}

	// Expect validation
	switch r.Expect {
	case ExpectPass, ExpectDiffer, ExpectAbsent, ExpectUnsupported:
	default:
		add("expect %q invalid", r.Expect)
	}

	// DifferNote validation
	if r.Expect == ExpectDiffer && strings.TrimSpace(r.DifferNote) == "" {
		add("differ_note required when expect=differ")
	}

	// Targets validation
	if len(r.Targets) == 0 {
		add("targets required")
	}
	seenTargets := make(map[Target]bool)
	for _, t := range r.Targets {
		switch t {
		case TargetHot, TargetCold, TargetGlobal:
			if seenTargets[t] {
				add("targets: duplicate %q", t)
			}
			seenTargets[t] = true
		default:
			add("targets: %q invalid", t)
		}
	}

	// Seed validation. The allow-list check (every named seed must be a
	// known dataset) applies to every row that has a seed, regardless of
	// Expect — including expect=unsupported rows, which still declare a
	// real seed dataset (e.g. logs.base) even though the endpoint itself
	// is documented as unsupported. Only the "required"/"must be empty"
	// rules are Expect-specific.
	switch r.Expect {
	case ExpectPass, ExpectDiffer:
		if len(r.Seed) == 0 && r.Kind != KindFlag && r.Kind != KindDurability && r.Kind != KindVisual && r.Kind != KindSharedCode {
			add("seed required when expect=%s", r.Expect)
		}
	case ExpectAbsent:
		if len(r.Seed) != 0 {
			add("seed must be empty when expect=absent")
		}
	}
	for _, s := range r.Seed {
		if !Seeds[s] {
			add("seed %q unknown", s)
		}
	}

	// Since validation
	for key, val := range r.Since {
		switch key {
		case "vl", "vt":
			if val == "" {
				add("since: %q empty", key)
			}
		default:
			add("since: key %q invalid", key)
		}
	}

	// Upstream validation
	switch r.Origin {
	case OriginNative, OriginLHShim:
		if r.Upstream.IsZero() {
			add("upstream required for origin=%s", r.Origin)
		} else {
			// Count non-empty fields in Upstream
			count := 0
			for _, field := range upstreamFields {
				if field.get(r.Upstream) != "" {
					count++
				}
			}
			if count > 1 {
				add("upstream: exactly one of route/pipe/filter/stats/traceql/flag must be set, got %d", count)
			}
		}
	case OriginLHAddition:
		if r.Upstream != nil {
			add("upstream must be empty for origin=lh-addition")
		}
	}

	// Request validation
	if r.Kind != KindUI && r.Kind != KindFlag && r.Kind != KindDurability && r.Kind != KindVisual && r.Kind != KindSharedCode && r.Request == nil {
		add("request required for kind=%s", r.Kind)
	}
	if r.Request != nil {
		switch r.Request.Method {
		case "GET", "POST", "PUT", "DELETE", "HEAD", "PATCH":
		default:
			add("request.method %q invalid", r.Request.Method)
		}
		if r.Request.Path == "" || !strings.HasPrefix(r.Request.Path, "/") {
			add("request.path %q must start with /", r.Request.Path)
		}
	}

	// Compare validation
	if r.Kind == KindDurability {
		if r.Compare != nil {
			add("compare not allowed for kind=durability (the cell is proved by its tests)")
		}
		if r.Request != nil {
			add("request not allowed for kind=durability")
		}
	} else if r.Kind == KindSharedCode {
		// no compare: the guard tests prove it (validateSharedCode rejects one)
	} else if r.Compare == nil {
		add("compare required")
	} else {
		if !Comparators[r.Compare.Type] {
			add("compare.type %q unknown", r.Compare.Type)
		}
		// Comparator coherence: ui kind requires ui comparator
		if (r.Kind == KindUI || r.Kind == KindVisual) && r.Compare.Type != "ui" {
			add("compare.type %q incompatible with kind %q", r.Compare.Type, r.Kind)
		}
		// Non-ui kinds must not use ui comparator
		if r.Kind != KindUI && r.Kind != KindVisual && r.Compare.Type == "ui" {
			add("compare.type %q incompatible with kind %q", r.Compare.Type, r.Kind)
		}
		// absent expect requires specific comparators
		if r.Expect == ExpectAbsent && r.Compare.Type != "absent" && r.Compare.Type != "status" {
			add("compare.type %q incompatible with expect=absent", r.Compare.Type)
		}
	}

	// Layers validation
	if len(r.Layers) == 0 {
		add("layers required")
	}
	for _, layer := range r.Layers {
		if !Layers[layer] {
			add("layers: %q invalid", layer)
		}
	}

	// Durability block: required on, and only on, kind=durability. The cell is
	// proved by Go tests, so it names at least one; a known gap names its issue.
	switch {
	case r.Kind == KindDurability && r.Durability == nil:
		add("durability block required for kind=durability")
	case r.Kind != KindDurability && r.Durability != nil:
		add("durability block only allowed for kind=durability")
	case r.Durability != nil:
		d := r.Durability
		if !durabilityFaultRe.MatchString(d.Fault) {
			add("durability.fault %q must match %s", d.Fault, durabilityFaultRe)
		}
		if !DurabilityModes[d.Mode] {
			add("durability.mode %q invalid", d.Mode)
		}
		if !DurabilitySignals[d.Signal] {
			add("durability.signal %q invalid", d.Signal)
		}
		if len(d.Guarantees) == 0 {
			add("durability.guarantees required")
		}
		seen := map[string]bool{}
		for _, g := range d.Guarantees {
			switch {
			case !DurabilityGuarantees[g]:
				add("durability.guarantees: %q invalid", g)
			case seen[g]:
				add("durability.guarantees: %q repeated", g)
			}
			seen[g] = true
		}
		switch {
		case seen["loss-counted"] && !durabilityMetricRe.MatchString(d.Metric):
			add("durability.metric must name the counter (lakehouse_...) when guarantees has loss-counted, got %q", d.Metric)
		case seen["loss-counted"] && !durabilityLossMetricRe.MatchString(d.Metric):
			add("durability.metric %q is not a counter of drops, losses or failures: a loss-counted cell names a metric whose name contains dropped, lost or errors", d.Metric)
		case !seen["loss-counted"] && d.Metric != "":
			add("durability.metric only allowed with the guarantee loss-counted")
		}
		if r.Origin != OriginLHAddition {
			add("durability cells are Lakehouse additions: origin must be lh-addition")
		}
		if r.Expect != ExpectPass && r.Expect != ExpectDiffer {
			add("durability expect must be pass or differ, got %q", r.Expect)
		}
		if r.Expect == ExpectDiffer && !strings.HasPrefix(d.Gap, "https://github.com/") {
			add("durability.gap must be an issue link when expect=differ (a known gap)")
		}
		if r.Expect == ExpectPass && d.Gap != "" {
			add("durability.gap only allowed when expect=differ")
		}
		if r.Refs == nil || len(r.Refs.Tests) == 0 {
			add("refs.tests required for kind=durability (a cell is proved by tests)")
		}
		// The signal in the id must be the cell's signal, so the logs/traces
		// twins cannot be mixed up.
		if !strings.HasSuffix(r.ID, "_"+d.Signal) {
			add("durability id %q must end in _%s", r.ID, d.Signal)
		}
	}

	// Perf block: required on perf-layer rows, forbidden elsewhere. A budget
	// only on an exact (expect=pass) cell — a wrong answer is never timed.
	perfLayer := false
	for _, layer := range r.Layers {
		if layer == "perf" {
			perfLayer = true
		}
	}
	switch {
	case perfLayer && r.Perf == nil:
		add("perf block required on a perf-layer row")
	case !perfLayer && r.Perf != nil:
		add("perf block only allowed on a perf-layer row")
	case r.Perf != nil:
		if strings.TrimSpace(r.Perf.Cell) == "" {
			add("perf.cell required")
		}
		if r.Perf.Counters == nil {
			add("perf.counters required")
		} else {
			c := r.Perf.Counters
			if c.S3Gets < 0 || c.S3Bytes < 0 || c.RowGroups < 0 || c.Pages < 0 {
				add("perf.counters must not be negative")
			}
			if strings.TrimSpace(c.Path) == "" {
				add("perf.counters.path required")
			}
		}
		switch {
		case r.Expect == ExpectPass && r.Perf.Budget == nil:
			add("perf.budget required when expect=pass")
		case r.Expect != ExpectPass && r.Perf.Budget != nil:
			add("perf.budget only allowed when expect=pass (a wrong answer gets no budget)")
		case r.Perf.Budget != nil:
			b := r.Perf.Budget
			if b.P50Ms <= 0 || b.P90Ms < b.P50Ms {
				add("perf.budget needs 0 < p50_ms <= p90_ms")
			}
			if !perfValidRe.MatchString(b.Valid) {
				add("perf.budget.valid %q must be k/N", b.Valid)
			}
		}
	}

	r.validateProofExtensions(add)

	if len(errs) == 0 {
		return nil
	}
	sort.Strings(errs)
	return fmt.Errorf("row %s: %s", r.ID, strings.Join(errs, "; "))
}
