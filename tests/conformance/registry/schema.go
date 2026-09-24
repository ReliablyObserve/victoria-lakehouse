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
}

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
}

type Registry struct {
	Rows []Row
	// ByID is built after Rows is final; never append to Rows afterwards (pointers into the slice).
	ByID map[string]*Row
}

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
	case KindSelect, KindInsert, KindPipe, KindFilter, KindStats, KindTraceQL, KindFlag, KindUI, KindAdmin, KindInternal:
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
		if len(r.Seed) == 0 && r.Kind != KindFlag {
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
	if r.Kind != KindUI && r.Kind != KindFlag && r.Request == nil {
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
	if r.Compare == nil {
		add("compare required")
	} else {
		if !Comparators[r.Compare.Type] {
			add("compare.type %q unknown", r.Compare.Type)
		}
		// Comparator coherence: ui kind requires ui comparator
		if r.Kind == KindUI && r.Compare.Type != "ui" {
			add("compare.type %q incompatible with kind %q", r.Compare.Type, r.Kind)
		}
		// Non-ui kinds must not use ui comparator
		if r.Kind != KindUI && r.Compare.Type == "ui" {
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

	if len(errs) == 0 {
		return nil
	}
	sort.Strings(errs)
	return fmt.Errorf("row %s: %s", r.ID, strings.Join(errs, "; "))
}
