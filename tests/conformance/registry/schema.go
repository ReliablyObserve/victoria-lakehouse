// Package registry defines the conformance registry: one Row per endpoint or
// feature that the verification machine must check, native VL/VT rows first,
// Lakehouse additions second. Rows are declarative; the runner (M2) executes them.
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

// Comparators the runner (M2) implements. Kept here so lint rejects typos early.
var Comparators = map[string]bool{
	"exact-json": true, "ndjson-multiset": true, "values-with-hits": true, "count": true,
	"series": true, "trace": true, "status": true, "error": true, "schema": true,
	"golden": true, "absent": true, "ui": true,
}

var idRe = regexp.MustCompile(`^(vl|vt|lh|ui|fuzz)\.[a-z0-9_]+(\.[a-z0-9_]+)+$`)

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
	switch {
	case u.Route != "":
		return "route", u.Route
	case u.Pipe != "":
		return "pipe", u.Pipe
	case u.Filter != "":
		return "filter", u.Filter
	case u.Stats != "":
		return "stats", u.Stats
	case u.TraceQL != "":
		return "traceql", u.TraceQL
	case u.Flag != "":
		return "flag", u.Flag
	}
	return "", ""
}

// Key returns "<kind>:<name>" used to join with inventory items.
func (u *Upstream) Key() string {
	k, n := u.key()
	return k + ":" + n
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
}

type Registry struct {
	Rows []Row
	ByID map[string]*Row
}

func (r *Row) Validate() error {
	var errs []string
	add := func(f string, a ...any) { errs = append(errs, fmt.Sprintf(f, a...)) }
	if !idRe.MatchString(r.ID) {
		add("id %q must match %s", r.ID, idRe)
	}
	if r.Title == "" {
		add("title required")
	}
	switch r.Surface {
	case SurfaceVL, SurfaceVT, SurfaceLH:
	default:
		add("surface %q invalid", r.Surface)
	}
	switch r.Kind {
	case KindSelect, KindInsert, KindPipe, KindFilter, KindStats, KindTraceQL, KindFlag, KindUI, KindAdmin, KindInternal:
	default:
		add("kind %q invalid", r.Kind)
	}
	switch r.Origin {
	case OriginNative, OriginLHShim, OriginLHAddition:
	default:
		add("origin %q invalid", r.Origin)
	}
	switch r.Expect {
	case ExpectPass, ExpectDiffer, ExpectAbsent, ExpectUnsupported:
	default:
		add("expect %q invalid", r.Expect)
	}
	if r.Expect == ExpectDiffer && strings.TrimSpace(r.DifferNote) == "" {
		add("differ_note required when expect=differ")
	}
	if len(r.Targets) == 0 {
		add("targets required")
	}
	for _, t := range r.Targets {
		switch t {
		case TargetHot, TargetCold, TargetGlobal:
		default:
			add("targets: %q invalid", t)
		}
	}
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
	switch r.Origin {
	case OriginNative, OriginLHShim:
		if r.Upstream == nil || r.Upstream.Key() == ":" {
			add("upstream required for origin=%s", r.Origin)
		}
	case OriginLHAddition:
		if r.Upstream != nil {
			add("upstream must be empty for origin=lh-addition")
		}
	}
	if r.Kind != KindUI && r.Kind != KindFlag && r.Request == nil {
		add("request required for kind=%s", r.Kind)
	}
	if r.Compare == nil {
		add("compare required")
	} else if !Comparators[r.Compare.Type] {
		add("compare.type %q unknown", r.Compare.Type)
	}
	if len(r.Layers) == 0 {
		add("layers required")
	}
	if len(errs) == 0 {
		return nil
	}
	sort.Strings(errs)
	return fmt.Errorf("row %s: %s", r.ID, strings.Join(errs, "; "))
}
