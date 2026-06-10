package parquets3

import (
	"testing"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

func parseF(t *testing.T, q string) *logstorage.Filter {
	t.Helper()
	pq, err := logstorage.ParseQuery(q)
	if err != nil {
		t.Fatalf("parse %q: %v", q, err)
	}
	return parseFilterFromQuery(pq)
}

// The strict soundness gate: every recognized single-field shape passes with
// exactly that field; anything touching a second field, a pseudo-field, or an
// unknown node refuses.
func TestCountPushdownFilterFields(t *testing.T) {
	cases := []struct {
		q      string
		fields []string
		ok     bool
	}{
		{`service.name:api-gateway`, []string{"service.name"}, true},                                // word match (the bench shape)
		{`service.name:="api-gateway"`, []string{"service.name"}, true},                             // exact
		{`service.name:in(a, b)`, []string{"service.name"}, true},                                   // in
		{`service.name:api*`, []string{"service.name"}, true},                                       // prefix
		{`-service.name:api-gateway`, []string{"service.name"}, true},                               // negation
		{`service.name:a OR service.name:b`, []string{"service.name"}, true},                        // OR same field
		{`service.name:a AND severity_text:ERROR`, []string{"service.name", "severity_text"}, true}, // two fields — known but disqualifies the single-field gate
		{`_time:5m service.name:a`, []string{"_time", "service.name"}, true},                        // time pseudo-field reported
	}
	for _, c := range cases {
		f := parseF(t, c.q)
		got, ok := countPushdownFilterFields(f)
		if ok != c.ok {
			t.Errorf("%q: ok=%v want %v", c.q, ok, c.ok)
			continue
		}
		if !ok {
			continue
		}
		if len(got) != len(c.fields) {
			t.Errorf("%q: fields=%v want %v", c.q, got, c.fields)
			continue
		}
		for _, w := range c.fields {
			if !got[w] {
				t.Errorf("%q: missing field %q in %v", c.q, w, got)
			}
		}
	}
}

func TestCountByPushdownField_Filtered(t *testing.T) {
	cases := []struct {
		q          string
		pipeFields []string
		want       string
	}{
		// the benchmark shape: filtered count, no pipe fields
		{`service.name:api-gateway | stats count() c`, nil, "service.name"},
		// filtered + grouped by the SAME field
		{`service.name:api* | stats by (service.name) count()`, []string{"service.name"}, "service.name"},
		// filter field != pipe field — unsound, refuse
		{`severity_text:ERROR | stats by (service.name) count()`, []string{"service.name"}, ""},
		// two filter fields — refuse
		{`service.name:a severity_text:ERROR | stats count()`, nil, ""},
		// _time alongside the field is SOUND: containment runs against the
		// EFFECTIVE q.GetFilterTimeRange() and synthetic timestamps
		// interpolate within the contained file's bounds
		{`_time:5m service.name:a | stats count()`, nil, "service.name"},
		// _msg filter — refuse
		{`error | stats count()`, nil, ""},
		// unfiltered single-field — the original gate, unchanged
		{`* | stats by (service.name) count()`, []string{"service.name"}, "service.name"},
	}
	for _, c := range cases {
		f := parseF(t, c.q)
		got := countByPushdownField(c.pipeFields, f)
		if got != c.want {
			t.Errorf("%q (pipeFields=%v): got %q want %q", c.q, c.pipeFields, got, c.want)
		}
	}
}
