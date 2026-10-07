//go:build parity

package parity

import (
	"fmt"
	"net/url"
	"sort"
	"strconv"
	"testing"
	"time"
)

// fieldValueHits reads a /select/logsql/field_values answer as value -> hits.
func fieldValueHits(t *testing.T, label string, r fetchResult) map[string]int {
	t.Helper()
	if r.StatusCode != 200 {
		t.Fatalf("%s: status %d: %s", label, r.StatusCode, string(r.Body))
	}
	obj, err := parseJSON(r.Body)
	if err != nil {
		t.Fatalf("%s: parse: %v", label, err)
	}
	out := map[string]int{}
	values, _ := obj["values"].([]any)
	for _, e := range values {
		m, _ := e.(map[string]any)
		if m == nil {
			continue
		}
		v, _ := m["value"].(string)
		switch h := m["hits"].(type) {
		case float64:
			out[v] = int(h)
		case string:
			n, _ := strconv.Atoi(h)
			out[v] = n
		}
	}
	return out
}

// assertFieldValuesEqual requires the cold answer to equal hot's: the same
// values AND the same hits per value. An empty reference is refused, so a
// query that silently stopped matching the seed cannot pass as 0 == 0.
func assertFieldValuesEqual(t *testing.T, name string, ref, sut fetchResult) {
	t.Helper()
	compareFieldValues(t, name, ref, sut, false)
}

// compareFieldValues is assertFieldValuesEqual that, with allowEmpty, accepts
// an empty reference (a case that pins "hot lists nothing here").
func compareFieldValues(t *testing.T, name string, ref, sut fetchResult, allowEmpty bool) {
	t.Helper()
	want := fieldValueHits(t, name+" reference", ref)
	got := fieldValueHits(t, name+" SUT", sut)
	if len(want) == 0 && !allowEmpty {
		t.Fatalf("%s: reference returned no values; the case no longer exercises the seed", name)
	}
	var diffs []string
	for v, h := range want {
		if g, ok := got[v]; !ok {
			diffs = append(diffs, fmt.Sprintf("missing %q (hot hits %d)", v, h))
		} else if g != h {
			diffs = append(diffs, fmt.Sprintf("%q hits hot=%d cold=%d", v, h, g))
		}
	}
	for v, h := range got {
		if _, ok := want[v]; !ok {
			diffs = append(diffs, fmt.Sprintf("extra %q (cold hits %d)", v, h))
		}
	}
	sort.Strings(diffs)
	for _, d := range diffs {
		t.Errorf("%s: %s", name, d)
	}
}

// TestParity_FieldValues_MapAttributes pins field_values over attributes the
// cold tier stores in MAP columns: the values (and hits) of an attribute are
// its own, hits included, and a row without the attribute is one hit of the
// empty value exactly as on hot, with and without a filter, including a filter on another map
// attribute. A scan that addressed the wrong leaf of a MAP column returned
// another attribute's values, or its keys.
func TestParity_FieldValues_MapAttributes(t *testing.T) {
	t.Run("logs", func(t *testing.T) {
		for _, field := range []string{"cloud.region", "host.name", "host.arch", "os.type", "k8s.node.name", "k8s.cluster.name"} {
			for _, q := range []string{"*", `service.name:="api-gateway"`, `level:=ERROR`, `os.type:=linux`} {
				name := fmt.Sprintf("%s/%s", field, q)
				t.Run(name, func(t *testing.T) {
					params := fullRangeParams()
					params.Set("query", q)
					params.Set("field", field)
					ref := fetch(t, vlBaseURL, "/select/logsql/field_values", params)
					sut := fetch(t, lhBaseURL, "/select/logsql/field_values", params)
					assertFieldValuesEqual(t, name, ref, sut)
				})
			}
		}
	})

	// A field no row carries: hot answers one empty-value bucket (hits = rows).
	t.Run("logs_unknown_field", func(t *testing.T) {
		params := fullRangeParams()
		params.Set("query", "*")
		params.Set("field", "no.such.attribute.xyz")
		ref := fetch(t, vlBaseURL, "/select/logsql/field_values", params)
		sut := fetch(t, lhBaseURL, "/select/logsql/field_values", params)
		assertFieldValuesEqual(t, "unknown", ref, sut)
	})

	traceRange := func() url.Values {
		now := time.Now()
		return url.Values{
			"start": {fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano())},
			"end":   {fmt.Sprintf("%d", now.UnixNano())},
		}
	}
	t.Run("traces", func(t *testing.T) {
		for _, field := range []string{"span_attr:http.method", "span_attr:http.route", "span_attr:http.scheme", "resource_attr:cloud.region", "resource_attr:host.arch", "resource_attr:k8s.namespace.name"} {
			for _, q := range []string{"span_id:*", "span_id:* `resource_attr:service.name`:=\"api-gateway\"", "span_id:* `resource_attr:os.type`:=linux"} {
				name := fmt.Sprintf("%s/%s", field, q)
				t.Run(name, func(t *testing.T) {
					params := traceRange()
					params.Set("query", q)
					params.Set("field", field)
					ref := fetch(t, vtBaseURL, "/select/logsql/field_values", params)
					sut := fetch(t, lhtBaseURL, "/select/logsql/field_values", params)
					assertFieldValuesEqual(t, name, ref, sut)
				})
			}
		}
	})

	t.Run("traces_unknown_field", func(t *testing.T) {
		params := traceRange()
		params.Set("query", "span_id:*")
		params.Set("field", "span_attr:no.such.attribute.xyz")
		ref := fetch(t, vtBaseURL, "/select/logsql/field_values", params)
		sut := fetch(t, lhtBaseURL, "/select/logsql/field_values", params)
		assertFieldValuesEqual(t, "unknown", ref, sut)
	})

	// Numeric tenants: each seeded tenant's attribute values equal hot's for
	// the same tenant.
	t.Run("traces_tenants", func(t *testing.T) {
		for _, te := range requireSeededTenants(t) {
			for _, field := range []string{"span_attr:http.method", "resource_attr:cloud.region"} {
				name := fmt.Sprintf("tenant_%s/%s", te.AccountID, field)
				t.Run(name, func(t *testing.T) {
					params := traceRange()
					params.Set("query", "span_id:*")
					params.Set("field", field)
					ref := tenantFetch(t, vtBaseURL, "/select/logsql/field_values", params, te.AccountID, te.ProjectID)
					sut := tenantFetch(t, lhtBaseURL, "/select/logsql/field_values", params, te.AccountID, te.ProjectID)
					assertFieldValuesEqual(t, name, ref, sut)
				})
			}
		}
	})
}
