package schema

import (
	"reflect"
	"strings"
	"testing"
)

// dictColumns reflects a row struct and returns the Parquet column names of
// every string field encoded as `dict` — exactly the low/medium-card
// dimensional class that belongs in the manifest label index.
func dictColumns(t reflect.Type) map[string]bool {
	out := map[string]bool{}
	for i := 0; i < t.NumField(); i++ {
		f := t.Field(i)
		if f.Type.Kind() != reflect.String {
			continue
		}
		tag := f.Tag.Get("parquet")
		if tag == "" {
			continue
		}
		parts := strings.Split(tag, ",")
		name := parts[0]
		isDict := false
		for _, p := range parts[1:] {
			if p == "dict" {
				isDict = true
			}
		}
		if isDict {
			out[name] = true
		}
	}
	return out
}

// TestLabelColumns_ClassifyEveryDictColumn is the drift guard: every dict-encoded
// string column in row.go MUST be either indexed (LogLabelColumns /
// TraceLabelColumns) or named in the documented exclusion set below. This is what
// stops a newly promoted dedicated column from silently going unindexed — the
// exact regression that left k8s.cluster.name et al. showing cardinality 0 in the
// Cardinality Explorer after #167.
func TestLabelColumns_ClassifyEveryDictColumn(t *testing.T) {
	// Dict columns deliberately NOT indexed as label dimensions, with the reason.
	logExclude := map[string]string{
		"_stream":           "VL engine internal stream key",
		"_stream_id":        "VL engine internal stream id",
		"scope.name":        "instrumentation scope, low faceting value",
		"exception.message": "free text, not a group-by dimension (high-card)",
		"ded_s01":           "operator slot — surfaced under its configured name via SlotResolver",
		"ded_s02":           "operator slot",
		"ded_s03":           "operator slot",
		"ded_s04":           "operator slot",
		"ded_s05":           "operator slot",
		"ded_s06":           "operator slot",
		"ded_s07":           "operator slot",
		"ded_s08":           "operator slot",
	}
	traceExclude := map[string]string{
		"_stream":        "VL engine internal stream key",
		"_stream_id":     "VL engine internal stream id",
		"scope.name":     "instrumentation scope, low faceting value",
		"server.address": "address, high-card — bloom not group-by",
		"db.query.text":  "free text query body (high-card)",
		"ded_s01":        "operator slot",
		"ded_s02":        "operator slot",
		"ded_s03":        "operator slot",
		"ded_s04":        "operator slot",
		"ded_s05":        "operator slot",
		"ded_s06":        "operator slot",
		"ded_s07":        "operator slot",
		"ded_s08":        "operator slot",
	}

	check := func(t *testing.T, signal string, dict map[string]bool, indexed map[string]bool, exclude map[string]string) {
		for name := range dict {
			if indexed[name] || exclude[name] != "" {
				continue
			}
			t.Errorf("%s: dict column %q is neither indexed (add to %sLabelColumns) nor excluded "+
				"(add to the test's exclusion set with a reason) — it would silently never appear in the "+
				"Cardinality Explorer / breakdown", signal, name, signal)
		}
	}

	logIndexed := map[string]bool{}
	for _, c := range LogLabelColumns {
		logIndexed[c.Name] = true
	}
	traceIndexed := map[string]bool{}
	for _, c := range TraceLabelColumns {
		traceIndexed[c.Name] = true
	}

	check(t, "Log", dictColumns(reflect.TypeOf(LogRow{})), logIndexed, logExclude)
	check(t, "Trace", dictColumns(reflect.TypeOf(TraceRow{})), traceIndexed, traceExclude)
}

// TestLabelColumns_IncludeDedicatedDimensions pins the specific Tier-1 columns the
// #167 gap dropped, so they can't regress back out of the indexed set.
func TestLabelColumns_IncludeDedicatedDimensions(t *testing.T) {
	logIndexed := map[string]bool{}
	for _, c := range LogLabelColumns {
		logIndexed[c.Name] = true
	}
	for _, want := range []string{"k8s.cluster.name", "service.version", "exception.type", "telemetry.sdk.name", "cloud.provider", "os.type"} {
		if !logIndexed[want] {
			t.Errorf("LogLabelColumns missing dedicated dimension %q", want)
		}
	}
	traceIndexed := map[string]bool{}
	for _, c := range TraceLabelColumns {
		traceIndexed[c.Name] = true
	}
	for _, want := range []string{"k8s.cluster.name", "db.operation.name", "rpc.method", "exception.type"} {
		if !traceIndexed[want] {
			t.Errorf("TraceLabelColumns missing dedicated dimension %q", want)
		}
	}
}

// TestLabelColumns_AccessorsMatchNames sanity-checks that each accessor actually
// reads a distinct field (no copy-paste accessor pointing at the wrong column).
func TestLabelColumns_AccessorsMatchNames(t *testing.T) {
	r := LogRow{K8sClusterName: "C", ServiceVersion: "V", ExceptionType: "E", OSType: "linux"}
	byName := map[string]func(*LogRow) string{}
	for _, c := range LogLabelColumns {
		byName[c.Name] = c.Get
	}
	cases := map[string]string{"k8s.cluster.name": "C", "service.version": "V", "exception.type": "E", "os.type": "linux"}
	for name, want := range cases {
		if got := byName[name](&r); got != want {
			t.Errorf("LogLabelColumns[%q] accessor = %q, want %q (wrong field?)", name, got, want)
		}
	}
}
