package parquets3

import (
	"sort"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// fieldsAsMap collapses a []field to name->value for comparison.
func fieldsAsMap(fs []field) map[string]string {
	m := make(map[string]string, len(fs))
	for _, f := range fs {
		if s, ok := f.value.(string); ok {
			m[f.name] = s
		}
	}
	return m
}

// TestDedicated_DualReadEquivalence_Logs is the VL/VT-compatibility proof: a row
// that carries a promoted attribute in the MAP (how OLD, pre-promotion files
// store it) and a row that carries the same attribute in the COLUMN (how NEW
// files store it) MUST surface the IDENTICAL query field. This is what makes the
// schema migration invisible to LogsQL — queries see the same field name+value
// regardless of physical storage.
func TestDedicated_DualReadEquivalence_Logs(t *testing.T) {
	cases := []struct {
		name   string
		setCol func(*schema.LogRow, string)
		key    string
	}{
		{"container.id", func(r *schema.LogRow, v string) { r.ContainerID = v }, "container.id"},
		{"service.instance.id", func(r *schema.LogRow, v string) { r.ServiceInstanceID = v }, "service.instance.id"},
		{"k8s.cluster.name", func(r *schema.LogRow, v string) { r.K8sClusterName = v }, "k8s.cluster.name"},
		{"telemetry.sdk.name", func(r *schema.LogRow, v string) { r.TelemetrySDKName = v }, "telemetry.sdk.name"},
		{"exception.type", func(r *schema.LogRow, v string) { r.ExceptionType = v }, "exception.type"},
	}
	const val = "the-value-42"
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			// OLD file: attribute lives in the map, column empty.
			oldRow := &schema.LogRow{Body: "m", ResourceAttributes: map[string]string{c.key: val}}
			// NEW file: attribute lives in the column, map empty.
			newRow := &schema.LogRow{Body: "m"}
			c.setCol(newRow, val)

			oldF := fieldsAsMap(logRowToFields(oldRow, nil))
			newF := fieldsAsMap(logRowToFields(newRow, nil))

			if oldF[c.key] != val {
				t.Errorf("OLD (map-stored) %q = %q, want %q", c.key, oldF[c.key], val)
			}
			if newF[c.key] != val {
				t.Errorf("NEW (column-stored) %q = %q, want %q", c.key, newF[c.key], val)
			}
			// the field must be emitted EXACTLY once in each (no dup)
			countIn := func(fs []field) int {
				n := 0
				for _, f := range fs {
					if f.name == c.key {
						n++
					}
				}
				return n
			}
			if g := countIn(logRowToFields(oldRow, nil)); g != 1 {
				t.Errorf("OLD: %q emitted %d times, want 1", c.key, g)
			}
			if g := countIn(logRowToFields(newRow, nil)); g != 1 {
				t.Errorf("NEW: %q emitted %d times, want 1", c.key, g)
			}
			// full field set identical (the equivalence)
			ok := len(oldF) == len(newF)
			ks := []string{}
			for k := range oldF {
				ks = append(ks, k)
			}
			sort.Strings(ks)
			for _, k := range ks {
				if oldF[k] != newF[k] {
					ok = false
				}
			}
			if !ok {
				t.Errorf("dual-read field sets differ:\n old=%v\n new=%v", oldF, newF)
			}
		})
	}
}

// TestDedicated_DualReadEquivalence_Traces mirrors the logs proof. NOTE: this is
// the ROOT module's traceRowToFields, which surfaces both map attrs and promoted
// columns under their BARE name; the traces *binary* module applies the VT
// span_attr:/resource_attr: prefix (its own test covers that). The invariant
// here is dual-read: map-stored and column-stored produce the IDENTICAL field.
func TestDedicated_DualReadEquivalence_Traces(t *testing.T) {
	const val = "https://x/y"
	oldRow := &schema.TraceRow{TraceID: "t", SpanAttributes: map[string]string{"url.full": val}}
	newRow := &schema.TraceRow{TraceID: "t", URLFull: val}
	oldF := fieldsAsMap(traceRowToFields(oldRow, nil))
	newF := fieldsAsMap(traceRowToFields(newRow, nil))
	if oldF["url.full"] != val || newF["url.full"] != val {
		t.Errorf("url.full: old=%q new=%q, want %q", oldF["url.full"], newF["url.full"], val)
	}
	if oldF["url.full"] != newF["url.full"] {
		t.Error("dual-read trace url.full differs between map-stored and column-stored")
	}
}
