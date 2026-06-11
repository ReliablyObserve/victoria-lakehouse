package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestDedicated_DualReadEquivalence_TracesPrefixed is the VL/VT-compat proof for
// the traces BINARY: a promoted span/resource attribute stored in the MAP (old
// file) and stored in the COLUMN (new file) MUST both surface under the VT
// stream-tag prefix (span_attr:/resource_attr:) — identical field either way.
func TestDedicated_DualReadEquivalence_TracesPrefixed(t *testing.T) {
	asMap := func(fs []field) map[string]string {
		m := map[string]string{}
		for _, f := range fs {
			if s, ok := f.value.(string); ok {
				m[f.name] = s
			}
		}
		return m
	}
	cases := []struct {
		col       string // bare key
		emitted   string // expected prefixed field name
		setOld    func(*schema.TraceRow, string)
		setNewCol func(*schema.TraceRow, string)
	}{
		{"url.full", "url.full",
			func(r *schema.TraceRow, v string) { r.SpanAttributes = map[string]string{"url.full": v} },
			func(r *schema.TraceRow, v string) { r.URLFull = v }},
		{"container.id", "container.id",
			func(r *schema.TraceRow, v string) { r.ResourceAttributes = map[string]string{"container.id": v} },
			func(r *schema.TraceRow, v string) { r.ContainerID = v }},
	}
	const val = "dual-read-value"
	for _, c := range cases {
		t.Run(c.col, func(t *testing.T) {
			oldR := &schema.TraceRow{TraceID: "t"}
			c.setOld(oldR, val)
			newR := &schema.TraceRow{TraceID: "t"}
			c.setNewCol(newR, val)
			oldF := asMap(traceRowToFields(oldR, nil))
			newF := asMap(traceRowToFields(newR, nil))
			if oldF[c.emitted] != val {
				t.Errorf("OLD (map) %q not surfaced as %q: got %q", c.col, c.emitted, oldF[c.emitted])
			}
			if newF[c.emitted] != val {
				t.Errorf("NEW (column) %q not surfaced as %q: got %q", c.col, c.emitted, newF[c.emitted])
			}
			if oldF[c.emitted] != newF[c.emitted] {
				t.Errorf("dual-read differs for %q", c.emitted)
			}
		})
	}
}
