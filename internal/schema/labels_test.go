package schema

import (
	"fmt"
	"sort"
	"testing"
)

// The label SET is what the manifest's inverted index is built from and what
// the pmeta field catalog is fed with. The flush writer, the compactor and the
// delete rewriter all produce it through this one implementation; these tests
// pin the properties they rely on.

func sortedLabels(m map[string][]string, field string) []string {
	v := append([]string(nil), m[field]...)
	sort.Strings(v)
	return v
}

func TestExtractLogLabels_DistinctValuesPerField(t *testing.T) {
	rows := []LogRow{
		{AccountID: 7, ProjectID: 3, ServiceName: "web", SeverityText: "info"},
		{AccountID: 7, ProjectID: 3, ServiceName: "api", SeverityText: "info"},
		{AccountID: 7, ProjectID: 3, ServiceName: "web", SeverityText: ""},
	}
	labels := ExtractLogLabels(rows)

	if got := sortedLabels(labels, "service.name"); len(got) != 2 || got[0] != "api" || got[1] != "web" {
		t.Errorf("service.name labels = %v, want [api web]", got)
	}
	// Tenancy is embedded for the per-tenant retention and lifecycle rules.
	if got := labels["account_id"]; len(got) != 1 || got[0] != "7" {
		t.Errorf("account_id = %v, want [7]", got)
	}
	if got := labels["project_id"]; len(got) != 1 || got[0] != "3" {
		t.Errorf("project_id = %v, want [3]", got)
	}
	// Empty values are not labels.
	for field, vals := range labels {
		for _, v := range vals {
			if v == "" {
				t.Errorf("field %s carries an empty label value", field)
			}
		}
	}
	if ExtractLogLabels(nil) != nil {
		t.Error("no rows, no labels")
	}
}

// TestExtractLogLabels_ReflectsOnlyTheRowsGiven is the property the delete
// rewriter depends on: extracting from the kept rows must not carry a value
// that only a removed row had, or the catalog serves it after the rows are gone.
func TestExtractLogLabels_ReflectsOnlyTheRowsGiven(t *testing.T) {
	all := []LogRow{{ServiceName: "keep"}, {ServiceName: "gone"}}
	kept := all[:1]
	for _, v := range ExtractLogLabels(kept)["service.name"] {
		if v == "gone" {
			t.Fatal("labels extracted from the kept rows carry a value only a removed row had")
		}
	}
}

func TestExtractLogLabels_CapsEachField(t *testing.T) {
	rows := make([]LogRow, 0, MaxLabelAggregateValues+25)
	for i := 0; i < MaxLabelAggregateValues+25; i++ {
		rows = append(rows, LogRow{ServiceName: fmt.Sprintf("svc-%03d", i)})
	}
	if got := len(ExtractLogLabels(rows)["service.name"]); got != MaxLabelAggregateValues {
		t.Fatalf("service.name kept %d values, want the cap %d (a field at the cap is treated as incomplete by every pruning path)", got, MaxLabelAggregateValues)
	}
}

func TestExtractTraceLabels(t *testing.T) {
	rows := []TraceRow{
		{AccountID: 1, ProjectID: 2, ServiceName: "checkout", SpanName: "GET /cart"},
		{AccountID: 1, ProjectID: 2, ServiceName: "checkout", SpanName: "POST /pay"},
	}
	labels := ExtractTraceLabels(rows)
	if got := labels["service.name"]; len(got) != 1 || got[0] != "checkout" {
		t.Errorf("service.name = %v, want [checkout]", got)
	}
	if got := labels["account_id"]; len(got) != 1 || got[0] != "1" {
		t.Errorf("account_id = %v, want [1]", got)
	}
	if ExtractTraceLabels(nil) != nil {
		t.Error("no spans, no labels")
	}
}
