package schema

import (
	"fmt"
	"testing"
)

// TestExtractLogLabelAggregates_CountsAndCap pins the shared flush/compactor
// extraction contract: per-(field,value) row counts over the fixed
// low-cardinality field list, empty values skipped, and any field with more
// than MaxLabelAggregateValues distinct values dropped entirely (absent, not
// truncated) — the same absent-value behavior the manifest fast-paths rely on.
func TestExtractLogLabelAggregates_CountsAndCap(t *testing.T) {
	rows := []LogRow{
		{ServiceName: "api-gateway", SeverityText: "INFO", DeployEnv: "prod", K8sNamespaceName: "default", CloudRegion: "eu-west-1"},
		{ServiceName: "api-gateway", SeverityText: "ERROR"},
		{ServiceName: "user-service", SeverityText: "INFO"},
	}
	agg := ExtractLogLabelAggregates(rows)
	if agg["service.name"]["api-gateway"] != 2 || agg["service.name"]["user-service"] != 1 {
		t.Fatalf("service.name counts wrong: %v", agg["service.name"])
	}
	if agg["severity_text"]["INFO"] != 2 || agg["severity_text"]["ERROR"] != 1 {
		t.Fatalf("severity_text counts wrong: %v", agg["severity_text"])
	}
	if agg["deployment.environment"]["prod"] != 1 || agg["k8s.namespace.name"]["default"] != 1 || agg["cloud.region"]["eu-west-1"] != 1 {
		t.Fatalf("single-valued fields wrong: %v", agg)
	}

	// Empty values never become a counted bucket.
	if _, present := agg["deployment.environment"][""]; present {
		t.Fatal("empty value must not be counted")
	}

	// A high-cardinality field (>MaxLabelAggregateValues distinct service
	// names) is dropped — ABSENT from the result, not truncated.
	many := make([]LogRow, MaxLabelAggregateValues+5)
	for i := range many {
		many[i] = LogRow{ServiceName: fmt.Sprintf("svc-%d", i)}
	}
	capped := ExtractLogLabelAggregates(many)
	if _, present := capped["service.name"]; present {
		t.Fatalf("field exceeding MaxLabelAggregateValues must be dropped, got %d values", len(capped["service.name"]))
	}

	if ExtractLogLabelAggregates(nil) != nil {
		t.Fatal("empty input must return nil")
	}
	// Rows whose aggregate fields are all empty yield nil (nothing survives the cap pass).
	if got := ExtractLogLabelAggregates([]LogRow{{Body: "no labels"}}); got != nil {
		t.Fatalf("rows without aggregate fields must return nil, got %v", got)
	}
}

// TestExtractTraceLabelAggregates_CountsAndCap is the traces twin
// (service.name + span.name only).
func TestExtractTraceLabelAggregates_CountsAndCap(t *testing.T) {
	rows := []TraceRow{
		{ServiceName: "api-gateway", SpanName: "GET /users"},
		{ServiceName: "api-gateway", SpanName: "GET /users"},
		{ServiceName: "db", SpanName: "SELECT"},
	}
	agg := ExtractTraceLabelAggregates(rows)
	if agg["service.name"]["api-gateway"] != 2 || agg["service.name"]["db"] != 1 {
		t.Fatalf("service.name counts wrong: %v", agg["service.name"])
	}
	if agg["span.name"]["GET /users"] != 2 || agg["span.name"]["SELECT"] != 1 {
		t.Fatalf("span.name counts wrong: %v", agg["span.name"])
	}

	// Over-cap span.name is dropped while service.name survives — the
	// targeted-drop contract (one bad field doesn't nil the whole map).
	many := make([]TraceRow, MaxLabelAggregateValues+1)
	for i := range many {
		many[i] = TraceRow{ServiceName: "svc", SpanName: fmt.Sprintf("span-%d", i)}
	}
	capped := ExtractTraceLabelAggregates(many)
	if _, present := capped["span.name"]; present {
		t.Fatal("over-cap span.name must be dropped")
	}
	if capped["service.name"]["svc"] != int64(len(many)) {
		t.Fatalf("service.name must survive the cap pass: %v", capped)
	}

	if ExtractTraceLabelAggregates(nil) != nil {
		t.Fatal("empty input must return nil")
	}
}
