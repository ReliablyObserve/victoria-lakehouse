package parquets3

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestExtractLogLabelAggregates_CountsAndCap(t *testing.T) {
	rows := []schema.LogRow{
		{ServiceName: "api-gateway", SeverityText: "INFO"},
		{ServiceName: "api-gateway", SeverityText: "ERROR"},
		{ServiceName: "user-service", SeverityText: "INFO"},
	}
	agg := extractLogLabelAggregates(rows)
	if agg["service.name"]["api-gateway"] != 2 || agg["service.name"]["user-service"] != 1 {
		t.Fatalf("service.name counts wrong: %v", agg["service.name"])
	}
	if agg["severity_text"]["INFO"] != 2 || agg["severity_text"]["ERROR"] != 1 {
		t.Fatalf("severity_text counts wrong: %v", agg["severity_text"])
	}

	// A high-cardinality field (>maxLabelsPerField distinct service names) is dropped.
	many := make([]schema.LogRow, maxLabelsPerField+5)
	for i := range many {
		many[i] = schema.LogRow{ServiceName: fmt.Sprintf("svc-%d", i)}
	}
	cap := extractLogLabelAggregates(many)
	if _, present := cap["service.name"]; present {
		t.Fatalf("field exceeding maxLabelsPerField must be dropped, got %d values", len(cap["service.name"]))
	}

	if extractLogLabelAggregates(nil) != nil {
		t.Fatal("empty input must return nil")
	}
}
