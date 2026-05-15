package parquets3

import (
	"fmt"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func TestExtractLogLabels(t *testing.T) {
	rows := []schema.LogRow{
		{ServiceName: "api", SeverityText: "INFO", K8sNamespaceName: "prod"},
		{ServiceName: "api", SeverityText: "ERROR", K8sNamespaceName: "prod"},
		{ServiceName: "worker", SeverityText: "INFO", K8sNamespaceName: "staging"},
	}

	labels := extractLogLabels(rows)

	if len(labels["service.name"]) != 2 {
		t.Errorf("service.name values = %d, want 2", len(labels["service.name"]))
	}
	if len(labels["severity_text"]) != 2 {
		t.Errorf("severity_text values = %d, want 2", len(labels["severity_text"]))
	}
	if len(labels["k8s.namespace.name"]) != 2 {
		t.Errorf("k8s.namespace.name values = %d, want 2", len(labels["k8s.namespace.name"]))
	}
}

func TestExtractLogLabels_Empty(t *testing.T) {
	labels := extractLogLabels(nil)
	if len(labels) != 0 {
		t.Error("empty rows should produce empty labels")
	}
}

func TestExtractLogLabels_Cap(t *testing.T) {
	rows := make([]schema.LogRow, 200)
	for i := range rows {
		rows[i].ServiceName = fmt.Sprintf("svc-%d", i)
	}

	labels := extractLogLabels(rows)
	if len(labels["service.name"]) > 100 {
		t.Errorf("should cap at 100, got %d", len(labels["service.name"]))
	}
}

func TestExtractTraceLabels(t *testing.T) {
	rows := []schema.TraceRow{
		{ServiceName: "api", SpanName: "GET /users"},
		{ServiceName: "api", SpanName: "POST /orders"},
	}

	labels := extractTraceLabels(rows)

	if len(labels["service.name"]) != 1 {
		t.Errorf("service.name values = %d, want 1", len(labels["service.name"]))
	}
	if len(labels["span.name"]) != 2 {
		t.Errorf("span.name values = %d, want 2", len(labels["span.name"]))
	}
}

func TestExtractLogLabels_EmptyValues(t *testing.T) {
	rows := []schema.LogRow{
		{ServiceName: "api", SeverityText: "", K8sNamespaceName: ""},
	}
	labels := extractLogLabels(rows)
	if _, ok := labels["severity_text"]; ok {
		t.Error("empty values should not be in labels")
	}
	if _, ok := labels["k8s.namespace.name"]; ok {
		t.Error("empty values should not be in labels")
	}
}
