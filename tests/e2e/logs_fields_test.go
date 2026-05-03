//go:build e2e

package e2e

import (
	"net/url"
	"testing"
)

func TestFieldNames(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_names", params)
	entries := assertValuesResponse(t, body)
	names := extractValueStrings(t, entries)

	if len(names) == 0 {
		t.Fatal("expected at least one field name")
	}

	// Core VL fields that must be present
	requiredFields := []string{"_time", "_msg", "level", "service.name", "trace_id"}
	for _, field := range requiredFields {
		if !containsString(names, field) {
			t.Errorf("field_names missing required field %q; got: %v", field, names)
		}
	}

	t.Logf("field_names returned %d fields: %v", len(names), names)
}

func TestFieldNames_NewAttributes(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_names", params)
	entries := assertValuesResponse(t, body)
	names := extractValueStrings(t, entries)

	// These are datagen resource attributes promoted to top-level columns.
	// The schema maps Parquet column names to internal names.
	// For logs profile: deployment.environment, cloud.region, host.name, k8s.node.name
	// are Parquet column names that may be returned as-is since they have no explicit
	// mapping in LogsProfile (they pass through via updateLabelIndex).
	newAttrs := []string{
		"deployment.environment",
		"cloud.region",
		"host.name",
		"k8s.node.name",
	}
	for _, attr := range newAttrs {
		if !containsString(names, attr) {
			t.Errorf("field_names missing expected attribute %q; available: %v", attr, names)
		}
	}
}

func TestFieldValues_ServiceName(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", "service.name")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_values", params)
	entries := assertValuesResponse(t, body)
	values := extractValueStrings(t, entries)

	expectedServices := []string{
		"api-gateway", "user-service", "order-service",
		"payment-service", "notification-service",
	}

	for _, svc := range expectedServices {
		if !containsString(values, svc) {
			t.Errorf("field_values for service.name missing %q; got: %v", svc, values)
		}
	}

	t.Logf("service.name values: %v", values)
}

func TestFieldValues_Level(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", "level")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_values", params)
	entries := assertValuesResponse(t, body)
	values := extractValueStrings(t, entries)

	expectedLevels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	for _, lvl := range expectedLevels {
		if !containsString(values, lvl) {
			t.Errorf("field_values for level missing %q; got: %v", lvl, values)
		}
	}
}

func TestFieldValues_Limit(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", "service.name")
	params.Set("limit", "2")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_values", params)
	entries := assertValuesResponse(t, body)

	if len(entries) > 2 {
		t.Errorf("expected at most 2 values with limit=2, got %d", len(entries))
	}
	t.Logf("field_values with limit=2 returned %d entries", len(entries))
}

func TestStreamFieldNames(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/stream_field_names", params)
	entries := assertValuesResponse(t, body)
	names := extractValueStrings(t, entries)

	// LogsProfile StreamFields: service.name, k8s.namespace.name, k8s.pod.name
	expectedStreamFields := []string{"service.name", "k8s.namespace.name", "k8s.pod.name"}
	for _, field := range expectedStreamFields {
		if !containsString(names, field) {
			t.Errorf("stream_field_names missing %q; got: %v", field, names)
		}
	}
}

func TestStreams(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/streams", params)
	entries := assertValuesResponse(t, body)

	if len(entries) == 0 {
		t.Fatal("expected at least one stream")
	}

	values := extractValueStrings(t, entries)
	// Each stream value should look like {service.name="...",k8s.namespace.name="..."}
	for i, v := range values {
		if v == "" {
			t.Errorf("stream[%d] has empty value", i)
		}
	}

	t.Logf("streams returned %d unique streams", len(entries))
}

func TestFieldValues_DeploymentEnvironment(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", "deployment.environment")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_values", params)
	entries := assertValuesResponse(t, body)
	values := extractValueStrings(t, entries)

	if len(values) == 0 {
		t.Fatal("expected at least one deployment.environment value")
	}

	expectedEnvs := []string{"production", "staging", "canary"}
	foundAny := false
	for _, env := range expectedEnvs {
		if containsString(values, env) {
			foundAny = true
		}
	}
	if !foundAny {
		t.Errorf("deployment.environment values don't match expected; got: %v", values)
	}
}

func TestStreamFieldValues_ServiceName(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("field", "service.name")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/stream_field_values", url.Values{
		"query": {"*"},
		"start": {params.Get("start")},
		"end":   {params.Get("end")},
		"field": {"service.name"},
	})
	entries := assertValuesResponse(t, body)
	values := extractValueStrings(t, entries)

	if len(values) == 0 {
		t.Fatal("expected at least one stream field value for service.name")
	}

	t.Logf("stream_field_values for service.name: %v", values)
}
