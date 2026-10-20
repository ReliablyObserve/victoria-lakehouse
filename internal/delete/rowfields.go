package delete

import (
	"fmt"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// LogRowFields renders a LogRow as the []logstorage.Field shape VL filters
// evaluate against, so a test (or a later physical removal) can apply the SAME
// tombstone predicate the query path uses.
func LogRowFields(row *schema.LogRow) []logstorage.Field {
	m := logRowToMap(row)
	return fieldsFromMap(m)
}

// TraceRowFields is LogRowFields for spans.
func TraceRowFields(row *schema.TraceRow) []logstorage.Field {
	m := traceRowToMap(row)
	return fieldsFromMap(m)
}

func fieldsFromMap(m map[string]string) []logstorage.Field {
	fields := make([]logstorage.Field, 0, len(m))
	for k, v := range m {
		fields = append(fields, logstorage.Field{Name: k, Value: v})
	}
	return fields
}

// logRowToMap converts a LogRow into a map[string]string for tombstone matching.
func logRowToMap(row *schema.LogRow) map[string]string {
	m := map[string]string{
		"body":                   row.Body,
		"severity_text":          row.SeverityText,
		"service.name":           row.ServiceName,
		"k8s.namespace.name":     row.K8sNamespaceName,
		"k8s.pod.name":           row.K8sPodName,
		"k8s.deployment.name":    row.K8sDeploymentName,
		"k8s.node.name":          row.K8sNodeName,
		"deployment.environment": row.DeployEnv,
		"cloud.region":           row.CloudRegion,
		"host.name":              row.HostName,
		"trace_id":               row.TraceID,
		"span_id":                row.SpanID,
		"_stream":                row.Stream,
		"_stream_id":             row.StreamID,
		"scope.name":             row.ScopeName,
	}
	for k, v := range row.ResourceAttributes {
		m[k] = v
	}
	for k, v := range row.LogAttributes {
		m[k] = v
	}
	return m
}

// traceRowToMap converts a TraceRow into a map[string]string for tombstone matching.
func traceRowToMap(row *schema.TraceRow) map[string]string {
	m := map[string]string{
		"body":                   row.Body,
		"_msg":                   row.Body,
		"trace_id":               row.TraceID,
		"span_id":                row.SpanID,
		"parent_span_id":         row.ParentSpanID,
		"span.name":              row.SpanName,
		"service.name":           row.ServiceName,
		"status.message":         row.StatusMessage,
		"scope.name":             row.ScopeName,
		"deployment.environment": row.DeployEnv,
		"cloud.region":           row.CloudRegion,
		"host.name":              row.HostName,
		"k8s.namespace.name":     row.K8sNamespaceName,
		"k8s.deployment.name":    row.K8sDeploymentName,
		"k8s.node.name":          row.K8sNodeName,
		"http.method":            row.HTTPMethod,
		"http.status_code":       row.HTTPStatusCode,
		"http.url":               row.HTTPUrl,
		"db.system":              row.DBSystem,
		"db.statement":           row.DBStatement,
	}
	if row.SpanKind != nil {
		m["span.kind"] = fmt.Sprintf("%d", *row.SpanKind)
	}
	if row.StatusCode != nil {
		m["status.code"] = fmt.Sprintf("%d", *row.StatusCode)
	}
	if row.DurationNs != nil {
		m["duration_ns"] = fmt.Sprintf("%d", *row.DurationNs)
	}
	for k, v := range row.ResourceAttributes {
		k = schema.TraceMessageAttributeName("resource_attr:", k)
		m[k] = v
	}
	for k, v := range row.SpanAttributes {
		k = schema.TraceMessageAttributeName("span_attr:", k)
		m[k] = v
	}
	for k, v := range row.ScopeAttributes {
		k = schema.TraceMessageAttributeName("scope_attr:", k)
		m[k] = v
	}
	// Span events and links, under the field names VictoriaTraces gives them,
	// so a delete filter on `event:event_name:0` matches the same spans as hot.
	for _, c := range [...]struct{ col, js string }{
		{schema.ColSpanEventsJSON, row.EventsJSON},
		{schema.ColSpanLinksJSON, row.LinksJSON},
	} {
		_ = schema.ForEachSpanSubField(c.col, c.js, func(name, value string) { m[name] = value })
	}
	return m
}
