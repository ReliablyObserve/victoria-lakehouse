package parquets3

import (
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

const maxLabelsPerField = 100

func extractLogLabels(rows []schema.LogRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := map[string]map[string]bool{}
	for i := range rows {
		addLabel(sets, "service.name", rows[i].ServiceName)
		addLabel(sets, "severity_text", rows[i].SeverityText)
		addLabel(sets, "k8s.namespace.name", rows[i].K8sNamespaceName)
		addLabel(sets, "k8s.pod.name", rows[i].K8sPodName)
		addLabel(sets, "k8s.deployment.name", rows[i].K8sDeploymentName)
		addLabel(sets, "k8s.node.name", rows[i].K8sNodeName)
		addLabel(sets, "deployment.environment", rows[i].DeployEnv)
		addLabel(sets, "cloud.region", rows[i].CloudRegion)
		addLabel(sets, "host.name", rows[i].HostName)
		addLabel(sets, "trace_id", rows[i].TraceID)
	}
	return setsToLabels(sets)
}

func extractTraceLabels(rows []schema.TraceRow) map[string][]string {
	if len(rows) == 0 {
		return nil
	}
	sets := map[string]map[string]bool{}
	for i := range rows {
		addLabel(sets, "service.name", rows[i].ServiceName)
		addLabel(sets, "span.name", rows[i].SpanName)
		addLabel(sets, "trace_id", rows[i].TraceID)
	}
	return setsToLabels(sets)
}

func addLabel(sets map[string]map[string]bool, field, value string) {
	if value == "" {
		return
	}
	s, ok := sets[field]
	if !ok {
		s = make(map[string]bool)
		sets[field] = s
	}
	if len(s) < maxLabelsPerField {
		s[value] = true
	}
}

func setsToLabels(sets map[string]map[string]bool) map[string][]string {
	labels := make(map[string][]string, len(sets))
	for k, vs := range sets {
		vals := make([]string, 0, len(vs))
		for v := range vs {
			vals = append(vals, v)
		}
		labels[k] = vals
	}
	return labels
}
