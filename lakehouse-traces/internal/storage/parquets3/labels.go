package parquets3

import (
	"strconv"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// maxLabelsPerField caps per-field distinct values in the label SETS, and is
// deliberately the same constant as the label-AGGREGATE cap so the inverted
// index and the aggregate fast-path never disagree on which fields are
// "low-cardinality enough" to keep.
const maxLabelsPerField = schema.MaxLabelAggregateValues

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
		// trace_id omitted: high-cardinality (unique per row), always exceeds
		// maxLabelsPerField cap — bloom filters handle it instead.
	}
	// Per Phase 1, every flushed file holds rows from exactly one tenant.
	addLabel(sets, "account_id", strconv.FormatUint(uint64(rows[0].AccountID), 10))
	addLabel(sets, "project_id", strconv.FormatUint(uint64(rows[0].ProjectID), 10))
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
		// trace_id omitted: high-cardinality, bloom filters handle it.
	}
	addLabel(sets, "account_id", strconv.FormatUint(uint64(rows[0].AccountID), 10))
	addLabel(sets, "project_id", strconv.FormatUint(uint64(rows[0].ProjectID), 10))
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

// Label AGGREGATES (per-(field,value) row counts) moved to
// internal/schema/label_aggregates.go: schema.ExtractLogLabelAggregates /
// schema.ExtractTraceLabelAggregates are the ONE shared implementation used by
// the flush writers (both modules) AND the compactor, so compaction extracts
// from merged rows with the exact field list and cap the flush path uses.
