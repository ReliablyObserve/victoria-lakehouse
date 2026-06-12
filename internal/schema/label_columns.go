package schema

// This file is the SINGLE source of truth for which columns are surfaced as
// manifest labels — used by BOTH the inverted cardinality index (label SETS, in
// internal/storage/parquets3/labels.go) AND the per-(field,value) row-count
// aggregates (label_aggregates.go). Keeping one list means the two can never
// disagree on which fields are "low/medium-card dimensional columns worth
// grouping by / faceting on", and adding a new dedicated dimension is a one-line
// change here that both consumers pick up.
//
// Membership rule ("by cardinality class"): index dict-encoded dimensional
// columns. High-card id-like columns (container.id, service.instance.id,
// trace_id, *.address) are plain-encoded + bloom-indexed for point lookups, not
// grouped, so they are excluded. Any included column that nonetheless turns out
// high-card at runtime is auto-dropped by the MaxLabelAggregateValues cap, so
// this list is safe to grow. Free-text dict columns (exception.message,
// db.query.text) and engine internals (_stream*) are intentionally excluded.
//
// The reflection test TestLabelColumns_ClassifyEveryDictColumn forces every
// dict-tagged string column in row.go to be either listed here or named in the
// test's documented exclusion set — so a newly promoted dedicated column can't
// silently go unindexed (the #167 regression this fixes).

// LogLabelColumn pairs a manifest-label name with an accessor for its value.
type LogLabelColumn struct {
	Name string
	Get  func(*LogRow) string
}

// TraceLabelColumn is the traces counterpart.
type TraceLabelColumn struct {
	Name string
	Get  func(*TraceRow) string
}

// LogLabelColumns is the dimensional label set for logs.
var LogLabelColumns = []LogLabelColumn{
	// Legacy / always-indexed dimensions.
	{"service.name", func(r *LogRow) string { return r.ServiceName }},
	{"severity_text", func(r *LogRow) string { return r.SeverityText }},
	{"k8s.namespace.name", func(r *LogRow) string { return r.K8sNamespaceName }},
	{"k8s.pod.name", func(r *LogRow) string { return r.K8sPodName }},
	{"k8s.deployment.name", func(r *LogRow) string { return r.K8sDeploymentName }},
	{"k8s.node.name", func(r *LogRow) string { return r.K8sNodeName }},
	{"deployment.environment", func(r *LogRow) string { return r.DeployEnv }},
	{"cloud.region", func(r *LogRow) string { return r.CloudRegion }},
	{"host.name", func(r *LogRow) string { return r.HostName }},
	// Tier-1 dedicated dict columns (the #167 indexing gap).
	{"k8s.cluster.name", func(r *LogRow) string { return r.K8sClusterName }},
	{"service.version", func(r *LogRow) string { return r.ServiceVersion }},
	{"exception.type", func(r *LogRow) string { return r.ExceptionType }},
	{"telemetry.sdk.name", func(r *LogRow) string { return r.TelemetrySDKName }},
	{"telemetry.sdk.language", func(r *LogRow) string { return r.TelemetrySDKLang }},
	{"telemetry.sdk.version", func(r *LogRow) string { return r.TelemetrySDKVer }},
	{"cloud.provider", func(r *LogRow) string { return r.CloudProvider }},
	{"cloud.account.id", func(r *LogRow) string { return r.CloudAccountID }},
	{"os.type", func(r *LogRow) string { return r.OSType }},
	{"host.arch", func(r *LogRow) string { return r.HostArch }},
	{"process.runtime.name", func(r *LogRow) string { return r.ProcessRuntimeName }},
	{"process.runtime.version", func(r *LogRow) string { return r.ProcessRuntimeVer }},
}

// TraceLabelColumns is the dimensional label set for traces.
var TraceLabelColumns = []TraceLabelColumn{
	// Legacy / always-indexed dimensions.
	{"service.name", func(r *TraceRow) string { return r.ServiceName }},
	{"span.name", func(r *TraceRow) string { return r.SpanName }},
	{"status.message", func(r *TraceRow) string { return r.StatusMessage }},
	{"http.method", func(r *TraceRow) string { return r.HTTPMethod }},
	{"http.status_code", func(r *TraceRow) string { return r.HTTPStatusCode }},
	{"db.system", func(r *TraceRow) string { return r.DBSystem }},
	{"k8s.namespace.name", func(r *TraceRow) string { return r.K8sNamespaceName }},
	{"k8s.pod.name", func(r *TraceRow) string { return r.K8sPodName }},
	{"k8s.deployment.name", func(r *TraceRow) string { return r.K8sDeploymentName }},
	{"k8s.node.name", func(r *TraceRow) string { return r.K8sNodeName }},
	{"deployment.environment", func(r *TraceRow) string { return r.DeployEnv }},
	{"cloud.region", func(r *TraceRow) string { return r.CloudRegion }},
	{"host.name", func(r *TraceRow) string { return r.HostName }},
	// Tier-1 dedicated dict columns.
	{"db.operation.name", func(r *TraceRow) string { return r.DBOperationName }},
	{"db.collection.name", func(r *TraceRow) string { return r.DBCollectionName }},
	{"rpc.method", func(r *TraceRow) string { return r.RPCMethod }},
	{"messaging.destination.name", func(r *TraceRow) string { return r.MessagingDestination }},
	{"code.function.name", func(r *TraceRow) string { return r.CodeFunctionName }},
	{"exception.type", func(r *TraceRow) string { return r.ExceptionType }},
	{"k8s.cluster.name", func(r *TraceRow) string { return r.K8sClusterName }},
	{"telemetry.sdk.name", func(r *TraceRow) string { return r.TelemetrySDKName }},
	{"cloud.account.id", func(r *TraceRow) string { return r.CloudAccountID }},
}

// LogSketchIDColumns / TraceSketchIDColumns pair each always-sketch id column
// with its row accessor, so the flush tap (pmeta_wire.go) sketches any of them in
// one loop instead of a hardcoded per-field block. These are the high-card,
// id-like columns the Cardinality Explorer sketches (an HLL distinct estimate)
// rather than enumerates, so each reports a real distinct count instead of a
// misleading 0. The id set is identical for logs and traces; DefaultSketchIDColumns
// derives its names from the log list so the three can never drift.
var LogSketchIDColumns = []LogLabelColumn{
	{"trace_id", func(r *LogRow) string { return r.TraceID }},
	{"span_id", func(r *LogRow) string { return r.SpanID }},
	{"container.id", func(r *LogRow) string { return r.ContainerID }},
	{"service.instance.id", func(r *LogRow) string { return r.ServiceInstanceID }},
}

var TraceSketchIDColumns = []TraceLabelColumn{
	{"trace_id", func(r *TraceRow) string { return r.TraceID }},
	{"span_id", func(r *TraceRow) string { return r.SpanID }},
	{"container.id", func(r *TraceRow) string { return r.ContainerID }},
	{"service.instance.id", func(r *TraceRow) string { return r.ServiceInstanceID }},
}

// DefaultSketchIDColumns are the id columns sketched by default — unioned into the
// effective always-sketch set regardless of pmeta.always_sketch_fields (which an
// operator's YAML would otherwise REPLACE) — because they're known unbounded ids
// the dedicated-column work promotes + blooms (container.id, service.instance.id)
// plus the trace-correlation ids. The flush tap feeds each into the durable
// per-partition catalog HLL so the count survives restart; the stats API counts
// them as "indexed" so they never render as "—".
var DefaultSketchIDColumns = sketchIDColumnNames()

func sketchIDColumnNames() []string {
	out := make([]string, len(LogSketchIDColumns))
	for i, c := range LogSketchIDColumns {
		out[i] = c.Name
	}
	return out
}
