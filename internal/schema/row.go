package schema

// DedicatedSlotCount is the number of generic spare string columns reserved on
// every signal's row struct for operator-configured custom-attribute promotion
// (Tier 2 of the dedicated-columns design). A configured custom attribute name
// is routed to the next free slot at ingest; the name->slot mapping is written
// to the Parquet footer KV (key DedicatedSlotsMetaKey) so the file stays
// self-describing and portable. Read paths remap the slot column back to the
// configured attribute name. Fixed at 8: dynamic column NAMES can't be struct
// fields, but a fixed set of slots + footer-KV remapping gives the same
// capability without runtime schemas.
const DedicatedSlotCount = 8

// DedicatedSlotsMetaKey is the Parquet footer key-value-metadata key under which
// the per-file {slotColumn: configuredAttrName} mapping is stored as JSON, e.g.
// {"ded_s01":"tenant_id","ded_s02":"feature_flag"}. Standard Parquet metadata —
// DuckDB/pyarrow see the real slot columns and can join this mapping via a view.
const DedicatedSlotsMetaKey = "lakehouse.dedicated_slots"

// DedicatedSlotColumns lists the parquet column names of the spare slots in
// deterministic order. ded_s01..ded_s08. The slot->name mapping the writer
// chooses comes from config; the column identity here is stable across files.
var DedicatedSlotColumns = []string{
	"ded_s01", "ded_s02", "ded_s03", "ded_s04",
	"ded_s05", "ded_s06", "ded_s07", "ded_s08",
}

type LogRow struct {
	AccountID         uint32 `json:"account_id" parquet:"account_id"`
	ProjectID         uint32 `json:"project_id" parquet:"project_id"`
	TimestampUnixNano int64  `json:"timestamp_unix_nano" parquet:"timestamp_unix_nano,delta"`
	Body              string `json:"body" parquet:"body"`
	SeverityText      string `json:"severity_text" parquet:"severity_text,dict"`
	SeverityNumber    int32  `json:"severity_number" parquet:"severity_number"`
	ServiceName       string `json:"service.name" parquet:"service.name,dict"`
	TraceID           string `json:"trace_id" parquet:"trace_id"`
	SpanID            string `json:"span_id" parquet:"span_id"`
	K8sNamespaceName  string `json:"k8s.namespace.name" parquet:"k8s.namespace.name,dict"`
	K8sPodName        string `json:"k8s.pod.name" parquet:"k8s.pod.name,dict"`
	K8sDeploymentName string `json:"k8s.deployment.name" parquet:"k8s.deployment.name,dict"`
	K8sNodeName       string `json:"k8s.node.name" parquet:"k8s.node.name,dict"`
	DeployEnv         string `json:"deployment.environment" parquet:"deployment.environment,dict"`
	CloudRegion       string `json:"cloud.region" parquet:"cloud.region,dict"`
	HostName          string `json:"host.name" parquet:"host.name,dict"`
	Stream            string `json:"_stream" parquet:"_stream,dict"`
	StreamID          string `json:"_stream_id" parquet:"_stream_id,dict"`
	ScopeName         string `json:"scope.name" parquet:"scope.name,dict"`

	// Dedicated columns — Tier 1 (strict OTel semantic-convention attributes).
	// Encoding by cardinality CLASS: high-card id-like -> plain (+ bloom);
	// high-byte low-card -> dict (compression win, no bloom). The bloom set
	// is declared in LogBloomColumns; the catalog can gate it at compaction.
	ContainerID        string `json:"container.id,omitempty" parquet:"container.id,optional"`
	ServiceInstanceID  string `json:"service.instance.id,omitempty" parquet:"service.instance.id,optional"`
	ServiceVersion     string `json:"service.version,omitempty" parquet:"service.version,optional,dict"`
	ExceptionType      string `json:"exception.type,omitempty" parquet:"exception.type,optional,dict"`
	ExceptionMessage   string `json:"exception.message,omitempty" parquet:"exception.message,optional,dict"`
	K8sClusterName     string `json:"k8s.cluster.name,omitempty" parquet:"k8s.cluster.name,optional,dict"`
	TelemetrySDKName   string `json:"telemetry.sdk.name,omitempty" parquet:"telemetry.sdk.name,optional,dict"`
	TelemetrySDKLang   string `json:"telemetry.sdk.language,omitempty" parquet:"telemetry.sdk.language,optional,dict"`
	TelemetrySDKVer    string `json:"telemetry.sdk.version,omitempty" parquet:"telemetry.sdk.version,optional,dict"`
	CloudAccountID     string `json:"cloud.account.id,omitempty" parquet:"cloud.account.id,optional,dict"`
	CloudProvider      string `json:"cloud.provider,omitempty" parquet:"cloud.provider,optional,dict"`
	OSType             string `json:"os.type,omitempty" parquet:"os.type,optional,dict"`
	HostArch           string `json:"host.arch,omitempty" parquet:"host.arch,optional,dict"`
	ProcessRuntimeName string `json:"process.runtime.name,omitempty" parquet:"process.runtime.name,optional,dict"`
	ProcessRuntimeVer  string `json:"process.runtime.version,omitempty" parquet:"process.runtime.version,optional,dict"`

	// Dedicated columns — Tier 2 (custom config-driven spare slots). A
	// configured custom attribute name is routed to the next free slot at
	// ingest; the slot->name mapping is written to the footer KV. dict by
	// default (the writer can pick plain per slot from a config cardinality
	// hint). Empty/RLE when a slot is unused, so the cost is negligible.
	DedS01 string `json:"ded_s01,omitempty" parquet:"ded_s01,optional,dict"`
	DedS02 string `json:"ded_s02,omitempty" parquet:"ded_s02,optional,dict"`
	DedS03 string `json:"ded_s03,omitempty" parquet:"ded_s03,optional,dict"`
	DedS04 string `json:"ded_s04,omitempty" parquet:"ded_s04,optional,dict"`
	DedS05 string `json:"ded_s05,omitempty" parquet:"ded_s05,optional,dict"`
	DedS06 string `json:"ded_s06,omitempty" parquet:"ded_s06,optional,dict"`
	DedS07 string `json:"ded_s07,omitempty" parquet:"ded_s07,optional,dict"`
	DedS08 string `json:"ded_s08,omitempty" parquet:"ded_s08,optional,dict"`

	ResourceAttributes map[string]string `json:"resource.attributes,omitempty" parquet:"resource.attributes,optional"`
	LogAttributes      map[string]string `json:"log.attributes,omitempty" parquet:"log.attributes,optional"`
	ScopeAttributes    map[string]string `json:"scope.attributes,omitempty" parquet:"scope.attributes,optional"`
}

type TraceRow struct {
	AccountID         uint32 `json:"account_id" parquet:"account_id"`
	ProjectID         uint32 `json:"project_id" parquet:"project_id"`
	TimestampUnixNano int64  `json:"timestamp_unix_nano" parquet:"timestamp_unix_nano,delta"`
	StartTimeUnixNano int64  `json:"start_time_unix_nano" parquet:"start_time_unix_nano,delta"`
	TraceID           string `json:"trace_id" parquet:"trace_id"`
	SpanID            string `json:"span_id" parquet:"span_id"`
	ParentSpanID      string `json:"parent_span_id" parquet:"parent_span_id"`
	SpanName          string `json:"span.name" parquet:"span.name,dict"`
	ServiceName       string `json:"service.name" parquet:"service.name,dict"`
	DurationNs        int64  `json:"duration_ns" parquet:"duration_ns"`
	StatusCode        int32  `json:"status.code" parquet:"status.code"`
	StatusMessage     string `json:"status.message" parquet:"status.message,dict"`
	SpanKind          int32  `json:"span.kind" parquet:"span.kind"`
	HTTPMethod        string `json:"http.method" parquet:"http.method,dict"`
	HTTPStatusCode    string `json:"http.status_code" parquet:"http.status_code,dict"`
	HTTPUrl           string `json:"http.url" parquet:"http.url"`
	DBSystem          string `json:"db.system" parquet:"db.system,dict"`
	DBStatement       string `json:"db.statement" parquet:"db.statement"`
	K8sNamespaceName  string `json:"k8s.namespace.name" parquet:"k8s.namespace.name,dict"`
	K8sPodName        string `json:"k8s.pod.name" parquet:"k8s.pod.name,dict"`
	K8sDeploymentName string `json:"k8s.deployment.name" parquet:"k8s.deployment.name,dict"`
	K8sNodeName       string `json:"k8s.node.name" parquet:"k8s.node.name,dict"`
	DeployEnv         string `json:"deployment.environment" parquet:"deployment.environment,dict"`
	CloudRegion       string `json:"cloud.region" parquet:"cloud.region,dict"`
	HostName          string `json:"host.name" parquet:"host.name,dict"`
	Stream            string `json:"_stream" parquet:"_stream,dict"`
	StreamID          string `json:"_stream_id" parquet:"_stream_id,dict"`
	ScopeName         string `json:"scope.name" parquet:"scope.name,dict"`

	// Dedicated columns — Tier 1 (strict OTel span/resource attributes).
	// Per-request high-card identifiers get plain + bloom; lookup-key medium
	// columns get dict + bloom (selective equality); low-card resource
	// columns get dict, no bloom. db.query.text stays in the map (huge); if
	// present it lands in DBQueryText as dict, never bloomed.
	URLFull              string `json:"url.full,omitempty" parquet:"url.full,optional"`
	ClientAddress        string `json:"client.address,omitempty" parquet:"client.address,optional"`
	ServerAddress        string `json:"server.address,omitempty" parquet:"server.address,optional,dict"`
	NetworkPeerAddress   string `json:"network.peer.address,omitempty" parquet:"network.peer.address,optional"`
	DBCollectionName     string `json:"db.collection.name,omitempty" parquet:"db.collection.name,optional,dict"`
	DBOperationName      string `json:"db.operation.name,omitempty" parquet:"db.operation.name,optional,dict"`
	RPCMethod            string `json:"rpc.method,omitempty" parquet:"rpc.method,optional,dict"`
	MessagingDestination string `json:"messaging.destination.name,omitempty" parquet:"messaging.destination.name,optional,dict"`
	CodeFunctionName     string `json:"code.function.name,omitempty" parquet:"code.function.name,optional,dict"`
	ExceptionType        string `json:"exception.type,omitempty" parquet:"exception.type,optional,dict"`
	ContainerID          string `json:"container.id,omitempty" parquet:"container.id,optional"`
	ServiceInstanceID    string `json:"service.instance.id,omitempty" parquet:"service.instance.id,optional"`
	K8sClusterName       string `json:"k8s.cluster.name,omitempty" parquet:"k8s.cluster.name,optional,dict"`
	TelemetrySDKName     string `json:"telemetry.sdk.name,omitempty" parquet:"telemetry.sdk.name,optional,dict"`
	CloudAccountID       string `json:"cloud.account.id,omitempty" parquet:"cloud.account.id,optional,dict"`
	DBQueryText          string `json:"db.query.text,omitempty" parquet:"db.query.text,optional,dict"`

	// Dedicated columns — Tier 2 (custom config-driven spare slots). See LogRow.
	DedS01 string `json:"ded_s01,omitempty" parquet:"ded_s01,optional,dict"`
	DedS02 string `json:"ded_s02,omitempty" parquet:"ded_s02,optional,dict"`
	DedS03 string `json:"ded_s03,omitempty" parquet:"ded_s03,optional,dict"`
	DedS04 string `json:"ded_s04,omitempty" parquet:"ded_s04,optional,dict"`
	DedS05 string `json:"ded_s05,omitempty" parquet:"ded_s05,optional,dict"`
	DedS06 string `json:"ded_s06,omitempty" parquet:"ded_s06,optional,dict"`
	DedS07 string `json:"ded_s07,omitempty" parquet:"ded_s07,optional,dict"`
	DedS08 string `json:"ded_s08,omitempty" parquet:"ded_s08,optional,dict"`

	ResourceAttributes map[string]string `json:"resource.attributes,omitempty" parquet:"resource.attributes,optional"`
	SpanAttributes     map[string]string `json:"span.attributes,omitempty" parquet:"span.attributes,optional"`
	ScopeAttributes    map[string]string `json:"scope.attributes,omitempty" parquet:"scope.attributes,optional"`

	// Service-graph edge fields. Populated only for rows tagged
	// {trace_service_graph_stream="-"} emitted by VT's upstream
	// servicegraph background task; NULL/empty on regular span rows
	// (Parquet RLE keeps the storage cost negligible). These columns
	// surface as top-level fields named `parent`, `child`, `callCount`
	// so the upstream Jaeger Dependencies reader
	// (/select/jaeger/api/dependencies) can serve them via the query
	// `{trace_service_graph_stream="-"} | fields parent, child,
	// callCount | stats by (parent, child) sum(callCount)` exactly as
	// it does on hot VT. Without these columns, the writer's
	// mapFieldToTraceRow would have nowhere to land the edge fields
	// and the reader would return zero edges despite the rows being
	// persisted.
	ServiceGraphParent    string `json:"parent,omitempty" parquet:"parent,optional"`
	ServiceGraphChild     string `json:"child,omitempty" parquet:"child,optional"`
	ServiceGraphCallCount string `json:"callCount,omitempty" parquet:"callCount,optional"`
}

// SetDedicatedSlot writes value into the Nth (1-based) spare slot of a LogRow.
// Returns false for an out-of-range slot. Used by the ingest path's
// name->slot router (built from config) so the slot list stays the single
// source of truth for the column identities.
func (r *LogRow) SetDedicatedSlot(slot int, value string) bool {
	switch slot {
	case 1:
		r.DedS01 = value
	case 2:
		r.DedS02 = value
	case 3:
		r.DedS03 = value
	case 4:
		r.DedS04 = value
	case 5:
		r.DedS05 = value
	case 6:
		r.DedS06 = value
	case 7:
		r.DedS07 = value
	case 8:
		r.DedS08 = value
	default:
		return false
	}
	return true
}

// SetDedicatedSlot writes value into the Nth (1-based) spare slot of a TraceRow.
func (r *TraceRow) SetDedicatedSlot(slot int, value string) bool {
	switch slot {
	case 1:
		r.DedS01 = value
	case 2:
		r.DedS02 = value
	case 3:
		r.DedS03 = value
	case 4:
		r.DedS04 = value
	case 5:
		r.DedS05 = value
	case 6:
		r.DedS06 = value
	case 7:
		r.DedS07 = value
	case 8:
		r.DedS08 = value
	default:
		return false
	}
	return true
}
