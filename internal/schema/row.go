package schema

// LogRow defines the Parquet schema for log records.
// Field names use OTEL semantic convention dot-notation.
// Non-promoted fields are captured in ResourceAttributes and LogAttributes MAP columns.
type LogRow struct {
	TimestampUnixNano  int64             `parquet:"timestamp_unix_nano"`
	Body               string            `parquet:"body"`
	SeverityText       string            `parquet:"severity_text"`
	SeverityNumber     int32             `parquet:"severity_number"`
	ServiceName        string            `parquet:"service.name"`
	K8sNamespaceName   string            `parquet:"k8s.namespace.name"`
	K8sPodName         string            `parquet:"k8s.pod.name"`
	K8sDeploymentName  string            `parquet:"k8s.deployment.name"`
	K8sNodeName        string            `parquet:"k8s.node.name"`
	DeployEnv          string            `parquet:"deployment.environment"`
	CloudRegion        string            `parquet:"cloud.region"`
	HostName           string            `parquet:"host.name"`
	TraceID            string            `parquet:"trace_id"`
	SpanID             string            `parquet:"span_id"`
	Stream             string            `parquet:"_stream"`
	StreamID           string            `parquet:"_stream_id"`
	ScopeName          string            `parquet:"scope.name"`
	ResourceAttributes map[string]string `parquet:"resource.attributes"`
	LogAttributes      map[string]string `parquet:"log.attributes"`
}

// TraceRow defines the Parquet schema for trace span records.
// Prefixed fields (resource_attr:, span_attr:) map to MAP columns on read.
// Non-promoted fields are captured in ResourceAttributes, SpanAttributes, and ScopeAttributes.
type TraceRow struct {
	TimestampUnixNano  int64             `parquet:"timestamp_unix_nano"`
	StartTimeUnixNano  int64             `parquet:"start_time_unix_nano"`
	TraceID            string            `parquet:"trace_id"`
	SpanID             string            `parquet:"span_id"`
	ParentSpanID       string            `parquet:"parent_span_id"`
	SpanName           string            `parquet:"span.name"`
	SpanKind           int32             `parquet:"span.kind"`
	StatusCode         int32             `parquet:"status.code"`
	StatusMessage      string            `parquet:"status.message"`
	DurationNs         int64             `parquet:"duration_ns"`
	ServiceName        string            `parquet:"service.name"`
	ScopeName          string            `parquet:"scope.name"`
	DeployEnv          string            `parquet:"resource_attr:deployment.environment"`
	CloudRegion        string            `parquet:"resource_attr:cloud.region"`
	HostName           string            `parquet:"resource_attr:host.name"`
	K8sNamespaceName   string            `parquet:"resource_attr:k8s.namespace.name"`
	K8sDeploymentName  string            `parquet:"resource_attr:k8s.deployment.name"`
	K8sNodeName        string            `parquet:"resource_attr:k8s.node.name"`
	HTTPMethod         string            `parquet:"span_attr:http.method"`
	HTTPStatusCode     string            `parquet:"span_attr:http.status_code"`
	HTTPUrl            string            `parquet:"span_attr:http.url"`
	DBSystem           string            `parquet:"span_attr:db.system"`
	DBStatement        string            `parquet:"span_attr:db.statement"`
	ResourceAttributes map[string]string `parquet:"resource.attributes"`
	SpanAttributes     map[string]string `parquet:"span.attributes"`
	ScopeAttributes    map[string]string `parquet:"scope.attributes"`
}
