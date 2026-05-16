package schema

type LogRow struct {
	TimestampUnixNano  int64             `json:"timestamp_unix_nano" parquet:"timestamp_unix_nano"`
	Body               string            `json:"body" parquet:"body"`
	SeverityText       string            `json:"severity_text" parquet:"severity_text"`
	SeverityNumber     int32             `json:"severity_number" parquet:"severity_number"`
	ServiceName        string            `json:"service.name" parquet:"service.name"`
	TraceID            string            `json:"trace_id" parquet:"trace_id"`
	SpanID             string            `json:"span_id" parquet:"span_id"`
	K8sNamespaceName   string            `json:"k8s.namespace.name" parquet:"k8s.namespace.name"`
	K8sPodName         string            `json:"k8s.pod.name" parquet:"k8s.pod.name"`
	K8sDeploymentName  string            `json:"k8s.deployment.name" parquet:"k8s.deployment.name"`
	K8sNodeName        string            `json:"k8s.node.name" parquet:"k8s.node.name"`
	DeployEnv          string            `json:"deployment.environment" parquet:"deployment.environment"`
	CloudRegion        string            `json:"cloud.region" parquet:"cloud.region"`
	HostName           string            `json:"host.name" parquet:"host.name"`
	Stream             string            `json:"_stream" parquet:"_stream"`
	StreamID           string            `json:"_stream_id" parquet:"_stream_id"`
	ScopeName          string            `json:"scope.name" parquet:"scope.name"`
	ResourceAttributes map[string]string `json:"resource.attributes,omitempty" parquet:"resource.attributes,optional"`
	LogAttributes      map[string]string `json:"log.attributes,omitempty" parquet:"log.attributes,optional"`
	ScopeAttributes    map[string]string `json:"scope.attributes,omitempty" parquet:"scope.attributes,optional"`
}

type TraceRow struct {
	TimestampUnixNano  int64             `json:"timestamp_unix_nano" parquet:"timestamp_unix_nano"`
	StartTimeUnixNano  int64             `json:"start_time_unix_nano" parquet:"start_time_unix_nano"`
	TraceID            string            `json:"trace_id" parquet:"trace_id"`
	SpanID             string            `json:"span_id" parquet:"span_id"`
	ParentSpanID       string            `json:"parent_span_id" parquet:"parent_span_id"`
	SpanName           string            `json:"span.name" parquet:"span.name"`
	ServiceName        string            `json:"service.name" parquet:"service.name"`
	DurationNs         int64             `json:"duration_ns" parquet:"duration_ns"`
	StatusCode         int32             `json:"status.code" parquet:"status.code"`
	StatusMessage      string            `json:"status.message" parquet:"status.message"`
	SpanKind           int32             `json:"span.kind" parquet:"span.kind"`
	HTTPMethod         string            `json:"http.method" parquet:"http.method"`
	HTTPStatusCode     string            `json:"http.status_code" parquet:"http.status_code"`
	HTTPUrl            string            `json:"http.url" parquet:"http.url"`
	DBSystem           string            `json:"db.system" parquet:"db.system"`
	DBStatement        string            `json:"db.statement" parquet:"db.statement"`
	K8sNamespaceName   string            `json:"k8s.namespace.name" parquet:"k8s.namespace.name"`
	K8sPodName         string            `json:"k8s.pod.name" parquet:"k8s.pod.name"`
	K8sDeploymentName  string            `json:"k8s.deployment.name" parquet:"k8s.deployment.name"`
	K8sNodeName        string            `json:"k8s.node.name" parquet:"k8s.node.name"`
	DeployEnv          string            `json:"deployment.environment" parquet:"deployment.environment"`
	CloudRegion        string            `json:"cloud.region" parquet:"cloud.region"`
	HostName           string            `json:"host.name" parquet:"host.name"`
	Stream             string            `json:"_stream" parquet:"_stream"`
	StreamID           string            `json:"_stream_id" parquet:"_stream_id"`
	ScopeName          string            `json:"scope.name" parquet:"scope.name"`
	ResourceAttributes map[string]string `json:"resource.attributes,omitempty" parquet:"resource.attributes,optional"`
	SpanAttributes     map[string]string `json:"span.attributes,omitempty" parquet:"span.attributes,optional"`
	ScopeAttributes    map[string]string `json:"scope.attributes,omitempty" parquet:"scope.attributes,optional"`
}
