package schema

// fixedLogRowBytes covers LogRow's fixed scalars: 2× uint32 tenant (8),
// timestamp_ns (8), severity_number int32 (4) = 20 bytes per row.
const fixedLogRowBytes = 20

// fixedTraceRowBytes covers TraceRow's fixed scalars: 2× uint32 tenant (8),
// timestamp_ns (8), start_time_ns (8), duration_ns (8), status_code int32 (4),
// span_kind int32 (4) = 40 bytes per row.
const fixedTraceRowBytes = 40

// EstimateRawBytesLogs sums the byte count of every column the writer actually
// persists, so the manifest's RawBytes is comparable to len(parquet_file) and
// the compression ratio doesn't invert for rows where the heavy fields are in
// K8s / host columns rather than in body.
//
// It lives here, next to the row definition, because all three producers of a
// manifest entry need the identical measure: the flush writer, the compactor,
// and the delete rewriter. A rewriter with its own copy would drift from the
// writer's and make a rewritten file's compression ratio incomparable to every
// other file's.
func EstimateRawBytesLogs(rows []LogRow) int64 {
	var total int64
	for i := range rows {
		r := &rows[i]
		total += fixedLogRowBytes
		total += int64(len(r.Body))
		total += int64(len(r.SeverityText))
		total += int64(len(r.ServiceName))
		total += int64(len(r.TraceID))
		total += int64(len(r.SpanID))
		total += int64(len(r.K8sNamespaceName))
		total += int64(len(r.K8sPodName))
		total += int64(len(r.K8sDeploymentName))
		total += int64(len(r.K8sNodeName))
		total += int64(len(r.DeployEnv))
		total += int64(len(r.CloudRegion))
		total += int64(len(r.HostName))
		total += int64(len(r.Stream))
		total += int64(len(r.StreamID))
		total += int64(len(r.ScopeName))
		for k, v := range r.ResourceAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range r.LogAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range r.ScopeAttributes {
			total += int64(len(k) + len(v))
		}
	}
	return total
}

// EstimateRawBytesTraces is EstimateRawBytesLogs for spans. See its doc for why
// this measure is shared rather than reimplemented per producer.
func EstimateRawBytesTraces(rows []TraceRow) int64 {
	var total int64
	for i := range rows {
		r := &rows[i]
		total += fixedTraceRowBytes
		total += int64(len(r.TraceID))
		total += int64(len(r.SpanID))
		total += int64(len(r.ParentSpanID))
		total += int64(len(r.SpanName))
		total += int64(len(r.ServiceName))
		total += int64(len(r.StatusMessage))
		total += int64(len(r.HTTPMethod))
		total += int64(len(r.HTTPStatusCode))
		total += int64(len(r.HTTPUrl))
		total += int64(len(r.DBSystem))
		total += int64(len(r.DBStatement))
		total += int64(len(r.K8sNamespaceName))
		total += int64(len(r.K8sPodName))
		total += int64(len(r.K8sDeploymentName))
		total += int64(len(r.K8sNodeName))
		total += int64(len(r.DeployEnv))
		total += int64(len(r.CloudRegion))
		total += int64(len(r.HostName))
		total += int64(len(r.Stream))
		total += int64(len(r.StreamID))
		total += int64(len(r.ScopeName))
		for k, v := range r.ResourceAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range r.SpanAttributes {
			total += int64(len(k) + len(v))
		}
		for k, v := range r.ScopeAttributes {
			total += int64(len(k) + len(v))
		}
	}
	return total
}
