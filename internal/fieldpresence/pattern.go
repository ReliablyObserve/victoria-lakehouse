package fieldpresence

import "github.com/ReliablyObserve/victoria-lakehouse/internal/schema"

// The names a row's string columns produce depend only on which of them are
// not empty (and, for the span event and link JSON columns, on their content),
// so a writer computes the names of a row once per pattern, with the read
// path's own row-to-field conversion, and not once per row. These functions are
// the patterns; TestPatternCoversEveryStringColumn fails when a column is added
// to a row type and not here.

// LogPattern is the set of non-empty string columns of a row, one bit each.
func LogPattern(r *schema.LogRow) (p uint64) {
	if r.Body != "" {
		p |= 1 << 0
	}
	if r.SeverityText != "" {
		p |= 1 << 1
	}
	if r.ServiceName != "" {
		p |= 1 << 2
	}
	if r.TraceID != "" {
		p |= 1 << 3
	}
	if r.SpanID != "" {
		p |= 1 << 4
	}
	if r.K8sNamespaceName != "" {
		p |= 1 << 5
	}
	if r.K8sPodName != "" {
		p |= 1 << 6
	}
	if r.K8sDeploymentName != "" {
		p |= 1 << 7
	}
	if r.K8sNodeName != "" {
		p |= 1 << 8
	}
	if r.DeployEnv != "" {
		p |= 1 << 9
	}
	if r.CloudRegion != "" {
		p |= 1 << 10
	}
	if r.HostName != "" {
		p |= 1 << 11
	}
	if r.Stream != "" {
		p |= 1 << 12
	}
	if r.StreamID != "" {
		p |= 1 << 13
	}
	if r.ScopeName != "" {
		p |= 1 << 14
	}
	if r.ContainerID != "" {
		p |= 1 << 15
	}
	if r.ServiceInstanceID != "" {
		p |= 1 << 16
	}
	if r.ServiceVersion != "" {
		p |= 1 << 17
	}
	if r.ExceptionType != "" {
		p |= 1 << 18
	}
	if r.ExceptionMessage != "" {
		p |= 1 << 19
	}
	if r.K8sClusterName != "" {
		p |= 1 << 20
	}
	if r.TelemetrySDKName != "" {
		p |= 1 << 21
	}
	if r.TelemetrySDKLang != "" {
		p |= 1 << 22
	}
	if r.TelemetrySDKVer != "" {
		p |= 1 << 23
	}
	if r.CloudAccountID != "" {
		p |= 1 << 24
	}
	if r.CloudProvider != "" {
		p |= 1 << 25
	}
	if r.OSType != "" {
		p |= 1 << 26
	}
	if r.HostArch != "" {
		p |= 1 << 27
	}
	if r.ProcessRuntimeName != "" {
		p |= 1 << 28
	}
	if r.ProcessRuntimeVer != "" {
		p |= 1 << 29
	}
	if r.DedS01 != "" {
		p |= 1 << 30
	}
	if r.DedS02 != "" {
		p |= 1 << 31
	}
	if r.DedS03 != "" {
		p |= 1 << 32
	}
	if r.DedS04 != "" {
		p |= 1 << 33
	}
	if r.DedS05 != "" {
		p |= 1 << 34
	}
	if r.DedS06 != "" {
		p |= 1 << 35
	}
	if r.DedS07 != "" {
		p |= 1 << 36
	}
	if r.DedS08 != "" {
		p |= 1 << 37
	}
	return p
}

// tracePatternLow is the first half of TracePattern (bits 0 to 25).
func tracePatternLow(r *schema.TraceRow) (p uint64) {
	if r.Body != "" {
		p |= 1 << 0
	}
	if r.TraceID != "" {
		p |= 1 << 1
	}
	if r.SpanID != "" {
		p |= 1 << 2
	}
	if r.ParentSpanID != "" {
		p |= 1 << 3
	}
	if r.SpanName != "" {
		p |= 1 << 4
	}
	if r.ServiceName != "" {
		p |= 1 << 5
	}
	if r.StatusMessage != "" {
		p |= 1 << 6
	}
	if r.HTTPMethod != "" {
		p |= 1 << 7
	}
	if r.HTTPStatusCode != "" {
		p |= 1 << 8
	}
	if r.HTTPUrl != "" {
		p |= 1 << 9
	}
	if r.DBSystem != "" {
		p |= 1 << 10
	}
	if r.DBStatement != "" {
		p |= 1 << 11
	}
	if r.K8sNamespaceName != "" {
		p |= 1 << 12
	}
	if r.K8sPodName != "" {
		p |= 1 << 13
	}
	if r.K8sDeploymentName != "" {
		p |= 1 << 14
	}
	if r.K8sNodeName != "" {
		p |= 1 << 15
	}
	if r.DeployEnv != "" {
		p |= 1 << 16
	}
	if r.CloudRegion != "" {
		p |= 1 << 17
	}
	if r.HostName != "" {
		p |= 1 << 18
	}
	if r.Stream != "" {
		p |= 1 << 19
	}
	if r.StreamID != "" {
		p |= 1 << 20
	}
	if r.ScopeName != "" {
		p |= 1 << 21
	}
	if r.URLFull != "" {
		p |= 1 << 22
	}
	if r.ClientAddress != "" {
		p |= 1 << 23
	}
	if r.ServerAddress != "" {
		p |= 1 << 24
	}
	if r.NetworkPeerAddress != "" {
		p |= 1 << 25
	}
	return p
}

// tracePatternHigh is the second half of TracePattern.
func tracePatternHigh(r *schema.TraceRow) (p uint64) {
	if r.DBCollectionName != "" {
		p |= 1 << 26
	}
	if r.DBOperationName != "" {
		p |= 1 << 27
	}
	if r.RPCMethod != "" {
		p |= 1 << 28
	}
	if r.MessagingDestination != "" {
		p |= 1 << 29
	}
	if r.CodeFunctionName != "" {
		p |= 1 << 30
	}
	if r.ExceptionType != "" {
		p |= 1 << 31
	}
	if r.ContainerID != "" {
		p |= 1 << 32
	}
	if r.ServiceInstanceID != "" {
		p |= 1 << 33
	}
	if r.K8sClusterName != "" {
		p |= 1 << 34
	}
	if r.TelemetrySDKName != "" {
		p |= 1 << 35
	}
	if r.CloudAccountID != "" {
		p |= 1 << 36
	}
	if r.DBQueryText != "" {
		p |= 1 << 37
	}
	if r.DedS01 != "" {
		p |= 1 << 38
	}
	if r.DedS02 != "" {
		p |= 1 << 39
	}
	if r.DedS03 != "" {
		p |= 1 << 40
	}
	if r.DedS04 != "" {
		p |= 1 << 41
	}
	if r.DedS05 != "" {
		p |= 1 << 42
	}
	if r.DedS06 != "" {
		p |= 1 << 43
	}
	if r.DedS07 != "" {
		p |= 1 << 44
	}
	if r.DedS08 != "" {
		p |= 1 << 45
	}
	if r.ServiceGraphParent != "" {
		p |= 1 << 46
	}
	if r.ServiceGraphChild != "" {
		p |= 1 << 47
	}
	if r.ServiceGraphCallCount != "" {
		p |= 1 << 48
	}
	if r.EventsJSON != "" {
		p |= 1 << 49
	}
	if r.LinksJSON != "" {
		p |= 1 << 50
	}
	return p
}

// TracePattern is the set of non-empty string columns of a span, one bit each.
func TracePattern(r *schema.TraceRow) uint64 { return tracePatternLow(r) | tracePatternHigh(r) }
