package schema

import "testing"

// The raw-byte estimate feeds manifest.FileInfo.RawBytes, which is the
// denominator of every compression ratio the cold tier reports. It lives here
// so the flush writer, the compactor and the delete rewriter all compute it
// identically; these tests pin the properties that make that sharing worth
// anything.

func TestEstimateRawBytesLogs_CountsEveryPersistedColumn(t *testing.T) {
	empty := EstimateRawBytesLogs([]LogRow{{}})
	if empty != fixedLogRowBytes {
		t.Fatalf("an empty row costs %d, want the fixed scalar size %d", empty, fixedLogRowBytes)
	}

	// Each variable-width column must contribute its own bytes. An estimate
	// that counted only body + service under-counted real workloads by ~70% and
	// produced compression ratios below 1.0 on small files.
	for _, tc := range []struct {
		name string
		row  LogRow
		want int64
	}{
		{"body", LogRow{Body: "abcd"}, 4},
		{"severity_text", LogRow{SeverityText: "error"}, 5},
		{"service.name", LogRow{ServiceName: "web"}, 3},
		{"trace_id", LogRow{TraceID: "abcdef"}, 6},
		{"span_id", LogRow{SpanID: "ab"}, 2},
		{"k8s.namespace.name", LogRow{K8sNamespaceName: "ns"}, 2},
		{"k8s.pod.name", LogRow{K8sPodName: "pod"}, 3},
		{"k8s.deployment.name", LogRow{K8sDeploymentName: "dep"}, 3},
		{"k8s.node.name", LogRow{K8sNodeName: "node"}, 4},
		{"deployment.environment", LogRow{DeployEnv: "prod"}, 4},
		{"cloud.region", LogRow{CloudRegion: "eu-1"}, 4},
		{"host.name", LogRow{HostName: "h"}, 1},
		{"_stream", LogRow{Stream: "{a}"}, 3},
		{"_stream_id", LogRow{StreamID: "sid"}, 3},
		{"scope.name", LogRow{ScopeName: "sc"}, 2},
		{"resource attributes", LogRow{ResourceAttributes: map[string]string{"k": "vv"}}, 3},
		{"log attributes", LogRow{LogAttributes: map[string]string{"kk": "v"}}, 3},
		{"scope attributes", LogRow{ScopeAttributes: map[string]string{"k": "v"}}, 2},
	} {
		got := EstimateRawBytesLogs([]LogRow{tc.row})
		if want := fixedLogRowBytes + tc.want; got != want {
			t.Errorf("%s: estimate = %d, want %d", tc.name, got, want)
		}
	}
}

func TestEstimateRawBytesLogs_IsAdditiveAcrossRows(t *testing.T) {
	one := LogRow{Body: "hello", ServiceName: "web"}
	single := EstimateRawBytesLogs([]LogRow{one})
	if got := EstimateRawBytesLogs([]LogRow{one, one, one}); got != 3*single {
		t.Fatalf("three identical rows estimate %d, want 3 × %d", got, single)
	}
	if got := EstimateRawBytesLogs(nil); got != 0 {
		t.Fatalf("no rows estimate %d, want 0", got)
	}
}

func TestEstimateRawBytesTraces_CountsEveryPersistedColumn(t *testing.T) {
	empty := EstimateRawBytesTraces([]TraceRow{{}})
	if empty != fixedTraceRowBytes {
		t.Fatalf("an empty span costs %d, want the fixed scalar size %d", empty, fixedTraceRowBytes)
	}

	for _, tc := range []struct {
		name string
		row  TraceRow
		want int64
	}{
		{"trace_id", TraceRow{TraceID: "abcd"}, 4},
		{"span_id", TraceRow{SpanID: "ab"}, 2},
		{"parent_span_id", TraceRow{ParentSpanID: "pp"}, 2},
		{"span.name", TraceRow{SpanName: "GET /x"}, 6},
		{"service.name", TraceRow{ServiceName: "svc"}, 3},
		{"status.message", TraceRow{StatusMessage: "ok"}, 2},
		{"http.method", TraceRow{HTTPMethod: "GET"}, 3},
		{"http.status_code", TraceRow{HTTPStatusCode: "200"}, 3},
		{"http.url", TraceRow{HTTPUrl: "/a"}, 2},
		{"db.system", TraceRow{DBSystem: "pg"}, 2},
		{"db.statement", TraceRow{DBStatement: "SELECT"}, 6},
		{"k8s.namespace.name", TraceRow{K8sNamespaceName: "ns"}, 2},
		{"k8s.pod.name", TraceRow{K8sPodName: "pod"}, 3},
		{"k8s.deployment.name", TraceRow{K8sDeploymentName: "dep"}, 3},
		{"k8s.node.name", TraceRow{K8sNodeName: "node"}, 4},
		{"deployment.environment", TraceRow{DeployEnv: "prod"}, 4},
		{"cloud.region", TraceRow{CloudRegion: "eu-1"}, 4},
		{"host.name", TraceRow{HostName: "h"}, 1},
		{"_stream", TraceRow{Stream: "{a}"}, 3},
		{"_stream_id", TraceRow{StreamID: "sid"}, 3},
		{"scope.name", TraceRow{ScopeName: "sc"}, 2},
		{"resource attributes", TraceRow{ResourceAttributes: map[string]string{"k": "vv"}}, 3},
		{"span attributes", TraceRow{SpanAttributes: map[string]string{"kk": "v"}}, 3},
		{"scope attributes", TraceRow{ScopeAttributes: map[string]string{"k": "v"}}, 2},
	} {
		got := EstimateRawBytesTraces([]TraceRow{tc.row})
		if want := fixedTraceRowBytes + tc.want; got != want {
			t.Errorf("%s: estimate = %d, want %d", tc.name, got, want)
		}
	}

	if got := EstimateRawBytesTraces(nil); got != 0 {
		t.Fatalf("no spans estimate %d, want 0", got)
	}
}

// TestEstimateRawBytes_ShrinksWithRemovedRows is the property the delete
// rewriter depends on: a file with rows removed must report a smaller raw size
// than the file it replaced, or its compression ratio keeps describing data it
// no longer holds.
func TestEstimateRawBytes_ShrinksWithRemovedRows(t *testing.T) {
	all := []LogRow{
		{Body: "keep", ServiceName: "web"},
		{Body: "drop-a-long-body", ServiceName: "web"},
		{Body: "keep-2", ServiceName: "api"},
	}
	kept := []LogRow{all[0], all[2]}
	if EstimateRawBytesLogs(kept) >= EstimateRawBytesLogs(all) {
		t.Fatal("removing rows must reduce the raw-byte estimate")
	}

	spans := []TraceRow{
		{SpanName: "keep", ServiceName: "svc"},
		{SpanName: "drop-a-long-span-name", ServiceName: "svc"},
	}
	if EstimateRawBytesTraces(spans[:1]) >= EstimateRawBytesTraces(spans) {
		t.Fatal("removing spans must reduce the raw-byte estimate")
	}
}
