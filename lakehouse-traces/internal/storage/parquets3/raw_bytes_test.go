package parquets3

import (
	"strings"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// TestEstimateRawBytesLogs_CountsEveryStringColumn guards against the
// regression where only Body + ServiceName + TraceID + two attribute
// maps were counted, leaving K8s / host / stream fields invisible.
// Real-world Kubernetes ingest populates the K8s columns heavily;
// without this fix raw_bytes < compressed bytes and the API's
// compression ratio inverts to < 1.
func TestEstimateRawBytesLogs_CountsEveryStringColumn(t *testing.T) {
	rows := []schema.LogRow{
		{
			Body:               strings.Repeat("b", 100),
			SeverityText:       "INFO",
			ServiceName:        "checkout",
			TraceID:            strings.Repeat("t", 32),
			SpanID:             strings.Repeat("s", 16),
			K8sNamespaceName:   "prod",
			K8sPodName:         "checkout-7d8f9b-x2k4q",
			K8sDeploymentName:  "checkout",
			K8sNodeName:        "ip-10-0-1-23.ec2.internal",
			DeployEnv:          "production",
			CloudRegion:        "us-east-1",
			HostName:           "checkout-pod-1",
			Stream:             "{service.name=checkout}",
			StreamID:           strings.Repeat("S", 16),
			ScopeName:          "go.opentelemetry.io/contrib",
			ResourceAttributes: map[string]string{"k8s.cluster.name": "prod-east"},
			LogAttributes:      map[string]string{"request_id": "abc123"},
			ScopeAttributes:    map[string]string{"version": "1.2.0"},
		},
	}

	got := estimateRawBytesLogs(rows)
	if got < 350 {
		t.Errorf("raw bytes = %d, want >= 350 (sum of every populated column)", got)
	}

	// Sanity: a sparse row with only Body populated should be far less.
	sparse := []schema.LogRow{{Body: strings.Repeat("b", 100)}}
	sparseGot := estimateRawBytesLogs(sparse)
	if sparseGot >= got {
		t.Errorf("sparse=%d should be much less than populated=%d", sparseGot, got)
	}
}

func TestEstimateRawBytesTraces_CountsEveryStringColumn(t *testing.T) {
	rows := []schema.TraceRow{
		{
			TraceID:           strings.Repeat("t", 32),
			SpanID:            strings.Repeat("s", 16),
			ParentSpanID:      strings.Repeat("p", 16),
			SpanName:          "GET /api/v1/orders",
			ServiceName:       "orders",
			StatusMessage:     "ok",
			HTTPMethod:        "GET",
			HTTPStatusCode:    "200",
			HTTPUrl:           "https://api.example.com/v1/orders",
			DBSystem:          "postgresql",
			DBStatement:       strings.Repeat("S", 200),
			K8sNamespaceName:  "prod",
			K8sPodName:        "orders-abc",
			K8sDeploymentName: "orders",
			K8sNodeName:       "ip-10-0-1-23",
			DeployEnv:         "prod",
			CloudRegion:       "eu-west-1",
			HostName:          "host-1",
			Stream:            "{service.name=orders}",
			StreamID:          strings.Repeat("S", 16),
			ScopeName:         "otel.tracer",
		},
	}
	got := estimateRawBytesTraces(rows)
	if got < 450 {
		t.Errorf("trace raw bytes = %d, want >= 450", got)
	}
}

func TestEstimateRawBytesLogs_EmptyRow_NonNegative(t *testing.T) {
	got := estimateRawBytesLogs([]schema.LogRow{{}})
	if got < int64(fixedLogRowBytes) {
		t.Errorf("empty row raw_bytes = %d, want >= fixed scalar size %d", got, fixedLogRowBytes)
	}
}
