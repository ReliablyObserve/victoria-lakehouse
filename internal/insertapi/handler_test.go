package insertapi

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

type mockStore struct {
	rows     []schema.LogRow
	writeErr error
}

func (m *mockStore) MustAddLogRows(rows []schema.LogRow) {
	m.rows = append(m.rows, rows...)
}

func (m *mockStore) CanWriteData() error {
	return m.writeErr
}

func testHandler(store LogStore) *Handler {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	return NewHandler(store, cfg)
}

// --- jsonFieldsToLogRow tests ---

func TestJsonFieldsToLogRow_Basic(t *testing.T) {
	fields := map[string]any{
		"_msg":                   "hello world",
		"level":                  "INFO",
		"service.name":           "api-gateway",
		"trace_id":               "abc123",
		"deployment.environment": "production",
	}

	row := jsonFieldsToLogRow(fields, promotedLogFields)

	if row.Body != "hello world" {
		t.Errorf("Body = %q, want %q", row.Body, "hello world")
	}
	if row.SeverityText != "INFO" {
		t.Errorf("SeverityText = %q, want %q", row.SeverityText, "INFO")
	}
	if row.ServiceName != "api-gateway" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "api-gateway")
	}
	if row.TraceID != "abc123" {
		t.Errorf("TraceID = %q, want %q", row.TraceID, "abc123")
	}
	if row.DeployEnv != "production" {
		t.Errorf("DeployEnv = %q, want %q", row.DeployEnv, "production")
	}
	if row.TimestampUnixNano <= 0 {
		t.Error("TimestampUnixNano should be set to current time")
	}
}

func TestJsonFieldsToLogRow_WithTime(t *testing.T) {
	ts := time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC)
	fields := map[string]any{
		"_time": ts.Format(time.RFC3339Nano),
		"_msg":  "test",
	}

	row := jsonFieldsToLogRow(fields, promotedLogFields)

	if row.TimestampUnixNano != ts.UnixNano() {
		t.Errorf("TimestampUnixNano = %d, want %d", row.TimestampUnixNano, ts.UnixNano())
	}
}

func TestJsonFieldsToLogRow_NumericTime(t *testing.T) {
	tsNano := float64(time.Date(2026, 5, 3, 12, 0, 0, 0, time.UTC).UnixNano())
	fields := map[string]any{
		"_time": tsNano,
		"_msg":  "test",
	}

	row := jsonFieldsToLogRow(fields, promotedLogFields)

	if row.TimestampUnixNano != int64(tsNano) {
		t.Errorf("TimestampUnixNano = %d, want %d", row.TimestampUnixNano, int64(tsNano))
	}
}

func TestJsonFieldsToLogRow_InvalidTime(t *testing.T) {
	fields := map[string]any{
		"_time": "not-a-timestamp",
		"_msg":  "test",
	}
	row := jsonFieldsToLogRow(fields, promotedLogFields)
	if row.TimestampUnixNano <= 0 {
		t.Error("should fall back to current time for invalid time string")
	}
}

func TestJsonFieldsToLogRow_AllFields(t *testing.T) {
	fields := map[string]any{
		"_msg":                   "full test",
		"level":                  "ERROR",
		"service.name":           "svc",
		"k8s.namespace.name":     "ns",
		"k8s.pod.name":           "pod-1",
		"k8s.deployment.name":    "deploy-1",
		"k8s.node.name":          "node-1",
		"deployment.environment": "staging",
		"cloud.region":           "us-west-2",
		"host.name":              "host-1",
		"trace_id":               "t123",
		"span_id":                "s456",
		"scope.name":             "scope-1",
	}

	row := jsonFieldsToLogRow(fields, promotedLogFields)

	checks := map[string]struct{ got, want string }{
		"Body":              {row.Body, "full test"},
		"SeverityText":      {row.SeverityText, "ERROR"},
		"ServiceName":       {row.ServiceName, "svc"},
		"K8sNamespaceName":  {row.K8sNamespaceName, "ns"},
		"K8sPodName":        {row.K8sPodName, "pod-1"},
		"K8sDeploymentName": {row.K8sDeploymentName, "deploy-1"},
		"K8sNodeName":       {row.K8sNodeName, "node-1"},
		"DeployEnv":         {row.DeployEnv, "staging"},
		"CloudRegion":       {row.CloudRegion, "us-west-2"},
		"HostName":          {row.HostName, "host-1"},
		"TraceID":           {row.TraceID, "t123"},
		"SpanID":            {row.SpanID, "s456"},
		"ScopeName":         {row.ScopeName, "scope-1"},
	}

	for name, c := range checks {
		if c.got != c.want {
			t.Errorf("%s = %q, want %q", name, c.got, c.want)
		}
	}

	if row.Stream == "" {
		t.Error("Stream should be set when ServiceName is present")
	}
}

func TestJsonFieldsToLogRow_NoStreamWithoutServiceOrNamespace(t *testing.T) {
	fields := map[string]any{
		"_msg": "test",
	}
	row := jsonFieldsToLogRow(fields, promotedLogFields)
	if row.Stream != "" {
		t.Errorf("Stream should be empty, got %q", row.Stream)
	}
}

// --- applyStreamLabels tests ---

func TestApplyStreamLabels(t *testing.T) {
	row := &schema.LogRow{}
	labels := map[string]string{
		"service":   "my-service",
		"namespace": "production",
		"level":     "WARN",
	}

	applyStreamLabels(row, labels)

	if row.ServiceName != "my-service" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "my-service")
	}
	if row.K8sNamespaceName != "production" {
		t.Errorf("K8sNamespaceName = %q, want %q", row.K8sNamespaceName, "production")
	}
	if row.SeverityText != "WARN" {
		t.Errorf("SeverityText = %q, want %q", row.SeverityText, "WARN")
	}
	if row.Stream == "" {
		t.Error("Stream should be set from labels")
	}
}

func TestApplyStreamLabels_AllAliases(t *testing.T) {
	row := &schema.LogRow{}
	labels := map[string]string{
		"service.name":           "svc-dot",
		"k8s.namespace.name":     "ns-dot",
		"k8s.pod.name":           "pod-dot",
		"k8s.deployment.name":    "deploy-dot",
		"k8s.node.name":          "node-dot",
		"deployment.environment": "env-dot",
		"cloud.region":           "region-dot",
		"host.name":              "host-dot",
	}

	applyStreamLabels(row, labels)

	if row.ServiceName != "svc-dot" {
		t.Errorf("ServiceName = %q, want %q", row.ServiceName, "svc-dot")
	}
	if row.K8sNamespaceName != "ns-dot" {
		t.Errorf("K8sNamespaceName = %q, want %q", row.K8sNamespaceName, "ns-dot")
	}
	if row.K8sPodName != "pod-dot" {
		t.Errorf("K8sPodName = %q, want %q", row.K8sPodName, "pod-dot")
	}
	if row.K8sDeploymentName != "deploy-dot" {
		t.Errorf("K8sDeploymentName = %q, want %q", row.K8sDeploymentName, "deploy-dot")
	}
	if row.K8sNodeName != "node-dot" {
		t.Errorf("K8sNodeName = %q, want %q", row.K8sNodeName, "node-dot")
	}
	if row.DeployEnv != "env-dot" {
		t.Errorf("DeployEnv = %q, want %q", row.DeployEnv, "env-dot")
	}
	if row.CloudRegion != "region-dot" {
		t.Errorf("CloudRegion = %q, want %q", row.CloudRegion, "region-dot")
	}
	if row.HostName != "host-dot" {
		t.Errorf("HostName = %q, want %q", row.HostName, "host-dot")
	}
}

func TestApplyStreamLabels_ShortAliases(t *testing.T) {
	row := &schema.LogRow{}
	labels := map[string]string{
		"pod":        "pod-short",
		"deployment": "deploy-short",
		"node":       "node-short",
		"env":        "env-short",
		"region":     "region-short",
		"host":       "host-short",
	}

	applyStreamLabels(row, labels)

	if row.K8sPodName != "pod-short" {
		t.Errorf("K8sPodName = %q, want %q", row.K8sPodName, "pod-short")
	}
	if row.K8sDeploymentName != "deploy-short" {
		t.Errorf("K8sDeploymentName = %q, want %q", row.K8sDeploymentName, "deploy-short")
	}
	if row.K8sNodeName != "node-short" {
		t.Errorf("K8sNodeName = %q, want %q", row.K8sNodeName, "node-short")
	}
	if row.DeployEnv != "env-short" {
		t.Errorf("DeployEnv = %q, want %q", row.DeployEnv, "env-short")
	}
	if row.CloudRegion != "region-short" {
		t.Errorf("CloudRegion = %q, want %q", row.CloudRegion, "region-short")
	}
	if row.HostName != "host-short" {
		t.Errorf("HostName = %q, want %q", row.HostName, "host-short")
	}
}

func TestApplyStreamLabels_Empty(t *testing.T) {
	row := &schema.LogRow{}
	applyStreamLabels(row, map[string]string{})
	if row.Stream != "" {
		t.Errorf("Stream should be empty for no labels, got %q", row.Stream)
	}
}

// --- NewHandler + Register tests ---

func TestNewHandler(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)
	if h == nil {
		t.Fatal("NewHandler returned nil")
	}
	if h.store == nil {
		t.Error("store is nil")
	}
}

func TestRegister(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)
	mux := http.NewServeMux()
	h.Register(mux)

	paths := []string{
		"/insert/jsonline",
		"/insert/loki/api/v1/push",
		"/insert/elasticsearch/_bulk",
	}
	for _, p := range paths {
		req := httptest.NewRequest(http.MethodPost, p, nil)
		rr := httptest.NewRecorder()
		mux.ServeHTTP(rr, req)
		if rr.Code == http.StatusNotFound {
			t.Errorf("path %s not registered (404)", p)
		}
	}
}

// --- handleJSONLine tests ---

func TestHandleJSONLine_Success(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	body := `{"_msg":"hello","level":"INFO","service.name":"svc1"}
{"_msg":"world","level":"ERROR","service.name":"svc2"}
`
	req := httptest.NewRequest(http.MethodPost, "/insert/jsonline", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.handleJSONLine(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(store.rows))
	}
	if store.rows[0].Body != "hello" {
		t.Errorf("rows[0].Body = %q, want %q", store.rows[0].Body, "hello")
	}
	if store.rows[1].SeverityText != "ERROR" {
		t.Errorf("rows[1].SeverityText = %q, want %q", store.rows[1].SeverityText, "ERROR")
	}
}

func TestHandleJSONLine_EmptyBody(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/insert/jsonline", strings.NewReader(""))
	rr := httptest.NewRecorder()

	h.handleJSONLine(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 0 {
		t.Errorf("rows = %d, want 0", len(store.rows))
	}
}

func TestHandleJSONLine_InvalidJSON(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	body := `not json
{"_msg":"valid"}
also not json
`
	req := httptest.NewRequest(http.MethodPost, "/insert/jsonline", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.handleJSONLine(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 1 {
		t.Fatalf("rows = %d, want 1 (only valid line)", len(store.rows))
	}
	if store.rows[0].Body != "valid" {
		t.Errorf("Body = %q, want %q", store.rows[0].Body, "valid")
	}
}

func TestHandleJSONLine_MethodNotAllowed(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodGet, "/insert/jsonline", nil)
	rr := httptest.NewRecorder()

	h.handleJSONLine(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleJSONLine_CannotWrite(t *testing.T) {
	store := &mockStore{writeErr: fmt.Errorf("S3 unreachable")}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/insert/jsonline", strings.NewReader(`{"_msg":"test"}`))
	rr := httptest.NewRecorder()

	h.handleJSONLine(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleJSONLine_BlankLines(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	body := "\n\n{\"_msg\":\"test\"}\n\n"
	req := httptest.NewRequest(http.MethodPost, "/insert/jsonline", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.handleJSONLine(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 1 {
		t.Errorf("rows = %d, want 1", len(store.rows))
	}
}

// --- handleLokiPush tests ---

func TestHandleLokiPush_Success(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	tsNano := fmt.Sprintf("%d", time.Now().UnixNano())
	push := lokiPushRequest{
		Streams: []lokiStream{
			{
				Stream: map[string]string{"service": "svc1", "level": "INFO"},
				Values: [][]string{
					{tsNano, "log line 1"},
					{tsNano, "log line 2"},
				},
			},
		},
	}
	body, _ := json.Marshal(push)
	req := httptest.NewRequest(http.MethodPost, "/insert/loki/api/v1/push", strings.NewReader(string(body)))
	rr := httptest.NewRecorder()

	h.handleLokiPush(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(store.rows))
	}
	if store.rows[0].Body != "log line 1" {
		t.Errorf("Body = %q, want %q", store.rows[0].Body, "log line 1")
	}
	if store.rows[0].ServiceName != "svc1" {
		t.Errorf("ServiceName = %q, want %q", store.rows[0].ServiceName, "svc1")
	}
	if store.rows[0].SeverityText != "INFO" {
		t.Errorf("SeverityText = %q, want %q", store.rows[0].SeverityText, "INFO")
	}
}

func TestHandleLokiPush_InvalidTimestamp(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	push := lokiPushRequest{
		Streams: []lokiStream{
			{
				Stream: map[string]string{"service": "svc"},
				Values: [][]string{
					{"not-a-number", "line"},
				},
			},
		},
	}
	body, _ := json.Marshal(push)
	req := httptest.NewRequest(http.MethodPost, "/insert/loki/api/v1/push", strings.NewReader(string(body)))
	rr := httptest.NewRecorder()

	h.handleLokiPush(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(store.rows))
	}
	if store.rows[0].TimestampUnixNano <= 0 {
		t.Error("should fall back to current time")
	}
}

func TestHandleLokiPush_ShortEntry(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	push := lokiPushRequest{
		Streams: []lokiStream{
			{
				Stream: map[string]string{},
				Values: [][]string{
					{"only-one-element"},
				},
			},
		},
	}
	body, _ := json.Marshal(push)
	req := httptest.NewRequest(http.MethodPost, "/insert/loki/api/v1/push", strings.NewReader(string(body)))
	rr := httptest.NewRecorder()

	h.handleLokiPush(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 0 {
		t.Errorf("rows = %d, want 0 (short entry skipped)", len(store.rows))
	}
}

func TestHandleLokiPush_MethodNotAllowed(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodGet, "/insert/loki/api/v1/push", nil)
	rr := httptest.NewRecorder()

	h.handleLokiPush(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleLokiPush_CannotWrite(t *testing.T) {
	store := &mockStore{writeErr: fmt.Errorf("S3 unreachable")}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/insert/loki/api/v1/push", strings.NewReader(`{"streams":[]}`))
	rr := httptest.NewRecorder()

	h.handleLokiPush(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleLokiPush_InvalidJSON(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/insert/loki/api/v1/push", strings.NewReader("not json"))
	rr := httptest.NewRecorder()

	h.handleLokiPush(rr, req)

	if rr.Code != http.StatusBadRequest {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusBadRequest)
	}
}

func TestHandleLokiPush_EmptyStreams(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/insert/loki/api/v1/push", strings.NewReader(`{"streams":[]}`))
	rr := httptest.NewRecorder()

	h.handleLokiPush(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 0 {
		t.Errorf("rows = %d, want 0", len(store.rows))
	}
}

func TestHandleLokiPush_MultipleStreams(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	tsNano := fmt.Sprintf("%d", time.Now().UnixNano())
	push := lokiPushRequest{
		Streams: []lokiStream{
			{
				Stream: map[string]string{"service": "svc1"},
				Values: [][]string{{tsNano, "line1"}},
			},
			{
				Stream: map[string]string{"service": "svc2"},
				Values: [][]string{{tsNano, "line2"}, {tsNano, "line3"}},
			},
		},
	}
	body, _ := json.Marshal(push)
	req := httptest.NewRequest(http.MethodPost, "/insert/loki/api/v1/push", strings.NewReader(string(body)))
	rr := httptest.NewRecorder()

	h.handleLokiPush(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 3 {
		t.Errorf("rows = %d, want 3", len(store.rows))
	}
}

// --- handleESBulk tests ---

func TestHandleESBulk_Success(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	body := `{"index":{}}
{"_msg":"es line 1","level":"INFO"}
{"index":{}}
{"_msg":"es line 2","level":"WARN"}
`
	req := httptest.NewRequest(http.MethodPost, "/insert/elasticsearch/_bulk", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.handleESBulk(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if len(store.rows) != 2 {
		t.Fatalf("rows = %d, want 2", len(store.rows))
	}
	if store.rows[0].Body != "es line 1" {
		t.Errorf("Body = %q, want %q", store.rows[0].Body, "es line 1")
	}
	if store.rows[1].SeverityText != "WARN" {
		t.Errorf("SeverityText = %q, want %q", store.rows[1].SeverityText, "WARN")
	}

	ct := rr.Header().Get("Content-Type")
	if ct != "application/json" {
		t.Errorf("Content-Type = %q, want application/json", ct)
	}
}

func TestHandleESBulk_EmptyBody(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/insert/elasticsearch/_bulk", strings.NewReader(""))
	rr := httptest.NewRecorder()

	h.handleESBulk(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if len(store.rows) != 0 {
		t.Errorf("rows = %d, want 0", len(store.rows))
	}
}

func TestHandleESBulk_InvalidDocJSON(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	body := `{"index":{}}
not valid json
{"index":{}}
{"_msg":"valid"}
`
	req := httptest.NewRequest(http.MethodPost, "/insert/elasticsearch/_bulk", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.handleESBulk(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if len(store.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(store.rows))
	}
	if store.rows[0].Body != "valid" {
		t.Errorf("Body = %q, want %q", store.rows[0].Body, "valid")
	}
}

func TestHandleESBulk_MethodNotAllowed(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodGet, "/insert/elasticsearch/_bulk", nil)
	rr := httptest.NewRecorder()

	h.handleESBulk(rr, req)

	if rr.Code != http.StatusMethodNotAllowed {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusMethodNotAllowed)
	}
}

func TestHandleESBulk_CannotWrite(t *testing.T) {
	store := &mockStore{writeErr: fmt.Errorf("S3 unreachable")}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/insert/elasticsearch/_bulk", strings.NewReader(`{"index":{}}\n{"_msg":"test"}`))
	rr := httptest.NewRecorder()

	h.handleESBulk(rr, req)

	if rr.Code != http.StatusServiceUnavailable {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusServiceUnavailable)
	}
}

func TestHandleESBulk_OddLineCount(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	body := `{"index":{}}
{"_msg":"doc1"}
{"index":{}}
`
	req := httptest.NewRequest(http.MethodPost, "/insert/elasticsearch/_bulk", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.handleESBulk(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if len(store.rows) != 1 {
		t.Errorf("rows = %d, want 1", len(store.rows))
	}
}

func TestHandleESBulk_BlankLines(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	body := "\n{\"index\":{}}\n\n{\"_msg\":\"doc1\"}\n\n"
	req := httptest.NewRequest(http.MethodPost, "/insert/elasticsearch/_bulk", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.handleESBulk(rr, req)

	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want %d", rr.Code, http.StatusOK)
	}
	if len(store.rows) != 1 {
		t.Errorf("rows = %d, want 1", len(store.rows))
	}
}

// --- Task 10: Buffer endpoint registration tests ---

func TestHandler_RegisterBufferEndpoint(t *testing.T) {
	store := &mockStore{}
	cfg := config.Default()
	cfg.Mode = config.ModeLogs

	bq := &mockBufferStore{
		logRows: []schema.LogRow{
			{TimestampUnixNano: 1000, Body: "buffered"},
		},
	}
	h := NewHandler(store, cfg, bq)
	if h.bufferHandler == nil {
		t.Fatal("bufferHandler should not be nil when BufferQuerier is provided")
	}

	mux := http.NewServeMux()
	h.Register(mux)

	// Verify the buffer query endpoint is registered and functional
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=2000&mode=logs", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code == http.StatusNotFound {
		t.Error("/internal/buffer/query should be registered (got 404)")
	}
	if rr.Code != http.StatusOK {
		t.Errorf("status = %d, want 200", rr.Code)
	}
}

func TestHandler_NoBufferEndpoint(t *testing.T) {
	store := &mockStore{}
	cfg := config.Default()
	cfg.Mode = config.ModeLogs

	// No BufferQuerier provided
	h := NewHandler(store, cfg)
	if h.bufferHandler != nil {
		t.Error("bufferHandler should be nil when no BufferQuerier is provided")
	}

	mux := http.NewServeMux()
	h.Register(mux)

	// Verify the buffer query endpoint is NOT registered
	req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query?start=0&end=1000&mode=logs", nil)
	rr := httptest.NewRecorder()
	mux.ServeHTTP(rr, req)

	if rr.Code != http.StatusNotFound {
		t.Errorf("status = %d, want 404 (endpoint should not be registered)", rr.Code)
	}
}

func TestHandler_NilBufferQuerier(t *testing.T) {
	store := &mockStore{}
	cfg := config.Default()
	cfg.Mode = config.ModeLogs

	// Explicit nil BufferQuerier
	h := NewHandler(store, cfg, nil)
	if h.bufferHandler != nil {
		t.Error("bufferHandler should be nil when nil BufferQuerier is passed")
	}
}

func TestHandleESBulk_ResponseBody(t *testing.T) {
	store := &mockStore{}
	h := testHandler(store)

	req := httptest.NewRequest(http.MethodPost, "/insert/elasticsearch/_bulk", strings.NewReader(""))
	rr := httptest.NewRecorder()

	h.handleESBulk(rr, req)

	var resp map[string]any
	if err := json.Unmarshal(rr.Body.Bytes(), &resp); err != nil {
		t.Fatalf("response not valid JSON: %v", err)
	}
	if errors, ok := resp["errors"].(bool); !ok || errors {
		t.Errorf("errors = %v, want false", resp["errors"])
	}
}

// --- MAP column capture tests ---

func TestJsonFieldsToLogRow_CapturesNonPromotedFields(t *testing.T) {
	fields := map[string]any{
		"_msg":         "test",
		"service.name": "svc",
		"custom_field": "custom_value",
		"http.method":  "GET",
		"request.id":   "req-123",
	}

	row := jsonFieldsToLogRow(fields, promotedLogFields)

	if row.Body != "test" {
		t.Errorf("Body = %q, want test", row.Body)
	}
	if row.ServiceName != "svc" {
		t.Errorf("ServiceName = %q, want svc", row.ServiceName)
	}
	if row.LogAttributes == nil {
		t.Fatal("LogAttributes should not be nil")
	}
	if row.LogAttributes["custom_field"] != "custom_value" {
		t.Errorf("LogAttributes[custom_field] = %q, want custom_value", row.LogAttributes["custom_field"])
	}
	if row.LogAttributes["http.method"] != "GET" {
		t.Errorf("LogAttributes[http.method] = %q, want GET", row.LogAttributes["http.method"])
	}
	if row.LogAttributes["request.id"] != "req-123" {
		t.Errorf("LogAttributes[request.id] = %q, want req-123", row.LogAttributes["request.id"])
	}
	if _, exists := row.LogAttributes["_msg"]; exists {
		t.Error("promoted field _msg should not be in LogAttributes")
	}
	if _, exists := row.LogAttributes["service.name"]; exists {
		t.Error("promoted field service.name should not be in LogAttributes")
	}
}

func TestJsonFieldsToLogRow_NonStringValues(t *testing.T) {
	fields := map[string]any{
		"_msg":        "test",
		"status_code": 200,
		"is_error":    false,
		"latency":     1.5,
	}

	row := jsonFieldsToLogRow(fields, promotedLogFields)

	if row.LogAttributes["status_code"] != "200" {
		t.Errorf("status_code = %q, want '200'", row.LogAttributes["status_code"])
	}
	if row.LogAttributes["is_error"] != "false" {
		t.Errorf("is_error = %q, want 'false'", row.LogAttributes["is_error"])
	}
	if row.LogAttributes["latency"] != "1.5" {
		t.Errorf("latency = %q, want '1.5'", row.LogAttributes["latency"])
	}
}

func TestJsonFieldsToLogRow_NoExtraFields(t *testing.T) {
	fields := map[string]any{
		"_msg":  "test",
		"level": "INFO",
	}

	row := jsonFieldsToLogRow(fields, promotedLogFields)

	if len(row.LogAttributes) > 0 {
		t.Errorf("LogAttributes should be nil/empty for all-promoted fields, got %v", row.LogAttributes)
	}
}

func TestApplyStreamLabels_CapturesUnknownLabels(t *testing.T) {
	row := &schema.LogRow{}
	labels := map[string]string{
		"service":    "my-service",
		"app":        "my-app",
		"datacenter": "dc1",
	}

	applyStreamLabels(row, labels)

	if row.ServiceName != "my-service" {
		t.Errorf("ServiceName = %q, want my-service", row.ServiceName)
	}
	if row.ResourceAttributes == nil {
		t.Fatal("ResourceAttributes should not be nil")
	}
	if row.ResourceAttributes["app"] != "my-app" {
		t.Errorf("ResourceAttributes[app] = %q, want my-app", row.ResourceAttributes["app"])
	}
	if row.ResourceAttributes["datacenter"] != "dc1" {
		t.Errorf("ResourceAttributes[datacenter] = %q, want dc1", row.ResourceAttributes["datacenter"])
	}
	if _, exists := row.ResourceAttributes["service"]; exists {
		t.Error("known label 'service' should not be in ResourceAttributes")
	}
}

// --- extra_promoted tests ---

func TestJsonFieldsToLogRow_ExtraPromotedNotInMAP(t *testing.T) {
	extraPromoted := make(map[string]bool, len(promotedLogFields)+2)
	for k, v := range promotedLogFields {
		extraPromoted[k] = v
	}
	extraPromoted["http.status_code"] = true
	extraPromoted["customer_id"] = true

	fields := map[string]any{
		"_msg":             "test",
		"http.status_code": "200",
		"customer_id":      "cust-42",
		"random_field":     "random_value",
	}

	row := jsonFieldsToLogRow(fields, extraPromoted)

	if row.LogAttributes == nil {
		t.Fatal("LogAttributes should not be nil")
	}
	if _, exists := row.LogAttributes["http.status_code"]; exists {
		t.Error("extra promoted field http.status_code should NOT be in LogAttributes")
	}
	if _, exists := row.LogAttributes["customer_id"]; exists {
		t.Error("extra promoted field customer_id should NOT be in LogAttributes")
	}
	if row.LogAttributes["random_field"] != "random_value" {
		t.Errorf("random_field should be in LogAttributes, got %q", row.LogAttributes["random_field"])
	}
}

func TestNewHandler_WithExtraPromoted(t *testing.T) {
	store := &mockStore{}
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.Schema.ExtraPromoted = []config.ExtraPromotedColumn{
		{Name: "http.status_code", Type: "string", Bloom: true},
		{Name: "customer_id", Type: "string", Bloom: true},
	}

	h := NewHandler(store, cfg)

	if !h.promotedFields["http.status_code"] {
		t.Error("http.status_code should be in promotedFields")
	}
	if !h.promotedFields["customer_id"] {
		t.Error("customer_id should be in promotedFields")
	}
	if !h.promotedFields["_msg"] {
		t.Error("default promoted field _msg should still be present")
	}
}

func TestHandleJSONLine_ExtraPromotedExcludedFromMAP(t *testing.T) {
	store := &mockStore{}
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.Schema.ExtraPromoted = []config.ExtraPromotedColumn{
		{Name: "customer_id", Type: "string", Bloom: true},
	}

	h := NewHandler(store, cfg)

	body := `{"_msg":"test","customer_id":"cust-1","random":"val"}`
	req := httptest.NewRequest(http.MethodPost, "/insert/jsonline", strings.NewReader(body))
	rr := httptest.NewRecorder()

	h.handleJSONLine(rr, req)

	if rr.Code != http.StatusNoContent {
		t.Fatalf("status = %d, want %d", rr.Code, http.StatusNoContent)
	}
	if len(store.rows) != 1 {
		t.Fatalf("rows = %d, want 1", len(store.rows))
	}
	row := store.rows[0]
	if _, exists := row.LogAttributes["customer_id"]; exists {
		t.Error("extra promoted customer_id should not be in LogAttributes")
	}
	if row.LogAttributes["random"] != "val" {
		t.Errorf("non-promoted random should be in LogAttributes, got %q", row.LogAttributes["random"])
	}
}
