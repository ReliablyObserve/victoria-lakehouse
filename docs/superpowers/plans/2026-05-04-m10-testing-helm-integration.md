# M10: Testing, Helm & Integration — Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** Enhance E2E testing with VL/proxy/multi-level select, add empirical benchmarks, rewrite Helm chart to Victoria patterns, and fix upstream sync GHA.

**Architecture:** Docker Compose internal networking with VL hot tier + lakehouse cold + vlselect fan-out + loki-vl-proxy. Helm chart uses single ConfigMap YAML blob, per-component StatefulSets, generic HPA/VPA/PDB templates. Benchmark suite measures real Parquet file size × row group × compression combinations.

**Tech Stack:** Go 1.26, parquet-go v0.29.0, Docker Compose, Helm 3, GitHub Actions, MinIO, VictoriaLogs, loki-vl-proxy

---

### Task 1: Docker Compose Rewrite

**Files:**
- Modify: `deployment/docker/docker-compose-e2e.yml`

- [ ] **Step 1: Rewrite docker-compose-e2e.yml**

```yaml
name: victoria-lakehouse

networks:
  lakehouse-net:
    driver: bridge

services:
  minio:
    image: minio/minio:latest
    command: server /data --console-address ":9001"
    networks: [lakehouse-net]
    environment:
      MINIO_ROOT_USER: minioadmin
      MINIO_ROOT_PASSWORD: minioadmin
    healthcheck:
      test: ["CMD", "mc", "ready", "local"]
      interval: 5s
      timeout: 5s
      retries: 5

  minio-init:
    image: minio/mc:latest
    depends_on:
      minio:
        condition: service_healthy
    networks: [lakehouse-net]
    entrypoint: >
      /bin/sh -c "
      mc alias set local http://minio:9000 minioadmin minioadmin &&
      mc mb local/obs-archive --ignore-existing &&
      echo 'MinIO bucket ready'
      "

  victorialogs:
    image: victoriametrics/victoria-logs:v1.20.0-victorialogs
    command:
      - "-storageDataPath=/data"
      - "-retentionPeriod=7d"
      - "-loggerLevel=INFO"
    networks: [lakehouse-net]
    volumes:
      - vl-data:/data
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:9428/health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s

  datagen-seed:
    build:
      context: ../../
      dockerfile: Dockerfile.datagen
    depends_on:
      minio-init:
        condition: service_completed_successfully
      victorialogs:
        condition: service_healthy
    networks: [lakehouse-net]
    command:
      - "--endpoint=http://minio:9000"
      - "--bucket=obs-archive"
      - "--access-key=minioadmin"
      - "--secret-key=minioadmin"
      - "--logs=5000"
      - "--traces=1000"
      - "--hours-back=48"
      - "--dual-write"
      - "--vl-endpoint=http://victorialogs:9428"

  datagen-continuous:
    build:
      context: ../../
      dockerfile: Dockerfile.datagen
    depends_on:
      datagen-seed:
        condition: service_completed_successfully
    restart: unless-stopped
    networks: [lakehouse-net]
    command:
      - "--endpoint=http://minio:9000"
      - "--bucket=obs-archive"
      - "--access-key=minioadmin"
      - "--secret-key=minioadmin"
      - "--logs=500"
      - "--traces=200"
      - "--hours-back=1"
      - "--interval=30s"
      - "--dual-write"
      - "--vl-endpoint=http://victorialogs:9428"

  lakehouse-logs:
    build:
      context: ../../
      dockerfile: Dockerfile
    depends_on:
      datagen-seed:
        condition: service_completed_successfully
    networks: [lakehouse-net]
    command:
      - "-lakehouse.mode=logs"
      - "-lakehouse.s3.bucket=obs-archive"
      - "-lakehouse.s3.endpoint=http://minio:9000"
      - "-lakehouse.s3.access-key=minioadmin"
      - "-lakehouse.s3.secret-key=minioadmin"
      - "-lakehouse.s3.force-path-style=true"
      - "-lakehouse.manifest.refresh-interval=30s"
      - "-loggerLevel=DEBUG"
    volumes:
      - lakehouse-cache-logs:/data/lakehouse
    healthcheck:
      test: ["CMD", "/usr/local/bin/healthcheck", "http://localhost:9428/health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s

  lakehouse-traces:
    build:
      context: ../../
      dockerfile: Dockerfile
    depends_on:
      datagen-seed:
        condition: service_completed_successfully
    networks: [lakehouse-net]
    command:
      - "-lakehouse.mode=traces"
      - "-lakehouse.s3.bucket=obs-archive"
      - "-lakehouse.s3.endpoint=http://minio:9000"
      - "-lakehouse.s3.access-key=minioadmin"
      - "-lakehouse.s3.secret-key=minioadmin"
      - "-lakehouse.s3.force-path-style=true"
      - "-lakehouse.manifest.refresh-interval=30s"
      - "-loggerLevel=DEBUG"
    volumes:
      - lakehouse-cache-traces:/data/lakehouse
    healthcheck:
      test: ["CMD", "/usr/local/bin/healthcheck", "http://localhost:10428/health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 10s

  vlselect:
    image: victoriametrics/victoria-logs:v1.20.0-victorialogs
    command:
      - "-storageNode=victorialogs:9428"
      - "-storageNode=lakehouse-logs:9428"
      - "-httpListenAddr=:9471"
      - "-loggerLevel=INFO"
    depends_on:
      victorialogs:
        condition: service_healthy
      lakehouse-logs:
        condition: service_healthy
    networks: [lakehouse-net]
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:9471/health"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s

  loki-vl-proxy:
    image: ghcr.io/reliablyobserve/loki-vl-proxy:latest
    environment:
      BACKEND_URL: "http://lakehouse-logs:9428"
    depends_on:
      lakehouse-logs:
        condition: service_healthy
    networks: [lakehouse-net]
    healthcheck:
      test: ["CMD", "wget", "-qO-", "http://localhost:3100/ready"]
      interval: 5s
      timeout: 3s
      retries: 10
      start_period: 5s

  grafana:
    image: grafana/grafana:latest
    ports:
      - "3003:3000"
    networks: [lakehouse-net]
    environment:
      GF_AUTH_ANONYMOUS_ENABLED: "true"
      GF_AUTH_ANONYMOUS_ORG_ROLE: Admin
      GF_INSTALL_PLUGINS: "https://github.com/VictoriaMetrics/victorialogs-datasource/releases/download/v0.26.3/victoriametrics-logs-datasource-v0.26.3.zip;victoriametrics-logs-datasource"
      GF_PLUGINS_ALLOW_LOADING_UNSIGNED_PLUGINS: "victoriametrics-logs-datasource"
    volumes:
      - ./grafana/provisioning:/etc/grafana/provisioning
      - grafana-data:/var/lib/grafana
    depends_on:
      lakehouse-logs:
        condition: service_healthy
      lakehouse-traces:
        condition: service_healthy

volumes:
  vl-data: {}
  lakehouse-cache-logs: {}
  lakehouse-cache-traces: {}
  grafana-data: {}
```

- [ ] **Step 2: Verify compose config parses**

Run: `cd deployment/docker && docker compose -f docker-compose-e2e.yml config --quiet`
Expected: No errors

- [ ] **Step 3: Commit**

```bash
git add deployment/docker/docker-compose-e2e.yml
git commit -m "feat(e2e): rewrite compose with VL, vlselect, loki-vl-proxy, internal networking"
```

---

### Task 2: Grafana Datasource Updates

**Files:**
- Modify: `deployment/docker/grafana/provisioning/datasources/datasources.yaml`

- [ ] **Step 1: Add new datasources**

```yaml
apiVersion: 1

datasources:
  - name: Victoria Lakehouse Logs (Cold)
    type: victoriametrics-logs-datasource
    uid: victoria-lakehouse-logs
    access: proxy
    url: http://lakehouse-logs:9428
    isDefault: true
    jsonData:
      maxLines: 1000

  - name: Victoria Lakehouse Traces (Jaeger)
    type: jaeger
    uid: victoria-lakehouse-traces
    access: proxy
    url: http://lakehouse-traces:10428
    jsonData:
      tracesToLogsV2:
        datasourceUid: victoria-lakehouse-logs
        spanStartTimeShift: "-1h"
        spanEndTimeShift: "1h"
        filterByTraceID: true
        filterBySpanID: false

  - name: VictoriaLogs Hot
    type: victoriametrics-logs-datasource
    uid: victorialogs-hot
    access: proxy
    url: http://victorialogs:9428
    jsonData:
      maxLines: 1000

  - name: Multi-Level Select (Hot+Cold)
    type: victoriametrics-logs-datasource
    uid: vlselect-multilevel
    access: proxy
    url: http://vlselect:9471
    jsonData:
      maxLines: 1000

  - name: Loki via Proxy
    type: loki
    uid: loki-vl-proxy
    access: proxy
    url: http://loki-vl-proxy:3100
    jsonData:
      maxLines: 1000
```

- [ ] **Step 2: Commit**

```bash
git add deployment/docker/grafana/provisioning/datasources/datasources.yaml
git commit -m "feat(e2e): add VL hot, vlselect, loki-proxy Grafana datasources"
```

---

### Task 3: Datagen Log Patterns

**Files:**
- Create: `cmd/datagen/patterns.go`

- [ ] **Step 1: Create patterns.go with 5 log generators**

```go
package main

import (
	"fmt"
	"math/rand"
	"strings"
	"time"
)

type LogPattern func(rng *rand.Rand, ts time.Time, svc string) (body string, attrs map[string]string)

var logPatterns = []LogPattern{
	jsonAccessLog,
	logfmtLog,
	nginxCombinedLog,
	javaStackTrace,
	otelLog,
}

func pickPattern(rng *rand.Rand) LogPattern {
	return logPatterns[rng.Intn(len(logPatterns))]
}

func jsonAccessLog(rng *rand.Rand, ts time.Time, svc string) (string, map[string]string) {
	method := httpMethods[rng.Intn(len(httpMethods))]
	path := []string{"/api/v1/users", "/api/v1/orders", "/api/v1/products", "/api/v1/health", "/api/v2/search"}[rng.Intn(5)]
	status := []int{200, 200, 200, 201, 204, 400, 401, 404, 500}[rng.Intn(9)]
	dur := 1 + rng.Intn(500)
	reqID := randomHex(16)

	body := fmt.Sprintf(`{"method":"%s","path":"%s","status":%d,"duration_ms":%d,"request_id":"%s","service":"%s","ts":"%s"}`,
		method, path, status, dur, reqID, svc, ts.Format(time.RFC3339Nano))

	attrs := map[string]string{
		"http.method":      method,
		"http.target":      path,
		"http.status_code": fmt.Sprintf("%d", status),
		"request_id":       reqID,
	}
	return body, attrs
}

func logfmtLog(rng *rand.Rand, ts time.Time, svc string) (string, map[string]string) {
	lvl := levels[rng.Intn(len(levels))]
	components := []string{"http", "grpc", "db", "cache", "queue"}
	component := components[rng.Intn(len(components))]
	msgs := []string{
		"request handled", "connection opened", "query executed",
		"cache miss", "message published", "retry succeeded",
		"timeout exceeded", "circuit breaker tripped",
	}
	msg := msgs[rng.Intn(len(msgs))]
	dur := rng.Float64() * 100

	body := fmt.Sprintf("level=%s msg=%q component=%s duration=%.2fms service=%s ts=%s",
		strings.ToLower(lvl), msg, component, dur, svc, ts.Format(time.RFC3339Nano))

	attrs := map[string]string{
		"component": component,
		"format":    "logfmt",
	}
	return body, attrs
}

func nginxCombinedLog(rng *rand.Rand, ts time.Time, _ string) (string, map[string]string) {
	ips := []string{"10.0.1.42", "10.0.2.17", "192.168.1.100", "172.16.0.55", "10.1.2.33"}
	ip := ips[rng.Intn(len(ips))]
	method := httpMethods[rng.Intn(len(httpMethods))]
	paths := []string{"/", "/index.html", "/api/users", "/static/app.js", "/favicon.ico", "/health"}
	path := paths[rng.Intn(len(paths))]
	status := []int{200, 200, 200, 301, 304, 404, 500}[rng.Intn(7)]
	size := 200 + rng.Intn(50000)
	agents := []string{
		"Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36",
		"curl/7.88.1",
		"Go-http-client/2.0",
		"python-requests/2.31.0",
	}
	agent := agents[rng.Intn(len(agents))]

	body := fmt.Sprintf(`%s - - [%s] "%s %s HTTP/1.1" %d %d "-" "%s"`,
		ip, ts.Format("02/Jan/2006:15:04:05 -0700"), method, path, status, size, agent)

	attrs := map[string]string{
		"format":    "nginx",
		"client_ip": ip,
	}
	return body, attrs
}

func javaStackTrace(rng *rand.Rand, _ time.Time, svc string) (string, map[string]string) {
	exceptions := []struct {
		class string
		msg   string
	}{
		{"java.lang.NullPointerException", "Cannot invoke method on null object"},
		{"java.sql.SQLException", "Connection refused: connect"},
		{"java.util.concurrent.TimeoutException", "Timeout waiting for task"},
		{"io.grpc.StatusRuntimeException", "UNAVAILABLE: upstream connect error"},
		{"com.fasterxml.jackson.core.JsonParseException", "Unexpected character ('}'  (code 125))"},
		{"java.lang.OutOfMemoryError", "Java heap space"},
	}
	exc := exceptions[rng.Intn(len(exceptions))]

	packages := []string{
		"com.reliablyobserve." + svc + ".handler.RequestHandler",
		"com.reliablyobserve." + svc + ".service.ProcessingService",
		"com.reliablyobserve." + svc + ".repository.DataRepository",
		"org.springframework.web.servlet.FrameworkServlet",
		"io.netty.channel.AbstractChannelHandlerContext",
	}

	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("%s: %s\n", exc.class, exc.msg))
	depth := 3 + rng.Intn(8)
	for i := 0; i < depth; i++ {
		pkg := packages[rng.Intn(len(packages))]
		line := 10 + rng.Intn(500)
		sb.WriteString(fmt.Sprintf("\tat %s.process(Unknown Source:%d)\n", pkg, line))
	}
	sb.WriteString(fmt.Sprintf("\t... %d more", 5+rng.Intn(20)))

	attrs := map[string]string{
		"exception.type":    exc.class,
		"exception.message": exc.msg,
		"format":            "java_stacktrace",
	}
	return sb.String(), attrs
}

func otelLog(rng *rand.Rand, ts time.Time, svc string) (string, map[string]string) {
	lvl := levels[rng.Intn(len(levels))]
	msgs := []string{
		"Span started for incoming request",
		"Exporting batch of spans",
		"Metric collection completed",
		"Resource attributes resolved",
		"Baggage propagated to downstream",
		"Sampler decided to record span",
	}
	msg := msgs[rng.Intn(len(msgs))]
	traceID := randomHex(32)
	spanID := randomHex(16)

	body := fmt.Sprintf(`{"timestamp":"%s","severity":"%s","body":"%s","resource":{"service.name":"%s"},"traceId":"%s","spanId":"%s"}`,
		ts.Format(time.RFC3339Nano), lvl, msg, svc, traceID, spanID)

	attrs := map[string]string{
		"format":              "otel",
		"otel.trace_id":      traceID,
		"otel.span_id":       spanID,
		"instrumentation.lib": "opentelemetry-go/1.28.0",
	}
	return body, attrs
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./cmd/datagen/...`
Expected: Success

- [ ] **Step 3: Commit**

```bash
git add cmd/datagen/patterns.go
git commit -m "feat(datagen): add 5 realistic log patterns (json, logfmt, nginx, java, otel)"
```

---

### Task 4: Datagen Dual-Write & Continuous Timestamps

**Files:**
- Modify: `cmd/datagen/main.go`

- [ ] **Step 1: Add dual-write flags and VL push function**

Add these flags after existing flags:

```go
dualWrite := flag.Bool("dual-write", false, "also push logs to VictoriaLogs via /insert/jsonline")
vlEndpoint := flag.String("vl-endpoint", "http://localhost:9428", "VictoriaLogs endpoint for dual-write")
```

Add the pushNDJSON function at the end of main.go:

```go
func pushNDJSON(endpoint string, rows []LogRow) error {
	var buf bytes.Buffer
	for _, r := range rows {
		line := map[string]any{
			"_time":               time.Unix(0, r.TimestampUnixNano).Format(time.RFC3339Nano),
			"_msg":                r.Body,
			"level":               r.SeverityText,
			"service.name":        r.ServiceName,
			"k8s.namespace.name":  r.K8sNamespaceName,
			"k8s.pod.name":        r.K8sPodName,
			"k8s.deployment.name": r.K8sDeploymentName,
			"k8s.node.name":       r.K8sNodeName,
			"deployment.environment": r.DeployEnv,
			"cloud.region":        r.CloudRegion,
			"host.name":           r.HostName,
			"trace_id":            r.TraceID,
			"span_id":             r.SpanID,
		}
		enc, _ := json.Marshal(line)
		buf.Write(enc)
		buf.WriteByte('\n')
	}

	resp, err := http.Post(endpoint+"/insert/jsonline", "application/x-ndjson", &buf)
	if err != nil {
		return fmt.Errorf("push to VL: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("push to VL: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
```

- [ ] **Step 2: Integrate patterns and dual-write into generateBatch**

Replace the log row generation loop in `generateBatch` to use patterns:

```go
// In the log generation loop, replace the body line:
pattern := pickPattern(rng)
body, logAttrs := pattern(rng, ts, svc)

row := LogRow{
    TimestampUnixNano: ts.UnixNano(),
    Body:              body,
    SeverityText:      lvl,
    SeverityNumber:    levelNums[lvl],
    ServiceName:       svc,
    // ... rest unchanged ...
    LogAttributes:     logAttrs,
}
```

After each partition's logs are uploaded to S3, add dual-write:

```go
// After the S3 upload loop for logs, add:
if *dualWrite && *vlEndpoint != "" {
    var allLogs []LogRow
    for _, rows := range logsByPartition {
        allLogs = append(allLogs, rows...)
    }
    if err := pushNDJSON(*vlEndpoint, allLogs); err != nil {
        log.Printf("WARNING: dual-write to VL failed: %v", err)
    } else {
        log.Printf("  dual-write: pushed %d logs to VL at %s", len(allLogs), *vlEndpoint)
    }
}
```

- [ ] **Step 3: Add imports for dual-write**

Add `"encoding/json"`, `"io"`, and `"net/http"` to the import block.

- [ ] **Step 4: Verify it compiles**

Run: `go build ./cmd/datagen/...`
Expected: Success

- [ ] **Step 5: Commit**

```bash
git add cmd/datagen/main.go
git commit -m "feat(datagen): add dual-write to VL and realistic log patterns"
```

---

### Task 5: E2E Test Helper Updates

**Files:**
- Modify: `tests/e2e/helpers_test.go`

- [ ] **Step 1: Add env-var-based URL resolution and new constants**

Replace the hardcoded constants with env-based resolution:

```go
import (
    // add "os" to imports
    "os"
)

func envOrDefault(key, def string) string {
    if v := os.Getenv(key); v != "" {
        return v
    }
    return def
}

var (
    logsBaseURL   = envOrDefault("LOGS_BASE_URL", "http://localhost:19428")
    tracesBaseURL = envOrDefault("TRACES_BASE_URL", "http://localhost:20428")
    lokiProxyURL  = envOrDefault("LOKI_PROXY_URL", "http://localhost:3100")
    vlselectURL   = envOrDefault("VLSELECT_URL", "http://localhost:9471")
)
```

Add an `httpPost` helper:

```go
func httpPost(t *testing.T, baseURL, path string, contentType string, body []byte) *http.Response {
    t.Helper()
    client := &http.Client{Timeout: 60 * time.Second}
    resp, err := client.Post(baseURL+path, contentType, bytes.NewReader(body))
    if err != nil {
        t.Fatalf("POST %s%s failed: %v", baseURL, path, err)
    }
    return resp
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go build ./tests/e2e/...` (or `go vet ./tests/e2e/...` with e2e build tag)
Expected: May need `-tags e2e` to pass

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/helpers_test.go
git commit -m "feat(e2e): add env-based URLs and helpers for proxy/vlselect tests"
```

---

### Task 6: Loki-VL-Proxy E2E Tests

**Files:**
- Create: `tests/e2e/loki_proxy_test.go`

- [ ] **Step 1: Create loki_proxy_test.go**

```go
//go:build e2e

package e2e

import (
	"encoding/json"
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestLokiProxy_QueryRange(t *testing.T) {
	waitForHealth(t, lokiProxyURL, 30*time.Second)

	end := time.Now()
	start := end.Add(-72 * time.Hour)

	params := url.Values{
		"query":     {`{service_name=~".+"}`},
		"start":     {fmt.Sprintf("%d", start.UnixNano())},
		"end":       {fmt.Sprintf("%d", end.UnixNano())},
		"limit":     {"100"},
		"direction": {"backward"},
	}

	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/query_range", params)

	var result struct {
		Status string `json:"status"`
		Data   struct {
			ResultType string `json:"resultType"`
			Result     []struct {
				Stream map[string]string `json:"stream"`
				Values [][]string        `json:"values"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse response: %v\nraw: %s", err, string(body))
	}

	if result.Status != "success" {
		t.Fatalf("expected status=success, got %s", result.Status)
	}
	if len(result.Data.Result) == 0 {
		t.Fatal("expected at least one stream in results")
	}

	totalLines := 0
	for _, stream := range result.Data.Result {
		totalLines += len(stream.Values)
	}
	if totalLines == 0 {
		t.Fatal("expected at least one log line")
	}
	t.Logf("query_range returned %d streams, %d total lines", len(result.Data.Result), totalLines)
}

func TestLokiProxy_Labels(t *testing.T) {
	waitForHealth(t, lokiProxyURL, 30*time.Second)

	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/labels", nil)

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if result.Status != "success" {
		t.Fatalf("expected status=success, got %s", result.Status)
	}
	if len(result.Data) == 0 {
		t.Fatal("expected at least one label")
	}
	t.Logf("labels returned: %v", result.Data)
}

func TestLokiProxy_LabelValues(t *testing.T) {
	waitForHealth(t, lokiProxyURL, 30*time.Second)

	body := httpGetBody(t, lokiProxyURL, "/loki/api/v1/label/service_name/values", nil)

	var result struct {
		Status string   `json:"status"`
		Data   []string `json:"data"`
	}
	if err := json.Unmarshal(body, &result); err != nil {
		t.Fatalf("parse response: %v", err)
	}

	if result.Status != "success" {
		t.Fatalf("expected status=success, got %s", result.Status)
	}
	if len(result.Data) == 0 {
		t.Fatal("expected at least one service name value")
	}
	t.Logf("service_name values: %v", result.Data)
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go vet -tags e2e ./tests/e2e/...`
Expected: Success

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/loki_proxy_test.go
git commit -m "feat(e2e): add loki-vl-proxy integration tests"
```

---

### Task 7: Multi-Level vlselect E2E Tests

**Files:**
- Create: `tests/e2e/vlselect_multilevel_test.go`

- [ ] **Step 1: Create vlselect_multilevel_test.go**

```go
//go:build e2e

package e2e

import (
	"strings"
	"testing"
	"time"
)

func TestVLSelect_QueryReturnsData(t *testing.T) {
	waitForHealth(t, vlselectURL, 30*time.Second)

	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "50")

	body := httpGetBody(t, vlselectURL, "/select/logsql/query", params)

	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		t.Fatal("expected log lines from vlselect, got empty response")
	}
	t.Logf("vlselect returned %d lines", len(lines))
}

func TestVLSelect_FieldNames(t *testing.T) {
	waitForHealth(t, vlselectURL, 30*time.Second)

	params := defaultTimeParams()
	body := httpGetBody(t, vlselectURL, "/select/logsql/field_names", params)

	if len(body) == 0 {
		t.Fatal("expected field names from vlselect")
	}

	bodyStr := string(body)
	if !strings.Contains(bodyStr, "service.name") {
		t.Errorf("expected service.name in field names, got: %s", bodyStr)
	}
	t.Logf("vlselect field_names: %s", bodyStr[:min(len(bodyStr), 200)])
}

func TestVLSelect_ServiceFilter(t *testing.T) {
	waitForHealth(t, vlselectURL, 30*time.Second)

	params := defaultTimeParams()
	params.Set("query", `service.name:"api-gateway"`)
	params.Set("limit", "10")

	body := httpGetBody(t, vlselectURL, "/select/logsql/query", params)

	lines := strings.Split(strings.TrimSpace(string(body)), "\n")
	if len(lines) == 0 || (len(lines) == 1 && lines[0] == "") {
		t.Fatal("expected filtered results for api-gateway")
	}
	for _, line := range lines {
		if !strings.Contains(line, "api-gateway") {
			t.Errorf("line does not contain api-gateway: %s", line[:min(len(line), 100)])
		}
	}
	t.Logf("vlselect service filter returned %d lines", len(lines))
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go vet -tags e2e ./tests/e2e/...`
Expected: Success

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/vlselect_multilevel_test.go
git commit -m "feat(e2e): add multi-level vlselect integration tests"
```

---

### Task 8: Performance Assertion E2E Tests

**Files:**
- Create: `tests/e2e/perf_test.go`

- [ ] **Step 1: Create perf_test.go**

```go
//go:build e2e

package e2e

import (
	"fmt"
	"net/url"
	"testing"
	"time"
)

func TestPerf_ManifestFastPath(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	future := time.Now().Add(365 * 24 * time.Hour)
	params := url.Values{
		"query": {"*"},
		"start": {fmt.Sprintf("%d", future.UnixNano())},
		"end":   {fmt.Sprintf("%d", future.Add(time.Hour).UnixNano())},
	}

	start := time.Now()
	_ = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	elapsed := time.Since(start)

	if elapsed > 50*time.Millisecond {
		t.Errorf("manifest fast path took %s, expected <50ms (target <1ms, allowing network overhead)", elapsed)
	}
	t.Logf("manifest fast path: %s", elapsed)
}

func TestPerf_BloomPointQuery(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	params := defaultTimeParams()
	params.Set("query", `trace_id:="0000000000000001"`)
	params.Set("limit", "1")

	start := time.Now()
	_ = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	elapsed := time.Since(start)

	if elapsed > 500*time.Millisecond {
		t.Errorf("bloom point query took %s, expected <500ms (target <100ms, allowing E2E overhead)", elapsed)
	}
	t.Logf("bloom point query: %s", elapsed)
}

func TestPerf_TimeRangeScan(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	end := time.Now()
	start := end.Add(-1 * time.Hour)
	params := url.Values{
		"query": {"*"},
		"start": {fmt.Sprintf("%d", start.UnixNano())},
		"end":   {fmt.Sprintf("%d", end.UnixNano())},
		"limit": {"100"},
	}

	t0 := time.Now()
	_ = httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	elapsed := time.Since(t0)

	if elapsed > 2*time.Second {
		t.Errorf("time range scan took %s, expected <2s (target <500ms, allowing E2E overhead)", elapsed)
	}
	t.Logf("time range scan (1h): %s", elapsed)
}

func TestPerf_FieldNames(t *testing.T) {
	waitForHealth(t, logsBaseURL, 30*time.Second)

	params := defaultTimeParams()

	start := time.Now()
	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_names", params)
	elapsed := time.Since(start)

	if elapsed > 200*time.Millisecond {
		t.Errorf("field_names took %s, expected <200ms (target <1ms, allowing E2E overhead)", elapsed)
	}
	if len(body) == 0 {
		t.Error("field_names returned empty response")
	}
	t.Logf("field_names: %s (response: %d bytes)", elapsed, len(body))
}
```

- [ ] **Step 2: Verify it compiles**

Run: `go vet -tags e2e ./tests/e2e/...`
Expected: Success

- [ ] **Step 3: Commit**

```bash
git add tests/e2e/perf_test.go
git commit -m "feat(e2e): add performance assertion tests"
```

---

### Task 9: Benchmark Suite

**Files:**
- Create: `cmd/loadtest/benchmark.go`
- Modify: `cmd/loadtest/main.go`
- Modify: `cmd/loadtest/report.go`

- [ ] **Step 1: Add BenchmarkResult to report.go**

Add the type and update Report:

```go
type BenchmarkResult struct {
	FileSize       string             `json:"file_size"`
	RowGroupSize   int                `json:"row_group_size"`
	CompressionLvl int                `json:"compression_level"`
	WriteTimeMs    float64            `json:"write_time_ms"`
	ReadTimeMs     float64            `json:"read_time_ms"`
	FileSizeBytes  int64              `json:"file_size_bytes"`
	RawSizeBytes   int64              `json:"raw_size_bytes"`
	Ratio          float64            `json:"compression_ratio"`
	RowCount       int                `json:"row_count"`
	ColumnBreakdown map[string]int64  `json:"column_breakdown,omitempty"`
}
```

Add `Benchmarks []BenchmarkResult` field to Report struct and update PrintSummary to include benchmark output.

- [ ] **Step 2: Create benchmark.go**

```go
package main

import (
	"bytes"
	"context"
	"fmt"
	"log"
	"math/rand"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"
)

type BenchmarkConfig struct {
	Endpoint  string
	Bucket    string
	AccessKey string
	SecretKey string
}

func runBenchmarks(cfg BenchmarkConfig) []BenchmarkResult {
	var results []BenchmarkResult

	fileSizes := []struct {
		name string
		rows int
	}{
		{"1MB", 500},
		{"5MB", 2500},
		{"10MB", 5000},
		{"50MB", 25000},
		{"100MB", 50000},
		{"500MB", 250000},
	}

	rowGroupSizes := []int{1000, 5000, 10000, 50000, 100000}
	compressionLevels := []int{1, 3, 5, 9, 15, 19}

	for _, fs := range fileSizes {
		for _, rgs := range rowGroupSizes {
			if rgs > fs.rows {
				continue
			}
			for _, cl := range compressionLevels {
				result := benchmarkSingle(cfg, fs.name, fs.rows, rgs, cl)
				results = append(results, result)
				log.Printf("  %s rg=%d zstd=%d → %d bytes (ratio %.2f) write=%dms read=%dms",
					fs.name, rgs, cl, result.FileSizeBytes, result.Ratio,
					int(result.WriteTimeMs), int(result.ReadTimeMs))
			}
		}
	}

	return results
}

func benchmarkSingle(cfg BenchmarkConfig, sizeName string, rowCount, rowGroupSize, compressionLevel int) BenchmarkResult {
	rng := rand.New(rand.NewSource(time.Now().UnixNano()))
	rows := generateLogRows(rng, rowCount)

	rawSize := estimateRawSize(rows)

	writeStart := time.Now()
	data, err := writeParquetBenchmark(rows, rowGroupSize, compressionLevel)
	writeTime := time.Since(writeStart)
	if err != nil {
		log.Printf("ERROR writing parquet: %v", err)
		return BenchmarkResult{FileSize: sizeName, RowGroupSize: rowGroupSize, CompressionLvl: compressionLevel}
	}

	readStart := time.Now()
	_ = readParquetBenchmark(data)
	readTime := time.Since(readStart)

	var uploadTime time.Duration
	if cfg.Endpoint != "" {
		uploadStart := time.Now()
		if err := uploadBenchmark(cfg, sizeName, rowGroupSize, compressionLevel, data); err != nil {
			log.Printf("WARNING: upload failed: %v", err)
		}
		uploadTime = time.Since(uploadStart)
		_ = uploadTime
	}

	return BenchmarkResult{
		FileSize:       sizeName,
		RowGroupSize:   rowGroupSize,
		CompressionLvl: compressionLevel,
		WriteTimeMs:    float64(writeTime.Microseconds()) / 1000.0,
		ReadTimeMs:     float64(readTime.Microseconds()) / 1000.0,
		FileSizeBytes:  int64(len(data)),
		RawSizeBytes:   rawSize,
		Ratio:          float64(rawSize) / float64(len(data)),
		RowCount:       rowCount,
	}
}

func generateLogRows(rng *rand.Rand, count int) []schema.LogRow {
	now := time.Now()
	rows := make([]schema.LogRow, count)
	services := []string{"api-gateway", "user-service", "order-service", "payment-service", "notification-service"}
	namespaces := []string{"production", "staging"}
	levels := []string{"INFO", "WARN", "ERROR", "DEBUG"}
	levelNums := map[string]int32{"DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17}

	for i := range rows {
		ts := now.Add(-time.Duration(rng.Intn(48*3600)) * time.Second)
		svc := services[rng.Intn(len(services))]
		lvl := levels[rng.Intn(len(levels))]
		pattern := logPatterns[rng.Intn(len(logPatterns))]
		body, attrs := pattern(rng, ts, svc)

		rows[i] = schema.LogRow{
			TimestampUnixNano: ts.UnixNano(),
			Body:              body,
			SeverityText:      lvl,
			SeverityNumber:    levelNums[lvl],
			ServiceName:       svc,
			K8sNamespaceName:  namespaces[rng.Intn(len(namespaces))],
			K8sPodName:        fmt.Sprintf("%s-%x", svc, rng.Int31()),
			K8sDeploymentName: svc,
			K8sNodeName:       fmt.Sprintf("node-pool-%c-%d", 'a'+rng.Intn(2), 1+rng.Intn(4)),
			DeployEnv:         "production",
			CloudRegion:       "us-east-1",
			HostName:          fmt.Sprintf("ip-10-0-%d-%d", rng.Intn(4), rng.Intn(256)),
			TraceID:           fmt.Sprintf("%032x", rng.Int63()),
			SpanID:            fmt.Sprintf("%016x", rng.Int63()),
			Stream:            fmt.Sprintf(`{service.name=%q}`, svc),
			StreamID:          fmt.Sprintf("%016x", rng.Int63()),
			ScopeName:         "benchmark",
			LogAttributes:     attrs,
		}
	}
	return rows
}

func estimateRawSize(rows []schema.LogRow) int64 {
	var total int64
	for _, r := range rows {
		total += 8 + int64(len(r.Body)) + int64(len(r.SeverityText)) + 4
		total += int64(len(r.ServiceName) + len(r.K8sNamespaceName) + len(r.K8sPodName))
		total += int64(len(r.K8sDeploymentName) + len(r.K8sNodeName) + len(r.DeployEnv))
		total += int64(len(r.CloudRegion) + len(r.HostName) + len(r.TraceID) + len(r.SpanID))
		total += int64(len(r.Stream) + len(r.StreamID) + len(r.ScopeName))
		for k, v := range r.LogAttributes {
			total += int64(len(k) + len(v))
		}
	}
	return total
}

func writeParquetBenchmark(rows []schema.LogRow, rowGroupSize, compressionLevel int) ([]byte, error) {
	var buf bytes.Buffer
	codec := &zstd.Codec{Level: zstdLevelFromInt(compressionLevel)}
	writer := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.Compression(codec),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
	)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func zstdLevelFromInt(level int) zstd.Level {
	switch {
	case level <= 1:
		return zstd.SpeedFastest
	case level <= 5:
		return zstd.SpeedDefault
	case level <= 10:
		return zstd.SpeedBetterCompression
	default:
		return zstd.SpeedBestCompression
	}
}

func readParquetBenchmark(data []byte) []schema.LogRow {
	reader := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
	defer func() { _ = reader.Close() }()
	n := int(reader.NumRows())
	rows := make([]schema.LogRow, n)
	total, _ := reader.Read(rows)
	return rows[:total]
}

func uploadBenchmark(cfg BenchmarkConfig, sizeName string, rgs, cl int, data []byte) error {
	ctx := context.Background()
	awsCfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(cfg.AccessKey, cfg.SecretKey, "")),
	)
	if err != nil {
		return err
	}
	client := s3.NewFromConfig(awsCfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(cfg.Endpoint)
		o.UsePathStyle = true
	})
	key := fmt.Sprintf("benchmarks/%s-rg%d-zstd%d.parquet", sizeName, rgs, cl)
	_, err = client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: aws.String(cfg.Bucket),
		Key:    aws.String(key),
		Body:   bytes.NewReader(data),
	})
	return err
}
```

- [ ] **Step 3: Add "benchmark" mode to main.go**

Add a case in the switch statement:

```go
case "benchmark":
    bcfg := BenchmarkConfig{
        Endpoint:  *target,
        Bucket:    "obs-archive",
        AccessKey: "minioadmin",
        SecretKey: "minioadmin",
    }
    report.Benchmarks = runBenchmarks(bcfg)
```

- [ ] **Step 4: Verify it compiles**

Run: `go build ./cmd/loadtest/...`
Expected: Success

- [ ] **Step 5: Commit**

```bash
git add cmd/loadtest/benchmark.go cmd/loadtest/main.go cmd/loadtest/report.go
git commit -m "feat(loadtest): add benchmark mode for file size × row group × compression matrix"
```

---

### Task 10: Helm Chart — values.yaml Rewrite

**Files:**
- Modify: `charts/victoria-lakehouse/values.yaml`

- [ ] **Step 1: Rewrite values.yaml with common section and lakehouseConfig blob**

The new structure introduces:
- `common` section that deep-merges into each component
- `lakehouseConfig` section that becomes the single ConfigMap YAML blob
- `verticalPodAutoscaler` per component
- `extraManifests` at top level

Key changes from existing values.yaml:
- Move all `s3.*`, `cache.*`, `insert.*`, `selectConfig.*`, `discovery.*`, `peer.*`, `manifest.*`, `query.*`, `startup.*`, `schema.*`, `compaction.*` into `lakehouseConfig`
- Add `common` section with shared k8s options
- Add `verticalPodAutoscaler` to both select and insertComponent
- Add `extraManifests` at root
- Change vmauth config storage from ConfigMap to Secret

```yaml
# Full rewrite — see spec for complete structure
nameOverride: ""
fullnameOverride: ""

global:
  imagePullSecrets: []
  commonLabels: {}
  commonAnnotations: {}

image:
  repository: ghcr.io/reliablyobserve/victoria-lakehouse
  tag: ""
  pullPolicy: IfNotPresent

# Common settings deep-merged into each component
common:
  nodeSelector: {}
  tolerations: []
  affinity: {}
  resources: {}
  podSecurityContext:
    runAsNonRoot: true
    runAsUser: 65534
    runAsGroup: 65534
    fsGroup: 65534
    seccompProfile:
      type: RuntimeDefault
  securityContext:
    readOnlyRootFilesystem: true
    allowPrivilegeEscalation: false
    capabilities:
      drop: ["ALL"]
    runAsNonRoot: true
    runAsUser: 65534
    runAsGroup: 65534
    seccompProfile:
      type: RuntimeDefault

# Single YAML blob mounted as ConfigMap at /etc/lakehouse/config.yaml
lakehouseConfig:
  mode: logs
  role: all
  s3:
    bucket: ""
    region: us-east-1
    prefix: ""
    endpoint: ""
    access_key: ""
    secret_key: ""
    force_path_style: false
    max_connections: 128
    timeout: 30s
  cache:
    memory_limit: 512MB
    eviction_watermark: 0.8
  insert:
    flush_interval: 10s
    max_buffer_rows: 50000
    max_buffer_bytes: 256MB
    row_group_size: 10000
    target_file_size: 128MB
    bloom_columns: "service.name,trace_id"
    compression_level: default
    wal:
      enabled: true
      dir: /data/lakehouse/wal
      max_bytes: 512MB
  select:
    buffer_query_enabled: true
    insert_headless_service: ""
    buffer_query_timeout: 2s
  discovery:
    headless_service: ""
    storage_nodes: []
    partition_auth_key: ""
    peer_headless_service: ""
    refresh_interval: 5m
    timeout: 10s
  peer:
    auth_key: ""
    timeout: 5s
    max_connections: 32
  hot_boundary: ""
  manifest:
    refresh_interval: 5m
    persist_path: /data/lakehouse
    sqs_queue_url: ""
  query:
    max_concurrent: 32
    timeout: 60s
    slow_threshold: 5s
  startup:
    serve_stale: false
    warmup_window: 24h
    max_warmup_time: 5m
  schema:
    extra_promoted: []
  compaction:
    enabled: false
    interval: 5m
    max_concurrent: 1
    min_files_l0: 10
    min_files_l1: 10
    min_age: 1h
    leader_election: auto

select:
  enabled: true
  replicaCount: 2
  image: {}
  extraArgs: {}
  extraEnv: []
  extraEnvFrom: []
  extraVolumes: []
  extraVolumeMounts: []
  extraContainers: []
  initContainers: []
  resources: {}
  podSecurityContext: {}
  securityContext: {}
  service:
    type: ClusterIP
    port: ""
    annotations: {}
    labels: {}
  headlessService:
    enabled: true
    annotations: {}
  nodeSelector: {}
  tolerations: []
  affinity: {}
  podDisruptionBudget:
    enabled: false
    minAvailable: 1
  serviceAccount:
    create: true
    name: ""
    annotations: {}
  horizontalPodAutoscaler:
    enabled: false
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 80
  verticalPodAutoscaler:
    enabled: false
    updateMode: "Off"
  ingress:
    enabled: false
    className: ""
    annotations: {}
    hosts: []
    tls: []
  serviceMonitor:
    enabled: false
    interval: 30s
    labels: {}
  persistence:
    enabled: true
    size: 50Gi
    storageClass: ""
    accessModes:
      - ReadWriteOnce
  probe:
    liveness:
      initialDelaySeconds: 5
      periodSeconds: 10
      failureThreshold: 3
    readiness:
      initialDelaySeconds: 2
      periodSeconds: 5
      failureThreshold: 60
    startup:
      periodSeconds: 5
      failureThreshold: 120
  podAnnotations: {}
  podLabels: {}
  terminationGracePeriodSeconds: 60
  priorityClassName: ""
  topologySpreadConstraints: []

insertComponent:
  enabled: true
  replicaCount: 2
  image: {}
  extraArgs: {}
  extraEnv: []
  extraEnvFrom: []
  extraVolumes: []
  extraVolumeMounts: []
  extraContainers: []
  initContainers: []
  resources: {}
  podSecurityContext: {}
  securityContext: {}
  service:
    type: ClusterIP
    port: ""
    annotations: {}
    labels: {}
  headlessService:
    enabled: true
    annotations: {}
  nodeSelector: {}
  tolerations: []
  affinity: {}
  podDisruptionBudget:
    enabled: false
    minAvailable: 1
  serviceAccount:
    create: true
    name: ""
    annotations: {}
  horizontalPodAutoscaler:
    enabled: false
    minReplicas: 2
    maxReplicas: 10
    targetCPUUtilizationPercentage: 80
  verticalPodAutoscaler:
    enabled: false
    updateMode: "Off"
  ingress:
    enabled: false
    className: ""
    annotations: {}
    hosts: []
    tls: []
  serviceMonitor:
    enabled: false
    interval: 30s
    labels: {}
  persistence:
    enabled: true
    size: 50Gi
    storageClass: ""
    accessModes:
      - ReadWriteOnce
  probe:
    liveness:
      initialDelaySeconds: 5
      periodSeconds: 10
      failureThreshold: 3
    readiness:
      initialDelaySeconds: 2
      periodSeconds: 5
      failureThreshold: 60
    startup:
      periodSeconds: 5
      failureThreshold: 120
  podAnnotations: {}
  podLabels: {}
  terminationGracePeriodSeconds: 60
  priorityClassName: ""
  topologySpreadConstraints: []

vmauth:
  enabled: false
  image:
    repository: victoriametrics/vmauth
    tag: v1.106.1
    pullPolicy: IfNotPresent
  replicaCount: 1
  resources: {}
  service:
    type: ClusterIP
    port: 8427
    annotations: {}
    labels: {}
  config: ""
  extraArgs: {}
  podSecurityContext: {}
  securityContext: {}
  nodeSelector: {}
  tolerations: []
  affinity: {}
  serviceMonitor:
    enabled: false
    interval: 30s
    labels: {}
  ingress:
    enabled: false
    className: ""
    annotations: {}
    hosts: []
    tls: []
  podAnnotations: {}
  podLabels: {}
  terminationGracePeriodSeconds: 30

extraManifests: []
```

- [ ] **Step 2: Commit**

```bash
git add charts/victoria-lakehouse/values.yaml
git commit -m "feat(helm): rewrite values.yaml with common section, lakehouseConfig blob, VPA support"
```

---

### Task 11: Helm Chart — _helpers.tpl and ConfigMap

**Files:**
- Modify: `charts/victoria-lakehouse/templates/_helpers.tpl`
- Modify: `charts/victoria-lakehouse/templates/configmap.yaml` (create if needed)

- [ ] **Step 1: Rewrite _helpers.tpl**

Key helpers needed:
- `victoria-lakehouse.name` / `victoria-lakehouse.fullname` — standard naming
- `victoria-lakehouse.labels` / `victoria-lakehouse.selectorLabels` — standard labels
- `victoria-lakehouse.image` — resolve image with tag fallback to appVersion
- `victoria-lakehouse.componentLabels` — labels with component name
- `victoria-lakehouse.servicePort` — resolve port from mode (9428 for logs, 10428 for traces)
- `victoria-lakehouse.mergeCommon` — deep-merge common values into component

The `_helpers.tpl` should follow Victoria's patterns with component-aware label generation.

- [ ] **Step 2: Create configmap.yaml**

```yaml
apiVersion: v1
kind: ConfigMap
metadata:
  name: {{ include "victoria-lakehouse.fullname" . }}-config
  labels:
    {{- include "victoria-lakehouse.labels" . | nindent 4 }}
data:
  config.yaml: |
    lakehouse:
      {{- .Values.lakehouseConfig | toYaml | nindent 6 }}
```

- [ ] **Step 3: Verify with helm template**

Run: `helm template test charts/victoria-lakehouse/ --set lakehouseConfig.s3.bucket=test-bucket`
Expected: ConfigMap renders with lakehouse config YAML

- [ ] **Step 4: Commit**

```bash
git add charts/victoria-lakehouse/templates/_helpers.tpl charts/victoria-lakehouse/templates/configmap.yaml
git commit -m "feat(helm): add helpers and ConfigMap for single YAML config blob"
```

---

### Task 12: Helm Chart — StatefulSets and Services

**Files:**
- Modify: `charts/victoria-lakehouse/templates/select-statefulset.yaml`
- Modify: `charts/victoria-lakehouse/templates/insert-statefulset.yaml`
- Modify: `charts/victoria-lakehouse/templates/select-service.yaml`
- Modify: `charts/victoria-lakehouse/templates/insert-service.yaml`
- Create: `charts/victoria-lakehouse/templates/select-headless-service.yaml`
- Create: `charts/victoria-lakehouse/templates/insert-headless-service.yaml`

- [ ] **Step 1: Rewrite select-statefulset.yaml**

Key changes:
- Mount ConfigMap at `/etc/lakehouse/config.yaml`
- Single arg: `--lakehouse.config=/etc/lakehouse/config.yaml`
- Role override: `--lakehouse.role=select`
- Deep-merge common values (podSecurityContext, securityContext, nodeSelector, tolerations, affinity)
- Health probes use mode-aware port

- [ ] **Step 2: Rewrite insert-statefulset.yaml**

Same pattern as select but with `--lakehouse.role=insert`.

- [ ] **Step 3: Rewrite select-service.yaml and insert-service.yaml**

Standard ClusterIP services with mode-aware port resolution.

- [ ] **Step 4: Create headless services**

`select-headless-service.yaml`:
```yaml
{{- if and .Values.select.enabled .Values.select.headlessService.enabled }}
apiVersion: v1
kind: Service
metadata:
  name: {{ include "victoria-lakehouse.fullname" . }}-select-headless
  labels:
    {{- include "victoria-lakehouse.componentLabels" (dict "root" . "component" "select") | nindent 4 }}
  {{- with .Values.select.headlessService.annotations }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
spec:
  type: ClusterIP
  clusterIP: None
  ports:
    - name: http
      port: {{ include "victoria-lakehouse.servicePort" . }}
      targetPort: http
  selector:
    {{- include "victoria-lakehouse.selectorLabels" . | nindent 4 }}
    app.kubernetes.io/component: select
{{- end }}
```

Same pattern for `insert-headless-service.yaml`.

- [ ] **Step 5: Verify with helm template**

Run: `helm template test charts/victoria-lakehouse/ --set lakehouseConfig.s3.bucket=test`
Expected: Both StatefulSets, 4 services render correctly

- [ ] **Step 6: Commit**

```bash
git add charts/victoria-lakehouse/templates/select-statefulset.yaml \
  charts/victoria-lakehouse/templates/insert-statefulset.yaml \
  charts/victoria-lakehouse/templates/select-service.yaml \
  charts/victoria-lakehouse/templates/insert-service.yaml \
  charts/victoria-lakehouse/templates/select-headless-service.yaml \
  charts/victoria-lakehouse/templates/insert-headless-service.yaml
git commit -m "feat(helm): rewrite StatefulSets and services with ConfigMap mount, headless services"
```

---

### Task 13: Helm Chart — vmauth

**Files:**
- Modify: `charts/victoria-lakehouse/templates/vmauth-deployment.yaml`
- Modify: `charts/victoria-lakehouse/templates/vmauth-service.yaml`
- Rename: `charts/victoria-lakehouse/templates/vmauth-configmap.yaml` → `charts/victoria-lakehouse/templates/vmauth-secret.yaml`

- [ ] **Step 1: Create vmauth-secret.yaml (replaces vmauth-configmap.yaml)**

```yaml
{{- if .Values.vmauth.enabled }}
apiVersion: v1
kind: Secret
metadata:
  name: {{ include "victoria-lakehouse.fullname" . }}-vmauth
  labels:
    {{- include "victoria-lakehouse.componentLabels" (dict "root" . "component" "vmauth") | nindent 4 }}
type: Opaque
stringData:
  auth.yml: |
    {{- if .Values.vmauth.config }}
    {{ .Values.vmauth.config | nindent 4 }}
    {{- else }}
    unauthorized_user:
      url_map:
        - src_paths:
            - "/insert/.*"
            - "/internal/insert"
          url_prefix:
            - "http://{{ include "victoria-lakehouse.fullname" . }}-insert:{{ include "victoria-lakehouse.servicePort" . }}/"
          discover_backend_ips: true
        - src_paths:
            - "/.*"
          url_prefix:
            - "http://{{ include "victoria-lakehouse.fullname" . }}-select:{{ include "victoria-lakehouse.servicePort" . }}/"
          discover_backend_ips: true
    {{- end }}
{{- end }}
```

- [ ] **Step 2: Update vmauth-deployment.yaml**

Mount the Secret instead of ConfigMap. Use `--auth.config=/etc/vmauth/auth.yml`.

- [ ] **Step 3: Delete old vmauth-configmap.yaml**

Run: `rm charts/victoria-lakehouse/templates/vmauth-configmap.yaml`

- [ ] **Step 4: Verify with helm template**

Run: `helm template test charts/victoria-lakehouse/ --set vmauth.enabled=true --set lakehouseConfig.s3.bucket=test`
Expected: vmauth Secret, Deployment, Service render correctly

- [ ] **Step 5: Commit**

```bash
git add charts/victoria-lakehouse/templates/vmauth-secret.yaml \
  charts/victoria-lakehouse/templates/vmauth-deployment.yaml \
  charts/victoria-lakehouse/templates/vmauth-service.yaml
git rm charts/victoria-lakehouse/templates/vmauth-configmap.yaml
git commit -m "feat(helm): vmauth uses Secret for routing config, auto-generated routes"
```

---

### Task 14: Helm Chart — Generic HPA/VPA/PDB/ServiceMonitor/Ingress

**Files:**
- Modify: `charts/victoria-lakehouse/templates/hpa.yaml`
- Create: `charts/victoria-lakehouse/templates/vpa.yaml`
- Modify: `charts/victoria-lakehouse/templates/pdb.yaml`
- Modify: `charts/victoria-lakehouse/templates/servicemonitor.yaml`
- Modify: `charts/victoria-lakehouse/templates/ingress.yaml`

- [ ] **Step 1: Rewrite hpa.yaml as generic iterator**

```yaml
{{- range $name, $spec := dict "select" $.Values.select "insert" $.Values.insertComponent }}
{{- if and $spec.enabled (dig "horizontalPodAutoscaler" "enabled" false $spec) }}
---
apiVersion: autoscaling/v2
kind: HorizontalPodAutoscaler
metadata:
  name: {{ include "victoria-lakehouse.fullname" $ }}-{{ $name }}
  labels:
    {{- include "victoria-lakehouse.componentLabels" (dict "root" $ "component" $name) | nindent 4 }}
spec:
  scaleTargetRef:
    apiVersion: apps/v1
    kind: StatefulSet
    name: {{ include "victoria-lakehouse.fullname" $ }}-{{ $name }}
  minReplicas: {{ $spec.horizontalPodAutoscaler.minReplicas }}
  maxReplicas: {{ $spec.horizontalPodAutoscaler.maxReplicas }}
  metrics:
    - type: Resource
      resource:
        name: cpu
        target:
          type: Utilization
          averageUtilization: {{ $spec.horizontalPodAutoscaler.targetCPUUtilizationPercentage }}
{{- end }}
{{- end }}
```

- [ ] **Step 2: Create vpa.yaml**

Same pattern as HPA but for VerticalPodAutoscaler.

- [ ] **Step 3: Rewrite pdb.yaml, servicemonitor.yaml, ingress.yaml as generic iterators**

All follow the same pattern: iterate over `dict "select" $.Values.select "insert" $.Values.insertComponent`, check `enabled` and sub-feature `enabled`.

- [ ] **Step 4: Verify with helm template**

Run: `helm template test charts/victoria-lakehouse/ --set select.horizontalPodAutoscaler.enabled=true --set lakehouseConfig.s3.bucket=test`
Expected: HPA renders for select

- [ ] **Step 5: Commit**

```bash
git add charts/victoria-lakehouse/templates/hpa.yaml \
  charts/victoria-lakehouse/templates/vpa.yaml \
  charts/victoria-lakehouse/templates/pdb.yaml \
  charts/victoria-lakehouse/templates/servicemonitor.yaml \
  charts/victoria-lakehouse/templates/ingress.yaml
git commit -m "feat(helm): generic HPA/VPA/PDB/ServiceMonitor/Ingress iterating over components"
```

---

### Task 15: Helm Chart — extraManifests, ServiceAccount, Chart.yaml

**Files:**
- Create: `charts/victoria-lakehouse/templates/extra-manifests.yaml`
- Modify: `charts/victoria-lakehouse/templates/serviceaccount.yaml`
- Modify: `charts/victoria-lakehouse/Chart.yaml`

- [ ] **Step 1: Create extra-manifests.yaml**

```yaml
{{- range .Values.extraManifests }}
---
{{ toYaml . }}
{{- end }}
```

- [ ] **Step 2: Update serviceaccount.yaml to iterate over components**

```yaml
{{- range $name, $spec := dict "select" $.Values.select "insert" $.Values.insertComponent }}
{{- if and $spec.enabled (dig "serviceAccount" "create" true $spec) }}
---
apiVersion: v1
kind: ServiceAccount
metadata:
  name: {{ default (printf "%s-%s" (include "victoria-lakehouse.fullname" $) $name) (dig "serviceAccount" "name" "" $spec) }}
  labels:
    {{- include "victoria-lakehouse.componentLabels" (dict "root" $ "component" $name) | nindent 4 }}
  {{- with (dig "serviceAccount" "annotations" dict $spec) }}
  annotations:
    {{- toYaml . | nindent 4 }}
  {{- end }}
{{- end }}
{{- end }}
```

- [ ] **Step 3: Bump Chart.yaml to 0.10.0**

```yaml
version: 0.10.0
appVersion: "0.10.0"
```

- [ ] **Step 4: Remove compaction-rbac.yaml (moved to lakehouseConfig)**

Run: `rm charts/victoria-lakehouse/templates/compaction-rbac.yaml`

- [ ] **Step 5: Verify full chart renders**

Run: `helm template test charts/victoria-lakehouse/ --set lakehouseConfig.s3.bucket=test --set vmauth.enabled=true`
Expected: All templates render without errors

- [ ] **Step 6: Commit**

```bash
git add charts/victoria-lakehouse/templates/extra-manifests.yaml \
  charts/victoria-lakehouse/templates/serviceaccount.yaml \
  charts/victoria-lakehouse/Chart.yaml
git rm charts/victoria-lakehouse/templates/compaction-rbac.yaml 2>/dev/null || true
git commit -m "feat(helm): add extraManifests, per-component ServiceAccounts, bump to v0.10.0"
```

---

### Task 16: Upstream Sync GHA Rewrite

**Files:**
- Modify: `.github/workflows/upstream-check.yaml`
- Create: `.upstream-versions.json`

- [ ] **Step 1: Create .upstream-versions.json**

```json
{
  "victorialogs": "v1.20.0-victorialogs",
  "victoriatraces": "v1.5.0-victoriatraces"
}
```

- [ ] **Step 2: Rewrite upstream-check.yaml**

```yaml
name: Check Upstream Releases

on:
  schedule:
    - cron: '0 8 * * *'
  workflow_dispatch: {}

permissions:
  contents: write
  pull-requests: write

jobs:
  check-upstream:
    runs-on: ubuntu-latest
    steps:
      - uses: actions/checkout@v6

      - name: Check VictoriaLogs releases
        id: vl
        run: |
          LATEST=$(gh api repos/VictoriaMetrics/VictoriaLogs/releases/latest --jq .tag_name 2>/dev/null || echo "unknown")
          CURRENT=$(jq -r '.victorialogs' .upstream-versions.json 2>/dev/null || echo "none")
          echo "latest=$LATEST" >> $GITHUB_OUTPUT
          echo "current=$CURRENT" >> $GITHUB_OUTPUT
          if [ "$LATEST" != "$CURRENT" ] && [ "$LATEST" != "unknown" ] && [ "$CURRENT" != "none" ]; then
            echo "outdated=true" >> $GITHUB_OUTPUT
          else
            echo "outdated=false" >> $GITHUB_OUTPUT
          fi
          echo "VL: current=$CURRENT latest=$LATEST"
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - name: Check VictoriaTraces releases
        id: vt
        run: |
          LATEST=$(gh api repos/VictoriaMetrics/VictoriaTraces/releases/latest --jq .tag_name 2>/dev/null || echo "unknown")
          CURRENT=$(jq -r '.victoriatraces' .upstream-versions.json 2>/dev/null || echo "none")
          echo "latest=$LATEST" >> $GITHUB_OUTPUT
          echo "current=$CURRENT" >> $GITHUB_OUTPUT
          if [ "$LATEST" != "$CURRENT" ] && [ "$LATEST" != "unknown" ] && [ "$CURRENT" != "none" ]; then
            echo "outdated=true" >> $GITHUB_OUTPUT
          else
            echo "outdated=false" >> $GITHUB_OUTPUT
          fi
          echo "VT: current=$CURRENT latest=$LATEST"
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}

      - name: Create sync PR
        if: steps.vl.outputs.outdated == 'true' || steps.vt.outputs.outdated == 'true'
        run: |
          BRANCH="upstream-sync/$(date +%Y%m%d)"
          git checkout -b "$BRANCH"

          # Update version tracking file
          jq --arg vl "${{ steps.vl.outputs.latest }}" \
             --arg vt "${{ steps.vt.outputs.latest }}" \
             '.victorialogs = $vl | .victoriatraces = $vt' \
             .upstream-versions.json > .upstream-versions.json.tmp
          mv .upstream-versions.json.tmp .upstream-versions.json

          # Update docker-compose image tags
          if [ "${{ steps.vl.outputs.outdated }}" = "true" ]; then
            sed -i "s|victoriametrics/victoria-logs:[^ ]*|victoriametrics/victoria-logs:${{ steps.vl.outputs.latest }}|g" \
              deployment/docker/docker-compose-e2e.yml
          fi

          git add .upstream-versions.json deployment/docker/docker-compose-e2e.yml
          git commit -m "deps: upstream sync VL=${{ steps.vl.outputs.latest }} VT=${{ steps.vt.outputs.latest }}"
          git push -u origin "$BRANCH"

          # Build PR body
          BODY="## Upstream Release Update

          | Component | Previous | Latest |
          |---|---|---|
          | VictoriaLogs | ${{ steps.vl.outputs.current }} | ${{ steps.vl.outputs.latest }} |
          | VictoriaTraces | ${{ steps.vt.outputs.current }} | ${{ steps.vt.outputs.latest }} |

          ### Changelog
          - [VL releases](https://github.com/VictoriaMetrics/VictoriaLogs/releases)
          - [VT releases](https://github.com/VictoriaMetrics/VictoriaTraces/releases)

          ### Review Checklist
          - [ ] E2E tests pass with new images
          - [ ] No breaking API changes
          - [ ] Performance regression check"

          gh pr create \
            --title "Upstream sync: VL ${{ steps.vl.outputs.latest }}, VT ${{ steps.vt.outputs.latest }}" \
            --label "dependencies" \
            --body "$BODY"
        env:
          GH_TOKEN: ${{ secrets.GITHUB_TOKEN }}
```

- [ ] **Step 3: Commit**

```bash
git add .github/workflows/upstream-check.yaml .upstream-versions.json
git commit -m "feat(ci): rewrite upstream sync to track GitHub releases, update compose images"
```

---

### Task 17: Loadtest GHA Workflow Update

**Files:**
- Modify: `.github/workflows/nightly-loadtest.yaml` (if exists, otherwise skip)

- [ ] **Step 1: Add benchmark mode to nightly workflow**

If the workflow exists, add a benchmark step that runs the file size matrix. Otherwise, note this as already covered by the existing workflow.

- [ ] **Step 2: Commit if changes made**

```bash
git add .github/workflows/nightly-loadtest.yaml
git commit -m "feat(ci): add benchmark mode to nightly loadtest workflow"
```

---

### Task 18: Performance Documentation Rewrite

**Files:**
- Modify: `docs/performance.md`

- [ ] **Step 1: Rewrite docs/performance.md**

Structure:
1. **Performance Targets** — table from plan spec
2. **Benchmark Methodology** — how to run `cmd/loadtest -mode=benchmark`
3. **File Size Optimization** — placeholder table for benchmark results
4. **Compression Ratios** — placeholder table for ZSTD level results
5. **MinIO vs S3 Estimation** — extrapolation formula
6. **Cost Projections** — formula-based estimates
7. **Running Benchmarks** — CLI commands

Include the extrapolation formula:
```
Estimated S3 latency = MinIO latency + S3 first-byte overhead (50-150ms)
```

And cost formula:
```
Monthly S3 storage = data_size_gb × compression_ratio⁻¹ × $0.023/GB
Monthly S3 requests = queries_per_day × 30 × avg_gets_per_query × $0.0004/1000
```

- [ ] **Step 2: Commit**

```bash
git add docs/performance.md
git commit -m "docs: rewrite performance.md with benchmark methodology and cost projections"
```

---

### Task 19: CHANGELOG and Version Bump

**Files:**
- Modify: `CHANGELOG.md`

- [ ] **Step 1: Add M10 entry to CHANGELOG**

```markdown
## [0.10.0] — 2026-05-04

### Added
- E2E: VictoriaLogs hot tier, multi-level vlselect, loki-vl-proxy in Docker Compose
- E2E: Internal Docker networking (only Grafana on port 3003)
- E2E: Loki proxy integration tests, vlselect multi-level tests, performance assertion tests
- Datagen: 5 realistic log patterns (JSON, logfmt, nginx, Java stacktrace, OTEL)
- Datagen: Dual-write to VL and S3 for hot/cold verification
- Loadtest: Benchmark mode for file size × row group × compression matrix
- Helm: Single YAML config blob in ConfigMap (no individual flag mapping)
- Helm: Common section deep-merged into components
- Helm: Separate toggleable headless services for discovery
- Helm: VPA support, extraManifests, vmauth Secret routing
- CI: Upstream sync tracks GitHub releases (not Go module versions)
- Docs: Performance documentation with benchmark methodology and cost projections

### Changed
- Helm: vmauth config stored as Secret instead of ConfigMap
- Helm: All components use generic HPA/VPA/PDB/ServiceMonitor/Ingress templates
- Grafana: 5 datasources (cold, hot, multi-level, Loki proxy, Jaeger)

### Removed
- Docker Compose: Host port mappings for non-Grafana services
- Helm: compaction-rbac.yaml (config in lakehouseConfig blob)
```

- [ ] **Step 2: Commit**

```bash
git add CHANGELOG.md
git commit -m "chore: bump version to 0.10.0, update CHANGELOG for M10"
```
