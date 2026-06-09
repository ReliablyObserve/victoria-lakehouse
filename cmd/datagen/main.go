package main

import (
	"bytes"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	mrand "math/rand"
	"net/http"
	"sort"
	"time"
)

var (
	services    = []string{"api-gateway", "user-service", "order-service", "payment-service", "notification-service"}
	namespaces  = []string{"production", "staging"}
	deployEnvs  = []string{"production", "staging", "canary"}
	regions     = []string{"us-east-1", "eu-west-1", "ap-southeast-1"}
	k8sNodes    = []string{"node-pool-a-1", "node-pool-a-2", "node-pool-b-1", "node-pool-b-2"}
	hostNames   = []string{"ip-10-0-1-42", "ip-10-0-2-17", "ip-10-0-3-88", "ip-10-1-1-55", "ip-10-1-2-33"}
	levels      = []string{"INFO", "WARN", "ERROR", "DEBUG"}
	levelNums   = map[string]int32{"DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17}
	httpMethods = []string{"GET", "POST", "PUT", "DELETE", "PATCH"}
	httpCodes   = []string{"200", "201", "204", "400", "401", "403", "404", "500", "502", "503"}
	dbSystems   = []string{"postgresql", "redis", "elasticsearch"}
	spanNames   = []string{
		"HTTP GET /api/v1/users", "HTTP POST /api/v1/orders",
		"HTTP GET /api/v1/health", "DB SELECT users", "DB INSERT orders",
		"gRPC /payment.Process", "Redis GET session", "Kafka produce events",
		"HTTP GET /api/v1/products", "HTTP DELETE /api/v1/sessions",
	}
)

// continuousSpreadSec controls timestamp jitter in continuous-mode
// batches. When > 0, every row's ts is `now - rand(0, spreadSec)`
// — the rows in a 6-second batch land within the last 6 seconds of
// real time rather than being shotgunned across the last hour, so
// Grafana shows a steady ingest line instead of a sawtooth "mountain"
// where the seed-batch dominates current-time queries. Set from the
// --interval flag in continuous mode; zero outside continuous mode
// (initial seed-batch keeps the historical spread for backfill).
var continuousSpreadSec int

// httpClient bounds every push so a slow/hung endpoint (e.g. LH cold blocking on
// injected S3 latency) can't stall the whole batch indefinitely. Without a
// timeout, http.DefaultClient waits forever, the 6s ticker falls behind, and
// continuous-mode ingest develops gaps (empty per-minute buckets) that look like
// a cold-tier regression but are really the generator stuck on one endpoint. A
// timed-out push drops that one batch/endpoint and the next tick recovers.
var httpClient = &http.Client{Timeout: 30 * time.Second}

func main() {
	logsCount := flag.Int("logs", 5000, "number of log rows per batch")
	tracesCount := flag.Int("traces", 1000, "number of trace spans per batch")
	hoursBack := flag.Int("hours-back", 48, "generate historical data for this many hours back")
	interval := flag.Duration("interval", 0, "continuous mode: generate new data every interval (e.g. 30s)")
	vlEndpoint := flag.String("vl-endpoint", "", "VictoriaLogs hot endpoint (e.g. http://victorialogs:9428)")
	vtEndpoint := flag.String("vt-endpoint", "", "VictoriaTraces hot endpoint (e.g. http://victoriatraces:10428)")
	lhLogsEndpoint := flag.String("lh-logs-endpoint", "", "Lakehouse logs cold endpoint (e.g. http://lakehouse-logs:9428)")
	lhTracesEndpoint := flag.String("lh-traces-endpoint", "", "Lakehouse traces cold endpoint (e.g. http://lakehouse-traces:10428)")
	lokiEndpoint := flag.String("loki-endpoint", "", "Grafana Loki push endpoint (e.g. http://loki:3100)")
	tempoEndpoint := flag.String("tempo-endpoint", "", "Grafana Tempo OTLP endpoint (e.g. http://tempo:4318)")
	accountID := flag.String("account-id", "0", "tenant AccountID header")
	projectID := flag.String("project-id", "0", "tenant ProjectID header")
	orgID := flag.String("org-id", "", "string tenant ID via X-Scope-OrgID header (overrides account-id/project-id)")
	flag.Parse()

	if *vlEndpoint == "" && *lhLogsEndpoint == "" && *lokiEndpoint == "" {
		log.Fatal("at least one of --vl-endpoint, --lh-logs-endpoint, or --loki-endpoint is required")
	}

	generateBatch(*logsCount, *tracesCount, *hoursBack, *vlEndpoint, *vtEndpoint, *lhLogsEndpoint, *lhTracesEndpoint, *lokiEndpoint, *tempoEndpoint, *accountID, *projectID, *orgID)

	if *interval > 0 {
		// In continuous mode each batch's timestamps are clustered
		// within the last `interval` seconds so successive batches
		// look like real-time ingest (no historical mountain). The
		// initial seed batch above keeps the original spread to
		// backfill historical-window dashboards on startup.
		continuousSpreadSec = int((*interval).Seconds())
		if continuousSpreadSec < 1 {
			continuousSpreadSec = 1
		}
		log.Printf("Continuous mode: generating %d logs + %d traces every %s (timestamps spread over last %ds)",
			*logsCount, *tracesCount, *interval, continuousSpreadSec)
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		for range ticker.C {
			generateBatch(*logsCount, *tracesCount, 1, *vlEndpoint, *vtEndpoint, *lhLogsEndpoint, *lhTracesEndpoint, *lokiEndpoint, *tempoEndpoint, *accountID, *projectID, *orgID)
		}
	}
}

type traceCtx struct {
	traceID  string
	spanIDs  []string
	svc      string
	ns       string
	env      string
	region   string
	node     string
	host     string
	baseTime time.Time
}

type traceRow struct {
	TimestampUnixNano int64
	StartTimeUnixNano int64
	TraceID           string
	SpanID            string
	ParentSpanID      string
	SpanName          string
	SpanKind          int32
	StatusCode        int32
	StatusMessage     string
	DurationNs        int64
	ServiceName       string
	ScopeName         string
	DeployEnv         string
	CloudRegion       string
	HostName          string
	K8sNamespaceName  string
	K8sDeploymentName string
	K8sNodeName       string
	HTTPMethod        string
	HTTPStatusCode    string
	HTTPUrl           string
	DBSystem          string
	DBStatement       string
	ResourceAttrs     map[string]string
	SpanAttrs         map[string]string
	ScopeAttrs        map[string]string
}

type logRow struct {
	TimestampUnixNano int64
	Body              string
	SeverityText      string
	SeverityNumber    int32
	ServiceName       string
	K8sNamespaceName  string
	K8sPodName        string
	K8sDeploymentName string
	K8sNodeName       string
	DeployEnv         string
	CloudRegion       string
	HostName          string
	TraceID           string
	SpanID            string
	ScopeName         string
	ResourceAttrs     map[string]string
	LogAttrs          map[string]string
}

func generateBatch(logsCount, tracesCount, hoursBack int, vlEndpoint, vtEndpoint, lhLogsEndpoint, lhTracesEndpoint, lokiEndpoint, tempoEndpoint, accountID, projectID, orgID string) {
	now := time.Now().UTC()
	rng := mrand.New(mrand.NewSource(now.UnixNano())) // #nosec G404 -- synthetic test data

	if orgID != "" {
		log.Printf("Generating %d logs + %d trace spans over %dh (org_id=%s)...",
			logsCount, tracesCount, hoursBack, orgID)
	} else {
		log.Printf("Generating %d logs + %d trace spans over %dh (account=%s, project=%s)...",
			logsCount, tracesCount, hoursBack, accountID, projectID)
	}

	// ── Phase 1: Generate traces ──

	var allTraces []traceRow
	var traceContexts []traceCtx
	numTraces := tracesCount / 3
	if numTraces < 1 {
		numTraces = 1
	}
	for t := 0; t < numTraces; t++ {
		hoursAgo := rng.Intn(hoursBack) + 1
		if hoursBack <= 1 {
			hoursAgo = 0
		}
		baseTime := now.Add(-time.Duration(hoursAgo) * time.Hour).Add(time.Duration(rng.Intn(3600)) * time.Second)
		if hoursBack <= 1 {
			baseTime = now.Add(-time.Duration(rng.Intn(3600)) * time.Second)
		}
		if continuousSpreadSec > 0 {
			// Continuous mode: tight clustering on real-time, not
			// shotgunned across the full last hour.
			baseTime = now.Add(-time.Duration(rng.Intn(continuousSpreadSec)) * time.Second)
		}
		traceID := randomHex(32)
		// Build an RPC call-chain across services so span pairs encode real
		// cross-service edges: each hop is CLIENT(kind=3) in svc[i] linked to
		// SERVER(kind=2) in svc[i+1] via parent_span_id == span_id. VT's
		// servicegraph background task joins these to derive (parent, child).
		numHops := 2 + rng.Intn(4) // 2..5 hops → 3..6 services on the path
		chain := make([]string, numHops+1)
		chain[0] = services[rng.Intn(len(services))]
		for i := 1; i <= numHops; i++ {
			// Pick a DIFFERENT service for the next hop to avoid same-service edges
			// (real microservice topologies rarely call themselves over RPC).
			for {
				next := services[rng.Intn(len(services))]
				if next != chain[i-1] {
					chain[i] = next
					break
				}
			}
		}
		ns := namespaces[rng.Intn(len(namespaces))]
		env := deployEnvs[rng.Intn(len(deployEnvs))]
		region := regions[rng.Intn(len(regions))]
		node := k8sNodes[rng.Intn(len(k8sNodes))]
		host := hostNames[rng.Intn(len(hostNames))]

		tc := traceCtx{
			traceID: traceID, svc: chain[0], ns: ns, env: env,
			region: region, node: node, host: host, baseTime: baseTime,
		}

		spansPerTrace := numHops * 2 // CLIENT + SERVER per hop
		parentSpanID := ""
		for s := 0; s < spansPerTrace; s++ {
			hopIdx := s / 2
			isClient := s%2 == 0
			var svc string
			var spanKind int32
			if isClient {
				// CLIENT span in the caller service; its span_id becomes the
				// SERVER's parent_span_id on the next iteration.
				svc = chain[hopIdx]
				spanKind = 3
			} else {
				// SERVER span in the callee service; its parent_span_id is the
				// previous CLIENT's span_id (already in parentSpanID).
				svc = chain[hopIdx+1]
				spanKind = 2
			}
			spanID := randomHex(16)
			tc.spanIDs = append(tc.spanIDs, spanID)
			startTime := baseTime.Add(time.Duration(s*10) * time.Millisecond)
			dur := time.Duration(5+rng.Intn(50)) * time.Millisecond
			endTime := startTime.Add(dur)

			statusCode := int32(0)
			statusMsg := ""
			if rng.Float64() < 0.1 {
				statusCode = 2
				statusMsg = "internal error"
			}

			spanName := spanNames[rng.Intn(len(spanNames))]
			httpMethod, httpCode, httpUrl := "", "", ""
			dbSystem, dbStmt := "", ""

			if len(spanName) > 4 && spanName[:4] == "HTTP" {
				httpMethod = httpMethods[rng.Intn(len(httpMethods))]
				httpCode = httpCodes[rng.Intn(len(httpCodes))]
				httpUrl = fmt.Sprintf("http://%s:8080%s", svc, spanName[len("HTTP "+httpMethod):])
			} else if len(spanName) > 2 && spanName[:2] == "DB" {
				dbSystem = dbSystems[0]
				dbStmt = fmt.Sprintf("SELECT * FROM %s WHERE id = $1", spanName[3:])
			} else if spanName == "Redis GET session" {
				dbSystem = "redis"
				dbStmt = "GET session:user:" + randomHex(8)
			}

			resAttrs := map[string]string{
				"service.name":            svc,
				"service.version":         fmt.Sprintf("1.%d.0", rng.Intn(10)),
				"service.instance.id":     fmt.Sprintf("%s-%s", svc, randomHex(8)),
				"telemetry.sdk.name":      "opentelemetry",
				"telemetry.sdk.language":  "go",
				"telemetry.sdk.version":   "1.28.0",
				"deployment.environment":  env,
				"cloud.region":            region,
				"cloud.provider":          "aws",
				"cloud.account.id":        "123456789012",
				"host.name":               host,
				"host.arch":               "amd64",
				"os.type":                 "linux",
				"process.runtime.name":    "go",
				"process.runtime.version": "1.22.4",
				"container.id":            randomHex(64),
				"k8s.namespace.name":      ns,
				"k8s.deployment.name":     svc,
				"k8s.node.name":           node,
				"k8s.pod.name":            fmt.Sprintf("%s-%s", svc, randomHex(10)),
				"k8s.cluster.name":        "prod-" + region,
			}

			spanAttrs := map[string]string{
				"thread.id":     fmt.Sprintf("%d", 1+rng.Intn(32)),
				"code.function": spanName,
			}
			if httpMethod != "" {
				spanAttrs["http.method"] = httpMethod
				spanAttrs["http.request.method"] = httpMethod
				spanAttrs["http.route"] = fmt.Sprintf("/api/v1/%s", svc)
				spanAttrs["http.scheme"] = "http"
				spanAttrs["url.scheme"] = "http"
				spanAttrs["server.address"] = fmt.Sprintf("%s.%s.svc.cluster.local", svc, ns)
				spanAttrs["server.port"] = "8080"
				spanAttrs["network.protocol.version"] = "1.1"
				spanAttrs["user_agent.original"] = "Go-http-client/2.0"
			}
			if httpCode != "" {
				spanAttrs["http.status_code"] = httpCode
				spanAttrs["http.response.status_code"] = httpCode
			}
			if httpUrl != "" {
				spanAttrs["http.url"] = httpUrl
				spanAttrs["url.full"] = httpUrl
			}
			if dbSystem != "" {
				spanAttrs["db.system"] = dbSystem
				spanAttrs["db.name"] = "appdb"
				spanAttrs["db.operation"] = "SELECT"
				spanAttrs["db.connection_string"] = fmt.Sprintf("%s:6379", dbSystem)
				spanAttrs["server.address"] = fmt.Sprintf("%s.%s.svc.cluster.local", dbSystem, ns)
				spanAttrs["server.port"] = "6379"
			}
			if dbStmt != "" {
				spanAttrs["db.statement"] = dbStmt
			}
			if httpMethod == "" && dbSystem == "" {
				spanAttrs["rpc.system"] = "grpc"
				spanAttrs["rpc.service"] = fmt.Sprintf("com.reliablyobserve.%s.v1.Service", svc)
				spanAttrs["rpc.method"] = spanName
				spanAttrs["rpc.grpc.status_code"] = "0"
				spanAttrs["server.address"] = fmt.Sprintf("%s.%s.svc.cluster.local", svc, ns)
				spanAttrs["server.port"] = "9090"
			}

			row := traceRow{
				TimestampUnixNano: endTime.UnixNano(),
				StartTimeUnixNano: startTime.UnixNano(),
				TraceID:           traceID,
				SpanID:            spanID,
				ParentSpanID:      parentSpanID,
				SpanName:          spanName,
				SpanKind:          spanKind,
				StatusCode:        statusCode,
				StatusMessage:     statusMsg,
				DurationNs:        dur.Nanoseconds(),
				ServiceName:       svc,
				ScopeName:         "github.com/reliablyobserve/instrumentation",
				DeployEnv:         env,
				CloudRegion:       region,
				HostName:          host,
				K8sNamespaceName:  ns,
				K8sDeploymentName: svc,
				K8sNodeName:       node,
				HTTPMethod:        httpMethod,
				HTTPStatusCode:    httpCode,
				HTTPUrl:           httpUrl,
				DBSystem:          dbSystem,
				DBStatement:       dbStmt,
				ResourceAttrs:     resAttrs,
				SpanAttrs:         spanAttrs,
				ScopeAttrs: map[string]string{
					"otel.scope.name":    "github.com/reliablyobserve/instrumentation",
					"otel.scope.version": "0.5.0",
				},
			}
			allTraces = append(allTraces, row)
			parentSpanID = spanID
		}
		traceContexts = append(traceContexts, tc)
	}

	// Push traces to all configured endpoints
	if vtEndpoint != "" {
		if err := pushOTLPTraces(vtEndpoint, allTraces, accountID, projectID, orgID); err != nil {
			log.Printf("WARNING: push traces to VT hot failed: %v", err)
		} else {
			log.Printf("  pushed %d traces to VT hot at %s", len(allTraces), vtEndpoint)
		}
	}
	if lhTracesEndpoint != "" {
		if err := pushOTLPTraces(lhTracesEndpoint, allTraces, accountID, projectID, orgID); err != nil {
			log.Printf("WARNING: push traces to LH cold failed: %v", err)
		} else {
			log.Printf("  pushed %d traces to LH cold at %s", len(allTraces), lhTracesEndpoint)
		}
	}
	if tempoEndpoint != "" {
		if err := pushTempoTraces(tempoEndpoint, allTraces); err != nil {
			log.Printf("WARNING: push traces to Tempo failed: %v", err)
		} else {
			log.Printf("  pushed %d traces to Tempo at %s", len(allTraces), tempoEndpoint)
		}
	}

	// ── Phase 2: Generate logs — 70% correlated to traces, 30% independent ──

	var allLogs []logRow
	correlatedCount := 0
	for i := 0; i < logsCount; i++ {
		var traceID, spanID, svc, ns, env, region, node, host string
		var ts time.Time

		correlated := len(traceContexts) > 0 && rng.Float64() < 0.7
		if correlated {
			tc := traceContexts[rng.Intn(len(traceContexts))]
			traceID = tc.traceID
			spanID = tc.spanIDs[rng.Intn(len(tc.spanIDs))]
			svc = tc.svc
			ns = tc.ns
			env = tc.env
			region = tc.region
			node = tc.node
			host = tc.host
			ts = tc.baseTime.Add(time.Duration(rng.Intn(10000)-5000) * time.Millisecond)
			correlatedCount++
		} else {
			hoursAgo := rng.Intn(hoursBack) + 1
			if hoursBack <= 1 {
				hoursAgo = 0
			}
			ts = now.Add(-time.Duration(hoursAgo) * time.Hour).Add(time.Duration(rng.Intn(3600)) * time.Second)
			if hoursBack <= 1 {
				ts = now.Add(-time.Duration(rng.Intn(3600)) * time.Second)
			}
			if continuousSpreadSec > 0 {
				// Continuous mode: cluster on real-time so successive
				// batches form a steady stream.
				ts = now.Add(-time.Duration(rng.Intn(continuousSpreadSec)) * time.Second)
			}
			svc = services[rng.Intn(len(services))]
			ns = namespaces[rng.Intn(len(namespaces))]
			env = deployEnvs[rng.Intn(len(deployEnvs))]
			region = regions[rng.Intn(len(regions))]
			node = k8sNodes[rng.Intn(len(k8sNodes))]
			host = hostNames[rng.Intn(len(hostNames))]
			traceID = randomHex(32)
			spanID = randomHex(16)
		}

		lvl := levels[rng.Intn(len(levels))]
		pattern := pickPattern(rng)
		body, logAttrs := pattern(rng, ts, svc, lvl)
		body = fmt.Sprintf("%s trace_id=%s span_id=%s", body, traceID, spanID)

		row := logRow{
			TimestampUnixNano: ts.UnixNano(),
			Body:              body,
			SeverityText:      lvl,
			SeverityNumber:    levelNums[lvl],
			ServiceName:       svc,
			K8sNamespaceName:  ns,
			K8sPodName:        fmt.Sprintf("%s-%s", svc, randomHex(8)),
			K8sDeploymentName: svc,
			K8sNodeName:       node,
			DeployEnv:         env,
			CloudRegion:       region,
			HostName:          host,
			TraceID:           traceID,
			SpanID:            spanID,
			ScopeName:         "github.com/reliablyobserve/instrumentation",
			ResourceAttrs: map[string]string{
				"service.name":            svc,
				"service.version":         fmt.Sprintf("1.%d.0", rng.Intn(10)),
				"service.instance.id":     fmt.Sprintf("%s-%s", svc, randomHex(8)),
				"telemetry.sdk.name":      "opentelemetry",
				"telemetry.sdk.language":  "go",
				"telemetry.sdk.version":   "1.28.0",
				"deployment.environment":  env,
				"cloud.region":            region,
				"cloud.provider":          "aws",
				"cloud.account.id":        "123456789012",
				"host.name":               host,
				"host.arch":               "amd64",
				"os.type":                 "linux",
				"process.runtime.name":    "go",
				"process.runtime.version": "1.22.4",
				"container.id":            randomHex(64),
				"k8s.namespace.name":      ns,
				"k8s.deployment.name":     svc,
				"k8s.node.name":           node,
				"k8s.pod.name":            fmt.Sprintf("%s-%s", svc, randomHex(10)),
				"k8s.cluster.name":        "prod-" + region,
			},
			LogAttrs: logAttrs,
		}
		allLogs = append(allLogs, row)
	}

	// Push logs to all configured endpoints
	if vlEndpoint != "" {
		if err := pushNDJSON(vlEndpoint, allLogs, accountID, projectID, orgID); err != nil {
			log.Printf("WARNING: push logs to VL hot failed: %v", err)
		} else {
			log.Printf("  pushed %d logs to VL hot at %s", len(allLogs), vlEndpoint)
		}
	}
	if lhLogsEndpoint != "" {
		if err := pushNDJSON(lhLogsEndpoint, allLogs, accountID, projectID, orgID); err != nil {
			log.Printf("WARNING: push logs to LH cold failed: %v", err)
		} else {
			log.Printf("  pushed %d logs to LH cold at %s", len(allLogs), lhLogsEndpoint)
		}
	}
	if lokiEndpoint != "" {
		if err := pushLoki(lokiEndpoint, allLogs); err != nil {
			log.Printf("WARNING: push logs to Loki failed: %v", err)
		} else {
			log.Printf("  pushed %d logs to Loki at %s", len(allLogs), lokiEndpoint)
		}
	}

	log.Printf("Batch done: %d logs (%d correlated), %d trace spans", len(allLogs), correlatedCount, len(allTraces))
}

func randomHex(length int) string {
	b := make([]byte, length/2)
	if _, err := rand.Read(b); err != nil {
		n, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
		return fmt.Sprintf("%0*x", length, n)
	}
	return fmt.Sprintf("%x", b)
}

func setTenantHeaders(req *http.Request, accountID, projectID, orgID string) {
	if orgID != "" {
		req.Header.Set("X-Scope-OrgID", orgID)
	} else {
		req.Header.Set("AccountID", accountID)
		req.Header.Set("ProjectID", projectID)
	}
}

// ndjsonBatchSize bounds each log push so a slower cold-tier ingest (flushing to
// S3 mid-burst) absorbs it within the client timeout instead of the whole 100k
// batch timing out — which silently lost ~45% of logs on the cold tier and made
// the benchmark datasets unequal. Pushed with retry for the same reason.
const ndjsonBatchSize = 5000

func pushNDJSON(endpoint string, rows []logRow, accountID, projectID, orgID string) error {
	for i := 0; i < len(rows); i += ndjsonBatchSize {
		end := i + ndjsonBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]
		if err := withRetry(func() error { return pushNDJSONBatch(endpoint, batch, accountID, projectID, orgID) }); err != nil {
			return err
		}
	}
	return nil
}

// withRetry retries a push up to 3 times (the fn rebuilds the request body each
// attempt). Timeouts under bursty cold-tier ingest are transient, so a retry lands
// the batch rather than dropping it.
func withRetry(fn func() error) error {
	var err error
	for attempt := 1; attempt <= 3; attempt++ {
		if err = fn(); err == nil {
			return nil
		}
		time.Sleep(time.Duration(attempt) * time.Second)
	}
	return err
}

func pushNDJSONBatch(endpoint string, rows []logRow, accountID, projectID, orgID string) error {
	var buf bytes.Buffer
	for _, r := range rows {
		line := map[string]any{
			"_time":                  time.Unix(0, r.TimestampUnixNano).Format(time.RFC3339Nano),
			"_msg":                   r.Body,
			"level":                  r.SeverityText,
			"severity_number":        r.SeverityNumber,
			"service.name":           r.ServiceName,
			"k8s.namespace.name":     r.K8sNamespaceName,
			"k8s.pod.name":           r.K8sPodName,
			"k8s.deployment.name":    r.K8sDeploymentName,
			"k8s.node.name":          r.K8sNodeName,
			"deployment.environment": r.DeployEnv,
			"cloud.region":           r.CloudRegion,
			"host.name":              r.HostName,
			"trace_id":               r.TraceID,
			"span_id":                r.SpanID,
			"scope.name":             r.ScopeName,
		}
		for k, v := range r.ResourceAttrs {
			if _, exists := line[k]; !exists {
				line[k] = v
			}
		}
		for k, v := range r.LogAttrs {
			if _, exists := line[k]; !exists {
				line[k] = v
			}
		}
		enc, _ := json.Marshal(line)
		buf.Write(enc)
		buf.WriteByte('\n')
	}

	url := endpoint + "/insert/jsonline?_stream_fields=service.name,k8s.namespace.name,k8s.deployment.name,deployment.environment,cloud.region,host.name,k8s.node.name,k8s.pod.name,level"
	req, err := http.NewRequest("POST", url, &buf)
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/x-ndjson")
	setTenantHeaders(req, accountID, projectID, orgID)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("push to %s: %w", endpoint, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("push to %s: status %d: %s", endpoint, resp.StatusCode, string(body))
	}
	return nil
}

func pushOTLPTraces(endpoint string, rows []traceRow, accountID, projectID, orgID string) error {
	return pushOTLPTracesToURL(endpoint+"/insert/opentelemetry/v1/traces", rows, accountID, projectID, orgID)
}

// otlpBatchSize keeps each OTLP request well under the receiver's
// -opentelemetry.traces.maxRequestSize (64 MiB default) — pushing all 50k spans
// at once returned HTTP 400 "too big data size" and dropped the trace dataset on
// VT and LH alike. ~5k spans ≈ a few MiB.
const otlpBatchSize = 5000

func pushOTLPTracesToURL(url string, rows []traceRow, accountID, projectID, orgID string) error {
	for i := 0; i < len(rows); i += otlpBatchSize {
		end := i + otlpBatchSize
		if end > len(rows) {
			end = len(rows)
		}
		batch := rows[i:end]
		if err := withRetry(func() error { return pushOTLPTracesBatchToURL(url, batch, accountID, projectID, orgID) }); err != nil {
			return err
		}
	}
	return nil
}

func pushOTLPTracesBatchToURL(url string, rows []traceRow, accountID, projectID, orgID string) error {
	type otlpKV struct {
		Key   string      `json:"key"`
		Value interface{} `json:"value"`
	}
	type otlpSpan struct {
		TraceID           string   `json:"traceId"`
		SpanID            string   `json:"spanId"`
		ParentSpanID      string   `json:"parentSpanId,omitempty"`
		Name              string   `json:"name"`
		Kind              int32    `json:"kind"`
		StartTimeUnixNano string   `json:"startTimeUnixNano"`
		EndTimeUnixNano   string   `json:"endTimeUnixNano"`
		Attributes        []otlpKV `json:"attributes"`
		Status            *struct {
			Code int32 `json:"code"`
		} `json:"status,omitempty"`
	}
	type otlpScopeSpans struct {
		Scope struct {
			Name string `json:"name"`
		} `json:"scope"`
		Spans []otlpSpan `json:"spans"`
	}
	type otlpResourceSpans struct {
		Resource struct {
			Attributes []otlpKV `json:"attributes"`
		} `json:"resource"`
		ScopeSpans []otlpScopeSpans `json:"scopeSpans"`
	}
	type otlpPayload struct {
		ResourceSpans []otlpResourceSpans `json:"resourceSpans"`
	}

	strVal := func(s string) interface{} {
		return map[string]interface{}{"stringValue": s}
	}

	byService := map[string][]traceRow{}
	for _, r := range rows {
		byService[r.ServiceName] = append(byService[r.ServiceName], r)
	}

	var resourceSpans []otlpResourceSpans
	for svc, svcRows := range byService {
		r0 := svcRows[0]

		var resAttrs []otlpKV
		for k, v := range r0.ResourceAttrs {
			resAttrs = append(resAttrs, otlpKV{k, strVal(v)})
		}
		if _, ok := r0.ResourceAttrs["service.name"]; !ok {
			resAttrs = append(resAttrs, otlpKV{"service.name", strVal(svc)})
		}

		var spans []otlpSpan
		for _, r := range svcRows {
			var spanAttrs []otlpKV
			for k, v := range r.SpanAttrs {
				spanAttrs = append(spanAttrs, otlpKV{k, strVal(v)})
			}

			endTimeNano := r.StartTimeUnixNano + r.DurationNs
			span := otlpSpan{
				TraceID:           r.TraceID,
				SpanID:            r.SpanID,
				ParentSpanID:      r.ParentSpanID,
				Name:              r.SpanName,
				Kind:              r.SpanKind,
				StartTimeUnixNano: fmt.Sprintf("%d", r.StartTimeUnixNano),
				EndTimeUnixNano:   fmt.Sprintf("%d", endTimeNano),
				Attributes:        spanAttrs,
			}
			if r.StatusCode == 2 {
				span.Status = &struct {
					Code int32 `json:"code"`
				}{Code: 2}
			}
			spans = append(spans, span)
		}

		rs := otlpResourceSpans{}
		rs.Resource.Attributes = resAttrs
		rs.ScopeSpans = []otlpScopeSpans{{
			Scope: struct {
				Name string `json:"name"`
			}{Name: "github.com/reliablyobserve/instrumentation"},
			Spans: spans,
		}}
		resourceSpans = append(resourceSpans, rs)
	}

	payload := otlpPayload{ResourceSpans: resourceSpans}
	data, err := json.Marshal(payload)
	if err != nil {
		return fmt.Errorf("marshal otlp: %w", err)
	}

	req, err := http.NewRequest("POST", url, bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	setTenantHeaders(req, accountID, projectID, orgID)

	resp, err := httpClient.Do(req)
	if err != nil {
		return fmt.Errorf("push to %s: %w", url, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("push to %s: status %d: %s", url, resp.StatusCode, string(body))
	}
	return nil
}

func pushTempoTraces(endpoint string, rows []traceRow) error {
	const batchSize = 2000
	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		if err := pushOTLPTracesToURL(endpoint+"/v1/traces", rows[i:end], "", "", ""); err != nil {
			return err
		}
	}
	return nil
}

func pushLoki(endpoint string, rows []logRow) error {
	type lokiValue [2]string
	type lokiStream struct {
		Stream map[string]string `json:"stream"`
		Values []lokiValue       `json:"values"`
	}
	type lokiPush struct {
		Streams []lokiStream `json:"streams"`
	}

	byStream := map[string]*lokiStream{}
	for _, r := range rows {
		labels := map[string]string{
			"service_name":           r.ServiceName,
			"level":                  r.SeverityText,
			"k8s_namespace_name":     r.K8sNamespaceName,
			"k8s_deployment_name":    r.K8sDeploymentName,
			"k8s_node_name":          r.K8sNodeName,
			"deployment_environment": r.DeployEnv,
			"cloud_region":           r.CloudRegion,
			"host_name":              r.HostName,
		}
		key := fmt.Sprintf("%s|%s|%s|%s", r.ServiceName, r.SeverityText, r.K8sNamespaceName, r.DeployEnv)
		s, ok := byStream[key]
		if !ok {
			s = &lokiStream{Stream: labels}
			byStream[key] = s
		}
		line := r.Body
		if r.TraceID != "" {
			line = fmt.Sprintf("trace_id=%s %s", r.TraceID, line)
		}
		s.Values = append(s.Values, lokiValue{
			fmt.Sprintf("%d", r.TimestampUnixNano),
			line,
		})
	}

	var streams []lokiStream
	for _, s := range byStream {
		sort.Slice(s.Values, func(i, j int) bool {
			return s.Values[i][0] < s.Values[j][0]
		})
		streams = append(streams, *s)
	}

	const maxValuesPerBatch = 1000
	var batches [][]lokiStream
	var current []lokiStream
	currentValues := 0
	for _, s := range streams {
		for i := 0; i < len(s.Values); i += maxValuesPerBatch {
			end := i + maxValuesPerBatch
			if end > len(s.Values) {
				end = len(s.Values)
			}
			chunk := lokiStream{Stream: s.Stream, Values: s.Values[i:end]}
			if currentValues+len(chunk.Values) > maxValuesPerBatch && len(current) > 0 {
				batches = append(batches, current)
				current = nil
				currentValues = 0
			}
			current = append(current, chunk)
			currentValues += len(chunk.Values)
		}
	}
	if len(current) > 0 {
		batches = append(batches, current)
	}

	for _, batch := range batches {
		payload := lokiPush{Streams: batch}
		data, err := json.Marshal(payload)
		if err != nil {
			return fmt.Errorf("marshal loki payload: %w", err)
		}

		req, err := http.NewRequest("POST", endpoint+"/loki/api/v1/push", bytes.NewReader(data))
		if err != nil {
			return fmt.Errorf("create loki request: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")

		resp, err := httpClient.Do(req)
		if err != nil {
			return fmt.Errorf("push to loki %s: %w", endpoint, err)
		}
		if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent && resp.StatusCode != http.StatusAccepted {
			body, _ := io.ReadAll(resp.Body)
			_ = resp.Body.Close()
			return fmt.Errorf("push to loki %s: status %d: %s", endpoint, resp.StatusCode, string(body))
		}
		_ = resp.Body.Close()
	}
	return nil
}
