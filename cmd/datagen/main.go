package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"math/big"
	mrand "math/rand"
	"net/http"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
)

type LogRow = schema.LogRow
type TraceRow = schema.TraceRow

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

func main() {
	endpoint := flag.String("endpoint", "http://localhost:9000", "S3/MinIO endpoint")
	bucket := flag.String("bucket", "obs-archive", "S3 bucket name")
	accessKey := flag.String("access-key", "minioadmin", "S3 access key")
	secretKey := flag.String("secret-key", "minioadmin", "S3 secret key")
	logsCount := flag.Int("logs", 5000, "number of log rows per batch")
	tracesCount := flag.Int("traces", 1000, "number of trace spans per batch")
	hoursBack := flag.Int("hours-back", 48, "generate historical data for this many hours back")
	interval := flag.Duration("interval", 0, "continuous mode: generate new data every interval (e.g. 30s)")
	tenantPrefix := flag.String("tenant-prefix", "", "S3 key prefix for tenant isolation (e.g. '0/0/' or '1/1/')")
	dualWrite := flag.Bool("dual-write", false, "also push logs to VictoriaLogs and traces to VictoriaTraces")
	vlEndpoint := flag.String("vl-endpoint", "http://localhost:9428", "VictoriaLogs endpoint for dual-write")
	vtEndpoint := flag.String("vt-endpoint", "", "VictoriaTraces endpoint for dual-write (OTLP JSON format)")
	flag.Parse()

	ctx := context.Background()

	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(credentials.NewStaticCredentialsProvider(*accessKey, *secretKey, "")),
	)
	if err != nil {
		log.Fatalf("load aws config: %v", err)
	}

	client := s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(*endpoint)
		o.UsePathStyle = true
	})

	generateBatch(ctx, client, *bucket, *tenantPrefix, *logsCount, *tracesCount, *hoursBack, *dualWrite, *vlEndpoint, *vtEndpoint)

	if *interval > 0 {
		log.Printf("Continuous mode: generating %d logs + %d traces every %s", *logsCount, *tracesCount, *interval)
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		for range ticker.C {
			generateBatch(ctx, client, *bucket, *tenantPrefix, *logsCount, *tracesCount, 1, *dualWrite, *vlEndpoint, *vtEndpoint)
		}
	}
}

func generateBatch(ctx context.Context, client *s3.Client, bucket, tenantPrefix string, logsCount, tracesCount, hoursBack int, dualWrite bool, vlEndpoint, vtEndpoint string) {
	now := time.Now().UTC()
	rng := mrand.New(mrand.NewSource(now.UnixNano())) // #nosec G404 -- synthetic test data, not security-sensitive

	if tenantPrefix != "" {
		log.Printf("Generating %d log rows and %d trace spans over %d hours (tenant prefix: %s)...", logsCount, tracesCount, hoursBack, tenantPrefix)
	} else {
		log.Printf("Generating %d log rows and %d trace spans over %d hours...", logsCount, tracesCount, hoursBack)
	}

	batchID := randomHex(8)
	logsByPartition := make(map[string][]LogRow)
	for i := 0; i < logsCount; i++ {
		hoursAgo := rng.Intn(hoursBack) + 1
		if hoursBack <= 1 {
			hoursAgo = 0
		}
		ts := now.Add(-time.Duration(hoursAgo) * time.Hour).Add(time.Duration(rng.Intn(3600)) * time.Second)
		if hoursBack <= 1 {
			ts = now.Add(-time.Duration(rng.Intn(3600)) * time.Second)
		}
		svc := services[rng.Intn(len(services))]
		ns := namespaces[rng.Intn(len(namespaces))]
		env := deployEnvs[rng.Intn(len(deployEnvs))]
		region := regions[rng.Intn(len(regions))]
		node := k8sNodes[rng.Intn(len(k8sNodes))]
		host := hostNames[rng.Intn(len(hostNames))]
		lvl := levels[rng.Intn(len(levels))]
		pattern := pickPattern(rng)
		body, logAttrs := pattern(rng, ts, svc, lvl)
		traceID := randomHex(32)
		spanID := randomHex(16)

		row := LogRow{
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
			Stream:            fmt.Sprintf("{service.name=%q,k8s.namespace.name=%q,k8s.deployment.name=%q,deployment.environment=%q,cloud.region=%q}", svc, ns, svc, env, region),
			StreamID:          randomHex(16),
			ScopeName:         "github.com/reliablyobserve/instrumentation",
			ResourceAttributes: map[string]string{
				"service.version":    fmt.Sprintf("1.%d.0", rng.Intn(10)),
				"telemetry.sdk.name": "opentelemetry",
			},
			LogAttributes: logAttrs,
		}

		key := partitionKeyBatch(tenantPrefix, "logs", ts, batchID)
		logsByPartition[key] = append(logsByPartition[key], row)
	}

	for key, rows := range logsByPartition {
		data, err := writeLogsParquet(rows)
		if err != nil {
			log.Printf("ERROR write logs parquet: %v", err)
			return
		}
		if err := upload(ctx, client, bucket, key, data); err != nil {
			log.Printf("ERROR upload %s: %v", key, err)
			return
		}
		log.Printf("  uploaded %s (%d rows, %d bytes)", key, len(rows), len(data))
	}

	if dualWrite && vlEndpoint != "" {
		var allLogs []LogRow
		for _, rows := range logsByPartition {
			allLogs = append(allLogs, rows...)
		}
		if err := pushNDJSON(vlEndpoint, allLogs); err != nil {
			log.Printf("WARNING: dual-write to VL failed: %v", err)
		} else {
			log.Printf("  dual-write: pushed %d logs to VL at %s", len(allLogs), vlEndpoint)
		}
	}

	tracesByPartition := make(map[string][]TraceRow)
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
		traceID := randomHex(32)
		svc := services[rng.Intn(len(services))]
		ns := namespaces[rng.Intn(len(namespaces))]
		env := deployEnvs[rng.Intn(len(deployEnvs))]
		region := regions[rng.Intn(len(regions))]
		node := k8sNodes[rng.Intn(len(k8sNodes))]
		host := hostNames[rng.Intn(len(hostNames))]

		spansPerTrace := 2 + rng.Intn(4)
		parentSpanID := ""
		for s := 0; s < spansPerTrace; s++ {
			spanID := randomHex(16)
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
			httpMethod := ""
			httpCode := ""
			httpUrl := ""
			dbSystem := ""
			dbStmt := ""

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
				"service.version":    fmt.Sprintf("1.%d.0", rng.Intn(10)),
				"telemetry.sdk.name": "opentelemetry",
			}
			spanAttrs := map[string]string{}

			row := TraceRow{
				TimestampUnixNano:  endTime.UnixNano(),
				StartTimeUnixNano:  startTime.UnixNano(),
				TraceID:            traceID,
				SpanID:             spanID,
				ParentSpanID:       parentSpanID,
				SpanName:           spanName,
				SpanKind:           int32(1 + rng.Intn(3)),
				StatusCode:         statusCode,
				StatusMessage:      statusMsg,
				DurationNs:         dur.Nanoseconds(),
				ServiceName:        svc,
				ScopeName:          "github.com/reliablyobserve/instrumentation",
				DeployEnv:          env,
				CloudRegion:        region,
				HostName:           host,
				K8sNamespaceName:   ns,
				K8sDeploymentName:  svc,
				K8sNodeName:        node,
				HTTPMethod:         httpMethod,
				HTTPStatusCode:     httpCode,
				HTTPUrl:            httpUrl,
				DBSystem:           dbSystem,
				DBStatement:        dbStmt,
				ResourceAttributes: resAttrs,
				SpanAttributes:     spanAttrs,
				ScopeAttributes:    map[string]string{},
			}

			key := partitionKeyBatch(tenantPrefix, "traces", startTime, batchID)
			tracesByPartition[key] = append(tracesByPartition[key], row)
			parentSpanID = spanID
		}
	}

	for key, rows := range tracesByPartition {
		data, err := writeTracesParquet(rows)
		if err != nil {
			log.Printf("ERROR write traces parquet: %v", err)
			return
		}
		if err := upload(ctx, client, bucket, key, data); err != nil {
			log.Printf("ERROR upload %s: %v", key, err)
			return
		}
		log.Printf("  uploaded %s (%d rows, %d bytes)", key, len(rows), len(data))
	}

	if dualWrite && vtEndpoint != "" {
		var allTraces []TraceRow
		for _, rows := range tracesByPartition {
			allTraces = append(allTraces, rows...)
		}
		if err := pushOTLPTraces(vtEndpoint, allTraces); err != nil {
			log.Printf("WARNING: dual-write to VT failed: %v", err)
		} else {
			log.Printf("  dual-write: pushed %d traces to VT at %s", len(allTraces), vtEndpoint)
		}
	}

	totalLogs := 0
	for _, rows := range logsByPartition {
		totalLogs += len(rows)
	}
	totalTraces := 0
	for _, rows := range tracesByPartition {
		totalTraces += len(rows)
	}

	log.Printf("Batch done: %d log rows in %d partitions, %d trace spans in %d partitions",
		totalLogs, len(logsByPartition), totalTraces, len(tracesByPartition))
}

func partitionKeyBatch(prefix, signal string, ts time.Time, batchID string) string {
	return fmt.Sprintf("%s%s/dt=%s/hour=%02d/%s.parquet",
		prefix, signal, ts.Format("2006-01-02"), ts.Hour(), batchID)
}

func writeLogsParquet(rows []LogRow) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[LogRow](&buf,
		parquet.Compression(&parquet.Zstd),
	)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func writeTracesParquet(rows []TraceRow) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewGenericWriter[TraceRow](&buf,
		parquet.Compression(&parquet.Zstd),
	)
	if _, err := writer.Write(rows); err != nil {
		return nil, err
	}
	if err := writer.Close(); err != nil {
		return nil, err
	}
	return buf.Bytes(), nil
}

func upload(ctx context.Context, client *s3.Client, bucket, key string, data []byte) error {
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket:      aws.String(bucket),
		Key:         aws.String(key),
		Body:        bytes.NewReader(data),
		ContentType: aws.String("application/octet-stream"),
	})
	return err
}

func randomHex(length int) string {
	b := make([]byte, length/2)
	if _, err := rand.Read(b); err != nil {
		n, _ := rand.Int(rand.Reader, big.NewInt(1<<62))
		return fmt.Sprintf("%0*x", length, n)
	}
	return fmt.Sprintf("%x", b)
}

var spanKindNames = map[int32]string{1: "CLIENT", 2: "SERVER", 3: "PRODUCER", 4: "CONSUMER"}

func pushOTLPTraces(endpoint string, rows []TraceRow) error {
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
	intVal := func(n int64) interface{} {
		return map[string]interface{}{"intValue": fmt.Sprintf("%d", n)}
	}

	byService := map[string][]TraceRow{}
	for _, r := range rows {
		byService[r.ServiceName] = append(byService[r.ServiceName], r)
	}

	var resourceSpans []otlpResourceSpans
	for svc, svcRows := range byService {
		r0 := svcRows[0]
		resAttrs := []otlpKV{
			{"service.name", strVal(svc)},
			{"deployment.environment", strVal(r0.DeployEnv)},
			{"cloud.region", strVal(r0.CloudRegion)},
			{"host.name", strVal(r0.HostName)},
			{"k8s.namespace.name", strVal(r0.K8sNamespaceName)},
			{"k8s.deployment.name", strVal(r0.K8sDeploymentName)},
			{"k8s.node.name", strVal(r0.K8sNodeName)},
		}

		var spans []otlpSpan
		for _, r := range svcRows {
			spanAttrs := []otlpKV{}
			if r.HTTPMethod != "" {
				spanAttrs = append(spanAttrs, otlpKV{"http.method", strVal(r.HTTPMethod)})
			}
			if r.HTTPStatusCode != "" {
				spanAttrs = append(spanAttrs, otlpKV{"http.status_code", strVal(r.HTTPStatusCode)})
			}
			if r.HTTPUrl != "" {
				spanAttrs = append(spanAttrs, otlpKV{"http.url", strVal(r.HTTPUrl)})
			}
			if r.DBSystem != "" {
				spanAttrs = append(spanAttrs, otlpKV{"db.system", strVal(r.DBSystem)})
			}
			if r.DBStatement != "" {
				spanAttrs = append(spanAttrs, otlpKV{"db.statement", strVal(r.DBStatement)})
			}
			spanAttrs = append(spanAttrs, otlpKV{"service.version", strVal("1." + fmt.Sprintf("%d", mrand.Intn(5)) + ".0")})

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

		_ = intVal // suppress unused if not needed

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

	resp, err := http.Post(endpoint+"/insert/opentelemetry/v1/traces", "application/json", bytes.NewReader(data))
	if err != nil {
		return fmt.Errorf("push to VT: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusAccepted && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("push to VT: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}

func pushNDJSON(endpoint string, rows []LogRow) error {
	var buf bytes.Buffer
	for _, r := range rows {
		line := map[string]any{
			"_time":                  time.Unix(0, r.TimestampUnixNano).Format(time.RFC3339Nano),
			"_msg":                   r.Body,
			"level":                  r.SeverityText,
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
		}
		enc, _ := json.Marshal(line)
		buf.Write(enc)
		buf.WriteByte('\n')
	}

	resp, err := http.Post(endpoint+"/insert/jsonline?_stream_fields=service.name,k8s.namespace.name,k8s.deployment.name,deployment.environment,cloud.region,host.name,k8s.node.name,k8s.pod.name,level", "application/x-ndjson", &buf)
	if err != nil {
		return fmt.Errorf("push to VL: %w", err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK && resp.StatusCode != http.StatusNoContent {
		body, _ := io.ReadAll(resp.Body)
		return fmt.Errorf("push to VL: status %d: %s", resp.StatusCode, string(body))
	}
	return nil
}
