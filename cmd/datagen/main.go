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
	dualWrite := flag.Bool("dual-write", false, "also push logs to VictoriaLogs and traces to VictoriaTraces")
	vlEndpoint := flag.String("vl-endpoint", "http://localhost:9428", "VictoriaLogs endpoint for dual-write")
	vtEndpoint := flag.String("vt-endpoint", "", "VictoriaTraces endpoint for dual-write (Zipkin format)")
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

	generateBatch(ctx, client, *bucket, *logsCount, *tracesCount, *hoursBack, *dualWrite, *vlEndpoint, *vtEndpoint)

	if *interval > 0 {
		log.Printf("Continuous mode: generating %d logs + %d traces every %s", *logsCount, *tracesCount, *interval)
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		for range ticker.C {
			generateBatch(ctx, client, *bucket, *logsCount, *tracesCount, 1, *dualWrite, *vlEndpoint, *vtEndpoint)
		}
	}
}

func generateBatch(ctx context.Context, client *s3.Client, bucket string, logsCount, tracesCount, hoursBack int, dualWrite bool, vlEndpoint, vtEndpoint string) {
	now := time.Now().UTC()
	rng := mrand.New(mrand.NewSource(now.UnixNano())) // #nosec G404 -- synthetic test data, not security-sensitive

	log.Printf("Generating %d log rows and %d trace spans over %d hours...", logsCount, tracesCount, hoursBack)

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
			Stream:            fmt.Sprintf("{service.name=%q,k8s.namespace.name=%q}", svc, ns),
			StreamID:          randomHex(16),
			ScopeName:         "github.com/reliablyobserve/instrumentation",
			LogAttributes:     logAttrs,
		}

		key := partitionKeyBatch("logs", ts, batchID)
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

			row := TraceRow{
				TimestampUnixNano: endTime.UnixNano(),
				StartTimeUnixNano: startTime.UnixNano(),
				TraceID:           traceID,
				SpanID:            spanID,
				ParentSpanID:      parentSpanID,
				SpanName:          spanName,
				SpanKind:          int32(1 + rng.Intn(3)),
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
			}

			key := partitionKeyBatch("traces", startTime, batchID)
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
		if err := pushZipkinTraces(vtEndpoint, allTraces); err != nil {
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

func partitionKeyBatch(signal string, ts time.Time, batchID string) string {
	return fmt.Sprintf("%s/dt=%s/hour=%02d/%s.parquet",
		signal, ts.Format("2006-01-02"), ts.Hour(), batchID)
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

func pushZipkinTraces(endpoint string, rows []TraceRow) error {
	type zipkinEndpoint struct {
		ServiceName string `json:"serviceName"`
	}
	type zipkinSpan struct {
		TraceID       string            `json:"traceId"`
		ID            string            `json:"id"`
		ParentID      string            `json:"parentId,omitempty"`
		Name          string            `json:"name"`
		Timestamp     int64             `json:"timestamp"`
		Duration      int64             `json:"duration"`
		Kind          string            `json:"kind,omitempty"`
		LocalEndpoint zipkinEndpoint    `json:"localEndpoint"`
		Tags          map[string]string `json:"tags"`
	}

	spans := make([]zipkinSpan, 0, len(rows))
	for _, r := range rows {
		tags := map[string]string{
			"deployment.environment": r.DeployEnv,
			"cloud.region":           r.CloudRegion,
			"host.name":              r.HostName,
			"k8s.namespace.name":     r.K8sNamespaceName,
			"k8s.deployment.name":    r.K8sDeploymentName,
			"k8s.node.name":          r.K8sNodeName,
		}
		if r.HTTPMethod != "" {
			tags["http.method"] = r.HTTPMethod
		}
		if r.HTTPStatusCode != "" {
			tags["http.status_code"] = r.HTTPStatusCode
		}
		if r.HTTPUrl != "" {
			tags["http.url"] = r.HTTPUrl
		}
		if r.DBSystem != "" {
			tags["db.system"] = r.DBSystem
		}
		if r.DBStatement != "" {
			tags["db.statement"] = r.DBStatement
		}
		if r.StatusCode == 2 {
			tags["error"] = "true"
			tags["otel.status_code"] = "ERROR"
		}

		spans = append(spans, zipkinSpan{
			TraceID:       r.TraceID,
			ID:            r.SpanID,
			ParentID:      r.ParentSpanID,
			Name:          r.SpanName,
			Timestamp:     r.StartTimeUnixNano / 1000,
			Duration:      r.DurationNs / 1000,
			Kind:          spanKindNames[r.SpanKind],
			LocalEndpoint: zipkinEndpoint{ServiceName: r.ServiceName},
			Tags:          tags,
		})
	}

	data, err := json.Marshal(spans)
	if err != nil {
		return fmt.Errorf("marshal zipkin: %w", err)
	}

	resp, err := http.Post(endpoint+"/api/v2/spans", "application/json", bytes.NewReader(data))
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

	resp, err := http.Post(endpoint+"/insert/jsonline?_stream_fields=service.name,k8s.namespace.name", "application/x-ndjson", &buf)
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
