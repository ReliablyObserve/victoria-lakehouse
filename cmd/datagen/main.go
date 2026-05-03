package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"flag"
	"fmt"
	"log"
	"math/big"
	mrand "math/rand"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/parquet-go/parquet-go"
)

type LogRow struct {
	TimestampUnixNano int64  `parquet:"timestamp_unix_nano"`
	Body              string `parquet:"body"`
	SeverityText      string `parquet:"severity_text"`
	SeverityNumber    int32  `parquet:"severity_number"`
	ServiceName       string `parquet:"service.name"`
	K8sNamespaceName  string `parquet:"k8s.namespace.name"`
	K8sPodName        string `parquet:"k8s.pod.name"`
	TraceID           string `parquet:"trace_id"`
	SpanID            string `parquet:"span_id"`
	Stream            string `parquet:"_stream"`
	StreamID          string `parquet:"_stream_id"`
	ScopeName         string `parquet:"scope.name"`
}

type TraceRow struct {
	TimestampUnixNano  int64  `parquet:"timestamp_unix_nano"`
	StartTimeUnixNano  int64  `parquet:"start_time_unix_nano"`
	TraceID            string `parquet:"trace_id"`
	SpanID             string `parquet:"span_id"`
	ParentSpanID       string `parquet:"parent_span_id"`
	SpanName           string `parquet:"span.name"`
	SpanKind           int32  `parquet:"span.kind"`
	StatusCode         int32  `parquet:"status.code"`
	StatusMessage      string `parquet:"status.message"`
	DurationNs         int64  `parquet:"duration_ns"`
	ServiceName        string `parquet:"service.name"`
	ScopeName          string `parquet:"scope.name"`
}

var (
	services = []string{"api-gateway", "user-service", "order-service", "payment-service", "notification-service"}
	namespaces = []string{"production", "staging"}
	levels     = []string{"INFO", "WARN", "ERROR", "DEBUG"}
	levelNums  = map[string]int32{"DEBUG": 5, "INFO": 9, "WARN": 13, "ERROR": 17}
	spanNames  = []string{
		"HTTP GET /api/v1/users", "HTTP POST /api/v1/orders",
		"HTTP GET /api/v1/health", "DB SELECT users", "DB INSERT orders",
		"gRPC /payment.Process", "Redis GET session", "Kafka produce events",
		"HTTP GET /api/v1/products", "HTTP DELETE /api/v1/sessions",
	}
	logMessages = []string{
		"request completed successfully",
		"processing incoming request from client",
		"database query executed in 12ms",
		"cache miss for key user:1234",
		"connection established to upstream service",
		"rate limit threshold approaching",
		"failed to parse request body: unexpected EOF",
		"authentication token validated",
		"retry attempt 2/3 for downstream call",
		"graceful shutdown initiated",
		"health check passed all probes",
		"metrics exported to prometheus endpoint",
		"TLS handshake completed",
		"websocket connection upgraded",
		"batch processing completed: 1500 records",
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

	generateBatch(ctx, client, *bucket, *logsCount, *tracesCount, *hoursBack)

	if *interval > 0 {
		log.Printf("Continuous mode: generating %d logs + %d traces every %s", *logsCount, *tracesCount, *interval)
		ticker := time.NewTicker(*interval)
		defer ticker.Stop()
		for range ticker.C {
			generateBatch(ctx, client, *bucket, *logsCount, *tracesCount, 1)
		}
	}
}

func generateBatch(ctx context.Context, client *s3.Client, bucket string, logsCount, tracesCount, hoursBack int) {
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
		lvl := levels[rng.Intn(len(levels))]
		msg := logMessages[rng.Intn(len(logMessages))]
		traceID := randomHex(32)
		spanID := randomHex(16)

		row := LogRow{
			TimestampUnixNano: ts.UnixNano(),
			Body:              fmt.Sprintf("[%s] %s svc=%s", lvl, msg, svc),
			SeverityText:      lvl,
			SeverityNumber:    levelNums[lvl],
			ServiceName:       svc,
			K8sNamespaceName:  ns,
			K8sPodName:        fmt.Sprintf("%s-%s", svc, randomHex(8)),
			TraceID:           traceID,
			SpanID:            spanID,
			Stream:            fmt.Sprintf("{service.name=%q,k8s.namespace.name=%q}", svc, ns),
			StreamID:          randomHex(16),
			ScopeName:         "github.com/reliablyobserve/instrumentation",
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

			row := TraceRow{
				TimestampUnixNano:  endTime.UnixNano(),
				StartTimeUnixNano:  startTime.UnixNano(),
				TraceID:            traceID,
				SpanID:             spanID,
				ParentSpanID:       parentSpanID,
				SpanName:           spanNames[rng.Intn(len(spanNames))],
				SpanKind:           int32(1 + rng.Intn(3)),
				StatusCode:         statusCode,
				StatusMessage:      statusMsg,
				DurationNs:         dur.Nanoseconds(),
				ServiceName:        svc,
				ScopeName:          "github.com/reliablyobserve/instrumentation",
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
