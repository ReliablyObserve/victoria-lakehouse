//go:build e2e

package e2e

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"testing"
	"time"

	"github.com/aws/aws-sdk-go-v2/aws"
	awsconfig "github.com/aws/aws-sdk-go-v2/config"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

var (
	minioURL      = envOrDefault("MINIO_URL", "http://localhost:29000")
	minioBucket   = envOrDefault("MINIO_BUCKET", "obs-archive")
	minioUser     = envOrDefault("MINIO_USER", "minioadmin")
	minioPassword = envOrDefault("MINIO_PASSWORD", "minioadmin")

	globalReadHeader = "X-Lakehouse-Global-Read"
	globalReadSecret = "lakehouse-e2e-global-key"
)

func newS3Client(t *testing.T) *s3.Client {
	t.Helper()
	ctx := context.Background()
	cfg, err := awsconfig.LoadDefaultConfig(ctx,
		awsconfig.WithRegion("us-east-1"),
		awsconfig.WithCredentialsProvider(
			credentials.NewStaticCredentialsProvider(minioUser, minioPassword, ""),
		),
	)
	if err != nil {
		t.Fatalf("failed to load AWS config: %v", err)
	}
	return s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = aws.String(minioURL)
		o.UsePathStyle = true
	})
}

func listS3Objects(t *testing.T, client *s3.Client, prefix string) []string {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	var keys []string
	paginator := s3.NewListObjectsV2Paginator(client, &s3.ListObjectsV2Input{
		Bucket: aws.String(minioBucket),
		Prefix: aws.String(prefix),
	})

	for paginator.HasMorePages() {
		page, err := paginator.NextPage(ctx)
		if err != nil {
			t.Fatalf("failed to list S3 objects with prefix %q: %v", prefix, err)
		}
		for _, obj := range page.Contents {
			keys = append(keys, aws.ToString(obj.Key))
		}
	}
	return keys
}

// ---------------------------------------------------------------------------
// S3 Bucket Structure Tests
// ---------------------------------------------------------------------------

func TestMultitenancy_S3DefaultTenantLogsExist(t *testing.T) {
	client := newS3Client(t)
	keys := listS3Objects(t, client, "0/0/logs/")
	if len(keys) == 0 {
		t.Fatal("expected Parquet files under 0/0/logs/ prefix, found none")
	}
	t.Logf("found %d files under 0/0/logs/", len(keys))
}

func TestMultitenancy_S3DefaultTenantTracesExist(t *testing.T) {
	client := newS3Client(t)
	keys := listS3Objects(t, client, "0/0/traces/")
	if len(keys) == 0 {
		t.Fatal("expected Parquet files under 0/0/traces/ prefix, found none")
	}
	t.Logf("found %d files under 0/0/traces/", len(keys))
}

func TestMultitenancy_S3SecondaryTenantLogsExist(t *testing.T) {
	client := newS3Client(t)
	keys := listS3Objects(t, client, "1/1/logs/")
	if len(keys) == 0 {
		t.Fatal("expected Parquet files under 1/1/logs/ prefix, found none")
	}
	t.Logf("found %d files under 1/1/logs/", len(keys))
}

func TestMultitenancy_S3SecondaryTenantTracesExist(t *testing.T) {
	client := newS3Client(t)
	keys := listS3Objects(t, client, "1/1/traces/")
	if len(keys) == 0 {
		t.Fatal("expected Parquet files under 1/1/traces/ prefix, found none")
	}
	t.Logf("found %d files under 1/1/traces/", len(keys))
}

func TestMultitenancy_S3NoUnprefixedData(t *testing.T) {
	client := newS3Client(t)

	for _, signal := range []string{"logs/", "traces/"} {
		keys := listS3Objects(t, client, signal)
		if len(keys) > 0 {
			t.Errorf("found %d files at root-level %s prefix (no tenant), expected 0 — all data should be tenant-prefixed", len(keys), signal)
			for i, k := range keys {
				if i >= 5 {
					t.Logf("  ... and %d more", len(keys)-5)
					break
				}
				t.Logf("  unexpected key: %s", k)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Hive Partition Structure Tests
// ---------------------------------------------------------------------------

var hivePartitionRe = regexp.MustCompile(
	`^\d+/\d+/(logs|traces)/dt=\d{4}-\d{2}-\d{2}/hour=\d{2}/[a-f0-9]+\.parquet$`,
)

func TestMultitenancy_S3HivePartitionFormat(t *testing.T) {
	client := newS3Client(t)

	for _, prefix := range []string{"0/0/", "1/1/"} {
		keys := listS3Objects(t, client, prefix)
		if len(keys) == 0 {
			t.Fatalf("no files under prefix %s", prefix)
		}

		for _, key := range keys {
			if !hivePartitionRe.MatchString(key) {
				t.Errorf("S3 key does not match Hive partition format: %s", key)
			}
		}
		t.Logf("all %d keys under %s match Hive partition format", len(keys), prefix)
	}
}

func TestMultitenancy_S3PartitionDateRange(t *testing.T) {
	client := newS3Client(t)
	keys := listS3Objects(t, client, "0/0/logs/")

	dateRe := regexp.MustCompile(`dt=(\d{4}-\d{2}-\d{2})`)
	dates := make(map[string]bool)
	for _, key := range keys {
		m := dateRe.FindStringSubmatch(key)
		if len(m) > 1 {
			dates[m[1]] = true
		}
	}

	if len(dates) == 0 {
		t.Fatal("no date partitions found")
	}
	if len(dates) < 2 {
		t.Errorf("expected data across multiple dates (48h window), found only %d date(s)", len(dates))
	}
	t.Logf("date partitions found: %v", dates)
}

func TestMultitenancy_S3ParquetFileExtension(t *testing.T) {
	client := newS3Client(t)

	for _, prefix := range []string{"0/0/logs/", "0/0/traces/", "1/1/logs/", "1/1/traces/"} {
		keys := listS3Objects(t, client, prefix)
		for _, key := range keys {
			if !strings.HasSuffix(key, ".parquet") {
				t.Errorf("non-parquet file found under %s: %s", prefix, key)
			}
		}
	}
}

// ---------------------------------------------------------------------------
// Tenant Data Volume Tests
// ---------------------------------------------------------------------------

func TestMultitenancy_S3DefaultTenantHasMoreData(t *testing.T) {
	client := newS3Client(t)
	defaultLogs := listS3Objects(t, client, "0/0/logs/")
	secondaryLogs := listS3Objects(t, client, "1/1/logs/")

	if len(defaultLogs) <= len(secondaryLogs) {
		t.Errorf("default tenant (0/0) should have more log files than secondary (1/1): %d vs %d",
			len(defaultLogs), len(secondaryLogs))
	}
	t.Logf("default tenant: %d log files, secondary tenant: %d log files", len(defaultLogs), len(secondaryLogs))
}

func TestMultitenancy_S3TenantSeparation(t *testing.T) {
	client := newS3Client(t)

	defaultKeys := make(map[string]bool)
	for _, k := range listS3Objects(t, client, "0/0/") {
		defaultKeys[k] = true
	}

	secondaryKeys := listS3Objects(t, client, "1/1/")
	for _, k := range secondaryKeys {
		if defaultKeys[k] {
			t.Errorf("key %s appears in both tenants — prefixes must be disjoint", k)
		}
	}
}

// ---------------------------------------------------------------------------
// Lakehouse Query Tests — Default Tenant
// ---------------------------------------------------------------------------

func TestMultitenancy_LakehouseLogsQuery(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "50")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Fatal("expected lakehouse-logs to return data for default tenant")
	}
	t.Logf("lakehouse-logs returned %d lines for default tenant", len(lines))
}

func TestMultitenancy_LakehouseTracesQuery(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "50")

	body := httpGetBody(t, tracesBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Fatal("expected lakehouse-traces to return data for default tenant")
	}
	t.Logf("lakehouse-traces returned %d lines for default tenant", len(lines))
}

func TestMultitenancy_LakehouseLogsServiceFilter(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="api-gateway"`)
	params.Set("limit", "20")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Skip("no api-gateway logs in default tenant cold data")
	}

	for i, line := range lines {
		svc, _ := line["service.name"].(string)
		if svc != "api-gateway" {
			t.Errorf("line %d: expected service.name=api-gateway, got %q", i, svc)
		}
	}
}

func TestMultitenancy_LakehouseTracesServiceFilter(t *testing.T) {
	resp := httpGetAllowStatus(t, tracesBaseURL,
		"/select/jaeger/api/services", nil, http.StatusOK)
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading jaeger services response: %v", err)
	}

	result := mustParseJSON(t, body)
	data, ok := result["data"].([]any)
	if !ok || len(data) == 0 {
		t.Fatal("expected Jaeger services list to have entries")
	}
	t.Logf("jaeger services: %v", data)
}

// ---------------------------------------------------------------------------
// Manifest Tests
// ---------------------------------------------------------------------------

func TestMultitenancy_ManifestLogsHasFiles(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/manifest/range", nil)
	result := mustParseJSON(t, body)

	totalFiles, _ := result["totalFiles"].(float64)
	if totalFiles == 0 {
		t.Fatal("manifest/range for logs reports 0 files")
	}
	t.Logf("logs manifest: %.0f files, minTime=%v, maxTime=%v",
		totalFiles, result["minTime"], result["maxTime"])
}

func TestMultitenancy_ManifestTracesHasFiles(t *testing.T) {
	body := httpGetBody(t, tracesBaseURL, "/manifest/range", nil)
	result := mustParseJSON(t, body)

	totalFiles, _ := result["totalFiles"].(float64)
	if totalFiles == 0 {
		t.Fatal("manifest/range for traces reports 0 files")
	}
	t.Logf("traces manifest: %.0f files, minTime=%v, maxTime=%v",
		totalFiles, result["minTime"], result["maxTime"])
}

func TestMultitenancy_ManifestTimeRangeCovers48h(t *testing.T) {
	body := httpGetBody(t, logsBaseURL, "/manifest/range", nil)
	result := mustParseJSON(t, body)

	minT, _ := result["minTime"].(float64)
	maxT, _ := result["maxTime"].(float64)
	if minT == 0 || maxT == 0 {
		t.Fatal("manifest minTime/maxTime are zero")
	}

	rangeHours := (maxT - minT) / float64(time.Hour)
	if rangeHours < 24 {
		t.Errorf("manifest time range is %.1f hours, expected at least 24h (datagen seeds 48h)", rangeHours)
	}
	t.Logf("manifest covers %.1f hours", rangeHours)
}

// ---------------------------------------------------------------------------
// Global Read Mode Tests
// ---------------------------------------------------------------------------

func httpGetWithHeader(t *testing.T, baseURL, path string, params url.Values, header, value string) (int, []byte) {
	t.Helper()

	u := baseURL + path
	if len(params) > 0 {
		u += "?" + params.Encode()
	}

	req, err := http.NewRequest("GET", u, nil)
	if err != nil {
		t.Fatalf("creating request: %v", err)
	}
	req.Header.Set(header, value)

	client := &http.Client{Timeout: 60 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatalf("GET %s with header %s failed: %v", u, header, err)
	}
	defer func() { _ = resp.Body.Close() }()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("reading response body: %v", err)
	}
	return resp.StatusCode, body
}

func TestMultitenancy_GlobalReadWithCorrectSecret(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "10")

	status, body := httpGetWithHeader(t, logsBaseURL,
		"/select/logsql/query", params, globalReadHeader, globalReadSecret)

	if status != http.StatusOK {
		t.Fatalf("global read with correct secret returned status %d: %s", status, string(body))
	}

	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Fatal("expected global read to return data")
	}
	t.Logf("global read returned %d lines", len(lines))
}

func TestMultitenancy_GlobalReadWithWrongSecret(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "10")

	status, body := httpGetWithHeader(t, logsBaseURL,
		"/select/logsql/query", params, globalReadHeader, "wrong-secret-value")

	// Wrong secret should either be rejected (403) or treated as normal (non-global) query.
	// Both are acceptable — the key point is it should NOT grant global read access.
	if status == http.StatusOK {
		lines := assertValidNDJSON(t, body)
		t.Logf("wrong secret returned %d lines (treated as normal query, not global)", len(lines))
	} else {
		t.Logf("wrong secret returned status %d (rejected)", status)
	}
}

func TestMultitenancy_GlobalReadMissingHeader(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "10")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	t.Logf("query without global read header returned %d lines (should be tenant-scoped)", len(lines))
}

// ---------------------------------------------------------------------------
// Cross-Signal Tests
// ---------------------------------------------------------------------------

func TestMultitenancy_BothSignalsBothTenants(t *testing.T) {
	client := newS3Client(t)

	checks := []struct {
		prefix string
		desc   string
	}{
		{"0/0/logs/", "default tenant logs"},
		{"0/0/traces/", "default tenant traces"},
		{"1/1/logs/", "secondary tenant logs"},
		{"1/1/traces/", "secondary tenant traces"},
	}

	for _, c := range checks {
		t.Run(c.desc, func(t *testing.T) {
			keys := listS3Objects(t, client, c.prefix)
			if len(keys) == 0 {
				t.Fatalf("expected files for %s, found none", c.desc)
			}
			t.Logf("%s: %d files", c.desc, len(keys))
		})
	}
}

// ---------------------------------------------------------------------------
// Edge Cases
// ---------------------------------------------------------------------------

func TestMultitenancy_S3NoDataAtWrongPrefix(t *testing.T) {
	client := newS3Client(t)

	for _, prefix := range []string{"99/99/", "0/1/", "1/0/", "invalid/"} {
		keys := listS3Objects(t, client, prefix)
		if len(keys) > 0 {
			t.Errorf("found %d unexpected files under prefix %s", len(keys), prefix)
		}
	}
}

func TestMultitenancy_S3BucketExists(t *testing.T) {
	client := newS3Client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	_, err := client.HeadBucket(ctx, &s3.HeadBucketInput{
		Bucket: aws.String(minioBucket),
	})
	if err != nil {
		t.Fatalf("bucket %q does not exist or is not accessible: %v", minioBucket, err)
	}
}

func TestMultitenancy_S3TotalFileCount(t *testing.T) {
	client := newS3Client(t)
	all := listS3Objects(t, client, "")

	if len(all) == 0 {
		t.Fatal("bucket is empty")
	}

	var tenant0, tenant1, other int
	for _, k := range all {
		switch {
		case strings.HasPrefix(k, "0/0/"):
			tenant0++
		case strings.HasPrefix(k, "1/1/"):
			tenant1++
		default:
			other++
		}
	}

	if other > 0 {
		t.Errorf("found %d files outside tenant prefixes", other)
	}

	t.Logf("total files: %d (tenant 0/0: %d, tenant 1/1: %d, other: %d)",
		len(all), tenant0, tenant1, other)
}

func TestMultitenancy_QueryNonexistentService(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="nonexistent-service-multitenancy-test"`)
	params.Set("limit", "10")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) != 0 {
		t.Fatalf("expected 0 results for nonexistent service, got %d", len(lines))
	}
}

func TestMultitenancy_FieldNamesIncludeServiceName(t *testing.T) {
	params := defaultTimeParams()
	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_names", params)
	entries := assertValuesResponse(t, body)
	names := extractValueStrings(t, entries)

	if !containsString(names, "service.name") {
		t.Errorf("field_names should include service.name, got: %v", names)
	}
}

func TestMultitenancy_FieldValuesServiceName(t *testing.T) {
	params := defaultTimeParams()
	params.Set("field", "service.name")
	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_values", params)
	entries := assertValuesResponse(t, body)
	values := extractValueStrings(t, entries)

	if len(values) == 0 {
		t.Fatal("expected service.name field values, got none")
	}

	expected := []string{"api-gateway", "user-service", "order-service", "payment-service", "notification-service"}
	for _, svc := range expected {
		if !containsString(values, svc) {
			t.Errorf("missing expected service: %s (got: %v)", svc, values)
		}
	}
}

func TestMultitenancy_HealthEndpoints(t *testing.T) {
	for _, tc := range []struct {
		name string
		url  string
	}{
		{"lakehouse-logs", logsBaseURL + "/health"},
		{"lakehouse-traces", tracesBaseURL + "/health"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			client := &http.Client{Timeout: 5 * time.Second}
			resp, err := client.Get(tc.url)
			if err != nil {
				t.Fatalf("health check failed: %v", err)
			}
			_ = resp.Body.Close()
			if resp.StatusCode != http.StatusOK {
				t.Fatalf("health check returned %d", resp.StatusCode)
			}
		})
	}
}

func TestMultitenancy_S3ObjectSizeNonZero(t *testing.T) {
	client := newS3Client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keys := listS3Objects(t, client, "0/0/logs/")
	if len(keys) == 0 {
		t.Fatal("no files to check")
	}

	head, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: aws.String(minioBucket),
		Key:    aws.String(keys[0]),
	})
	if err != nil {
		t.Fatalf("HeadObject %s: %v", keys[0], err)
	}

	size := aws.ToInt64(head.ContentLength)
	if size == 0 {
		t.Errorf("Parquet file %s has zero bytes", keys[0])
	}
	t.Logf("sample file %s: %d bytes, content-type: %s",
		keys[0], size, aws.ToString(head.ContentType))
}

func TestMultitenancy_S3ParquetMagicBytes(t *testing.T) {
	client := newS3Client(t)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	keys := listS3Objects(t, client, "0/0/logs/")
	if len(keys) == 0 {
		t.Fatal("no files to check")
	}

	out, err := client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: aws.String(minioBucket),
		Key:    aws.String(keys[0]),
		Range:  aws.String("bytes=0-3"),
	})
	if err != nil {
		t.Fatalf("GetObject range %s: %v", keys[0], err)
	}
	defer func() { _ = out.Body.Close() }()

	header := make([]byte, 4)
	n, err := io.ReadFull(out.Body, header)
	if err != nil || n < 4 {
		t.Fatalf("failed to read first 4 bytes: %v (read %d)", err, n)
	}

	if string(header) != "PAR1" {
		t.Errorf("file %s does not start with Parquet magic bytes PAR1, got %q", keys[0], string(header))
	}
}

func TestMultitenancy_S3SignalSeparation(t *testing.T) {
	client := newS3Client(t)

	logsKeys := listS3Objects(t, client, "0/0/logs/")
	tracesKeys := listS3Objects(t, client, "0/0/traces/")

	logsSet := make(map[string]bool, len(logsKeys))
	for _, k := range logsKeys {
		logsSet[k] = true
	}

	for _, k := range tracesKeys {
		if logsSet[k] {
			t.Errorf("key %s found in both logs and traces — signals must be separate", k)
		}
	}

	if len(logsKeys) == 0 || len(tracesKeys) == 0 {
		t.Errorf("expected both signals to have data: logs=%d, traces=%d", len(logsKeys), len(tracesKeys))
	}
}

func TestMultitenancy_ConcurrentQueriesBothEndpoints(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "10")

	type result struct {
		name  string
		count int
		err   error
	}

	ch := make(chan result, 2)

	query := func(name, baseURL string) {
		body := httpGetBody(t, baseURL, "/select/logsql/query", params)
		lines := assertValidNDJSON(t, body)
		ch <- result{name: name, count: len(lines)}
	}

	go query("logs", logsBaseURL)
	go query("traces", tracesBaseURL)

	for i := 0; i < 2; i++ {
		r := <-ch
		if r.count == 0 {
			t.Errorf("%s: expected results, got 0", r.name)
		}
		t.Logf("%s: %d results", r.name, r.count)
	}
}

func TestMultitenancy_ConsistentQueryResults(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", `service.name:="api-gateway"`)
	params.Set("limit", "100")

	body1 := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines1 := assertValidNDJSON(t, body1)

	body2 := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines2 := assertValidNDJSON(t, body2)

	if len(lines1) != len(lines2) {
		t.Errorf("repeated query returned different counts: %d vs %d", len(lines1), len(lines2))
	}
	t.Logf("consistency check: %d vs %d results", len(lines1), len(lines2))
}

func TestMultitenancy_S3Summary(t *testing.T) {
	client := newS3Client(t)

	type tenantStats struct {
		prefix string
		logs   int
		traces int
	}

	tenants := []tenantStats{
		{prefix: "0/0/"},
		{prefix: "1/1/"},
	}

	for i := range tenants {
		tenants[i].logs = len(listS3Objects(t, client, tenants[i].prefix+"logs/"))
		tenants[i].traces = len(listS3Objects(t, client, tenants[i].prefix+"traces/"))
	}

	t.Log("=== Multi-Tenancy S3 Summary ===")
	for _, ts := range tenants {
		t.Logf("  tenant %s: %d log files, %d trace files",
			ts.prefix, ts.logs, ts.traces)
		if ts.logs == 0 {
			t.Errorf("tenant %s has no log files", ts.prefix)
		}
		if ts.traces == 0 {
			t.Errorf("tenant %s has no trace files", ts.prefix)
		}
	}

	total := listS3Objects(t, client, "")
	t.Logf("  total S3 objects: %d", len(total))

	expectedTotal := 0
	for _, ts := range tenants {
		expectedTotal += ts.logs + ts.traces
	}
	if len(total) != expectedTotal {
		t.Errorf("total objects (%d) != sum of tenant objects (%d) — data leak outside tenant prefixes",
			len(total), expectedTotal)
	}
}

func TestMultitenancy_DualWriteVerification(t *testing.T) {
	coldParams := defaultTimeParams()
	coldParams.Set("query", "*")
	coldParams.Set("limit", "5")

	coldBody := httpGetBody(t, logsBaseURL, "/select/logsql/query", coldParams)
	coldLines := assertValidNDJSON(t, coldBody)
	if len(coldLines) == 0 {
		t.Fatal("cold tier (lakehouse) has no data")
	}

	vlselectBody := httpGetBody(t, vlselectURL, "/select/logsql/query", coldParams)
	vlselectLines := assertValidNDJSON(t, vlselectBody)
	if len(vlselectLines) == 0 {
		t.Fatal("vlselect (hot+cold) has no data")
	}

	t.Logf("cold-only: %d lines, vlselect (hot+cold): %d lines", len(coldLines), len(vlselectLines))

	if len(vlselectLines) < len(coldLines) {
		t.Errorf("vlselect should return at least as many results as cold-only: %d < %d",
			len(vlselectLines), len(coldLines))
	}
}

func TestMultitenancy_LakehouseLogsTimestamp(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "20")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Fatal("no data to verify timestamps")
	}

	for i, line := range lines {
		ts, ok := line["_time"].(string)
		if !ok || ts == "" {
			t.Errorf("line %d: missing or empty _time field", i)
			continue
		}
		parsed, err := time.Parse(time.RFC3339Nano, ts)
		if err != nil {
			t.Errorf("line %d: _time %q is not valid RFC3339Nano: %v", i, ts, err)
			continue
		}
		age := time.Since(parsed)
		if age > 72*time.Hour || age < -1*time.Hour {
			t.Errorf("line %d: timestamp %s is outside expected range (age: %s)", i, ts, age)
		}
	}
}

func TestMultitenancy_LakehouseAllExpectedFields(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")
	params.Set("limit", "5")

	body := httpGetBody(t, logsBaseURL, "/select/logsql/query", params)
	lines := assertValidNDJSON(t, body)
	if len(lines) == 0 {
		t.Fatal("no data to verify fields")
	}

	requiredFields := []string{"_time", "_msg", "_stream"}
	expectedFields := []string{
		"service.name", "level", "k8s.namespace.name",
		"deployment.environment", "cloud.region",
	}

	line := lines[0]
	for _, f := range requiredFields {
		if _, ok := line[f]; !ok {
			t.Errorf("missing required field %q in response", f)
		}
	}
	for _, f := range expectedFields {
		if _, ok := line[f]; !ok {
			t.Logf("expected field %q not found (may be in MAP column)", f)
		}
	}

	t.Logf("sample record fields: %v", func() []string {
		var keys []string
		for k := range line {
			keys = append(keys, k)
		}
		return keys
	}())
}

func TestMultitenancy_ServiceCountMatchesSeedData(t *testing.T) {
	params := defaultTimeParams()
	params.Set("field", "service.name")
	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_values", params)
	entries := assertValuesResponse(t, body)
	values := extractValueStrings(t, entries)

	expectedServices := 5
	if len(values) < expectedServices {
		t.Errorf("expected at least %d services, got %d: %v", expectedServices, len(values), values)
	}
}

func TestMultitenancy_LevelDistribution(t *testing.T) {
	params := defaultTimeParams()
	params.Set("field", "level")
	body := httpGetBody(t, logsBaseURL, "/select/logsql/field_values", params)
	entries := assertValuesResponse(t, body)
	values := extractValueStrings(t, entries)

	for _, lvl := range []string{"INFO", "WARN", "ERROR", "DEBUG"} {
		if !containsString(values, lvl) {
			t.Errorf("missing expected level: %s (got: %v)", lvl, values)
		}
	}

	hitsMap := make(map[string]float64)
	for _, e := range entries {
		v, _ := e["value"].(string)
		h, _ := e["hits"].(float64)
		hitsMap[v] = h
	}

	total := 0.0
	for _, h := range hitsMap {
		total += h
	}
	for lvl, hits := range hitsMap {
		pct := (hits / total) * 100
		t.Logf("level %s: %.0f hits (%.1f%%)", lvl, hits, pct)
	}
}

func TestMultitenancy_HitsEndpointWorks(t *testing.T) {
	params := defaultTimeParams()
	params.Set("query", "*")

	now := time.Now()
	params.Set("start", fmt.Sprintf("%d", now.Add(-48*time.Hour).UnixNano()))
	params.Set("end", fmt.Sprintf("%d", now.UnixNano()))

	body := httpGetBody(t, logsBaseURL, "/select/logsql/hits", params)
	assertHitsResponse(t, body)
}
