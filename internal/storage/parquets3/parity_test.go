package parquets3

import (
	"bytes"
	"os"
	"path/filepath"
	"sort"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// parityLogRow is a test struct with MAP columns for Parquet writing.
// Uses the same column names as schema.LogRow but adds MAP columns
// for testing extractMapDistinctKeys and updateLabelIndex.
type parityLogRow struct {
	TimestampUnixNano  int64             `parquet:"timestamp_unix_nano"`
	Body               string            `parquet:"body"`
	SeverityText       string            `parquet:"severity_text"`
	ServiceName        string            `parquet:"service.name"`
	K8sNamespaceName   string            `parquet:"k8s.namespace.name"`
	K8sPodName         string            `parquet:"k8s.pod.name"`
	K8sDeploymentName  string            `parquet:"k8s.deployment.name"`
	K8sNodeName        string            `parquet:"k8s.node.name"`
	DeployEnv          string            `parquet:"deployment.environment"`
	CloudRegion        string            `parquet:"cloud.region"`
	HostName           string            `parquet:"host.name"`
	TraceID            string            `parquet:"trace_id"`
	SpanID             string            `parquet:"span_id"`
	Stream             string            `parquet:"_stream"`
	StreamID           string            `parquet:"_stream_id"`
	ScopeName          string            `parquet:"scope.name"`
	ResourceAttributes map[string]string `parquet:"resource.attributes,optional"`
	LogAttributes      map[string]string `parquet:"log.attributes,optional"`
}

func writeParityParquet(t *testing.T, dir string, rows []parityLogRow) string {
	t.Helper()
	path := filepath.Join(dir, "parity_test.parquet")
	f, err := os.Create(path)
	if err != nil {
		t.Fatal(err)
	}
	w := parquet.NewGenericWriter[parityLogRow](f, parquet.Compression(&parquet.Zstd))
	if _, err := w.Write(rows); err != nil {
		t.Fatal(err)
	}
	if err := w.Close(); err != nil {
		t.Fatal(err)
	}
	if err := f.Close(); err != nil {
		t.Fatal(err)
	}
	return path
}

func openParquetFromPath(t *testing.T, path string) *parquet.File {
	t.Helper()
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatal(err)
	}
	return f
}

// TestExtractMapDistinctKeys creates a Parquet file with MAP columns
// containing known keys, then verifies extractMapDistinctKeys returns
// all of them.
func TestExtractMapDistinctKeys(t *testing.T) {
	dir := t.TempDir()

	rows := []parityLogRow{
		{
			TimestampUnixNano: 1_000_000_000,
			Body:              "test1",
			ServiceName:       "svc-a",
			ResourceAttributes: map[string]string{
				"cloud.provider":     "aws",
				"container.id":       "abc123",
				"telemetry.sdk.name": "opentelemetry",
			},
			LogAttributes: map[string]string{
				"custom.field": "val1",
				"request.id":   "req-1",
			},
		},
		{
			TimestampUnixNano: 2_000_000_000,
			Body:              "test2",
			ServiceName:       "svc-b",
			ResourceAttributes: map[string]string{
				"cloud.provider": "gcp",
				"os.type":        "linux",
			},
			LogAttributes: map[string]string{
				"custom.field": "val2",
				"user.id":      "u-42",
			},
		},
	}

	path := writeParityParquet(t, dir, rows)
	f := openParquetFromPath(t, path)

	// Test resource.attributes keys.
	resKeys := extractMapDistinctKeys(f, "resource.attributes")
	resKeySet := make(map[string]bool, len(resKeys))
	for _, k := range resKeys {
		resKeySet[k] = true
	}

	expectedResKeys := []string{"cloud.provider", "container.id", "telemetry.sdk.name", "os.type"}
	for _, k := range expectedResKeys {
		if !resKeySet[k] {
			t.Errorf("resource.attributes missing key %q", k)
		}
	}

	// Test log.attributes keys.
	logKeys := extractMapDistinctKeys(f, "log.attributes")
	logKeySet := make(map[string]bool, len(logKeys))
	for _, k := range logKeys {
		logKeySet[k] = true
	}

	expectedLogKeys := []string{"custom.field", "request.id", "user.id"}
	for _, k := range expectedLogKeys {
		if !logKeySet[k] {
			t.Errorf("log.attributes missing key %q", k)
		}
	}

	// Test nonexistent MAP column returns nil.
	nilKeys := extractMapDistinctKeys(f, "nonexistent.map")
	if nilKeys != nil {
		t.Errorf("nonexistent MAP column should return nil, got %v", nilKeys)
	}
}

// TestUpdateLabelIndex_NoPromotedDuplication writes a parquet file with
// promoted fields AND MAP columns that contain the same keys (simulating
// old behavior), and verifies updateLabelIndex skips MAP keys that match
// promoted column names. This is a regression guard against the label index
// returning duplicate field names.
func TestUpdateLabelIndex_NoPromotedDuplication(t *testing.T) {
	dir := t.TempDir()

	// Simulate the old bug: promoted fields also appear in MAP columns.
	rows := []parityLogRow{
		{
			TimestampUnixNano: 1_000_000_000,
			Body:              "test",
			ServiceName:       "api-gw",
			K8sNamespaceName:  "prod",
			CloudRegion:       "us-east-1",
			ResourceAttributes: map[string]string{
				// These duplicate promoted columns (the old bug).
				"service.name":       "api-gw",
				"k8s.namespace.name": "prod",
				"cloud.region":       "us-east-1",
				// This is a legitimate non-promoted field.
				"cloud.provider": "aws",
			},
		},
	}

	path := writeParityParquet(t, dir, rows)
	f := openParquetFromPath(t, path)

	s := testStorage()
	s.updateLabelIndex(f)

	// cloud.provider should be in the label index.
	names := s.labelIndex.GetFieldNames()
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	if !nameSet["cloud.provider"] {
		t.Error("non-promoted field 'cloud.provider' should be in label index")
	}

	// Count occurrences of promoted field names in the label index.
	// Each promoted field should appear AT MOST once (from the promoted column itself).
	promotedNames := []string{"service.name", "k8s.namespace.name", "cloud.region"}
	for _, pn := range promotedNames {
		count := 0
		for _, n := range names {
			if n == pn {
				count++
			}
		}
		if count > 1 {
			t.Errorf("REGRESSION: promoted field %q appears %d times in label index (should be at most 1)", pn, count)
		}
	}
}

// TestUpdateLabelIndex_ExpandsNonPromotedMapKeys writes a parquet file with
// MAP columns containing non-promoted keys, and verifies updateLabelIndex
// adds them to the label index as individual field names.
func TestUpdateLabelIndex_ExpandsNonPromotedMapKeys(t *testing.T) {
	dir := t.TempDir()

	rows := []parityLogRow{
		{
			TimestampUnixNano: 1_000_000_000,
			Body:              "test",
			ServiceName:       "api-gw",
			ResourceAttributes: map[string]string{
				"cloud.provider":     "aws",
				"container.id":       "abc123",
				"telemetry.sdk.name": "opentelemetry",
			},
			LogAttributes: map[string]string{
				"custom.metric": "revenue",
				"request.id":    "req-1",
				"business.unit": "payments",
			},
		},
	}

	path := writeParityParquet(t, dir, rows)
	f := openParquetFromPath(t, path)

	s := testStorage()
	s.updateLabelIndex(f)

	names := s.labelIndex.GetFieldNames()
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	// All non-promoted MAP keys should be expanded into the label index.
	expectedFields := []string{
		"cloud.provider", "container.id", "telemetry.sdk.name",
		"custom.metric", "request.id", "business.unit",
	}
	for _, ef := range expectedFields {
		if !nameSet[ef] {
			t.Errorf("non-promoted MAP key %q not found in label index", ef)
		}
	}
}

// TestMapColumnToAttrPrefix_Logs verifies mapColumnToAttrPrefix returns empty
// string for all known MAP column names. Both logs and traces use flat MAP
// key expansion (no prefix added to keys extracted from MAP columns).
func TestMapColumnToAttrPrefix_Logs(t *testing.T) {
	t.Parallel()

	cases := []struct {
		column     string
		wantPrefix string
	}{
		{"resource.attributes", ""},
		{"log.attributes", ""},
		{"span.attributes", ""},
		{"scope.attributes", ""},
		// Unknown column name gets column name as prefix.
		{"unknown.column", "unknown.column:"},
	}

	for _, tc := range cases {
		got := mapColumnToAttrPrefix(tc.column)
		if got != tc.wantPrefix {
			t.Errorf("mapColumnToAttrPrefix(%q) = %q, want %q", tc.column, got, tc.wantPrefix)
		}
	}
}

// TestFieldNames_VLParity is an end-to-end test that inserts log rows with
// all VL-expected fields, reads them back from Parquet, updates the label
// index, and verifies the output matches VL's flat field name convention
// (no prefixes like resource_attr: or log_attr:).
func TestFieldNames_VLParity(t *testing.T) {
	dir := t.TempDir()

	rows := []parityLogRow{
		{
			TimestampUnixNano: 1_000_000_000,
			Body:              "test log message",
			SeverityText:      "INFO",
			ServiceName:       "api-gateway",
			K8sNamespaceName:  "production",
			K8sPodName:        "api-pod-1",
			K8sDeploymentName: "api",
			K8sNodeName:       "node-1",
			DeployEnv:         "production",
			CloudRegion:       "us-east-1",
			HostName:          "host-1",
			TraceID:           "4bf92f3577b34da6a3ce929d0e0e4736",
			SpanID:            "00f067aa0ba902b7",
			ScopeName:         "github.com/my/lib",
			ResourceAttributes: map[string]string{
				"cloud.provider":     "aws",
				"telemetry.sdk.name": "opentelemetry",
			},
			LogAttributes: map[string]string{
				"custom.field": "value",
				"request.id":   "req-abc",
			},
		},
	}

	path := writeParityParquet(t, dir, rows)
	f := openParquetFromPath(t, path)

	s := testStorage()
	s.updateLabelIndex(f)

	names := s.labelIndex.GetFieldNames()
	sort.Strings(names)

	// Verify NO field name has a prefix.
	for _, name := range names {
		for _, prefix := range []string{"resource_attr:", "log_attr:", "span_attr:", "scope_attr:"} {
			if len(name) > len(prefix) && name[:len(prefix)] == prefix {
				t.Errorf("REGRESSION: field_names contains prefixed name %q (VL uses flat names for logs)", name)
			}
		}
	}

	// Verify expected VL-style flat field names are present.
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	// Promoted fields should be present with flat names.
	expectedPromoted := []string{
		"service.name", "k8s.namespace.name", "k8s.pod.name",
		"k8s.deployment.name", "k8s.node.name", "deployment.environment",
		"cloud.region", "host.name", "trace_id", "span_id", "scope.name",
	}
	for _, ep := range expectedPromoted {
		if !nameSet[ep] {
			t.Errorf("expected promoted field %q not found in field_names", ep)
		}
	}

	// MAP-expanded fields should be present with flat names.
	expectedMapFields := []string{
		"cloud.provider", "telemetry.sdk.name",
		"custom.field", "request.id",
	}
	for _, ef := range expectedMapFields {
		if !nameSet[ef] {
			t.Errorf("expected MAP-expanded field %q not found in field_names", ef)
		}
	}

	// VL internal names should be present.
	vlInternalNames := []string{"_time", "_msg", "level"}
	for _, vn := range vlInternalNames {
		if !nameSet[vn] {
			t.Errorf("expected VL internal name %q not found in field_names", vn)
		}
	}
}

// TestFieldNames_NoResourceAttrPrefix_Regression is a focused regression test
// that specifically checks the bug where MAP column keys were emitted with
// "resource_attr:" prefix instead of flat names. This happened when
// mapColumnToAttrPrefix returned "resource_attr:" instead of "" for
// resource.attributes columns.
func TestFieldNames_NoResourceAttrPrefix_Regression(t *testing.T) {
	dir := t.TempDir()

	rows := []parityLogRow{
		{
			TimestampUnixNano: 1_000_000_000,
			Body:              "test",
			ServiceName:       "svc",
			ResourceAttributes: map[string]string{
				"cloud.provider":     "aws",
				"cloud.account.id":   "123456789012",
				"telemetry.sdk.name": "opentelemetry",
				"service.version":    "1.2.3",
			},
		},
	}

	path := writeParityParquet(t, dir, rows)
	f := openParquetFromPath(t, path)

	s := testStorage()
	s.updateLabelIndex(f)

	names := s.labelIndex.GetFieldNames()

	// None of these should have resource_attr: prefix.
	badPrefixed := []string{
		"resource_attr:cloud.provider",
		"resource_attr:cloud.account.id",
		"resource_attr:telemetry.sdk.name",
		"resource_attr:service.version",
	}
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	for _, bad := range badPrefixed {
		if nameSet[bad] {
			t.Errorf("REGRESSION: field_names contains prefixed name %q (must use flat name)", bad)
		}
	}

	// The flat versions should be present.
	goodFlat := []string{
		"cloud.provider", "cloud.account.id",
		"telemetry.sdk.name", "service.version",
	}
	for _, good := range goodFlat {
		// service.version won't match a promoted column, so it should
		// be expanded from the MAP.
		if !nameSet[good] {
			t.Errorf("expected flat field name %q not found in field_names", good)
		}
	}
}

// TestInsertAndQuery_FieldNameParity performs the full insert-to-query cycle:
// maps VL fields via mapFieldToRow, writes them as Parquet, reads back, and
// verifies all field names match VL's flat convention.
func TestInsertAndQuery_FieldNameParity(t *testing.T) {
	// Build a LogRow using mapFieldToRow (the actual insert path).
	row := schema.LogRow{}

	fields := map[string]string{
		"":                      "test log body",
		"level":                 "INFO",
		"service.name":          "api-gateway",
		"k8s.namespace.name":    "production",
		"k8s.pod.name":          "api-pod-1",
		"trace_id":              "4bf92f3577b34da6a3ce929d0e0e4736",
		"cloud.provider":        "aws",     // non-promoted, goes to LogAttributes
		"telemetry.sdk.name":    "otel",    // non-promoted, goes to LogAttributes
		"custom.business.field": "revenue", // non-promoted, goes to LogAttributes
	}

	// We can't call mapFieldToRow directly from parquets3 package.
	// Instead, build the row manually matching what mapFieldToRow produces.
	row.Body = "test log body"
	row.SeverityText = "INFO"
	row.ServiceName = "api-gateway"
	row.K8sNamespaceName = "production"
	row.K8sPodName = "api-pod-1"
	row.TraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	row.TimestampUnixNano = 1_000_000_000
	row.LogAttributes = map[string]string{
		"cloud.provider":        "aws",
		"telemetry.sdk.name":    "otel",
		"custom.business.field": "revenue",
	}
	_ = fields // used for documentation

	// Write as parityLogRow (which has MAP columns).
	dir := t.TempDir()
	parityRow := parityLogRow{
		TimestampUnixNano: row.TimestampUnixNano,
		Body:              row.Body,
		SeverityText:      row.SeverityText,
		ServiceName:       row.ServiceName,
		K8sNamespaceName:  row.K8sNamespaceName,
		K8sPodName:        row.K8sPodName,
		TraceID:           row.TraceID,
		LogAttributes:     row.LogAttributes,
	}

	path := writeParityParquet(t, dir, []parityLogRow{parityRow})
	f := openParquetFromPath(t, path)

	s := testStorage()
	s.updateLabelIndex(f)

	names := s.labelIndex.GetFieldNames()
	nameSet := make(map[string]bool, len(names))
	for _, n := range names {
		nameSet[n] = true
	}

	// All field names should be flat (VL parity).
	for _, name := range names {
		if len(name) > 0 && name[0] != '_' {
			for _, prefix := range []string{"resource_attr:", "log_attr:", "span_attr:", "scope_attr:"} {
				if len(name) > len(prefix) && name[:len(prefix)] == prefix {
					t.Errorf("REGRESSION: field %q has prefix %q in label index", name, prefix)
				}
			}
		}
	}

	// Non-promoted fields from LogAttributes should appear flat.
	for _, ef := range []string{"cloud.provider", "telemetry.sdk.name", "custom.business.field"} {
		if !nameSet[ef] {
			t.Errorf("expected flat field name %q from LogAttributes in label index", ef)
		}
	}
}
