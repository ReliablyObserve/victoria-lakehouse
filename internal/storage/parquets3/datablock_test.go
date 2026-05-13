package parquets3

import (
	"fmt"
	"math/rand"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func testTracesStorage() *Storage {
	cfg := testConfig()
	cfg.Mode = "traces"
	return &Storage{
		cfg:      cfg,
		registry: schema.NewRegistry(schema.TracesProfile),
	}
}

// --- Regression: duplicate _time column (PR #51) ---

func TestTraceRowToFields_NoDuplicateNames(t *testing.T) {
	row := &schema.TraceRow{
		TimestampUnixNano: time.Now().UnixNano(),
		StartTimeUnixNano: time.Now().UnixNano(),
		TraceID:           "abc123",
		SpanID:            "span1",
		SpanName:          "GET /api",
		ServiceName:       "api-gw",
		DurationNs:        42000,
	}

	fields := traceRowToFields(row)
	seen := make(map[string]int)
	for i, f := range fields {
		if prev, exists := seen[f.name]; exists {
			t.Errorf("duplicate field name %q at indices %d and %d", f.name, prev, i)
		}
		seen[f.name] = i
	}
}

func TestTraceRowToFields_NoCollisionCausingRenames(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	row := &schema.TraceRow{
		TimestampUnixNano: time.Now().UnixNano(),
		StartTimeUnixNano: time.Now().UnixNano(),
		TraceID:           "abc",
		SpanID:            "s1",
		SpanName:          "op",
		ServiceName:       "svc",
	}

	fields := traceRowToFields(row)
	explicitNames := make(map[string]bool)
	for _, f := range fields {
		explicitNames[f.name] = true
	}

	for _, f := range fields {
		if s, ok := f.value.(string); ok && s == "" {
			continue
		}
		m := reg.ResolveFromParquet(f.name)
		if m != nil && m.InternalName != f.name && explicitNames[m.InternalName] {
			t.Errorf("field %q renames to %q which already exists — this creates duplicate columns", f.name, m.InternalName)
		}
	}
}

func TestLogRowToFields_NoDuplicateNames(t *testing.T) {
	row := &schema.LogRow{
		TimestampUnixNano: time.Now().UnixNano(),
		Body:              "test message",
		SeverityText:      "INFO",
		ServiceName:       "api-gw",
		TraceID:           "trace1",
	}

	fields := logRowToFields(row)
	seen := make(map[string]int)
	for i, f := range fields {
		if prev, exists := seen[f.name]; exists {
			t.Errorf("duplicate field name %q at indices %d and %d", f.name, prev, i)
		}
		seen[f.name] = i
	}
}

// --- Regression: _time must be valid RFC3339Nano ---

func TestTypedRowsToDataBlock_TimeColumnValid(t *testing.T) {
	s := testTracesStorage()

	rows := []schema.TraceRow{
		{
			TimestampUnixNano: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC).UnixNano(),
			StartTimeUnixNano: time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC).UnixNano(),
			TraceID:           "t1",
			SpanID:            "s1",
			SpanName:          "op",
			ServiceName:       "svc",
			DurationNs:        1000,
		},
	}

	db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), traceRowToFields)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}

	cols := db.GetColumns(false)
	var timeCol *struct {
		name   string
		values []string
	}
	for _, c := range cols {
		if c.Name == "_time" {
			timeCol = &struct {
				name   string
				values []string
			}{c.Name, c.Values}
			break
		}
	}
	if timeCol == nil {
		t.Fatal("_time column not found in DataBlock")
	}
	for i, v := range timeCol.values {
		if v == "" {
			t.Errorf("_time value at index %d is empty", i)
			continue
		}
		if _, err := time.Parse(time.RFC3339Nano, v); err != nil {
			t.Errorf("_time value %q at index %d is not valid RFC3339Nano: %v", v, i, err)
		}
	}
}

// --- Regression: no duplicate columns in DataBlock output ---

func TestTypedRowsToDataBlock_NoDuplicateColumns(t *testing.T) {
	s := testTracesStorage()

	rows := []schema.TraceRow{
		{
			TimestampUnixNano: time.Now().UnixNano(),
			StartTimeUnixNano: time.Now().UnixNano(),
			TraceID:           "t1",
			SpanID:            "s1",
			SpanName:          "GET /api",
			ServiceName:       "api-gw",
			DurationNs:        5000,
			StatusCode:        0,
			HTTPMethod:        "GET",
			HTTPStatusCode:    "200",
			HTTPUrl:           "http://example.com",
		},
	}

	db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), traceRowToFields)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}

	cols := db.GetColumns(false)
	seen := make(map[string]int)
	for i, c := range cols {
		if prev, exists := seen[c.Name]; exists {
			t.Errorf("duplicate column %q at DataBlock indices %d and %d", c.Name, prev, i)
		}
		seen[c.Name] = i
	}
}

func TestTypedRowsToDataBlock_DeduplicationGuard(t *testing.T) {
	s := testTracesStorage()

	toFieldsWithDup := func(r *schema.TraceRow) []field {
		return []field{
			{"_time", r.TimestampUnixNano},
			{"_time", r.TimestampUnixNano},
			{"trace_id", r.TraceID},
		}
	}

	rows := []schema.TraceRow{
		{TimestampUnixNano: time.Now().UnixNano(), TraceID: "t1"},
	}

	db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), toFieldsWithDup)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}

	cols := db.GetColumns(false)
	timeCount := 0
	for _, c := range cols {
		if c.Name == "_time" {
			timeCount++
		}
	}
	if timeCount != 1 {
		t.Errorf("expected exactly 1 _time column, got %d", timeCount)
	}
}

// --- Schema registry collision detection ---

func TestSchemaRegistry_NoInternalNameCollisions(t *testing.T) {
	profiles := []struct {
		name    string
		profile schema.Profile
	}{
		{"logs", schema.LogsProfile},
		{"traces", schema.TracesProfile},
	}

	for _, p := range profiles {
		t.Run(p.name, func(t *testing.T) {
			seen := make(map[string]string)
			for _, m := range p.profile.Promoted {
				if prev, exists := seen[m.InternalName]; exists {
					t.Errorf("internal name %q maps to both %q and %q", m.InternalName, prev, m.ParquetColumn)
				}
				seen[m.InternalName] = m.ParquetColumn
			}
		})
	}
}

func TestSchemaRegistry_RenameDoesNotCollideWithExplicitFields(t *testing.T) {
	reg := schema.NewRegistry(schema.TracesProfile)
	traceRow := &schema.TraceRow{
		TimestampUnixNano: time.Now().UnixNano(),
		StartTimeUnixNano: time.Now().UnixNano(),
		TraceID:           "t1",
		SpanID:            "s1",
		SpanName:          "op",
		ServiceName:       "svc",
	}

	fields := traceRowToFields(traceRow)
	explicitNames := make(map[string]bool)
	for _, f := range fields {
		explicitNames[f.name] = true
	}

	for _, f := range fields {
		m := reg.ResolveFromParquet(f.name)
		if m != nil && m.InternalName != f.name {
			if explicitNames[m.InternalName] {
				t.Errorf("field %q renames to %q via registry, which already exists as an explicit field — this creates duplicate columns", f.name, m.InternalName)
			}
		}
	}
}

// --- DataBlock row count consistency ---

func TestTypedRowsToDataBlock_RowCountConsistency(t *testing.T) {
	s := testTracesStorage()

	now := time.Now().UnixNano()
	rows := make([]schema.TraceRow, 50)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: now + int64(i)*1000,
			StartTimeUnixNano: now + int64(i)*1000,
			TraceID:           fmt.Sprintf("trace-%d", i),
			SpanID:            fmt.Sprintf("span-%d", i),
			SpanName:          fmt.Sprintf("op-%d", i%5),
			ServiceName:       "svc",
			DurationNs:        int64(i * 100),
		}
	}

	db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), traceRowToFields)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}

	cols := db.GetColumns(false)
	expectedRows := db.RowsCount()
	for _, c := range cols {
		if len(c.Values) != expectedRows {
			t.Errorf("column %q has %d values, expected %d (RowsCount)", c.Name, len(c.Values), expectedRows)
		}
	}
}

// --- Edge cases ---

func TestTraceRowToFields_EmptyRow(t *testing.T) {
	row := &schema.TraceRow{}
	fields := traceRowToFields(row)
	if len(fields) == 0 {
		t.Fatal("expected at least some fields from empty row")
	}

	timeFound := false
	reg := schema.NewRegistry(schema.TracesProfile)
	for _, f := range fields {
		if f.name == "_time" {
			timeFound = true
			formatted := reg.FormatField(f.name, f.value)
			if _, err := time.Parse(time.RFC3339Nano, formatted); err != nil {
				t.Errorf("_time from zero timestamp should still be valid RFC3339Nano, got %q: %v", formatted, err)
			}
		}
	}
	if !timeFound {
		t.Error("_time field must always be present")
	}
}

func TestTraceRowToFields_AllFieldsHaveValidNames(t *testing.T) {
	row := &schema.TraceRow{
		TimestampUnixNano:  time.Now().UnixNano(),
		StartTimeUnixNano:  time.Now().UnixNano(),
		TraceID:            "t1",
		SpanID:             "s1",
		ParentSpanID:       "p1",
		SpanName:           "op",
		SpanKind:           1,
		StatusCode:         0,
		StatusMessage:      "OK",
		DurationNs:         1000,
		ServiceName:        "svc",
		ScopeName:          "scope",
		DeployEnv:          "prod",
		CloudRegion:        "us-east-1",
		HostName:           "host-1",
		K8sNamespaceName:   "ns",
		K8sDeploymentName:  "deploy",
		K8sNodeName:        "node",
		HTTPMethod:         "POST",
		HTTPStatusCode:     "201",
		HTTPUrl:            "http://example.com",
		DBSystem:           "postgres",
		DBStatement:        "SELECT 1",
	}

	fields := traceRowToFields(row)
	for _, f := range fields {
		if f.name == "" {
			t.Error("field has empty name")
		}
		if strings.Contains(f.name, "\n") || strings.Contains(f.name, "\r") {
			t.Errorf("field name %q contains newline", f.name)
		}
	}
}

// --- GetTimestamps compatibility ---

func TestTypedRowsToDataBlock_GetTimestampsSucceeds(t *testing.T) {
	s := testTracesStorage()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	rows := []schema.TraceRow{
		{
			TimestampUnixNano: now.UnixNano(),
			StartTimeUnixNano: now.UnixNano(),
			TraceID:           "t1",
			SpanID:            "s1",
			SpanName:          "op",
			ServiceName:       "svc",
		},
		{
			TimestampUnixNano: now.Add(time.Second).UnixNano(),
			StartTimeUnixNano: now.Add(time.Second).UnixNano(),
			TraceID:           "t2",
			SpanID:            "s2",
			SpanName:          "op2",
			ServiceName:       "svc",
		},
	}

	db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), traceRowToFields)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}

	timestamps, ok := db.GetTimestamps(nil)
	if !ok {
		cols := db.GetColumns(false)
		var colNames []string
		for _, c := range cols {
			colNames = append(colNames, c.Name)
		}
		t.Fatalf("GetTimestamps failed — this is the exact regression from PR #51; columns: %v", colNames)
	}
	if len(timestamps) != 2 {
		t.Errorf("expected 2 timestamps, got %d", len(timestamps))
	}
	for i, ts := range timestamps {
		if ts <= 0 {
			t.Errorf("timestamp[%d] = %d, expected positive nanosecond value", i, ts)
		}
	}
}

func TestTypedRowsToDataBlock_LogsGetTimestampsSucceeds(t *testing.T) {
	s := testStorage()

	now := time.Date(2026, 5, 10, 12, 0, 0, 0, time.UTC)
	rows := []schema.LogRow{
		{
			TimestampUnixNano: now.UnixNano(),
			Body:              "msg1",
			SeverityText:      "INFO",
			ServiceName:       "svc",
		},
	}

	db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), logRowToFields)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}

	timestamps, ok := db.GetTimestamps(nil)
	if !ok {
		t.Fatal("GetTimestamps failed for logs DataBlock")
	}
	if len(timestamps) != 1 {
		t.Errorf("expected 1 timestamp, got %d", len(timestamps))
	}
}

// --- Large batch stress ---

func TestTypedRowsToDataBlock_LargeBatch(t *testing.T) {
	s := testTracesStorage()

	now := time.Now().UnixNano()
	rows := make([]schema.TraceRow, 1000)
	for i := range rows {
		rows[i] = schema.TraceRow{
			TimestampUnixNano: now + int64(i)*1_000_000,
			StartTimeUnixNano: now + int64(i)*1_000_000,
			TraceID:           fmt.Sprintf("trace-%06d", i),
			SpanID:            fmt.Sprintf("span-%06d", i),
			SpanName:          fmt.Sprintf("operation-%d", i%10),
			ServiceName:       fmt.Sprintf("service-%d", i%5),
			DurationNs:        int64(i),
			HTTPMethod:        "GET",
			HTTPStatusCode:    fmt.Sprintf("%d", 200+i%5),
		}
	}

	db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), traceRowToFields)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}

	timestamps, ok := db.GetTimestamps(nil)
	if !ok {
		t.Fatal("GetTimestamps failed on large batch")
	}
	if len(timestamps) != 1000 {
		t.Errorf("expected 1000 timestamps, got %d", len(timestamps))
	}

	cols := db.GetColumns(false)
	seen := make(map[string]bool)
	for _, c := range cols {
		if seen[c.Name] {
			t.Errorf("duplicate column %q in large batch", c.Name)
		}
		seen[c.Name] = true
	}
}

// --- Randomized stress ---

func TestTypedRowsToDataBlock_RandomizedTraceRows(t *testing.T) {
	s := testTracesStorage()
	rng := rand.New(rand.NewSource(42))

	services := []string{"api-gw", "worker", "scheduler", "db-proxy", ""}
	methods := []string{"GET", "POST", "PUT", "DELETE", ""}
	statuses := []string{"200", "404", "500", ""}

	for iter := 0; iter < 100; iter++ {
		batchSize := rng.Intn(50) + 1
		rows := make([]schema.TraceRow, batchSize)
		for i := range rows {
			rows[i] = schema.TraceRow{
				TimestampUnixNano: rng.Int63n(2_000_000_000_000_000_000),
				StartTimeUnixNano: rng.Int63n(2_000_000_000_000_000_000),
				TraceID:           fmt.Sprintf("t-%d-%d", iter, i),
				SpanID:            fmt.Sprintf("s-%d-%d", iter, i),
				SpanName:          fmt.Sprintf("op-%d", rng.Intn(20)),
				ServiceName:       services[rng.Intn(len(services))],
				DurationNs:        rng.Int63n(10_000_000),
				StatusCode:        int32(rng.Intn(3)),
				HTTPMethod:        methods[rng.Intn(len(methods))],
				HTTPStatusCode:    statuses[rng.Intn(len(statuses))],
			}
			if rng.Float32() < 0.3 {
				rows[i].ResourceAttributes = map[string]string{
					fmt.Sprintf("rk%d", rng.Intn(5)): fmt.Sprintf("rv%d", rng.Intn(100)),
				}
			}
			if rng.Float32() < 0.3 {
				rows[i].SpanAttributes = map[string]string{
					fmt.Sprintf("sk%d", rng.Intn(5)): fmt.Sprintf("sv%d", rng.Intn(100)),
				}
			}
		}

		db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), traceRowToFields)
		if db == nil {
			continue
		}

		timestamps, ok := db.GetTimestamps(nil)
		if !ok {
			t.Fatalf("iter %d: GetTimestamps failed — duplicate _time column regression", iter)
		}
		if len(timestamps) == 0 {
			t.Fatalf("iter %d: got 0 timestamps from %d rows", iter, batchSize)
		}

		cols := db.GetColumns(false)
		seen := make(map[string]bool)
		for _, c := range cols {
			if seen[c.Name] {
				t.Fatalf("iter %d: duplicate column %q", iter, c.Name)
			}
			seen[c.Name] = true
		}
	}
}

func TestTypedRowsToDataBlock_LogsNoDuplicateColumns(t *testing.T) {
	s := testStorage()

	rows := []schema.LogRow{
		{
			TimestampUnixNano: time.Now().UnixNano(),
			Body:              "hello",
			SeverityText:      "INFO",
			ServiceName:       "api-gw",
			TraceID:           "t1",
			SpanID:            "s1",
			K8sNamespaceName:  "default",
			K8sPodName:        "pod-1",
		},
	}

	db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), logRowToFields)
	if db == nil {
		t.Fatal("expected non-nil DataBlock")
	}

	cols := db.GetColumns(false)
	seen := make(map[string]int)
	for i, c := range cols {
		if prev, exists := seen[c.Name]; exists {
			t.Errorf("duplicate column %q at indices %d and %d", c.Name, prev, i)
		}
		seen[c.Name] = i
	}
}

func TestLogRowToFields_NoParquetColumnNames(t *testing.T) {
	reg := schema.NewRegistry(schema.LogsProfile)
	row := &schema.LogRow{
		TimestampUnixNano: time.Now().UnixNano(),
		Body:              "test",
		SeverityText:      "INFO",
		ServiceName:       "svc",
	}

	fields := logRowToFields(row)
	for _, f := range fields {
		if s, ok := f.value.(string); ok && s == "" {
			continue
		}
		m := reg.ResolveFromParquet(f.name)
		if m != nil && m.InternalName != f.name {
			t.Errorf("field %q is a Parquet column that maps to %q — use the internal name to avoid duplicates", f.name, m.InternalName)
		}
	}
}
