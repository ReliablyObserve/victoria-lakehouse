package parquets3

import (
	"fmt"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func FuzzExtractExactMatch(f *testing.F) {
	f.Add(`service.name:="api-gw"`, "service.name")
	f.Add(`trace_id:="abc123"`, "trace_id")
	f.Add(`service.name:"api-gw"`, "service.name")
	f.Add(`no match here`, "service.name")
	f.Add(``, "")
	f.Add(`field:="value" AND other:="x"`, "field")
	f.Add(`field:="value" AND other:="x"`, "other")
	f.Add(`field:="unclosed`, "field")
	f.Add(`field:=""`, "field")
	f.Add(`a:="b" c:="d" e:="f"`, "c")
	f.Add("\x00\x01:=\"val\"", "\x00\x01")
	f.Add(`field:="val with \"quotes\""`, "field")

	f.Fuzz(func(t *testing.T, query, fieldName string) {
		result := extractExactMatch(query, fieldName)
		_ = result
	})
}

func FuzzIsPrintable(f *testing.F) {
	f.Add([]byte("hello world"))
	f.Add([]byte{})
	f.Add([]byte{0x00})
	f.Add([]byte{0x01, 0x02, 0x03})
	f.Add([]byte{'\t', '\n', '\r'})
	f.Add([]byte{0x20, 0x7e})
	f.Add([]byte{0x1f})
	f.Add([]byte("日本語"))
	f.Add([]byte{0xff, 0xfe})

	f.Fuzz(func(t *testing.T, b []byte) {
		result := isPrintable(b)
		_ = result
	})
}

func FuzzTraceRowToDataBlock(f *testing.F) {
	f.Add(int64(1778600000000000000), int64(1778600000000000000), "trace1", "span1", "op", "svc", int64(1000), "GET", "200")
	f.Add(int64(0), int64(0), "", "", "", "", int64(0), "", "")
	f.Add(int64(-1), int64(9999999999999999), "t", "s", "o", "s", int64(-1), "POST", "500")
	f.Add(int64(1<<62), int64(1<<62), "abc", "def", "name", "service", int64(1<<40), "DELETE", "404")
	f.Add(int64(1), int64(1), "a", "b", "_time", "_time", int64(0), "_time", "0")
	f.Add(int64(1778600000000000000), int64(0), "de070d43f1448557de8f1302ff3c7417", "b4e7c7b65c447cab", "HTTP DELETE /api/v1/sessions", "api-gateway", int64(22000), "DELETE", "403")
	f.Add(int64(946684800000000000), int64(946684800000000000), "", "", "", "", int64(0), "", "")
	f.Add(int64(1609459200000000000), int64(1609459200000000000), "trace\x00id", "span\nid", "op with spaces", "svc/with/slashes", int64(1000000000), "PATCH", "")

	f.Fuzz(func(t *testing.T, tsNano, startNano int64, traceID, spanID, spanName, serviceName string, durationNs int64, method, statusCode string) {
		if tsNano < 946684800000000000 || tsNano > 2524608000000000000 {
			return
		}
		if startNano < 946684800000000000 || startNano > 2524608000000000000 {
			return
		}
		row := schema.TraceRow{
			TimestampUnixNano: tsNano,
			StartTimeUnixNano: startNano,
			TraceID:           traceID,
			SpanID:            spanID,
			SpanName:          spanName,
			ServiceName:       serviceName,
			DurationNs:        durationNs,
			HTTPMethod:        method,
			HTTPStatusCode:    statusCode,
		}

		fields := traceRowToFields(&row, nil)
		seen := make(map[string]bool)
		for _, f := range fields {
			if seen[f.name] {
				t.Errorf("duplicate field name %q", f.name)
			}
			seen[f.name] = true
		}

		if !seen["_time"] {
			t.Error("_time field missing")
		}

		cfg := testConfig()
		cfg.Mode = "traces"
		s := &Storage{
			cfg:      cfg,
			registry: schema.NewRegistry(schema.TracesProfile),
		}

		db := typedRowsToDataBlock(s, []schema.TraceRow{row}, 0, int64(^uint64(0)>>1), traceRowToFields)
		if db == nil {
			return
		}

		cols := db.GetColumns(false)
		seenCols := make(map[string]bool)
		for _, c := range cols {
			if seenCols[c.Name] {
				t.Errorf("duplicate DataBlock column %q", c.Name)
			}
			seenCols[c.Name] = true
		}

		_, ok := db.GetTimestamps(nil)
		if !ok {
			t.Error("GetTimestamps failed — _time column invalid or missing")
		}
	})
}

func FuzzLogRowToDataBlock(f *testing.F) {
	f.Add(int64(1778600000000000000), "hello world", "INFO", "api-gw", "trace1")
	f.Add(int64(0), "", "", "", "")
	f.Add(int64(-1), "error\x00binary", "ERROR", "svc", "t")
	f.Add(int64(9223372036854775807), "max int64 timestamp", "FATAL", "service.name", "trace_id")
	f.Add(int64(1), "_time", "_msg", "level", "_stream")
	f.Add(int64(1778600000000000000), "msg with\nnewlines\tand\ttabs", "INFO", "svc/with/slash", "abc123def456")

	f.Fuzz(func(t *testing.T, tsNano int64, body, severity, serviceName, traceID string) {
		if tsNano < 946684800000000000 || tsNano > 2524608000000000000 {
			return
		}
		row := schema.LogRow{
			TimestampUnixNano: tsNano,
			Body:              body,
			SeverityText:      severity,
			ServiceName:       serviceName,
			TraceID:           traceID,
		}

		fields := logRowToFields(&row, nil)
		seen := make(map[string]bool)
		for _, f := range fields {
			if seen[f.name] {
				t.Errorf("duplicate field name %q", f.name)
			}
			seen[f.name] = true
		}

		s := testStorage()
		db := typedRowsToDataBlock(s, []schema.LogRow{row}, 0, int64(^uint64(0)>>1), logRowToFields)
		if db == nil {
			return
		}

		cols := db.GetColumns(false)
		seenCols := make(map[string]bool)
		for _, c := range cols {
			if seenCols[c.Name] {
				t.Errorf("duplicate DataBlock column %q", c.Name)
			}
			seenCols[c.Name] = true
		}

		_, ok := db.GetTimestamps(nil)
		if !ok {
			t.Error("GetTimestamps failed for logs")
		}
	})
}

func FuzzTraceRowWithMapAttributes(f *testing.F) {
	f.Add("key1", "val1", "key2", "val2")
	f.Add("service.name", "collision", "trace_id", "dup")
	f.Add("_time", "override", "timestamp_unix_nano", "raw")
	f.Add("_msg", "body_override", "_stream", "stream_override")
	f.Add("span.kind", "kind_collision", "status.code", "status_collision")
	f.Add("duration", "100", "name", "span_name")
	f.Add("start_time", "2026-01-01T00:00:00Z", "start_time_unix_nano", "1000000000")
	f.Add("", "", "", "")

	f.Fuzz(func(t *testing.T, rk, rv, sk, sv string) {
		row := schema.TraceRow{
			TimestampUnixNano:  time.Now().UnixNano(),
			TraceID:            "t1",
			SpanID:             "s1",
			ResourceAttributes: map[string]string{rk: rv},
			SpanAttributes:     map[string]string{sk: sv},
		}

		cfg := testConfig()
		cfg.Mode = "traces"
		s := &Storage{
			cfg:      cfg,
			registry: schema.NewRegistry(schema.TracesProfile),
		}

		db := typedRowsToDataBlock(s, []schema.TraceRow{row}, 0, int64(^uint64(0)>>1), traceRowToFields)
		if db == nil {
			return
		}

		cols := db.GetColumns(false)
		seen := make(map[string]bool)
		for _, c := range cols {
			if seen[c.Name] {
				t.Errorf("duplicate column %q (map attrs injected collision)", c.Name)
			}
			seen[c.Name] = true
		}

		_, ok := db.GetTimestamps(nil)
		if !ok {
			t.Error("GetTimestamps failed with custom map attributes")
		}
	})
}

func FuzzMultiRowDataBlock(f *testing.F) {
	f.Add(3, int64(1778600000000000000))
	f.Add(1, int64(0))
	f.Add(50, int64(1000000000))
	f.Add(100, int64(-1))
	f.Add(200, int64(9223372036854775807))
	f.Add(1, int64(1))

	f.Fuzz(func(t *testing.T, count int, baseTs int64) {
		if count <= 0 || count > 200 {
			return
		}

		cfg := testConfig()
		cfg.Mode = "traces"
		s := &Storage{
			cfg:      cfg,
			registry: schema.NewRegistry(schema.TracesProfile),
		}

		rows := make([]schema.TraceRow, count)
		for i := range rows {
			rows[i] = schema.TraceRow{
				TimestampUnixNano: baseTs + int64(i)*1000,
				TraceID:           fmt.Sprintf("t%d", i),
				SpanID:            fmt.Sprintf("s%d", i),
				SpanName:          "op",
				ServiceName:       "svc",
			}
		}

		db := typedRowsToDataBlock(s, rows, 0, int64(^uint64(0)>>1), traceRowToFields)
		if db == nil {
			return
		}

		cols := db.GetColumns(false)
		rowCount := db.RowsCount()
		for _, c := range cols {
			if len(c.Values) != rowCount {
				t.Errorf("column %q has %d values, RowsCount=%d", c.Name, len(c.Values), rowCount)
			}
		}

		seen := make(map[string]bool)
		for _, c := range cols {
			if seen[c.Name] {
				t.Errorf("duplicate column %q", c.Name)
			}
			seen[c.Name] = true
		}
	})
}
