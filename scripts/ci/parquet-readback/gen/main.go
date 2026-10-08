// gen writes deterministic synthetic log + trace Parquet files using the
// REAL production schemas (internal/schema.LogRow / TraceRow — the delta +
// dict encoding tags ride along automatically) and the REAL production
// writer options (zstd SpeedBestCompression, MaxRowsPerRowGroup,
// split-block bloom filters on service.name + trace_id, and the
// _trace_idx KV footer on the traces file), then emits a manifest JSON
// with writer-truth aggregates (row count, int64 column sums, distinct
// counts of low-cardinality strings) for verify.py to check against BOTH
// pyarrow and duckdb.
//
// This is the multi-engine readback CI gate: every parquet
// encoding change must keep our files readable — bit-identically — by
// the standard ecosystem readers. The generator deliberately imports
// ONLY parquet-go + dependency-light internal packages (schema,
// traceindex) so the CI job can `go run` it without the VictoriaLogs/
// VictoriaTraces deps/ clones.
//
// Usage: go run ./scripts/ci/parquet-readback/gen -out /tmp/parquet-readback
package main

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"flag"
	"fmt"
	"math/big"
	"math/rand"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strings"

	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/zstd"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/traceindex"
)

// fileTruth is the writer-side ground truth verify.py compares both
// engines against.
type fileTruth struct {
	File   string `json:"file"`
	Signal string `json:"signal"`
	Rows   int64  `json:"rows"`
	// Int64Sums holds exact (big-integer) sums of the integer
	// columns — 5k realistic nanosecond timestamps overflow int64,
	// and the two engines disagree on wraparound, so the truth (and
	// the comparison in verify.py) is done in exact arithmetic.
	// big.Int marshals as a bare JSON number; Python parses it back
	// losslessly into an arbitrary-precision int.
	Int64Sums      map[string]*big.Int `json:"int64_sums"`
	DistinctCounts map[string]int64    `json:"distinct_counts"`
	// NullCounts / ZeroCounts hold, for a nullable integer column, the number
	// of rows whose cell is NULL (the field was absent) and the number whose
	// cell is an explicit 0. An engine that reads NULL as 0 (or the reverse)
	// gets one of the two wrong; sums alone cannot tell them apart.
	NullCounts   map[string]int64 `json:"null_counts,omitempty"`
	ZeroCounts   map[string]int64 `json:"zero_counts,omitempty"`
	DeltaColumns []string         `json:"delta_columns"`
	DictColumns  []string         `json:"dict_columns"`
	// SpanExtras is the writer-side truth of the span events and links
	// columns (traces file only): what verify.py recomputes with pyarrow and
	// with duckdb from the JSON in span.events_json / span.links_json.
	SpanExtras *spanExtrasTruth `json:"span_extras,omitempty"`
}

// spanExtrasTruth counts the events and links written into the two JSON blob
// columns. The columns are plain optional BYTE_ARRAY (UTF-8 JSON arrays), so
// any engine with a JSON function can answer these.
type spanExtrasTruth struct {
	SpansWithEvents   int64 `json:"spans_with_events"`
	Events            int64 `json:"events"`
	ExceptionEvents   int64 `json:"exception_events"`
	SpansWithLinks    int64 `json:"spans_with_links"`
	Links             int64 `json:"links"`
	StackTraceBytes   int64 `json:"stacktrace_bytes"`     // sum of len(event_attr:exception.stacktrace) over every event
	LinkFlagsSum      int64 `json:"link_flags_sum"`       // sum of link_flags over every link
	FirstSpanEventsAt int64 `json:"first_span_events_at"` // row index of the first span with events (a NULL-vs-value spot check)
	// Text that is not valid UTF-8 is stored reversibly: a value as the object
	// {"$bytes":"<base64>"}, a field name as the key "$b64:<base64>". External
	// readers must be able to parse and decode both.
	BytesValues        int64 `json:"bytes_values"`         // values stored as a $bytes object (any field)
	BytesValueLenTotal int64 `json:"bytes_value_len"`      // total decoded length of those values
	BytesEventNames    int64 `json:"bytes_event_names"`    // events whose event_name is a $bytes object
	BytesEventNameLen  int64 `json:"bytes_event_name_len"` // total decoded length of those names
	B64Keys            int64 `json:"b64_keys"`             // keys starting with "$b64:"
}

type manifest struct {
	RowGroupSize int         `json:"row_group_size"`
	Files        []fileTruth `json:"files"`
}

func main() {
	out := flag.String("out", "/tmp/parquet-readback", "output directory")
	rows := flag.Int("rows", 5000, "rows per file")
	rowGroupSize := flag.Int("row-group-size", 2000, "max rows per row group (production MaxRowsPerRowGroup)")
	flag.Parse()

	if err := os.MkdirAll(*out, 0o755); err != nil {
		die(err)
	}

	logTruth, err := genLogs(filepath.Join(*out, "logs.parquet"), *rows, *rowGroupSize)
	if err != nil {
		die(fmt.Errorf("logs: %w", err))
	}
	traceTruth, err := genTraces(filepath.Join(*out, "traces.parquet"), *rows, *rowGroupSize)
	if err != nil {
		die(fmt.Errorf("traces: %w", err))
	}

	m := manifest{RowGroupSize: *rowGroupSize, Files: []fileTruth{logTruth, traceTruth}}
	data, err := json.MarshalIndent(m, "", "  ")
	if err != nil {
		die(err)
	}
	manifestPath := filepath.Join(*out, "manifest.json")
	if err := os.WriteFile(manifestPath, data, 0o644); err != nil {
		die(err)
	}
	fmt.Printf("gen: wrote %s (%d rows each, row groups of %d) + %s\n",
		*out, *rows, *rowGroupSize, manifestPath)
}

// productionWriterOptions mirrors the writer/compactor option set
// (internal/storage/parquets3/writer.go + internal/compaction/
// compactor.go): zstd at SpeedBestCompression (the deepest level the
// compaction schedule reaches), bounded row groups, the Lakehouse
// created_by, split-block blooms on service.name + trace_id.
func productionWriterOptions(rowGroupSize int) []parquet.WriterOption {
	return []parquet.WriterOption{
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedBestCompression}),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
		schema.ParquetCreatedBy(),
		parquet.BloomFilters(
			parquet.SplitBlockFilter(10, "service.name"),
			parquet.SplitBlockFilter(10, "trace_id"),
		),
	}
}

func genLogs(path string, n, rowGroupSize int) (fileTruth, error) {
	rng := rand.New(rand.NewSource(42)) //nolint:gosec // deterministic test data
	severities := []struct {
		text string
		num  int32
	}{{"DEBUG", 5}, {"INFO", 9}, {"WARN", 13}, {"ERROR", 17}, {"FATAL", 21}}

	base := int64(1_760_000_000_000_000_000) // fixed epoch ns
	logRows := make([]schema.LogRow, n)
	for i := range logRows {
		sev := severities[rng.Intn(len(severities))]
		svc := fmt.Sprintf("svc-%d", i%12)
		ns := fmt.Sprintf("ns-%d", i%4)
		// Near-sorted timestamps with small jitter — matches what the
		// insert buffer actually flushes.
		ts := base + int64(i)*1_000_000 + rng.Int63n(500_000)
		// severity_number is nullable: some rows never carried it (NULL), some
		// carry an explicit 0 (OTLP UNSPECIFIED), the rest a real severity.
		var sevNum *int32
		switch {
		case i%11 == 3:
			sevNum = nil
		case i%13 == 5:
			sevNum = schema.Int32Ptr(0)
		default:
			sevNum = schema.Int32Ptr(sev.num)
		}
		logRows[i] = schema.LogRow{
			AccountID:         uint32(i % 3),
			ProjectID:         uint32(i % 5),
			TimestampUnixNano: ts,
			Body:              fmt.Sprintf("processed request %d for user-%d in %dms", i, rng.Intn(10_000), rng.Intn(900)),
			SeverityText:      sev.text,
			SeverityNumber:    sevNum,
			ServiceName:       svc,
			TraceID:           hexID(rng, 16),
			SpanID:            hexID(rng, 8),
			K8sNamespaceName:  ns,
			K8sPodName:        fmt.Sprintf("%s-pod-%d", svc, i%50),
			K8sDeploymentName: svc,
			K8sNodeName:       fmt.Sprintf("node-%d", i%8),
			DeployEnv:         []string{"prod", "staging"}[i%2],
			CloudRegion:       []string{"eu-west-1", "us-east-1"}[i%2],
			HostName:          fmt.Sprintf("host-%d", i%8),
			Stream:            fmt.Sprintf("{service.name=%q,k8s.namespace.name=%q}", svc, ns),
			StreamID:          fmt.Sprintf("stream-%04d", i%24),
			ScopeName:         fmt.Sprintf("scope-%d", i%3),
			ResourceAttributes: map[string]string{
				"telemetry.sdk.language": "go",
				"cloud.zone":             fmt.Sprintf("zone-%d", i%3),
			},
			LogAttributes: map[string]string{
				"http.path":  fmt.Sprintf("/api/v%d/items", i%5),
				"request.id": fmt.Sprintf("req-%08d", i),
			},
			ScopeAttributes: map[string]string{
				"lib.version": fmt.Sprintf("1.2.%d", i%4),
			},
		}
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf, productionWriterOptions(rowGroupSize)...)
	if _, err := w.Write(logRows); err != nil {
		return fileTruth{}, err
	}
	if err := w.Close(); err != nil {
		return fileTruth{}, err
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fileTruth{}, err
	}

	truth := fileTruth{
		File:   filepath.Base(path),
		Signal: "logs",
		Rows:   int64(n),
		Int64Sums: map[string]*big.Int{
			"timestamp_unix_nano": big.NewInt(0),
			"severity_number":     big.NewInt(0),
			"account_id":          big.NewInt(0),
			"project_id":          big.NewInt(0),
		},
		DistinctCounts: map[string]int64{},
		NullCounts:     map[string]int64{"severity_number": 0},
		ZeroCounts:     map[string]int64{"severity_number": 0},
		DeltaColumns:   taggedColumns(reflect.TypeOf(schema.LogRow{}), "delta"),
		DictColumns:    taggedColumns(reflect.TypeOf(schema.LogRow{}), "dict"),
	}
	distinct := map[string]map[string]struct{}{
		"service.name":           {},
		"severity_text":          {},
		"k8s.namespace.name":     {},
		"k8s.node.name":          {},
		"deployment.environment": {},
		"_stream_id":             {},
	}
	addInt := func(col string, v int64) {
		truth.Int64Sums[col].Add(truth.Int64Sums[col], big.NewInt(v))
	}
	for _, r := range logRows {
		addInt("timestamp_unix_nano", r.TimestampUnixNano)
		switch {
		case r.SeverityNumber == nil:
			truth.NullCounts["severity_number"]++
		default:
			addInt("severity_number", int64(*r.SeverityNumber))
			if *r.SeverityNumber == 0 {
				truth.ZeroCounts["severity_number"]++
			}
		}
		addInt("account_id", int64(r.AccountID))
		addInt("project_id", int64(r.ProjectID))
		distinct["service.name"][r.ServiceName] = struct{}{}
		distinct["severity_text"][r.SeverityText] = struct{}{}
		distinct["k8s.namespace.name"][r.K8sNamespaceName] = struct{}{}
		distinct["k8s.node.name"][r.K8sNodeName] = struct{}{}
		distinct["deployment.environment"][r.DeployEnv] = struct{}{}
		distinct["_stream_id"][r.StreamID] = struct{}{}
	}
	for col, set := range distinct {
		truth.DistinctCounts[col] = int64(len(set))
	}
	return truth, nil
}

func genTraces(path string, n, rowGroupSize int) (fileTruth, error) {
	rng := rand.New(rand.NewSource(43)) //nolint:gosec // deterministic test data
	methods := []string{"GET", "POST", "PUT", "DELETE"}
	statuses := []string{"200", "201", "404", "500"}
	dbs := []string{"postgresql", "mysql", "redis"}

	base := int64(1_760_000_000_000_000_000)
	traceRows := make([]schema.TraceRow, n)
	for i := range traceRows {
		svc := fmt.Sprintf("svc-%d", i%10)
		dur := int64(rng.Intn(2_000_000_000) + 1000)
		end := base + int64(i)*1_000_000 + rng.Int63n(500_000)
		row := schema.TraceRow{
			AccountID:         uint32(i % 3),
			ProjectID:         uint32(i % 5),
			TimestampUnixNano: end,
			StartTimeUnixNano: schema.Int64Ptr(end - dur),
			TraceID:           hexID(rng, 16),
			SpanID:            hexID(rng, 8),
			ParentSpanID:      hexID(rng, 8),
			SpanName:          fmt.Sprintf("op-%d", i%20),
			ServiceName:       svc,
			DurationNs:        schema.Int64Ptr(dur),
			StatusCode:        schema.Int32Ptr(int32(i % 3)),
			StatusMessage:     []string{"", "OK", "deadline exceeded"}[i%3],
			SpanKind:          schema.Int32Ptr(int32(i%5 + 1)),
			HTTPMethod:        methods[i%len(methods)],
			HTTPStatusCode:    statuses[i%len(statuses)],
			HTTPUrl:           fmt.Sprintf("https://api.example.com/v1/items/%d", i),
			DBSystem:          dbs[i%len(dbs)],
			DBStatement:       fmt.Sprintf("SELECT * FROM items WHERE id = %d", i),
			K8sNamespaceName:  fmt.Sprintf("ns-%d", i%4),
			K8sPodName:        fmt.Sprintf("%s-pod-%d", svc, i%50),
			K8sDeploymentName: svc,
			K8sNodeName:       fmt.Sprintf("node-%d", i%8),
			DeployEnv:         []string{"prod", "staging"}[i%2],
			CloudRegion:       []string{"eu-west-1", "us-east-1"}[i%2],
			HostName:          fmt.Sprintf("host-%d", i%8),
			Stream:            fmt.Sprintf("{service.name=%q}", svc),
			StreamID:          fmt.Sprintf("stream-%04d", i%24),
			ScopeName:         fmt.Sprintf("scope-%d", i%3),
			ResourceAttributes: map[string]string{
				"telemetry.sdk.language": "go",
				"cloud.zone":             fmt.Sprintf("zone-%d", i%3),
			},
			SpanAttributes: map[string]string{
				"http.route": fmt.Sprintf("/v1/items/:id (%d)", i%7),
				"peer.port":  fmt.Sprintf("%d", 5000+i%32),
			},
			ScopeAttributes: map[string]string{
				"lib.version": fmt.Sprintf("1.2.%d", i%4),
			},
		}
		// A row that is not a span (a service-graph edge row) never carries the
		// numeric span columns: they are NULL, not 0.
		if i%19 == 7 {
			row.StartTimeUnixNano, row.DurationNs, row.StatusCode, row.SpanKind = nil, nil, nil, nil
		}
		// Span events and links, as VictoriaTraces writes them: a span with an
		// exception carries the exception event and its stack trace, some
		// spans a log event, a few a link. Most spans have neither (NULL).
		var sub schema.SpanSubFieldCollector
		if i%7 == 0 {
			for e, name := range []string{"exception", "retry"} {
				s := fmt.Sprintf(":%d", e)
				sub.Add("event:event_time_unix_nano"+s, fmt.Sprintf("%d", end-dur/2+int64(e)))
				sub.Add("event:event_name"+s, name)
				sub.Add("event:event_dropped_attributes_count"+s, "0")
				if name == "exception" {
					sub.Add("event:event_attr:exception.type"+s, "java.io.IOException")
					sub.Add("event:event_attr:exception.stacktrace"+s, fmt.Sprintf("java.io.IOException: boom %d\n\tat a.B.c(B.java:%d)\n\tat d.E.f(E.java:%d)", i, i%500, i%300))
				} else {
					sub.Add("event:event_attr:attempt"+s, fmt.Sprintf("%d", 1+i%3))
				}
			}
		}
		if i%13 == 0 {
			sub.Add("link:link_trace_id:0", hexID(rng, 16))
			sub.Add("link:link_span_id:0", hexID(rng, 8))
			sub.Add("link:link_trace_state:0", "vendor=x")
			sub.Add("link:link_dropped_attributes_count:0", "0")
			sub.Add("link:link_flags:0", fmt.Sprintf("%d", 256*(i%2)))
			sub.Add("link:link_attr:messaging.operation:0", "process")
		}
		if i%17 == 0 {
			// An event whose name, an attribute value and an attribute name
			// are not valid UTF-8 (a producer outside OTLP, which defines
			// strings as UTF-8): stored as $bytes objects and a $b64: key.
			sub.Add("event:event_name:5", "\xff\xfe")
			sub.Add("event:event_attr:raw:5", "a\x80b")
			sub.Add("event:event_attr:k\xffey:5", "v")
		}
		sub.Apply(&row)
		// Service-graph edge rows appear sparsely in production
		// (emitted by the servicegraph background task); mirror that
		// so the optional columns carry both NULLs and values.
		if i%100 == 0 {
			row.ServiceGraphParent = svc
			row.ServiceGraphChild = fmt.Sprintf("svc-%d", (i+1)%10)
			row.ServiceGraphCallCount = fmt.Sprintf("%d", rng.Intn(100)+1)
		}
		traceRows[i] = row
	}

	opts := productionWriterOptions(rowGroupSize)
	// The production trace writer + compactor both embed the
	// _trace_idx footer KV (internal/traceindex); keep the gate file
	// shaped identically so external readers see the same footer.
	if idxData := traceindex.Marshal(traceindex.Compute(traceRows)); len(idxData) > 0 {
		opts = append(opts, parquet.KeyValueMetadata(traceindex.MetadataKey, string(idxData)))
	}

	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.TraceRow](&buf, opts...)
	if _, err := w.Write(traceRows); err != nil {
		return fileTruth{}, err
	}
	if err := w.Close(); err != nil {
		return fileTruth{}, err
	}
	if err := os.WriteFile(path, buf.Bytes(), 0o644); err != nil {
		return fileTruth{}, err
	}

	truth := fileTruth{
		File:   filepath.Base(path),
		Signal: "traces",
		Rows:   int64(n),
		Int64Sums: map[string]*big.Int{
			"timestamp_unix_nano":  big.NewInt(0),
			"start_time_unix_nano": big.NewInt(0),
			"duration_ns":          big.NewInt(0),
			"status.code":          big.NewInt(0),
			"span.kind":            big.NewInt(0),
		},
		DistinctCounts: map[string]int64{},
		NullCounts:     map[string]int64{"start_time_unix_nano": 0, "duration_ns": 0, "status.code": 0, "span.kind": 0},
		ZeroCounts:     map[string]int64{"status.code": 0},
		DeltaColumns:   taggedColumns(reflect.TypeOf(schema.TraceRow{}), "delta"),
		DictColumns:    taggedColumns(reflect.TypeOf(schema.TraceRow{}), "dict"),
	}
	distinct := map[string]map[string]struct{}{
		"service.name": {},
		"span.name":    {},
		"http.method":  {},
		"db.system":    {},
		"_stream_id":   {},
	}
	addInt := func(col string, v int64) {
		truth.Int64Sums[col].Add(truth.Int64Sums[col], big.NewInt(v))
	}
	for _, r := range traceRows {
		addInt("timestamp_unix_nano", r.TimestampUnixNano)
		if r.StartTimeUnixNano != nil {
			addInt("start_time_unix_nano", *r.StartTimeUnixNano)
		} else {
			truth.NullCounts["start_time_unix_nano"]++
		}
		if r.DurationNs != nil {
			addInt("duration_ns", *r.DurationNs)
		} else {
			truth.NullCounts["duration_ns"]++
		}
		if r.StatusCode != nil {
			addInt("status.code", int64(*r.StatusCode))
			if *r.StatusCode == 0 {
				truth.ZeroCounts["status.code"]++
			}
		} else {
			truth.NullCounts["status.code"]++
		}
		if r.SpanKind != nil {
			addInt("span.kind", int64(*r.SpanKind))
		} else {
			truth.NullCounts["span.kind"]++
		}
		distinct["service.name"][r.ServiceName] = struct{}{}
		distinct["span.name"][r.SpanName] = struct{}{}
		distinct["http.method"][r.HTTPMethod] = struct{}{}
		distinct["db.system"][r.DBSystem] = struct{}{}
		distinct["_stream_id"][r.StreamID] = struct{}{}
	}
	extras := &spanExtrasTruth{FirstSpanEventsAt: -1}
	for i := range traceRows {
		r := &traceRows[i]
		if r.EventsJSON != "" {
			extras.SpansWithEvents++
			if extras.FirstSpanEventsAt < 0 {
				extras.FirstSpanEventsAt = int64(i)
			}
			var evs []map[string]any
			if err := json.Unmarshal([]byte(r.EventsJSON), &evs); err != nil {
				return fileTruth{}, fmt.Errorf("events_json of row %d: %w", i, err)
			}
			extras.Events += int64(len(evs))
			for _, e := range evs {
				if e["event_name"] == "exception" {
					extras.ExceptionEvents++
				}
				if st, ok := e["event_attr:exception.stacktrace"].(string); ok {
					extras.StackTraceBytes += int64(len(st))
				}
				if o, ok := e["event_name"].(map[string]any); ok {
					extras.BytesEventNames++
					raw, err := base64.StdEncoding.DecodeString(o["$bytes"].(string))
					if err != nil {
						return fileTruth{}, err
					}
					extras.BytesEventNameLen += int64(len(raw))
				}
				for k, v := range e {
					if strings.HasPrefix(k, "$b64:") {
						extras.B64Keys++
					}
					if o, ok := v.(map[string]any); ok {
						raw, err := base64.StdEncoding.DecodeString(o["$bytes"].(string))
						if err != nil {
							return fileTruth{}, err
						}
						extras.BytesValues++
						extras.BytesValueLenTotal += int64(len(raw))
					}
				}
			}
		}
		if r.LinksJSON != "" {
			extras.SpansWithLinks++
			var ls []map[string]string
			if err := json.Unmarshal([]byte(r.LinksJSON), &ls); err != nil {
				return fileTruth{}, fmt.Errorf("links_json of row %d: %w", i, err)
			}
			extras.Links += int64(len(ls))
			for _, l := range ls {
				var f int64
				if _, err := fmt.Sscanf(l["link_flags"], "%d", &f); err != nil {
					return fileTruth{}, fmt.Errorf("link_flags of row %d: %w", i, err)
				}
				extras.LinkFlagsSum += f
			}
		}
	}
	truth.SpanExtras = extras
	for col, set := range distinct {
		truth.DistinctCounts[col] = int64(len(set))
	}
	return truth, nil
}

// taggedColumns extracts the parquet column names carrying the given
// struct-tag option (e.g. "dict", "delta") so the manifest reflects
// the REAL schema tags — when internal/schema gains or loses an
// encoding tag, the gate's expectations follow automatically.
func taggedColumns(t reflect.Type, option string) []string {
	var cols []string
	for i := 0; i < t.NumField(); i++ {
		tag := t.Field(i).Tag.Get("parquet")
		if tag == "" {
			continue
		}
		parts := strings.Split(tag, ",")
		for _, p := range parts[1:] {
			if p == option {
				cols = append(cols, parts[0])
			}
		}
	}
	sort.Strings(cols)
	return cols
}

func hexID(rng *rand.Rand, nbytes int) string {
	b := make([]byte, nbytes)
	rng.Read(b) //nolint:errcheck // math/rand Read never fails
	return fmt.Sprintf("%x", b)
}

func die(err error) {
	fmt.Fprintln(os.Stderr, "gen:", err)
	os.Exit(1)
}
