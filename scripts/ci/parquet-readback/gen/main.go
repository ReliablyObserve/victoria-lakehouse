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
	DeltaColumns   []string            `json:"delta_columns"`
	DictColumns    []string            `json:"dict_columns"`
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
// compaction schedule reaches), bounded row groups, split-block blooms
// on service.name + trace_id.
func productionWriterOptions(rowGroupSize int) []parquet.WriterOption {
	return []parquet.WriterOption{
		parquet.Compression(&zstd.Codec{Level: zstd.SpeedBestCompression}),
		parquet.MaxRowsPerRowGroup(int64(rowGroupSize)),
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
		logRows[i] = schema.LogRow{
			AccountID:         uint32(i % 3),
			ProjectID:         uint32(i % 5),
			TimestampUnixNano: ts,
			Body:              fmt.Sprintf("processed request %d for user-%d in %dms", i, rng.Intn(10_000), rng.Intn(900)),
			SeverityText:      sev.text,
			SeverityNumber:    sev.num,
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
		addInt("severity_number", int64(r.SeverityNumber))
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
			StartTimeUnixNano: end - dur,
			TraceID:           hexID(rng, 16),
			SpanID:            hexID(rng, 8),
			ParentSpanID:      hexID(rng, 8),
			SpanName:          fmt.Sprintf("op-%d", i%20),
			ServiceName:       svc,
			DurationNs:        dur,
			StatusCode:        int32(i % 3),
			StatusMessage:     []string{"", "OK", "deadline exceeded"}[i%3],
			SpanKind:          int32(i%5 + 1),
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
		addInt("start_time_unix_nano", r.StartTimeUnixNano)
		addInt("duration_ns", r.DurationNs)
		addInt("status.code", int64(r.StatusCode))
		addInt("span.kind", int64(r.SpanKind))
		distinct["service.name"][r.ServiceName] = struct{}{}
		distinct["span.name"][r.SpanName] = struct{}{}
		distinct["http.method"][r.HTTPMethod] = struct{}{}
		distinct["db.system"][r.DBSystem] = struct{}{}
		distinct["_stream_id"][r.StreamID] = struct{}{}
	}
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
