package parquets3

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/nativeinsert"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/opentelemetry"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage/netinsert"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"
	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// The premise of the lh.trace_id_hex attestation: VictoriaTraces does NOT write
// only hex trace ids. Its OTLP/HTTP JSON path stores `traceId` as sent and
// /insert/native stores whatever the row carries. These tests drive
// VictoriaTraces' own insert handlers into the Lakehouse insert buffer, drain
// it to Parquet with the production writer, and check that every object holding
// a non-hex id says lh.trace_id_hex=0 (so a phrase is never pruned there as a
// value), while hex-only objects say 1.

func otlpJSONSpan(traceID string, ts time.Time) string {
	return fmt.Sprintf(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"premise"}}]},`+
		`"scopeSpans":[{"scope":{"name":"s"},"spans":[{"traceId":%q,"spanId":"00f067aa0ba902b7","name":"op",`+
		`"kind":1,"startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`, traceID, ts.UnixNano(), ts.Add(time.Millisecond).UnixNano())
}

func postOTLPJSON(t *testing.T, account int, body string) {
	t.Helper()
	r := httptest.NewRequest(http.MethodPost, "/insert/opentelemetry/v1/traces", bytes.NewBufferString(body))
	r.Header.Set("Content-Type", "application/json")
	r.Header.Set("AccountID", fmt.Sprint(account))
	w := httptest.NewRecorder()
	if !opentelemetry.RequestHandler("/insert/opentelemetry/v1/traces", w, r) || w.Code >= 300 {
		t.Fatalf("OTLP JSON insert: handled=%v status=%d body=%s", true, w.Code, w.Body.String())
	}
}

// postOTLPProtobuf sends one span over OTLP/HTTP protobuf, where trace_id is
// bytes: a hex id is sent as its bytes, anything else as its raw bytes.
func postOTLPProtobuf(t *testing.T, account int, traceID string, ts time.Time) {
	t.Helper()
	req := otelpb.ExportTraceServiceRequest{ResourceSpans: []*otelpb.ResourceSpans{{
		Resource: otelpb.Resource{Attributes: []*otelpb.KeyValue{{Key: "service.name", Value: &otelpb.AnyValue{StringValue: ptr("premise")}}}},
		ScopeSpans: []*otelpb.ScopeSpans{{Spans: []*otelpb.Span{{
			TraceID: traceID, SpanID: "00f067aa0ba902b7", Name: "op",
			StartTimeUnixNano: uint64(ts.UnixNano()), EndTimeUnixNano: uint64(ts.Add(time.Millisecond).UnixNano()),
		}}}},
	}}}
	r := httptest.NewRequest(http.MethodPost, "/insert/opentelemetry/v1/traces", bytes.NewReader(req.MarshalProtobuf(nil)))
	r.Header.Set("Content-Type", "application/x-protobuf")
	r.Header.Set("AccountID", fmt.Sprint(account))
	w := httptest.NewRecorder()
	if !opentelemetry.RequestHandler("/insert/opentelemetry/v1/traces", w, r) || w.Code >= 300 {
		t.Fatalf("OTLP protobuf insert: status=%d body=%s", w.Code, w.Body.String())
	}
}

func ptr[T any](v T) *T { return &v }

func postNative(t *testing.T, account int, traceID string, ts time.Time) {
	t.Helper()
	ir := logstorage.GetInsertRow()
	defer logstorage.PutInsertRow(ir)
	ir.Timestamp = ts.UnixNano()
	// A native row carries its stream as canonical stream tags (vtagent sends
	// what the upstream node computed); a row without valid tags is skipped.
	st := logstorage.GetStreamTags()
	st.Add("resource_attr:service.name", "premise")
	st.Add("name", "op")
	ir.StreamTagsCanonical = string(st.MarshalCanonical(nil))
	logstorage.PutStreamTags(st)
	ir.Fields = []logstorage.Field{
		{Name: "resource_attr:service.name", Value: "premise"},
		{Name: "name", Value: "op"},
		{Name: "span_id", Value: "00f067aa0ba902b7"},
		{Name: "trace_id", Value: traceID},
	}
	body := ir.Marshal(nil)
	r := httptest.NewRequest(http.MethodPost, "/insert/native?version="+netinsert.ProtocolVersion, bytes.NewReader(body))
	r.Header.Set("AccountID", fmt.Sprint(account))
	w := httptest.NewRecorder()
	nativeinsert.RequestHandler(w, r)
	if w.Code >= 300 {
		t.Fatalf("native insert: status=%d body=%s", w.Code, w.Body.String())
	}
}

// stored is the trace_id VictoriaTraces keeps for an id sent on path: the
// OTLP/HTTP JSON path lowercases the string it was sent and keeps it otherwise
// (hyphens, '/', '=' and all); /insert/native keeps it byte for byte; OTLP
// protobuf hex-encodes the id's bytes.
func stored(path, id string) string {
	switch path {
	case "otlp-json":
		return strings.ToLower(id)
	case "otlp-proto":
		b, err := hex.DecodeString(id)
		if err != nil {
			b = []byte(id)
		}
		return hex.EncodeToString(b)
	}
	return id
}

// discardRows is the insert storage left behind after the test: VictoriaTraces'
// trace-id index workers keep flushing index rows on a timer.
type discardRows struct{}

func (discardRows) MustAddRows(*logstorage.LogRows) {}
func (discardRows) CanWriteData() error             { return nil }
func (discardRows) IsLocalStorage() bool            { return true }

// startIndexWorkers starts VictoriaTraces' trace-id index workers once per test
// binary, as vtinsert.Init does at startup; its insert handlers need them.
var startIndexWorkers sync.Once

func TestIngestPremise_NonHexTraceIDsAreNotAttested(t *testing.T) {
	startIndexWorkers.Do(insertutil.MustStartIndexWorker)
	e := newSegEnv(t)
	vlstorage.SetInsertStorage(e.segs, e.dir)
	t.Cleanup(func() { insertutil.SetLogRowsStorage(discardRows{}) })
	ts := hourAgo.Add(10 * time.Minute)

	// account -> (trace id as sent, want attested)
	type send struct {
		path, id string
		want     bool
	}
	sends := map[int]send{
		1: {"otlp-json", "abc-def-ghi", false},
		2: {"otlp-json", "4bf92f35-77b3-4da6-a3ce-929d0e0bf736", false},
		3: {"otlp-json", "S/kvNXezTaajzpKdDvc2Aw==", false},
		4: {"otlp-json", "4bf92f3577b34da6a3ce929d0e0bf736", true}, // control: hex as sent
		5: {"otlp-json", "4bf92f35", true},                         // short hex is still one hex token
		6: {"native", "abc-def-ghi", false},
		7: {"native", "4bf92f35-77b3-4da6-a3ce-929d0e0bf736", false},
		8: {"native", "0af7651916cd43dd8448eb211c80319c", true},
		// OTLP protobuf carries the id as bytes and VictoriaTraces writes it as
		// hex, whatever the bytes are: these files keep the trace-by-ID speed-up.
		9:  {"otlp-proto", "4bf92f3577b34da6a3ce929d0e0bf736", true},
		10: {"otlp-proto", "abc-def-ghi", true},
	}
	for account, s := range sends {
		switch s.path {
		case "native":
			postNative(t, account, s.id, ts)
		case "otlp-proto":
			postOTLPProtobuf(t, account, s.id, ts)
		default:
			postOTLPJSON(t, account, otlpJSONSpan(s.id, ts))
		}
	}
	e.seal()
	e.drainAll(e.flusher(1000))

	seen := map[string]bool{}
	for _, k := range e.storedDataKeys() {
		e.u.mu.Lock()
		data := e.u.data[k]
		e.u.mu.Unlock()
		r := parquet.NewGenericReader[schema.TraceRow](bytes.NewReader(data))
		rows := make([]schema.TraceRow, r.NumRows())
		n, _ := r.Read(rows)
		_ = r.Close()
		var spans []schema.TraceRow
		for _, row := range rows[:n] {
			if row.SpanID != "" {
				spans = append(spans, row)
			}
		}
		if len(spans) == 0 {
			continue
		}
		id := spans[0].TraceID
		var want, found bool
		for _, s := range sends {
			if stored(s.path, s.id) == id {
				want, found = s.want, true
				seen[s.id] = true
			}
		}
		if !found {
			t.Fatalf("%s: stored trace_id %q is not any id that was sent (the premise says ids are stored as sent)", k, id)
		}
		pf, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
		if err != nil {
			t.Fatal(err)
		}
		kv, _ := pf.Lookup(schema.TraceIDHexMetaKey)
		if (kv == "1") != want {
			t.Errorf("%s (trace_id %q): lh.trace_id_hex=%q, want attested=%v", k, id, kv, want)
		}
		if fi, ok := e.m.GetFileByKey(k); !ok || fi.TraceIDHex != want {
			t.Errorf("%s (trace_id %q): manifest TraceIDHex=%v, want %v", k, id, fi.TraceIDHex, want)
		}
	}
	for _, s := range sends {
		if !seen[s.id] {
			t.Errorf("%s id %q never reached Parquet", s.path, s.id)
		}
	}
}
