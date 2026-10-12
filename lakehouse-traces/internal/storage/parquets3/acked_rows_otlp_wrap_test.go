package parquets3

import (
	"bytes"
	"fmt"
	"math"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/opentelemetry"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// OTLP times are unsigned 64-bit. VictoriaTraces turns a span's end time into
// its _time and indexes the trace by start and end time (strconv.ParseInt), and
// a value above 2^63-1 ns does not parse: its index push fails, it logs "cannot
// parse endTime ..." and the span is NOT added (trace_processor.go AddRow), with
// vt_traceid_index_push_error_total counting it. Nothing reaches the Lakehouse
// insert adapter. So over OTLP a span can never arrive with a negative _time; the
// refusal of rows before the Unix epoch (storage.BeforeEpoch) is reachable in the
// traces binary through /insert/native, where the sender sets the timestamp.
// These tests pin that premise, so the docs say what is true.

func otlpSpanTimes(traceID string, start, end uint64) string {
	return fmt.Sprintf(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"wrap"}}]},`+
		`"scopeSpans":[{"scope":{"name":"s"},"spans":[{"traceId":%q,"spanId":"00f067aa0ba902b7","name":"op",`+
		`"kind":1,"startTimeUnixNano":"%d","endTimeUnixNano":"%d"}]}]}]}`, traceID, start, end)
}

func TestIngestPremise_OTLPTimesAboveInt64NeverReachTheBuffer(t *testing.T) {
	startIndexWorkers.Do(insertutil.MustStartIndexWorker)
	e := newSegEnv(t)
	vlstorage.SetInsertStorage(e.segs, e.dir)
	t.Cleanup(func() { insertutil.SetLogRowsStorage(discardRows{}) })

	now := uint64(time.Now().UnixNano())
	wrapped := uint64(math.MaxUint64) - 4_999_999_999 // 2^64 - 5e9; as int64: -5e9, 1969-12-31T23:59:55Z

	post := func(contentType string, body []byte) {
		t.Helper()
		r := httptest.NewRequest(http.MethodPost, "/insert/opentelemetry/v1/traces", bytes.NewReader(body))
		r.Header.Set("Content-Type", contentType)
		w := httptest.NewRecorder()
		if !opentelemetry.RequestHandler("/insert/opentelemetry/v1/traces", w, r) || w.Code >= 300 {
			t.Fatalf("OTLP insert: status=%d body=%s", w.Code, w.Body.String())
		}
	}
	proto := func(traceID string, start, end uint64) []byte {
		req := otelpb.ExportTraceServiceRequest{ResourceSpans: []*otelpb.ResourceSpans{{
			Resource: otelpb.Resource{Attributes: []*otelpb.KeyValue{{Key: "service.name", Value: &otelpb.AnyValue{StringValue: ptr("wrap")}}}},
			ScopeSpans: []*otelpb.ScopeSpans{{Spans: []*otelpb.Span{{
				TraceID: traceID, SpanID: "00f067aa0ba902b7", Name: "op", StartTimeUnixNano: start, EndTimeUnixNano: end,
			}}}},
		}}}
		return req.MarshalProtobuf(nil)
	}

	rows0 := e.segs.Active().Rows()
	// The control: a span with ordinary times is added.
	post("application/json", []byte(otlpSpanTimes("0123456789abcdef0123456789abcd01", now, now+1_000_000)))
	if got := e.segs.Active().Rows() - rows0; got != 1 {
		t.Fatalf("the control span added %d rows to the buffer, want 1", got)
	}
	rows1 := e.segs.Active().Rows()

	cases := []struct {
		name        string
		contentType string
		body        []byte
	}{
		{"json, end above 2^63", "application/json", []byte(otlpSpanTimes("0123456789abcdef0123456789abcd02", now, wrapped))},
		{"json, start above 2^63 (end ordinary)", "application/json", []byte(otlpSpanTimes("0123456789abcdef0123456789abcd03", wrapped, now))},
		{"protobuf, end above 2^63", "application/x-protobuf", proto("0123456789abcdef0123456789abcd04", now, wrapped)},
		{"protobuf, start above 2^63 (end ordinary)", "application/x-protobuf", proto("0123456789abcdef0123456789abcd05", wrapped, now)},
	}
	for _, c := range cases {
		post(c.contentType, c.body)
		if got := e.segs.Active().Rows() - rows1; got != 0 {
			t.Errorf("%s: VictoriaTraces handed %d rows to the insert storage, want none (it drops the span itself)", c.name, got)
		}
	}
}

// /insert/native is the path where a span can arrive with a negative _time: the
// sender sets the row's timestamp as an int64. The insert adapter refuses it,
// counts it as a span, and a span from the epoch on is added.
func TestPre1970Rows_NativeInsertIsRefusedAtTheBuffer(t *testing.T) {
	startIndexWorkers.Do(insertutil.MustStartIndexWorker)
	e := newSegEnv(t)
	vlstorage.SetInsertStorage(e.segs, e.dir)
	t.Cleanup(func() { insertutil.SetLogRowsStorage(discardRows{}) })

	before := refusedNow()
	rows0 := e.segs.Active().Rows()
	postNative(t, 1, "0123456789abcdef0123456789abcd06", time.Date(1965, 3, 1, 12, 0, 0, 0, time.UTC))
	if got := e.segs.Active().Rows() - rows0; got != 0 {
		t.Fatalf("a native span of 1965 added %d rows to the buffer, want none", got)
	}
	if got := refusedNow() - before; got != 1 {
		t.Fatalf("lakehouse_insert_rows_dropped_total{reason=too_small_timestamp} rose by %d, want 1", got)
	}
	postNative(t, 1, "0123456789abcdef0123456789abcd07", hourAgo)
	if got := e.segs.Active().Rows() - rows0; got != 1 {
		t.Fatalf("a native span of the last hour added %d rows, want 1", got)
	}
}
