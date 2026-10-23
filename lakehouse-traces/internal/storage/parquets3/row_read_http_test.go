package parquets3

// The same guarantee at the HTTP layer, through upstream's own select handlers
// (vlselect.RequestHandler) over the Lakehouse adapter and a storage whose S3 has
// a faulty object: an endpoint that cannot read an object it selected does not
// answer 200 with a complete-looking body. Status and body are logged per
// endpoint for the PR record (go test -v -run TestRowReadHTTP).
//
// Mirror of internal/storage/parquets3/row_read_http_test.go for spans, with the
// Jaeger and Tempo endpoints (trace-by-ID included).

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaTraces/app/vtselect"
	"github.com/VictoriaMetrics/VictoriaTraces/app/vtstorage"

	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

func rcServe(t *testing.T, fx *rcFixture) {
	t.Helper()
	vtselect.Init()
	vtstorageadapter.Init(fx.s)
	t.Cleanup(func() {
		vtstorage.SetExternalStorage(nil)
		vtselect.Stop()
	})
}

func rcGet(fx *rcFixture, path string, extra url.Values) (int, string) {
	q := url.Values{
		"start": {fx.base.Add(-time.Hour).Format(time.RFC3339)},
		"end":   {fx.base.Add(time.Hour).Format(time.RFC3339)},
	}
	for k, v := range extra {
		q[k] = v
	}
	rec := httptest.NewRecorder()
	vtselect.RequestHandler(rec, httptest.NewRequest("GET", path+"?"+q.Encode(), nil))
	return rec.Code, rec.Body.String()
}

// rcComplete is true when the answer looks like a full one: 200 and no error text
// (upstream appends the error to the body of a stream whose status was sent).
func rcComplete(code int, body string) bool {
	return code == 200 && !strings.Contains(body, "cannot execute query") && !strings.Contains(body, "cannot read")
}

func TestRowReadHTTP_UnreadableObject_IsNotAnOKAnswer(t *testing.T) {
	endpoints := []struct {
		name, path string
		params     url.Values
		status     int  // the status of an unreadable object; 0 is 502 Bad Gateway
		stream     bool // a streaming answer: 200 is possible once rows were flushed, with the error appended to the body, as upstream does
	}{
		{"query", "/select/logsql/query", url.Values{"query": {"*"}}, 0, true},
		{"query_filtered", "/select/logsql/query", url.Values{"query": {"service.name:=svc-1"}}, 0, true},
		{"stats_query", "/select/logsql/stats_query", url.Values{"query": {"* | stats count() as n"}}, 0, false},
		{"stats_query_range", "/select/logsql/stats_query_range", url.Values{"query": {"* | stats count() as n"}, "step": {"1h"}}, 0, false},
		{"hits", "/select/logsql/hits", url.Values{"query": {"*"}, "step": {"1h"}}, 0, false},
		{"field_names", "/select/logsql/field_names", url.Values{"query": {"*"}}, 0, false},
		{"field_values", "/select/logsql/field_values", url.Values{"query": {"*"}, "field": {"service.name"}}, 0, false},
		{"streams", "/select/logsql/streams", url.Values{"query": {"*"}}, 0, false},
		{"stream_ids", "/select/logsql/stream_ids", url.Values{"query": {"*"}}, 0, false},
		{"stream_field_names", "/select/logsql/stream_field_names", url.Values{"query": {"*"}}, 0, false},
		{"stream_field_values", "/select/logsql/stream_field_values", url.Values{"query": {"*"}, "field": {"service.name"}}, 0, false},
		{"jaeger_trace_by_id", "/select/jaeger/api/traces/" + rcTraceID(1, 0), url.Values{
			"start": {fmt.Sprint(time.Now().Add(-3 * time.Hour).UnixMicro())}, "end": {fmt.Sprint(time.Now().Add(time.Hour).UnixMicro())}}, http.StatusBadRequest, false},
		{"jaeger_services", "/select/jaeger/api/services", url.Values{}, http.StatusBadRequest, false},
	}
	for _, ep := range endpoints {
		t.Run(ep.name, func(t *testing.T) {
			fx := newRCFixture(t, 4, rcKeyFmt)
			rcServe(t, fx)
			began := time.Now()
			code, body := rcGet(fx, ep.path, ep.params)
			t.Logf("control took %s", time.Since(began).Round(time.Millisecond))
			if !rcComplete(code, body) || body == "" {
				t.Fatalf("control: %d %q", code, body)
			}

			fx = newRCFixture(t, 4, rcKeyFmt)
			rcServe(t, fx)
			fx.fs.setFault(fx.keys[1], accessDenied)
			began = time.Now()
			code, body = rcGet(fx, ep.path, ep.params)
			t.Logf("unreadable took %s", time.Since(began).Round(time.Millisecond))
			t.Logf("HTTP unreadable %-20s -> %d %.110q", ep.name, code, body)
			if rcComplete(code, body) {
				t.Fatalf("%s answered %d %q with an unreadable object", ep.name, code, body)
			}
			// 502 Bad Gateway, the status upstream gives a read whose storage node is
			// unavailable. Jaeger's handlers format the error with %s, which drops its
			// status: they stay 400 until an export patch of VictoriaTraces keeps it
			// (issue #773); the test pins that, it does not endorse it.
			want := ep.status
			if want == 0 {
				want = http.StatusBadGateway
			}
			if ep.stream && code == http.StatusOK && strings.Contains(body, "cannot read") {
				// Rows of the readable objects were flushed first, so the status was
				// already sent; upstream appends the error to the body and aborts.
				want = http.StatusOK
			}
			if code != want {
				t.Fatalf("%s answered %d %.120q, want %d", ep.name, code, body, want)
			}
			if strings.Contains(body, "not found") {
				// Jaeger's GetTrace answers 404 "trace not found" for a trace it cannot
				// locate: an unreadable object must not be reported as an absent trace.
				t.Fatalf("%s answered %d %q: an unreadable object reported as a missing trace", ep.name, code, body)
			}

			fx = newRCFixture(t, 4, rcKeyFmt)
			rcServe(t, fx)
			fx.fs.mock.mu.Lock()
			delete(fx.fs.mock.files, fx.keys[2]) // not the object the traces looked up are in
			fx.fs.mock.mu.Unlock()
			code, body = rcGet(fx, ep.path, ep.params)
			t.Logf("HTTP removed    %-20s -> %d %.110q", ep.name, code, body)
			if !rcComplete(code, body) {
				t.Fatalf("%s: %d %q, a removed object must stay tolerated", ep.name, code, body)
			}
		})
	}
}
