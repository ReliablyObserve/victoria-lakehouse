package selectapi

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	vtstorageadapter "github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vtstorage_adapter"
)

// The served Jaeger and Tempo path, end to end through the select handler, over
// a store that answers at once: what the query deadline adds to a request
// (parsing 'timeout', one context.WithTimeout and its timer) shows here and
// nowhere else. docs/read-path.md records the numbers (benchstat, -count=10).
func benchTraceRoute(b *testing.B, path string, args url.Values) {
	start := time.Now().Add(-10 * time.Minute).Truncate(time.Millisecond)
	st := &spanStore{start: start, end: start.Add(2 * time.Second)}
	vtstorageadapter.Init(st)
	mux := http.NewServeMux()
	NewHandler(st, testConfig(config.ModeTraces)).Register(mux)
	args.Set("start", strconv.FormatInt(start.Add(-time.Hour).Unix(), 10))
	args.Set("end", strconv.FormatInt(start.Add(time.Hour).Unix(), 10))
	target := path + "?" + args.Encode()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		rec := httptest.NewRecorder()
		mux.ServeHTTP(rec, httptest.NewRequest("GET", target, nil))
		if rec.Code != http.StatusOK {
			b.Fatalf("%s: %d %q", path, rec.Code, rec.Body.String())
		}
	}
}

func BenchmarkJaegerServices_Handler(b *testing.B) {
	benchTraceRoute(b, "/select/jaeger/api/services", url.Values{})
}

func BenchmarkJaegerTraceByID_Handler(b *testing.B) {
	benchTraceRoute(b, "/select/jaeger/api/traces/0af7651916cd43dd8448eb211c80319c", url.Values{})
}

func BenchmarkTempoSearch_Handler(b *testing.B) {
	benchTraceRoute(b, "/select/tempo/api/search", url.Values{"q": {"{}"}, "limit": {"5"}})
}

func BenchmarkTempoTags_Handler(b *testing.B) {
	benchTraceRoute(b, "/select/tempo/api/v2/search/tags", url.Values{})
}
