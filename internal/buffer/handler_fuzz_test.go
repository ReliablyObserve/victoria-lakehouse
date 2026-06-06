package buffer

import (
	"net/http"
	"net/http/httptest"
	"net/url"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// FuzzHandlerParams stresses the buffer-bridge ServeHTTP path with arbitrary
// query-string parameters. The handler must never panic regardless of the
// inputs — any HTTP status code is acceptable.
//
// Handler surface under test (from internal/buffer/handler.go):
//
//	func (h *Handler) ServeHTTP(w http.ResponseWriter, r *http.Request)
//	  with query params: start (int64 ns), end (int64 ns), mode (logs|traces)
func FuzzHandlerParams(f *testing.F) {
	// Seed: valid happy-path params.
	f.Add("0", "1000000000", "logs", "")
	f.Add("0", "1000000000", "traces", "")
	// Seed: start > end.
	f.Add("9999999999", "1", "logs", "")
	// Seed: both zero.
	f.Add("0", "0", "logs", "")
	// Seed: both at max int64.
	f.Add("9223372036854775807", "9223372036854775807", "traces", "")
	// Seed: range larger than total observable range.
	f.Add("-9223372036854775808", "9223372036854775807", "logs", "")
	// Seed: negative timestamps.
	f.Add("-1000", "-500", "logs", "")
	// Seed: non-numeric.
	f.Add("not-a-number", "also-not", "logs", "")
	f.Add("0x1f", "1e9", "logs", "")
	// Seed: unknown mode.
	f.Add("0", "1000", "metrics", "")
	// Seed: empty mode.
	f.Add("0", "1000", "", "")
	// Seed: extremely long mode (4096 chars).
	long := make([]byte, 4096)
	for i := range long {
		long[i] = 'x'
	}
	f.Add("0", "1000", string(long), "")
	// Seed: embedded NUL / unicode / control chars in mode and timestamps.
	f.Add("0\x00", "1\x01\x02", "logs\x00\x01", "")
	f.Add("0", "1000", "тraces", "") // cyrillic т
	// Seed: with bearer auth.
	f.Add("0", "1000", "logs", "Bearer secret")
	f.Add("0", "1000", "logs", "garbage")

	store := &mockBufferStore{
		logRows: []schema.LogRow{
			{TimestampUnixNano: 100, Body: "a", ServiceName: "svc"},
			{TimestampUnixNano: 200, Body: "b", ServiceName: "svc"},
		},
		traceRows: []schema.TraceRow{
			{TimestampUnixNano: 100, TraceID: "t1", SpanName: "op1"},
		},
	}
	hNoAuth := NewHandler(store, "")
	hAuth := NewHandler(store, "secret")

	f.Fuzz(func(t *testing.T, start, end, mode, authHdr string) {
		q := url.Values{}
		q.Set("start", start)
		q.Set("end", end)
		q.Set("mode", mode)

		// url.Values.Encode handles arbitrary bytes safely; we still
		// guard NewRequest from rejecting the URL by using a literal
		// path and attaching the raw query directly.
		req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query", nil)
		req.URL.RawQuery = q.Encode()
		if authHdr != "" {
			req.Header.Set("Authorization", authHdr)
		}
		rec := httptest.NewRecorder()

		// Run both auth and no-auth flavors; neither must panic.
		hNoAuth.ServeHTTP(rec, req)
		_ = rec.Code

		rec2 := httptest.NewRecorder()
		hAuth.ServeHTTP(rec2, req)
		_ = rec2.Code
	})
}
