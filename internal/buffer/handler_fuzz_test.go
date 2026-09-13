package buffer

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
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

// FuzzHandlerTenantParams fuzzes the tenant-scoping parameters and asserts the
// property that matters: whatever the handler decides to answer, every row it
// emits belongs to the tenant it echoed in the tenant-scope header. It must
// never panic, and it must never emit a row of some other tenant.
func FuzzHandlerTenantParams(f *testing.F) {
	seeds := [][2]string{
		{"0", "0"}, {"1001", "0"}, {"2002", "7"},
		{"", ""}, {"0", ""}, {"", "0"},
		{"-1", "0"}, {"0", "-1"},
		{"4294967295", "4294967295"}, {"4294967296", "0"},
		{"00", "00"}, {"+1", "1"}, {" 1", "1"},
		{"0x1f", "0"}, {"1e3", "0"}, {"NaN", "0"},
		{"0\x00", "0"}, {"１", "0"}, // fullwidth digit
		{"999999999999999999999999", "0"},
	}
	for _, s := range seeds {
		f.Add(s[0], s[1], "logs", TenantScopeVersion)
		f.Add(s[0], s[1], "traces", TenantScopeVersion)
	}
	f.Add("0", "0", "logs", "")
	f.Add("0", "0", "logs", "v0")
	f.Add("all:true", "", "logs", TenantScopeVersion)
	f.Add("all:true", "0", "traces", TenantScopeVersion)
	f.Add("all:false", "", "logs", TenantScopeVersion)
	f.Add("all:TRUE", "", "logs", TenantScopeVersion)

	store := &mockBufferStore{
		logRows: []schema.LogRow{
			{TimestampUnixNano: 100, AccountID: 0, ProjectID: 0, Body: "t0"},
			{TimestampUnixNano: 150, AccountID: 1001, ProjectID: 0, Body: "t1001"},
			{TimestampUnixNano: 200, AccountID: 2002, ProjectID: 7, Body: "t2002"},
		},
		traceRows: []schema.TraceRow{
			{TimestampUnixNano: 100, AccountID: 0, ProjectID: 0, TraceID: "t0"},
			{TimestampUnixNano: 150, AccountID: 1001, ProjectID: 0, TraceID: "t1001"},
		},
	}
	h := NewHandler(store, "")

	f.Fuzz(func(t *testing.T, account, project, mode, scopeVersion string) {
		q := url.Values{}
		q.Set("start", "0")
		q.Set("end", "1000")
		q.Set("mode", mode)
		// An account value of the form "all:<x>" drives the all_tenants form
		// instead, so the fuzzer explores both selection shapes (and their
		// combinations) through the same corpus.
		if rest, ok := strings.CutPrefix(account, "all:"); ok {
			q.Set("all_tenants", rest)
			if project != "" {
				q.Set("project_id", project)
			}
		} else {
			q.Set("account_id", account)
			q.Set("project_id", project)
		}
		q.Set("tenant_scope", scopeVersion)

		req := httptest.NewRequest(http.MethodGet, "/internal/buffer/query", nil)
		req.URL.RawQuery = q.Encode()
		rec := httptest.NewRecorder()
		h.ServeHTTP(rec, req)

		if rec.Code != http.StatusOK {
			return // refused: nothing was disclosed
		}
		scope := rec.Header().Get(TenantScopeHeader)
		if scope == "" {
			t.Fatalf("handler answered 200 without declaring the tenant it filtered to (account=%q project=%q)", account, project)
		}
		if scope == AllTenantsScope {
			if q.Get("all_tenants") != "true" || q.Has("account_id") || q.Has("project_id") {
				t.Fatalf("handler widened to all tenants without an exact all_tenants=true request: %s", req.URL.RawQuery)
			}
			return // a cross-tenant answer may carry any tenant's rows
		}
		dec := json.NewDecoder(rec.Body)
		for dec.More() {
			switch mode {
			case "logs":
				var row schema.LogRow
				if err := dec.Decode(&row); err != nil {
					return
				}
				if got := fmt.Sprintf("%d:%d", row.AccountID, row.ProjectID); got != scope {
					t.Fatalf("answer declared tenant %s but carried a row of tenant %s", scope, got)
				}
			case "traces":
				var row schema.TraceRow
				if err := dec.Decode(&row); err != nil {
					return
				}
				if got := fmt.Sprintf("%d:%d", row.AccountID, row.ProjectID); got != scope {
					t.Fatalf("answer declared tenant %s but carried a row of tenant %s", scope, got)
				}
			default:
				return
			}
		}
	})
}
