//go:build e2e

package e2e

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"testing"
	"time"
)

// /internal/buffer/query on a live stack, both binaries (#384, #383).
//
// The insert pods serve their unflushed rows to the select pods' buffer
// bridge at /internal/buffer/query. Who may read what:
//
//   - LH_PEER_AUTH_KEY empty (a stack whose pods carry no peer.auth_key): a
//     single-tenant request is answered with that tenant's rows only, and
//     all_tenants=true is refused with 403 to every caller;
//   - LH_PEER_AUTH_KEY set (the CI e2e stack, whose pods carry that peer.auth_key):
//     every request needs Authorization: Bearer <key> (401 otherwise), and
//     all_tenants=true is served with it.
//
// The rows are this test's own, ingested with a numeric AccountID/ProjectID
// and with a configured X-Scope-OrgID alias (acme-corp = 1001:0 in the e2e
// stack), and read back from the buffer and through the select path. On a
// split stack (LOGS_INSERT_URL / TRACES_INSERT_URL pointing at an insert pod,
// LOGS_BASE_URL / TRACES_BASE_URL at a select pod) the select-path read goes
// through the buffer bridge, so it proves the bridge carries the key: with
// auth on, a select pod sees every unflushed row.

var (
	logsInsertURL   = envOrDefault("LOGS_INSERT_URL", logsBaseURL)
	tracesInsertURL = envOrDefault("TRACES_INSERT_URL", tracesBaseURL)
	// bufferAuthOrgID is a configured alias of the e2e stack and its pair.
	bufferAuthOrgID = envOrDefault("LH_BUFFER_AUTH_ORGID", "acme-corp:1001:0")
)

type bufferAuthSignal struct {
	name, insertURL, selectURL, mode string
}

func bufferAuthSignals() []bufferAuthSignal {
	return []bufferAuthSignal{
		{"logs", logsInsertURL, logsBaseURL, "logs"},
		{"traces", tracesInsertURL, tracesBaseURL, "traces"},
	}
}

type bufferAuthTenant struct {
	name             string
	headers          map[string]string
	account, project uint32
}

func bufferAuthTenants(t *testing.T) []bufferAuthTenant {
	t.Helper()
	parts := strings.Split(bufferAuthOrgID, ":")
	if len(parts) != 3 {
		t.Fatalf("LH_BUFFER_AUTH_ORGID = %q, want name:account:project", bufferAuthOrgID)
	}
	a, err1 := strconv.ParseUint(parts[1], 10, 32)
	p, err2 := strconv.ParseUint(parts[2], 10, 32)
	if err1 != nil || err2 != nil {
		t.Fatalf("LH_BUFFER_AUTH_ORGID = %q: %v %v", bufferAuthOrgID, err1, err2)
	}
	return []bufferAuthTenant{
		{"numeric 3101:7", map[string]string{"AccountID": "3101", "ProjectID": "7"}, 3101, 7},
		{"orgid " + parts[0], map[string]string{"X-Scope-OrgID": parts[0]}, uint32(a), uint32(p)},
	}
}

// ingestBufferAuthRows acknowledges n rows (logs) or spans (traces) carrying
// marker as tenant ten on the insert pod.
func ingestBufferAuthRows(t *testing.T, sig bufferAuthSignal, ten bufferAuthTenant, marker string, n int) {
	t.Helper()
	var body []byte
	var path, ctype string
	now := time.Now()
	switch sig.mode {
	case "logs":
		var b bytes.Buffer
		for i := 0; i < n; i++ {
			fmt.Fprintf(&b, `{"_time":%q,"_msg":"%s-%d","service.name":"buffer-auth"}`+"\n", now.Add(-time.Duration(i)*time.Millisecond).Format(time.RFC3339Nano), marker, i)
		}
		body, path, ctype = b.Bytes(), "/insert/jsonline?_stream_fields=service.name", "application/stream+json"
	case "traces":
		var spans []string
		for i := 0; i < n; i++ {
			end := now.Add(-time.Duration(i) * time.Millisecond).UnixNano()
			spans = append(spans, fmt.Sprintf(`{"traceId":"%032x","spanId":"%016x","name":"%s-%d","kind":2,"startTimeUnixNano":"%d","endTimeUnixNano":"%d"}`,
				end, end, marker, i, end-1000, end))
		}
		body = []byte(`{"resourceSpans":[{"resource":{"attributes":[{"key":"service.name","value":{"stringValue":"buffer-auth"}}]},` +
			`"scopeSpans":[{"scope":{"name":"buffer-auth"},"spans":[` + strings.Join(spans, ",") + `]}]}]}`)
		path, ctype = "/insert/opentelemetry/v1/traces", "application/json"
	}
	req, err := http.NewRequest(http.MethodPost, sig.insertURL+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", ctype)
	for k, v := range ten.headers {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 30 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s ingest as %s: %v", sig.name, ten.name, err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("%s ingest as %s: status %d: %s", sig.name, ten.name, resp.StatusCode, b)
	}
}

// bufferRow is the part of a buffer row this test reads.
type bufferRow struct {
	AccountID uint32 `json:"account_id"`
	ProjectID uint32 `json:"project_id"`
	Body      string `json:"body"`
	SpanName  string `json:"span.name"`
	SpanID    string `json:"span_id"`
}

func (r bufferRow) text() string { return r.Body + r.SpanName }

// readBuffer asks the insert pod's /internal/buffer/query; sel is
// "all" or "<account>:<project>". It returns the status, the rows of a 200
// and the error body otherwise.
func readBuffer(t *testing.T, sig bufferAuthSignal, sel, authz string) (int, []bufferRow, string) {
	t.Helper()
	q := url.Values{
		"start":        {strconv.FormatInt(time.Now().Add(-time.Hour).UnixNano(), 10)},
		"end":          {strconv.FormatInt(time.Now().Add(time.Minute).UnixNano(), 10)},
		"mode":         {sig.mode},
		"tenant_scope": {"v1"},
	}
	if sel == "all" {
		q.Set("all_tenants", "true")
	} else {
		a, p, _ := strings.Cut(sel, ":")
		q.Set("account_id", a)
		q.Set("project_id", p)
	}
	req, err := http.NewRequest(http.MethodGet, sig.insertURL+"/internal/buffer/query?"+q.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	if authz != "" {
		req.Header.Set("Authorization", authz)
	}
	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s buffer query: %v", sig.name, err)
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, nil, strings.TrimSpace(string(b))
	}
	var rows []bufferRow
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var r bufferRow
		if err := dec.Decode(&r); err != nil {
			t.Fatalf("%s buffer query: decode: %v", sig.name, err)
		}
		rows = append(rows, r)
	}
	return resp.StatusCode, rows, ""
}

func markerRows(rows []bufferRow, marker string) []bufferRow {
	var out []bufferRow
	for _, r := range rows {
		if strings.HasPrefix(r.text(), marker+"-") && (r.SpanID != "" || r.Body != "") {
			out = append(out, r)
		}
	}
	return out
}

// selectCount counts the marker's rows through the select path as ten.
func selectCount(t *testing.T, sig bufferAuthSignal, ten bufferAuthTenant, marker string) int {
	t.Helper()
	field := "_msg"
	if sig.mode == "traces" {
		field = "name"
	}
	q := url.Values{
		"query": {fmt.Sprintf(`%s:%q* | stats count() as n`, field, marker+"-")},
		"start": {strconv.FormatInt(time.Now().Add(-time.Hour).UnixNano(), 10)},
		"end":   {strconv.FormatInt(time.Now().Add(time.Minute).UnixNano(), 10)},
	}
	if sig.mode == "traces" {
		q.Set("disable_latency_offset", "true")
	}
	req, err := http.NewRequest(http.MethodGet, sig.selectURL+"/select/logsql/query?"+q.Encode(), nil)
	if err != nil {
		t.Fatal(err)
	}
	for k, v := range ten.headers {
		req.Header.Set(k, v)
	}
	resp, err := (&http.Client{Timeout: 60 * time.Second}).Do(req)
	if err != nil {
		t.Fatalf("%s select: %v", sig.name, err)
	}
	b, _ := io.ReadAll(resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("%s select as %s: status %d: %s", sig.name, ten.name, resp.StatusCode, b)
	}
	var row struct {
		N string `json:"n"`
	}
	if s := strings.TrimSpace(string(b)); s != "" {
		if err := json.Unmarshal([]byte(strings.SplitN(s, "\n", 2)[0]), &row); err != nil {
			t.Fatalf("%s select: %v: %s", sig.name, err, b)
		}
	}
	n, _ := strconv.Atoi(row.N)
	return n
}

// waitInBuffer polls the buffer until the marker's n rows are there (rows are
// searchable in upstream storage about a second after they are acknowledged).
func waitInBuffer(t *testing.T, sig bufferAuthSignal, sel, marker string, n int) []bufferRow {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		code, rows, errBody := readBuffer(t, sig, sel, bearer())
		if code != http.StatusOK {
			t.Fatalf("%s buffer query %s with the stack's credential: %d %s", sig.name, sel, code, errBody)
		}
		got := markerRows(rows, marker)
		if len(got) >= n || time.Now().After(deadline) {
			return got
		}
		time.Sleep(time.Second)
	}
}

func TestBufferQueryAuth_Credentials(t *testing.T) {
	for _, sig := range bufferAuthSignals() {
		t.Run(sig.name, func(t *testing.T) {
			if peerAuthKey == "" {
				code, _, body := readBuffer(t, sig, "all", "")
				if code != http.StatusForbidden {
					t.Fatalf("all_tenants without a credential on a pod without a key: %d, want 403 (%s)", code, body)
				}
				code, _, _ = readBuffer(t, sig, "all", "Bearer guessed")
				if code != http.StatusForbidden {
					t.Fatalf("all_tenants with an invented key on a pod without a key: %d, want 403", code)
				}
				if code, _, body := readBuffer(t, sig, "0:0", ""); code != http.StatusOK {
					t.Fatalf("single tenant without a key configured: %d, want 200 (%s)", code, body)
				}
				return
			}
			for _, tc := range []struct{ sel, authz string }{
				{"all", ""}, {"all", "Bearer wrong-" + peerAuthKey}, {"0:0", ""}, {"0:0", "Bearer wrong"},
			} {
				if code, _, body := readBuffer(t, sig, tc.sel, tc.authz); code != http.StatusUnauthorized {
					t.Errorf("%s with Authorization %q: %d, want 401 (%s)", tc.sel, tc.authz, code, body)
				}
			}
			for _, sel := range []string{"all", "0:0"} {
				if code, _, body := readBuffer(t, sig, sel, bearer()); code != http.StatusOK {
					t.Errorf("%s with the right key: %d, want 200 (%s)", sel, code, body)
				}
			}
		})
	}
}

// Each tenant form reads exactly its own unflushed rows from the buffer and,
// through the select path, every one of them.
func TestBufferQueryAuth_TenantIsolationAndSelectSeesUnflushedRows(t *testing.T) {
	const n = 200
	for _, sig := range bufferAuthSignals() {
		t.Run(sig.name, func(t *testing.T) {
			tenants := bufferAuthTenants(t)
			markers := map[string]string{}
			for _, ten := range tenants {
				markers[ten.name] = fmt.Sprintf("bufauth-%s-%d-%d", sig.name, ten.account, time.Now().UnixNano())
				ingestBufferAuthRows(t, sig, ten, markers[ten.name], n)
			}
			for _, ten := range tenants {
				sel := fmt.Sprintf("%d:%d", ten.account, ten.project)
				got := waitInBuffer(t, sig, sel, markers[ten.name], n)
				if len(got) != n {
					t.Fatalf("%s: buffer holds %d of its %d rows", ten.name, len(got), n)
				}
				for _, other := range tenants {
					if other.name == ten.name {
						continue
					}
					if leaked := waitInBuffer(t, sig, sel, markers[other.name], 0); len(leaked) != 0 {
						t.Fatalf("%s's buffer read carries %d row(s) of %s", ten.name, len(leaked), other.name)
					}
				}
				for _, r := range got {
					if r.AccountID != ten.account || r.ProjectID != ten.project {
						t.Fatalf("%s's buffer read carries a row of %d:%d", ten.name, r.AccountID, r.ProjectID)
					}
				}
				if c := selectCount(t, sig, ten, markers[ten.name]); c != n {
					t.Errorf("%s: the select path answers %d of %d unflushed rows", ten.name, c, n)
				}
			}
			if peerAuthKey != "" {
				code, rows, body := readBuffer(t, sig, "all", bearer())
				if code != http.StatusOK {
					t.Fatalf("all_tenants with the key: %d %s", code, body)
				}
				for _, ten := range tenants {
					if c := len(markerRows(rows, markers[ten.name])); c != n {
						t.Errorf("all_tenants with the key carries %d of %s's %d rows", c, ten.name, n)
					}
				}
			}
		})
	}
}
