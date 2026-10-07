//go:build parity

package parity

// B8 (resolved, #427): the order rows take under a sort over all columns.
//
// `sort` without `by`, and `first N` / `last N` without `by`, order rows by
// every column of the row (pipe_sort.go: "Sort by all the columns"), compared
// in the order the block lists its columns. VictoriaLogs lists _time,
// _stream_id, _stream and _msg first (blockResult.initColumnsByFilter), so
// rows that share a _time are ordered by _stream_id next. The cold tier builds
// its blocks with _msg right after _time and _stream_id further down, so it
// orders the same rows by _msg instead and keeps different rows under a limit.
//
// The seeded corpus only meets this when a _time tie lands on a limit's edge,
// which made TestParity_PipesExtended/first_pipe and last_pipe fail now and
// then. This case writes six rows that share one _time on separate streams,
// so the divergence shows on every run, for both binaries. Each write goes to
// a tenant no other test reads (logs: allColumnSortAccount; traces: the
// latency probe's tenant, which requireSeededTenants already leaves out), and
// the case returns only once the cold tier has flushed what it wrote.
//
// The rows are compared twice. First while Lakehouse still holds them in its
// insert buffer, which answers with upstream's own engine and must agree with
// hot ("buffer"). Then once they have left the buffer and are read from
// Parquet only ("parquet"): that is where B8 lived. Cold blocks listed their
// columns in map-iteration order, different on every read, so the cold order
// was random and this case passed about once in 64 runs; they now list them in
// upstream's order and the case must pass on every run.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strings"
	"testing"
	"time"
)

// allColumnSortAccount is the logs tenant this case owns.
const allColumnSortAccount = "7311"

// allColumnSortQueries are the sorts over all columns compared row for row.
var allColumnSortQueries = []string{"first 2", "last 2", "sort | limit 2"}

func TestParity_AllColumnSortTieOrder(t *testing.T) {
	stamp := time.Now().UnixNano()
	at := time.Now().Add(-97 * time.Minute)

	t.Run("logs", func(t *testing.T) {
		token := fmt.Sprintf("acs%d", stamp)
		var rows bytes.Buffer
		for i := 1; i <= 6; i++ {
			fmt.Fprintf(&rows, `{"_time":%q,"_msg":"%s zz%d","svc":"svc-%d"}`+"\n",
				at.Format(time.RFC3339Nano), token, 7-i, i)
		}
		for _, base := range []string{vlBaseURL, lhBaseURL} {
			postTenant(t, base+"/insert/jsonline?_stream_fields=svc", "application/stream+json", rows.Bytes(), allColumnSortAccount)
		}
		t.Run("buffer", func(t *testing.T) {
			compareAllColumnSorts(t, vlBaseURL, lhBaseURL, token, allColumnSortAccount, "_msg")
		})
		waitLeftBuffer(t, lhBaseURL, "logs", allColumnSortAccount, at.Add(-time.Minute), at.Add(time.Minute))
		t.Run("parquet", func(t *testing.T) {
			compareAllColumnSorts(t, vlBaseURL, lhBaseURL, token, allColumnSortAccount, "_msg")
		})
		waitTenantRows(t, lhBaseURL, allColumnSortAccount, 6)
	})

	t.Run("traces", func(t *testing.T) {
		var ids []string
		for i := 1; i <= 6; i++ {
			ids = append(ids, fmt.Sprintf("%016x%016x", stamp, 0xac50+i))
		}
		before := tenantRows(t, lhtBaseURL, latencyProbeAccount)
		for _, base := range []string{vtBaseURL, lhtBaseURL} {
			for i, id := range ids {
				pushOTLPSpan(t, base, id, fmt.Sprintf("%016x", i+1), fmt.Sprintf("acs-svc-%d", i+1), at)
			}
		}
		filter := "trace_id:in(" + strings.Join(ids, ",") + ")"
		t.Run("buffer", func(t *testing.T) {
			compareAllColumnSorts(t, vtBaseURL, lhtBaseURL, filter, latencyProbeAccount, "trace_id")
		})
		waitLeftBuffer(t, lhtBaseURL, "traces", latencyProbeAccount, at.Add(-time.Minute), at.Add(time.Minute))
		t.Run("parquet", func(t *testing.T) {
			compareAllColumnSorts(t, vtBaseURL, lhtBaseURL, filter, latencyProbeAccount, "trace_id")
		})
		waitTenantRows(t, lhtBaseURL, latencyProbeAccount, before+int64(len(ids)))
	})
}

// compareAllColumnSorts waits until both tiers return all six rows of filter,
// then runs each sort over all columns and requires the cold tier to return
// the same rows in the same order as the hot tier. VictoriaLogs' order here is
// deterministic: _time, then _stream_id, then _stream, then _msg.
func compareAllColumnSorts(t *testing.T, hotBase, coldBase, filter, account, field string) {
	t.Helper()
	deadline := time.Now().Add(30 * time.Second)
	for {
		hot := allColumnSortRows(t, hotBase, filter, account, field)
		cold := allColumnSortRows(t, coldBase, filter, account, field)
		if len(hot) == 6 && len(cold) == 6 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the six written rows never became visible: hot=%v cold=%v", hot, cold)
		}
		time.Sleep(time.Second)
	}
	for _, q := range allColumnSortQueries {
		hot := allColumnSortRows(t, hotBase, filter+" | "+q, account, field)
		cold := allColumnSortRows(t, coldBase, filter+" | "+q, account, field)
		if len(hot) != 2 {
			t.Fatalf("hot answered %q with %d rows, want 2: %v", q, len(hot), hot)
		}
		if strings.Join(hot, ",") != strings.Join(cold, ",") {
			t.Errorf("%s | %s: hot=%v cold=%v (B8: rows sharing a _time are ordered by _stream_id on hot, by another column on cold)", filter, q, hot, cold)
		}
	}
}

// allColumnSortRows returns field of every row a tier answers query with, in
// answer order.
func allColumnSortRows(t *testing.T, base, query, account, field string) []string {
	t.Helper()
	params := url.Values{"query": {query}, "disable_latency_offset": {"true"}}
	r := tenantFetch(t, base, "/select/logsql/query", params, account, "0")
	if r.StatusCode != http.StatusOK {
		t.Fatalf("%s %q returned %d: %s", base, query, r.StatusCode, r.Body)
	}
	var out []string
	for _, row := range parseNDJSON(r.Body) {
		v, _ := row[field].(string)
		out = append(out, v)
	}
	return out
}

// postTenant POSTs body to u as tenant account:0 and requires a 2xx answer.
func postTenant(t *testing.T, u, contentType string, body []byte, account string) {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, u, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	req.Header.Set("AccountID", account)
	req.Header.Set("ProjectID", "0")
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("POST %s: %v", u, err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		t.Fatalf("POST %s: status %d", u, resp.StatusCode)
	}
}

// tenantRows returns the row total the cold manifest records for account:0.
func tenantRows(t *testing.T, coldBase, account string) int64 {
	t.Helper()
	r := fetch(t, coldBase, "/lakehouse/api/v1/tenants", nil)
	if r.StatusCode != http.StatusOK {
		return 0
	}
	var d struct {
		Tenants []tenantSummary `json:"tenants"`
	}
	if json.Unmarshal(r.Body, &d) != nil {
		return 0
	}
	for _, te := range d.Tenants {
		if te.AccountID == account && te.ProjectID == "0" {
			return te.TotalRows
		}
	}
	return 0
}

// waitTenantRows returns once the cold manifest records at least want rows for
// account:0, so the flush of this case's rows is over before a later test reads
// the manifest.
func waitTenantRows(t *testing.T, coldBase, account string, want int64) {
	t.Helper()
	deadline := time.Now().Add(45 * time.Second)
	for time.Now().Before(deadline) {
		if tenantRows(t, coldBase, account) >= want {
			return
		}
		time.Sleep(time.Second)
	}
	t.Logf("the cold manifest did not record %d rows for tenant %s:0 within 45s; a later manifest read may see this case's flush", want, account)
}
