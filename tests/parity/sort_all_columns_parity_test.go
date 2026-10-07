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
// a tenant no other test reads (logs: allColumnSortLogsAccount; traces: the
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

// The case tenants: numeric AccountIDs and, for the alias form, the account its
// OrgID maps to (layer_controls_test.go). No other test reads them.
const (
	allColumnSortLogsAccount      = "7311"
	allColumnSortLogsAliasAccount = "7312"
)

// allColumnSortQueries are the sorts over all columns compared row for row.
// The last two return every row, so the whole answer (rows, their order, the
// order of their columns and JSON keys) is compared.
var allColumnSortQueries = []string{"first 2", "last 2", "sort | limit 2", "sort desc | limit 3", "sort | limit 100"}

// allColumnSortLayer is one data layer the same rows are compared in.
type allColumnSortLayer struct {
	name string
	// prepare brings the stack into the layer; it may write more rows (to hot
	// and to cold alike) and returns how many rows each tenant then holds.
	prepare func(t *testing.T, c *allColumnSortCase) int
	// prove checks, for one tenant form, that the stack really is in the layer
	// the cell claims. It runs immediately before and after the compares.
	prove func(t *testing.T, c *allColumnSortCase, f tenantForm, rows int)
}

// allColumnSortCase is one signal's fixture. Every tenant form writes and reads
// its own tenant, so the cells are layer x tenant form x signal.
type allColumnSortCase struct {
	hot, cold string // base URLs
	forms     []tenantForm
	filter    string // selects exactly the rows this case wrote
	mode      string // logs | traces
	at        time.Time
	from, to  time.Time                                              // the window around at
	write     func(t *testing.T, c *allColumnSortCase, first, n int) // n rows, numbered from first, to hot and cold, for every form
	restart   []string                                               // compose services to restart for the restart layer
	startedAt time.Time                                              // the restarted container's new StartedAt
}

func TestParity_AllColumnSortTieOrder(t *testing.T) {
	stamp := time.Now().UnixNano()
	// An hour in which none of the case tenants holds an object: the compacted
	// layer then merges exactly this case's objects, on a rerun too.
	at := pickQuietHour(t, map[string][]string{
		"logs": {allColumnSortLogsAccount, allColumnSortLogsAliasAccount}, "traces": {latencyProbeAccount, parityTracesAccount}})
	from, to := at.Add(-time.Minute), at.Add(time.Minute)

	for _, sig := range []string{"logs", "traces"} {
		t.Run(sig, func(t *testing.T) {
			t.Parallel() // the two signals use disjoint services and tenants
			c := &allColumnSortCase{at: at, from: from, to: to, mode: sig}
			var written []string
			if sig == "logs" {
				token := fmt.Sprintf("acs%d", stamp)
				c.hot, c.cold, c.filter = vlBaseURL, lhBaseURL, token
				c.forms = []tenantForm{numericTenant(allColumnSortLogsAccount), aliasTenant(allColumnSortLogsAliasAccount, parityLogsOrgID)}
				c.restart = []string{"lakehouse-logs"}
				c.write = func(t *testing.T, c *allColumnSortCase, first, n int) {
					var rows bytes.Buffer
					for i := first; i < first+n; i++ {
						fmt.Fprintf(&rows, `{"_time":%q,"_msg":"%s zz%d","svc":"svc-%d"}`+"\n",
							at.Format(time.RFC3339Nano), token, 99-i, i)
					}
					for _, f := range c.forms {
						post(t, c.hot+"/insert/jsonline?_stream_fields=svc", "application/stream+json", rows.Bytes(), f.header(false))
						post(t, c.cold+"/insert/jsonline?_stream_fields=svc", "application/stream+json", rows.Bytes(), f.header(true))
					}
				}
			} else {
				c.hot, c.cold = vtBaseURL, lhtBaseURL
				c.forms = []tenantForm{numericTenant(latencyProbeAccount), aliasTenant(parityTracesAccount, parityTracesOrgID)}
				c.restart = []string{"lakehouse-traces"}
				c.write = func(t *testing.T, c *allColumnSortCase, first, n int) {
					for i := first; i < first+n; i++ {
						id := fmt.Sprintf("%016x%016x", stamp, 0xac50+i)
						written = append(written, id)
						for _, f := range c.forms {
							pushSpanAs(t, c.hot, f.header(false), id, fmt.Sprintf("%016x", i+1), fmt.Sprintf("acs-svc-%d", i+1), at)
							pushSpanAs(t, c.cold, f.header(true), id, fmt.Sprintf("%016x", i+1), fmt.Sprintf("acs-svc-%d", i+1), at)
						}
					}
					c.filter = "trace_id:in(" + strings.Join(written, ",") + ")"
				}
			}
			before := make([]int64, len(c.forms))
			for i, f := range c.forms {
				before[i] = tenantRows(t, c.cold, f.account)
			}
			// buffered waits for the batch just written to show in every form's
			// buffer (so the wait for it to leave cannot pass before it is in).
			buffered := func(t *testing.T, c *allColumnSortCase, rows int) {
				for _, f := range c.forms {
					waitBuffered(t, c.cold, c.mode, f.account, c.from, c.to, rows)
				}
			}
			leftBuffer := func(t *testing.T, c *allColumnSortCase) {
				for _, f := range c.forms {
					waitLeftBuffer(t, c.cold, c.mode, f.account, c.from, c.to)
				}
			}

			layers := []allColumnSortLayer{
				// The rows are still in Lakehouse's insert buffer: upstream's own
				// engine answers, and it must agree with hot.
				{"buffer",
					func(t *testing.T, c *allColumnSortCase) int { c.write(t, c, 1, 6); buffered(t, c, 6); return 6 },
					func(t *testing.T, c *allColumnSortCase, f tenantForm, rows int) {
						requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, rows)
					}},
				// They have left the buffer: Parquet only. B8 lived here.
				{"parquet",
					func(t *testing.T, c *allColumnSortCase) int { leftBuffer(t, c); return 6 },
					func(t *testing.T, c *allColumnSortCase, f tenantForm, rows int) {
						requireFlushedOnly(t, c.cold, c.mode, f.account, c.at, c.from, c.to)
					}},
				// A second batch of tied rows on new streams is flushed to a second
				// object, then the two are merged by the manual recompaction trigger.
				{"compacted",
					func(t *testing.T, c *allColumnSortCase) int {
						c.write(t, c, 7, 6)
						buffered(t, c, 6)
						leftBuffer(t, c)
						recompactHourPartition(t, c.cold, c.at, 2*len(c.forms))
						return 12
					},
					func(t *testing.T, c *allColumnSortCase, f tenantForm, rows int) {
						requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, 0)
						requireCompactedOnce(t, c.mode, f.account, c.at)
					}},
				// After restart: recovered segment flushed, read from S3. A third
				// batch is written just before the restart; the restarted pod
				// recovers its segment, flushes it (proved by the new L0 being
				// written after the new StartedAt, so not by a shutdown drain)
				// and answers from S3 with a fresh manifest.
				{"restart",
					func(t *testing.T, c *allColumnSortCase) int {
						// No wait for the rows to show in the buffer first: the writes
						// are acknowledged, and the buffer flushes a segment 5 s after
						// it opened, so the restart must follow the write at once or
						// the regular flush could write it before the restart.
						c.write(t, c, 13, 6)
						c.startedAt = restartComposeServices(t, c.cold, c.mode, c.restart)
						leftBuffer(t, c)
						return 18
					},
					func(t *testing.T, c *allColumnSortCase, f tenantForm, rows int) {
						requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, 0)
						requireRecoveredSegmentFlushed(t, c.mode, f.account, c.at, c.startedAt)
					}},
			}
			for _, l := range layers {
				t.Run(l.name, func(t *testing.T) {
					rows := l.prepare(t, c)
					for _, f := range c.forms {
						t.Run(f.name, func(t *testing.T) {
							l.prove(t, c, f, rows)
							compareAllColumnSorts(t, c, f, rows)
							l.prove(t, c, f, rows)
						})
					}
				})
			}
			for i, f := range c.forms {
				waitTenantRows(t, c.cold, f.account, before[i]+18)
			}
		})
	}
}

// compareAllColumnSorts waits until both tiers return all rows of the case for
// the tenant form, then runs each sort over all columns and requires the cold
// tier to return the same answer as the hot tier, byte for byte per row: the
// same rows in the same order, with the same fields in the same JSON key order
// (the order of the columns of the block they came from). VictoriaLogs' order
// is deterministic: _time, then _stream_id, then _stream, then _msg. Hot has
// no aliases, so it is asked with the numeric IDs the alias maps to.
func compareAllColumnSorts(t *testing.T, c *allColumnSortCase, f tenantForm, rows int) {
	t.Helper()
	deadline := time.Now().Add(60 * time.Second)
	for {
		hot := allColumnSortRaw(t, c, c.hot, c.filter+" | sort | limit 100", f.header(false))
		cold := allColumnSortRaw(t, c, c.cold, c.filter+" | sort | limit 100", f.header(true))
		if len(hot) == rows && len(cold) == rows {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the %d written rows never became visible: hot has %d, cold has %d", rows, len(hot), len(cold))
		}
		time.Sleep(time.Second)
	}
	for _, q := range allColumnSortQueries {
		hot := allColumnSortRaw(t, c, c.hot, c.filter+" | "+q, f.header(false))
		cold := dropKnownColdOnlyFields(allColumnSortRaw(t, c, c.cold, c.filter+" | "+q, f.header(true)))
		if len(hot) == 0 {
			t.Fatalf("hot answered %q with no rows", q)
		}
		if len(hot) != len(cold) {
			t.Errorf("%s | %s: hot returned %d rows, cold %d", c.filter, q, len(hot), len(cold))
			continue
		}
		for i := range hot {
			if hot[i] != cold[i] {
				t.Errorf("%s | %s row %d (B8: rows sharing a _time are ordered by _stream_id on hot; field order follows the block's column order):\n hot:  %s\n cold: %s",
					c.filter, q, i, hot[i], cold[i])
				break
			}
		}
	}
}

// dropKnownColdOnlyFields removes the one field the cold logs path adds to
// every row and hot VictoriaLogs does not return: "severity_number":"0", open
// issue #274. Nothing else is dropped, so any other difference in fields or in
// their order still fails.
func dropKnownColdOnlyFields(lines []string) []string {
	out := make([]string, len(lines))
	for i, l := range lines {
		out[i] = strings.Replace(l, `,"severity_number":"0"`, "", 1)
	}
	return out
}

// allColumnSortRaw returns the raw NDJSON lines a tier answers query with, in
// answer order, so key order is part of the comparison.
func allColumnSortRaw(t *testing.T, c *allColumnSortCase, base, query string, hdr http.Header) []string {
	t.Helper()
	params := url.Values{"query": {query}, "disable_latency_offset": {"true"},
		"start": {c.from.UTC().Format(time.RFC3339Nano)}, "end": {c.to.UTC().Format(time.RFC3339Nano)}}
	r := getWith(t, base, "/select/logsql/query", params, hdr)
	if r.StatusCode != http.StatusOK {
		t.Fatalf("%s %q returned %d: %s", base, query, r.StatusCode, r.Body)
	}
	var out []string
	for _, line := range strings.Split(strings.TrimSpace(string(r.Body)), "\n") {
		if line != "" {
			out = append(out, line)
		}
	}
	return out
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
