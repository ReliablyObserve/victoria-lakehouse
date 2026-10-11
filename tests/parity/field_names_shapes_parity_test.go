//go:build parity

package parity

// field_names credits a field with the matching rows of the stored block that
// lists it, and a stream that reached hot in batches of DIFFERENT shapes is one
// block once hot has merged its parts: a column that only the first batch
// carries is listed for the whole stream, and a filter that selects only the
// later batches still credits it (issue 480, review of PR 487). The cases write
// a stream whose first batch carries a field the later ones lack, ask hot to
// merge (POST /internal/force_merge, polled until the merge is done: the merge
// is asynchronous and the time hot takes to converge on its own varies), and
// compare hot with Lakehouse for the whole window, for the rows without the
// field, for the rows with it and for the rows of the later batch alone, in the
// insert buffer and in Parquet objects, for both binaries and both tenant forms.
//
// The tenants and the layer helpers are the ones of TestParity_FieldNames_Layers.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"testing"
	"time"
)

// forceMergeHot merges the parts of the partition of at on the hot instance and
// returns once the merge is done: the endpoint answers at once and merges in the
// background, so the instance's active-merges gauge is polled until it has been
// zero three times in a row.
func forceMergeHot(t *testing.T, hot, mode string, at time.Time) {
	t.Helper()
	gauge := "vl_active_force_merges"
	if mode == "traces" {
		gauge = "vt_active_force_merges"
	}
	post(t, hot+"/internal/force_flush", "text/plain", nil, http.Header{})
	post(t, hot+"/internal/force_merge?partition_prefix="+at.UTC().Format("20060102"), "text/plain", nil, http.Header{})
	deadline := time.Now().Add(2 * time.Minute)
	time.Sleep(time.Second) // the gauge is raised by the merge goroutine, not by the request
	for idle := 0; idle < 3; {
		if time.Now().After(deadline) {
			t.Fatalf("%s: the forced merge on %s did not finish in 2 minutes", gauge, hot)
		}
		if n := hotGaugeValue(t, hot, gauge); n == 0 {
			idle++
		} else {
			idle = 0
		}
		time.Sleep(500 * time.Millisecond)
	}
}

// hotGaugeValue reads one unlabelled metric from the instance's /metrics.
func hotGaugeValue(t *testing.T, base, name string) float64 {
	t.Helper()
	resp, err := httpClient.Get(base + "/metrics")
	if err != nil {
		t.Fatalf("GET %s/metrics: %v", base, err)
	}
	defer func() { _ = resp.Body.Close() }()
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatalf("read %s/metrics: %v", base, err)
	}
	for _, line := range strings.Split(string(body), "\n") {
		if rest, ok := strings.CutPrefix(line, name+" "); ok {
			v, err := strconv.ParseFloat(strings.TrimSpace(rest), 64)
			if err != nil {
				t.Fatalf("metric %s: %q: %v", name, line, err)
			}
			return v
		}
	}
	// The gauge is exposed from start-up with value 0, so a missing name is a wrong
	// name, not a merge that has not begun: fail instead of "waiting" on nothing.
	t.Fatalf("metric %s is not exposed by %s/metrics", name, base)
	return 0
}

// fnShapeLayers runs the cases in the insert buffer (both batches of the first
// object's shape unflushed), in the objects those batches flush into, and after
// a later batch of another shape has flushed into an object of its own.
func fnShapeLayers(t *testing.T, c *fnFixture, cases func(second bool) []fnCase, writeFirst, writeSecond func(t *testing.T, c *fnFixture)) {
	t.Helper()
	compare := func(t *testing.T, rows int, second bool) {
		for _, f := range c.forms {
			t.Run(f.name, func(t *testing.T) {
				waitRowsVisible(t, c.hot, false, f, c.filter, c.from, c.to, rows)
				waitRowsVisible(t, c.cold, true, f, c.filter, c.from, c.to, rows)
				runFnCases(t, c.hot, c.cold, f, c.from, c.to, c.filter, cases(second))
			})
		}
	}
	t.Run("buffer", func(t *testing.T) {
		writeFirst(t, c)
		for _, f := range c.forms {
			waitBuffered(t, c.cold, c.mode, f.account, c.from, c.to, c.rowsPer)
		}
		forceMergeHot(t, c.hot, c.mode, c.at)
		for _, f := range c.forms {
			requireBuffered(t, c.cold, c.mode, f.account, c.from, c.to, c.rowsPer)
		}
		compare(t, c.rowsPer, false)
	})
	t.Run("parquet", func(t *testing.T) {
		for _, f := range c.forms {
			waitLeftBuffer(t, c.cold, c.mode, f.account, c.from, c.to)
		}
		for _, f := range c.forms {
			requireFlushedOnly(t, c.cold, c.mode, f.account, c.at, c.from, c.to)
		}
		compare(t, c.rowsPer, false)
	})
	t.Run("second_object", func(t *testing.T) {
		for _, f := range c.forms {
			waitFlushIdle(t, c.cold, c.mode, f.account, c.from, c.to)
		}
		writeSecond(t, c)
		for _, f := range c.forms {
			waitBuffered(t, c.cold, c.mode, f.account, c.from, c.to, c.rowsPer)
		}
		for _, f := range c.forms {
			waitLeftBuffer(t, c.cold, c.mode, f.account, c.from, c.to)
		}
		forceMergeHot(t, c.hot, c.mode, c.at)
		for _, f := range c.forms {
			requireFlushedOnly(t, c.cold, c.mode, f.account, c.at, c.from, c.to)
			if l0, _ := objectLevels(partitionObjects(t, c.mode, f.account, c.at)); l0 < 2 {
				t.Fatalf("layer proof: tenant %s has %d L0 objects, want at least 2 (the first batches and the second)", f.account, l0)
			}
		}
		compare(t, 2*c.rowsPer, true)
	})
}

func TestParity_FieldNames_BatchShapes(t *testing.T) {
	stamp := time.Now().UnixNano()
	at := pickQuietHour(t, map[string][]string{
		"logs": {fvEmptyLogsAccount, allColumnSortLogsAliasAccount}, "traces": {fvEmptyTracesAccount, parityTracesAccount}})
	from, to := at.Add(-time.Minute), at.Add(time.Minute)

	t.Run("logs", func(t *testing.T) {
		t.Parallel()
		token := fmt.Sprintf("fs%d", stamp)
		c := &fnFixture{hot: vlBaseURL, cold: lhBaseURL, mode: "logs", at: at, from: from, to: to, filter: "_msg:=" + token, rowsPer: 6,
			forms: []tenantForm{numericTenant(fvEmptyLogsAccount), aliasTenant(allColumnSortLogsAliasAccount, parityLogsOrgID)}}
		seq := 0
		send := func(t *testing.T, c *fnFixture, rows []map[string]string) {
			var body bytes.Buffer
			for _, r := range rows {
				r["_time"] = at.Add(time.Duration(seq) * time.Second).Format(time.RFC3339Nano)
				r["_msg"] = token
				r["run"] = token // a stream of its own per run
				seq++
				b, _ := json.Marshal(r)
				body.Write(b)
				body.WriteByte('\n')
			}
			for _, f := range c.forms {
				post(t, c.hot+"/insert/jsonline?_stream_fields=app,run", "application/stream+json", body.Bytes(), f.header(false))
				post(t, c.cold+"/insert/jsonline?_stream_fields=app,run", "application/stream+json", body.Bytes(), f.header(true))
			}
		}
		row := func(gen, shape string, withField bool) map[string]string {
			r := map[string]string{"app": "ax", "gen": gen, "shape": shape}
			if withField {
				r["onlya"] = "a"
			}
			return r
		}
		// The first object holds a request whose rows carry onlya and a request
		// whose rows do not (two parts on hot until it merges them); the second
		// object, flushed later, holds rows without it.
		writeFirst := func(t *testing.T, c *fnFixture) {
			send(t, c, []map[string]string{row("g0", "with", true), row("g0", "with", true), row("g0", "with", true)})
			send(t, c, []map[string]string{row("g0", "without", false), row("g0", "without", false), row("g0", "without", false)})
		}
		writeSecond := func(t *testing.T, c *fnFixture) {
			rows := make([]map[string]string, 6)
			for i := range rows {
				rows[i] = row("g1", "without", false)
			}
			send(t, c, rows)
		}
		cases := func(second bool) []fnCase {
			out := []fnCase{
				{name: "field_names", endpoint: "field_names", query: "@F"},
				{name: "field_names_rows_without_the_field", endpoint: "field_names", query: "@F shape:=without"},
				{name: "field_names_rows_with_the_field", endpoint: "field_names", query: "@F shape:=with"},
			}
			if second {
				out = append(out, fnCase{name: "field_names_rows_of_the_second_object_only", endpoint: "field_names", query: "@F gen:=g1"})
			}
			return out
		}
		fnShapeLayers(t, c, cases, writeFirst, writeSecond)
	})

	t.Run("traces", func(t *testing.T) {
		t.Parallel()
		var ids []string
		c := &fnFixture{hot: vtBaseURL, cold: lhtBaseURL, mode: "traces", at: at, from: from, to: to, rowsPer: 4,
			forms: []tenantForm{numericTenant(fvEmptyTracesAccount), aliasTenant(parityTracesAccount, parityTracesOrgID)}}
		seq := 0
		send := func(t *testing.T, c *fnFixture, gen, shape string, n int, withField bool) {
			spans := make([]fnOTLPSpan, 0, n)
			for i := 0; i < n; i++ {
				id := fmt.Sprintf("%016x%016x", stamp, 0xfb00+seq)
				ids = append(ids, id)
				attrs := map[string]string{"gen": gen, "shape": shape}
				if withField {
					attrs["onlya"] = "a"
				}
				spans = append(spans, fnOTLPSpan{traceID: id, spanID: fmt.Sprintf("%016x", seq+1), name: "op",
					service: fmt.Sprintf("fs-svc-%d", stamp), spanAttrs: attrs, at: at.Add(time.Duration(seq) * time.Second)})
				seq++
			}
			for _, f := range c.forms {
				pushOTLPSpansOneRequest(t, c.hot, f.header(false), spans)
				pushOTLPSpansOneRequest(t, c.cold, f.header(true), spans)
			}
			c.filter = "trace_id:in(" + strings.Join(ids, ",") + ")"
		}
		writeFirst := func(t *testing.T, c *fnFixture) {
			send(t, c, "g0", "with", 2, true)
			send(t, c, "g0", "without", 2, false)
		}
		writeSecond := func(t *testing.T, c *fnFixture) { send(t, c, "g1", "without", 4, false) }
		cases := func(second bool) []fnCase {
			out := []fnCase{
				{name: "field_names", endpoint: "field_names", query: "@F"},
				{name: "field_names_spans_without_the_attribute", endpoint: "field_names", query: "@F `span_attr:shape`:=without"},
				{name: "field_names_spans_with_the_attribute", endpoint: "field_names", query: "@F `span_attr:shape`:=with"},
			}
			if second {
				out = append(out, fnCase{name: "field_names_spans_of_the_second_object_only", endpoint: "field_names", query: "@F `span_attr:gen`:=g1"})
			}
			return out
		}
		fnShapeLayers(t, c, cases, writeFirst, writeSecond)
	})
}
