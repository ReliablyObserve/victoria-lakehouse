//go:build parity

package parity

// #274: a field a row never carried stays absent. severity_number is a typed
// (INT32) column on the cold tier. It used to be a required column, so a row
// stored without the field came back with severity_number="0" while hot
// VictoriaLogs returned the row as written, and the two answers differed in the
// rows, in field_names, in field_values and in every filter on the field.
//
// The case writes the same three rows to hot and to Lakehouse: no severity at
// all, an explicit "0" (what OTLP writes for UNSPECIFIED) and "9". Every answer is then
// compared with hot in each data layer (buffer, Parquet, compacted, after a
// restart of the service) and in both tenant forms: the rows byte for byte, in
// order and with their JSON keys in order; field_values of the field; and the
// row count of each filter on the field. The cold tier must
// return exactly what hot returns. Logs only: the traces binary stores no
// severity_number; the traces columns are covered by the Go tests of
// lakehouse-traces and by TestParity_Traces_ColdSpanFields.

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"sort"
	"strings"
	"testing"
	"time"
)

const (
	sevAbsentAccount = "7331"
	// sevAbsentRowsPerBatch is how many rows one write puts into each tenant.
	sevAbsentRowsPerBatch = 3
)

type sevAbsentLayer struct {
	name    string
	prepare func(t *testing.T, c *sevAbsentCase) int
	prove   func(t *testing.T, c *sevAbsentCase, f tenantForm)
}

type sevAbsentCase struct {
	token          string
	at             time.Time
	from, to       time.Time
	forms          []tenantForm
	startedAt      time.Time
	restartBatches int
}

// write puts one batch of the three rows into every tenant form on hot and cold.
func (c *sevAbsentCase) write(t *testing.T, batch int) {
	t.Helper()
	sev := []string{"", "0", "9"}
	var body bytes.Buffer
	for i, s := range sev {
		row := map[string]string{
			"_time": c.at.Add(time.Duration(batch*sevAbsentRowsPerBatch+i) * time.Second).Format(time.RFC3339Nano),
			"_msg":  fmt.Sprintf("%s row%d", c.token, i),
			"svc":   "sevabsent",
			// An explicit level keeps Lakehouse from deriving one out of the
			// number (a separate, tracked difference): this case is about
			// severity_number only.
			"level": "INFO",
		}
		if s != "" {
			row["severity_number"] = s
		}
		b, _ := json.Marshal(row)
		body.Write(b)
		body.WriteByte('\n')
	}
	for _, f := range c.forms {
		post(t, vlBaseURL+"/insert/jsonline?_stream_fields=svc", "application/stream+json", body.Bytes(), f.header(false))
		post(t, lhBaseURL+"/insert/jsonline?_stream_fields=svc", "application/stream+json", body.Bytes(), f.header(true))
	}
}

func (c *sevAbsentCase) params(query string, extra map[string]string) url.Values {
	p := url.Values{"query": {query}, "disable_latency_offset": {"true"},
		"start": {c.from.UTC().Format(time.RFC3339Nano)}, "end": {c.to.UTC().Format(time.RFC3339Nano)}}
	for k, v := range extra {
		p.Set(k, v)
	}
	return p
}

// get asks both tiers the same question and returns hot's and cold's answers.
func (c *sevAbsentCase) get(t *testing.T, f tenantForm, path, query string, extra map[string]string) (hot, cold fetchResult) {
	t.Helper()
	p := c.params(query, extra)
	hot = getWith(t, vlBaseURL, path, p, f.header(false))
	cold = getWith(t, lhBaseURL, path, p, f.header(true))
	if hot.StatusCode != http.StatusOK || cold.StatusCode != http.StatusOK {
		t.Fatalf("%s %q: hot answered %d (%s), cold %d (%s)", path, query, hot.StatusCode, hot.Body, cold.StatusCode, cold.Body)
	}
	return hot, cold
}

func sevAbsentLines(r fetchResult) []string {
	var out []string
	for _, l := range strings.Split(strings.TrimSpace(string(r.Body)), "\n") {
		if l != "" {
			out = append(out, l)
		}
	}
	return out
}

// sevAbsentValues renders a field_names / field_values answer as sorted
// "value=hits" strings, so two answers compare independently of their order.
func sevAbsentValues(t *testing.T, r fetchResult) []string {
	t.Helper()
	var d struct {
		Values []struct {
			Value string `json:"value"`
			Hits  uint64 `json:"hits"`
		} `json:"values"`
	}
	if err := json.Unmarshal(r.Body, &d); err != nil {
		t.Fatalf("decode %s: %v", r.Body, err)
	}
	out := make([]string, 0, len(d.Values))
	for _, v := range d.Values {
		out = append(out, fmt.Sprintf("%s=%d", v.Value, v.Hits))
	}
	sort.Strings(out)
	return out
}

func sevAbsentStat(t *testing.T, r fetchResult) string {
	t.Helper()
	var d struct {
		Data struct {
			Result []struct {
				Value []any `json:"value"`
			} `json:"result"`
		} `json:"data"`
	}
	if err := json.Unmarshal(r.Body, &d); err != nil {
		t.Fatalf("decode %s: %v", r.Body, err)
	}
	if len(d.Data.Result) != 1 || len(d.Data.Result[0].Value) != 2 {
		return fmt.Sprintf("unexpected shape: %s", r.Body)
	}
	return fmt.Sprint(d.Data.Result[0].Value[1])
}

// compare runs every comparison of the case for one tenant form and layer and
// returns how many cells (comparisons) it made.
func (c *sevAbsentCase) compare(t *testing.T, f tenantForm, rows int) int {
	t.Helper()
	filter := c.token // a word filter on _msg selects exactly this case's rows
	deadline := time.Now().Add(60 * time.Second)
	for {
		hot, cold := c.get(t, f, "/select/logsql/query", filter+" | sort by (_time)", map[string]string{"limit": "1000"})
		if len(sevAbsentLines(hot)) == rows && len(sevAbsentLines(cold)) == rows {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("the %d written rows never became visible: hot has %d, cold %d", rows, len(sevAbsentLines(hot)), len(sevAbsentLines(cold)))
		}
		time.Sleep(time.Second)
	}
	cells := 0

	// Rows, in order, with their JSON keys in order: byte for byte.
	hot, cold := c.get(t, f, "/select/logsql/query", filter+" | sort by (_time)", map[string]string{"limit": "1000"})
	hl, cl := sevAbsentLines(hot), sevAbsentLines(cold)
	for i := range hl {
		cells++
		if hl[i] != cl[i] {
			t.Errorf("row %d differs (#274):\n hot:  %s\n cold: %s", i, hl[i], cl[i])
		}
	}
	// An absent field must not appear on its row, an explicit zero must.
	if len(cl) >= 2 {
		cells++
		if strings.Contains(cl[0], "severity_number") {
			t.Errorf("the row written without severity_number carries it on cold: %s", cl[0])
		}
		if !strings.Contains(cl[1], `"severity_number":"0"`) {
			t.Errorf("the row written with severity_number=0 lost it on cold: %s", cl[1])
		}
	}

	// field_values of the field. (field_names is not compared here: its hits and
	// its list are the field_names parity work, #280 and related.)
	hot, cold = c.get(t, f, "/select/logsql/field_values", filter, map[string]string{"field": "severity_number"})
	cells++
	if h, cc := sevAbsentValues(t, hot), sevAbsentValues(t, cold); strings.Join(h, ",") != strings.Join(cc, ",") {
		t.Errorf("field_values(severity_number) differ (#274):\n hot:  %v\n cold: %v", h, cc)
	}

	// Filters and aggregates over the field.
	for _, q := range []string{
		filter + " severity_number:* | stats count() c",
		filter + " -severity_number:* | stats count() c",
		filter + ` severity_number:="0" | stats count() c`,
		filter + ` severity_number:="9" | stats count() c`,
		filter + " | stats count(severity_number) c",
		filter + " | stats count_uniq(severity_number) c",
	} {
		hot, cold = c.get(t, f, "/select/logsql/stats_query", q, nil)
		cells++
		if h, cc := sevAbsentStat(t, hot), sevAbsentStat(t, cold); h != cc {
			t.Errorf("%q: hot %s, cold %s (#274)", q, h, cc)
		}
	}
	return cells
}

func TestParity_SeverityNumber_Absent(t *testing.T) {
	at := pickQuietHour(t, map[string][]string{"logs": {sevAbsentAccount, allColumnSortLogsAliasAccount}})
	c := &sevAbsentCase{
		token: fmt.Sprintf("sevabs%d", time.Now().UnixNano()), at: at, from: at.Add(-time.Minute), to: at.Add(time.Minute),
		forms: []tenantForm{numericTenant(sevAbsentAccount), aliasTenant(allColumnSortLogsAliasAccount, parityLogsOrgID)},
	}
	buffered := func(t *testing.T, rows int) {
		for _, f := range c.forms {
			waitBuffered(t, lhBaseURL, "logs", f.account, c.from, c.to, rows)
		}
	}
	leftBuffer := func(t *testing.T) {
		for _, f := range c.forms {
			waitLeftBuffer(t, lhBaseURL, "logs", f.account, c.from, c.to)
		}
	}
	layers := []sevAbsentLayer{
		{"buffer",
			func(t *testing.T, c *sevAbsentCase) int {
				c.write(t, 0)
				buffered(t, sevAbsentRowsPerBatch)
				return sevAbsentRowsPerBatch
			},
			func(t *testing.T, c *sevAbsentCase, f tenantForm) {
				requireBuffered(t, lhBaseURL, "logs", f.account, c.from, c.to, sevAbsentRowsPerBatch)
			}},
		{"parquet",
			func(t *testing.T, c *sevAbsentCase) int { leftBuffer(t); return sevAbsentRowsPerBatch },
			func(t *testing.T, c *sevAbsentCase, f tenantForm) {
				requireFlushedOnly(t, lhBaseURL, "logs", f.account, c.at, c.from, c.to)
			}},
		{"compacted",
			func(t *testing.T, c *sevAbsentCase) int {
				c.write(t, 1)
				buffered(t, sevAbsentRowsPerBatch)
				leftBuffer(t)
				recompactHourPartition(t, lhBaseURL, c.at, 2*len(c.forms))
				return 2 * sevAbsentRowsPerBatch
			},
			func(t *testing.T, c *sevAbsentCase, f tenantForm) {
				requireBuffered(t, lhBaseURL, "logs", f.account, c.from, c.to, 0)
				requireCompactedOnce(t, "logs", f.account, c.at)
			}},
		{"restart",
			func(t *testing.T, c *sevAbsentCase) int {
				for attempt := 1; attempt <= restartAttempts; attempt++ {
					for _, f := range c.forms {
						waitFlushIdle(t, lhBaseURL, "logs", f.account, c.from, c.to)
					}
					c.write(t, 1+attempt)
					c.restartBatches = attempt
					c.startedAt = restartComposeServices(t, lhBaseURL, "logs", []string{"lakehouse-logs"}, restartStopTimeout)
					for _, f := range c.forms {
						if n := waitL0Objects(t, "logs", f.account, c.at, attempt, 90*time.Second); n < attempt {
							t.Fatalf("tenant %s has %d L0 objects 90s after the restart, want %d: the acknowledged batch was not recovered", f.account, n, attempt)
						}
					}
					leftBuffer(t)
					lost := ""
					for _, f := range c.forms {
						if why := recoveredSegmentFlushed(t, "logs", f.account, c.at, c.startedAt, attempt); why != "" {
							lost = why
						}
					}
					if lost == "" {
						return (2 + attempt) * sevAbsentRowsPerBatch
					}
					t.Logf("restart attempt %d of %d: the periodic flush won the race (%s); retrying with a new batch", attempt, restartAttempts, lost)
				}
				t.Fatalf("the periodic flush won the race in all %d restart attempts", restartAttempts)
				return 0
			},
			func(t *testing.T, c *sevAbsentCase, f tenantForm) {
				requireBuffered(t, lhBaseURL, "logs", f.account, c.from, c.to, 0)
				if why := recoveredSegmentFlushed(t, "logs", f.account, c.at, c.startedAt, c.restartBatches); why != "" {
					t.Fatalf("layer proof: %s", why)
				}
			}},
	}
	cells := 0
	for _, l := range layers {
		t.Run(l.name, func(t *testing.T) {
			rows := l.prepare(t, c)
			for _, f := range c.forms {
				t.Run(f.name, func(t *testing.T) {
					l.prove(t, c, f)
					cells += c.compare(t, f, rows)
					l.prove(t, c, f)
				})
			}
		})
	}
	reportLockCells(t, cells)
}
