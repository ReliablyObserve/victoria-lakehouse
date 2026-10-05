//go:build parity

package parity

// Offline checks of the tie-group rule in rows_ties.go and of the Tempo
// sample union. They issue no requests to the parity stack (the end-to-end
// cases use local httptest servers), so they run without it:
//
//	GOWORK=off go test -tags=parity -run '^TestHarness_' ./tests/parity/

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
	"strconv"
	"strings"
	"testing"
	"time"
)

// row renders one NDJSON row the way the query endpoint answers.
func row(ts, msg, svc string) string {
	return fmt.Sprintf(`{"_time":%q,"_msg":%q,"level":"WARN","service.name":%q}`, ts, msg, svc)
}

func body(rows ...string) fetchResult {
	return fetchResult{StatusCode: 200, Body: []byte(strings.Join(rows, "\n") + "\n")}
}

// fixedTies is a tieGroupFetcher that answers with canned bodies.
type fixedTies struct{ ref, sut fetchResult }

func (f *fixedTies) fetch(_ *testing.T, _ time.Time) (fetchResult, fetchResult) {
	return f.ref, f.sut
}

const (
	tieTS   = "2026-10-01T20:11:36.916949216Z"
	afterTS = "2026-10-01T20:11:36.916950216Z" // one microsecond after the tie, inside the re-read window
	newer1  = "2026-10-01T20:40:01.5Z"
	newer2  = "2026-10-01T20:30:01.5Z"
	newer3  = "2026-10-01T20:20:01.5Z"
	newer4  = "2026-10-01T20:15:01.5Z"
	olderTS = "2026-10-01T19:00:00Z"
)

// The four rows both tiers agree on, newest first, above the tie.
var commonTop = []string{
	row(newer1, "n1", "api-gateway"),
	row(newer2, "n2", "order-service"),
	row(newer3, "n3", "user-service"),
	row(newer4, "n4", "api-gateway"),
}

// tiedA/tiedB are the two rows of run 36918592235 attempt 2
// (delete_filter_fields_sort): same _time to the nanosecond, different rows.
var (
	tiedA  = row(tieTS, "172.16.0.55 PATCH /favicon.ico", "payment-service")
	tiedB  = row(tieTS, "192.168.1.100 PATCH /static/app.js", "notification-service")
	tiedC  = row(tieTS, "third", "user-service")
	ghost  = row(tieTS, "invented", "ghost-service")
	nearby = row(afterTS, "next microsecond", "api-gateway")
)

func answer(last ...string) fetchResult {
	return body(append(append([]string(nil), commonTop...), last...)...)
}

func TestHarness_TieGroupQuery(t *testing.T) {
	cases := []struct {
		in, want string
		ok       bool
	}{
		{`* | delete trace_id | filter level:="WARN" | fields _time, _msg, level, service.name | sort by(_time) desc | limit 5`,
			`* | delete trace_id | filter level:="WARN" | fields _time, _msg, level, service.name | sort by(_time) desc`, true},
		{`* | sort by(_time) desc | offset 5 | limit 5`, `* | sort by(_time) desc`, true},
		{`* | fields _time, _msg, level`, `* | fields _time, _msg, level`, true},
		{`* | last 3 by (_time)`, `*`, true},
		{`*`, `*`, true},
		{`* | limit 1_000`, `*`, true},
		// A `|` inside a quoted phrase or a subquery is not a pipe.
		{`_msg:"a | limit 5" | sort by (_time) desc | limit 2`, `_msg:"a | limit 5" | sort by (_time) desc`, true},
		{`service.name:in(* | limit 5 | fields service.name) | limit 3`, `service.name:in(* | limit 5 | fields service.name)`, true},
		// A sort key other than _time, or an ordering this file cannot undo:
		// the tie rule does not apply.
		{`* | sort by(level, _time) | limit 10`, ``, false},
		{`* | sort by(_time) desc limit 5`, ``, false},
		{`* | sort`, ``, false},
		{`* | top 5 by(service.name)`, ``, false},
		{`* | sort by(_time) | first 5`, ``, false},
		{`* | last 5 by (_time) partition by (host)`, ``, false},
		{`* | uniq by (level)`, ``, false},
		{`_msg:"unterminated | limit 5`, ``, false},
		{`* | filter (a or b`, ``, false},
	}
	for _, tc := range cases {
		got, ok := tieGroupQuery(tc.in)
		if ok != tc.ok || got != tc.want {
			t.Errorf("tieGroupQuery(%q) = %q, %v; want %q, %v", tc.in, got, ok, tc.want, tc.ok)
		}
	}
}

func TestHarness_TieGroupFetcher(t *testing.T) {
	var got []url.Values
	srv := newRecordingServer(t, func(q url.Values) string { got = append(got, q); return "" })
	params := url.Values{"query": {`* | sort by(_time) desc | limit 5`}, "limit": {"5"}, "start": {"1"}, "end": {"2"}}
	at, _ := time.Parse(time.RFC3339Nano, tieTS)
	fetchTies := newTieGroupFetcher(srv, srv, "/select/logsql/query", params)
	if fetchTies == nil {
		t.Fatal("no fetcher for a case ordered by _time alone")
	}
	fetchTies(t, at)
	if len(got) != 2 {
		t.Fatalf("fetcher made %d requests, want one per tier", len(got))
	}
	for _, q := range got {
		if q.Get("query") != `* | sort by(_time) desc` {
			t.Errorf("query = %q, want the limit pipe removed", q.Get("query"))
		}
		start, _ := strconv.ParseInt(q.Get("start"), 10, 64)
		end, _ := strconv.ParseInt(q.Get("end"), 10, 64)
		if start >= at.UnixNano() || end <= at.UnixNano() {
			t.Errorf("window [%d, %d] does not contain the tie %d", start, end, at.UnixNano())
		}
		// B7: the cold tier drops a row whose microsecond holds the end bound.
		if end/1000 <= at.UnixNano()/1000+1 {
			t.Errorf("end %d does not reach past the microsecond after the tie", end)
		}
		if q.Get("limit") != "1000" {
			t.Errorf("limit = %q, want it raised so the group comes back whole", q.Get("limit"))
		}
	}
	if params.Get("limit") != "5" || params.Get("start") != "1" {
		t.Errorf("fetcher modified the caller's params: %v", params)
	}
	if newTieGroupFetcher(srv, srv, "/select/logsql/query", url.Values{"query": {`* | sort by(level, _time) | limit 3`}}) != nil {
		t.Error("a fetcher was built for a case with a secondary sort key")
	}
}

func TestHarness_MultisetDiffCountsDuplicates(t *testing.T) {
	a := []string{"x", "x", "y"}
	b := []string{"x", "y", "z"}
	aOnly, bOnly := multisetDiff(a, b)
	if !reflect.DeepEqual(aOnly, []string{"x"}) || !reflect.DeepEqual(bOnly, []string{"z"}) {
		t.Fatalf("multisetDiff = %v, %v; want [x], [z]", aOnly, bOnly)
	}
	if aOnly, bOnly := multisetDiff(a, a); len(aOnly)+len(bOnly) != 0 {
		t.Fatalf("a multiset differs from itself: %v %v", aOnly, bOnly)
	}
}

func TestHarness_TruncatedTieGroup(t *testing.T) {
	refAns, sutAns := answer(tiedA), answer(tiedB)
	fullGroup := body(tiedA, tiedB)

	cases := []struct {
		name     string
		ref, sut fetchResult
		ties     *fixedTies
		accept   bool
		why      string
	}{
		{
			// The failure of run 36918592235 attempt 2: each tier kept a
			// different member of a two-row group cut by `limit 5`.
			name: "limit cuts a tie group the tiers hold identically", ref: refAns, sut: sutAns,
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, accept: true,
		},
		{
			name: "rows of other times in the re-read window are ignored", ref: refAns, sut: sutAns,
			ties: &fixedTies{ref: body(tiedA, tiedB, nearby), sut: fullGroup}, accept: true,
		},
		{
			name: "a tie at the newest edge of an ascending answer",
			ref:  body(row(olderTS, "o", "c"), tiedA), sut: body(row(olderTS, "o", "c"), tiedB),
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, accept: true,
		},
		{
			name: "identical answers need no re-read", ref: refAns, sut: refAns, accept: true,
		},
		{
			name: "no re-read available keeps the strict comparison", ref: refAns, sut: sutAns,
			why: "does not order rows by _time alone",
		},
		{
			name: "the cold group lacks a member", ref: refAns, sut: sutAns,
			ties: &fixedTies{ref: fullGroup, sut: body(tiedB)}, why: "full group differs",
		},
		{
			name: "the cold group has an extra member", ref: refAns, sut: sutAns,
			ties: &fixedTies{ref: fullGroup, sut: body(tiedA, tiedB, tiedC)}, why: "full group differs",
		},
		{
			name: "the group is identical but no larger than what was kept", ref: refAns, sut: answer(ghost),
			ties: &fixedTies{ref: body(tiedA), sut: body(tiedA)}, why: "no limit cut it",
		},
		{
			name: "a row the SUT kept is not a member of the group", ref: refAns, sut: answer(ghost),
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, why: "not a member",
		},
		{
			name: "a row the reference kept is not a member of the group", ref: answer(ghost), sut: sutAns,
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, why: "not a member",
		},
		{
			name: "rows differ away from the edge of the answers",
			ref:  body(row(newer1, "n1", "a"), tiedA, row(olderTS, "o", "c")),
			sut:  body(row(newer1, "n1", "a"), tiedB, row(olderTS, "o", "c")),
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, why: "sharing one _time",
		},
		{
			name: "the reference's differing row is at the edge, the SUT's is not",
			ref:  body(row(newer1, "n1", "a"), tiedA),
			sut:  body(row(newer1, "n1", "a"), row(newer4, "inside", "z")),
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, why: "sharing one _time",
		},
		{
			name: "the SUT's differing row is at the edge, the reference's is not",
			ref:  body(row(newer1, "n1", "a"), row(newer4, "inside", "z")),
			sut:  body(row(newer1, "n1", "a"), tiedB),
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, why: "sharing one _time",
		},
		{
			name: "differing rows at the edge and inside",
			ref:  body(row(newer1, "n1", "a"), row(newer2, "inside-ref", "x"), tiedA, tiedC),
			sut:  body(row(newer1, "n1", "a"), row(newer3, "inside-sut", "z"), tiedB, tiedC),
			ties: &fixedTies{ref: body(tiedA, tiedB, tiedC), sut: body(tiedA, tiedB, tiedC)},
			why:  "sharing one _time",
		},
		{
			// Every differing row is in the group, but the SUT kept more of it.
			name: "one side kept more rows of the tie",
			ref:  answer(tiedA),
			sut:  body(append(append([]string(nil), commonTop...), tiedB, tiedC)...),
			ties: &fixedTies{ref: body(tiedA, tiedB, tiedC), sut: body(tiedA, tiedB, tiedC)},
			why:  "row counts differ",
		},
		{
			name: "reference re-read failed", ref: refAns, sut: sutAns,
			ties: &fixedTies{ref: fetchResult{StatusCode: 500}, sut: fullGroup}, why: "status ref=500",
		},
		{
			name: "SUT re-read failed", ref: refAns, sut: sutAns,
			ties: &fixedTies{ref: fullGroup, sut: fetchResult{StatusCode: 502}}, why: "sut=502",
		},
		{
			name: "both re-reads empty", ref: refAns, sut: sutAns,
			ties: &fixedTies{ref: body(), sut: body()}, why: "no limit cut it",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ties tieGroupFetcher
			if tc.ties != nil {
				ties = tc.ties.fetch
			}
			problems, note := judgeRows(t, parseNDJSON(tc.ref.Body), parseNDJSON(tc.sut.Body), nil, ties)
			if accepted := len(problems) == 0; accepted != tc.accept {
				t.Fatalf("accepted = %v (%s; %v), want %v", accepted, note, problems, tc.accept)
			}
			if !tc.accept && !strings.Contains(note, tc.why) {
				t.Errorf("note = %q, want one containing %q", note, tc.why)
			}
		})
	}
}

// TestHarness_RunParityAcceptsATieCutByTheLimit drives the real path —
// RunParity, the fetcher it builds and the HTTP re-read — against two local
// servers that answer like hot and cold in run 36918592235 attempt 2.
func TestHarness_RunParityAcceptsATieCutByTheLimit(t *testing.T) {
	serve := func(kept string) string {
		return newRecordingServer(t, func(q url.Values) string {
			if strings.Contains(q.Get("query"), "limit 5") {
				return string(answer(kept).Body)
			}
			return string(body(tiedA, tiedB).Body)
		})
	}
	hot, cold := serve(tiedA), serve(tiedB)
	RunParity(t, hot, cold, []ParityCase{{
		Name:     "delete_filter_fields_sort",
		Endpoint: "/select/logsql/query",
		Params:   map[string]string{"query": `* | sort by(_time) desc | limit 5`, "limit": "5"},
		Compare:  RowsMatch,
	}})
}

func TestHarness_UnionOfSamplesRecoversDroppedValues(t *testing.T) {
	// What the upstream race does to one call: a random value goes missing.
	answers := [][]string{
		{"api-gateway", "order-service", "user-service"},
		{"api-gateway", "payment-service", "user-service"},
		{"order-service", "payment-service", "user-service"},
		{"api-gateway", "order-service", "payment-service", "user-service"},
		{"api-gateway", "order-service", "payment-service"},
	}
	i := 0
	got := unionOfSamples(t, "test", func() []string { i++; return answers[i-1] })
	want := []string{"api-gateway", "order-service", "payment-service", "user-service"}
	if !reflect.DeepEqual(got, want) {
		t.Fatalf("union = %v, want %v", got, want)
	}
	if i != tempoRaceSamples {
		t.Fatalf("read %d samples, want %d", i, tempoRaceSamples)
	}
	// A value no sample ever returns stays missing: the union never invents.
	none := unionOfSamples(t, "test", func() []string { return []string{"api-gateway"} })
	if !reflect.DeepEqual(none, []string{"api-gateway"}) {
		t.Fatalf("union of identical samples = %v", none)
	}
}

// newRecordingServer starts a server that hands each request's query
// parameters to record and answers 200 with what record returns.
func newRecordingServer(t *testing.T, record func(url.Values) string) string {
	t.Helper()
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		_, _ = w.Write([]byte(record(r.URL.Query())))
	}))
	t.Cleanup(srv.Close)
	return srv.URL
}

// TestHarness_TieNeedsEqualDifferences calls the tie rule directly with
// answers of different lengths, which judgeRows already refuses on its own:
// the rule must not lean on that.
func TestHarness_TieNeedsEqualDifferences(t *testing.T) {
	group := body(tiedA, tiedB, tiedC)
	ref := extractRowKeys(parseNDJSON(answer(tiedA).Body), nil)
	sut := extractRowKeys(parseNDJSON(answer(tiedB, tiedC).Body), nil)
	if _, why, ok := explainedByTruncatedTie(t, (&fixedTies{ref: group, sut: group}).fetch, ref, sut, nil); ok {
		t.Fatal("one row against two was accepted as a tie")
	} else if !strings.Contains(why, "equal number") {
		t.Errorf("reason = %q", why)
	}
}
