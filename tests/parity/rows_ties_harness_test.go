//go:build parity

package parity

// Offline checks of the tie-group rule in rows_ties.go and of the Tempo
// sample union. They issue no requests, so they run without the parity stack:
//
//	GOWORK=off go test -tags=parity -run '^TestHarness_' ./tests/parity/

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"reflect"
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

// fixedTies is a tieGroupFetcher that answers with canned bodies and records
// the times it was asked about.
type fixedTies struct {
	ref, sut fetchResult
	asked    []time.Time
}

func (f *fixedTies) fetch(_ *testing.T, at time.Time) (fetchResult, fetchResult) {
	f.asked = append(f.asked, at)
	return f.ref, f.sut
}

func keysOf(r fetchResult) []string {
	return extractRowKeys(parseNDJSON(r.Body), nil)
}

const (
	tieTS   = "2026-10-01T20:11:36.916949216Z"
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
	tiedA = row(tieTS, "172.16.0.55 PATCH /favicon.ico", "payment-service")
	tiedB = row(tieTS, "192.168.1.100 PATCH /static/app.js", "notification-service")
)

func answer(last string) fetchResult {
	return body(append(append([]string(nil), commonTop...), last)...)
}

func TestHarness_TieGroupQueryDropsRowLimitPipes(t *testing.T) {
	cases := map[string]string{
		`* | delete trace_id | filter level:="WARN" | fields _time, _msg | sort by(_time) desc | limit 5`: `* | delete trace_id | filter level:="WARN" | fields _time, _msg | sort by(_time) desc`,
		`* | sort by(_time) desc | offset 5 | limit 5`:                                                    `* | sort by(_time) desc`,
		`* | sort by(_time) | first 5`:                                                                    `* | sort by(_time)`,
		`* | last 3 by (_time)`:                                                                           `*`,
		`* | top 5 by(service.name)`:                                                                      `* | top 5 by(service.name)`,
		`*`:                                                                                               `*`,
	}
	for in, want := range cases {
		if got := tieGroupQuery(in); got != want {
			t.Errorf("tieGroupQuery(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestHarness_TieGroupFetcherWindow(t *testing.T) {
	var got []url.Values
	srv := newRecordingServer(t, func(q url.Values) string { got = append(got, q); return "" })
	params := url.Values{"query": {`* | sort by(_time) desc | limit 5`}, "limit": {"5"}, "start": {"1"}, "end": {"2"}}
	at, _ := time.Parse(time.RFC3339Nano, tieTS)
	newTieGroupFetcher(srv, srv, "/select/logsql/query", params)(t, at)
	if len(got) != 2 {
		t.Fatalf("fetcher made %d requests, want one per tier", len(got))
	}
	for _, q := range got {
		if q.Get("query") != `* | sort by(_time) desc` {
			t.Errorf("query = %q, want the limit pipe removed", q.Get("query"))
		}
		if q.Get("start") != fmt.Sprint(at.UnixNano()-1) || q.Get("end") != fmt.Sprint(at.UnixNano()+1) {
			t.Errorf("window = [%s, %s], want [at-1ns, at+1ns]", q.Get("start"), q.Get("end"))
		}
		if q.Get("limit") != "1000" {
			t.Errorf("limit = %q, want it raised so the group comes back whole", q.Get("limit"))
		}
	}
	if params.Get("limit") != "5" || params.Get("start") != "1" {
		t.Errorf("fetcher modified the caller's params: %v", params)
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
		nilTies  bool
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
			name: "no re-read available keeps the strict comparison", ref: refAns, sut: sutAns,
			nilTies: true, why: "no way to re-read",
		},
		{
			name: "the group itself differs between tiers", ref: refAns, sut: sutAns,
			ties: &fixedTies{ref: fullGroup, sut: body(tiedB)}, why: "full group differs",
		},
		{
			name: "the group is identical but no larger than what was kept", ref: refAns, sut: answer(row(tieTS, "invented", "ghost-service")),
			ties: &fixedTies{ref: body(tiedA), sut: body(tiedA)}, why: "no limit cut it",
		},
		{
			name: "a kept row is not a member of the group", ref: refAns, sut: answer(row(tieTS, "invented", "ghost-service")),
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, why: "not a member",
		},
		{
			name: "rows differ away from the edge of the answers",
			ref:  body(row(newer1, "n1", "a"), tiedA, row(olderTS, "o", "c")),
			sut:  body(row(newer1, "n1", "a"), tiedB, row(olderTS, "o", "c")),
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, why: "edge of both answers",
		},
		{
			name: "differing rows at different times",
			ref:  answer(tiedA),
			sut:  answer(row(olderTS, "o", "c")),
			ties: &fixedTies{ref: fullGroup, sut: fullGroup}, why: "sharing one _time",
		},
		{
			// One differing pair sits at the edge, the other inside: the
			// inside pair is a real divergence a tie cannot explain.
			name: "differing rows at the edge and inside",
			ref:  body(row(newer1, "n1", "a"), row(newer2, "inside-ref", "x"), tiedA, row(tieTS, "y", "y")),
			sut:  body(row(newer1, "n1", "a"), row(newer3, "inside-sut", "z"), tiedB, row(tieTS, "y", "y")),
			ties: &fixedTies{ref: body(tiedA, tiedB, row(tieTS, "y", "y")), sut: body(tiedA, tiedB, row(tieTS, "y", "y"))},
			why:  "sharing one _time",
		},
		{
			// Every differing row is in the group, but the SUT kept more of it.
			name: "one side kept more rows of the tie",
			ref:  answer(tiedA),
			sut:  body(append(append([]string(nil), commonTop...), tiedB, row(tieTS, "y", "y"))...),
			ties: &fixedTies{ref: body(tiedA, tiedB, row(tieTS, "y", "y")), sut: body(tiedA, tiedB, row(tieTS, "y", "y"))},
			why:  "sharing one _time",
		},
		{
			name: "re-read failed",
			ref:  refAns, sut: sutAns,
			ties: &fixedTies{ref: fetchResult{StatusCode: 500}, sut: fullGroup}, why: "status ref=500",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			var ties tieGroupFetcher
			if !tc.nilTies {
				ties = tc.ties.fetch
			}
			_, why, ok := explainedByTruncatedTie(t, ties, keysOf(tc.ref), keysOf(tc.sut), nil)
			if ok != tc.accept {
				t.Fatalf("accepted = %v (%s), want %v", ok, why, tc.accept)
			}
			if !tc.accept && !strings.Contains(why, tc.why) {
				t.Errorf("reason = %q, want one containing %q", why, tc.why)
			}
		})
	}
}

func TestHarness_TieAtEitherEdgeOnly(t *testing.T) {
	group := body(tiedA, tiedB)
	ties := (&fixedTies{ref: group, sut: group}).fetch
	// An ascending answer (`sort by(_time) | first N`) is cut at its newest
	// _time: a tie there is the cut edge and is accepted.
	ref := body(row(olderTS, "o", "c"), tiedA)
	sut := body(row(olderTS, "o", "c"), tiedB)
	if _, why, ok := explainedByTruncatedTie(t, ties, keysOf(ref), keysOf(sut), nil); !ok {
		t.Fatalf("a tie at the newest edge was rejected: %s", why)
	}
	// A tie strictly inside the answer is never a cut, whatever the group.
	ref = body(row(olderTS, "o", "c"), tiedA, row(newer1, "n1", "a"))
	sut = body(row(olderTS, "o", "c"), tiedB, row(newer1, "n1", "a"))
	if _, _, ok := explainedByTruncatedTie(t, ties, keysOf(ref), keysOf(sut), nil); ok {
		t.Fatalf("a tie inside the answer was accepted")
	}
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
