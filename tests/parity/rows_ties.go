//go:build parity

package parity

// Row comparisons where a row limit cuts through a _time tie.
//
// VictoriaLogs' `sort` pipe does not break ties: sortBlockLess
// (lib/logstorage/pipe_sort.go) returns false for rows whose sort keys are
// equal, the shards are ordered with the unstable sort.Sort and merged through
// a heap, and the top-k path behind `sort ... | limit N` keeps whichever tied
// rows it meets first. Which members of a tie group survive a limit therefore
// depends on the storage layout and the worker that read each block. Hot
// VictoriaLogs and the cold tier hold the same rows in different layouts, so
// when the last place inside the limit falls on a _time several rows share,
// each tier can legitimately return a different member of that group.
//
// cmd/datagen makes such ties common: every uncorrelated log row sits on a
// whole-second grid (`now - h + rand(3600)s`), so a 10k-row seed carries tens
// of exact-nanosecond collisions.
//
// RowsMatch already compares rows as a multiset, so the order inside a group
// never matters. What remains is the group a limit cut in two. A difference is
// accepted only when all of these hold:
//
//   - both answers have the same number of rows, and every differing row has
//     the same _time, at the first or last _time of both answers (the edge a
//     limit or offset cuts);
//   - re-read in full from both tiers (the case's own query over a 2ns window
//     around that _time, with its row-limit pipes removed), the group is
//     identical on hot and cold and larger than what the answers kept;
//   - every row each tier kept at that _time is a member of the group.
//
// Anything else is still a mismatch.

import (
	"fmt"
	"net/url"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"testing"
	"time"
)

// tieGroupFetcher reads, from the reference and the system under test, every
// row a case's query yields at exactly one _time.
type tieGroupFetcher func(t *testing.T, at time.Time) (ref, sut fetchResult)

// rowLimitPipe matches the pipes that cap or skip rows. They are dropped when a
// tie group is re-read: the group must come back whole, and a skip would land
// inside the narrow window instead of where the case put it.
var rowLimitPipe = regexp.MustCompile(`(?i)\|\s*(limit|head|offset|skip|first|last)\s+\d+(\s+by\s*\([^)]*\))?`)

// tieGroupQuery returns query with its row-limit pipes removed.
func tieGroupQuery(query string) string {
	return strings.TrimSpace(rowLimitPipe.ReplaceAllString(query, ""))
}

// newTieGroupFetcher re-issues a case against both tiers over [at-1ns, at+1ns].
// The window is one nanosecond wider than the tie on each side so the read
// does not depend on how either tier treats an inclusive bound at the exact
// timestamp (see divergence B7); rowsAt then keeps only the rows at `at`.
func newTieGroupFetcher(refBase, sutBase, endpoint string, params url.Values) tieGroupFetcher {
	return func(t *testing.T, at time.Time) (fetchResult, fetchResult) {
		t.Helper()
		p := url.Values{}
		for k, v := range params {
			p[k] = append([]string(nil), v...)
		}
		p.Set("query", tieGroupQuery(p.Get("query")))
		p.Set("start", strconv.FormatInt(at.UnixNano()-1, 10))
		p.Set("end", strconv.FormatInt(at.UnixNano()+1, 10))
		if p.Has("limit") {
			p.Set("limit", "1000")
		}
		return fetch(t, refBase, endpoint, p), fetch(t, sutBase, endpoint, p)
	}
}

// rowKeyTime returns the _time a key built by extractRowKeys starts with.
func rowKeyTime(key string) (time.Time, bool) {
	rest, ok := strings.CutPrefix(key, "t=")
	if !ok {
		return time.Time{}, false
	}
	if i := strings.IndexByte(rest, '|'); i >= 0 {
		rest = rest[:i]
	}
	ts, err := time.Parse(time.RFC3339Nano, rest)
	if err != nil {
		return time.Time{}, false
	}
	return ts, true
}

// multisetDiff returns the keys of a missing from b and of b missing from a,
// counting duplicates.
func multisetDiff(a, b []string) (aOnly, bOnly []string) {
	count := map[string]int{}
	for _, k := range a {
		count[k]++
	}
	for _, k := range b {
		count[k]--
	}
	for _, k := range a {
		if count[k] > 0 {
			aOnly = append(aOnly, k)
			count[k]--
		}
	}
	for k, n := range count {
		for ; n < 0; n++ {
			bOnly = append(bOnly, k)
		}
	}
	sort.Strings(aOnly)
	sort.Strings(bOnly)
	return aOnly, bOnly
}

// timeBounds returns the earliest and latest _time among keys.
func timeBounds(keys []string) (lo, hi time.Time, ok bool) {
	for i, k := range keys {
		ts, good := rowKeyTime(k)
		if !good {
			return time.Time{}, time.Time{}, false
		}
		if i == 0 || ts.Before(lo) {
			lo = ts
		}
		if i == 0 || ts.After(hi) {
			hi = ts
		}
	}
	return lo, hi, len(keys) > 0
}

// boundaryTieTime reports the _time every differing row shares, when that
// _time is the first or the last of both answers and both sides differ by the
// same number of rows.
func boundaryTieTime(refKeys, sutKeys, refOnly, sutOnly []string) (time.Time, bool) {
	if len(refOnly) == 0 || len(refOnly) != len(sutOnly) {
		return time.Time{}, false
	}
	var at time.Time
	for i, k := range append(append([]string(nil), refOnly...), sutOnly...) {
		ts, ok := rowKeyTime(k)
		if !ok {
			return time.Time{}, false
		}
		if i == 0 {
			at = ts
		} else if !ts.Equal(at) {
			return time.Time{}, false
		}
	}
	refLo, refHi, ok1 := timeBounds(refKeys)
	sutLo, sutHi, ok2 := timeBounds(sutKeys)
	if !ok1 || !ok2 {
		return time.Time{}, false
	}
	atLow := at.Equal(refLo) && at.Equal(sutLo)
	atHigh := at.Equal(refHi) && at.Equal(sutHi)
	return at, atLow || atHigh
}

// keysAt returns the keys whose _time is at.
func keysAt(keys []string, at time.Time) []string {
	var out []string
	for _, k := range keys {
		if ts, ok := rowKeyTime(k); ok && ts.Equal(at) {
			out = append(out, k)
		}
	}
	return out
}

// isSubMultiset reports whether every key of sub occurs in super at least as
// often.
func isSubMultiset(sub, super []string) bool {
	extra, _ := multisetDiff(sub, super)
	return len(extra) == 0
}

// judgeTruncatedTieGroup re-reads the tie group at `at` from both tiers and
// reports whether the answers' difference is fully explained by a limit
// cutting that group, and why not when it is not.
func judgeTruncatedTieGroup(t *testing.T, ties tieGroupFetcher, at time.Time, refKeys, sutKeys, skipFields []string) (string, bool) {
	t.Helper()
	// boundaryTieTime proved every differing row carries `at` and that both
	// sides differ by as many rows, so both tiers kept as many rows of `at`.
	refKept, sutKept := keysAt(refKeys, at), keysAt(sutKeys, at)
	refRes, sutRes := ties(t, at)
	if refRes.StatusCode != 200 || sutRes.StatusCode != 200 {
		return fmt.Sprintf("re-reading the group returned status ref=%d sut=%d", refRes.StatusCode, sutRes.StatusCode), false
	}
	refGroup := keysAt(extractRowKeys(parseNDJSON(refRes.Body), skipFields), at)
	sutGroup := keysAt(extractRowKeys(parseNDJSON(sutRes.Body), skipFields), at)
	if gRef, gSut := multisetDiff(refGroup, sutGroup); len(gRef) != 0 || len(gSut) != 0 {
		return fmt.Sprintf("the full group differs between tiers: ref-only %v, sut-only %v", gRef, gSut), false
	}
	if len(refGroup) <= len(refKept) {
		return fmt.Sprintf("the group has %d rows and the answers kept %d, so no limit cut it", len(refGroup), len(refKept)), false
	}
	if !isSubMultiset(refKept, refGroup) || !isSubMultiset(sutKept, sutGroup) {
		return "a kept row of that _time is not a member of the group", false
	}
	// boundaryTieTime already proved every differing row carries `at`, so the
	// differing rows are among refKept/sutKept, which are now group members.
	return "", true
}

// explainedByTruncatedTie reports whether two answers that differ as row
// multisets differ only because a limit cut a _time tie group, re-reading the
// group through ties. It returns the tie's _time and, when not explained, why.
func explainedByTruncatedTie(t *testing.T, ties tieGroupFetcher, refKeys, sutKeys, skipFields []string) (time.Time, string, bool) {
	t.Helper()
	if ties == nil {
		return time.Time{}, "no way to re-read the tiers", false
	}
	refOnly, sutOnly := multisetDiff(refKeys, sutKeys)
	at, ok := boundaryTieTime(refKeys, sutKeys, refOnly, sutOnly)
	if !ok {
		return time.Time{}, "the differing rows are not an equal number on each side sharing one _time at the edge of both answers", false
	}
	why, ok := judgeTruncatedTieGroup(t, ties, at, refKeys, sutKeys, skipFields)
	return at, why, ok
}
