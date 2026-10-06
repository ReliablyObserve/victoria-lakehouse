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
//   - the case orders rows by _time alone (no sort pipe: the query endpoint's
//     `limit` returns the newest rows; `sort by (_time)`; `first|last N by
//     (_time)`), or by one `sort by (k1, k2, ..., _time)` whose keys are plain
//     field names and include _time. Rows tie only when EVERY sort key is
//     equal, so a wrong order on any key stays a mismatch: only members of one
//     full-key tie group may differ;
//   - both answers have the same number of rows, and every differing row has
//     the same _time, at the first or last _time of the two answers (the edge
//     a limit or offset cuts). With more sort keys the edge is not the first or
//     last _time, so there the differing rows must instead all carry the same
//     value on every sort key, and the group re-read below must show the limit
//     cut it;
//   - re-read from both tiers (the case's own query with its row-limit pipes
//     removed, over a window a few microseconds around that _time, keeping the
//     rows at exactly that _time and, for a multi-key sort, the same value on
//     every other sort key), the group is identical on hot and cold and larger
//     than what the answers kept;
//   - every row each tier kept in that group is a member of it.
//
// Anything else is still a mismatch, and so is any case whose query this file
// cannot take apart safely.

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
// row a case's query yields around one _time.
type tieGroupFetcher func(t *testing.T, at time.Time) (ref, sut fetchResult)

// splitTopLevelPipes splits a LogsQL query at the `|` characters that separate
// pipes, skipping any inside quotes or parentheses (a quoted phrase, a
// subquery). ok is false when the quotes or parentheses do not balance.
func splitTopLevelPipes(query string) (segments []string, ok bool) {
	depth := 0
	var quote rune
	escaped := false
	last := 0
	for i, r := range query {
		switch {
		case escaped:
			escaped = false
		case quote != 0:
			if r == '\\' && quote != '`' {
				escaped = true
			} else if r == quote {
				quote = 0
			}
		case r == '"' || r == '\'' || r == '`':
			quote = r
		case r == '(':
			depth++
		case r == ')':
			depth--
			if depth < 0 {
				return nil, false
			}
		case r == '|' && depth == 0:
			segments = append(segments, strings.TrimSpace(query[last:i]))
			last = i + 1
		}
	}
	if quote != 0 || depth != 0 {
		return nil, false
	}
	return append(segments, strings.TrimSpace(query[last:])), true
}

var (
	// limitPipe is a pipe that only caps or skips rows: dropped on a re-read.
	limitPipe = regexp.MustCompile(`(?i)^(limit|head|offset|skip)\s+\S+$`)
	// timeFirstLastPipe is `first|last N by (_time [asc|desc])`, a sort by
	// _time plus a limit: dropped on a re-read.
	timeFirstLastPipe = regexp.MustCompile(`(?i)^(first|last)\s+\S+\s+by\s*\(\s*_time(\s+(asc|desc))?\s*\)(\s+(asc|desc))?$`)
	// sortByPipe is `sort by (<keys>) [asc|desc]` with no inline limit or
	// offset: kept on a re-read. sortKeyItem is one `<field> [asc|desc]` key.
	sortByPipe  = regexp.MustCompile(`(?i)^sort\s+by\s*\(([^()]*)\)(\s+(asc|desc))?$`)
	sortKeyItem = regexp.MustCompile(`(?i)^([A-Za-z_][A-Za-z0-9_.\-]*)(\s+(asc|desc))?$`)
	// rowOrderPipe names every pipe that orders, samples or aggregates rows.
	// Outside the _time-only forms above, any of them makes the tie rule
	// inapplicable.
	rowOrderPipe = regexp.MustCompile(`(?i)^(sort|order|first|last|top|uniq|stats|sample|facets|field_values|field_names|block_stats|union|join|limit|head|offset|skip)\b`)
)

// sortPipeKeys parses `sort by (k1 [asc|desc], k2, ...) [asc|desc]` into its
// key names. ok is false for any other sort form (no `by`, an inline limit or
// offset, an expression) or when a key is not a plain field name.
func sortPipeKeys(seg string) (keys []string, ok bool) {
	m := sortByPipe.FindStringSubmatch(seg)
	if m == nil {
		return nil, false
	}
	for _, item := range strings.Split(m[1], ",") {
		km := sortKeyItem.FindStringSubmatch(strings.TrimSpace(item))
		if km == nil {
			return nil, false
		}
		keys = append(keys, km[1])
	}
	return keys, len(keys) > 0
}

// tieGroupQuery returns the query a tie group is re-read with: the case's
// query without the pipes that cap or skip rows, and the sort keys rows tie
// on ([_time] unless the case has a multi-key sort). ok is false when the case
// orders by anything this file cannot undo, or when its query cannot be split
// safely; the tie rule then does not apply and the comparison stays strict.
func tieGroupQuery(query string) (string, []string, bool) {
	segments, ok := splitTopLevelPipes(query)
	if !ok {
		return "", nil, false
	}
	kept := segments[:1]
	sortFields := []string{"_time"}
	sorts := 0
	for _, seg := range segments[1:] {
		switch {
		case limitPipe.MatchString(seg), timeFirstLastPipe.MatchString(seg):
			continue
		case sortByPipe.MatchString(seg):
			keys, ok := sortPipeKeys(seg)
			if !ok || sorts > 0 || !containsString(keys, "_time") {
				// An unparsable key, a second sort, or no _time key to window
				// the re-read by.
				return "", nil, false
			}
			sorts++
			if len(keys) > 1 || keys[0] != "_time" {
				sortFields = keys
			}
		case rowOrderPipe.MatchString(seg):
			return "", nil, false
		}
		kept = append(kept, seg)
	}
	return strings.Join(kept, " | "), sortFields, true
}

// timeOnly reports whether rows tie on _time alone (the default).
func timeOnly(sortFields []string) bool {
	return len(sortFields) == 0 || (len(sortFields) == 1 && sortFields[0] == "_time")
}

// tieWindowBefore/tieWindowAfter widen the re-read window around the tie.
// Divergence B7 makes the cold tier drop a row when the request's end bound
// falls in the same microsecond as the row (measured: end = T+1..T+500ns
// returns nothing, end past the next whole microsecond returns the row), so
// the window reaches two microseconds past the tie; keysAt then keeps only the
// rows at exactly the tie. B7 itself is asserted by its own test.
const (
	tieWindowBefore = time.Microsecond
	tieWindowAfter  = 2 * time.Microsecond
)

// newTieGroupFetcher returns a fetcher that re-issues a case against both
// tiers around one _time, or nil when the tie rule does not apply to the case
// (see tieGroupQuery).
func newTieGroupFetcher(refBase, sutBase, endpoint string, params url.Values) tieGroupFetcher {
	query, _, ok := tieGroupQuery(params.Get("query"))
	if !ok {
		return nil
	}
	return func(t *testing.T, at time.Time) (fetchResult, fetchResult) {
		t.Helper()
		p := url.Values{}
		for k, v := range params {
			p[k] = append([]string(nil), v...)
		}
		p.Set("query", query)
		p.Set("start", strconv.FormatInt(at.Add(-tieWindowBefore).UnixNano(), 10))
		p.Set("end", strconv.FormatInt(at.Add(tieWindowAfter).UnixNano(), 10))
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
// _time is the first or the last of the two answers and both sides differ by
// the same number of rows.
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
	lo, hi, ok := timeBounds(append(append([]string(nil), refKeys...), sutKeys...))
	if !ok {
		return time.Time{}, false
	}
	return at, at.Equal(lo) || at.Equal(hi)
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

// tieSortFields returns the sort keys rows of the case's query tie on, or nil
// when the tie rule does not apply to the query.
func tieSortFields(query string) []string {
	_, fields, ok := tieGroupQuery(query)
	if !ok {
		return nil
	}
	return fields
}

// explainedByTruncatedTie reports whether two answers that differ as row
// multisets differ only because a limit cut a tie group, re-reading the group
// through ties. sortFields are the keys rows tie on (nil or [_time]: _time
// alone). It returns the tie's _time and, when not explained, why.
func explainedByTruncatedTie(t *testing.T, ties tieGroupFetcher, refKeys, sutKeys, skipFields []string) (time.Time, string, bool) {
	t.Helper()
	if ties == nil {
		return time.Time{}, "the case does not order rows by _time alone, or its query cannot be re-read safely", false
	}
	refOnly, sutOnly := multisetDiff(refKeys, sutKeys)
	at, ok := boundaryTieTime(refKeys, sutKeys, refOnly, sutOnly)
	if !ok {
		return time.Time{}, "the differing rows are not an equal number on each side sharing one _time at the edge of both answers", false
	}
	why, ok := judgeTruncatedTieGroup(t, ties, at, refKeys, sutKeys, skipFields)
	return at, why, ok
}

// sortTuple renders a row's values on the sort keys, the identity of its tie
// group.
func sortTuple(row map[string]any, sortFields []string) string {
	parts := make([]string, len(sortFields))
	for i, f := range sortFields {
		parts[i] = fmt.Sprintf("%s=%v", f, row[f])
	}
	return strings.Join(parts, "|")
}

// rowsWithTuple returns the rows whose sort tuple is tuple.
func rowsWithTuple(rows []map[string]any, sortFields []string, tuple string) []map[string]any {
	var out []map[string]any
	for _, r := range rows {
		if sortTuple(r, sortFields) == tuple {
			out = append(out, r)
		}
	}
	return out
}

// explainedByTruncatedSortTie is explainedByTruncatedTie for a case ordered by
// several keys (`sort by (level, _time)`). Rows tie only when every sort key is
// equal: the differing rows must all carry one tuple of key values, and that
// tuple's group, re-read from both tiers, must be identical on both and larger
// than what each answer kept (so the limit cut it). A difference in any other
// way, including a row ordered wrongly on one key, stays a mismatch.
func explainedByTruncatedSortTie(t *testing.T, ties tieGroupFetcher, sortFields []string, refRows, sutRows []map[string]any, skipFields []string) (string, string, bool) {
	t.Helper()
	if ties == nil {
		return "", "the case's query cannot be re-read safely", false
	}
	keyRow := map[string]map[string]any{}
	keysOf := func(rows []map[string]any) []string {
		keys := extractRowKeys(rows, skipFields)
		for _, r := range rows {
			keyRow[extractRowKeys([]map[string]any{r}, skipFields)[0]] = r
		}
		return keys
	}
	refKeys, sutKeys := keysOf(refRows), keysOf(sutRows)
	refOnly, sutOnly := multisetDiff(refKeys, sutKeys)
	if len(refOnly) == 0 || len(refOnly) != len(sutOnly) {
		return "", "the differing rows are not an equal number on each side", false
	}
	tuple := ""
	for i, k := range append(append([]string(nil), refOnly...), sutOnly...) {
		tp := sortTuple(keyRow[k], sortFields)
		if i == 0 {
			tuple = tp
		} else if tp != tuple {
			return "", fmt.Sprintf("the differing rows do not share one value on every sort key (%s vs %s)", tuple, tp), false
		}
	}
	at, err := time.Parse(time.RFC3339Nano, fmt.Sprint(keyRow[refOnly[0]]["_time"]))
	if err != nil {
		return "", "the differing rows have no parsable _time", false
	}
	refRes, sutRes := ties(t, at)
	if refRes.StatusCode != 200 || sutRes.StatusCode != 200 {
		return "", fmt.Sprintf("re-reading the group returned status ref=%d sut=%d", refRes.StatusCode, sutRes.StatusCode), false
	}
	refGroup := extractRowKeys(rowsWithTuple(parseNDJSON(refRes.Body), sortFields, tuple), skipFields)
	sutGroup := extractRowKeys(rowsWithTuple(parseNDJSON(sutRes.Body), sortFields, tuple), skipFields)
	if gRef, gSut := multisetDiff(refGroup, sutGroup); len(gRef) != 0 || len(gSut) != 0 {
		return "", fmt.Sprintf("the full group differs between tiers: ref-only %v, sut-only %v", gRef, gSut), false
	}
	refKept := extractRowKeys(rowsWithTuple(refRows, sortFields, tuple), skipFields)
	sutKept := extractRowKeys(rowsWithTuple(sutRows, sortFields, tuple), skipFields)
	if len(refGroup) <= len(refKept) {
		return "", fmt.Sprintf("the group has %d rows and the answers kept %d, so no limit cut it", len(refGroup), len(refKept)), false
	}
	if !isSubMultiset(refKept, refGroup) || !isSubMultiset(sutKept, sutGroup) {
		return "", "a kept row of that group is not a member of it", false
	}
	return tuple, "", true
}
