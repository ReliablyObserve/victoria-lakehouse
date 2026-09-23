package delete

import (
	"sync"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// parsedFilters memoises logstorage.ParseFilter by query string.
//
// Tombstone.MatchesRow is called once per candidate ROW — by the query row
// filter, by the rewriter, by the compactor and by the field enumeration paths.
// Re-parsing the LogsQL expression on every one of those calls dominated the
// cost of having any tombstone at all (a parse is orders of magnitude more
// expensive than the match it enables). The parse is a pure function of the
// query string, so the result is cached forever; the set of distinct tombstone
// queries is bounded by the number of delete requests an operator has issued,
// and each entry is a few hundred bytes.
//
// A query that fails to parse is cached as a nil filter so a malformed
// tombstone costs one parse, not one per row. A nil filter never matches, which
// is the fail-closed direction: a tombstone we cannot evaluate hides nothing
// rather than hiding everything.
//
// A tombstone's relative time filters (`_time:5m`) are evaluated at the
// timestamp the delete was issued at (Tombstone.FilterAt), exactly as upstream
// evaluates a delete task's filter at the task's start time
// (lib/logstorage/storage.go, processDeleteTask: ParseFilterAtTimestamp). The
// cache is keyed by (query, timestamp), so the parse stays pure and the window
// a tombstone covers never drifts with the clock or a restart.
var parsedFilters sync.Map // filterKey -> *logstorage.Filter (nil when unparseable)

type filterKey struct {
	query string
	at    int64
}

func parseFilterCached(query string, at int64) *logstorage.Filter {
	k := filterKey{query: query, at: at}
	if v, ok := parsedFilters.Load(k); ok {
		f, _ := v.(*logstorage.Filter)
		return f
	}
	f, err := parseFilterAt(query, at)
	if err != nil {
		parsedFilters.Store(k, (*logstorage.Filter)(nil))
		return nil
	}
	parsedFilters.Store(k, f)
	return f
}

// forgetFilterLocked drops the cached parse of ts's filter unless another
// tombstone of the store still uses it, which keeps the cache bounded by the
// live tombstones. It is not exact — the cache is process-wide, and a reader
// holding a snapshot copy of a removed tombstone can repopulate an entry — but
// an entry is a pure function of (query, timestamp), so it can never produce a
// wrong match. Caller holds s.mu (write) and has already removed ts.
func (s *TombstoneStore) forgetFilterLocked(ts Tombstone) {
	k := filterKey{query: ts.Query, at: ts.FilterAt}
	for _, other := range s.tombstones {
		if other.Query == k.query && other.FilterAt == k.at {
			return
		}
	}
	parsedFilters.Delete(k)
}

// parseFilterAt parses query with relative time filters evaluated at at (unix
// nanoseconds), upstream's ParseFilterAtTimestamp; at == 0 is "now".
func parseFilterAt(query string, at int64) (*logstorage.Filter, error) {
	if at == 0 {
		return logstorage.ParseFilter(query)
	}
	return logstorage.ParseFilterAtTimestamp(query, at)
}
