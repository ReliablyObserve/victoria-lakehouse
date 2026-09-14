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
var parsedFilters sync.Map // query string -> *logstorage.Filter (nil when unparseable)

func parseFilterCached(query string) *logstorage.Filter {
	if v, ok := parsedFilters.Load(query); ok {
		f, _ := v.(*logstorage.Filter)
		return f
	}
	f, err := logstorage.ParseFilter(query)
	if err != nil {
		parsedFilters.Store(query, (*logstorage.Filter)(nil))
		return nil
	}
	parsedFilters.Store(query, f)
	return f
}
