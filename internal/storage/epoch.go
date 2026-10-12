package storage

import (
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// ReasonTooSmallTimestamp is the reason label of
// lakehouse_insert_rows_dropped_total for a row whose _time is before the Unix
// epoch (upstream's name for a row below its time window).
const ReasonTooSmallTimestamp = "too_small_timestamp"

// ReasonCardinalityLimit is the reason label of lakehouse_insert_rows_dropped_total
// for a row of a stream the tenant's cardinality limit (cardinality.max_streams)
// does not admit.
const ReasonCardinalityLimit = "cardinality_limit"

var (
	beforeEpochLogger = logger.WithThrottler("too_small_timestamp", 5*time.Second)
	cardinalityLogger = logger.WithThrottler("cardinality_limit", 5*time.Second)
)

// BeforeEpoch reports whether a row's _time (Unix nanoseconds) is before the
// Unix epoch. The Lakehouse time domain starts at the epoch: Parquet partitions
// are named from it, the manifest uses 0 for an unknown bound, and the insert
// buffer is drained over [0, MaxInt64]. A row before it could be acknowledged
// but never written, so both binaries refuse it when it is added, exactly where
// upstream drops a row outside its time window: counted, logged (throttled),
// and the request still answered as a success.
func BeforeEpoch(ts int64) bool { return ts < 0 }

// NoteBeforeEpoch counts a row refused for BeforeEpoch and logs it with
// upstream's wording (at most once per 5 s).
func NoteBeforeEpoch(r *logstorage.InsertRow) {
	metrics.InsertRowsDropped.Inc(ReasonTooSmallTimestamp)
	tsf := logstorage.TimeFormatter(r.Timestamp)
	beforeEpochLogger.Warnf("skipping log entry with too small timestamp=%s; it must not be before the Unix epoch (1970-01-01T00:00:00Z), "+
		"the start of the time range the Lakehouse stores; log entry: %s", &tsf, logstorage.MarshalFieldsToJSON(nil, r.Fields))
}

// NoteCardinalityRefused counts rows of a stream that the tenant's cardinality
// limit does not admit (lakehouse_insert_rows_dropped_total{reason="cardinality_limit"})
// and logs it (at most once per 5 s). The request that carried the rows is still
// answered as a success: the limit has always dropped the rows silently, and an
// operator learns of it from this counter and the warning. The limiter keeps its
// set of streams in the memory of one process, so a restart empties it and a
// stream that was admitted before can be refused afterwards (#485).
func NoteCardinalityRefused(accountID, projectID uint32, stream string, rows int) {
	if rows <= 0 {
		return
	}
	metrics.InsertRowsDropped.Add(ReasonCardinalityLimit, rows)
	cardinalityLogger.Warnf("skipping %d log entries of stream %s of tenant %d:%d: the tenant is at its stream cardinality limit (cardinality.max_streams); "+
		"the limiter's set of streams is kept in memory and starts empty after a restart", rows, stream, accountID, projectID)
}
