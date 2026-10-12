package parquets3

import (
	"fmt"
	"math"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	vtinsertutil "github.com/VictoriaMetrics/VictoriaTraces/app/vtinsert/insertutil"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// Spans with a _time before 1970 (#388). The Parquet layout, the manifest and
// the drain's time range all start at the Unix epoch, so the insert path
// refuses such rows when they are added — with upstream's warning and a counter,
// as upstream refuses rows outside its retention — and never acknowledges a row
// the drain cannot store. The invariant: every row handed to the buffer is
// either refused then (counted) or reaches Parquet exactly once; none is
// acknowledged and lost later.

const tooSmallTimestamp = "too_small_timestamp"

// ackLedger records, for rows handed to the buffer, which were expected to be
// accepted and which to be refused.
type ackLedger struct {
	accepted map[string]bool
	refused  int
}

func newAckLedger() *ackLedger { return &ackLedger{accepted: map[string]bool{}} }

// sendViaAdapter hands rows (field sets with their _time) of tenant to the
// buffer the way an insert request does: through the insert adapter of the binary
// and upstream's log-message processor, in one batch.
func (e *segEnv) sendViaAdapter(tn logstorage.TenantID, streamField string, rows []adapterRow) {
	e.t.Helper()
	vlstorage.SetInsertStorage(e.segs, filepath.Join(e.dir, "buffer"))
	e.t.Cleanup(func() { vtinsertutil.SetLogRowsStorage(nil) })
	cp := &vtinsertutil.CommonParams{TenantID: tn, TimeFields: []string{"_time"}, StreamFields: []string{streamField}}
	lmp := cp.NewLogMessageProcessor("test", false)
	for _, r := range rows {
		lmp.AddRow(r.ts, r.fields, -1)
	}
	lmp.MustClose()
}

// adapterRow is one row of an insert request.
type adapterRow struct {
	ts     int64
	fields []logstorage.Field
}

// ingestTracked sends n rows of tenant with _time from ts on (1 ms apart) in one
// request and records each row in l by whether _time is before the epoch.
func (e *segEnv) ingestTracked(l *ackLedger, tn logstorage.TenantID, ts time.Time, n int) {
	e.t.Helper()
	rows := make([]adapterRow, 0, n)
	for i := 0; i < n; i++ {
		t := ts.Add(time.Duration(i) * time.Millisecond).UnixNano()
		msg := fmt.Sprintf("tracked-%d", e.seq.Add(1))
		rows = append(rows, adapterRow{ts: t, fields: []logstorage.Field{
			{Name: "resource_attr:service.name", Value: "api"},
			{Name: "span_id", Value: msg},
			{Name: "trace_id", Value: "trace-" + msg},
			{Name: "name", Value: "op"},
		}})
		if t < 0 {
			l.refused++
		} else {
			l.accepted[msg] = true
		}
	}
	e.sendViaAdapter(tn, "resource_attr:service.name", rows)
}

// mustStoreExactly fails unless the stored objects hold every accepted row of l
// once and nothing else.
func (e *segEnv) mustStoreExactly(l *ackLedger, when string) {
	e.t.Helper()
	got := e.storedMsgs()
	var missing, extra, dup []string
	for k := range l.accepted {
		switch c := got[k]; {
		case c == 0:
			missing = append(missing, k)
		case c > 1:
			dup = append(dup, fmt.Sprintf("%s×%d", k, c))
		}
	}
	for k := range got {
		if !l.accepted[k] {
			extra = append(extra, k)
		}
	}
	if len(missing)+len(extra)+len(dup) > 0 {
		sort.Strings(missing)
		sort.Strings(extra)
		e.t.Fatalf("%s: accepted %d rows, stored %d; missing %d %v; unexpected %d %v; duplicated %v",
			when, len(l.accepted), len(got), len(missing), head(missing, 5), len(extra), head(extra, 5), head(dup, 5))
	}
	if n := len(e.segs.Pending()); n != 0 {
		e.t.Fatalf("%s: %d segments still pending", when, n)
	}
}

func refusedNow() uint64 { return metrics.InsertRowsDropped.Get(tooSmallTimestamp) }

// The reproduction of #388: 5 rows of 1965 and 5 of the last hour are handed to
// the buffer. All 10 were acknowledged and the segment was committed with 5 in
// Parquet. Now the 5 rows of 1965 are refused when added, counted, and every
// row that was accepted reaches Parquet.
func TestPre1970Rows_AreRefusedAtTheBufferNotLostAtTheDrain(t *testing.T) {
	e := newSegEnv(t)
	l := newAckLedger()
	before := refusedNow()
	e.ingestTracked(l, segTenantA, time.Date(1965, 3, 1, 12, 0, 0, 0, time.UTC), 5)
	e.ingestTracked(l, segTenantA, hourAgo, 5)
	held := e.segs.Active().Rows() // what the buffer has taken responsibility for
	e.seal()
	e.drainAll(e.flusher(1000))
	e.mustStoreExactly(l, "after the drain")
	if stored := int64(len(e.storedMsgs())); stored != held {
		t.Fatalf("the buffer held %d acknowledged rows, Parquet has %d: %d rows committed without reaching Parquet", held, stored, held-stored)
	}
	if got := refusedNow() - before; got != 5 {
		t.Fatalf("lakehouse_insert_rows_dropped_total{reason=%q} rose by %d, want 5", tooSmallTimestamp, got)
	}
}

// The edge of the accepted range: the Unix epoch itself is stored, one
// nanosecond before it is refused, and a batch of only refused rows adds
// nothing (the segment stays empty, so it is not sealed or drained).
func TestPre1970Rows_EpochEdgeAndAllRefusedBatch(t *testing.T) {
	e := newSegEnv(t)
	l := newAckLedger()
	before := refusedNow()
	e.ingestTracked(l, segTenantA, time.Unix(0, -1), 1)            // refused
	e.ingestTracked(l, segTenantA, time.Unix(0, 0), 1)             // the epoch: stored
	e.ingestTracked(l, segTenantA, time.Unix(0, math.MinInt64), 1) // refused
	e.ingestTracked(l, segTenantA, time.Unix(0, 1), 1)             // stored
	if got := e.segs.Active().Rows(); got != 2 {
		t.Fatalf("the active segment holds %d rows, want the 2 accepted", got)
	}
	e.seal()
	e.drainAll(e.flusher(1000))
	e.mustStoreExactly(l, "epoch edge")
	if got := refusedNow() - before; got != 2 {
		t.Fatalf("refused counter rose by %d, want 2", got)
	}

	only := newAckLedger()
	e.ingestTracked(only, segTenantA, time.Date(1901, 1, 1, 0, 0, 0, 0, time.UTC), 3)
	if got := e.segs.Active().Rows(); got != 0 {
		t.Fatalf("a batch of only refused rows left %d rows in the active segment", got)
	}
	if _, ok := e.segs.Seal(); ok {
		t.Fatal("a batch of only refused rows made a segment to seal")
	}
}

// What the buffer refuses it does not serve either: the rows are in no segment.
func TestPre1970Rows_AreNotReadableFromTheBuffer(t *testing.T) {
	e := newSegEnv(t)
	l := newAckLedger()
	e.ingestTracked(l, segTenantA, time.Date(1965, 3, 1, 12, 0, 0, 0, time.UTC), 4)
	e.ingestTracked(l, segTenantA, hourAgo, 2)
	if got := e.segs.Active().Rows(); got != 2 {
		t.Fatalf("the buffer holds %d rows, want 2", got)
	}
}

// Property: for random batches (random tenants, sizes, and a _time drawn from
// before the epoch, around it, in the past and now), across restarts and
// partial drains, every row handed to the buffer is refused and counted, or is
// stored exactly once.
func TestPre1970Rows_PropertyEveryRowIsRefusedOrStoredOnce(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			e := newSegEnv(t)
			l := newAckLedger()
			before := refusedNow()
			tenants := []logstorage.TenantID{segTenantA, segTenantB, {AccountID: 7, ProjectID: 0}}
			f := e.flusher(40)
			for step := 0; step < 14; step++ {
				switch rng.Intn(5) {
				case 0:
					if e.segs.Active().Rows() > 0 {
						e.seal()
					}
					e.drainAll(f)
				case 1:
					if e.segs.Active().Rows() > 0 {
						e.seal()
					}
					e.restart()
					f = e.flusher(40)
				default:
					e.ingestTracked(l, tenants[rng.Intn(len(tenants))], randomTime(rng), 1+rng.Intn(9))
				}
			}
			if e.segs.Active().Rows() > 0 {
				e.seal()
			}
			e.drainAll(f)
			e.mustStoreExactly(l, "end")
			if got := refusedNow() - before; got != uint64(l.refused) {
				t.Fatalf("refused counter rose by %d, want %d", got, l.refused)
			}
		})
	}
}

// randomTime draws a _time from the places that matter: far before the epoch,
// just before and after it, the distant past within the buffer's retention, and
// recent.
func randomTime(rng *rand.Rand) time.Time {
	switch rng.Intn(6) {
	case 0:
		return time.Unix(0, -rng.Int63n(math.MaxInt64))
	case 1:
		return time.Unix(0, -rng.Int63n(int64(48*time.Hour)))
	case 2:
		return time.Unix(0, rng.Int63n(int64(48*time.Hour)))
	case 3:
		return time.Now().Add(-time.Duration(rng.Int63n(int64(20 * 365 * 24 * time.Hour))))
	default:
		return time.Now().Add(-time.Duration(rng.Int63n(int64(6 * time.Hour))))
	}
}
