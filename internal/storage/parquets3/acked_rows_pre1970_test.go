package parquets3

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Rows with a _time before 1970 (#388). The Parquet layout, the manifest and
// the drain's time range all start at the Unix epoch, so the insert path
// refuses such rows when they are added — with upstream's warning and a counter,
// as upstream refuses rows outside its retention — and never acknowledges a row
// the drain cannot store. The invariant: every row handed to the buffer is
// either refused then (counted) or reaches Parquet exactly once; none is
// acknowledged and lost later.
//
// The tests do not decide by the rule "_time >= 0" which rows the buffer holds:
// after each request they read the buffer back with a query, and the rows found
// there are the ones the buffer took responsibility for. Every one of them must
// reach Parquet; the rule is checked on top of that, so a loss shows up as a
// missing row, not only as a wrong counter.

const tooSmallTimestamp = "too_small_timestamp"

// ackLedger records, for rows handed to the buffer through the insert adapter,
// what the buffer held afterwards.
type ackLedger struct {
	held     map[string]bool // read back from the buffer after the request
	storable map[string]bool // _time >= 0: the rows the rule says the buffer takes
	sent     int
}

func newAckLedger() *ackLedger {
	return &ackLedger{held: map[string]bool{}, storable: map[string]bool{}}
}

// refused is the number of rows sent that the buffer did not take.
func (l *ackLedger) refused() int { return l.sent - len(l.held) }

// ingestTracked sends n rows of tenant with _time from ts on (1 ms apart) in one
// request, and records which of them the buffer holds afterwards.
func (e *segEnv) ingestTracked(l *ackLedger, tn logstorage.TenantID, ts time.Time, n int) {
	e.t.Helper()
	rows := make([]adapterRow, 0, n)
	var msgs []string
	for i := 0; i < n; i++ {
		t := ts.Add(time.Duration(i) * time.Millisecond).UnixNano()
		msg := fmt.Sprintf("tracked-%d", e.seq.Add(1))
		rows = append(rows, adapterRow{ts: t, fields: ackFields("api", msg)})
		msgs = append(msgs, msg)
		if t >= 0 {
			l.storable[msg] = true
		}
	}
	before := e.bufferMsgs()
	e.sendViaAdapter(tn, rows)
	after := e.bufferMsgs()
	// A just-added row can take a moment to be searchable even after a flush:
	// wait (bounded) until every row the rule says is stored shows up, so that
	// "not found" means refused, not "not indexed yet".
	for try := 0; try < 40 && !allSeen(after, before, msgs, l.storable); try++ {
		time.Sleep(25 * time.Millisecond)
		after = e.bufferMsgs()
	}
	l.sent += n
	for _, m := range msgs {
		if after[m] > before[m] {
			l.held[m] = true
		}
	}
}

// allSeen reports whether every message in msgs that the rule marks as storable
// is found by the query result after but was not in before.
func allSeen(after, before map[string]int, msgs []string, storable map[string]bool) bool {
	for _, m := range msgs {
		if storable[m] && after[m] <= before[m] {
			return false
		}
	}
	return true
}

// mustStoreExactly fails unless the stored objects hold every row the buffer
// held, once, and nothing else; and unless the rows the buffer held are the rows
// the epoch rule allows.
func (e *segEnv) mustStoreExactly(l *ackLedger, when string) {
	e.t.Helper()
	got := e.storedMsgs()
	var missing, extra, dup []string
	for k := range l.held {
		switch c := got[k]; {
		case c == 0:
			missing = append(missing, k)
		case c > 1:
			dup = append(dup, fmt.Sprintf("%s×%d", k, c))
		}
	}
	for k := range got {
		if !l.held[k] {
			extra = append(extra, k)
		}
	}
	if len(missing)+len(extra)+len(dup) > 0 {
		sort.Strings(missing)
		sort.Strings(extra)
		e.t.Fatalf("%s: the buffer held %d rows, Parquet has %d; held but missing from Parquet: %d %v; in Parquet but not held: %d %v; duplicated %v",
			when, len(l.held), len(got), len(missing), head(missing, 5), len(extra), head(extra, 5), head(dup, 5))
	}
	if n := len(e.segs.Pending()); n != 0 {
		e.t.Fatalf("%s: %d segments still pending", when, n)
	}
	var over, under []string
	for k := range l.held {
		if !l.storable[k] {
			over = append(over, k)
		}
	}
	for k := range l.storable {
		if !l.held[k] {
			under = append(under, k)
		}
	}
	if len(over)+len(under) > 0 {
		sort.Strings(over)
		sort.Strings(under)
		e.t.Fatalf("%s: the buffer holds a row it should have refused: %d %v; refused a row it should hold: %d %v",
			when, len(over), head(over, 5), len(under), head(under, 5))
	}
}

func refusedNow() uint64 { return metrics.InsertRowsDropped.Get(tooSmallTimestamp) }

// The reproduction of #388: 5 rows of 1965 and 5 of the last hour are handed to
// the buffer. All 10 were acknowledged and the segment was committed with 5 in
// Parquet. Now the 5 rows of 1965 are refused when added, counted, and every
// row the buffer holds reaches Parquet.
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

// What the buffer refuses it does not serve either. The check is a real query
// over the whole of int64 time against the buffer's segments (what a read from
// the buffer runs), not the segment's row counter: the rows before the epoch
// are in no segment, the others are all there.
func TestPre1970Rows_AreNotReadableFromTheBuffer(t *testing.T) {
	e := newSegEnv(t)
	l := newAckLedger()
	e.ingestTracked(l, segTenantA, time.Date(1965, 3, 1, 12, 0, 0, 0, time.UTC), 4)
	e.ingestTracked(l, segTenantA, hourAgo, 2)
	got := e.bufferMsgs()
	if len(got) != 2 {
		t.Fatalf("a query over all time finds %d rows in the buffer, want the 2 accepted: %v", len(got), got)
	}
	for m := range got {
		if !l.storable[m] {
			t.Errorf("the buffer serves %s, a row before the epoch", m)
		}
	}
	// Still unreadable after the seal and the drain.
	e.seal()
	if got := e.bufferMsgs(); len(got) != 2 {
		t.Fatalf("after the seal a query finds %d rows, want 2", len(got))
	}
	e.drainAll(e.flusher(1000))
	e.mustStoreExactly(l, "after the drain")
}

// Property: for random batches (random tenants, sizes, and a _time drawn from
// before the epoch, around it, in the past and now), across restarts and
// partial drains, every row handed to the buffer is refused and counted, or is
// stored exactly once. The rows the buffer holds are read back with a query
// after every request; both the loss (a held row missing from Parquet) and the
// refusal rule are checked on that, not on the rule alone.
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
			if got := refusedNow() - before; got != uint64(l.refused()) {
				t.Fatalf("refused counter rose by %d, the buffer refused %d rows", got, l.refused())
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
