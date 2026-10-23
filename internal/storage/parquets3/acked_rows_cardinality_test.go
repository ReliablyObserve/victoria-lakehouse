package parquets3

import (
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/storage"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/vlstorage"
)

// The tenant cardinality limit is decided once, when a row is added (#389). The
// limiter lives in one process and starts empty: judging the rows again when a
// segment is drained, in a process that has since restarted, dropped rows that
// had been admitted and acknowledged. The drain keeps only the stateless
// filters. The matrix below acknowledges rows under one limiter, restarts at
// every point of the life of a segment, lets a fresh limiter admit other
// streams first, and requires every acknowledged row in Parquet exactly once and
// every refused row absent from it.
//
// What a stream's limit is, is stated by the tests themselves in limitModel (a
// tenant admits its first N distinct streams, per process), not read back from
// the limiter under test: the rows the insert adapter really added to the buffer
// and the rows it counted as refused are compared to the model after every
// request, so an adapter that admits too much or too little is caught there.

const cardinalityLimit = storage.ReasonCardinalityLimit

// ackLimits are the cardinality.max_streams overrides of the tests; a tenant
// without an entry has no limit.
func ackLimits() map[logstorage.TenantID]int {
	return map[logstorage.TenantID]int{segTenantA: 1, segTenantB: 2}
}

func policyFor(t *testing.T, limits map[logstorage.TenantID]int) *tenant.PolicyRegistry {
	t.Helper()
	ov := map[string]config.TenantOverride{}
	for tn, n := range limits {
		ov[fmt.Sprintf("%d:%d", tn.AccountID, tn.ProjectID)] = config.TenantOverride{Cardinality: config.TenantCardinalityOverride{MaxStreams: n}}
	}
	p, err := tenant.NewPolicyRegistry(ov, nil)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	return p
}

// limitModel is the test's own statement of the rule: within one process, a
// tenant with a limit of N admits the first N distinct streams it is sent and
// refuses every row of any other stream; a tenant without a limit admits all. A
// restart forgets the streams.
type limitModel struct {
	limits map[logstorage.TenantID]int
	seen   map[logstorage.TenantID]map[string]bool
}

func newLimitModel(limits map[logstorage.TenantID]int) *limitModel {
	m := &limitModel{limits: limits}
	m.restart()
	return m
}

func (m *limitModel) restart() { m.seen = map[logstorage.TenantID]map[string]bool{} }

// admits records that stream app of tn was sent and says whether its rows are
// admitted.
func (m *limitModel) admits(tn logstorage.TenantID, app string) bool {
	n := m.limits[tn]
	if n <= 0 {
		return true
	}
	if m.seen[tn] == nil {
		m.seen[tn] = map[string]bool{}
	}
	if m.seen[tn][app] {
		return true
	}
	if len(m.seen[tn]) >= n {
		return false
	}
	m.seen[tn][app] = true
	return true
}

// ackSet is the outcome of requests as the model predicts and the buffer
// confirms: the rows that were acknowledged into the buffer and the rows that
// were refused (answered as a success and not stored).
type ackSet struct{ acked, refused []string }

func (a *ackSet) add(o ackSet) {
	a.acked = append(a.acked, o.acked...)
	a.refused = append(a.refused, o.refused...)
}

// ingestApp sends n rows of stream <app> of tenant tn through the insert adapter
// of the binary, whose gate is the process's limiter. It checks what the adapter
// did against the model m: the rows in the active segment and the refusal
// counter must move by n in the way the model says, and then returns the rows as
// acknowledged or refused.
func (e *segEnv) ingestApp(m *limitModel, tn logstorage.TenantID, app string, ts time.Time, n int) ackSet {
	e.t.Helper()
	rows := make([]adapterRow, 0, n)
	var msgs []string
	for i := 0; i < n; i++ {
		msg := fmt.Sprintf("%s-%d-%d", app, tn.AccountID, e.seq.Add(1))
		rows = append(rows, adapterRow{ts: ts.Add(time.Duration(i) * time.Millisecond).UnixNano(), fields: ackFields(app, msg)})
		msgs = append(msgs, msg)
	}
	admit := m.admits(tn, app)
	rows0, refused0 := e.segs.Active().Rows(), metrics.InsertRowsDropped.Get(cardinalityLimit)
	e.sendViaAdapter(tn, rows)
	gotRows := e.segs.Active().Rows() - rows0
	gotRefused := metrics.InsertRowsDropped.Get(cardinalityLimit) - refused0
	switch {
	case admit && (gotRows != int64(n) || gotRefused != 0):
		e.t.Fatalf("stream %s of tenant %d:%d: the rule admits it, but the buffer took %d of %d rows and %d were counted as refused",
			app, tn.AccountID, tn.ProjectID, gotRows, n, gotRefused)
	case !admit && (gotRows != 0 || gotRefused != uint64(n)):
		e.t.Fatalf("stream %s of tenant %d:%d: the rule refuses it, but the buffer took %d of %d rows and %d were counted as refused",
			app, tn.AccountID, tn.ProjectID, gotRows, n, gotRefused)
	}
	if admit {
		return ackSet{acked: msgs}
	}
	return ackSet{refused: msgs}
}

// mustStoreAll fails unless every acknowledged row is stored exactly once and no
// refused row is stored at all.
func (e *segEnv) mustStoreAll(a ackSet, when string) {
	e.t.Helper()
	got := e.storedMsgs()
	var missing, dup, leaked []string
	for _, m := range a.acked {
		switch c := got[m]; {
		case c == 0:
			missing = append(missing, m)
		case c > 1:
			dup = append(dup, m)
		}
	}
	for _, m := range a.refused {
		if got[m] > 0 {
			leaked = append(leaked, m)
		}
	}
	if len(missing)+len(dup)+len(leaked) > 0 {
		sort.Strings(missing)
		e.t.Fatalf("%s: %d acknowledged rows, %d stored; missing %d %v; duplicated %v; refused rows stored: %d %v (pending segments: %d)",
			when, len(a.acked), len(got), len(missing), head(missing, 5), head(dup, 5), len(leaked), head(leaked, 5), len(e.segs.Pending()))
	}
	if n := len(e.segs.Pending()); n != 0 {
		e.t.Fatalf("%s: %d segments still pending", when, n)
	}
}

// useLimiter installs a fresh limiter as the process's gate, the way a restarted
// process has one, and makes the flusher use the process's flush filter.
func (e *segEnv) useLimiter(p *tenant.PolicyRegistry) {
	vlstorage.SetCardinalityGate(tenant.NewCardinalityLimiter(p))
	e.keep = FlushRowFilter(vlstorage.FlushRowKeeper())
}

// The reproduction of #389 as a matrix over where the restart falls.
func TestAckedRows_NotRejudgedByAFreshCardinalityLimiter(t *testing.T) {
	cases := []struct {
		name     string
		restarts bool
		// run is the life of the segment after the rows of process 1 were added.
		run func(e *segEnv)
	}{
		{"no restart (control)", false, func(e *segEnv) {
			e.seal()
		}},
		{"restart while the segment is active", true, func(e *segEnv) {
			e.restart()
		}},
		{"restart after the seal", true, func(e *segEnv) {
			e.seal()
			e.restart()
		}},
		// Every restart in this test is a clean close of the buffer (the segment
		// tests call Segments.Close), so this is an upload failure in the middle
		// of a drain followed by a clean restart, not a crash of the process.
		{"upload failure in the middle of the drain, then a clean restart", true, func(e *segEnv) {
			e.seal()
			uploads := 0
			e.failFn.Store(func(key string) bool {
				if !strings.Contains(key, "_segments/") {
					uploads++
				}
				return uploads > 1
			})
			// The same group size as the drain after the restart: the plan of a segment depends on it, and the objects
			// of the first attempt are the ones the restart recognises. Each tenant is a group, so the drain stops
			// after the first upload.
			f := e.flusher(1000)
			e.drainAll(f)
			e.failFn.Store(func(string) bool { return false })
			e.restart()
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer vlstorage.SetCardinalityGate(nil)
			e := newSegEnv(t)
			policy := policyFor(t, ackLimits())
			m := newLimitModel(ackLimits())

			// Process 1: stream a of tenant 1:2 and streams x, y of tenant 2:3
			// are admitted and acknowledged; stream z of 2:3 is over its limit.
			e.useLimiter(policy)
			var all ackSet
			all.add(e.ingestApp(m, segTenantA, "a", hourAgo, 12))
			all.add(e.ingestApp(m, segTenantB, "x", hourAgo, 7))
			all.add(e.ingestApp(m, segTenantB, "y", hourAgo, 7))
			z := e.ingestApp(m, segTenantB, "z", hourAgo, 3)
			if len(z.refused) != 3 {
				t.Fatalf("a third stream of a tenant with room for two was admitted")
			}
			all.add(z)

			c.run(e)

			// Process 2: its limiter is empty, and other streams arrive first
			// and take the room before the drain reaches the old rows.
			if c.restarts {
				e.useLimiter(policy)
				m.restart()
			}
			all.add(e.ingestApp(m, segTenantA, "b", hourAgo, 5))
			all.add(e.ingestApp(m, segTenantB, "p", hourAgo, 4))
			all.add(e.ingestApp(m, segTenantB, "q", hourAgo, 4))

			// Stream a, admitted in process 1, is sent again. Without a restart
			// the limiter still knows it. After a restart the fresh limiter has
			// given the tenant's only slot to b, so new rows of a are refused:
			// they are answered as a success, counted under
			// lakehouse_insert_rows_dropped_total{reason="cardinality_limit"} and
			// not stored. This is the known gap of #485 (the limiter's set of
			// streams is per process); the assertion pins today's behaviour so a
			// change of it is a decision, and the rows already acknowledged are
			// still never lost (checked below).
			again := e.ingestApp(m, segTenantA, "a", hourAgo, 2)
			if c.restarts && len(again.acked) != 0 {
				t.Fatalf("known gap #485 changed: a stream admitted before the restart was admitted again after it")
			}
			if !c.restarts && len(again.refused) != 0 {
				t.Fatalf("without a restart the limiter must still know stream a, but it refused %d rows", len(again.refused))
			}
			all.add(again)

			if e.segs.Active().Rows() > 0 {
				e.seal()
			}
			e.drainAll(e.flusher(1000))
			e.mustStoreAll(all, c.name)
		})
	}
}

// The filters that stay at the drain are stateless: whatever the limiter has
// seen, and whatever its limits, the flush filter keeps every stream a user can
// send to the logs endpoint (the trace-shaped drop of the logs binary is the only
// filter, and those rows are refused when added as well).
func TestAckedRows_FlushFilterNeverDropsOnCardinality(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	defer vlstorage.SetCardinalityGate(nil)
	for iter := 0; iter < 200; iter++ {
		limit := rng.Intn(4)
		lim := tenant.NewCardinalityLimiter(policyFor(t, map[logstorage.TenantID]int{{AccountID: 1, ProjectID: 2}: limit}))
		vlstorage.SetCardinalityGate(lim)
		keep := vlstorage.FlushRowKeeper()
		for i := 0; i < 1+rng.Intn(12); i++ {
			s := fmt.Sprintf(`{%s="s%d"}`, ackStreamField, rng.Intn(9))
			admitted := lim.AllowStream(1, 2, s) // what the insert adapter asks
			if !keep(1, 2, s) {
				t.Fatalf("iteration %d: the flush filter dropped %s at limit %d (admitted at ingest: %v); only a stateless filter may drop at the drain", iter, s, limit, admitted)
			}
		}
	}
}

// Property: random tenants, streams and limits, with restarts and seals at
// random points. Which rows each process's limiter admits is decided by the
// model; the rows the adapter took are checked against it on every request, all
// admitted rows reach Parquet once and none of the refused ones does.
func TestAckedRows_PropertyRandomRestartsAndLimits(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			defer vlstorage.SetCardinalityGate(nil)
			rng := rand.New(rand.NewSource(seed))
			e := newSegEnv(t)
			limits := map[logstorage.TenantID]int{segTenantA: 1 + rng.Intn(3), segTenantB: 1 + rng.Intn(3)}
			policy := policyFor(t, limits)
			m := newLimitModel(limits)
			tenants := []logstorage.TenantID{segTenantA, segTenantB, {AccountID: 9, ProjectID: 9}} // the last has no limit
			e.useLimiter(policy)
			f := e.flusher(25)
			var all ackSet
			for step := 0; step < 16; step++ {
				switch rng.Intn(6) {
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
					e.useLimiter(policy)
					m.restart()
					f = e.flusher(25)
				default:
					tn := tenants[rng.Intn(len(tenants))]
					app := fmt.Sprintf("app%d", rng.Intn(5))
					all.add(e.ingestApp(m, tn, app, hourAgo.Add(time.Duration(rng.Intn(120))*time.Minute), 1+rng.Intn(8)))
				}
			}
			if e.segs.Active().Rows() > 0 {
				e.seal()
			}
			e.drainAll(f)
			e.mustStoreAll(all, "end")
		})
	}
}
