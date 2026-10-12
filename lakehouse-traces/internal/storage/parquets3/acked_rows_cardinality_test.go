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
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// The tenant cardinality limit is decided once, when a row is added (#389). The
// limiter lives in one process and starts empty: judging the rows again when a
// segment is drained, in a process that has since restarted, dropped rows that
// had been admitted and acknowledged. The drain keeps only the stateless
// filters. The matrix below acknowledges rows under one limiter, restarts at
// every point of the life of a segment, lets a fresh limiter admit other
// streams first, and requires every acknowledged row in Parquet exactly once.

// limitedPolicy gives tenant 1:2 room for one stream and 2:3 room for two.
func limitedPolicy(t *testing.T) *tenant.PolicyRegistry {
	t.Helper()
	p, err := tenant.NewPolicyRegistry(map[string]config.TenantOverride{
		"1:2": {Cardinality: config.TenantCardinalityOverride{MaxStreams: 1}},
		"2:3": {Cardinality: config.TenantCardinalityOverride{MaxStreams: 2}},
	}, nil)
	if err != nil {
		t.Fatalf("policy: %v", err)
	}
	return p
}

// ingestApp sends n rows of stream {app=<app>} of tenant through the insert
// adapter, whose gate is lim. It returns the _msg of the rows the adapter
// admitted (acknowledged into the buffer): none if the stream is over the
// tenant's limit.
func (e *segEnv) ingestApp(lim *tenant.CardinalityLimiter, tn logstorage.TenantID, app string, ts time.Time, n int) []string {
	e.t.Helper()
	rows := make([]adapterRow, 0, n)
	var msgs []string
	for i := 0; i < n; i++ {
		msg := fmt.Sprintf("%s-%d-%d", app, tn.AccountID, e.seq.Add(1))
		rows = append(rows, adapterRow{ts: ts.Add(time.Duration(i) * time.Millisecond).UnixNano(), fields: []logstorage.Field{
			{Name: "resource_attr:service.name", Value: app}, {Name: "span_id", Value: msg},
			{Name: "trace_id", Value: "trace-" + msg}, {Name: "name", Value: "op"},
		}})
		msgs = append(msgs, msg)
	}
	e.sendViaAdapter(tn, "resource_attr:service.name", rows)
	// The adapter admitted the stream iff the limiter knows it now: asking again
	// admits a stream with room and refuses one without, as it did.
	if !lim.AllowStream(tn.AccountID, tn.ProjectID, fmt.Sprintf(`{resource_attr:service.name=%q}`, app)) {
		return nil
	}
	return msgs
}

// mustStoreAll fails unless every message in acked is stored exactly once.
func (e *segEnv) mustStoreAll(acked []string, when string) {
	e.t.Helper()
	got := e.storedMsgs()
	var missing, dup []string
	for _, m := range acked {
		switch c := got[m]; {
		case c == 0:
			missing = append(missing, m)
		case c > 1:
			dup = append(dup, m)
		}
	}
	if len(missing)+len(dup) > 0 {
		sort.Strings(missing)
		e.t.Fatalf("%s: %d acknowledged rows, %d stored; missing %d %v; duplicated %v (pending segments: %d)",
			when, len(acked), len(got), len(missing), head(missing, 5), head(dup, 5), len(e.segs.Pending()))
	}
	if n := len(e.segs.Pending()); n != 0 {
		e.t.Fatalf("%s: %d segments still pending", when, n)
	}
}

// useLimiter installs a fresh limiter as the process's gate, the way a restarted
// process has one, and makes the flusher use the process's flush filter.
func (e *segEnv) useLimiter(p *tenant.PolicyRegistry) *tenant.CardinalityLimiter {
	l := tenant.NewCardinalityLimiter(p)
	vlstorage.SetCardinalityGate(l)
	e.keep = FlushRowFilter(vlstorage.FlushRowKeeper())
	return l
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
		{"crash in the middle of the drain", true, func(e *segEnv) {
			e.seal()
			uploads := 0
			e.failFn.Store(func(key string) bool {
				if !strings.Contains(key, "_segments/") {
					uploads++
				}
				return uploads > 1
			})
			f := e.flusher(5) // small groups: the drain stops after the first upload
			e.drainAll(f)
			e.failFn.Store(func(string) bool { return false })
			e.restart()
		}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			defer vlstorage.SetCardinalityGate(nil)
			e := newSegEnv(t)
			policy := limitedPolicy(t)

			// Process 1: stream a of tenant 1:2 and streams x, y of tenant 2:3
			// are admitted and acknowledged; stream z of 2:3 is over its limit.
			l1 := e.useLimiter(policy)
			var acked []string
			acked = append(acked, e.ingestApp(l1, segTenantA, "a", hourAgo, 12)...)
			acked = append(acked, e.ingestApp(l1, segTenantB, "x", hourAgo, 7)...)
			acked = append(acked, e.ingestApp(l1, segTenantB, "y", hourAgo, 7)...)
			if got := e.ingestApp(l1, segTenantB, "z", hourAgo, 3); got != nil {
				t.Fatalf("the limiter admitted a third stream of a tenant with room for two")
			}

			c.run(e)

			// Process 2: its limiter is empty, and other streams arrive first
			// and take the room before the drain reaches the old rows.
			l2 := l1
			if c.restarts {
				l2 = e.useLimiter(policy)
			}
			acked = append(acked, e.ingestApp(l2, segTenantA, "b", hourAgo, 5)...)
			acked = append(acked, e.ingestApp(l2, segTenantB, "p", hourAgo, 4)...)
			acked = append(acked, e.ingestApp(l2, segTenantB, "q", hourAgo, 4)...)
			if got := e.ingestApp(l2, segTenantA, "a", hourAgo, 2); c.restarts && got != nil {
				t.Fatalf("the fresh limiter admitted a second stream of a tenant with room for one")
			}
			if e.segs.Active().Rows() > 0 {
				e.seal()
			}
			e.drainAll(e.flusher(1000))
			e.mustStoreAll(acked, c.name)
		})
	}
}

// The filters that stay at the drain are stateless: whatever the limiter has
// seen, and whatever its limits, the flush filter keeps every span stream
// (VictoriaTraces' trace_id_idx rows are the only drop, and they do not depend on the limiter).
func TestAckedRows_FlushFilterNeverDropsOnCardinality(t *testing.T) {
	rng := rand.New(rand.NewSource(42))
	defer vlstorage.SetCardinalityGate(nil)
	for iter := 0; iter < 200; iter++ {
		limit := rng.Intn(4)
		p, err := tenant.NewPolicyRegistry(map[string]config.TenantOverride{
			"1:2": {Cardinality: config.TenantCardinalityOverride{MaxStreams: limit}},
		}, nil)
		if err != nil {
			t.Fatal(err)
		}
		lim := tenant.NewCardinalityLimiter(p)
		vlstorage.SetCardinalityGate(lim)
		keep := vlstorage.FlushRowKeeper()
		for i := 0; i < 1+rng.Intn(12); i++ {
			s := fmt.Sprintf(`{resource_attr:service.name="s%d"}`, rng.Intn(9))
			admitted := lim.AllowStream(1, 2, s) // what the insert adapter asks
			if !keep(1, 2, s) {
				t.Fatalf("iteration %d: the flush filter dropped %s at limit %d (admitted at ingest: %v); only a stateless filter may drop at the drain", iter, s, limit, admitted)
			}
		}
	}
}

// Property: random tenants, streams and limits, with restarts and seals at
// random points. The rows added under each process's limiter are the
// acknowledged ones; all of them reach Parquet once.
func TestAckedRows_PropertyRandomRestartsAndLimits(t *testing.T) {
	for seed := int64(1); seed <= 8; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			defer vlstorage.SetCardinalityGate(nil)
			rng := rand.New(rand.NewSource(seed))
			e := newSegEnv(t)
			policy, err := tenant.NewPolicyRegistry(map[string]config.TenantOverride{
				"1:2": {Cardinality: config.TenantCardinalityOverride{MaxStreams: 1 + rng.Intn(3)}},
				"2:3": {Cardinality: config.TenantCardinalityOverride{MaxStreams: 1 + rng.Intn(3)}},
			}, nil)
			if err != nil {
				t.Fatal(err)
			}
			tenants := []logstorage.TenantID{segTenantA, segTenantB, {AccountID: 9, ProjectID: 9}} // the last has no limit
			lim := e.useLimiter(policy)
			f := e.flusher(25)
			var acked []string
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
					lim = e.useLimiter(policy)
					f = e.flusher(25)
				default:
					tn := tenants[rng.Intn(len(tenants))]
					app := fmt.Sprintf("app%d", rng.Intn(5))
					acked = append(acked, e.ingestApp(lim, tn, app, hourAgo.Add(time.Duration(rng.Intn(120))*time.Minute), 1+rng.Intn(8))...)
				}
			}
			if e.segs.Active().Rows() > 0 {
				e.seal()
			}
			e.drainAll(f)
			e.mustStoreAll(acked, "end")
		})
	}
}
