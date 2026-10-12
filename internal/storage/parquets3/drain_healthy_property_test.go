package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// A healthy segment is never stalled (the drain's plan check never fires): random tenants, times from the epoch on
// (including the epoch itself, 1 ns, second and hour edges), rows with equal
// timestamps and exact duplicates, several parts, random group sizes.
func TestDrain_HealthySegmentsAreNeverStalled(t *testing.T) {
	tenants := []logstorage.TenantID{segTenantA, segTenantB, {}, {AccountID: ^uint32(0), ProjectID: ^uint32(0)}}
	edges := []int64{0, 1, int64(time.Second) - 1, int64(time.Second), int64(time.Hour) - 1, int64(time.Hour),
		hourAgo.UnixNano() - 1, hourAgo.UnixNano(), time.Now().Add(-time.Minute).UnixNano()}
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			e := newSegEnv(t)
			want := map[string]int{}
			sent := 0
			for part := 0; part < 1+rng.Intn(5); part++ {
				lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
				for i := 0; i < 1+rng.Intn(60); i++ {
					tn := tenants[rng.Intn(len(tenants))]
					var ts int64
					switch rng.Intn(3) {
					case 0:
						ts = edges[rng.Intn(len(edges))]
					case 1:
						ts = rng.Int63n(time.Now().UnixNano())
					default:
						ts = hourAgo.UnixNano() + rng.Int63n(int64(3*time.Hour)) - int64(2*time.Hour)
					}
					msg := fmt.Sprintf("row-%d", e.seq.Add(1))
					copies := 1
					if rng.Intn(8) == 0 {
						copies = 2 // an exact duplicate row
					}
					for c := 0; c < copies; c++ {
						lr.MustAdd(tn, ts, []logstorage.Field{{Name: "service.name", Value: fmt.Sprintf("svc%d", rng.Intn(3))}, {Name: "_msg", Value: msg}}, 1)
						want[msg]++
						sent++
					}
				}
				e.segs.MustAddRows(lr)
				logstorage.PutLogRows(lr)
				e.segs.DebugFlush()
			}
			g := e.seal()
			f := e.flusher(int64(1 + rng.Intn(20)))
			before := invariantNow()
			if err := f.drain(context.Background(), g); err != nil {
				t.Fatalf("drain of a healthy segment (%d rows): %v", sent, err)
			}
			if d := invariantNow() - before; d != 0 {
				t.Fatalf("invariant counter +%d", d)
			}
			got := e.storedMsgs()
			for k, n := range want {
				if got[k] != n {
					t.Errorf("%s stored %d times, want %d", k, got[k], n)
				}
			}
			if len(got) != len(want) {
				t.Errorf("stored %d distinct rows, want %d", len(got), len(want))
			}
		})
	}
}
