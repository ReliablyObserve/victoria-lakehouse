package parquets3

import (
	"context"
	"fmt"
	"math/rand"
	"strconv"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
	otelpb "github.com/VictoriaMetrics/VictoriaTraces/lib/protoparser/opentelemetry/pb"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/vlstorage"
)

// A healthy segment is never stalled (traces; the drain's plan check never fires): spans through the insert adapter,
// with VictoriaTraces' trace_id_idx rows (some stamped before the epoch, so
// refused, some after, so dropped at the flush), the production keep filter,
// spans at the epoch and at second/hour edges, several parts, random group sizes.
func TestDrain_HealthySegmentsAreNeverStalled(t *testing.T) {
	tenants := []logstorage.TenantID{segTenantA, segTenantB, {}, {AccountID: ^uint32(0), ProjectID: ^uint32(0)}}
	edges := []int64{0, 1, int64(time.Second) - 1, int64(time.Second), int64(time.Hour) - 1, int64(time.Hour),
		hourAgo.UnixNano() - 1, hourAgo.UnixNano(), time.Now().Add(-time.Minute).UnixNano()}
	for seed := int64(1); seed <= 6; seed++ {
		t.Run(fmt.Sprintf("seed%d", seed), func(t *testing.T) {
			rng := rand.New(rand.NewSource(seed))
			e := newSegEnv(t)
			e.keep = vlstorage.FlushRowKeeper()
			want := map[string]int{}
			for part := 0; part < 1+rng.Intn(5); part++ {
				for _, tn := range tenants {
					var rows []adapterRow
					for i := 0; i < rng.Intn(20); i++ {
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
						rows = append(rows, adapterRow{ts: ts, fields: ackFields(fmt.Sprintf("svc%d", rng.Intn(3)), msg)})
						want[msg]++
						// the trace's index row, stamped with a start time that may be before the epoch
						start := ts - rng.Int63n(int64(10*time.Second))
						rows = append(rows, adapterRow{ts: start, streamLen: 1, fields: []logstorage.Field{
							{Name: otelpb.TraceIDIndexStreamName, Value: strconv.Itoa(rng.Intn(1024))}, {Name: "_msg", Value: "-"},
							{Name: otelpb.TraceIDIndexFieldName, Value: "trace-" + msg},
							{Name: otelpb.TraceIDIndexStartTimeFieldName, Value: strconv.FormatInt(start, 10)},
							{Name: otelpb.TraceIDIndexEndTimeFieldName, Value: strconv.FormatInt(ts, 10)},
						}})
					}
					if len(rows) > 0 {
						e.sendViaAdapter(tn, rows)
					}
				}
				e.segs.DebugFlush()
			}
			g := e.seal()
			f := e.flusher(int64(1 + rng.Intn(20)))
			before := invariantNow()
			if err := f.drain(context.Background(), g); err != nil {
				t.Fatalf("drain of a healthy segment: %v", err)
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
				t.Errorf("stored %d distinct spans, want %d (%v)", len(got), len(want), head(keysOf(got), 5))
			}
		})
	}
}

func keysOf(m map[string]int) []string {
	var out []string
	for k := range m {
		out = append(out, k)
	}
	return out
}
