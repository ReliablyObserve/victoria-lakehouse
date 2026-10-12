package parquets3

import (
	"context"
	"fmt"
	"math"
	"path/filepath"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// The cost of the drain's plan check: tenantRowCount (one `stats count()` per
// tenant over all of int64 time) next to tenantSecondCounts (the per-second plan
// the drain already built). BenchmarkPlanSegment is the whole planning pass,
// including the check.

// benchSegment returns a sealed segment holding rows rows for each of tenants
// tenants, spread over hours hours.
func benchSegment(b *testing.B, tenants, rows, hours int) *membuffer.Segment {
	b.Helper()
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: filepath.Join(b.TempDir(), "buffer")})
	if err != nil {
		b.Fatal(err)
	}
	b.Cleanup(segs.Close)
	base := time.Now().Add(-time.Duration(hours) * time.Hour).Truncate(time.Hour)
	for tn := 0; tn < tenants; tn++ {
		lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
		for i := 0; i < rows; i++ {
			ts := base.Add(time.Duration(i) * time.Duration(hours) * time.Hour / time.Duration(rows))
			lr.MustAdd(logstorage.TenantID{AccountID: uint32(tn + 1), ProjectID: 2}, ts.UnixNano(), []logstorage.Field{
				{Name: "service.name", Value: "api"}, {Name: "_msg", Value: fmt.Sprintf("row-%d", i)},
			}, 1)
		}
		segs.MustAddRows(lr)
		logstorage.PutLogRows(lr)
	}
	g, ok := segs.Seal()
	if !ok {
		b.Fatal("nothing to seal")
	}
	return g
}

func BenchmarkPlan(b *testing.B) {
	for _, c := range []struct{ tenants, rows, hours int }{{1, 10000, 1}, {1, 100000, 24}, {20, 5000, 24}} {
		g := benchSegment(b, c.tenants, c.rows, c.hours)
		tenant := logstorage.TenantID{AccountID: 1, ProjectID: 2}
		name := fmt.Sprintf("tenants=%d/rows=%d/hours=%d", c.tenants, c.rows, c.hours)
		b.Run("tenantSecondCounts/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := tenantSecondCounts(context.Background(), g, tenant); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("tenantRowCount/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := tenantRowCount(context.Background(), g, tenant); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("tenantIDs-from-epoch/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := g.GetTenantIDs(context.Background(), 0, math.MaxInt64); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("tenantIDs-all-time/"+name, func(b *testing.B) {
			for i := 0; i < b.N; i++ {
				if _, err := g.GetTenantIDs(context.Background(), math.MinInt64, math.MaxInt64); err != nil {
					b.Fatal(err)
				}
			}
		})
		b.Run("plan-as-before/"+name, func(b *testing.B) {
			// the loop the drain ran before the check: the tenants from the epoch,
			// each tenant's per-second counts cut into groups
			f := &BufferFlusher{maxRows: 100000}
			for i := 0; i < b.N; i++ {
				tenants, err := g.GetTenantIDs(context.Background(), 0, math.MaxInt64)
				if err != nil {
					b.Fatal(err)
				}
				for _, tn := range tenants {
					counts, err := tenantSecondCounts(context.Background(), g, tn)
					if err != nil {
						b.Fatal(err)
					}
					_ = planSlices(counts, f.maxRows)
				}
			}
		})
		b.Run("planSegment/"+name, func(b *testing.B) {
			f := &BufferFlusher{maxRows: 100000}
			for i := 0; i < b.N; i++ {
				if _, _, err := f.planSegment(context.Background(), g); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
