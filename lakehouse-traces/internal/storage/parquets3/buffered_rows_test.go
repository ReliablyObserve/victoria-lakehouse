package parquets3

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// BufferedRows serves the admin parity check: every tenant's buffered rows in
// the window and the nonces of the live segments.
func TestBufferedRows_LocalBufferAllTenants(t *testing.T) {
	segs, err := membuffer.OpenSegments(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	defer segs.Close()

	now := time.Now().UnixNano()
	for tenant, n := range map[uint32]int{0: 6, 1: 3, 9: 2} {
		lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
		for i := 0; i < n; i++ {
			lr.MustAdd(logstorage.TenantID{AccountID: tenant}, now+int64(i), []logstorage.Field{
				{Name: "service.name", Value: "svc"},
				{Name: "_msg", Value: fmt.Sprintf("m%d", i)},
			}, 1)
		}
		segs.MustAddRows(lr)
		logstorage.PutLogRows(lr)
	}
	segs.DebugFlush()

	s := &Storage{localBuffer: segs}
	rows, nonces, err := s.BufferedRows(context.Background(), now-int64(time.Hour), now+int64(time.Hour))
	if err != nil {
		t.Fatal(err)
	}
	if rows != 11 {
		t.Fatalf("rows=%d, want 11 (three tenants)", rows)
	}
	if len(nonces) != 1 {
		t.Fatalf("nonces=%v, want the one live segment", nonces)
	}
}

func TestBufferedRows_NoBufferNoPeers(t *testing.T) {
	s := &Storage{}
	rows, nonces, err := s.BufferedRows(context.Background(), 0, time.Now().UnixNano())
	if err != nil || rows != 0 || len(nonces) != 0 {
		t.Fatalf("rows=%d nonces=%v err=%v, want 0, none, nil", rows, nonces, err)
	}
}
