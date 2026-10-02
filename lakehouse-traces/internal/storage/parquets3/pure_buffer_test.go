package parquets3

import (
	"context"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/lakehouse-traces/internal/membuffer"
)

// pureBufferView is a view over a real single-store buffer holding 5
// checkout rows and 3 payments rows (the store seen as one segment).
func pureBufferView(t *testing.T) *bufferView {
	t.Helper()
	st, err := membuffer.Open(membuffer.Config{Path: t.TempDir()})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(st.Close)
	now := time.Now()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 8; i++ {
		svc := "checkout"
		if i >= 5 {
			svc = "payments"
		}
		lr.MustAdd(logstorage.TenantID{}, now.Add(-time.Duration(i)*time.Second).UnixNano(), []logstorage.Field{
			{Name: "service.name", Value: svc},
			{Name: "_msg", Value: fmt.Sprintf("m%d", i)},
		}, 1)
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()
	return &bufferView{local: st.Snapshot()}
}

// TestServePureBufferQuery_EngagesWithRawRows guards the pure-buffer fast path:
// when the query window is entirely unflushed (no parquet files for the
// request's tenants) on a single node, the buffer answers with RAW rows. The
// storage adapters run the query's pipes over whatever RunQuery emits
// (logstorage.RunQueryExternal*), so handing the pipes to the buffer too would
// aggregate twice or have the row filter drop the aggregates. The query's
// filter must still reach the buffer.
func TestServePureBufferQuery_EngagesWithRawRows(t *testing.T) {
	view := pureBufferView(t)
	s := &Storage{} // single node: bufferBridge nil

	q := mustParseQuery(t, `service.name:="checkout" | stats count()`)
	if got := q.String(); !containsStr(got, "stats") {
		t.Fatalf("precondition: query lost its stats pipe before the call: %q", got)
	}

	var mu sync.Mutex
	rows := 0
	var cols []string
	wb := func(_ uint, db *logstorage.DataBlock) {
		mu.Lock()
		defer mu.Unlock()
		rows += db.RowsCount()
		for _, c := range db.GetColumns(false) {
			cols = append(cols, c.Name)
		}
	}
	if !s.servePureBufferQuery(context.Background(), view, q, nil, false, wb) {
		t.Fatal("fast path did not engage for a single-node unflushed window")
	}
	if rows != 5 {
		t.Errorf("the buffer must hand back the 5 raw checkout rows (filter applied, pipes not), got %d rows", rows)
	}
	for _, c := range cols {
		if c == "count(*)" {
			t.Errorf("an aggregated column reached the caller: %v", cols)
		}
	}
	if !containsStr(q.String(), "stats") {
		t.Error("the caller's query must not be modified")
	}
}

// TestServePureBufferQuery_SkipsWhenUnsafe locks the condition under which the
// fast path MUST decline (returning false so the caller falls through to the
// bridge): when the view has no local segments — a select-only node, or a node
// with insert peers, whose own segments hold only its OWN unflushed rows, so
// serving locally would silently drop every other pod's recent data.
func TestServePureBufferQuery_SkipsWhenUnsafe(t *testing.T) {
	q := mustParseQuery(t, "*| stats count()")
	wb := func(uint, *logstorage.DataBlock) {}

	t.Run("no local buffer", func(t *testing.T) {
		s := &Storage{}
		v := s.openBufferView(context.Background(), 0, 1, nil)
		if s.servePureBufferQuery(context.Background(), v, q, nil, false, wb) {
			t.Error("fast path engaged with no local buffer; must defer to the bridge")
		}
	})

	t.Run("peers present", func(t *testing.T) {
		view := pureBufferView(t)
		s := &Storage{
			localBuffer:  localBufferOf(view),
			bufferBridge: &BufferBridge{endpoints: []string{"http://peer-1:8480"}},
		}
		if s.useLocalBuffer() {
			t.Fatal("a node with peers reads its own segments; it must fan out through the bridge")
		}
		if s.servePureBufferQuery(context.Background(), &bufferView{bridged: true}, q, nil, false, wb) {
			t.Error("fast path engaged for a bridged view; would drop peers' unflushed rows")
		}
	})
}

// localBufferOf wraps the view's snapshot as a LocalBuffer.
func localBufferOf(v *bufferView) LocalBuffer { return snapshotBuffer{v.local} }

type snapshotBuffer struct{ snap *membuffer.Snapshot }

func (b snapshotBuffer) Snapshot() *membuffer.Snapshot { return b.snap }
func (b snapshotBuffer) Close()                        {}

// TestServePureBufferQuery_DeclinesUnderATombstone: the buffer's aggregated
// result carries no row a tombstone filter could drop, so the fast path must
// not even run while one overlaps the window.
func TestServePureBufferQuery_DeclinesUnderATombstone(t *testing.T) {
	view := pureBufferView(t)
	s := &Storage{}
	q := mustParseQuery(t, "*| stats count()")
	var called atomic.Int32
	if s.servePureBufferQuery(context.Background(), view, q, nil, true, func(uint, *logstorage.DataBlock) { called.Add(1) }) {
		t.Fatal("the pure-buffer path engaged while a tombstone overlaps the window")
	}
	if called.Load() != 0 {
		t.Fatal("the buffer must not be queried for an aggregate a tombstone cannot be applied to")
	}
}

func containsStr(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}
