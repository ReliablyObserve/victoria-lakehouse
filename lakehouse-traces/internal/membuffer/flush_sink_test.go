package membuffer

import (
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// TestFlushSink_FiresLiveAndShadowKeepsData exercises the P0 flush-sink seam for
// the first time with real data: a registered FlushSink observes every row a
// flushing part carries, and — returning false (shadow mode) — the rows STAY in
// the store and remain queryable. This is the foundation of P2: the sink can
// validate/convert flushed rows without taking them away from the buffer.
func TestFlushSink_FiresLiveAndShadowKeepsData(t *testing.T) {
	// Long FlushInterval so only the explicit Close triggers the disk-flush
	// seam — deterministic.
	st, err := Open(Config{Path: t.TempDir(), FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var mu sync.Mutex
	var seen []string
	shadow := func(it logstorage.FlushRowsIterator) bool {
		it(func(_ string, _ int64, fields []logstorage.Field) bool {
			for _, f := range fields {
				if f.Name == "trace_id" {
					mu.Lock()
					seen = append(seen, f.Value)
					mu.Unlock()
				}
			}
			return true
		})
		return false // shadow: do NOT take ownership; store keeps the rows
	}
	logstorage.FlushSink = shadow
	defer func() { logstorage.FlushSink = nil }()

	now := time.Now().UnixNano()
	tid := logstorage.TenantID{}
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	want := []string{"x0", "x1", "x2", "x3"}
	for _, id := range want {
		addRow(lr, tid, now, "api-gateway", id)
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()

	// Before close: rows queryable (in-memory part not yet flushed to disk).
	if got := countQuery(t, st, []logstorage.TenantID{tid}, "*", now); got != int64(len(want)) {
		t.Fatalf("pre-close: want %d, got %d", len(want), got)
	}

	// Close triggers the final inmemory->disk flush, which fires the sink.
	st.Close()

	mu.Lock()
	defer mu.Unlock()
	sort.Strings(seen)
	if len(seen) != len(want) {
		t.Fatalf("sink observed %d rows via the live seam, want %d (%v)", len(seen), len(want), seen)
	}
	for i := range want {
		if seen[i] != want[i] {
			t.Fatalf("sink row %d = %q, want %q", i, seen[i], want[i])
		}
	}
}

// TestFlushSink_StreamTagsCapturedViaFlusher validates the production path: the
// periodic inmemory->disk flusher (live idb, not the Close edge) fires the sink,
// and the sink sees the block's canonical STREAM tags — i.e. service.name and
// other stream fields are recoverable, not just regular fields. This is what the
// real shadow converter needs to rebuild a full schema row.
func TestFlushSink_StreamTagsCapturedViaFlusher(t *testing.T) {
	st, err := Open(Config{Path: t.TempDir(), FlushInterval: 150 * time.Millisecond})
	if err != nil {
		t.Fatalf("open: %v", err)
	}
	defer st.Close()

	var mu sync.Mutex
	var canonicals []string
	logstorage.FlushSink = func(it logstorage.FlushRowsIterator) bool {
		it(func(canonical string, _ int64, _ []logstorage.Field) bool {
			mu.Lock()
			canonicals = append(canonicals, canonical)
			mu.Unlock()
			return true
		})
		return false // shadow
	}
	defer func() { logstorage.FlushSink = nil }()

	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 4; i++ {
		addRow(lr, logstorage.TenantID{}, now, "api-gateway", "z")
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()

	// Poll until the periodic flusher fires the sink (condition-based wait).
	deadline := time.Now().Add(5 * time.Second)
	for {
		mu.Lock()
		n := len(canonicals)
		mu.Unlock()
		if n > 0 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatal("periodic flusher did not fire the sink within 5s")
		}
		time.Sleep(50 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	for _, c := range canonicals {
		if c == "" {
			t.Fatal("flusher-path flush yielded empty stream tags (idb should be live here)")
		}
		// The canonical stream-tags blob must reference the stream field.
		if !containsSub(c, "service.name") && !containsSub(c, "api-gateway") {
			t.Fatalf("canonical stream tags missing service.name/value: %q", c)
		}
	}
}

func containsSub(s, sub string) bool {
	for i := 0; i+len(sub) <= len(s); i++ {
		if s[i:i+len(sub)] == sub {
			return true
		}
	}
	return false
}

// TestFlushSink_AuthoritativeDropsParts confirms the other contract: when the
// sink returns true (authoritative), the parts are dropped from the store. We
// re-open the same path afterwards and find no rows — they were taken by the
// sink, not persisted to a VL disk part.
func TestFlushSink_AuthoritativeDropsParts(t *testing.T) {
	dir := t.TempDir()
	st, err := Open(Config{Path: dir, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("open: %v", err)
	}

	var observed int
	logstorage.FlushSink = func(it logstorage.FlushRowsIterator) bool {
		it(func(_ string, _ int64, _ []logstorage.Field) bool { observed++; return true })
		return true // authoritative: take ownership; parts must be dropped
	}
	defer func() { logstorage.FlushSink = nil }()

	now := time.Now().UnixNano()
	lr := logstorage.GetLogRows([]string{"service.name"}, nil, nil, nil, "")
	for i := 0; i < 3; i++ {
		addRow(lr, logstorage.TenantID{}, now, "api-gateway", "y")
	}
	st.MustAddRows(lr)
	logstorage.PutLogRows(lr)
	st.DebugFlush()
	st.Close() // fires sink (authoritative) -> parts dropped, not written to disk

	if observed != 3 {
		t.Fatalf("sink observed %d rows, want 3", observed)
	}
	// Re-open the same dir: authoritative sink took the rows, so nothing was
	// persisted to a VL disk part.
	st2, err := Open(Config{Path: dir, FlushInterval: time.Hour})
	if err != nil {
		t.Fatalf("reopen: %v", err)
	}
	defer st2.Close()
	if got := countQuery(t, st2, []logstorage.TenantID{{}}, "*", now); got != 0 {
		t.Fatalf("after authoritative drop + reopen: want 0 persisted rows, got %d", got)
	}
}
