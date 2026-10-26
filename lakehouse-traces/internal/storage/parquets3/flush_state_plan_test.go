package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"math/rand"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// The state of a drain records the maxRows it was planned with.
func TestFlushState_RecordsTheDrainsMaxRows(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 12)
	g := e.seal()
	f := e.flusher(5)
	e.failFn.Store(func(key string) bool { return !strings.Contains(key, "_segments/") })
	if err := f.drain(context.Background(), g); err == nil {
		t.Fatal("the drain succeeded with every upload failing")
	}
	st, err := readFlushState(f.statePath)
	if err != nil {
		t.Fatal(err)
	}
	if st.DrainingSeq != g.Seq() || st.DrainingMaxRows != 5 || st.DrainingPlan != planScheme {
		t.Fatalf("state draining_seq %d draining_max_rows %d draining_plan %d; want %d, 5 and %d", st.DrainingSeq, st.DrainingMaxRows, st.DrainingPlan, g.Seq(), planScheme)
	}
	// committed: the record is gone
	e.failFn.Store(func(string) bool { return false })
	e.drainAll(f)
	st, err = readFlushState(f.statePath)
	if err != nil {
		t.Fatal(err)
	}
	if st.DrainingSeq != 0 || st.DrainingMaxRows != 0 || st.DrainingPlan != 0 || st.CommittedThroughSeq != g.Seq() {
		t.Fatalf("after the commit: %+v", st)
	}
}

// A state that draws a draining segment without its maxRows, or a maxRows
// without a draining segment, is not a state this release wrote: it is
// rejected, not guessed (a guessed maxRows is the bug).
func TestReadFlushState_DrainingNeedsItsMaxRows(t *testing.T) {
	dir := t.TempDir()
	for name, c := range map[string]struct {
		body string
		ok   bool
	}{
		"draining with its maxRows":            {fmt.Sprintf(`{"version":%d,"committed_through_seq":1,"draining_seq":2,"draining_max_rows":5,"draining_plan":1}`, flushStateVersion), true},
		"nothing draining":                     {fmt.Sprintf(`{"version":%d,"committed_through_seq":1}`, flushStateVersion), true},
		"draining without a plan scheme":       {fmt.Sprintf(`{"version":%d,"draining_seq":2,"draining_max_rows":5}`, flushStateVersion), false},
		"a plan scheme without a draining seq": {fmt.Sprintf(`{"version":%d,"draining_plan":1}`, flushStateVersion), false},
		"draining without maxRows":             {fmt.Sprintf(`{"version":%d,"draining_seq":2}`, flushStateVersion), false},
		"draining with a zero maxRows":         {fmt.Sprintf(`{"version":%d,"draining_seq":2,"draining_max_rows":0,"draining_plan":1}`, flushStateVersion), false},
		"draining with a negative one":         {fmt.Sprintf(`{"version":%d,"draining_seq":2,"draining_max_rows":-3,"draining_plan":1}`, flushStateVersion), false},
		"maxRows without a draining seq":       {fmt.Sprintf(`{"version":%d,"draining_max_rows":5}`, flushStateVersion), false},
	} {
		p := filepath.Join(dir, strings.ReplaceAll(name, " ", "_"))
		if err := os.WriteFile(p, []byte(c.body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := readFlushState(p); (err == nil) != c.ok {
			t.Errorf("%s: err %v, want ok=%v", name, err, c.ok)
		}
	}
}

// A state of the release before this one, written in the middle of a drain, is
// not resumed with a guessed plan and its rows are not skipped: the process stops
// at start and the message says what to do.
func TestNewBufferFlusher_StateOfTheReleaseBeforeStopsStartup(t *testing.T) {
	e := newSegEnv(t)
	dir := filepath.Join(e.dir, "buffer")
	old := `{"version":5,"committed_through_seq":3,"draining_seq":4}`
	for _, p := range []string{filepath.Join(dir, "buffer_flush_state.json"), filepath.Join(dir, "buffer_flush_state.json.prev")} {
		if err := os.WriteFile(p, []byte(old), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	var msg string
	prev := fatalf
	fatalf = func(format string, args ...interface{}) { msg = fmt.Sprintf(format, args...) }
	defer func() { fatalf = prev }()
	NewBufferFlusher(e.bw, e.segs, dir, nil, BufferFlusherConfig{TargetBytes: 5 * 512, MaxAge: time.Hour})
	if msg == "" {
		t.Fatal("a state of another version was accepted")
	}
	for _, want := range []string{"insert.target_file_size", "same", "Do NOT delete the state", "pending"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the message %q does not mention %q", msg, want)
		}
	}
	if strings.Contains(msg, "delete both") {
		t.Errorf("the message advises deleting the state, which re-plans the segment (#748): %q", msg)
	}
}

// flushStateDecoderRun is one case of the decoder property (run by
// TestReadFlushState_RandomStates and, for the fuzz engine's seeds, by
// FuzzBufferRestart): a state made of random field values, sometimes with its bytes
// damaged, is decoded. The decoder never panics; an undamaged state is accepted
// exactly when it has the version and the shape rules (a draining segment has its
// maxRows and plan scheme, and nothing else has them); a state it accepts is one
// the flusher can use and writes back as it is.
func flushStateDecoderRun(t *testing.T, seed int64) {
	t.Helper()
	rng := rand.New(rand.NewSource(seed))
	pick := func(vs ...int64) int64 { return vs[rng.Intn(len(vs))] }
	st := map[string]any{"version": pick(flushStateVersion, flushStateVersion, flushStateVersion, flushStateVersion-1, flushStateVersion+1)}
	if rng.Intn(2) == 0 {
		st["committed_through_seq"] = uint64(rng.Intn(100))
	}
	draining := uint64(pick(0, 0, 1, 7, 1<<62))
	maxRows := pick(0, 0, 1, 5, 262144, -1, -1<<62)
	plan := pick(0, 0, 1, 1, 2, -1)
	if rng.Intn(2) == 0 {
		st["draining_seq"] = draining
	}
	if rng.Intn(2) == 0 {
		st["draining_max_rows"] = maxRows
	}
	if rng.Intn(2) == 0 {
		st["draining_plan"] = plan
	}
	if rng.Intn(5) == 0 {
		st["draining_seq"], st["draining_max_rows"], st["draining_plan"] = draining, int64(5), int64(1)
	}
	b, err := json.Marshal(st)
	if err != nil {
		t.Fatal(err)
	}
	damaged := false
	switch rng.Intn(6) {
	case 0:
		b, damaged = b[:rng.Intn(len(b))], true
	case 1:
		b[rng.Intn(len(b))] ^= byte(1 + rng.Intn(255))
		damaged = true
	}
	p := filepath.Join(t.TempDir(), "state")
	if err := os.WriteFile(p, b, 0o600); err != nil {
		t.Fatal(err)
	}
	got, err := readFlushState(p)
	if !damaged {
		var want flushState
		_ = json.Unmarshal(b, &want)
		wantOK := want.Version == flushStateVersion && want.DrainingMaxRows >= 0 && want.DrainingPlan >= 0 &&
			(want.DrainingSeq != 0) == (want.DrainingMaxRows != 0) && (want.DrainingSeq != 0) == (want.DrainingPlan != 0)
		if (err == nil) != wantOK {
			t.Fatalf("%s: err %v, want accepted=%v", b, err, wantOK)
		}
	}
	if err != nil {
		return
	}
	checkAcceptedFlushState(t, p, got)
}

// checkAcceptedFlushState: a state the decoder accepted is one the flusher can use
// and writes back as it is.
func checkAcceptedFlushState(t *testing.T, p string, got flushState) {
	t.Helper()
	if got.Version != flushStateVersion {
		t.Fatalf("accepted version %d", got.Version)
	}
	if (got.DrainingSeq != 0) != (got.DrainingMaxRows != 0) || (got.DrainingSeq != 0) != (got.DrainingPlan != 0) || got.DrainingMaxRows < 0 || got.DrainingPlan < 0 {
		t.Fatalf("accepted draining_seq %d with draining_max_rows %d, draining_plan %d", got.DrainingSeq, got.DrainingMaxRows, got.DrainingPlan)
	}
	out, err := json.Marshal(got)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(p, out, 0o600); err != nil {
		t.Fatal(err)
	}
	again, err := readFlushState(p)
	if err != nil || fmt.Sprint(again) != fmt.Sprint(got) {
		t.Fatalf("round trip: %v %+v != %+v", err, again, got)
	}
}

// FuzzReadFlushState takes any bytes: the decoder never panics, and a state it
// accepts is one the flusher can use. (flushStateDecoderRun, which FuzzBufferRestart
// calls, is the same property over states made of random field values.)
func FuzzReadFlushState(f *testing.F) {
	f.Add([]byte(fmt.Sprintf(`{"version":%d,"committed_through_seq":1,"draining_seq":2,"draining_max_rows":5,"draining_plan":1}`, flushStateVersion)))
	f.Add([]byte(fmt.Sprintf(`{"version":%d,"draining_seq":2}`, flushStateVersion)))
	f.Add([]byte(`{"version":5,"draining_seq":2}`))
	f.Add([]byte(`{`))
	f.Add([]byte(`{"version":5,"draining_max_rows":-1,"draining_seq":18446744073709551615}`))
	f.Fuzz(func(t *testing.T, b []byte) {
		p := filepath.Join(t.TempDir(), "state")
		if err := os.WriteFile(p, b, 0o600); err != nil {
			t.Fatal(err)
		}
		if st, err := readFlushState(p); err == nil {
			checkAcceptedFlushState(t, p, st)
		}
	})
}

func TestReadFlushState_RandomStates(t *testing.T) {
	for seed := int64(0); seed < 400; seed++ {
		flushStateDecoderRun(t, seed)
	}
}

// A drain started under a plan scheme this release does not know (the bucket
// scheme, the summaries or the key of a crowded second differ) is not resumed:
// the process stops at start, the segment's rows are not skipped, and the message
// says what to do.
func TestNewBufferFlusher_UnknownPlanSchemeStopsStartup(t *testing.T) {
	e := newSegEnv(t)
	dir := filepath.Join(e.dir, "buffer")
	write := func(scheme int) {
		state := fmt.Sprintf(`{"version":%d,"committed_through_seq":3,"draining_seq":4,"draining_max_rows":5,"draining_plan":%d}`, flushStateVersion, scheme)
		for _, p := range []string{filepath.Join(dir, "buffer_flush_state.json"), filepath.Join(dir, "buffer_flush_state.json.prev")} {
			if err := os.WriteFile(p, []byte(state), 0o600); err != nil {
				t.Fatal(err)
			}
		}
	}
	var msg string
	prev := fatalf
	fatalf = func(format string, args ...interface{}) { msg = fmt.Sprintf(format, args...) }
	defer func() { fatalf = prev }()
	write(planScheme + 1)
	NewBufferFlusher(e.bw, e.segs, dir, nil, BufferFlusherConfig{TargetBytes: 5 * 512, MaxAge: time.Hour})
	for _, want := range []string{fmt.Sprintf("plan scheme %d", planScheme+1), "insert.target_file_size", "Do NOT delete the state"} {
		if !strings.Contains(msg, want) {
			t.Errorf("the message %q does not mention %q", msg, want)
		}
	}
	// this release's own scheme starts
	msg = ""
	write(planScheme)
	NewBufferFlusher(e.bw, e.segs, dir, nil, BufferFlusherConfig{TargetBytes: 5 * 512, MaxAge: time.Hour})
	if msg != "" {
		t.Errorf("a state of this release's scheme stopped startup: %s", msg)
	}
}

// Outside a drain the plan functions use the configured rows per group, whatever
// the last drain was resumed with.
func TestBufferFlusher_GroupRowsOutsideADrainIsTheConfiguredValue(t *testing.T) {
	e := newSegEnv(t)
	e.ingest(segTenantA, hourAgo, 12)
	g := e.seal()
	f := e.flusher(5)
	if err := f.writeState(flushState{DrainingSeq: g.Seq(), DrainingMaxRows: 7, DrainingPlan: planScheme}); err != nil {
		t.Fatal(err)
	}
	f = e.flusher(5) // a restart: the state says 7
	if err := f.drain(context.Background(), g); err != nil {
		t.Fatal(err)
	}
	if f.groupRows() != 5 {
		t.Errorf("groupRows %d after a drain resumed with 7; want the configured 5", f.groupRows())
	}
	e.mustHaveExactly(12, "resumed with 7")
}
