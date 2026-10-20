package delete

import (
	"context"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/aws/smithy-go"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// Start polls until it is cancelled: it learns a delete taken elsewhere without
// any explicit sync, and returns when the context ends.
func TestStart_PollsUntilCancelled(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	reader, _ := newPod(t, b, "reader", func(o *Options) { o.Interval = 20 * time.Millisecond })

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { reader.Start(ctx); close(done) }()

	create(t, writer, "late")
	deadline := time.Now().Add(5 * time.Second)
	for !has(reader, "late") {
		if time.Now().After(deadline) {
			t.Fatal("the poll loop did not learn the delete")
		}
		b.Advance(time.Minute) // store time moves, so the poll window covers the new key
		time.Sleep(10 * time.Millisecond)
	}
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
}

// Start retries a failing first sync and stops when cancelled during the retry.
func TestStart_RetriesTheFirstSyncAndStopsOnCancel(t *testing.T) {
	b := deletetest.NewBucket()
	reader, pod := newPod(t, b, "reader", func(o *Options) { o.Interval = 10 * time.Millisecond })
	pod.FailNext("list", 1_000_000)
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { reader.Start(ctx); close(done) }()
	time.Sleep(50 * time.Millisecond)
	cancel()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Start did not return after cancel")
	}
	if _, ok := reader.ViewAge(); ok {
		t.Fatal("a view whose sync never succeeded reports an age")
	}
}

func TestStart_WithoutABackendReturns(t *testing.T) {
	NewTombstoneStore().Start(context.Background())
	if err := NewTombstoneStore().SyncNow(context.Background(), true); !errors.Is(err, ErrNoBackend) {
		t.Fatalf("SyncNow without a backend = %v", err)
	}
	if _, ok := NewTombstoneStore().ViewAge(); ok {
		t.Fatal("a store without a protocol reports a view age")
	}
}

func TestViewAge_AfterASync(t *testing.T) {
	s, _ := newSyncedPod(t, deletetest.NewBucket(), "pod")
	age, ok := s.ViewAge()
	if !ok || age < 0 || age > time.Minute {
		t.Fatalf("ViewAge = %v %v", age, ok)
	}
	if got := activeProtocol.viewAgeSeconds(); got < 0 {
		t.Fatalf("view age gauge = %v", got)
	}
}

func TestViewAgeGauge_NeverSyncedIsThePodsAge(t *testing.T) {
	s, _ := newPod(t, deletetest.NewBucket(), "pod")
	_ = s
	if got := activeProtocol.viewAgeSeconds(); got < 0 {
		t.Fatalf("gauge = %v", got)
	}
}

type fakeAPIError struct{ code string }

func (e fakeAPIError) Error() string                 { return e.code }
func (e fakeAPIError) ErrorCode() string             { return e.code }
func (e fakeAPIError) ErrorMessage() string          { return e.code }
func (e fakeAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultServer }

func TestIsConditionalUnsupported(t *testing.T) {
	cases := []struct {
		err  error
		want bool
	}{
		{fakeAPIError{"NotImplemented"}, true},
		{fakeAPIError{"XNotImplemented"}, true},
		{fakeAPIError{"AccessDenied"}, false},
		{fmt.Errorf("operation error S3: PutObject, NotImplemented: x"), true},
		{errors.New("connection reset"), false},
	}
	for _, c := range cases {
		if got := isConditionalUnsupported(c.err); got != c.want {
			t.Errorf("isConditionalUnsupported(%v) = %v, want %v", c.err, got, c.want)
		}
	}
}

func TestBodyCache_RoundTripEachAndFailures(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "cache")
	c := newBodyCache(dir)
	if newBodyCache("") != nil {
		t.Fatal("an empty directory must mean no cache")
	}
	var nilCache *bodyCache
	nilCache.put("k", []byte("v"))
	nilCache.remove("k")
	nilCache.each(func(string, []byte) { t.Fatal("a nil cache has no entries") })
	if _, ok := nilCache.get("k"); ok {
		t.Fatal("a nil cache returned a body")
	}

	c.put("p/key1", []byte("body1\nwith newline"))
	c.put("p/key2", []byte("body2"))
	if b, ok := c.get("p/key1"); !ok || string(b) != "body1\nwith newline" {
		t.Fatalf("get = %q %v", b, ok)
	}
	seen := map[string]string{}
	c.each(func(k string, b []byte) { seen[k] = string(b) })
	if len(seen) != 2 || seen["p/key2"] != "body2" {
		t.Fatalf("each saw %v", seen)
	}
	c.remove("p/key2")
	if _, ok := c.get("p/key2"); ok {
		t.Fatal("remove left the entry")
	}

	// A file holding another key, or no newline at all, is not an entry.
	if err := os.WriteFile(c.file("p/key3"), []byte("p/other\nbody"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.get("p/key3"); ok {
		t.Fatal("a file for another key was returned")
	}
	if err := os.WriteFile(c.file("p/key4"), []byte("garbage without newline"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, ok := c.get("p/key4"); ok {
		t.Fatal("a file without a key line was returned")
	}
	// Stray files and directories are skipped by each.
	if err := os.WriteFile(filepath.Join(dir, "notes.txt"), []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.Mkdir(filepath.Join(dir, "sub.tomb"), 0o750); err != nil {
		t.Fatal(err)
	}
	n := 0
	c.each(func(string, []byte) { n++ })
	// key1, and the file that holds "p/other" on its first line (each trusts the
	// key a file names; get checks it against the key asked for).
	if n != 2 {
		t.Fatalf("each visited %d entries, want 2", n)
	}
}

func TestBodyCache_UnwritableDirIsTolerated(t *testing.T) {
	blocker := filepath.Join(t.TempDir(), "file")
	if err := os.WriteFile(blocker, []byte("x"), 0o600); err != nil {
		t.Fatal(err)
	}
	c := newBodyCache(filepath.Join(blocker, "cache")) // a path under a regular file
	c.put("k", []byte("v"))
	c.put("k2", []byte("v"))
	if _, ok := c.get("k"); ok {
		t.Fatal("a put into an unwritable directory reported success")
	}
	c.each(func(string, []byte) { t.Fatal("an unreadable directory has no entries") })
}

func TestTombstoneFilter(t *testing.T) {
	if f := (&Tombstone{Query: "*"}).Filter(); f != nil {
		t.Fatal("match-all has no filter")
	}
	if f := (&Tombstone{}).Filter(); f != nil {
		t.Fatal("an empty query has no filter")
	}
	if f := (&Tombstone{Query: `level:="error"`}).Filter(); f == nil {
		t.Fatal("a parsable query has a filter")
	}
}

// LogRowFields and TraceRowFields render a row as the fields a tombstone's filter
// matches (the property tests of the read path build their expectations on them).
func TestRowFields_RenderTheFieldsATombstoneMatches(t *testing.T) {
	get := func(fs []logstorage.Field, name string) (string, bool) {
		for _, f := range fs {
			if f.Name == name {
				return f.Value, true
			}
		}
		return "", false
	}
	lr := &schema.LogRow{Body: "boom", ServiceName: "svc", TraceID: "t1", ResourceAttributes: map[string]string{"r": "1"}, LogAttributes: map[string]string{"l": "2"}}
	lf := LogRowFields(lr)
	for name, want := range map[string]string{"body": "boom", "service.name": "svc", "trace_id": "t1", "r": "1", "l": "2"} {
		if got, ok := get(lf, name); !ok || got != want {
			t.Errorf("log row field %s = %q (present %v), want %q", name, got, ok, want)
		}
	}
	kind, status := int32(2), int32(1)
	tr := &schema.TraceRow{SpanName: "op", ServiceName: "svc", TraceID: "t2", SpanKind: &kind, StatusCode: &status}
	tf := TraceRowFields(tr)
	for name, want := range map[string]string{"span.name": "op", "service.name": "svc", "trace_id": "t2", "span.kind": "2", "status.code": "1"} {
		if got, ok := get(tf, name); !ok || got != want {
			t.Errorf("trace row field %s = %q (present %v), want %q", name, got, ok, want)
		}
	}
	// A tombstone on those fields matches the rendered rows.
	ts := Tombstone{ID: "x", Query: `service.name:="svc"`, StartNs: 0, EndNs: 100, Tenants: []TenantRef{{}}, Mode: "hide"}
	if !ts.MatchesFields(lf, 50) || !ts.MatchesFields(tf, 50) {
		t.Error("a tombstone on service.name does not match the rendered rows")
	}
}

// Synced is false until the first complete sync, true after it, and true for a
// view with no bucket.
func TestSynced_FollowsTheFirstCompleteSync(t *testing.T) {
	if !NewTombstoneStore().Synced() {
		t.Error("a view without a bucket must report synced")
	}
	s, _ := newPod(t, deletetest.NewBucket(), "p")
	if s.Synced() {
		t.Error("synced before the first sync")
	}
	mustSync(t, s, true)
	if !s.Synced() {
		t.Error("not synced after the first sync")
	}
}
