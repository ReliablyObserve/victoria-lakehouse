package delete

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
)

// Multi-instance tests (rule 9): two writers and two select pods over one shared
// bucket, each pod with its own view, request counters and fault injection.

type fleet struct {
	t       *testing.T
	b       *deletetest.Bucket
	writers []*TombstoneStore
	selects []*TombstoneStore
	pods    map[*TombstoneStore]*deletetest.Pod
}

func newFleet(t *testing.T, opts ...func(*Options)) *fleet {
	t.Helper()
	f := &fleet{t: t, b: deletetest.NewBucket(), pods: map[*TombstoneStore]*deletetest.Pod{}}
	for _, n := range []string{"writer-1", "writer-2"} {
		o := append([]func(*Options){func(o *Options) { o.Maintenance = true }}, opts...)
		s, p := newSyncedPod(t, f.b, n, o...)
		f.writers = append(f.writers, s)
		f.pods[s] = p
	}
	for _, n := range []string{"select-1", "select-2"} {
		s, p := newSyncedPod(t, f.b, n, opts...)
		f.selects = append(f.selects, s)
		f.pods[s] = p
	}
	return f
}

func (f *fleet) all() []*TombstoneStore {
	return append(append([]*TombstoneStore(nil), f.writers...), f.selects...)
}

// poll advances the store's clock by one poll interval and has every pod poll.
func (f *fleet) poll() {
	f.t.Helper()
	f.b.Advance(time.Minute)
	for _, s := range f.all() {
		mustSync(f.t, s, false)
	}
}

// A delete taken on a select pod is enforced on the other select pod and on
// both writers after one poll interval; the creating pod enforces it at once.
func TestMI_Delete_VisibleEverywhereWithinOnePoll(t *testing.T) {
	f := newFleet(t)
	create(t, f.selects[0], "d1")
	if !has(f.selects[0], "d1") {
		t.Fatal("not enforced on the creating pod at once")
	}
	for i, s := range []*TombstoneStore{f.selects[1], f.writers[0], f.writers[1]} {
		if has(s, "d1") {
			t.Fatalf("pod %d knows the delete before it polled", i)
		}
	}
	f.poll()
	for i, s := range f.all() {
		if !has(s, "d1") {
			t.Fatalf("pod %d does not enforce the delete one poll interval later", i)
		}
	}
	// And the same for an un-delete taken on a writer.
	if err := f.writers[1].Undelete(context.Background(), "d1"); err != nil {
		t.Fatal(err)
	}
	f.poll()
	for i, s := range f.all() {
		if has(s, "d1") {
			t.Fatalf("pod %d still enforces an un-deleted tombstone one poll later", i)
		}
	}
}

// Two pods creating the same delete task at the same instant (the fan-out of one
// task to every node) leave exactly one definition object with identical bytes,
// and every create succeeds.
func TestMI_ConcurrentCreateOfOneTask_ExactlyOneObject(t *testing.T) {
	for _, ignoreCondition := range []bool{false, true} {
		t.Run(fmt.Sprintf("store ignores If-None-Match=%v", ignoreCondition), func(t *testing.T) {
			f := newFleet(t)
			f.b.IgnoreIfNoneMatch(ignoreCondition)
			var wg sync.WaitGroup
			errs := make(chan error, 16)
			for i := 0; i < 16; i++ {
				wg.Add(1)
				go func(i int) {
					defer wg.Done()
					s := f.all()[i%4]
					errs <- RunTask(context.Background(), s, false, "fanout", 5_000_000, []TenantRef{{AccountID: 7}}, "level:error", "hide")
				}(i)
			}
			wg.Wait()
			close(errs)
			for err := range errs {
				if err != nil {
					t.Fatalf("a create of the same task failed: %v", err)
				}
			}
			keys := f.b.Keys(tombPrefix())
			if len(keys) != 1 {
				t.Fatalf("keys = %v, want exactly one definition", keys)
			}
			body, _ := f.b.Body(keys[0])
			want, _ := encodeDefinition(Tombstone{ID: "fanout", Query: "level:error", EndNs: 5_000_000, FilterAt: 5_000_000, Mode: "hide",
				StartNs: -1 << 63, Tenants: []TenantRef{{AccountID: 7}}})
			if !bytes.Equal(body, want) {
				t.Fatalf("definition bytes differ between pods:\n%s\n%s", body, want)
			}
		})
	}
}

// Nothing under the tombstone prefix is ever overwritten: every PUT any
// pod makes is a create, a key's content never changes, and an owed write cannot
// exist (a failed create leaves nothing queued).
func TestMI_NoKeyIsEverOverwritten(t *testing.T) {
	f := newFleet(t)
	snapshots := func() map[string]string {
		out := map[string]string{}
		for _, k := range f.b.Keys(tombPrefix()) {
			body, _ := f.b.Body(k)
			out[k] = string(body)
		}
		return out
	}
	var seen []map[string]string
	step := func(fn func()) {
		fn()
		seen = append(seen, snapshots())
	}
	step(func() { create(t, f.selects[0], "a") })
	step(func() {
		f.pods[f.writers[0]].FailNext("put", 1)
		_, _ = f.writers[0].Create(context.Background(), hideTombstone("lost"))
	})
	step(func() { create(t, f.writers[0], "b") })
	step(func() { f.poll() })
	step(func() { _ = f.writers[1].Undelete(context.Background(), "a") })
	step(func() { _ = f.selects[1].Undelete(context.Background(), "a") })
	step(func() { f.poll() })
	step(func() { create(t, f.selects[1], "c") })
	step(func() { _ = f.writers[0].Undelete(context.Background(), "c") })
	for _, s := range f.all() {
		_ = s.SyncNow(context.Background(), true)
	}
	for i := 1; i < len(seen); i++ {
		for k, v := range seen[i-1] {
			if cur, ok := seen[i][k]; !ok || cur != v {
				t.Fatalf("step %d changed or removed %s", i, k)
			}
		}
	}
	for _, op := range f.b.Log() {
		switch op.Kind {
		case "PUT":
			if !strings.Contains(op.Key, "_tombstones/") {
				t.Fatalf("a pod wrote outside the tombstone prefix: %s", op.Key)
			}
		case "DELETE":
			t.Fatalf("%s deleted %s: only the retention GC may delete", op.Pod, op.Key)
		}
	}
	// An id that failed to be created is free, and no pod holds it.
	for _, s := range f.all() {
		if has(s, "lost") {
			t.Fatal("a delete whose write failed is held by a pod")
		}
	}
}

// The objects of a retired (un-deleted) tombstone need no reaper: they are
// plain keys that expire with retention, and a pod restart loses no state it
// needs. No shared progress record exists for a non-owner to hold.
func TestMI_RestartLosesNothingAndNoProgressRecordExists(t *testing.T) {
	f := newFleet(t)
	create(t, f.selects[0], "a")
	create(t, f.selects[0], "b")
	if err := f.writers[0].Undelete(context.Background(), "b"); err != nil {
		t.Fatal(err)
	}
	f.poll()
	// Every key under the prefix is one of the two shapes of the protocol.
	for _, k := range f.b.Keys(tombPrefix()) {
		pk, ok := parseKey(tombPrefix(), k)
		if !ok {
			t.Fatalf("key %s is not a protocol key", k)
		}
		if pk.kind != kindDefinition && pk.kind != kindUndo {
			t.Fatalf("unexpected kind %s", pk.kind)
		}
	}
	// A restart (a pod with no cache and no memory) enforces exactly what is in
	// force.
	for _, name := range []string{"writer-1", "select-1"} {
		s, p := newSyncedPod(t, f.b, name+"-restarted")
		if !has(s, "a") || has(s, "b") {
			t.Fatalf("%s restarted into the wrong view: a=%v b=%v", name, has(s, "a"), has(s, "b"))
		}
		if c := p.Counts(); c.Puts != 0 || c.Deletes != 0 {
			t.Fatalf("%s wrote to the bucket at start: %+v", name, c)
		}
	}
}

// A boot of one writer neither resolves nor changes anything another
// writer has in flight: a booting pod performs no write at all, whatever the
// bucket holds (a fresh create, an un-delete, an undo key whose definition is
// not there).
func TestMI_BootWritesNothing(t *testing.T) {
	f := newFleet(t)
	create(t, f.writers[0], "in-flight")
	create(t, f.writers[0], "undone")
	if err := f.writers[0].Undelete(context.Background(), "undone"); err != nil {
		t.Fatal(err)
	}
	f.b.Put(formatKey(tombPrefix(), tombKey{kind: kindUndo, timeNs: f.b.Now().UnixNano(), id: "no-definition", defNs: 7}), nil)
	before := f.b.Keys(tombPrefix())
	logBefore := len(f.b.Log())

	_, p := newSyncedPod(t, f.b, "writer-3", func(o *Options) { o.Maintenance = true })
	if c := p.Counts(); c.Puts != 0 || c.Deletes != 0 {
		t.Fatalf("a booting writer wrote to the bucket: %+v", c)
	}
	for _, op := range f.b.Log()[logBefore:] {
		if op.Kind == "PUT" || op.Kind == "DELETE" {
			t.Fatalf("boot did %s %s", op.Kind, op.Key)
		}
	}
	after := f.b.Keys(tombPrefix())
	sort.Strings(before)
	sort.Strings(after)
	if strings.Join(before, "|") != strings.Join(after, "|") {
		t.Fatal("the key set changed during a boot")
	}
}

// A slow pod never serves a deleted row: its gate closes and, when it can sync
// again, it learns everything it missed.
func TestMI_SlowReaderFailsClosedThenCatchesUp(t *testing.T) {
	f := newFleet(t, func(o *Options) { o.Interval = 100 * time.Millisecond })
	slow := f.selects[0]
	pod := f.pods[slow]
	pod.FailNext("list", 1000)
	for i := 0; i < 3; i++ {
		create(t, f.writers[0], fmt.Sprintf("d%d", i))
		f.b.Advance(time.Minute)
	}
	time.Sleep(400 * time.Millisecond)
	if err := slow.Gate(context.Background(), nil); err == nil {
		t.Fatal("a pod that could not refresh serves reads")
	}
	pod.FailNext("list", 0)
	mustSync(t, slow, false)
	if err := slow.Gate(context.Background(), nil); err != nil {
		t.Fatalf("gate after catching up: %v", err)
	}
	for i := 0; i < 3; i++ {
		if !has(slow, fmt.Sprintf("d%d", i)) {
			t.Fatalf("the pod did not catch up on d%d", i)
		}
	}
}

// Every time is a pod's own clock: a delete of the Lakehouse API takes the key
// time of the creating pod (a skewed pod writes a skewed key, which the hourly
// full listing still catches), and an upstream task takes the time of its id, so
// the same task lands on the same key whichever pod, with whatever clock, takes it.
func TestMI_KeyTimeIsThePodsClockOrTheTaskID(t *testing.T) {
	f := newFleet(t)
	f.pods[f.selects[1]].SetSkew(5 * time.Hour)
	got := mustCreate(t, f.selects[1], hideTombstone("api"))
	if !got.CreatedAt.Equal(f.pods[f.selects[1]].Now().UTC()) {
		t.Fatalf("CreatedAt %v is not the creating pod's clock %v", got.CreatedAt, f.pods[f.selects[1]].Now())
	}
	const taskID = "1792310275123456789"
	for _, s := range []*TombstoneStore{f.selects[1], f.writers[0], f.writers[1]} {
		ts := hideTombstone(taskID)
		if _, err := s.Create(context.Background(), ts); err != nil {
			t.Fatalf("a pod %v from the id's key: %v", s, err)
		}
	}
	var n int
	for _, k := range f.b.Keys(tombPrefix()) {
		if pk, ok := parseKey(tombPrefix(), k); ok && pk.id == taskID {
			n++
			if pk.timeNs != 1792310275123456789 {
				t.Fatalf("an upstream task's key time is %d, want its id", pk.timeNs)
			}
		}
	}
	if n != 1 {
		t.Fatalf("%d keys for one task created on three pods with different clocks, want 1", n)
	}
}

// A late-flushing writer cannot resurrect rows: the tombstone lives until
// retention, so a row ingested after the delete whose _time is in its range is
// still hidden (the documented difference from upstream).
func TestMI_TombstoneOutlivesALateFlush(t *testing.T) {
	f := newFleet(t)
	ts := hideTombstone("range")
	ts.EndNs = 1_000_000
	mustCreate(t, f.selects[0], ts)
	f.b.Advance(2 * time.Hour)
	f.poll()
	for i, s := range f.all() {
		got, ok := s.Get("range")
		if !ok {
			t.Fatalf("pod %d lost the tombstone two hours later", i)
		}
		// A row written two hours after the delete, with a time inside its range.
		if !got.MatchesRow(map[string]string{"_msg": "error late"}, 500_000) {
			t.Fatalf("pod %d: a late row inside the range is not hidden", i)
		}
	}
}
