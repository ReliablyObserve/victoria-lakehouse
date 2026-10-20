package delete

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"strings"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete/deletetest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// advancePolling moves the store's clock by d in poll-sized steps, every pod
// polling at each step, as running pods do.
func advancePolling(b *deletetest.Bucket, d time.Duration, pods ...*TombstoneStore) {
	for elapsed := time.Duration(0); elapsed < d; elapsed += time.Minute {
		b.Advance(time.Minute)
		for _, p := range pods {
			if err := p.SyncNow(context.Background(), false); err != nil {
				panic(err)
			}
		}
	}
}

func tombPrefix() string { return TombstonePrefix(testPrefix) }

// create makes a tombstone of tenant 1001 through the pod and returns its id's
// definition key.
func create(t *testing.T, s *TombstoneStore, id string) {
	t.Helper()
	ts := hideTombstone(id)
	mustCreate(t, s, ts)
}

func mustSync(t *testing.T, s *TombstoneStore, full bool) {
	t.Helper()
	if err := s.SyncNow(context.Background(), full); err != nil {
		t.Fatalf("sync(full=%v): %v", full, err)
	}
}

func has(s *TombstoneStore, id string) bool {
	_, ok := s.Get(id)
	return ok
}

// A delete created on one pod is hidden on every other pod at its next poll,
// and the poll reads only what is new.
func TestPoll_LearnsADeleteTakenElsewhere(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	reader, rpod := newSyncedPod(t, b, "reader")

	create(t, writer, "d1")
	if has(reader, "d1") {
		t.Fatal("the reader knows the delete before it looked")
	}
	rpod.ResetCounts()
	mustSync(t, reader, false)
	if !has(reader, "d1") {
		t.Fatal("the reader did not learn the delete at its poll")
	}
	c := rpod.Counts()
	if c.ListPages != 1 || c.Gets != 1 || c.Puts != 0 || c.Deletes != 0 {
		t.Fatalf("a poll that found one new delete cost %+v, want 1 LIST page + 1 GET and nothing else", c)
	}

	// A second poll finds nothing new: one LIST, no GET.
	rpod.ResetCounts()
	mustSync(t, reader, false)
	if c := rpod.Counts(); c.ListPages != 1 || c.Gets != 0 {
		t.Fatalf("an idle poll cost %+v, want exactly one LIST page", c)
	}
}

// The poll lists only keys after (store time - 10 min): old keys are not listed.
func TestPoll_ListsOnlyRecentKeys(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	reader, rpod := newSyncedPod(t, b, "reader")
	for i := 0; i < 50; i++ {
		create(t, writer, fmt.Sprintf("old-%d", i))
		b.Advance(time.Second)
	}
	mustSync(t, reader, true)
	// Half an hour of polls every minute: nothing new, nothing but a LIST each.
	for i := 0; i < 30; i++ {
		b.Advance(time.Minute)
		mustSync(t, reader, false)
	}
	create(t, writer, "fresh")
	var after string
	rpod.ResetCounts()
	mustSync(t, reader, false)
	for _, op := range b.Log() {
		if op.Pod == "reader" && op.Kind == "LIST" {
			after = op.After
		}
	}
	want := startAfterKey(tombPrefix(), b.Now().Add(-pollWindow).UnixNano())
	if after != want {
		t.Fatalf("the poll resumed from %q, want %q", after, want)
	}
	if !has(reader, "fresh") {
		t.Fatal("the fresh delete was not learned")
	}
	if c := rpod.Counts(); c.Gets != 1 {
		t.Fatalf("poll read %d objects, want only the new definition", c.Gets)
	}
}

// A listing that is short without an error (a lagging or buggy store) never
// makes a pod forget a delete it holds, and never causes a DELETE of the live
// object, also across a restart. A new delete missed by the short listing is
// learned at the next listing.
func TestShortListing_NeverDropsOrDeletes(t *testing.T) {
	dir := t.TempDir()
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	for i := 0; i < 5; i++ {
		create(t, writer, fmt.Sprintf("d%d", i))
	}
	opt := func(o *Options) { o.CacheDir = dir }
	reader, rpod := newSyncedPod(t, b, "reader", opt)
	for i := 0; i < 5; i++ {
		if !has(reader, fmt.Sprintf("d%d", i)) {
			t.Fatalf("fixture: d%d not learned", i)
		}
	}

	// One full listing returns only 2 of 5 keys, silently.
	rpod.ShortListOnce(2)
	mustSync(t, reader, true)
	for i := 0; i < 5; i++ {
		if !has(reader, fmt.Sprintf("d%d", i)) {
			t.Fatalf("d%d was dropped because a listing was short", i)
		}
	}
	for _, op := range b.Log() {
		if op.Kind == "DELETE" {
			t.Fatalf("%s deleted %s after a short listing", op.Pod, op.Key)
		}
	}
	if keys := b.Keys(tombPrefix()); len(keys) != 5 {
		t.Fatalf("bucket holds %d keys, want 5", len(keys))
	}

	// The same short listing at a restart: the pod restores what its cache knows
	// and the listing adds nothing it must forget.
	reader2, r2pod := newPod(t, b, "reader", opt)
	r2pod.ShortListOnce(2)
	mustSync(t, reader2, true)
	for i := 0; i < 5; i++ {
		if !has(reader2, fmt.Sprintf("d%d", i)) {
			t.Fatalf("after a restart with a short listing d%d is not enforced", i)
		}
	}
	if c := r2pod.Counts(); c.Deletes != 0 || c.Puts != 0 {
		t.Fatalf("a restarting pod wrote to the bucket: %+v", c)
	}

	// A delete created after, and missed by a short poll, is learned later.
	create(t, writer, "late")
	r2pod.ShortListOnce(1)
	mustSync(t, reader2, false)
	mustSync(t, reader2, true)
	if !has(reader2, "late") {
		t.Fatal("a delete missed by a short listing was not learned by the next full listing")
	}
}

// A pod that was down for a long time restarts from its disk copy and must
// not enforce a delete that was un-deleted meanwhile, nor write anything back.
func TestRestart_FromStaleCache_DoesNotResurrectAnUndeletedTombstone(t *testing.T) {
	dir := t.TempDir()
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	create(t, writer, "gone")
	create(t, writer, "kept")
	opt := func(o *Options) { o.CacheDir = dir }
	old, _ := newSyncedPod(t, b, "reader", opt)
	if !has(old, "gone") || !has(old, "kept") {
		t.Fatal("fixture: the reader must have both")
	}

	// The pod is down. Meanwhile the delete is un-deleted and a day passes.
	if err := writer.Undelete(context.Background(), "gone"); err != nil {
		t.Fatal(err)
	}
	b.Advance(24 * time.Hour)

	before := len(b.Log())
	restarted, rpod := newPod(t, b, "reader", opt)
	// Seeded from the cache, before any listing: the old state is in force, and
	// reads are closed until the first sync.
	if !has(restarted, "gone") {
		t.Fatal("fixture: the cache did not seed the view")
	}
	mustSync(t, restarted, true)
	if has(restarted, "gone") {
		t.Fatal("a tombstone that was un-deleted came back from the disk copy")
	}
	if !has(restarted, "kept") {
		t.Fatal("the delete that is still in force was lost")
	}
	for _, op := range b.Log()[before:] {
		if op.Kind == "PUT" || op.Kind == "DELETE" {
			t.Fatalf("restart wrote %s %s: the disk copy must never be written back", op.Kind, op.Key)
		}
	}
	if c := rpod.Counts(); c.Puts != 0 || c.Deletes != 0 {
		t.Fatalf("restart counts %+v", c)
	}
}

// The cache is a plain optimisation: with it, the first listing reads nothing
// it has; a corrupt or foreign cache file is ignored.
func TestCache_SavesTheReadsOfTheFirstListing(t *testing.T) {
	dir := t.TempDir()
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	for i := 0; i < 20; i++ {
		create(t, writer, fmt.Sprintf("d%d", i))
	}
	opt := func(o *Options) { o.CacheDir = dir }
	_, first := newSyncedPod(t, b, "r1", opt)
	if c := first.Counts(); c.Gets != 20 {
		t.Fatalf("first pod read %d objects, want 20", c.Gets)
	}
	_, second := newSyncedPod(t, b, "r2", opt)
	if c := second.Counts(); c.Gets != 0 {
		t.Fatalf("a pod with a warm cache read %d objects, want 0", c.Gets)
	}
	// A cache file for another key under the same name is not trusted.
	c := newBodyCache(dir)
	c.put("some/other/key", []byte("x"))
	if body, ok := c.get("some/other/key"); !ok || string(body) != "x" {
		t.Fatal("cache round trip failed")
	}
	if _, ok := newBodyCache("").get("k"); ok {
		t.Fatal("a nil cache returned a body")
	}
	// An unwritable cache never fails a sync.
	bad := newBodyCache("/proc/definitely/not/writable")
	bad.put("k", []byte("x"))
}

// A tombstone whose definition key was removed by hand is forgotten once a HEAD
// confirms it; any other answer keeps it.
func TestFullListing_ForgetsOnlyOnA404(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	create(t, writer, "removed")
	create(t, writer, "present")
	reader, rpod := newSyncedPod(t, b, "reader")

	var removedKey string
	for _, k := range b.Keys(tombPrefix()) {
		if strings.Contains(k, "-removed.json") {
			removedKey = k
		}
	}
	b.Remove(removedKey)

	// The HEAD fails: keep.
	rpod.FailNext("head", 1)
	mustSync(t, reader, true)
	if !has(reader, "removed") {
		t.Fatal("a tombstone was forgotten on a failed HEAD")
	}
	before := metrics.DeleteTombstonesForgotten.Get()
	mustSync(t, reader, true)
	if has(reader, "removed") {
		t.Fatal("a tombstone whose key no longer exists is still enforced")
	}
	if !has(reader, "present") {
		t.Fatal("a tombstone that exists was forgotten")
	}
	if metrics.DeleteTombstonesForgotten.Get() != before+1 {
		t.Fatal("the forgotten tombstone was not counted")
	}
}

// The stale-view gate: a read fails closed when the view is older than three
// intervals, and the first read waits for the first sync.
func TestGate_FailsClosedWhenTheViewIsStale(t *testing.T) {
	b := deletetest.NewBucket()
	s, pod := newPod(t, b, "p", func(o *Options) { o.Interval = 100 * time.Millisecond })

	// Before the first sync a read waits for it, then gives up.
	start := time.Now()
	if err := s.Gate(context.Background(), nil); !errors.Is(err, ErrViewNotSynced) {
		t.Fatalf("gate before the first sync = %v, want ErrViewNotSynced", err)
	}
	if time.Since(start) < 250*time.Millisecond {
		t.Fatal("the gate did not wait for the first sync")
	}

	mustSync(t, s, true)
	if err := s.Gate(context.Background(), nil); err != nil {
		t.Fatalf("gate on a fresh view: %v", err)
	}

	// The store stops answering: polls fail and the view ages.
	pod.FailNext("list", 100)
	time.Sleep(150 * time.Millisecond)
	if err := s.Gate(context.Background(), nil); err != nil {
		t.Fatalf("a view younger than 3 intervals must be trusted: %v", err)
	}
	if err := s.SyncNow(context.Background(), false); err == nil {
		t.Fatal("fixture: the sync should fail")
	}
	time.Sleep(250 * time.Millisecond)
	if err := s.Gate(context.Background(), nil); !errors.Is(err, ErrViewStale) {
		t.Fatalf("gate on a view older than 3 intervals = %v, want ErrViewStale", err)
	}

	// The store recovers.
	pod.FailNext("list", 0)
	mustSync(t, s, false)
	if err := s.Gate(context.Background(), nil); err != nil {
		t.Fatalf("gate after recovery: %v", err)
	}
}

// A first sync that only starts later (the store is down at boot) releases the
// reads that wait for it.
func TestGate_WaitingReadsAreReleasedByTheFirstSync(t *testing.T) {
	b := deletetest.NewBucket()
	s, _ := newPod(t, b, "p")
	done := make(chan error, 1)
	go func() { done <- s.Gate(context.Background(), nil) }()
	time.Sleep(50 * time.Millisecond)
	mustSync(t, s, true)
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("gate = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the gate was not released by the first sync")
	}
}

// A view without a bucket (a read-path unit test) is always trusted.
func TestGate_NoBackendIsAlwaysOpen(t *testing.T) {
	if err := NewTombstoneStore().Gate(context.Background(), nil); err != nil {
		t.Fatal(err)
	}
}

// A definition that cannot be decoded (corrupt, or a newer version) closes the
// gate only for the tenants its body names; the sync itself completes, other
// tenants read normally, and the closure ends when the key is readable or gone.
func TestGate_UndecodableDefinition_ClosesOnlyItsTenants(t *testing.T) {
	b := deletetest.NewBucket()
	reader, _ := newSyncedPod(t, b, "reader")
	key := tombPrefix() + "t" + hexTime(b.Now().UnixNano()) + "-future.json"
	b.Put(key, []byte(`{"v":99,"tenants":[{"a":1001,"p":0}],"shape":"from a newer release"}`))

	if err := reader.SyncNow(context.Background(), false); err != nil {
		t.Fatalf("an undecodable definition must not make the sync incomplete: %v", err)
	}
	if got := metrics.DeleteTombstoneUnreadable.Get(); got != 1 {
		t.Fatalf("unreadable gauge = %d, want 1", got)
	}
	ctx := context.Background()
	if err := reader.Gate(ctx, []TenantRef{{AccountID: 1001}}); !errors.Is(err, ErrTenantUnreadable) {
		t.Fatalf("gate for the named tenant = %v, want closed", err)
	}
	if err := reader.Gate(ctx, []TenantRef{{AccountID: 2002}}); err != nil {
		t.Fatalf("gate for another tenant = %v, want open", err)
	}
	if err := reader.Gate(ctx, []TenantRef{{AccountID: 2002}, {AccountID: 1001}}); !errors.Is(err, ErrTenantUnreadable) {
		t.Fatalf("a request spanning the named tenant = %v, want closed", err)
	}
	if err := reader.Gate(ctx, nil); !errors.Is(err, ErrTenantUnreadable) {
		t.Fatalf("a read of every tenant = %v, want closed", err)
	}
	// Still undecodable at the next full listing: it is read again, not forgotten.
	mustSync(t, reader, true)
	if err := reader.Gate(ctx, []TenantRef{{AccountID: 1001}}); !errors.Is(err, ErrTenantUnreadable) {
		t.Fatalf("gate after a full listing = %v, want still closed", err)
	}
	// An operator removes it.
	b.Remove(key)
	mustSync(t, reader, true)
	if got := metrics.DeleteTombstoneUnreadable.Get(); got != 0 {
		t.Fatalf("unreadable gauge = %d after the key was removed", got)
	}
	if err := reader.Gate(ctx, []TenantRef{{AccountID: 1001}}); err != nil {
		t.Fatalf("gate after recovery = %v", err)
	}
}

// A body that does not even say which tenants it names closes the gate for all.
func TestGate_UndecodableDefinitionNamingNoTenant_ClosesEveryTenant(t *testing.T) {
	b := deletetest.NewBucket()
	reader, _ := newSyncedPod(t, b, "reader")
	b.Put(tombPrefix()+"t"+hexTime(b.Now().UnixNano())+"-junk.json", []byte(`{not json`))
	if err := reader.SyncNow(context.Background(), false); err != nil {
		t.Fatal(err)
	}
	for _, tenants := range [][]TenantRef{{{AccountID: 1001}}, {{AccountID: 2002}}, nil} {
		if err := reader.Gate(context.Background(), tenants); !errors.Is(err, ErrTenantUnreadable) {
			t.Fatalf("gate for %v = %v, want closed for every tenant", tenants, err)
		}
	}
}

// A failing LIST cannot be scoped to tenants (the missing delete could be anyone's):
// once the view is stale the gate closes for every tenant, as an upstream cluster
// fails a query when any storage node is unreachable.
func TestGate_ListFailure_ClosesAllTenants(t *testing.T) {
	b := deletetest.NewBucket()
	reader, rpod := newSyncedPod(t, b, "reader", func(o *Options) { o.Interval = 50 * time.Millisecond })
	rpod.FailNext("list", 1000)
	time.Sleep(250 * time.Millisecond)
	_ = reader.SyncNow(context.Background(), false)
	for _, tenants := range [][]TenantRef{{{AccountID: 1}}, {{AccountID: 999}}, nil} {
		if err := reader.Gate(context.Background(), tenants); !errors.Is(err, ErrViewStale) {
			t.Fatalf("gate for %v with a failing LIST = %v, want ErrViewStale", tenants, err)
		}
	}
	rpod.FailNext("list", 0)
	mustSync(t, reader, false)
	if err := reader.Gate(context.Background(), []TenantRef{{AccountID: 1}}); err != nil {
		t.Fatalf("gate after the LIST works again = %v", err)
	}
}

// With delete.enabled false the view is still kept and enforced, but nothing is
// ever refused: a stale view or an undecodable definition must not turn reads
// into 503 on a deployment that has switched the delete API off.
func TestGate_DeleteDisabled_EnforcesButNeverRefuses(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	create(t, writer, "d1")
	reader, rpod := newSyncedPod(t, b, "reader", func(o *Options) { o.GateOff = true; o.Interval = 50 * time.Millisecond })
	if !has(reader, "d1") {
		t.Fatal("a pod with the gate off must still enforce the deletes it knows")
	}
	b.Put(tombPrefix()+"t"+hexTime(b.Now().UnixNano())+"-junk.json", []byte(`{not json`))
	rpod.FailNext("list", 1000)
	time.Sleep(250 * time.Millisecond)
	for _, tenants := range [][]TenantRef{{{AccountID: 1001}}, nil} {
		if err := reader.Gate(context.Background(), tenants); err != nil {
			t.Fatalf("gate off: %v", err)
		}
	}
	if !has(reader, "d1") {
		t.Fatal("the delete stopped being enforced")
	}
	// A fresh pod with the gate off does not wait for its first sync either.
	fresh, fpod := newPod(t, b, "fresh", func(o *Options) { o.GateOff = true })
	fpod.FailNext("list", 1000)
	start := time.Now()
	if err := fresh.Gate(context.Background(), nil); err != nil || time.Since(start) > time.Second {
		t.Fatalf("gate off waited for the first sync: %v after %v", err, time.Since(start))
	}
}

// A transient read error is retried the same way and heals.
func TestTransientReadError_HealsOnTheNextSync(t *testing.T) {
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	reader, rpod := newSyncedPod(t, b, "reader")
	create(t, writer, "d1")
	rpod.FailNext("get", 1)
	if err := reader.SyncNow(context.Background(), false); err == nil {
		t.Fatal("a failed read left the sync complete")
	}
	if has(reader, "d1") {
		t.Fatal("fixture: not learned yet")
	}
	mustSync(t, reader, false)
	if !has(reader, "d1") {
		t.Fatal("the delete was not learned after the retry")
	}
}

// Keys that are not tombstone objects (the previous protocol's <id>.json, any
// stray file) are ignored and counted, never read, and do not close the gate.
func TestForeignKeysAreIgnoredAndCounted(t *testing.T) {
	b := deletetest.NewBucket()
	b.Put(tombPrefix()+"8f14e45f-ceea-467a-9b3e-0c5e7d8f1a22.json",
		[]byte(`{"ID":"8f14e45f","Query":"*","EndNs":5,"AffectedKeys":["k"],"Tenants":[{"AccountID":1}]}`))
	b.Put(tombPrefix()+"README", []byte("x"))
	before := metrics.DeleteTombstoneKeysRejected.Get()
	s, pod := newSyncedPod(t, b, "p")
	if s.Count() != 0 {
		t.Fatal("a previous-protocol record was read")
	}
	if got := metrics.DeleteTombstoneKeysRejected.Get() - before; got != 2 {
		t.Fatalf("rejected keys counted = %d, want 2", got)
	}
	if c := pod.Counts(); c.Gets != 0 {
		t.Fatalf("a foreign key was read (%d GETs)", c.Gets)
	}
	if err := s.Gate(context.Background(), nil); err != nil {
		t.Fatalf("foreign keys closed the gate: %v", err)
	}
}

// A definition under the key of another id is unreadable, not trusted: it is not
// enforced, and it closes the gate for the tenant it names.
func TestDefinitionIDMustMatchItsKey(t *testing.T) {
	b := deletetest.NewBucket()
	body, _ := encodeDefinition(hideTombstone("real"))
	b.Put(formatKey(tombPrefix(), tombKey{kind: kindDefinition, timeNs: 5, id: "claimed"}), body)
	s, _ := newPod(t, b, "p")
	if err := s.SyncNow(context.Background(), true); err != nil {
		t.Fatalf("the sync must complete: %v", err)
	}
	if s.Count() != 0 {
		t.Fatal("a definition whose id differs from its key was enforced")
	}
	if err := s.Gate(context.Background(), []TenantRef{{AccountID: 1001}}); !errors.Is(err, ErrTenantUnreadable) {
		t.Fatalf("gate for the tenant the mismatched definition names = %v, want closed", err)
	}
	if err := s.Gate(context.Background(), []TenantRef{{AccountID: 2002}}); err != nil {
		t.Fatalf("gate for another tenant = %v", err)
	}
}

// Two definition keys for one id: the earlier key is the one in force, on every
// pod, whatever the order they are learned in.
func TestTwoDefinitionsForOneID_EarlierKeyWins(t *testing.T) {
	b := deletetest.NewBucket()
	early, late := hideTombstone("dup"), hideTombstone("dup")
	late.Tenants = []TenantRef{{AccountID: 9}}
	be, _ := encodeDefinition(early)
	bl, _ := encodeDefinition(late)
	b.Put(formatKey(tombPrefix(), tombKey{kind: kindDefinition, timeNs: 100, id: "dup"}), be)
	b.Put(formatKey(tombPrefix(), tombKey{kind: kindDefinition, timeNs: 200, id: "dup"}), bl)
	for _, name := range []string{"a", "b"} {
		s, _ := newSyncedPod(t, b, name)
		got, ok := s.Get("dup")
		if !ok || !got.ScopedExactlyTo(1001, 0) {
			t.Fatalf("pod %s enforces %+v, want the earlier key's definition", name, got)
		}
	}
}

// Create: the definition is written once with its bytes canonical; the view
// holds it at once; a repeat with the same bytes succeeds and a repeat with other
// content is refused.
func TestCreate_WritesOnceAndRepeatsAreIdempotent(t *testing.T) {
	b := deletetest.NewBucket()
	s, pod := newSyncedPod(t, b, "p")
	ts := hideTombstone("d1")
	got, err := s.Create(context.Background(), ts)
	if err != nil {
		t.Fatal(err)
	}
	if got.CreatedAt.IsZero() || !got.CreatedAt.Equal(b.Now().UTC()) {
		t.Fatalf("CreatedAt = %v, want the creating pod's time %v", got.CreatedAt, b.Now())
	}
	keys := b.Keys(tombPrefix())
	if len(keys) != 1 || !strings.HasSuffix(keys[0], "-d1.json") {
		t.Fatalf("keys = %v", keys)
	}
	body, _ := b.Body(keys[0])
	want, _ := encodeDefinition(ts)
	if !bytes.Equal(body, want) {
		t.Fatalf("stored bytes %s differ from the canonical encoding %s", body, want)
	}
	pod.ResetCounts()
	again := hideTombstone("d1")
	if _, err := s.Create(context.Background(), again); err != nil {
		t.Fatalf("identical repeat: %v", err)
	}
	if pod.Counts().Puts != 0 {
		t.Fatal("an identical repeat wrote again")
	}
	other := hideTombstone("d1")
	other.Query = "warn"
	if _, err := s.Create(context.Background(), other); !errors.Is(err, ErrTaskExists) {
		t.Fatalf("different content under one id = %v, want ErrTaskExists", err)
	}
	if n := len(b.Keys(tombPrefix())); n != 1 {
		t.Fatalf("%d keys after repeats, want 1", n)
	}
}

// A failed create leaves nothing behind: no key, nothing owed, nothing in force.
func TestCreate_FailureOwesNothing(t *testing.T) {
	b := deletetest.NewBucket()
	s, pod := newSyncedPod(t, b, "p")
	pod.FailNext("put", 1)
	if _, err := s.Create(context.Background(), hideTombstone("d1")); err == nil {
		t.Fatal("create succeeded although the write failed")
	}
	if s.Count() != 0 || len(b.Keys(tombPrefix())) != 0 {
		t.Fatal("a failed create left state behind")
	}
	// There is no retry queue: a later sync does not write anything.
	pod.ResetCounts()
	mustSync(t, s, true)
	if pod.Counts().Puts != 0 {
		t.Fatal("a sync wrote to the bucket")
	}
	// And the id is free to try again.
	create(t, s, "d1")
}

func TestCreate_Refusals(t *testing.T) {
	if _, err := NewTombstoneStore().Create(context.Background(), hideTombstone("x")); !errors.Is(err, ErrNoBackend) {
		t.Fatalf("no backend: %v", err)
	}
	s := newTestStore(t)
	for name, ts := range map[string]Tombstone{
		"no tenant":      {ID: "x", Query: "*", Mode: "hide"},
		"bad filter":     {ID: "x", Query: "level:(", Mode: "hide", Tenants: tenant1001()},
		"inverted range": {ID: "x", Query: "*", StartNs: 9, EndNs: 1, Mode: "hide", Tenants: tenant1001()},
		"no id":          {Query: "*", Mode: "hide", Tenants: tenant1001()},
		"unknown mode":   {ID: "x", Query: "*", Mode: "shred", Tenants: tenant1001()},
	} {
		if _, err := s.Create(context.Background(), ts); err == nil {
			t.Errorf("%s: accepted", name)
		}
	}
	if s.Count() != 0 {
		t.Fatal("a refused create is in force")
	}
}

// Un-delete of a hide-mode tombstone: the rows are visible again on the pod at
// once and on others at the next poll, via the undo event (one key, no fate);
// repeating is safe.
func TestUndelete_PropagatesWithinOnePoll(t *testing.T) {
	b := deletetest.NewBucket()
	a, _ := newSyncedPod(t, b, "a")
	c, cpod := newSyncedPod(t, b, "c")
	create(t, a, "d1")
	mustSync(t, c, false)
	if !has(c, "d1") {
		t.Fatal("fixture")
	}
	advancePolling(b, time.Hour, a, c) // the delete is old: only the undo event is new

	if err := a.Undelete(context.Background(), "d1"); err != nil {
		t.Fatal(err)
	}
	if has(a, "d1") {
		t.Fatal("not undone on the pod that undid it")
	}
	cpod.ResetCounts()
	mustSync(t, c, false)
	if has(c, "d1") {
		t.Fatal("the other pod still enforces an un-deleted tombstone after a poll")
	}
	if cn := cpod.Counts(); cn.ListPages != 1 || cn.Gets != 0 {
		t.Fatalf("learning an un-delete cost %+v, want 1 LIST page and no GET (the undo key carries the news)", cn)
	}
	var defs, undos int
	for _, k := range b.Keys(tombPrefix()) {
		switch pk, _ := parseKey(tombPrefix(), k); pk.kind {
		case kindDefinition:
			defs++
		case kindUndo:
			undos++
		}
	}
	if defs != 1 || undos != 1 || len(b.Keys(tombPrefix())) != 2 {
		t.Fatalf("keys: %v, want one definition and one undo", b.Keys(tombPrefix()))
	}
	// Idempotent, also from the other pod.
	if err := a.Undelete(context.Background(), "d1"); err != nil {
		t.Fatalf("second un-delete: %v", err)
	}
	if err := c.Undelete(context.Background(), "d1"); err != nil {
		t.Fatalf("un-delete from the pod that learned it: %v", err)
	}
	if err := a.Undelete(context.Background(), "never-existed"); !errors.Is(err, ErrTombstoneNotFound) {
		t.Fatalf("unknown id: %v", err)
	}
	// The listing of a fresh pod shows it as undone as well.
	fresh, _ := newSyncedPod(t, b, "fresh")
	if has(fresh, "d1") {
		t.Fatal("a fresh pod enforces an un-deleted tombstone")
	}
	// And a late repeat of the task meets the existing definition: refused (an
	// un-deleted id is not reusable until its retention).
	if _, err := fresh.Create(context.Background(), hideTombstone("d1")); !errors.Is(err, ErrTaskExists) {
		t.Fatalf("recreating an un-deleted id = %v, want ErrTaskExists", err)
	}
}

// Reversibility follows the mode, like upstream (where nothing is reversible):
// permanent and auto deletes (the default of run_task) cannot be undone, nothing
// is written, and the tombstone stays in force.
func TestUndelete_PermanentAndAutoAreIrreversible(t *testing.T) {
	for _, mode := range []string{"permanent", "auto"} {
		t.Run(mode, func(t *testing.T) {
			b := deletetest.NewBucket()
			a, apod := newSyncedPod(t, b, "a")
			ts := hideTombstone("d1")
			ts.Mode = mode
			mustCreate(t, a, ts)
			before := len(b.Keys(tombPrefix()))
			apod.ResetCounts()
			if err := a.Undelete(context.Background(), "d1"); !errors.Is(err, ErrNotReversible) {
				t.Fatalf("un-delete of a %s delete = %v, want ErrNotReversible", mode, err)
			}
			if !has(a, "d1") {
				t.Fatal("the tombstone stopped being enforced")
			}
			if len(b.Keys(tombPrefix())) != before || apod.Counts().Puts != 0 {
				t.Fatal("a refused un-delete wrote keys")
			}
		})
	}
}

// stop_task is a no-op that answers ok, as stopping a finished upstream task: it
// never brings rows back, for any mode, and writes nothing.
func TestStopTask_IsANoOp_RowsStayHidden(t *testing.T) {
	b := deletetest.NewBucket()
	a, apod := newSyncedPod(t, b, "a")
	for _, mode := range []string{"hide", "permanent", "auto"} {
		ts := hideTombstone("d-" + mode)
		ts.Mode = mode
		mustCreate(t, a, ts)
	}
	apod.ResetCounts()
	for _, id := range []string{"d-hide", "d-permanent", "d-auto", "never-existed"} {
		if err := StopTask(context.Background(), a, id); err != nil {
			t.Fatalf("stop_task %s = %v, want ok", id, err)
		}
	}
	for _, id := range []string{"d-hide", "d-permanent", "d-auto"} {
		if !has(a, id) {
			t.Fatalf("stop_task brought the rows of %s back", id)
		}
	}
	if apod.Counts().Puts != 0 {
		t.Fatal("stop_task wrote to the bucket")
	}
}

// An undo key names exactly its own tombstone, and only a hide tombstone can be
// undone: a key written by hand for a permanent or auto delete, or for another id,
// changes nothing; an undo that arrives before its definition waits for it.
func TestUndo_NamesOnlyItsOwnDefinition(t *testing.T) {
	b := deletetest.NewBucket()
	w, _ := newSyncedPod(t, b, "w")
	create(t, w, "hidden")
	create(t, w, "other")
	perm := hideTombstone("perm")
	perm.Mode = "permanent"
	mustCreate(t, w, perm)

	r, rpod := newSyncedPod(t, b, "r")
	b.Put(undoKeyOf(t, w, "hidden", b.Now().UnixNano()), nil)
	b.Put(undoKeyOf(t, w, "perm", b.Now().UnixNano()), nil)
	mustSync(t, r, true)
	if has(r, "hidden") {
		t.Fatal("an undo key did not undo its tombstone")
	}
	if !has(r, "other") {
		t.Fatal("an undo key for one id undid another")
	}
	if !has(r, "perm") {
		t.Fatal("an undo key undid a permanent delete")
	}
	if c := rpod.Counts(); c.Puts != 0 || c.Deletes != 0 {
		t.Fatalf("learning undo keys wrote to the bucket: %+v", c)
	}

	// An undo listed by a poll before its definition is known applies when the
	// definition is learned.
	late, _ := newPod(t, b, "late")
	defAt := b.Now().UnixNano()
	b.Put(formatKey(tombPrefix(), tombKey{kind: kindUndo, timeNs: b.Now().UnixNano(), id: "later", defNs: defAt}), nil)
	mustSync(t, late, true)
	if has(late, "later") {
		t.Fatal("fixture")
	}
	w2 := b.Pod("w2")
	defTS := hideTombstone("later")
	body, _ := encodeDefinition(defTS)
	if _, err := w2.PutIfAbsent(context.Background(), formatKey(tombPrefix(), tombKey{kind: kindDefinition, timeNs: defAt, id: "later"}), body); err != nil {
		t.Fatal(err)
	}
	mustSync(t, late, true)
	if has(late, "later") {
		t.Fatal("a tombstone learned after its undo key is in force")
	}
}

// A corrupt cache file must never keep the view incomplete: a body that fails to
// decode is dropped and the bucket's copy is read in the same sync.
func TestCache_CorruptFileFallsBackToTheBucket(t *testing.T) {
	dir := t.TempDir()
	b := deletetest.NewBucket()
	writer, _ := newSyncedPod(t, b, "writer")
	create(t, writer, "d1")
	create(t, writer, "d2")
	if err := writer.Undelete(context.Background(), "d2"); err != nil {
		t.Fatal(err)
	}
	// Garbage in the cache under the real keys.
	cache := newBodyCache(dir)
	for _, k := range b.Keys(tombPrefix()) {
		pk, _ := parseKey(tombPrefix(), k)
		if pk.kind == kindDefinition {
			cache.put(k, []byte("{garbage"))
		}
	}
	// At start the seed drops the corrupt files and the sync completes from the bucket.
	s, pod := newSyncedPod(t, b, "reader", func(o *Options) { o.CacheDir = dir })
	if !has(s, "d1") || has(s, "d2") {
		t.Fatalf("view after a corrupt cache: d1=%v d2=%v, want d1 enforced and d2 undone", has(s, "d1"), has(s, "d2"))
	}
	if err := s.Gate(context.Background(), nil); err != nil {
		t.Fatalf("gate: %v", err)
	}
	if pod.Counts().Gets == 0 {
		t.Fatal("the bucket's copy was never read")
	}

	// A cache file that goes bad between start and the next read (disk damage):
	// the same sync reads the bucket and the cache is repaired.
	late, _ := newSyncedPod(t, b, "late", func(o *Options) { o.CacheDir = dir })
	create(t, writer, "d3")
	for _, k := range b.Keys(tombPrefix()) {
		if pk, _ := parseKey(tombPrefix(), k); pk.id == "d3" {
			newBodyCache(dir).put(k, []byte("not a definition"))
		}
	}
	mustSync(t, late, false)
	if !has(late, "d3") {
		t.Fatal("a corrupt cached body kept the delete from being learned")
	}
}

// A silent short listing at full size must not turn the sync into a storm of
// HEAD requests: the check of unlisted keys is bounded per listing, the sync
// completes, and the gate stays open.
func TestFullListing_ShortListingDoesNotStormHEADs(t *testing.T) {
	b := deletetest.NewBucket()
	seedDefinitions(b, 10000)
	s, pod := newSyncedPod(t, b, "reader")
	if s.Count() != 10000 {
		t.Fatalf("fixture: %d tombstones", s.Count())
	}
	pod.ResetCounts()
	pod.ShortListOnce(1)
	before := metrics.DeleteTombstoneConfirmDeferred.Get()
	mustSync(t, s, true)
	if c := pod.Counts(); c.Heads > 100 {
		t.Fatalf("a short listing caused %d HEAD requests, want at most the budget of 100", c.Heads)
	}
	if metrics.DeleteTombstoneConfirmDeferred.Get() <= before {
		t.Fatal("the deferred checks were not counted")
	}
	if s.Count() != 10000 {
		t.Fatalf("a short listing dropped tombstones: %d left", s.Count())
	}
	if err := s.Gate(context.Background(), nil); err != nil {
		t.Fatalf("the gate closed: %v", err)
	}
}

// One unreadable key must not turn every poll into a full listing: the poll
// window follows the last successful listing, not the last complete sync.
func TestUnreadableKeyKeepsPollsAsPolls(t *testing.T) {
	b := deletetest.NewBucket()
	s, _ := newSyncedPod(t, b, "reader")
	bad := tombPrefix() + "t" + hexTime(b.Now().UnixNano()) + "-corrupt.json"
	b.Put(bad, []byte("{not json"))
	_ = s.SyncNow(context.Background(), false) // fails: the key is unreadable
	listsBefore := len(b.Log())
	for i := 0; i < 3; i++ {
		b.Advance(time.Minute)
		_ = s.SyncNow(context.Background(), false)
	}
	for _, op := range b.Log()[listsBefore:] {
		if op.Kind == "LIST" && op.After == "" {
			t.Fatal("a poll became a full listing because one key is unreadable")
		}
	}
}

// A pod that does not remove expired keys (no maintenance, or no DeleteObject
// permission) reads an expired definition once, not at every full listing.
func TestExpiredDefinitionsAreNotReadAgain(t *testing.T) {
	b := deletetest.NewBucket()
	w, _ := newSyncedPod(t, b, "w")
	ts := hideTombstone("old")
	ts.EndNs = b.Now().UnixNano()
	mustCreate(t, w, ts)
	b.Advance(testMaxTTL + 2*retentionMargin)

	r, rpod := newSyncedPod(t, b, "reader", withRetention)
	if has(r, "old") {
		t.Fatal("an expired tombstone is enforced")
	}
	rpod.ResetCounts()
	mustSync(t, r, true)
	mustSync(t, r, true)
	if c := rpod.Counts(); c.Gets != 0 {
		t.Fatalf("expired definitions were read again: %d GETs", c.Gets)
	}
}

// An id whose delete was un-deleted is spent until its keys expire: registering
// it again (the same task delivered late by a fan-out, or a reuse of the id) is
// refused as already registered, writes nothing and does not hide the rows again;
// a different task under the id is refused the same way.
func TestUndelete_AnUndoneIdIsNotReusable(t *testing.T) {
	b := deletetest.NewBucket()
	a, _ := newSyncedPod(t, b, "a")
	c, _ := newSyncedPod(t, b, "c")
	const id = "1792310275123456789" // an upstream task id: its own timestamp is the key time
	create(t, a, id)
	if err := a.Undelete(context.Background(), id); err != nil {
		t.Fatal(err)
	}
	mustSync(t, c, true)
	keys := len(b.Keys(tombPrefix()))

	for name, s := range map[string]*TombstoneStore{"a": a, "c": c} {
		if _, err := s.Create(context.Background(), hideTombstone(id)); !errors.Is(err, ErrTaskExists) {
			t.Fatalf("a late repeat of a spent id on %s: %v, want ErrTaskExists", name, err)
		}
		mustSync(t, s, true)
		if has(s, id) {
			t.Fatalf("pod %s hides the rows again after a repeat of an un-deleted id", name)
		}
	}
	other := hideTombstone(id)
	other.Query = "warn"
	if _, err := a.Create(context.Background(), other); !errors.Is(err, ErrTaskExists) {
		t.Fatalf("a different task under a spent id: %v, want ErrTaskExists", err)
	}
	if n := len(b.Keys(tombPrefix())); n != keys {
		t.Fatalf("%d keys after the refused repeats, want the original %d: a spent id writes nothing", n, keys)
	}
}

// A definition naming ProjectID 0 closes the gate for every project of that
// account (a tombstone of the account-only key layout acts on all its projects); a
// definition naming a project closes only that project.
func TestGate_UndecodableDefinition_ProjectZeroIsTheWholeAccount(t *testing.T) {
	b := deletetest.NewBucket()
	reader, _ := newSyncedPod(t, b, "reader")
	b.Put(tombPrefix()+"t"+hexTime(b.Now().UnixNano())+"-acct.json", []byte(`{"v":99,"tenants":[{"a":1001,"p":0},{"a":2002,"p":3}]}`))
	mustSync(t, reader, false)
	ctx := context.Background()
	for _, c := range []struct {
		t      TenantRef
		closed bool
	}{
		{TenantRef{AccountID: 1001, ProjectID: 0}, true},
		{TenantRef{AccountID: 1001, ProjectID: 9}, true}, // project 0 is the whole account
		{TenantRef{AccountID: 2002, ProjectID: 3}, true},
		{TenantRef{AccountID: 2002, ProjectID: 4}, false}, // a named project is only itself
		{TenantRef{AccountID: 2002, ProjectID: 0}, false},
		{TenantRef{AccountID: 3003}, false},
	} {
		err := reader.Gate(ctx, []TenantRef{c.t})
		if closed := errors.Is(err, ErrTenantUnreadable); closed != c.closed {
			t.Errorf("gate for %v: closed=%v (%v), want closed=%v", c.t, closed, err, c.closed)
		}
	}
}

// undoKeyOf is the key of an undo of the definition the view s holds for id.
func undoKeyOf(t *testing.T, s *TombstoneStore, id string, at int64) string {
	t.Helper()
	e, ok := s.state(id)
	if !ok {
		t.Fatalf("fixture: %s is not in the view", id)
	}
	dk, ok := parseKey(tombPrefix(), e.defKey)
	if !ok {
		t.Fatalf("fixture: %s has no definition key", id)
	}
	return formatKey(tombPrefix(), tombKey{kind: kindUndo, timeNs: at, id: id, defNs: dk.timeNs})
}

// An undo binds to the definition key it names, not to the id: two creates of one
// id on pods that had not polled each other leave two definitions (the earlier key
// wins), and an un-delete of the later one, by its own tenant, must not undo the
// earlier one everywhere.
func TestUndo_BindsToItsDefinitionKeyNotTheID(t *testing.T) {
	b := deletetest.NewBucket()
	a, _ := newSyncedPod(t, b, "a")
	other, _ := newSyncedPod(t, b, "b") // has not polled a
	first := hideTombstone("dup")
	mustCreate(t, a, first) // tenant 1001
	b.Advance(time.Second)
	second := hideTombstone("dup")
	second.Tenants = []TenantRef{{AccountID: 2002}}
	mustCreate(t, other, second) // a second definition key for the same id
	if err := other.Undelete(context.Background(), "dup"); err != nil {
		t.Fatal(err)
	}

	// A full sync everywhere: the earlier key (a's) is the tombstone, and the undo
	// that names the later key does not touch it.
	reader, _ := newSyncedPod(t, b, "reader")
	for name, s := range map[string]*TombstoneStore{"a": a, "reader": reader} {
		mustSync(t, s, true)
		ts, ok := s.Get("dup")
		if !ok {
			t.Fatalf("pod %s: tenant 1001's tombstone was undone by another definition's undo key", name)
		}
		if !ts.ScopedExactlyTo(1001, 0) {
			t.Fatalf("pod %s: the id resolved to %v, want the earlier definition (1001)", name, ts.Tenants)
		}
	}
	// The earlier definition's own undo still works.
	if err := a.Undelete(context.Background(), "dup"); err != nil {
		t.Fatal(err)
	}
	mustSync(t, reader, true)
	if has(reader, "dup") {
		t.Fatal("the undo of the earlier definition did not undo it")
	}
}
