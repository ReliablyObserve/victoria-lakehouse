package manifest

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Claims and holds are the two things that keep two writers off the same object.
//
// A CLAIM reserves a key before anything is written to it: writers draw keys
// from a short random id, so without the claim an unlucky draw uploads over a
// live file. A HOLD marks a key the manifest already serves that another publish
// may not supersede yet — a rewrite's replacement whose swap is not durable, so
// the swap may still be undone and a compaction that merged the replacement
// would carry its rows into an output the undo knows nothing about.

func TestClaimPending_RefusesEveryKeyThatIsAlreadyInUse(t *testing.T) {
	registered, retired, claimed := refreshKey("registered"), refreshKey("retired"), refreshKey("claimed")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(registered, 3))
	m.Retire(retired, "", true)
	if !m.ClaimPending(claimed) {
		t.Fatal("the first claim of a free key must succeed")
	}
	if !m.IsPending(claimed) {
		t.Fatal("a claimed key must be pending")
	}

	for _, tc := range []struct{ name, key, reason string }{
		{"a file the manifest serves", registered, "registered"},
		{"a key awaiting deletion", retired, "retired"},
		{"a key another writer claimed", claimed, "pending"},
	} {
		before := metrics.ManifestKeyClaimRejected.Get(tc.reason)
		if m.ClaimPending(tc.key) {
			t.Errorf("ClaimPending succeeded for %s — two writers would own %s", tc.name, tc.key)
		}
		if metrics.ManifestKeyClaimRejected.Get(tc.reason) <= before {
			t.Errorf("a claim rejected because the key is %s must be counted under that reason", tc.reason)
		}
	}
	if m.ClaimPending("") {
		t.Error("the empty key is not claimable")
	}
}

func TestReleasePending_GivesTheKeyBackWithoutRetiringIt(t *testing.T) {
	k := refreshKey("claimed")
	m := New("b", "")
	m.ClaimPending(k)
	m.ReleasePending(k)

	if m.IsPending(k) {
		t.Fatal("a released claim is no longer pending")
	}
	if m.IsRetired(k) {
		t.Fatal("nothing was written, so there is nothing to delete: releasing must not retire the key")
	}
	if !m.ClaimPending(k) {
		t.Fatal("a released key must be claimable again")
	}
}

func TestPendingKeys_ListsUnpublishedUploadsOldestFirst(t *testing.T) {
	first, second, held := refreshKey("first"), refreshKey("second"), refreshKey("held")
	m := New("b", "")
	if len(m.PendingKeys()) != 0 {
		t.Fatal("a fresh manifest has no pending uploads")
	}
	m.ClaimPending(first)
	time.Sleep(2 * time.Millisecond)
	m.ClaimPending(second)
	time.Sleep(2 * time.Millisecond)
	m.ClaimPending(held)
	m.Hold(held)

	got := m.PendingKeys()
	if len(got) != 3 {
		t.Fatalf("PendingKeys = %+v, want 3 entries", got)
	}
	if got[0].Key != first || got[1].Key != second || got[2].Key != held {
		t.Fatalf("PendingKeys is not oldest-first: %+v", got)
	}
	if got[0].At.IsZero() {
		t.Error("a pending entry must carry when it was claimed")
	}
	if got[0].Held || !got[2].Held {
		t.Errorf("PendingKeys must report which claims are held: %+v", got)
	}

	// A publish clears the claim.
	m.AddFile(refreshPartition, enriched(first, 1))
	for _, pk := range m.PendingKeys() {
		if pk.Key == first {
			t.Fatalf("a published key is still listed as pending: %+v", pk)
		}
	}
}

func TestHold_KeepsAnotherPublishOffTheKeyUntilItIsReleased(t *testing.T) {
	src, repl, other := refreshKey("src"), refreshKey("repl"), refreshKey("other")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(src, 5))
	m.AddFile(refreshPartition, enriched(repl, 3))

	m.Hold("") // no-op: there is nothing to hold
	m.Hold(repl)
	if !m.IsHeld(repl) || m.IsHeld(src) {
		t.Fatalf("held=%v, want only %s held", m.HeldKeys(), repl)
	}
	if got := m.HeldKeys(); len(got) != 1 || got[0] != repl {
		t.Fatalf("HeldKeys = %v, want [%s]", got, repl)
	}

	// A compaction (or any other publish) must not supersede it.
	if m.ReplaceFile(refreshPartition, repl, enriched(other, 8)) {
		t.Fatal("ReplaceFile superseded a held key: the rows of a rewrite that may still be undone would move into another file")
	}
	if m.ReplaceFiles(refreshPartition, []string{src, repl}, enriched(other, 8)) {
		t.Fatal("ReplaceFiles merged a held key")
	}
	if m.RemoveFileIfPresent(refreshPartition, repl) {
		t.Fatal("RemoveFileIfPresent removed a held key")
	}
	if !m.HasKey(repl) {
		t.Fatal("the held entry must still be served")
	}

	// And once the swap is durable the hold is lifted and publishing works.
	m.Release(repl)
	if m.IsHeld(repl) || len(m.HeldKeys()) != 0 {
		t.Fatalf("Release must lift the hold: %v", m.HeldKeys())
	}
	if !m.ReplaceFile(refreshPartition, repl, enriched(other, 8)) {
		t.Fatal("a released key must be publishable again")
	}
}

func TestReplaceFiles_RefusesAnOutputKeyTheManifestAlreadyServes(t *testing.T) {
	a, b, live := refreshKey("a"), refreshKey("b"), refreshKey("live")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(a, 1))
	m.AddFile(refreshPartition, enriched(b, 2))
	m.AddFile(refreshPartition, enriched(live, 9))

	before := metrics.ManifestKeyClaimRejected.Get("publish_key_taken")
	if m.ReplaceFiles(refreshPartition, []string{a, b}, enriched(live, 3)) {
		t.Fatal("a merge published onto a key the manifest already serves: the live file's rows would be replaced by the merge's")
	}
	if metrics.ManifestKeyClaimRejected.Get("publish_key_taken") <= before {
		t.Error("a publish refused because its key is taken must be counted")
	}
	if fi, ok := m.GetFileByKey(live); !ok || fi.RowCount != 9 {
		t.Fatalf("the live entry changed: %+v ok=%v", fi, ok)
	}
	if !m.HasKey(a) || !m.HasKey(b) {
		t.Fatal("a refused merge must leave its sources registered")
	}

	// Re-publishing one of the merged keys under its own name is not a
	// collision: the publish is replacing it.
	if !m.ReplaceFiles(refreshPartition, []string{a, b}, enriched(a, 3)) {
		t.Fatal("a merge that reuses one of its own sources' keys must be allowed")
	}
}

func TestListed_OnlyAnAppliedListingSaysWhatTheBucketHolds(t *testing.T) {
	k := refreshKey("k")
	m := New("b", "")
	if m.Listed() {
		t.Fatal("a fresh manifest has not listed the bucket")
	}
	m.AddFile(refreshPartition, enriched(k, 1))
	if m.Listed() {
		t.Fatal("registering a file is not a listing")
	}
	if !m.ApplyListing([]ListedObject{{Key: k, Size: 1}}, time.Now()) {
		t.Fatal("the listing was rejected")
	}
	if !m.Listed() {
		t.Fatal("an applied listing must be remembered")
	}
}
