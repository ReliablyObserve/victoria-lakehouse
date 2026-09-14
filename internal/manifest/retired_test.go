package manifest

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

func TestRetire_RemovesTheEntryAndRemembersIt(t *testing.T) {
	k := refreshKey("r")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(k, 3))

	if !m.Retire(k, refreshKey("by"), true) {
		t.Fatal("Retire must report that it removed the registered entry")
	}
	if m.HasKey(k) {
		t.Fatal("a retired key must leave the manifest")
	}
	rk, ok := m.LookupRetired(k)
	if !ok || rk.By != refreshKey("by") || !rk.Reclaim || rk.At.IsZero() || !m.IsRetired(k) {
		t.Fatalf("retirement record = %+v ok=%v", rk, ok)
	}

	// Retiring a key that is not registered still remembers it (a peer's push
	// for a file this node has not adopted yet).
	other := refreshKey("unregistered")
	if m.Retire(other, "", false) {
		t.Fatal("nothing was removed")
	}
	if !m.IsRetired(other) {
		t.Fatal("the unregistered key must still be kept out of the refresh")
	}
}

func TestRetire_ASecondRetirementNeverDropsAnOwedReclaim(t *testing.T) {
	k := refreshKey("r")
	m := New("b", "")
	m.Retire(k, refreshKey("replacement"), true)
	m.RemoveFile(refreshPartition, k) // someone else's removal of the same key
	rk, _ := m.LookupRetired(k)
	if !rk.Reclaim || rk.By != refreshKey("replacement") {
		t.Fatalf("the owed reclaim and the replacement must survive: %+v", rk)
	}
}

func TestAddFile_ClearsPendingAndRetired(t *testing.T) {
	a, b := refreshKey("a"), refreshKey("b")
	m := New("b", "")
	m.MarkPending(a)
	m.Retire(b, "", true)
	m.AddFile(refreshPartition, enriched(a, 1))
	m.AddFile(refreshPartition, enriched(b, 1))
	if m.IsPending(a) || m.IsRetired(b) {
		t.Fatal("a published key is neither pending nor retired")
	}
}

func TestAbandonPending_RetiresForReclaim(t *testing.T) {
	k := refreshKey("out")
	m := New("b", "")
	m.MarkPending(k)
	m.AbandonPending(k)
	if m.IsPending(k) {
		t.Fatal("an abandoned upload is no longer pending")
	}
	if rk, ok := m.LookupRetired(k); !ok || !rk.Reclaim {
		t.Fatalf("an abandoned upload is retired with its delete owed, got %+v ok=%v", rk, ok)
	}

	// An abandoned key a stale refresh had adopted is removed too.
	adopted := refreshKey("adopted")
	m.AddFile(refreshPartition, enriched(adopted, 1))
	m.AbandonPending(adopted)
	if m.HasKey(adopted) {
		t.Fatal("abandoning must remove a registered entry")
	}
}

func TestUnretire(t *testing.T) {
	k := refreshKey("r")
	m := New("b", "")
	if m.Unretire(k) {
		t.Fatal("nothing to unretire")
	}
	m.Retire(k, "x", true)
	if !m.Unretire(k) || m.IsRetired(k) {
		t.Fatal("Unretire must forget the retirement")
	}
	m.MarkPending(k)
	if !m.Unretire(k) || m.IsPending(k) {
		t.Fatal("Unretire must forget a pending mark too")
	}
}

func TestUnretireIfReplacedBy_OnlyUndoesThatPublish(t *testing.T) {
	k := refreshKey("src")
	m := New("b", "")
	if m.UnretireIfReplacedBy(k, "mine") {
		t.Fatal("nothing to undo")
	}
	m.Retire(k, "someone-else", true)
	if m.UnretireIfReplacedBy(k, "mine") || !m.IsRetired(k) {
		t.Fatal("a retirement made by another publish must not be undone")
	}
	m.Retire(k, "mine", true) // re-retired: By stays the first non-empty one
	m.ForgetRetired(k)
	m.Retire(k, "mine", true)
	if !m.UnretireIfReplacedBy(k, "mine") || m.IsRetired(k) {
		t.Fatal("this publish's retirement must be undone")
	}
}

func TestRetiredKeys_OldestFirst(t *testing.T) {
	m := New("b", "")
	for i := 0; i < 3; i++ {
		m.Retire(refreshKey("k"+strconv.Itoa(i)), "", false)
		time.Sleep(time.Millisecond)
	}
	got := m.RetiredKeys()
	if len(got) != 3 || got[0].Key != refreshKey("k0") || got[2].Key != refreshKey("k2") {
		t.Fatalf("RetiredKeys = %+v", got)
	}
	m.ForgetRetired(refreshKey("k1"))
	m.ForgetRetired(refreshKey("never-retired"))
	if len(m.RetiredKeys()) != 2 {
		t.Fatal("ForgetRetired must drop exactly that key")
	}
}

func TestReclaimRetired(t *testing.T) {
	m := New("b", "")
	owed, notOwed, failing, relisted := refreshKey("owed"), refreshKey("not-owed"), refreshKey("failing"), refreshKey("relisted")
	m.Retire(owed, "x", true)
	m.Retire(notOwed, "", false)
	m.Retire(failing, "x", true)
	m.Retire(relisted, "x", true)
	m.AddFile(refreshPartition, enriched(relisted, 1)) // published again: not retired any more

	var deletedKeys []string
	del := func(_ context.Context, key string) error {
		if key == failing {
			return errors.New("s3 down")
		}
		deletedKeys = append(deletedKeys, key)
		return nil
	}
	beforeErr := metrics.ManifestRetiredReclaimErrors.Get()
	deleted, failed := m.ReclaimRetired(context.Background(), del, 0)
	if deleted != 1 || failed != 1 || len(deletedKeys) != 1 || deletedKeys[0] != owed {
		t.Fatalf("deleted=%d failed=%d keys=%v, want only %s deleted and %s failed", deleted, failed, deletedKeys, owed, failing)
	}
	if m.IsRetired(owed) || !m.IsRetired(failing) || !m.IsRetired(notOwed) {
		t.Fatal("only a confirmed delete forgets its key; a failed one is retried, an un-owed one is never deleted")
	}
	if metrics.ManifestRetiredReclaimErrors.Get() <= beforeErr {
		t.Error("a failed reclaim must be counted")
	}

	// The limit bounds one pass.
	for i := 0; i < 5; i++ {
		m.Retire(refreshKey("bulk"+strconv.Itoa(i)), "x", true)
	}
	if d, f := m.ReclaimRetired(context.Background(), func(context.Context, string) error { return nil }, 2); d+f != 2 {
		t.Fatalf("a limited pass attempted %d, want 2", d+f)
	}

	// A cancelled context attempts nothing.
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if d, f := m.ReclaimRetired(ctx, del, 0); d+f != 0 {
		t.Fatalf("a cancelled pass attempted %d", d+f)
	}
}

func TestPruneRetired_AgeAndSizeBounds(t *testing.T) {
	m := New("b", "")
	now := time.Now()
	m.mu.Lock()
	m.retireLocked(RetiredKey{Key: "ancient", At: now.Add(-retiredKeyTTL - time.Hour)})
	for i := 0; i < maxRetiredKeys+5; i++ {
		m.retireLocked(RetiredKey{Key: "k" + strconv.Itoa(i), At: now.Add(time.Duration(i) * time.Microsecond)})
	}
	beforeTTL, beforeCap := metrics.ManifestRetiredEvicted.Get("ttl"), metrics.ManifestRetiredEvicted.Get("cap")
	m.pruneRetiredLocked(now.Add(time.Second))
	n := len(m.retired)
	_, ancient := m.retired["ancient"]
	_, oldest := m.retired["k0"]
	_, newest := m.retired["k"+strconv.Itoa(maxRetiredKeys+4)]
	m.mu.Unlock()

	if n != maxRetiredKeys || ancient || oldest || !newest {
		t.Fatalf("after pruning: %d keys (want %d), ancient=%v oldest=%v newest=%v", n, maxRetiredKeys, ancient, oldest, newest)
	}
	if metrics.ManifestRetiredEvicted.Get("ttl") <= beforeTTL || metrics.ManifestRetiredEvicted.Get("cap") <= beforeCap {
		t.Error("evictions must be counted by reason")
	}
}

func TestSnapshot_CarriesRetiredKeysButNotPending(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.bin")
	src, repl, pending := refreshKey("src"), refreshKey("repl"), refreshKey("pending")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(src, 5))
	m.ReplaceFile(refreshPartition, src, enriched(repl, 3))
	m.MarkPending(pending)
	if err := m.SaveTo(path); err != nil {
		t.Fatalf("SaveTo: %v", err)
	}

	restored := New("b", "")
	if err := restored.LoadFrom(path); err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	rk, ok := restored.LookupRetired(src)
	if !ok || rk.By != repl || !rk.Reclaim {
		t.Fatalf("the retirement must survive a restart, got %+v ok=%v", rk, ok)
	}
	if restored.IsPending(pending) {
		t.Fatal("pending uploads are the previous process's in-flight work and must not be restored")
	}
	if !restored.HasKey(repl) || restored.HasKey(src) {
		t.Fatal("snapshot files are restored as saved")
	}
}

func TestSnapshot_ARetiredKeyThatIsAlsoTrackedIsNotRestoredAsRetired(t *testing.T) {
	path := filepath.Join(t.TempDir(), "manifest.json")
	k := refreshKey("both")
	snap := persistedManifest{
		Files:       map[string][]FileInfo{refreshPartition: {enriched(k, 1)}},
		TotalFiles_: 1,
		Retired:     []RetiredKey{{Key: k, At: time.Now(), Reclaim: true}, {Key: refreshKey("gone"), At: time.Now()}},
	}
	data, err := json.Marshal(snap)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	m := New("b", "")
	if err := m.LoadFrom(path); err != nil {
		t.Fatalf("LoadFrom: %v", err)
	}
	if m.IsRetired(k) || !m.HasKey(k) {
		t.Fatal("a tracked key must never also be retired")
	}
	if !m.IsRetired(refreshKey("gone")) {
		t.Fatal("an untracked retired key is restored")
	}
}

func TestApplyListing_ForgetsARetiredKeyOnlyOnceAListingAfterItLacksIt(t *testing.T) {
	k, live := refreshKey("r"), refreshKey("live")
	m := New("b", "")
	m.AddFile(refreshPartition, enriched(live, 1))
	beforeRetire := time.Now()
	time.Sleep(time.Millisecond)
	m.Retire(k, "x", true)

	// A listing that started before the retirement proves nothing.
	m.ApplyListing([]ListedObject{{Key: live, Size: 1}}, beforeRetire)
	if !m.IsRetired(k) {
		t.Fatal("a listing begun before the retirement cannot confirm the object is gone")
	}
	// Still listed: stays retired and is not adopted.
	m.ApplyListing([]ListedObject{{Key: live, Size: 1}, {Key: k, Size: 1}}, time.Now())
	if !m.IsRetired(k) || m.HasKey(k) {
		t.Fatal("a retired key still in the listing stays retired and unregistered")
	}
	// A later listing without it: gone for good.
	m.ApplyListing([]ListedObject{{Key: live, Size: 1}}, time.Now())
	if m.IsRetired(k) {
		t.Fatal("a listing begun after the retirement that lacks the key confirms the delete")
	}
}

func TestApplyListing_ARejectedListingForgetsNothing(t *testing.T) {
	m := New("b", "")
	var keys []ListedObject
	for i := 0; i < 10; i++ {
		k := refreshKey("f" + strconv.Itoa(i))
		m.AddFile(refreshPartition, enriched(k, 1))
		keys = append(keys, ListedObject{Key: k, Size: 1})
	}
	retired := refreshKey("retired")
	m.Retire(retired, "x", true)
	time.Sleep(time.Millisecond)

	// A sparse listing (a LIST hiccup) is rejected by the cliff guard; it must
	// not be taken as proof that the retired object is gone.
	if m.ApplyListing(keys[:2], time.Now()) {
		t.Fatal("fixture: the cliff guard must reject a listing that lost most files")
	}
	if !m.IsRetired(retired) {
		t.Fatal("a rejected listing must not forget a retired key")
	}
}

func TestApplyListing_IgnoresNonParquetAndUnpartitionedKeys(t *testing.T) {
	m := New("b", "")
	m.ApplyListing([]ListedObject{
		{Key: "logs/_tombstones/x.json", Size: 1},
		{Key: "logs/no-partition.parquet", Size: 1},
		{Key: refreshKey("ok"), Size: 1},
	}, time.Now())
	if m.TotalFiles() != 1 || !m.HasKey(refreshKey("ok")) {
		t.Fatalf("files = %d, want only the partitioned parquet object", m.TotalFiles())
	}
}

func TestRecentAdds_StaySortedAndBounded(t *testing.T) {
	m := New("b", "")
	now := time.Now()
	m.mu.Lock()
	m.noteAddedLocked("a", now)
	m.noteAddedLocked("b", now.Add(-time.Hour)) // clock went backwards
	if !m.recentAdds[1].at.Equal(now) {
		t.Fatal("the add log must stay non-decreasing")
	}
	for i := 0; i < 2*maxRecentAdds+1; i++ {
		m.noteAddedLocked("k"+strconv.Itoa(i), now)
	}
	n := len(m.recentAdds)
	m.mu.Unlock()
	if n > 2*maxRecentAdds {
		t.Fatalf("add log holds %d entries, bound is %d", n, 2*maxRecentAdds)
	}
}

func TestRefresh_PendingKeyIsNotAdopted(t *testing.T) {
	k := refreshKey("uploading")
	m := New("b", "")
	m.MarkPending(k)
	m.ApplyListing([]ListedObject{{Key: k, Size: 1}}, time.Now())
	if m.HasKey(k) {
		t.Fatal("an upload that is not published yet must not be adopted")
	}
	if !m.IsPending(k) {
		t.Fatal("the refresh must leave the pending mark to the publish")
	}
}

func TestRemoveFileIfPresent_RetiresWithReclaim(t *testing.T) {
	k := refreshKey("all-rows-removed")
	m := New("b", "")
	if m.RemoveFileIfPresent(refreshPartition, k) || m.IsRetired(k) {
		t.Fatal("removing an absent key changes nothing")
	}
	m.AddFile(refreshPartition, enriched(k, 1))
	if !m.RemoveFileIfPresent(refreshPartition, k) {
		t.Fatal("fixture: removal refused")
	}
	if rk, ok := m.LookupRetired(k); !ok || !rk.Reclaim {
		t.Fatalf("the removed key must be retired with its delete owed, got %+v ok=%v", rk, ok)
	}
}
