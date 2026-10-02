package compaction

import (
	"context"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

var errInjected = errors.New("injected fault")

func ofTenant(prefix string) func(string) error {
	return func(key string) error {
		if strings.HasPrefix(key, prefix) && !strings.Contains(key, "compacted-") {
			return errInjected
		}
		return nil
	}
}

func outputOf(prefix string) func(string) error {
	return func(key string) error {
		if strings.HasPrefix(key, prefix) && strings.Contains(key, "compacted-") {
			return errInjected
		}
		return nil
	}
}

// faultSetup: three tenants with ten L0 files each in one closed hour, and a
// scheduler (budget 3) so every tenant merges in the first scan.
func faultSetup(t *testing.T, mode config.Mode) (*planWorld, *ledger, *faultPool, *Scheduler, string, map[string][]manifest.FileInfo) {
	w := newPlanWorld(t, mode)
	l := newLedger(w)
	p := partitionAt(time.Now().Add(-3 * time.Hour))
	files := map[string][]manifest.FileInfo{}
	for _, tenant := range []string{"1001/0", "1002/0", "1003/0"} {
		files[tenant] = l.tenantL0(tenant, p, 10)
	}
	fp := &faultPool{mockPool: w.pool}
	l.check("seeded", false)
	return w, l, fp, w.schedulerOn(fp), p, files
}

func scan(t *testing.T, s *Scheduler) int {
	t.Helper()
	n, err := s.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

func converge(t *testing.T, l *ledger, s *Scheduler, stage string) {
	t.Helper()
	for i := 0; i < 10; i++ {
		if scan(t, s) == 0 {
			l.check(stage+": converged", false)
			if scan(t, s) != 0 {
				t.Fatalf("%s: a scan after convergence compacted", stage)
			}
			return
		}
		l.check(stage, true)
	}
	t.Fatalf("%s: no convergence in 10 scans", stage)
}

// TestStorageHealth_Fault_DownloadError: tenant 1001/0's merge fails on a source download.
// Guards isolation of a failing merge (the loop must `continue`, not return):
// the other tenants still merge in the same scan; rows are conserved; the
// failed tenant's files are untouched and merge on a later scan.
func TestStorageHealth_Fault_DownloadError(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		_, l, fp, s, p, files := faultSetup(t, mode)
		fp.set(func() { fp.downloadErr = ofTenant("1001/0/") })
		if n := scan(t, s); n != 2 {
			t.Fatalf("scan with 1001/0 failing: compactions=%d, want 2", n)
		}
		l.check("after failed download", true)
		if got := len(l.w.m.FilesForPartition(p)); got != 10+1+1 {
			t.Fatalf("files %d, want 10 untouched + 2 merged outputs", got)
		}
		_ = files
		fp.set(func() { fp.downloadErr = nil })
		converge(t, l, s, "download retry")
		if got := len(l.w.m.FilesForPartition(p)); got != 3 {
			t.Fatalf("after retry: %d files, want one per tenant", got)
		}
	})
}

// TestStorageHealth_Fault_UploadError: the output upload fails. Guards ReleasePending on the
// error path: the claimed key is released, nothing is registered, no orphan is
// left in the bucket, and the next scan merges.
func TestStorageHealth_Fault_UploadError(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		_, l, fp, s, _, _ := faultSetup(t, mode)
		fp.set(func() { fp.uploadErr = outputOf("1002/0/") })
		if n := scan(t, s); n != 2 {
			t.Fatalf("compactions=%d, want 2", n)
		}
		l.check("after failed upload", false) // strict: no awaiting-deletion exemption
		for _, k := range l.pool.Keys() {
			if strings.HasPrefix(k, "1002/0/") && strings.Contains(k, "compacted-") {
				t.Fatalf("failed upload left %s", k)
			}
		}
		fp.set(func() { fp.uploadErr = nil })
		converge(t, l, s, "upload retry")
	})
}

// TestStorageHealth_Fault_SourceDeleteError: the merge publishes but deleting the sources
// fails. Guards the retire-before-delete order: the sources stop being served
// at once (no row twice), stay in the bucket as owed deletes, and the next
// scan's ReclaimRetired removes them.
func TestStorageHealth_Fault_SourceDeleteError(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w, l, fp, s, p, files := faultSetup(t, mode)
		fp.set(func() { fp.deleteErr = ofTenant("1001/0/") })
		if n := scan(t, s); n != 3 {
			t.Fatalf("compactions=%d, want 3 (the publish succeeded)", n)
		}
		l.check("after failed source delete", true)
		if got := len(w.m.FilesForPartition(p)); got != 3 {
			t.Fatalf("served files %d, want one per tenant", got)
		}
		stuck := 0
		for _, f := range files["1001/0"] {
			if w.pool.get(f.Key) != nil {
				stuck++
			}
		}
		if stuck != 10 {
			t.Fatalf("%d source objects still in the bucket, want all 10 awaiting delete", stuck)
		}
		fp.set(func() { fp.deleteErr = nil })
		if n := scan(t, s); n != 0 {
			t.Fatalf("compactions=%d, want 0", n)
		}
		l.check("after reclaim", false)
		for _, rk := range w.m.RetiredKeys() {
			if rk.Reclaim {
				t.Fatalf("%s still owed a delete after reclaim", rk.Key)
			}
		}
	})
}

// swapOneSource installs a hook that, on the first download of a tenant's
// source, replaces another source of that tenant the way a delete rewrite
// does: the original leaves the manifest and an equivalent object with the same
// rows takes its place. The merge must be abandoned at publish. The original
// object is deleted only after the scan, as the rewriter would.
func swapOneSource(t *testing.T, l *ledger, fp *faultPool, p string, files []manifest.FileInfo, tenantPrefix string) (originalKey func() string) {
	var once sync.Once
	victim := files[len(files)-1]
	fp.set(func() {
		fp.onDownload = func(key string) {
			if !strings.HasPrefix(key, tenantPrefix) {
				return
			}
			once.Do(func() {
				l.w.m.RemoveFile(p, victim.Key)
				l.putIDs(strings.Replace(victim.Key, "batch-L0", "rewritten-L0", 1), "", p, 0, l.cache[victim.Key], victim.MinTimeNs, 0)
			})
		}
	})
	return func() string { return victim.Key }
}

// TestStorageHealth_Fault_SourceRemovedMidMerge: a source leaves the manifest while the merge
// runs. Guards the ReplaceFiles conflict path: the merge is abandoned (no row
// from the swapped source is duplicated), its output is deleted, other tenants
// are unaffected, and the next scan merges the new set.
func TestStorageHealth_Fault_SourceRemovedMidMerge(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w, l, fp, s, p, files := faultSetup(t, mode)
		victim := swapOneSource(t, l, fp, p, files["1001/0"], "1001/0/")
		if n := scan(t, s); n != 2 {
			t.Fatalf("compactions=%d, want 2 (1001/0 abandoned)", n)
		}
		w.pool.Delete(context.Background(), victim())
		l.check("after abandoned merge", true)
		for _, k := range l.pool.Keys() {
			if strings.HasPrefix(k, "1001/0/") && strings.Contains(k, "compacted-") {
				t.Fatalf("abandoned output %s left in the bucket", k)
			}
		}
		fp.set(func() { fp.onDownload = nil })
		converge(t, l, s, "after conflict")
		if got := len(w.m.FilesForPartition(p)); got != 3 {
			t.Fatalf("%d files, want one per tenant", got)
		}
	})
}

// TestStorageHealth_Fault_SourceRemovedMidMergeAndOutputDeleteFails: the abandoned output
// cannot be deleted either. Guards AbandonPending-before-delete: the output is
// retired so no refresh adopts it, and the next scan's reclaim removes it.
func TestStorageHealth_Fault_SourceRemovedMidMergeAndOutputDeleteFails(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		w, l, fp, s, p, files := faultSetup(t, mode)
		victim := swapOneSource(t, l, fp, p, files["1001/0"], "1001/0/")
		fp.set(func() { fp.deleteErr = outputOf("1001/0/") })
		if n := scan(t, s); n != 2 {
			t.Fatalf("compactions=%d, want 2", n)
		}
		w.pool.Delete(context.Background(), victim())
		l.check("abandoned output still in bucket", true)
		leftover := 0
		for _, k := range l.pool.Keys() {
			if strings.HasPrefix(k, "1001/0/") && strings.Contains(k, "compacted-") && !w.m.HasKey(k) {
				leftover++
			}
		}
		if leftover != 1 {
			t.Fatalf("expected the abandoned output to be awaiting reclaim, found %d", leftover)
		}
		fp.set(func() { fp.deleteErr = nil; fp.onDownload = nil })
		converge(t, l, s, "reclaim")
	})
}

// TestStorageHealth_Fault_EveryTenantFailsEveryOperation: a flaky store failing a random-ish
// third of all operations for several scans never loses or duplicates a row
// and converges once the store recovers.
func TestStorageHealth_Fault_FlakyStoreConverges(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		_, l, fp, s, _, _ := faultSetup(t, mode)
		var mu sync.Mutex
		n := 0
		flaky := func(string) error {
			mu.Lock()
			defer mu.Unlock()
			n++
			if n%3 == 0 {
				return errInjected
			}
			return nil
		}
		fp.set(func() { fp.downloadErr, fp.uploadErr, fp.deleteErr = flaky, flaky, flaky })
		for i := 0; i < 6; i++ {
			scan(t, s)
			l.check("flaky", true)
		}
		fp.set(func() { fp.downloadErr, fp.uploadErr, fp.deleteErr = nil, nil, nil })
		converge(t, l, s, "recovered")
	})
}
