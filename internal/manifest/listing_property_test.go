package manifest

import (
	"context"
	"fmt"
	"math/rand"
	"path/filepath"
	"sort"
	"testing"
	"time"
)

// Property (#418): over random sequences of complete listings, partial
// listings, sparse listings that miss live objects, legitimate peer shrinks,
// HEAD outages, retirements with an owed delete, reclaims and peer flushes,
//
//   - an owed retirement is never forgotten while its object exists, and its key
//     never reaches the manifest;
//   - a rejected listing changes nothing (entries, Listed, complete generation);
//   - the complete generation advances exactly on accepted COMPLETE listings,
//     and after one the manifest is exactly the bucket minus retired keys;
//   - a partial listing keeps every previous entry of the skipped account;
//   - the "absent => gone" predicate the destructive consumers use
//     (Listed && CompleteSince(object's mtime) && !HasKey) is never true for a
//     live object, so nothing that gates on it can delete one;
//   - a legitimate shrink is always accepted (the guard never sticks);
//   - each key is tracked at most once, including a file published while a
//     partial or complete listing runs (#404 round 4);
//   - a kill and restart from the persisted snapshot (whatever the last
//     refresh's outcome) keeps every owed retirement and is not trusted as a
//     listing (Listed false, no complete generation).

type propObj struct {
	account int
	mtime   time.Time
}

type propWorld struct {
	t       *testing.T
	rnd     *rand.Rand
	m       *Manifest
	bucket  map[string]propObj // objects that exist
	owed    map[string]bool    // retired by compaction, delete owed, object still exists
	headErr bool
	seq     int
}

func spin() time.Time {
	a := time.Now()
	for {
		b := time.Now()
		if b.After(a) {
			return b
		}
	}
}

func (w *propWorld) newKey(account int) string {
	w.seq++
	return fmt.Sprintf("%d/0/logs/%s/o%04d.parquet", account, refreshPartition, w.seq)
}

func (w *propWorld) listing(skipAccount int) []ListedObject {
	var out []ListedObject
	for k, o := range w.bucket {
		if skipAccount != 0 && o.account == skipAccount {
			continue
		}
		out = append(out, ListedObject{Key: k, Size: 1})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Key < out[j].Key })
	return out
}

func (w *propWorld) keys() map[string]bool {
	out := map[string]bool{}
	for _, files := range w.m.AllFiles() {
		for _, fi := range files {
			out[fi.Key] = true
		}
	}
	return out
}

type propState struct {
	keys   map[string]bool
	listed bool
	gen    uint64
}

func (w *propWorld) state() propState {
	return propState{w.keys(), w.m.Listed(), w.m.LastCompleteRefresh().Generation}
}

func sameKeys(a, b map[string]bool) bool {
	if len(a) != len(b) {
		return false
	}
	for k := range a {
		if !b[k] {
			return false
		}
	}
	return true
}

func (w *propWorld) check(op string, before propState, accepted, complete bool) {
	t := w.t
	t.Helper()
	after := w.state()
	cur := w.keys()
	// I6: each key is tracked at most once.
	seen := map[string]int{}
	for _, files := range w.m.AllFiles() {
		for _, fi := range files {
			seen[fi.Key]++
			if seen[fi.Key] > 1 {
				t.Fatalf("%s: key %s is tracked %d times", op, fi.Key, seen[fi.Key])
			}
		}
	}
	if n := w.m.TotalFiles(); n != len(seen) {
		t.Fatalf("%s: TotalFiles=%d, distinct keys=%d", op, n, len(seen))
	}
	// I1/I5: owed retirements stay owed and out of the manifest.
	for k := range w.owed {
		if cur[k] {
			t.Fatalf("%s: owed retired key %s was adopted into the manifest", op, k)
		}
		if !w.m.IsRetired(k) {
			t.Fatalf("%s: the owed retirement of %s (object still in the bucket) was forgotten", op, k)
		}
	}
	// I2: a rejected listing changes nothing.
	if !accepted && (!sameKeys(before.keys, after.keys) || before.listed != after.listed || before.gen != after.gen) {
		t.Fatalf("%s: a rejected listing changed the manifest", op)
	}
	// I3: generation moves exactly on accepted complete listings.
	wantGen := before.gen
	if accepted && complete {
		wantGen++
	}
	if after.gen != wantGen {
		t.Fatalf("%s: generation %d -> %d, want %d (accepted=%v complete=%v)", op, before.gen, after.gen, wantGen, accepted, complete)
	}
	if after.listed != (after.gen > 0) {
		t.Fatalf("%s: Listed()=%v with generation %d", op, after.listed, after.gen)
	}
	// I4: a complete listing leaves exactly the bucket minus retired keys.
	if accepted && complete {
		for k := range w.bucket {
			if w.owed[k] {
				continue
			}
			if !cur[k] {
				t.Fatalf("%s: complete listing left live %s out of the manifest", op, k)
			}
		}
		for k := range cur {
			if _, ok := w.bucket[k]; !ok {
				t.Fatalf("%s: manifest holds %s, which the bucket does not", op, k)
			}
		}
	}
	// The consumers' predicate must never mark a live object absent-and-gone.
	for k, o := range w.bucket {
		if w.owed[k] {
			continue
		}
		if w.m.Listed() && w.m.CompleteSince(o.mtime) && !cur[k] {
			t.Fatalf("%s: live object %s would be treated as gone (Listed, complete listing after its mtime, absent from the manifest)", op, k)
		}
	}
}

func TestProperty_ListingsNeverLoseLiveObjectsOrForgetOwedRetirements(t *testing.T) {
	seeds := 150
	if testing.Short() {
		seeds = 30
	}
	for seed := 0; seed < seeds; seed++ {
		runListingProperty(t, int64(seed))
	}
}

func runListingProperty(t *testing.T, seed int64) {
	w := &propWorld{t: t, rnd: rand.New(rand.NewSource(seed)), bucket: map[string]propObj{}, owed: map[string]bool{}}
	// The previous process: some objects, persisted as a snapshot.
	src := New("b", "")
	for i := 0; i < 12+w.rnd.Intn(20); i++ {
		acct := 1 + w.rnd.Intn(2)
		k := w.newKey(acct)
		w.bucket[k] = propObj{acct, spin()}
		src.AddFile(refreshPartition, enriched(k, 1))
	}
	w.m = snapshotLoaded(t, src)
	w.m.SetObjectProber(func(_ context.Context, _, key string) (bool, error) {
		if w.headErr {
			return false, fmt.Errorf("503")
		}
		_, ok := w.bucket[key]
		return ok, nil
	})
	before := w.state()
	w.check("snapshot load", before, true, false)

	for step := 0; step < 40; step++ {
		before = w.state()
		op := w.rnd.Intn(12)
		label := fmt.Sprintf("seed=%d step=%d op=%d", seed, step, op)
		switch op {
		case 0, 1: // complete listing
			ok := w.m.ApplyListing(w.listing(0), spin())
			if !ok {
				// Only a >50% shrink can be refused; with HEAD working it must pass.
				if !w.headErr {
					t.Fatalf("%s: a complete listing of the bucket was rejected", label)
				}
			}
			w.check(label+" complete", before, ok, true)
		case 2: // partial listing: account 2 skipped
			ok := w.m.ApplyPartialListing(w.listing(2), spin(), []string{"2/"})
			if !ok && !w.headErr {
				t.Fatalf("%s: a partial listing that carries account 2 was rejected", label)
			}
			for k := range before.keys {
				if !ok {
					break
				}
				if _, live := w.bucket[k]; live && len(k) > 2 && k[:2] == "2/" && !w.keys()[k] {
					t.Fatalf("%s: the partial listing dropped %s of the skipped account", label, k)
				}
			}
			w.check(label+" partial", before, ok, false)
		case 3: // sparse listing missing live keys (kept share < 50% of tracked)
			tracked := len(before.keys)
			if tracked < 4 {
				continue
			}
			// Every dropped tracked key must be live for the HEAD sample to be
			// certain to find one (a sample of 16 can miss a rare live key among
			// many dead ones: the documented residual of sampling).
			stale := false
			for k := range before.keys {
				if _, live := w.bucket[k]; !live {
					stale = true
				}
			}
			if stale {
				continue
			}
			all := w.listing(0)
			w.rnd.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
			keep := all
			if len(keep) >= tracked/2 {
				keep = keep[:tracked/2-1]
			}
			ok := w.m.ApplyListing(keep, spin())
			// Every dropped tracked key is live (or owed), so the HEAD sample finds it.
			if ok {
				t.Fatalf("%s: a sparse listing (%d of %d tracked) with live dropped keys was applied", label, len(keep), tracked)
			}
			w.check(label+" sparse", before, ok, false)
		case 4: // legitimate peer shrink: >50% of the live objects are gone
			var victims []string
			for k := range w.bucket {
				if !w.owed[k] {
					victims = append(victims, k)
				}
			}
			sort.Strings(victims)
			w.rnd.Shuffle(len(victims), func(i, j int) { victims[i], victims[j] = victims[j], victims[i] })
			for _, k := range victims[:len(victims)*7/10] {
				delete(w.bucket, k)
			}
			ok := w.m.ApplyListing(w.listing(0), spin())
			if !ok && !w.headErr {
				t.Fatalf("%s: a legitimate shrink was rejected (the guard stuck)", label)
			}
			w.check(label+" shrink", before, ok, true)
		case 5: // retire a tracked key with its delete owed
			var cands []string
			for k := range before.keys {
				// Only an object that still exists can be owed a delete.
				if _, live := w.bucket[k]; live && !w.owed[k] {
					cands = append(cands, k)
				}
			}
			if len(cands) == 0 {
				continue
			}
			sort.Strings(cands)
			k := cands[w.rnd.Intn(len(cands))]
			w.m.Retire(k, "x", true)
			w.owed[k] = true
			w.check(label+" retire", before, true, false)
			// Retire changes the manifest; not a refresh. Keep generation.
		case 6: // reclaim: the owed object is deleted
			var ks []string
			for k := range w.owed {
				ks = append(ks, k)
			}
			if len(ks) == 0 {
				continue
			}
			sort.Strings(ks)
			k := ks[w.rnd.Intn(len(ks))]
			delete(w.bucket, k)
			delete(w.owed, k)
			w.m.ConfirmDeleted(k)
			w.check(label+" reclaim", before, true, false)
		case 7: // a peer flushes a new object
			acct := 1 + w.rnd.Intn(2)
			k := w.newKey(acct)
			w.bucket[k] = propObj{acct, spin()}
			w.check(label+" flush", before, true, false)
		case 9, 10: // this node publishes while a partial (9) or complete (10) listing runs
			w.publishDuringListing(label, op == 9)
		case 11: // kill -9 and restart from the persisted snapshot
			w.restartFromSnapshot(label, before)
		case 8: // HEAD outage on/off
			w.headErr = !w.headErr
			if w.headErr {
				// A shrinking listing during the outage is unconfirmed, never accepted.
				tracked := len(before.keys)
				if tracked >= 4 {
					all := w.listing(0)
					keep := all[:0:0]
					if tracked/2-1 > 0 && len(all) > tracked/2-1 {
						keep = all[:tracked/2-1]
					}
					if w.m.ApplyListing(keep, spin()) {
						t.Fatalf("%s: a shrinking listing was applied while HEAD was failing", label)
					}
					w.check(label+" head outage", before, false, false)
				}
			}
		}
	}
}

// publishDuringListing: this node flushes a file while a listing runs. The
// listing was read before the upload, so it cannot contain it; the file must
// stay tracked, once (#404 round 4).
func (w *propWorld) publishDuringListing(label string, partial bool) {
	t := w.t
	t.Helper()
	skip := 0
	if partial {
		skip = 2
	}
	listStart := spin()
	objs := w.listing(skip)
	acct := 1 + w.rnd.Intn(2)
	k := w.newKey(acct)
	w.bucket[k] = propObj{acct, spin()}
	w.m.AddFile(refreshPartition, enriched(k, 1))
	before := w.state() // the publish is not the listing's doing
	var ok bool
	if partial {
		ok = w.m.ApplyPartialListing(objs, listStart, []string{"2/"})
	} else {
		ok = w.m.ApplyListing(objs, listStart)
	}
	if ok && !w.keys()[k] {
		t.Fatalf("%s: the file published during the listing was dropped", label)
	}
	w.check(label+" publish during listing", before, ok, !partial)
}

// restartFromSnapshot: kill -9 and restart from the persisted snapshot,
// whatever the last refresh's outcome. Owed retirements survive (I1 in check),
// the tracked keys are restored, and the loaded state is not a listing.
func (w *propWorld) restartFromSnapshot(label string, before propState) {
	t := w.t
	t.Helper()
	path := filepath.Join(t.TempDir(), "m.snap")
	if err := w.m.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	re := New("b", "")
	if err := re.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	re.SetObjectProber(w.m.prober)
	w.m = re
	if w.m.Listed() || w.m.LastCompleteRefresh().Generation != 0 {
		t.Fatalf("%s: a loaded snapshot is trusted as a listing", label)
	}
	if !sameKeys(before.keys, w.keys()) {
		t.Fatalf("%s: the snapshot did not restore the tracked keys", label)
	}
	w.check(label+" restart", w.state(), true, false)
}
