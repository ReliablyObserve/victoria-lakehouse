package manifest

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Review regression (#404 round 4, F1): a partial listing carries the skipped account's previous
// entries, but a file published under that account while the listing ran is
// ALSO carried by refreshExclusionsLocked (published_during_listing). The two
// carries stack: the key appears twice in m.files, so a query reads it twice.
func TestReview404R4_PartialListingTracksAFilePublishedDuringItOnce(t *testing.T) {
	part := refreshPartition
	k := func(acct int, name string) string {
		return fmt.Sprintf("%d/0/logs/%s/%s.parquet", acct, part, name)
	}
	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	m.AddFile(part, enriched(k(1, "a"), 1))
	m.AddFile(part, enriched(k(2, "b"), 1))
	m.AddFile(part, enriched(k(2, "c"), 1))
	if !m.ApplyListing([]ListedObject{{Key: k(1, "a"), Size: 100}, {Key: k(2, "b"), Size: 100}, {Key: k(2, "c"), Size: 100}}, time.Now()) {
		t.Fatal("fixture")
	}
	listStart := time.Now()
	time.Sleep(2 * time.Millisecond)
	m.AddFile(part, enriched(k(2, "new"), 1)) // flushed while the listing runs
	dupBefore := metrics.DuplicateFileKeys.Get("manifest_refresh")
	if !m.ApplyPartialListing([]ListedObject{{Key: k(1, "a"), Size: 100}}, listStart, []string{"2/"}) {
		t.Fatal("partial listing rejected")
	}
	// The carry itself must not duplicate: the invariant check behind it is a
	// safety net, not the mechanism.
	if got := metrics.DuplicateFileKeys.Get("manifest_refresh") - dupBefore; got != 0 {
		t.Fatalf("the partial listing's carry produced %d duplicate entr(ies) for the invariant check to drop", got)
	}
	n := 0
	for _, fi := range m.AllFiles()[part] {
		if fi.Key == k(2, "new") {
			n++
		}
	}
	q := 0
	for _, fi := range m.GetFilesForRange(0, 1<<62) {
		if fi.Key == k(2, "new") {
			q++
		}
	}
	t.Logf("GetFilesForRange returns the key %d times; FilesForPartition (compaction input) %d", q, func() int {
		c := 0
		for _, fi := range m.FilesForPartition(part) {
			if fi.Key == k(2, "new") {
				c++
			}
		}
		return c
	}())
	if n != 1 || m.TotalFiles() != 4 {
		t.Fatalf("the file published during a partial listing is tracked %d times (TotalFiles=%d, want 4): its rows are served twice", n, m.TotalFiles())
	}
}

// Review regression (#404 round 4): Listed() is sticky. After one complete listing, a later
// listing that silently drops < half of the objects (a successful LIST, every
// prefix covered) is applied as complete and advances CompleteSince: every
// consumer then reads the dropped LIVE key as gone. This documents the
// residual the consumers must defend against per object.
func TestReview404R4_SilentSmallDropIsBelievedComplete(t *testing.T) {
	m, keys := manifestWithKeys(10)
	var objs []ListedObject
	for _, key := range keys {
		objs = append(objs, ListedObject{Key: key, Size: 1})
	}
	if !m.ApplyListing(objs, time.Now()) {
		t.Fatal("fixture")
	}
	before := time.Now()
	time.Sleep(2 * time.Millisecond)
	if !m.ApplyListing(objs[1:], time.Now()) { // keys[0] still exists, LIST lost it
		t.Fatal("a 10% drop was rejected (no probe below half)")
	}
	if m.HasKey(keys[0]) || !m.CompleteSince(before) {
		t.Fatalf("expected the silent drop to be believed: has=%v completeSince=%v", m.HasKey(keys[0]), m.CompleteSince(before))
	}
}

// Review regression (#404 round 4, F5): the warm-up and periodic persists
// write whatever state the last refresh left, including after a partial one.
// Such a state is safe to persist (a partial listing carries the skipped
// account's entries, a rejected one changes nothing) and losing it is not: with
// one tenant's LIST failing for good, skipping the write lost every key retired
// since the last snapshot, so a kill -9 let the next start adopt a compaction's
// sources next to its output. Reloaded, the state keeps its retired keys and
// the skipped entries, and is not trusted as a listing.
func TestReview404R4_PartialStatePersistsRetiredKeysAndIsNotTrustedOnLoad(t *testing.T) {
	part := refreshPartition
	k := func(acct int, name string) string { return fmt.Sprintf("%d/0/logs/%s/%s.parquet", acct, part, name) }
	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	for _, key := range []string{k(1, "a"), k(1, "b"), k(2, "c")} {
		m.AddFile(part, enriched(key, 1))
	}
	all := []ListedObject{{Key: k(1, "a"), Size: 100}, {Key: k(1, "b"), Size: 100}, {Key: k(2, "c"), Size: 100}}
	if !m.ApplyListing(all, time.Now()) {
		t.Fatal("fixture")
	}
	// A compaction replaces a and b; their deletes fail (owed, objects remain).
	if !m.ReplaceFiles(part, []string{k(1, "a"), k(1, "b")}, enriched(k(1, "out"), 2)) {
		t.Fatal("fixture: publish refused")
	}
	// Account 2's LIST fails from now on: every refresh is partial.
	time.Sleep(2 * time.Millisecond)
	listed := []ListedObject{{Key: k(1, "a"), Size: 100}, {Key: k(1, "b"), Size: 100}, {Key: k(1, "out"), Size: 100}}
	if !m.ApplyPartialListing(listed, time.Now(), []string{"2/"}) {
		t.Fatal("partial listing rejected")
	}
	path := filepath.Join(t.TempDir(), "m.snap")
	if err := m.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	re := New("b", "")
	re.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	if err := re.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	for _, src := range []string{k(1, "a"), k(1, "b")} {
		rk, ok := re.LookupRetired(src)
		if !ok || !rk.Reclaim {
			t.Fatalf("the owed retirement of %s was lost across the restart: %+v ok=%v", src, rk, ok)
		}
		if re.HasKey(src) {
			t.Fatalf("%s is tracked after the restart", src)
		}
	}
	if !re.HasKey(k(1, "out")) || !re.HasKey(k(2, "c")) {
		t.Fatal("the output or the skipped account's entry was lost")
	}
	if re.Listed() || re.LastCompleteRefresh().Generation != 0 || re.CompleteSince(time.Time{}) {
		t.Fatal("a loaded snapshot is trusted as a complete listing")
	}
	// The next listing still sees the sources: they stay out of the manifest.
	if !re.ApplyPartialListing(listed, time.Now(), []string{"2/"}) {
		t.Fatal("partial listing after restart rejected")
	}
	if re.HasKey(k(1, "a")) || re.HasKey(k(1, "b")) || re.TotalFiles() != 2 {
		t.Fatalf("after the restart the sources were adopted next to their output (files=%d)", re.TotalFiles())
	}
}

// Review regression (#420): SaveTo encodes the snapshot after releasing the
// lock; it used to encode m.files itself, so a flush adding a partition while
// the encoder iterated the map was a concurrent map iteration and write (a
// fatal runtime error). Run with -race.
func TestReview420_SaveToConcurrentWithAddFile(t *testing.T) {
	m := New("b", "")
	for i := 0; i < 50; i++ {
		m.AddFile(fmt.Sprintf("dt=2026-06-%02d/hour=00", 1+i%28), enriched(fmt.Sprintf("logs/seed-%03d.parquet", i), 1))
	}
	path := filepath.Join(t.TempDir(), "m.snap")
	done := make(chan struct{})
	go func() {
		defer close(done)
		for i := 0; i < 400; i++ {
			m.AddFile(fmt.Sprintf("dt=2026-07-%02d/hour=%02d", 1+i%28, i%24), enriched(fmt.Sprintf("logs/add-%04d.parquet", i), 1))
		}
	}()
	for i := 0; i < 20; i++ {
		if err := m.SaveTo(path); err != nil {
			t.Fatal(err)
		}
	}
	<-done
	re := New("b", "")
	if err := re.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	if err := m.SaveTo(path); err != nil {
		t.Fatal(err)
	}
	if err := re.LoadFrom(path); err != nil || re.TotalFiles() != m.TotalFiles() {
		t.Fatalf("reload: err=%v files=%d want %d", err, re.TotalFiles(), m.TotalFiles())
	}
}

// Review regression (#404 round 4, F6): the refresh timestamps the manifest
// alerts read. A complete listing sets both (the complete one to the listing's
// start), a partial one only the last-refresh one, a rejected one neither.
func TestReview404R4_RefreshTimestampGauges(t *testing.T) {
	part := refreshPartition
	k := func(acct int, name string) string { return fmt.Sprintf("%d/0/logs/%s/%s.parquet", acct, part, name) }
	m := New("b", "")
	m.SetPrefixTemplate("{AccountID}/{ProjectID}/")
	var objs []ListedObject
	for i := 0; i < 6; i++ {
		key := k(1+i%2, fmt.Sprintf("f%d", i))
		m.AddFile(part, enriched(key, 1))
		objs = append(objs, ListedObject{Key: key, Size: 100})
	}
	start := time.Now().Add(-time.Second)
	if !m.ApplyListing(objs, start) {
		t.Fatal("fixture")
	}
	near := func(got float64, want time.Time) bool {
		d := got - float64(want.UnixNano())/1e9
		return d > -0.01 && d < 0.01
	}
	if !near(metrics.ManifestLastCompleteRefreshTimestamp.Get(), start) {
		t.Fatalf("complete timestamp %v, want the listing start %v", metrics.ManifestLastCompleteRefreshTimestamp.Get(), start)
	}
	if !near(metrics.ManifestLastRefreshTimestamp.Get(), time.Now()) && metrics.ManifestLastRefreshTimestamp.Get() < float64(start.Unix()) {
		t.Fatal("last refresh timestamp not set by a complete listing")
	}
	complete := metrics.ManifestLastCompleteRefreshTimestamp.Get()
	time.Sleep(20 * time.Millisecond)
	before := metrics.ManifestLastRefreshTimestamp.Get()
	if !m.ApplyPartialListing(objs[:3], time.Now(), []string{"2/"}) {
		t.Fatal("partial rejected")
	}
	if metrics.ManifestLastCompleteRefreshTimestamp.Get() != complete {
		t.Fatal("a partial listing moved the complete-refresh timestamp")
	}
	if metrics.ManifestLastRefreshTimestamp.Get() <= before {
		t.Fatal("a partial listing did not move the last-refresh timestamp")
	}
	last := metrics.ManifestLastRefreshTimestamp.Get()
	time.Sleep(20 * time.Millisecond)
	if m.ApplyListing(objs[:1], time.Now()) { // drops most, no prober: rejected
		t.Fatal("fixture: the shrink must be rejected without a prober")
	}
	if metrics.ManifestLastRefreshTimestamp.Get() != last || metrics.ManifestLastCompleteRefreshTimestamp.Get() != complete {
		t.Fatal("a rejected listing moved a refresh timestamp")
	}
}

// Review regression (#404 round 4, F1 defence in depth): the "each key at most
// once" invariant is enforced where a file map replaces the tracked set — a
// refresh and a snapshot load — whatever built the map: the second entry is
// dropped and counted by site.
func TestReview404R4_DuplicateKeysDroppedOnRefreshAndLoad(t *testing.T) {
	a := fmt.Sprintf("logs/%s/a.parquet", refreshPartition)
	other := "dt=2026-06-04/hour=01"
	files := map[string][]FileInfo{
		refreshPartition: {enriched(a, 1), enriched(a, 1), enriched(fmt.Sprintf("logs/%s/b.parquet", refreshPartition), 1)},
		other:            {enriched(a, 1)},
	}
	before := metrics.DuplicateFileKeys.Get("manifest_refresh")
	if n := dedupeRefreshedFiles(files, "manifest_refresh"); n != 2 {
		t.Fatalf("dropped %d, want 2", n)
	}
	if got := metrics.DuplicateFileKeys.Get("manifest_refresh") - before; got != 2 {
		t.Fatalf("counted %d, want 2", got)
	}
	if len(files[refreshPartition]) != 2 || len(files[other]) != 0 {
		t.Fatalf("after dedupe: %v", files)
	}

	// A snapshot holding a key twice loads it once, with consistent totals.
	snap := persistedManifest{Files: map[string][]FileInfo{refreshPartition: {enriched(a, 1), enriched(a, 1)}}, TotalFiles_: 2, TotalBytes_: 200}
	var buf bytes.Buffer
	buf.Write(manifestBinaryMagic)
	if err := gob.NewEncoder(&buf).Encode(&snap); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(t.TempDir(), "dup.snap")
	if err := os.WriteFile(path, buf.Bytes(), 0o600); err != nil {
		t.Fatal(err)
	}
	beforeLoad := metrics.DuplicateFileKeys.Get("manifest_load")
	m := New("b", "")
	if err := m.LoadFrom(path); err != nil {
		t.Fatal(err)
	}
	if m.TotalFiles() != 1 || len(m.FilesForPartition(refreshPartition)) != 1 || m.TotalBytes() != 100 {
		t.Fatalf("loaded files=%d entries=%d bytes=%d, want 1/1/100", m.TotalFiles(), len(m.FilesForPartition(refreshPartition)), m.TotalBytes())
	}
	if got := metrics.DuplicateFileKeys.Get("manifest_load") - beforeLoad; got != 1 {
		t.Fatalf("manifest_load counted %d, want 1", got)
	}
}

// The HEAD sample of a shrinking listing draws from crypto/rand (gosec G404)
// through sampleIntn, which stays in range and is the only randomness the
// sample uses: an injected sampler sees one draw per dropped key past the
// first cliffProbeSample.
func TestReview404R4_ShrinkSampleUsesTheInjectedSampler(t *testing.T) {
	for _, n := range []int{0, 1, 2, 17, 1 << 20} {
		for i := 0; i < 200; i++ {
			if v := sampleIntn(n); v < 0 || (n > 1 && v >= n) || (n <= 1 && v != 0) {
				t.Fatalf("sampleIntn(%d) = %d", n, v)
			}
		}
	}
	orig := sampleIntn
	defer func() { sampleIntn = orig }()
	draws := 0
	sampleIntn = func(n int) int { draws++; return n - 1 }
	m, keys := manifestWithKeys(40)
	m.SetObjectProber(func(context.Context, string, string) (bool, error) { return false, nil })
	if !m.ApplyListing([]ListedObject{{Key: keys[0], Size: 1}}, time.Now()) {
		t.Fatal("a confirmed shrink was rejected")
	}
	if want := 39 - cliffProbeSample; draws != want {
		t.Fatalf("sampler draws = %d, want %d", draws, want)
	}
}
