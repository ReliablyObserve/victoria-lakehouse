package compaction

import (
	"context"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/delete"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// TestPlanGroup_SizeNotOK guards sizeOK=false (a tenant with no lifecycle rule
// past SizeMergeMaxAge): none of the size merges plan (L0 and L1 thresholds,
// closed-hour rollup, fragmentation hint), the stale-schema heal still does,
// and MinAge still applies. Without the early planHint return, backfill into
// old data keeps rewriting objects S3 may already have moved.
func TestPlanGroup_SizeNotOK(t *testing.T) {
	p := unitPolicy()
	old := ago(400 * 24 * time.Hour)
	for name, files := range map[string][]manifest.FileInfo{
		"l0 threshold":  pfs("a", 12, 0, "fp"),
		"l1 threshold":  pfs("a", 12, 1, "fp"),
		"rollup":        cat(pfs("a", 2, 0, "fp"), pfs("b", 1, 1, "fp")),
		"fragmentation": pfs("a", 3, 2, "fp"),
	} {
		if _, sel, reason, ok := p.planGroup(files, old, unitNow, "fp", false); ok {
			t.Errorf("%s: sizeOK=false planned %d files (%s)", name, len(sel), reason)
		}
		if _, _, _, ok := p.planGroup(files, old, unitNow, "fp", true); !ok {
			t.Errorf("%s: control with sizeOK=true must plan", name)
		}
	}
	level, sel, reason, ok := p.planGroup(pfs("a", 2, 1, "old"), old, unitNow, "new", false)
	if !ok || reason != reasonStale || level != 1 || len(sel) != 2 {
		t.Fatalf("stale heal with sizeOK=false: level=%d n=%d reason=%q ok=%v", level, len(sel), reason, ok)
	}
	if _, _, _, ok := p.planGroup(pfs("a", 1, 1, "old"), old, unitNow, "new", false); ok {
		t.Fatal("a lone stale file was rewritten")
	}
	if _, _, _, ok := p.planGroup(pfs("a", 2, 1, "old"), ago(10*time.Minute), unitNow, "new", false); ok {
		t.Fatal("MinAge ignored for the stale heal")
	}
}

// TestPlanGroup_MatureFilesAndHint guards planHint (M19/R10): the
// fragmentation hint merges only non-mature files, two mature top-level files
// are left alone, and a stale-schema pair heals whatever its size.
func TestPlanGroup_MatureFilesAndHint(t *testing.T) {
	p := unitPolicy()
	mature := func(key string, level int, fp string) manifest.FileInfo { return pf(key, level, matureBytes, fp) }
	two := []manifest.FileInfo{mature("a", 2, "fp"), mature("b", 2, "fp")}
	if _, _, _, ok := p.planGroup(two, ago(2*time.Hour), unitNow, "fp", true); ok {
		t.Fatal("two mature L2 files were merged")
	}
	if _, _, _, ok := p.planGroup(two, ago(72*time.Hour), unitNow, "fp", true); ok {
		t.Fatal("two mature L2 files were rolled up")
	}
	one := cat(two, pfs("s", 1, 2, "fp"))
	if _, _, _, ok := p.planGroup(one, ago(2*time.Hour), unitNow, "fp", true); ok {
		t.Fatal("one small + two mature L2 files must not merge (the small one is alone)")
	}
	small := cat(two, pfs("s", 2, 2, "fp"))
	level, sel, reason, ok := p.planGroup(small, ago(2*time.Hour), unitNow, "fp", true)
	if !ok || reason != reasonFragmented || level != 2 || len(sel) != 2 {
		t.Fatalf("two small L2 files next to mature ones: level=%d n=%d reason=%q ok=%v", level, len(sel), reason, ok)
	}
	for _, f := range sel {
		if f.Size >= matureBytes {
			t.Fatalf("mature file selected by the fragmentation hint: %+v", f)
		}
	}
	stale := []manifest.FileInfo{mature("a", 2, "old"), mature("b", 2, "old")}
	if _, sel, reason, ok := p.planGroup(stale, ago(2*time.Hour), unitNow, "new", true); !ok || reason != reasonStale || len(sel) != 2 {
		t.Fatalf("stale mature pair must still heal: n=%d reason=%q ok=%v", len(sel), reason, ok)
	}
}

// TestScan_MatureTopLevelFilesAreKept guards the hint exclusion end to end
// (R10): two mature L2 files are never merged by a scan, two small ones are,
// and a stale-schema mature pair still heals.
func TestScan_MatureTopLevelFilesAreKept(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		p := partitionAt(time.Now().Add(-72 * time.Hour))
		matureSize := func(fi *manifest.FileInfo) { fi.Size = matureBytes }

		w := newPlanWorld(t, mode)
		w.add("1001/0", p, 2, 2, 2, matureSize)
		if n, err := w.shippedScheduler().Scan(context.Background()); err != nil || n != 0 {
			t.Fatalf("two mature L2 files: merges=%d err=%v, want 0", n, err)
		}

		w = newPlanWorld(t, mode)
		w.add("1001/0", p, 2, 2, 2, nil)
		if n, err := w.shippedScheduler().Scan(context.Background()); err != nil || n != 1 {
			t.Fatalf("two small L2 files: merges=%d err=%v, want 1", n, err)
		}

		w = newPlanWorld(t, mode)
		w.add("1001/0", p, 2, 2, 2, func(fi *manifest.FileInfo) { fi.Size = matureBytes; fi.SchemaFingerprint = "fp-old" })
		if n, err := w.shippedScheduler().Scan(context.Background()); err != nil || n != 1 {
			t.Fatalf("stale mature pair: merges=%d err=%v, want 1 (heal)", n, err)
		}
	})
}

func scanOnce(t *testing.T, w *planWorld, freeze *LifecycleFreeze) int {
	t.Helper()
	s := w.shippedScheduler()
	s.freeze = freeze
	n, err := s.Scan(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	return n
}

// TestScan_SizeMergeMaxAge guards the no-rule cap end to end: with no
// lifecycle rule a 400-day-old partition gets no size merge (gauge
// size_age), its stale-schema files still heal, a young partition merges, a
// rule makes the cap not apply, a negative cap disables it, and a custom cap
// moves the line. Without it a Glacier object whose bucket rule is not
// mirrored in config would be downloaded and rewritten.
func TestScan_SizeMergeMaxAge(t *testing.T) {
	bothModes(t, func(t *testing.T, mode config.Mode) {
		ancient := partitionAt(time.Now().Add(-400 * 24 * time.Hour))
		noRule := func() *LifecycleFreeze { return &LifecycleFreeze{Detector: delete.NewStorageClassDetector(nil)} }

		w := newPlanWorld(t, mode)
		w.add("1001/0", ancient, 0, 12, 2, nil)
		if n := scanOnce(t, w, noRule()); n != 0 {
			t.Fatalf("400-day partition, no rule: merges=%d, want 0", n)
		}
		if g := metrics.CompactionFrozenFiles.Get(frozenSizeAge); g != 12 {
			t.Fatalf("frozen_files{size_age} = %d, want 12", g)
		}
		if got := len(w.m.FilesForPartition(ancient)); got != 12 {
			t.Fatalf("files rewritten: %d left", got)
		}

		// Stale-schema heal still runs on the ancient partition.
		w = newPlanWorld(t, mode)
		w.add("1001/0", ancient, 0, 3, 2, func(fi *manifest.FileInfo) { fi.SchemaFingerprint = "fp-old" })
		if n := scanOnce(t, w, noRule()); n != 1 {
			t.Fatalf("stale-schema heal on an ancient partition: merges=%d, want 1", n)
		}

		// A young partition (3 days) is under the 7-day cap.
		w = newPlanWorld(t, mode)
		w.add("1001/0", partitionAt(time.Now().Add(-72*time.Hour)), 0, 12, 2, nil)
		if n := scanOnce(t, w, noRule()); n != 1 {
			t.Fatalf("3-day partition, no rule: merges=%d, want 1", n)
		}

		// With a rule present the cap does not apply (the age freeze governs).
		w = newPlanWorld(t, mode)
		w.add("1001/0", ancient, 0, 12, 2, nil)
		withRule := &LifecycleFreeze{Detector: delete.NewStorageClassDetector(rules(3650*5, delete.ClassGlacier))}
		if n := scanOnce(t, w, withRule); n != 1 {
			t.Fatalf("400-day partition with a distant rule: merges=%d, want 1", n)
		}

		// A negative cap removes it.
		w = newPlanWorld(t, mode)
		w.add("1001/0", ancient, 0, 12, 2, nil)
		off := &LifecycleFreeze{Detector: delete.NewStorageClassDetector(nil), SizeMergeMaxAge: -1}
		if n := scanOnce(t, w, off); n != 1 {
			t.Fatalf("negative cap: merges=%d, want 1", n)
		}

		// A custom 2-day cap freezes a 3-day-old partition.
		w = newPlanWorld(t, mode)
		w.add("1001/0", partitionAt(time.Now().Add(-72*time.Hour)), 0, 12, 2, nil)
		short := &LifecycleFreeze{Detector: delete.NewStorageClassDetector(nil), SizeMergeMaxAge: 48 * time.Hour}
		if n := scanOnce(t, w, short); n != 0 {
			t.Fatalf("2-day cap, 3-day partition: merges=%d, want 0", n)
		}
	})
}
