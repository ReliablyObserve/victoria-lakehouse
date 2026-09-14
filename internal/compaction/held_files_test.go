package compaction

import (
	"context"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A HELD file is one a delete rewrite has swapped into the manifest while the
// record authorising that swap is not durable yet: the swap may still be undone,
// and merging the file in that window would carry its rows into an output the
// undo knows nothing about — the rows would exist twice, or not at all. So
// compaction never selects a held file.

// TestCompaction_HeldFilesAreNeverSelected: with one of three files held, the
// forced compaction merges the other two and leaves the held one alone.
func TestCompaction_HeldFilesAreNeverSelected(t *testing.T) {
	const partition = "dt=2026-06-03/hour=00"
	m := manifest.New("test", "logs/")
	pool := newMockPool()
	keys := seedL2(t, m, pool, partition, "v2", 3)
	held := keys[0]
	m.Hold(held)

	sched := newRecompactScheduler(m, pool, "v2")
	res, err := sched.ForceCompactPartition(context.Background(), partition, 0)
	if err != nil {
		t.Fatalf("ForceCompactPartition: %v", err)
	}
	for _, in := range res.InputFiles {
		if in == held {
			t.Fatalf("the held file %s was merged: an undo of its rewrite would now have to reach inside %s", held, res.OutputFile)
		}
	}
	if !m.HasKey(held) {
		t.Fatalf("the held file %s left the manifest", held)
	}
	if pool.get(held) == nil {
		t.Fatalf("the held object %s was deleted", held)
	}

	// Released — the swap is durable, the undo is off the table — it is back in
	// the set a compaction selects from.
	m.Release(held)
	var selectable bool
	for _, fi := range withoutHeld(m, m.FilesForPartition(partition)) {
		if fi.Key == held {
			selectable = true
		}
	}
	if !selectable {
		t.Errorf("the released file %s is still excluded from compaction", held)
	}
}

// TestWithoutHeld covers the filter on its own, including the empty and
// all-held cases the selection paths rely on.
func TestWithoutHeld(t *testing.T) {
	m := manifest.New("test", "logs/")
	files := []manifest.FileInfo{{Key: "a.parquet"}, {Key: "b.parquet"}}
	if got := withoutHeld(m, files); len(got) != 2 {
		t.Fatalf("nothing is held, got %d of 2 files", len(got))
	}
	m.Hold("a.parquet")
	got := withoutHeld(m, files)
	if len(got) != 1 || got[0].Key != "b.parquet" {
		t.Fatalf("withoutHeld = %+v, want only b.parquet", got)
	}
	m.Hold("b.parquet")
	if got := withoutHeld(m, files); len(got) != 0 {
		t.Fatalf("every file is held, got %+v", got)
	}
	if got := withoutHeld(m, nil); len(got) != 0 {
		t.Fatalf("withoutHeld(nil) = %+v", got)
	}
}

// registerOnUploadPool registers the compaction's output key in the manifest at
// the moment it is uploaded — a stand-in for another writer publishing that
// exact key between this compaction's claim and its publish. The claim makes
// that impossible within one process; the publish still has to refuse, because
// the object under the key would be the other writer's file.
type registerOnUploadPool struct {
	*mockPool
	m         *manifest.Manifest
	partition string
}

func (p *registerOnUploadPool) Upload(ctx context.Context, key string, data []byte) error {
	if err := p.mockPool.Upload(ctx, key, data); err != nil {
		return err
	}
	if len(key) > 0 && p.m != nil {
		p.m.AddFile(p.partition, manifest.FileInfo{
			Key: key, Size: 1, RowCount: 42, MinTimeNs: 1, MaxTimeNs: 2,
		})
		p.m = nil // once is enough: only the output key is hijacked
	}
	return nil
}

// TestCompaction_PublishOntoARegisteredKeyIsRefusedAndKeepsTheObject: the merge
// is abandoned with an error, and the object under the contested key — which
// belongs to the other writer — is NOT deleted.
func TestCompaction_PublishOntoARegisteredKeyIsRefusedAndKeepsTheObject(t *testing.T) {
	const partition = "dt=2026-07-01/hour=03"
	m := manifest.New("test-bucket", "")
	inner := newMockPool()

	var files []manifest.FileInfo
	for i, batch := range [][]schema.LogRow{
		{{TimestampUnixNano: 1000, Body: "a-1", SeverityText: "info", ServiceName: "web"}},
		{{TimestampUnixNano: 2000, Body: "b-1", SeverityText: "info", ServiceName: "api"}},
	} {
		key := partition + "/src-" + string(rune('a'+i)) + ".parquet"
		data, err := writeCompactedLogs(batch, 100, 1)
		if err != nil {
			t.Fatalf("write source parquet: %v", err)
		}
		inner.put(key, data)
		fi := manifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: 1,
			MinTimeNs: batch[0].TimestampUnixNano, MaxTimeNs: batch[0].TimestampUnixNano,
		}
		m.AddFile(partition, fi)
		files = append(files, fi)
	}

	pool := &registerOnUploadPool{mockPool: inner, m: m, partition: partition}
	c := NewCompactor(CompactorConfig{
		Pool: pool, Manifest: m, Mode: config.ModeLogs, RowGroupSize: 100,
	})

	before := metrics.ManifestKeyClaimRejected.Get("publish_key_taken")
	res, err := c.Compact(context.Background(), partition, files, 0)
	if err == nil {
		t.Fatalf("the publish onto a key the manifest already serves succeeded: %+v", res)
	}
	if metrics.ManifestKeyClaimRejected.Get("publish_key_taken") <= before {
		t.Error("a publish refused because its key is taken must be counted")
	}
	for _, src := range files {
		if inner.get(src.Key) == nil {
			t.Errorf("source %s was deleted although the compaction never published", src.Key)
		}
		if !m.HasKey(src.Key) {
			t.Errorf("the manifest stopped serving source %s", src.Key)
		}
	}
	// The contested object is the other writer's: it must still be there.
	var contested string
	for _, k := range inner.Keys() {
		if k != files[0].Key && k != files[1].Key {
			contested = k
		}
	}
	if contested == "" {
		t.Fatal("fixture: no output object was written")
	}
	if inner.get(contested) == nil {
		t.Fatalf("the contested object %s was deleted although it belongs to another writer", contested)
	}
	if !m.HasKey(contested) {
		t.Fatalf("the contested key %s left the manifest", contested)
	}
}
