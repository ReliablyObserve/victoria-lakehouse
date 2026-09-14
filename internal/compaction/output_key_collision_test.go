package compaction

import (
	"bytes"
	"context"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// A compaction output key is 8 hex characters inside a fixed name. Drawing one
// that is already in use is unlikely and, without a guard, destructive: the
// upload overwrites a live file with the merge of other files, and the publish
// that follows registers the merged rows while the overwritten file's rows are
// gone from the bucket but still counted in the manifest. The key is claimed
// before the upload, and a collision redraws.

// withCompactionOutputIDs makes newCompactionOutputID hand out the given ids in
// order, repeating the last one forever.
func withCompactionOutputIDs(t *testing.T, ids ...string) {
	t.Helper()
	old := newCompactionOutputID
	i := 0
	newCompactionOutputID = func() string {
		id := ids[i]
		if i < len(ids)-1 {
			i++
		}
		return id
	}
	t.Cleanup(func() { newCompactionOutputID = old })
}

const compactionCollisionID = "deadbeef"

// outputCollisionWorld: two sources to merge plus a live file already sitting on
// the key the first draw produces.
type outputCollisionWorld struct {
	pool      *mockPool
	manifest  *manifest.Manifest
	files     []manifest.FileInfo
	partition string
	bystander string
	before    []byte
}

func newOutputCollisionWorld(t *testing.T) *outputCollisionWorld {
	t.Helper()
	const partition = "dt=2026-07-01/hour=03"
	pool := newMockPool()
	m := manifest.New("test-bucket", "")

	var files []manifest.FileInfo
	var keys []string
	for i, batch := range [][]schema.LogRow{
		{{TimestampUnixNano: 1000, Body: "a-1", SeverityText: "info", ServiceName: "web"}},
		{{TimestampUnixNano: 2000, Body: "b-1", SeverityText: "info", ServiceName: "api"}},
	} {
		key := partition + "/src-" + string(rune('a'+i)) + ".parquet"
		data, err := writeCompactedLogs(batch, 100, 1)
		if err != nil {
			t.Fatalf("write source parquet: %v", err)
		}
		pool.put(key, data)
		fi := manifest.FileInfo{
			Key: key, Size: int64(len(data)), RowCount: int64(len(batch)),
			MinTimeNs: batch[0].TimestampUnixNano, MaxTimeNs: batch[0].TimestampUnixNano,
		}
		m.AddFile(partition, fi)
		files = append(files, fi)
		keys = append(keys, key)
	}

	// The bystander: a live file already under the key the first draw names.
	bystander := partition + "/compacted-L1-" + compactionCollisionID + ".parquet"
	bystanderRows := []schema.LogRow{
		{TimestampUnixNano: 3000, Body: "bystander-1", SeverityText: "info", ServiceName: "db"},
		{TimestampUnixNano: 4000, Body: "bystander-2", SeverityText: "info", ServiceName: "db"},
	}
	data, err := writeCompactedLogs(bystanderRows, 100, 1)
	if err != nil {
		t.Fatalf("write bystander parquet: %v", err)
	}
	pool.put(bystander, data)
	m.AddFile(partition, manifest.FileInfo{
		Key: bystander, Size: int64(len(data)), RowCount: int64(len(bystanderRows)),
		MinTimeNs: 3000, MaxTimeNs: 4000,
	})
	keys = append(keys, bystander)
	markListed(t, m, keys)

	return &outputCollisionWorld{
		pool: pool, manifest: m, files: files, partition: partition,
		bystander: bystander, before: append([]byte(nil), data...),
	}
}

func (w *outputCollisionWorld) compactor() *Compactor {
	return NewCompactor(CompactorConfig{
		Pool: w.pool, Manifest: w.manifest, Mode: config.ModeLogs, RowGroupSize: 100,
	})
}

func (w *outputCollisionWorld) assertBystanderIntact(t *testing.T, stage string) {
	t.Helper()
	got := w.pool.get(w.bystander)
	if got == nil {
		t.Fatalf("%s: the bystander object %s is gone", stage, w.bystander)
	}
	if !bytes.Equal(got, w.before) {
		t.Fatalf("%s: the bystander object %s was overwritten (%d bytes, was %d)",
			stage, w.bystander, len(got), len(w.before))
	}
	fi, ok := w.manifest.GetFileByKey(w.bystander)
	if !ok {
		t.Fatalf("%s: the manifest stopped serving the bystander %s", stage, w.bystander)
	}
	if fi.RowCount != 2 {
		t.Fatalf("%s: the bystander's entry claims %d rows, the object holds 2", stage, fi.RowCount)
	}
}

// TestCompaction_OutputKeyCollisionRedrawsAndNeverOverwrites: the first draw
// names a live file, so the claim rejects it and compaction draws again.
func TestCompaction_OutputKeyCollisionRedrawsAndNeverOverwrites(t *testing.T) {
	w := newOutputCollisionWorld(t)
	withCompactionOutputIDs(t, compactionCollisionID, "0badc0de")

	before := metrics.ManifestKeyClaimRejected.Get("compaction_output")
	res, err := w.compactor().Compact(context.Background(), w.partition, w.files, 0)
	if err != nil {
		t.Fatalf("Compact: %v", err)
	}
	if res.OutputFile == w.bystander {
		t.Fatalf("compaction published onto the live key %s", res.OutputFile)
	}
	if metrics.ManifestKeyClaimRejected.Get("compaction_output") == before {
		t.Error("the rejected claim was not counted")
	}
	w.assertBystanderIntact(t, "after the redraw")
	if !w.manifest.HasKey(res.OutputFile) {
		t.Errorf("the manifest does not serve the compacted output %s", res.OutputFile)
	}
}

// TestCompaction_OutputKeyCollisionThatNeverClearsAbandonsTheCompaction: every
// draw collides, so compaction fails with nothing written and nothing deleted.
func TestCompaction_OutputKeyCollisionThatNeverClearsAbandonsTheCompaction(t *testing.T) {
	w := newOutputCollisionWorld(t)
	withCompactionOutputIDs(t, compactionCollisionID)

	res, err := w.compactor().Compact(context.Background(), w.partition, w.files, 0)
	if err == nil {
		t.Fatalf("Compact succeeded with no free output key: %+v", res)
	}
	w.assertBystanderIntact(t, "after the exhausted redraws")
	for _, src := range w.files {
		if w.pool.get(src.Key) == nil {
			t.Errorf("source %s was deleted although the compaction never published", src.Key)
		}
		if !w.manifest.HasKey(src.Key) {
			t.Errorf("the manifest stopped serving source %s although the compaction failed", src.Key)
		}
	}
}
