package delete

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// Rollback compatibility is ONE-DIRECTIONAL, and these tests pin exactly how.
//
// Forward (old files, new binary): this release reads the previous release's
// bare id → record map and its per-id S3 objects unchanged.
//
// Backward (new files, old binary): the previous release cannot read the v2
// disk envelope at all, and the per-id S3 objects it can read carry rewrite
// records (`Superseded`) and removal markers it knows nothing about. So a
// rollback with an unfinished rewrite in flight leaves objects nothing will
// resolve. The operator instruction that follows from these tests —
// "drain the rewrites first" — is in docs/operations.md and docs/durability.md,
// and this file is what keeps that instruction true.

// legacyTombstone is the record type of the previous release, verbatim: no
// Clean, no Superseded.
type legacyTombstone struct {
	ID           string
	Query        string
	StartNs      int64
	EndNs        int64
	AffectedKeys []string
	CreatedAt    time.Time
	CreatedBy    string
	Reaped       map[string]bool
	Mode         string
}

func writeDiskCopyForTest(t *testing.T, store *TombstoneStore) (dir string, data []byte) {
	t.Helper()
	dir = t.TempDir()
	if err := store.PersistToDisk(dir); err != nil {
		t.Fatalf("PersistToDisk: %v", err)
	}
	data, err := os.ReadFile(filepath.Join(dir, "tombstones.json"))
	if err != nil {
		t.Fatalf("read tombstones.json: %v", err)
	}
	return dir, data
}

// TestRollback_ThePreviousReleaseCannotReadTheV2DiskCopy documents the failure
// an operator would hit: the old binary unmarshals tombstones.json straight into
// a map of records, and the envelope's own keys are not records.
func TestRollback_ThePreviousReleaseCannotReadTheV2DiskCopy(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(sampleTombstone("ts-rollback"))
	_, data := writeDiskCopyForTest(t, store)

	var legacy map[string]legacyTombstone
	if err := json.Unmarshal(data, &legacy); err == nil {
		t.Fatalf("the previous release can now read this file (%d records); "+
			"if that is intended, update the rollback note in docs/operations.md and docs/durability.md", len(legacy))
	}

	// The envelope is what makes it unreadable, and it is deliberate: it is what
	// carries the removal markers.
	var probe map[string]json.RawMessage
	if err := json.Unmarshal(data, &probe); err != nil {
		t.Fatalf("the disk copy is not even JSON: %v", err)
	}
	if _, ok := probe[tombstonesFileFormatKey]; !ok {
		t.Fatalf("the disk copy carries no format marker; keys=%v", probe)
	}
}

// TestRollback_ThePreviousReleasesRecordsAreRejected: the previous release's
// disk copy (a bare id → record map) still decodes, but its records name no
// tenant, so none is applied — an unscoped record is not a tombstone.
func TestRollback_ThePreviousReleasesRecordsAreRejected(t *testing.T) {
	dir := t.TempDir()
	legacy := map[string]legacyTombstone{
		"ts-old": {
			ID: "ts-old", Query: `service.name:="leaked"`, StartNs: 1, EndNs: 1 << 40,
			AffectedKeys: []string{"logs/dt=2026-03-01/hour=07/a.parquet"},
			CreatedAt:    time.Now().Add(-time.Hour), Mode: "permanent",
		},
	}
	data, err := json.Marshal(legacy)
	if err != nil {
		t.Fatalf("marshal legacy file: %v", err)
	}
	if err := os.WriteFile(filepath.Join(dir, "tombstones.json"), data, 0o600); err != nil {
		t.Fatalf("write legacy file: %v", err)
	}

	store := NewTombstoneStore()
	if err := store.LoadFromDisk(dir); err != nil {
		t.Fatalf("LoadFromDisk on the previous release's format: %v", err)
	}
	if _, ok := store.Get("ts-old"); ok {
		t.Fatal("an unscoped record from the previous release was applied")
	}
}

// TestRollback_TheS3CopyLosesTheRewriteRecords is the reason for the operator
// instruction. The old binary CAN read a per-id S3 object — and silently drops
// the record of a rewrite that is half-finished, so nothing ever resolves it:
// the replacement stays unmanifested (until the orphan sweep reclaims it) or the
// superseded object stays in the bucket with its rows.
func TestRollback_TheS3CopyLosesTheRewriteRecords(t *testing.T) {
	pool := newMockS3Pool()
	store := NewTombstoneStore()
	store.EnablePersistence(PersistenceConfig{Pool: pool, Prefix: "logs/"})
	ts := sampleTombstone("ts-inflight")
	store.Add(ts)
	store.Update("ts-inflight", func(cur *Tombstone) bool {
		cur.Superseded = map[string]Supersession{
			"logs/dt=2026-03-01/hour=07/a.parquet": {
				NewKey: "logs/dt=2026-03-01/hour=07/b2709b0d.parquet",
				State:  SupersessionPublished, At: time.Now(),
			},
		}
		return true
	})
	if n := store.FlushPending(context.Background()); n != 0 {
		t.Fatalf("%d records still owed to S3", n)
	}

	data, ok := pool.Get(TombstonePrefix("logs/") + "ts-inflight.json")
	if !ok {
		t.Fatalf("no S3 copy was written; bucket=%v", pool.Keys())
	}
	var old legacyTombstone
	if err := json.Unmarshal(data, &old); err != nil {
		t.Fatalf("the previous release cannot read the S3 copy either: %v", err)
	}
	if old.ID != "ts-inflight" || old.Query != ts.Query {
		t.Fatalf("the S3 copy no longer round-trips into the previous release's type: %+v", old)
	}

	// What it loses, and what the operator therefore has to drain first.
	if n := store.UnfinishedRewrites(); n != 1 {
		t.Fatalf("fixture: %d unfinished rewrites, want 1", n)
	}
	var full Tombstone
	if err := json.Unmarshal(data, &full); err != nil {
		t.Fatalf("unmarshal the S3 copy: %v", err)
	}
	if len(full.Superseded) != 1 {
		t.Fatalf("the S3 copy carries %d rewrite records, want 1", len(full.Superseded))
	}
	// The old type has no field for it: what a rolled-back node would write back
	// is a record with the rewrite gone, and nothing would ever finish or undo
	// it. lakehouse_delete_rewrites_unfinished is what tells the operator to
	// wait before rolling back.
	roundTripped, err := json.Marshal(old)
	if err != nil {
		t.Fatalf("marshal the legacy record: %v", err)
	}
	var afterRollback Tombstone
	if err := json.Unmarshal(roundTripped, &afterRollback); err != nil {
		t.Fatalf("unmarshal the rolled-back record: %v", err)
	}
	if len(afterRollback.Superseded) != 0 {
		t.Fatalf("the legacy type in this test is not the previous release's: it kept %d rewrite records",
			len(afterRollback.Superseded))
	}
}

// TestRollback_DrainedStoreIsSafeToRollBackTo: once no rewrite is unfinished and
// nothing is owed to S3, the S3 copies carry exactly what the old binary
// understands — the state the documented procedure waits for.
func TestRollback_DrainedStoreIsSafeToRollBackTo(t *testing.T) {
	pool := newMockS3Pool()
	store := NewTombstoneStore()
	store.EnablePersistence(PersistenceConfig{Pool: pool, Prefix: "logs/"})
	store.Add(sampleTombstone("ts-drained"))
	if n := store.FlushPending(context.Background()); n != 0 {
		t.Fatalf("%d records still owed to S3", n)
	}
	if n := store.UnfinishedRewrites(); n != 0 {
		t.Fatalf("fixture: %d unfinished rewrites, want 0", n)
	}
	if n := store.PendingS3Writes(); n != 0 {
		t.Fatalf("%d records still owed to S3 after the flush", n)
	}

	data, _ := pool.Get(TombstonePrefix("logs/") + "ts-drained.json")
	var old legacyTombstone
	if err := json.Unmarshal(data, &old); err != nil {
		t.Fatalf("the previous release cannot read the drained S3 copy: %v", err)
	}
	var full Tombstone
	if err := json.Unmarshal(data, &full); err != nil {
		t.Fatalf("unmarshal the S3 copy: %v", err)
	}
	if len(full.Superseded) != 0 {
		t.Errorf("a drained store still carries rewrite records: %+v", full.Superseded)
	}
	if old.ID != "ts-drained" {
		t.Errorf("the drained S3 copy does not round-trip: %+v", old)
	}
}
