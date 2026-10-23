package parquets3

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// A kill inside the removal of a committed, reaped segment (#390), seen from
// the flusher: the restart opens the buffer, the half-removed segment is gone,
// its rows are not uploaded again (they are in Parquet already), and every
// other acknowledged row is drained exactly once. The states are those a kill
// inside upstream's fs.MustRemoveDir can leave (its marker file, written before
// anything is deleted, with some entries gone; or the empty directory); the
// membuffer tests cover every such state and a removal stopped by the code.
func TestSegments_RestartAfterAnInterruptedReap(t *testing.T) {
	const marker = ".delete-this-dir" // upstream lib/fs's marker of a directory being removed
	cases := []struct {
		name   string
		damage func(t *testing.T, dir string)
	}{
		{"marker only", func(*testing.T, string) {}},
		{"parts.json gone", func(t *testing.T, dir string) {
			m, err := filepath.Glob(filepath.Join(dir, "partitions", "*", "datadb", "parts.json"))
			if err != nil || len(m) != 1 {
				t.Fatalf("parts.json in %s: %v (%v)", dir, m, err)
			}
			if err := os.Remove(m[0]); err != nil {
				t.Fatal(err)
			}
		}},
		{"empty directory", func(t *testing.T, dir string) {
			for _, name := range []string{"partitions", "flock.lock", marker} {
				if err := os.RemoveAll(filepath.Join(dir, name)); err != nil {
					t.Fatal(err)
				}
			}
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := newSegEnv(t)
			e.ingest(segTenantA, hourAgo.Add(5*time.Minute), 10)
			reaped := e.seal()
			f := e.flusher(1000)
			if err := f.drain(context.Background(), reaped); err != nil {
				t.Fatal(err)
			}
			if !e.markerStored(reaped.Nonce()) {
				t.Fatal("the first segment did not commit")
			}
			e.ingest(segTenantB, hourAgo.Add(6*time.Minute), 7)
			e.seal()                                            // pending at the crash
			e.ingest(segTenantA, hourAgo.Add(7*time.Minute), 3) // in the active segment at the crash

			// The process dies while it removes the reaped segment's directory.
			e.u.mu.Lock()
			e.u.allowAttemptDrift = true
			e.u.mu.Unlock()
			e.segs.Close()
			dir := filepath.Join(e.dir, "buffer", fmt.Sprintf("seg-%016x-%s", reaped.Seq(), reaped.Nonce()))
			if err := os.WriteFile(filepath.Join(dir, marker), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			tc.damage(t, dir)
			e.newWriter()
			e.open()

			f2 := e.flusher(1000)
			e.drainAll(f2)
			e.mustHaveExactly(20, "after the restart")
			e.storedOnce("after the restart")
			if _, err := os.Stat(dir); !os.IsNotExist(err) {
				t.Errorf("the half-removed segment directory is still there (%v)", err)
			}
			if n := len(e.segs.Pending()); n != 0 {
				t.Errorf("%d segments still pending", n)
			}
		})
	}
}
