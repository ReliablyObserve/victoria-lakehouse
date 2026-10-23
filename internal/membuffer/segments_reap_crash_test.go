package membuffer

import (
	"bufio"
	"bytes"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"
)

// A crash inside Reap's removal of a committed segment directory (#390).
//
// Reap removes the directory with upstream fs.MustRemoveDir: a marker file
// (.delete-this-dir) is created and fsynced before anything is deleted, and is
// deleted last. A kill at any point of the removal therefore leaves either the
// marker with some of the directory's entries, or an empty directory. Opening
// such a directory with logstorage.MustOpenStorage panics (parts.json, indexdb
// or a part missing) or, if only a part's file is gone, panics at the first
// query; so OpenSegments removes it first, as upstream does with its own
// partitions (fs.IsPartiallyRemovedDir in logstorage.MustOpenStorage).
//
// Opening a half-removed directory panics inside upstream's goroutines, which
// no recover can catch, so the opening runs in a child process.

// partialRemovalMarker is upstream lib/fs's marker of a directory being removed.
const partialRemovalMarker = ".delete-this-dir"

// reapCrashChildEnv tells the test binary to run one step of these tests in a
// child process: "open:<root>" or "reap:<root>".
const reapCrashChildEnv = "LH_MEMBUFFER_REAP_CRASH_CHILD"

// reapCrashTenant holds every row of these tests.
var reapCrashTenant = logstorage.TenantID{AccountID: 7, ProjectID: 9}

// runReapCrashChild runs the step named by reapCrashChildEnv, if any, and
// reports whether it did. It prints one RESULT line for the parent.
func runReapCrashChild(t *testing.T) bool {
	v := os.Getenv(reapCrashChildEnv)
	if v == "" {
		return false
	}
	step, root, _ := strings.Cut(v, ":")
	switch step {
	case "open":
		s := openSegs(t, root)
		s.DebugFlush()
		p := s.Snapshot()
		rows := countSnap(t, p, reapCrashTenant)
		p.Release()
		fmt.Printf("RESULT rows=%d active=%d dirs=%s\n", rows, s.Active().Seq(), strings.Join(segDirs(t, root), ","))
		s.Close()
	case "reap":
		// One committed segment to reap and one pending segment; nothing in the
		// active one, so the crash below loses nothing upstream had not written.
		s := openSegs(t, root)
		addRows(s, reapCrashTenant, "reaped", 3)
		g, _ := s.Seal()
		addRows(s, reapCrashTenant, "pending", 4)
		s.Seal()
		s.Commit(g, time.Now())
		blocked := firstPartDir(t, g.dir)
		if err := os.Chmod(blocked, 0o500); err != nil {
			t.Fatal(err)
		}
		fmt.Printf("SEGDIR %s\nBLOCKED %s\n", g.dir, blocked)
		_ = os.Stdout.Sync()
		// The removal cannot delete the blocked part's files: upstream's
		// MustRemoveDir stops the process there, as a kill would.
		s.Reap(time.Now(), 0)
		fmt.Println("RESULT reaped")
		s.Close()
	default:
		t.Fatalf("unknown child step %q", step)
	}
	return true
}

// runChild runs the current test's binary as a child that performs step on root.
func runChild(t *testing.T, test, step, root string) (out string, err error) {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^"+test+"$", "-test.count=1", "-test.v")
	cmd.Env = append(os.Environ(), reapCrashChildEnv+"="+step+":"+root)
	var buf bytes.Buffer
	cmd.Stdout = &buf
	cmd.Stderr = &buf
	err = cmd.Run()
	return buf.String(), err
}

// childLine returns the value after prefix on the child's first line that
// starts with it.
func childLine(out, prefix string) (string, bool) {
	sc := bufio.NewScanner(strings.NewReader(out))
	sc.Buffer(make([]byte, 1<<20), 1<<20)
	for sc.Scan() {
		if v, ok := strings.CutPrefix(sc.Text(), prefix); ok {
			return v, true
		}
	}
	return "", false
}

// openResult is what the "open" child saw.
type openResult struct {
	rows   int64
	active uint64
	dirs   []string
}

func parseOpenResult(t *testing.T, out string) openResult {
	t.Helper()
	line, ok := childLine(out, "RESULT ")
	if !ok {
		t.Fatalf("the restarted buffer did not open and answer a query; its output:\n%s", tail(out, 40))
	}
	var r openResult
	for _, f := range strings.Fields(line) {
		k, v, _ := strings.Cut(f, "=")
		switch k {
		case "rows":
			r.rows, _ = strconv.ParseInt(v, 10, 64)
		case "active":
			r.active, _ = strconv.ParseUint(v, 10, 64)
		case "dirs":
			if v != "" {
				r.dirs = strings.Split(v, ",")
			}
		}
	}
	return r
}

func tail(s string, n int) string {
	lines := strings.Split(strings.TrimRight(s, "\n"), "\n")
	if len(lines) > n {
		lines = lines[len(lines)-n:]
	}
	return strings.Join(lines, "\n")
}

// firstPartDir is the first datadb part directory of a segment.
func firstPartDir(t *testing.T, segDir string) string {
	t.Helper()
	parts, err := filepath.Glob(filepath.Join(segDir, "partitions", "*", "datadb", "*"))
	if err != nil {
		t.Fatal(err)
	}
	for _, p := range parts {
		if st, err := os.Stat(p); err == nil && st.IsDir() {
			return p
		}
	}
	t.Fatalf("no part directory in %s", segDir)
	return ""
}

// onePath is the single path matching pattern under dir.
func onePath(t *testing.T, dir, pattern string) string {
	t.Helper()
	m, err := filepath.Glob(filepath.Join(dir, pattern))
	if err != nil || len(m) != 1 {
		t.Fatalf("%s in %s: %v (%v)", pattern, dir, m, err)
	}
	return m[0]
}

// interruptedRemovals are the states a kill inside fs.MustRemoveDir can leave
// a segment directory in: the marker is there from the first deletion to the
// last, and the entries are deleted in no fixed order; after the marker itself
// is deleted only the empty directory can be left.
var interruptedRemovals = []struct {
	name   string
	damage func(t *testing.T, dir string)
}{
	{"marker only", func(*testing.T, string) {}},
	{"parts.json gone", func(t *testing.T, dir string) {
		mustRemove(t, onePath(t, dir, "partitions/*/datadb/parts.json"))
	}},
	{"indexdb gone", func(t *testing.T, dir string) {
		mustRemove(t, onePath(t, dir, "partitions/*/indexdb"))
	}},
	{"a part gone", func(t *testing.T, dir string) {
		mustRemove(t, firstPartDir(t, dir))
	}},
	{"a part's file gone", func(t *testing.T, dir string) {
		mustRemove(t, filepath.Join(firstPartDir(t, dir), "values.bin0"))
	}},
	{"flock.lock gone", func(t *testing.T, dir string) {
		mustRemove(t, filepath.Join(dir, "flock.lock"))
	}},
	{"everything but the marker gone", func(t *testing.T, dir string) {
		mustRemove(t, filepath.Join(dir, "partitions"))
		mustRemove(t, filepath.Join(dir, "flock.lock"))
	}},
	{"empty directory", func(t *testing.T, dir string) {
		mustRemove(t, filepath.Join(dir, "partitions"))
		mustRemove(t, filepath.Join(dir, "flock.lock"))
		mustRemove(t, filepath.Join(dir, partialRemovalMarker))
	}},
}

func mustRemove(t *testing.T, path string) {
	t.Helper()
	if err := os.RemoveAll(path); err != nil {
		t.Fatal(err)
	}
}

func markRemoving(t *testing.T, dir string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, partialRemovalMarker), nil, 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestSegments_InterruptedReapIsRemovedAtOpen: for every state a kill inside
// the removal of a reaped segment can leave, the restarted buffer opens, the
// half-removed directory is gone, and every other segment (committed and not
// yet reaped, pending, the old active one) is opened with all its rows.
func TestSegments_InterruptedReapIsRemovedAtOpen(t *testing.T) {
	if runReapCrashChild(t) {
		return
	}
	for _, tc := range interruptedRemovals {
		t.Run(tc.name, func(t *testing.T) {
			root := t.TempDir()
			s := openSegs(t, root)
			addRows(s, reapCrashTenant, "reaped", 3)
			reaped, _ := s.Seal()
			addRows(s, reapCrashTenant, "committed", 4)
			committed, _ := s.Seal()
			addRows(s, reapCrashTenant, "pending", 5)
			pending, _ := s.Seal()
			addRows(s, reapCrashTenant, "active", 6)
			active := s.Active()
			s.Commit(reaped, time.Now())
			s.Commit(committed, time.Now())
			s.Close()

			markRemoving(t, reaped.dir)
			tc.damage(t, reaped.dir)

			out, err := runChild(t, "TestSegments_InterruptedReapIsRemovedAtOpen", "open", root)
			if err != nil {
				t.Fatalf("the restart after an interrupted reap failed (%v); its output:\n%s", err, tail(out, 40))
			}
			r := parseOpenResult(t, out)
			if _, err := os.Stat(reaped.dir); !os.IsNotExist(err) {
				t.Errorf("the half-removed segment directory is still there (%v)", err)
			}
			if r.rows != 4+5+6 {
				t.Errorf("the restarted buffer holds %d rows, want 15 (the committed, pending and active segments' rows, none of the reaped one's)", r.rows)
			}
			want := []string{filepath.Base(committed.dir), filepath.Base(pending.dir), filepath.Base(active.dir)}
			for _, w := range want {
				found := false
				for _, d := range r.dirs {
					found = found || d == w
				}
				if !found {
					t.Errorf("segment %s is missing after the restart: dirs %v", w, r.dirs)
				}
			}
			if len(r.dirs) != len(want)+1 {
				t.Errorf("segment dirs after the restart = %v, want %v and the new active one", r.dirs, want)
			}
			if r.active <= active.Seq() {
				t.Errorf("the new active segment has seq %d, want above %d", r.active, active.Seq())
			}
		})
	}
}

// TestSegments_CrashDuringReapThenRestart is the same fault made by the code
// itself: Reap's removal is stopped half-way (a part directory it cannot
// delete stops the process, as a kill there would), and the restarted buffer
// must open, remove the rest of that segment and keep the pending one. It also
// shows that the removal marks the directory before it deletes anything.
func TestSegments_CrashDuringReapThenRestart(t *testing.T) {
	if runReapCrashChild(t) {
		return
	}
	if os.Geteuid() == 0 {
		t.Skip("permissions are not enforced for root: the removal cannot be stopped half-way")
	}
	root := t.TempDir()
	// The child is expected to stop at the blocked removal; its exit status is
	// logged, and what it left on disk is checked below.
	out, reapErr := runChild(t, "TestSegments_CrashDuringReapThenRestart", "reap", root)
	t.Logf("the reap child ended with %v", reapErr)
	segDir, ok1 := childLine(out, "SEGDIR ")
	blocked, ok2 := childLine(out, "BLOCKED ")
	if !ok1 || !ok2 {
		t.Fatalf("the reap child did not get to the reap; its output:\n%s", tail(out, 40))
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o750) })
	if _, err := os.Stat(segDir); err != nil {
		t.Fatalf("the blocked removal deleted the whole segment directory (%v); the fault did not fire", err)
	}
	if _, err := os.Stat(filepath.Join(segDir, partialRemovalMarker)); err != nil {
		t.Errorf("the interrupted removal left no %s marker in %s (%v): a restart cannot tell the half-removed segment from a live one", partialRemovalMarker, segDir, err)
	}
	if err := os.Chmod(blocked, 0o750); err != nil {
		t.Fatal(err)
	}

	out, err := runChild(t, "TestSegments_CrashDuringReapThenRestart", "open", root)
	if err != nil {
		t.Fatalf("the restart after a crash during the reap failed (%v); its output:\n%s", err, tail(out, 40))
	}
	r := parseOpenResult(t, out)
	if _, err := os.Stat(segDir); !os.IsNotExist(err) {
		t.Errorf("the half-removed segment directory is still there after the restart (%v)", err)
	}
	if r.rows != 4 {
		t.Errorf("the restarted buffer holds %d rows, want the pending segment's 4", r.rows)
	}
}

// TestSegments_InterruptedRemovalNeverReusesASeq: a removed directory's seq is
// still taken, so a new segment never gets the seq of one that was committed
// (the flusher's committed_through_seq would then skip it). Here the highest
// seq on disk is an empty directory (a crash right after a new segment's mkdir).
func TestSegments_InterruptedRemovalNeverReusesASeq(t *testing.T) {
	root := t.TempDir()
	s := openSegs(t, root)
	addRows(s, reapCrashTenant, "reaped", 2)
	g, _ := s.Seal()
	next := s.Active()
	s.Close()
	markRemoving(t, g.dir)
	for _, name := range []string{"partitions", "flock.lock"} {
		mustRemove(t, filepath.Join(next.dir, name))
	}

	s2 := openSegs(t, root)
	defer s2.Close()
	if got := s2.Active().Seq(); got <= next.Seq() {
		t.Fatalf("the new active segment has seq %d, want above %d (the seq of a removed directory was reused)", got, next.Seq())
	}
	for _, dir := range []string{g.dir, next.dir} {
		if _, err := os.Stat(dir); !os.IsNotExist(err) {
			t.Errorf("%s is still there (%v)", dir, err)
		}
	}
	if p := s2.Pending(); len(p) != 0 {
		t.Errorf("%d pending segments, want none", len(p))
	}
}

// TestSegments_StartupRemovalStoppedHalfWayIsFinishedNextStart: the removal
// that OpenSegments makes of a half-removed directory can itself be stopped
// (a kill, or here a part directory it cannot delete). It must keep the marker
// until the directory is gone, so the start after that recognises the directory
// again and finishes the removal; a removal that deleted the marker first and
// gave up would leave a half-deleted directory no start can tell from a live
// one (#390's crash loop again).
func TestSegments_StartupRemovalStoppedHalfWayIsFinishedNextStart(t *testing.T) {
	if runReapCrashChild(t) {
		return
	}
	if os.Geteuid() == 0 {
		t.Skip("permissions are not enforced for root: the removal cannot be stopped half-way")
	}
	root := t.TempDir()
	s := openSegs(t, root)
	addRows(s, reapCrashTenant, "reaped", 3)
	reaped, _ := s.Seal()
	addRows(s, reapCrashTenant, "pending", 4)
	s.Seal()
	s.Commit(reaped, time.Now())
	s.Close()

	markRemoving(t, reaped.dir)
	blocked := firstPartDir(t, reaped.dir)
	if err := os.Chmod(blocked, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(blocked, 0o750) })

	// The first start cannot finish the removal: it stops, as upstream's
	// MustRemoveDir stops the process on an error it cannot retry.
	out, err := runChild(t, "TestSegments_StartupRemovalStoppedHalfWayIsFinishedNextStart", "open", root)
	t.Logf("the first start ended with %v", err)
	if _, err := os.Stat(filepath.Join(reaped.dir, partialRemovalMarker)); err != nil {
		t.Fatalf("the stopped startup removal did not keep the %s marker in %s (%v): the next start cannot tell the half-removed segment from a live one; first start's output:\n%s", partialRemovalMarker, reaped.dir, err, tail(out, 40))
	}

	if err := os.Chmod(blocked, 0o750); err != nil {
		t.Fatal(err)
	}
	out, err = runChild(t, "TestSegments_StartupRemovalStoppedHalfWayIsFinishedNextStart", "open", root)
	if err != nil {
		t.Fatalf("the second start failed (%v); its output:\n%s", err, tail(out, 40))
	}
	r := parseOpenResult(t, out)
	if _, err := os.Stat(reaped.dir); !os.IsNotExist(err) {
		t.Errorf("the half-removed segment directory is still there after the second start (%v)", err)
	}
	if r.rows != 4 {
		t.Errorf("the buffer holds %d rows after the second start, want the pending segment's 4", r.rows)
	}
}
