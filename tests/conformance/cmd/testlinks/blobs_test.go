package main

import (
	"bytes"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func gitIn(t *testing.T, dir string, args ...string) {
	t.Helper()
	cmd := exec.Command("git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), "GIT_CONFIG_GLOBAL=/dev/null", "GIT_CONFIG_SYSTEM=/dev/null",
		"GIT_AUTHOR_NAME=t", "GIT_AUTHOR_EMAIL=t@example.com", "GIT_COMMITTER_NAME=t", "GIT_COMMITTER_EMAIL=t@example.com")
	if out, err := cmd.CombinedOutput(); err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, out)
	}
}

// TestBlobReaderMatchesGitShow pins the cat-file reader to the `git show` it replaced: every shape
// of name gives the same bytes, and nil where the old reader gave nil (missing path, empty blob).
func TestBlobReaderMatchesGitShow(t *testing.T) {
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	big := bytes.Repeat([]byte("0123456789abcdef"), 70000) // > 1 MiB: larger than the pipe buffer
	files := map[string][]byte{
		"plain.txt":         []byte("one\ntwo\n"),
		"no-newline.txt":    []byte("tail without newline"),
		"empty.txt":         {},
		"binary.bin":        {0, 1, 2, '\n', 0, '\r', 255, 254},
		"with space/a b.go": []byte("package x\n"),
		"dir/nested/c.yaml": []byte("- id: x\n"),
		"big.bin":           big,
		"é-unicode.txt":     []byte("é\n"),
	}
	for p, c := range files {
		full := filepath.Join(dir, p)
		if err := os.MkdirAll(filepath.Dir(full), 0o750); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(full, c, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if err := os.Symlink("plain.txt", filepath.Join(dir, "link")); err != nil {
		t.Fatal(err)
	}
	// a path with a line break cannot go through the line-based batch protocol
	if err := os.WriteFile(filepath.Join(dir, "new\nline.txt"), []byte("nl\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "one")
	if err := os.WriteFile(filepath.Join(dir, "plain.txt"), []byte("changed\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	gitIn(t, dir, "commit", "-q", "-a", "-m", "two")

	br := newBlobReader(dir)
	defer br.Close()
	names := []string{
		"plain.txt", "no-newline.txt", "empty.txt", "binary.bin", "with space/a b.go", "dir/nested/c.yaml",
		"big.bin", "é-unicode.txt", "link", "new\nline.txt", "missing.txt", "dir", "dir/nested", "dir/missing/x", "",
	}
	for _, rev := range []string{"HEAD", "HEAD~1", "main", "no-such-rev"} {
		for _, name := range names {
			want := br.gitShow(rev + ":" + name)
			got := br.Show(rev, name)
			if !bytes.Equal(got, want) || (got == nil) != (want == nil) {
				t.Errorf("%s:%q: batch reader gave %d bytes (nil=%v), git show gave %d bytes (nil=%v)", rev, name, len(got), got == nil, len(want), want == nil)
			}
			// a second read comes from the cache and must not move the batch stream
			if again := br.Show(rev, name); !bytes.Equal(again, want) {
				t.Errorf("%s:%q: second read differs", rev, name)
			}
		}
	}
	if got := br.Show("HEAD", "plain.txt"); string(got) != "changed\n" {
		t.Errorf("HEAD:plain.txt = %q", got)
	}
	if got := br.Show("HEAD~1", "plain.txt"); string(got) != "one\ntwo\n" {
		t.Errorf("HEAD~1:plain.txt = %q", got)
	}
}

// TestBlobReaderFallsBackWhenTheBatchProcessDies: killing the cat-file process mid-run only costs
// speed, never an answer.
func TestBlobReaderFallsBackWhenTheBatchProcessDies(t *testing.T) {
	dir := t.TempDir()
	gitIn(t, dir, "init", "-q", "-b", "main")
	for i, n := range []string{"a.txt", "b.txt", "c.txt"} {
		if err := os.WriteFile(filepath.Join(dir, n), []byte(strings.Repeat("x", i+1)+"\n"), 0o600); err != nil {
			t.Fatal(err)
		}
	}
	gitIn(t, dir, "add", "-A")
	gitIn(t, dir, "commit", "-q", "-m", "one")
	br := newBlobReader(dir)
	defer br.Close()
	if got := br.Show("HEAD", "a.txt"); string(got) != "x\n" {
		t.Fatalf("a.txt = %q", got)
	}
	br.mu.Lock()
	_ = br.cmd.Process.Kill()
	br.mu.Unlock()
	for n, want := range map[string]string{"b.txt": "xx\n", "c.txt": "xxx\n", "gone.txt": ""} {
		got := br.Show("HEAD", n)
		if string(got) != want || (want == "" && got != nil) {
			t.Errorf("after the batch process died, %s = %q (nil=%v), want %q", n, got, got == nil, want)
		}
	}
}
