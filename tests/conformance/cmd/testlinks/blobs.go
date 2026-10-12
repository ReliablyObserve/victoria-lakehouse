package main

import (
	"bufio"
	"bytes"
	"io"
	"os/exec"
	"strconv"
	"strings"
	"sync"
)

// blobReader reads file contents at a revision through ONE persistent `git cat-file --batch`
// process instead of one `git show rev:path` process per blob. The gate reads a few thousand
// blobs for a large pull request (every registry file at both revisions, every Go file of the
// lock packages), and a process per blob took about a minute alone and over 25 minutes on a
// busy host. Results are cached per (rev, path), so a file read by several checks costs one
// round trip.
//
// The answers are byte-identical to the old per-blob `git show`: a path that does not exist, an
// empty blob and any failure give nil; a name the batch protocol cannot carry (a newline in a
// path), a non-blob object (a tree) and every protocol surprise are answered by running
// `git show` for that one name, exactly as before. If the batch process dies the reader falls
// back to `git show` for the rest of the run, so it can only get slower, never different.
type blobReader struct {
	repo string

	mu     sync.Mutex
	cmd    *exec.Cmd
	in     io.WriteCloser
	out    *bufio.Reader
	broken bool
	cache  map[string][]byte
}

func newBlobReader(repo string) *blobReader {
	return &blobReader{repo: repo, cache: map[string][]byte{}}
}

// Show returns the content of rev:path, or nil when the path does not exist there (or is empty).
func (b *blobReader) Show(rev, path string) []byte {
	key := rev + ":" + path
	b.mu.Lock()
	defer b.mu.Unlock()
	if v, ok := b.cache[key]; ok {
		return v
	}
	v := b.read(key)
	b.cache[key] = v
	return v
}

// gitShow is the reference reader the batch reader must agree with.
func (b *blobReader) gitShow(key string) []byte {
	out, err := git(b.repo, "show", key)
	if err != nil || len(out) == 0 {
		return nil
	}
	return out
}

func (b *blobReader) start() bool {
	if b.cmd != nil {
		return !b.broken
	}
	if b.broken {
		return false
	}
	cmd := exec.Command("git", "cat-file", "--batch")
	cmd.Dir = b.repo
	in, err := cmd.StdinPipe()
	if err != nil {
		b.broken = true
		return false
	}
	out, err := cmd.StdoutPipe()
	if err != nil {
		b.broken = true
		return false
	}
	if err := cmd.Start(); err != nil {
		b.broken = true
		return false
	}
	b.cmd, b.in, b.out = cmd, in, bufio.NewReaderSize(out, 1<<20)
	return true
}

func (b *blobReader) read(key string) []byte {
	// the batch protocol is one name per line: a name with a line break (or a NUL) cannot be sent
	if strings.ContainsAny(key, "\n\r\x00") || !b.start() {
		return b.gitShow(key)
	}
	if _, err := io.WriteString(b.in, key+"\n"); err != nil {
		b.fail()
		return b.gitShow(key)
	}
	header, err := b.out.ReadString('\n')
	if err != nil {
		b.fail()
		return b.gitShow(key)
	}
	header = strings.TrimSuffix(header, "\n")
	if strings.HasSuffix(header, " missing") {
		return nil
	}
	fields := strings.Fields(header)
	if len(fields) != 3 {
		// ambiguous, or something this reader does not understand: the protocol is out of step
		b.fail()
		return b.gitShow(key)
	}
	size, err := strconv.Atoi(fields[2])
	if err != nil || size < 0 {
		b.fail()
		return b.gitShow(key)
	}
	body := make([]byte, size+1) // the content and the LF git prints after it
	if _, err := io.ReadFull(b.out, body); err != nil || body[size] != '\n' {
		b.fail()
		return b.gitShow(key)
	}
	if fields[1] != "blob" {
		return b.gitShow(key) // a tree or a commit: what `git show` prints for it
	}
	if size == 0 {
		return nil
	}
	return bytes.Clone(body[:size])
}

// fail abandons the batch process; every later read uses `git show`.
func (b *blobReader) fail() {
	b.broken = true
	b.closeProc()
}

func (b *blobReader) closeProc() {
	if b.cmd == nil {
		return
	}
	_ = b.in.Close()
	_ = b.cmd.Wait()
	b.cmd = nil
}

// Close ends the batch process.
func (b *blobReader) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.closeProc()
}
