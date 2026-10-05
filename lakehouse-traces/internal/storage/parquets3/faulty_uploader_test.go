package parquets3

import (
	"context"
	"crypto/sha256"
	"errors"
	"fmt"
	"sync"
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// faultyUploader stands in for object storage: fail decides, per key, whether
// an upload fails; block, when set, holds every upload until it is closed.
//
// It also enforces the storage invariant of the insert path: the bytes stored
// under a key never change. Every upload that succeeds records the sha256 of its
// bytes; a second one under the same key with different bytes is a violation,
// reported by checkByteInvariant (durabilityWriter registers it as a cleanup, so
// every test built on it asserts the invariant). Attempts that failed are
// recorded separately in attemptHashes: a retry in the same process must send
// the same bytes too, which allowAttemptDrift relaxes for the one case where a
// restarted flusher deliberately collects a never-stored group afresh.
type faultyUploader struct {
	mu       sync.Mutex
	fail     func(key string) error
	block    chan struct{}
	started  chan struct{}
	uploaded map[string]int
	// stored is the sha256 of the bytes stored under each key; data the bytes.
	stored map[string]string
	data   map[string][]byte
	// attemptHashes lists the sha256 of every attempt (stored or not) per key.
	attemptHashes map[string][]string
	violations    []string
	// allowAttemptDrift permits failed attempts of a key to differ in bytes.
	allowAttemptDrift bool
	// HEAD side: headErr injects an error for a key's existence check, heads
	// counts the checks per key, gone lists objects deleted from the bucket.
	headErr func(key string) error
	heads   map[string]int
	gone    map[string]bool
}

// Exists is the object store's HEAD: it reports whether key is stored and not
// deleted, or headErr's error.
func (u *faultyUploader) Exists(_ context.Context, key string) (bool, error) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.heads == nil {
		u.heads = make(map[string]int)
	}
	u.heads[key]++
	if u.headErr != nil {
		if err := u.headErr(key); err != nil {
			return false, err
		}
	}
	if u.gone[key] {
		return false, nil
	}
	_, ok := u.data[key]
	return ok, nil
}

// headCount returns how many existence checks key has had.
func (u *faultyUploader) headCount(key string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.heads[key]
}

func (u *faultyUploader) Upload(ctx context.Context, key string, data []byte) error {
	if u.started != nil {
		select {
		case u.started <- struct{}{}:
		default:
		}
	}
	if u.block != nil {
		select {
		case <-u.block:
		case <-ctx.Done():
			return ctx.Err()
		}
	}
	if err := ctx.Err(); err != nil {
		return err
	}
	u.mu.Lock()
	defer u.mu.Unlock()
	sum := fmt.Sprintf("%x", sha256.Sum256(data))
	if u.attemptHashes == nil {
		u.attemptHashes = make(map[string][]string)
	}
	if prev := u.attemptHashes[key]; len(prev) > 0 && prev[0] != sum && !u.allowAttemptDrift {
		u.violations = append(u.violations, fmt.Sprintf("%s attempted with different bytes (%s then %s)", key, prev[0][:12], sum[:12]))
	}
	u.attemptHashes[key] = append(u.attemptHashes[key], sum)
	if u.fail != nil {
		if err := u.fail(key); err != nil {
			return err
		}
	}
	if u.uploaded == nil {
		u.uploaded = make(map[string]int)
		u.stored = make(map[string]string)
		u.data = make(map[string][]byte)
	}
	if prev, ok := u.stored[key]; ok && prev != sum {
		u.violations = append(u.violations, fmt.Sprintf("%s rewritten with different bytes (%s then %s)", key, prev[:12], sum[:12]))
	}
	u.uploaded[key]++
	u.stored[key] = sum
	u.data[key] = append([]byte(nil), data...)
	return nil
}

// checkByteInvariant fails t if any key was ever stored, or attempted, with
// different bytes than an earlier upload of the same key.
func (u *faultyUploader) checkByteInvariant(t *testing.T) {
	t.Helper()
	u.mu.Lock()
	defer u.mu.Unlock()
	for _, v := range u.violations {
		t.Errorf("byte invariant violated: %s", v)
	}
}

// attempts returns how many uploads of key were attempted (stored or not).
func (u *faultyUploader) attempts(key string) int {
	u.mu.Lock()
	defer u.mu.Unlock()
	return len(u.attemptHashes[key])
}

// durabilityWriter is a BatchWriter whose span uploads go to u.
func durabilityWriter(t *testing.T, u *faultyUploader) (*BatchWriter, *manifest.Manifest) {
	t.Helper()
	s3srv := mockS3()
	t.Cleanup(s3srv.Close)
	bw, m := testWriter(t, s3srv.URL)
	bw.SetTenantBucket(func(uint32, uint32) string { return "durability" })
	bw.SetTenantPool(func(string) PoolWriter { return u })
	t.Cleanup(func() { u.checkByteInvariant(t) })
	return bw, m
}

func committedRows(m *manifest.Manifest) (rows int64, files int) {
	for _, part := range m.AllFiles() {
		for _, fi := range part {
			rows += fi.RowCount
			files++
		}
	}
	return rows, files
}

var errPutFailed = errors.New("PutObject: context deadline exceeded")
