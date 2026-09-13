package delete

import (
	"bytes"
	"context"
	"errors"
	"sort"
	"sync"
	"testing"

	"github.com/parquet-go/parquet-go"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/testutil/storageinvariants"
)

var errInjected = errors.New("injected fault")

// newTestManifest returns a manifest that already registers every key given,
// with the declared row counts. Every delete test starts from a manifest and a
// bucket that AGREE — an invariant check on a fixture that never agreed proves
// nothing.
func newTestManifest(t *testing.T, rowsByKey map[string]int64) *manifest.Manifest {
	t.Helper()
	m := manifest.New("test-bucket", "")
	keys := make([]string, 0, len(rowsByKey))
	for k := range rowsByKey {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		m.AddFile(extractPartition(k), manifest.FileInfo{
			Key:       k,
			Size:      1024,
			RowCount:  rowsByKey[k],
			MinTimeNs: 1,
			MaxTimeNs: 1 << 40,
		})
	}
	return m
}

// manifestFromPool registers every .parquet object the pool holds, reading each
// object's true row count out of the file so manifest metadata and file
// contents start in agreement.
func manifestFromPool(t *testing.T, pool *mockRewriterPool) *manifest.Manifest {
	t.Helper()
	rows := map[string]int64{}
	for _, k := range pool.Keys() {
		data := mustGet(t, pool, k)
		rows[k] = int64(countLogRows(t, data))
	}
	return newTestManifest(t, rows)
}

// countLogRows returns how many rows a stored object actually holds, so a test
// can compare what the manifest CLAIMS against what the file HOLDS.
func countLogRows(t *testing.T, data []byte) int {
	t.Helper()
	r := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
	defer func() { _ = r.Close() }()
	n := int(r.NumRows())
	if n == 0 {
		return 0
	}
	rows := make([]schema.LogRow, n)
	got, err := r.Read(rows)
	if err != nil && got == 0 {
		t.Fatalf("read parquet rows: %v", err)
	}
	return got
}

// scanLogRows returns every row a stored object holds — the full-scan answer a
// test compares the manifest's RowCount sum against.
func scanLogRows(t *testing.T, pool *mockRewriterPool) []schema.LogRow {
	t.Helper()
	var all []schema.LogRow
	for _, k := range pool.Keys() {
		data := mustGet(t, pool, k)
		r := parquet.NewGenericReader[schema.LogRow](bytes.NewReader(data))
		n := int(r.NumRows())
		if n > 0 {
			rows := make([]schema.LogRow, n)
			got, err := r.Read(rows)
			if err != nil && got == 0 {
				_ = r.Close()
				t.Fatalf("read parquet rows from %s: %v", k, err)
			}
			all = append(all, rows[:got]...)
		}
		_ = r.Close()
	}
	return all
}

// faultPool wraps the in-memory pool and fails one chosen operation, so a test
// can break the rewrite at an exact step and assert what recovery converges to.
type faultPool struct {
	*mockRewriterPool

	mu sync.Mutex
	// failUpload fires once on the next Upload.
	failUpload bool
	// failDeleteOn fires once on the next Delete of that exact key.
	failDeleteOn string
}

func newFaultPool(inner *mockRewriterPool) *faultPool {
	return &faultPool{mockRewriterPool: inner}
}

func (f *faultPool) Upload(ctx context.Context, key string, data []byte) error {
	f.mu.Lock()
	fail := f.failUpload
	f.failUpload = false
	f.mu.Unlock()
	if fail {
		return errInjected
	}
	return f.mockRewriterPool.Upload(ctx, key, data)
}

func (f *faultPool) Delete(ctx context.Context, key string) error {
	f.mu.Lock()
	fail := f.failDeleteOn != "" && f.failDeleteOn == key
	if fail {
		f.failDeleteOn = ""
	}
	f.mu.Unlock()
	if fail {
		return errInjected
	}
	return f.mockRewriterPool.Delete(ctx, key)
}

// failingManifest wraps a real manifest and can refuse one step of the
// hand-off. That is how the crash matrix reaches the windows between
// "replacement uploaded" and "manifest updated" — the exact states a process
// killed mid-rewrite leaves behind.
// Each flag fires ONCE and then clears, matching faultPool: a crash is a single
// event, and the retry that follows has to be clean for the test to show that
// recovery converges.
type failingManifest struct {
	mu    sync.Mutex
	inner ManifestUpdater
	// failReplace makes the next ReplaceFile a no-op: the replacement object
	// exists but the manifest never learns about it.
	failReplace bool
	// failRemove makes the next RemoveFile a no-op: the RowsKept == 0 path.
	failRemove bool
}

func wrapManifest(m ManifestUpdater) *failingManifest { return &failingManifest{inner: m} }

func (f *failingManifest) GetFileByKey(key string) (manifest.FileInfo, bool) {
	return f.inner.GetFileByKey(key)
}

func (f *failingManifest) PartitionForKey(key string) (string, bool) {
	return f.inner.PartitionForKey(key)
}

func (f *failingManifest) ReplaceFile(partition string, oldKey string, fi manifest.FileInfo) bool {
	f.mu.Lock()
	fail := f.failReplace
	f.failReplace = false
	f.mu.Unlock()
	if fail {
		return false
	}
	return f.inner.ReplaceFile(partition, oldKey, fi)
}

func (f *failingManifest) RemoveFile(partition string, key string) {
	f.mu.Lock()
	fail := f.failRemove
	f.failRemove = false
	f.mu.Unlock()
	if fail {
		return
	}
	f.inner.RemoveFile(partition, key)
}

func (f *failingManifest) HasKey(key string) bool { return f.inner.HasKey(key) }

// mustGet returns a stored object or fails the test — used where the object's
// absence would make the rest of the assertion meaningless.
func mustGet(t *testing.T, pool interface{ Get(string) ([]byte, bool) }, key string) []byte {
	t.Helper()
	data, ok := pool.Get(key)
	if !ok {
		t.Fatalf("expected object at %s", key)
	}
	return data
}

// buildTestParquetWithSlots writes a Parquet file carrying the Tier-2
// dedicated-slot binding in its footer KV, so a test can assert the rewriter
// carries that binding into the replacement.
func buildTestParquetWithSlots(t *testing.T, rows []schema.LogRow, slotJSON string) []byte {
	t.Helper()
	var buf bytes.Buffer
	w := parquet.NewGenericWriter[schema.LogRow](&buf,
		parquet.MaxRowsPerRowGroup(100),
		parquet.KeyValueMetadata(schema.DedicatedSlotsMetaKey, slotJSON),
	)
	if _, err := w.Write(rows); err != nil {
		t.Fatalf("write test parquet: %v", err)
	}
	if err := w.Close(); err != nil {
		t.Fatalf("close test parquet writer: %v", err)
	}
	return buf.Bytes()
}

// tombstoneViews adapts the store to the invariant checker's dependency-free
// view type. The checker cannot import this package (it is imported BY this
// package's tests), so the conversion lives here.
func tombstoneViews(store *TombstoneStore) []storageinvariants.TombstoneView {
	if store == nil {
		return nil
	}
	active := store.Active()
	out := make([]storageinvariants.TombstoneView, 0, len(active))
	for _, ts := range active {
		out = append(out, storageinvariants.TombstoneView{
			ID:           ts.ID,
			Mode:         ts.Mode,
			AffectedKeys: ts.AffectedKeys,
			Reaped:       ts.Reaped,
		})
	}
	return out
}
