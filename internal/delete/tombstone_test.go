package delete

import (
	"context"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

// mockS3Pool is an in-memory mock implementing the S3Pool interface.
type mockS3Pool struct {
	mu      sync.Mutex
	objects map[string][]byte
}

func newMockS3Pool() *mockS3Pool {
	return &mockS3Pool{objects: make(map[string][]byte)}
}

func (m *mockS3Pool) Upload(_ context.Context, key string, data []byte) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.objects[key] = append([]byte(nil), data...)
	return nil
}

func (m *mockS3Pool) Download(_ context.Context, key string) ([]byte, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	data, ok := m.objects[key]
	if !ok {
		return nil, context.DeadlineExceeded // simulate not-found
	}
	return data, nil
}

func (m *mockS3Pool) List(_ context.Context, prefix string) ([]string, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	var keys []string
	for k := range m.objects {
		if len(k) >= len(prefix) && k[:len(prefix)] == prefix {
			keys = append(keys, k)
		}
	}
	return keys, nil
}

func (m *mockS3Pool) Delete(_ context.Context, key string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.objects, key)
	return nil
}

func TestMatchesRow_WithinRangeMatchingField(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		Query:   `level:="error"`,
		StartNs: 1000,
		EndNs:   2000,
	}

	row := map[string]string{"level": "error", "body": "something failed"}
	if !ts.MatchesRow(row, 1500) {
		t.Fatal("expected MatchesRow to return true for matching row within time range")
	}
}

func TestMatchesRow_OutsideTimeRange(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		Query:   `level:="error"`,
		StartNs: 1000,
		EndNs:   2000,
	}

	row := map[string]string{"level": "error", "body": "something failed"}
	if ts.MatchesRow(row, 3000) {
		t.Fatal("expected MatchesRow to return false for timestamp outside range")
	}
}

func TestMatchesRow_DifferentFieldValue(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		Query:   `level:="error"`,
		StartNs: 1000,
		EndNs:   2000,
	}

	row := map[string]string{"level": "info", "body": "all good"}
	if ts.MatchesRow(row, 1500) {
		t.Fatal("expected MatchesRow to return false for non-matching field value")
	}
}

func TestMatchesRow_SubstringQuery(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		Query:   `body:"timeout"`,
		StartNs: 1000,
		EndNs:   2000,
	}

	row := map[string]string{"body": "connection timeout occurred"}
	if !ts.MatchesRow(row, 1500) {
		t.Fatal("expected substring query to match")
	}

	row2 := map[string]string{"body": "success"}
	if ts.MatchesRow(row2, 1500) {
		t.Fatal("expected substring query to not match different content")
	}
}

func TestMatchesRow_WildcardQuery(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		Query:   "*",
		StartNs: 1000,
		EndNs:   2000,
	}

	row := map[string]string{"body": "anything"}
	if !ts.MatchesRow(row, 1500) {
		t.Fatal("expected wildcard query to match any row")
	}
}

func TestMatchesRow_EmptyQuery(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		Query:   "",
		StartNs: 1000,
		EndNs:   2000,
	}

	row := map[string]string{"body": "anything"}
	if !ts.MatchesRow(row, 1500) {
		t.Fatal("expected empty query to match any row")
	}
}

func TestMatchesRow_FallbackBodySubstring(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		Query:   "panic",
		StartNs: 1000,
		EndNs:   2000,
	}

	row := map[string]string{"body": "goroutine panic in handler"}
	if !ts.MatchesRow(row, 1500) {
		t.Fatal("expected fallback body substring to match")
	}

	row2 := map[string]string{"body": "normal operation"}
	if ts.MatchesRow(row2, 1500) {
		t.Fatal("expected fallback body substring to not match")
	}
}

func TestAffectsFile_Overlapping(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		StartNs: 1000,
		EndNs:   2000,
	}

	// File range overlaps with tombstone
	if !ts.AffectsFile(1500, 2500) {
		t.Fatal("expected AffectsFile to return true for overlapping range")
	}

	// File range fully contains tombstone
	if !ts.AffectsFile(500, 3000) {
		t.Fatal("expected AffectsFile to return true when file contains tombstone")
	}

	// Tombstone fully contains file range
	if !ts.AffectsFile(1200, 1800) {
		t.Fatal("expected AffectsFile to return true when tombstone contains file range")
	}

	// Edge overlap
	if !ts.AffectsFile(2000, 3000) {
		t.Fatal("expected AffectsFile to return true for edge overlap")
	}
}

func TestAffectsFile_NonOverlapping(t *testing.T) {
	ts := Tombstone{
		ID:      "t1",
		StartNs: 1000,
		EndNs:   2000,
	}

	// File range entirely before tombstone
	if ts.AffectsFile(100, 999) {
		t.Fatal("expected AffectsFile to return false for range before tombstone")
	}

	// File range entirely after tombstone
	if ts.AffectsFile(2001, 3000) {
		t.Fatal("expected AffectsFile to return false for range after tombstone")
	}
}

func TestTombstoneStore_AddAndActive(t *testing.T) {
	store := NewTombstoneStore()

	ts1 := Tombstone{ID: "t1", Query: "*", StartNs: 1000, EndNs: 2000, CreatedAt: time.Now()}
	ts2 := Tombstone{ID: "t2", Query: `level:="error"`, StartNs: 3000, EndNs: 4000, CreatedAt: time.Now()}

	store.Add(ts1)
	store.Add(ts2)

	if store.Count() != 2 {
		t.Fatalf("expected count 2, got %d", store.Count())
	}

	active := store.Active()
	if len(active) != 2 {
		t.Fatalf("expected 2 active tombstones, got %d", len(active))
	}
}

func TestTombstoneStore_Remove(t *testing.T) {
	store := NewTombstoneStore()

	ts1 := Tombstone{ID: "t1", Query: "*", StartNs: 1000, EndNs: 2000}
	ts2 := Tombstone{ID: "t2", Query: "*", StartNs: 3000, EndNs: 4000}

	store.Add(ts1)
	store.Add(ts2)

	store.Remove("t1")

	if store.Count() != 1 {
		t.Fatalf("expected count 1 after remove, got %d", store.Count())
	}

	_, ok := store.Get("t1")
	if ok {
		t.Fatal("expected t1 to be removed")
	}

	got, ok := store.Get("t2")
	if !ok {
		t.Fatal("expected t2 to still exist")
	}
	if got.ID != "t2" {
		t.Fatalf("expected ID t2, got %s", got.ID)
	}
}

func TestTombstoneStore_ForRange(t *testing.T) {
	store := NewTombstoneStore()

	store.Add(Tombstone{ID: "t1", StartNs: 1000, EndNs: 2000})
	store.Add(Tombstone{ID: "t2", StartNs: 3000, EndNs: 4000})
	store.Add(Tombstone{ID: "t3", StartNs: 5000, EndNs: 6000})

	// Query range that overlaps t1 and t2 only
	result := store.ForRange(1500, 3500)
	if len(result) != 2 {
		t.Fatalf("expected 2 tombstones for range [1500,3500], got %d", len(result))
	}

	ids := make(map[string]bool)
	for _, ts := range result {
		ids[ts.ID] = true
	}
	if !ids["t1"] || !ids["t2"] {
		t.Fatalf("expected t1 and t2 in result, got %v", ids)
	}

	// Query range that overlaps nothing
	result = store.ForRange(7000, 8000)
	if len(result) != 0 {
		t.Fatalf("expected 0 tombstones for range [7000,8000], got %d", len(result))
	}

	// Query range that overlaps all
	result = store.ForRange(0, 10000)
	if len(result) != 3 {
		t.Fatalf("expected 3 tombstones for range [0,10000], got %d", len(result))
	}
}

func TestPersistAndLoadFromDisk_RoundTrip(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{
		ID:      "t1",
		Query:   `level:="error"`,
		StartNs: 1000,
		EndNs:   2000,
		Mode:    "hide",
	})
	store.Add(Tombstone{
		ID:      "t2",
		Query:   "*",
		StartNs: 3000,
		EndNs:   4000,
		Mode:    "permanent",
	})

	dir := t.TempDir()

	if err := store.PersistToDisk(dir); err != nil {
		t.Fatalf("PersistToDisk: %v", err)
	}

	// Load into a fresh store
	store2 := NewTombstoneStore()
	if err := store2.LoadFromDisk(dir); err != nil {
		t.Fatalf("LoadFromDisk: %v", err)
	}

	if store2.Count() != 2 {
		t.Fatalf("expected 2 tombstones after load, got %d", store2.Count())
	}

	got, ok := store2.Get("t1")
	if !ok {
		t.Fatal("expected t1 to exist after load")
	}
	if got.Query != `level:="error"` {
		t.Fatalf("expected query level:=\"error\", got %s", got.Query)
	}
	if got.Mode != "hide" {
		t.Fatalf("expected mode hide, got %s", got.Mode)
	}

	got2, ok := store2.Get("t2")
	if !ok {
		t.Fatal("expected t2 to exist after load")
	}
	if got2.StartNs != 3000 || got2.EndNs != 4000 {
		t.Fatalf("unexpected time range for t2: [%d, %d]", got2.StartNs, got2.EndNs)
	}
}

func TestLoadFromDisk_NonExistentDir(t *testing.T) {
	store := NewTombstoneStore()

	// Non-existent directory should return nil (no-op)
	err := store.LoadFromDisk(filepath.Join(t.TempDir(), "nonexistent"))
	if err != nil {
		t.Fatalf("expected nil error for non-existent dir, got %v", err)
	}

	if store.Count() != 0 {
		t.Fatalf("expected empty store, got count %d", store.Count())
	}
}

func TestPersistToDisk_CreatesDir(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{ID: "t1", Query: "*", StartNs: 100, EndNs: 200})

	dir := filepath.Join(t.TempDir(), "sub", "deep")

	if err := store.PersistToDisk(dir); err != nil {
		t.Fatalf("PersistToDisk should create nested dir: %v", err)
	}

	// Verify file exists
	store2 := NewTombstoneStore()
	if err := store2.LoadFromDisk(dir); err != nil {
		t.Fatalf("LoadFromDisk after create: %v", err)
	}
	if store2.Count() != 1 {
		t.Fatalf("expected 1 tombstone, got %d", store2.Count())
	}
}

func TestPersistToDisk_AtomicWrite(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{ID: "t1", Query: "*", StartNs: 100, EndNs: 200})

	dir := t.TempDir()
	if err := store.PersistToDisk(dir); err != nil {
		t.Fatalf("PersistToDisk: %v", err)
	}

	// Verify the JSON file is valid
	data, err := json.Marshal(store.tombstones)
	if err != nil {
		t.Fatalf("manual marshal: %v", err)
	}

	var check map[string]Tombstone
	if err := json.Unmarshal(data, &check); err != nil {
		t.Fatalf("unmarshal written data: %v", err)
	}
	if _, ok := check["t1"]; !ok {
		t.Fatal("expected t1 in persisted data")
	}
}

func TestSyncAndLoadFromS3_RoundTrip(t *testing.T) {
	store := NewTombstoneStore()
	store.Add(Tombstone{
		ID:      "s1",
		Query:   `app:="web"`,
		StartNs: 5000,
		EndNs:   6000,
		Mode:    "auto",
	})
	store.Add(Tombstone{
		ID:      "s2",
		Query:   "timeout",
		StartNs: 7000,
		EndNs:   8000,
		Mode:    "hide",
	})

	pool := newMockS3Pool()
	ctx := context.Background()

	if err := store.SyncToS3(ctx, pool, "mybucket", "tenant-a"); err != nil {
		t.Fatalf("SyncToS3: %v", err)
	}

	// Verify keys were created
	keys, _ := pool.List(ctx, "tenant-a/_tombstones/")
	if len(keys) != 2 {
		t.Fatalf("expected 2 keys in S3, got %d", len(keys))
	}

	// Load into fresh store
	store2 := NewTombstoneStore()
	if err := store2.LoadFromS3(ctx, pool, "mybucket", "tenant-a"); err != nil {
		t.Fatalf("LoadFromS3: %v", err)
	}

	if store2.Count() != 2 {
		t.Fatalf("expected 2 tombstones after S3 load, got %d", store2.Count())
	}

	got, ok := store2.Get("s1")
	if !ok {
		t.Fatal("expected s1 after S3 load")
	}
	if got.Query != `app:="web"` {
		t.Fatalf("unexpected query: %s", got.Query)
	}
	if got.Mode != "auto" {
		t.Fatalf("unexpected mode: %s", got.Mode)
	}

	got2, ok := store2.Get("s2")
	if !ok {
		t.Fatal("expected s2 after S3 load")
	}
	if got2.StartNs != 7000 || got2.EndNs != 8000 {
		t.Fatalf("unexpected time range for s2: [%d, %d]", got2.StartNs, got2.EndNs)
	}
}

func TestLoadFromS3_EmptyPrefix(t *testing.T) {
	pool := newMockS3Pool()
	ctx := context.Background()

	store := NewTombstoneStore()
	// Add something first to verify it stays empty after load
	store.Add(Tombstone{ID: "existing", Query: "*", StartNs: 1, EndNs: 2})

	// LoadFromS3 with empty prefix (no keys) should return nil
	err := store.LoadFromS3(ctx, pool, "mybucket", "empty-tenant")
	if err != nil {
		t.Fatalf("expected nil error for empty prefix, got %v", err)
	}

	// Store should retain existing data since LoadFromS3 returns early on empty keys
	if store.Count() != 1 {
		t.Fatalf("expected store to retain existing data, got count %d", store.Count())
	}
}
