package delete

import (
	"testing"
	"time"
)

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
