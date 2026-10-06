package manifest

import (
	"context"
	"testing"
	"time"
)

func TestCompactionSafety_OlderListCannotThawNewerClass(t *testing.T) {
	const p = "dt=2026-10-03/hour=01"
	const key = "logs/" + p + "/a.parquet"
	older := time.Now().Add(-time.Minute)
	newer := time.Now()
	m := New("test-bucket", "logs/")
	newerFiles := map[string][]FileInfo{p: {{Key: key, Size: 100, StorageClass: "STANDARD_IA", ClassCheckedAt: newer, ClassSource: ClassSourceList}}}
	if !m.applyRefreshedFiles(context.Background(), newerFiles, newer, nil, nil) {
		t.Fatal("newer listing rejected")
	}
	// A LIST started earlier while the object was STANDARD finishes after a
	// newer LIST observed its transition to IA. Concurrent refresh calls have
	// no outer serialization, so applying in this order is possible.
	olderFiles := map[string][]FileInfo{p: {{Key: key, Size: 100, StorageClass: "STANDARD", ClassCheckedAt: older, ClassSource: ClassSourceList}}}
	if !m.applyRefreshedFiles(context.Background(), olderFiles, older, nil, nil) {
		t.Fatal("older listing rejected")
	}
	f := m.FilesForPartition(p)[0]
	if f.StorageClass != "STANDARD_IA" {
		t.Fatalf("older LIST thawed newer known tiered object: class=%s checked=%s", f.StorageClass, f.ClassCheckedAt)
	}
}
