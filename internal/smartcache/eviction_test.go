package smartcache

import (
	"testing"
	"time"
)

func TestIsHot(t *testing.T) {
	now := time.Now()

	tests := []struct {
		name      string
		meta      EntryMeta
		threshold int
		window    time.Duration
		want      bool
	}{
		{
			name: "cold entry below threshold",
			meta: EntryMeta{
				AccessCount:       1,
				AccessWindowStart: now.Add(-5 * time.Minute),
			},
			threshold: 3,
			window:    10 * time.Minute,
			want:      false,
		},
		{
			name: "hot entry above threshold within window",
			meta: EntryMeta{
				AccessCount:       5,
				AccessWindowStart: now.Add(-5 * time.Minute),
			},
			threshold: 3,
			window:    10 * time.Minute,
			want:      true,
		},
		{
			name: "stale window resets",
			meta: EntryMeta{
				AccessCount:       5,
				AccessWindowStart: now.Add(-15 * time.Minute),
			},
			threshold: 3,
			window:    10 * time.Minute,
			want:      false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := IsHot(tt.meta, tt.threshold, tt.window)
			if got != tt.want {
				t.Errorf("IsHot() = %v, want %v", got, tt.want)
			}
		})
	}
}

func TestEvictionPriority(t *testing.T) {
	now := time.Now()
	maxAge := 1 * time.Hour
	hotThreshold := 3
	hotWindow := 10 * time.Minute

	entries := map[string]EntryMeta{
		"expired_cold": {
			CreatedAt:         now.Add(-2 * time.Hour),
			LastAccess:        now.Add(-90 * time.Minute),
			AccessCount:       1,
			AccessWindowStart: now.Add(-90 * time.Minute),
			Size:              100,
		},
		"cold_recent": {
			CreatedAt:         now.Add(-30 * time.Minute),
			LastAccess:        now.Add(-20 * time.Minute),
			AccessCount:       1,
			AccessWindowStart: now.Add(-20 * time.Minute),
			Size:              200,
		},
		"hot_entry": {
			CreatedAt:         now.Add(-30 * time.Minute),
			LastAccess:        now.Add(-1 * time.Minute),
			AccessCount:       10,
			AccessWindowStart: now.Add(-5 * time.Minute),
			Size:              300,
		},
		"pinned_entry": {
			CreatedAt:  now.Add(-2 * time.Hour),
			LastAccess: now.Add(-2 * time.Hour),
			PinnedBy:   map[string]time.Time{"q1": now.Add(5 * time.Minute)},
			Size:       400,
		},
	}

	order := SortByEvictionPriority(entries, maxAge, hotThreshold, hotWindow)

	if len(order) != 4 {
		t.Fatalf("expected 4 entries, got %d", len(order))
	}

	if order[0] != "expired_cold" {
		t.Errorf("first eviction = %q, want %q", order[0], "expired_cold")
	}
	if order[3] != "pinned_entry" {
		t.Errorf("last eviction = %q, want %q", order[3], "pinned_entry")
	}
}

func TestCollectExpired(t *testing.T) {
	now := time.Now()
	maxAge := 1 * time.Hour
	hotThreshold := 3
	hotWindow := 10 * time.Minute

	m := NewMetadataMap()
	m.Set("expired", EntryMeta{
		CreatedAt:         now.Add(-2 * time.Hour),
		LastAccess:        now.Add(-2 * time.Hour),
		AccessCount:       0,
		AccessWindowStart: now.Add(-2 * time.Hour),
		Size:              100,
	})
	m.Set("fresh", EntryMeta{
		CreatedAt:         now.Add(-10 * time.Minute),
		LastAccess:        now.Add(-5 * time.Minute),
		AccessCount:       0,
		AccessWindowStart: now.Add(-5 * time.Minute),
		Size:              200,
	})
	m.Set("hot_expired_created_but_accessed_recently", EntryMeta{
		CreatedAt:         now.Add(-2 * time.Hour),
		LastAccess:        now.Add(-1 * time.Minute),
		AccessCount:       5,
		AccessWindowStart: now.Add(-5 * time.Minute),
		Size:              300,
	})
	m.Set("pinned_expired", EntryMeta{
		CreatedAt:  now.Add(-2 * time.Hour),
		LastAccess: now.Add(-2 * time.Hour),
		PinnedBy:   map[string]time.Time{"q1": now.Add(5 * time.Minute)},
		Size:       400,
	})

	expired := CollectExpired(m, maxAge, hotThreshold, hotWindow)

	if len(expired) != 1 {
		t.Fatalf("expected 1 expired entry, got %d: %v", len(expired), expired)
	}
	if expired[0] != "expired" {
		t.Errorf("expired entry = %q, want %q", expired[0], "expired")
	}
}

func TestCollectLRU(t *testing.T) {
	now := time.Now()
	hotThreshold := 3
	hotWindow := 10 * time.Minute

	m := NewMetadataMap()
	m.Set("oldest", EntryMeta{
		LastAccess:        now.Add(-1 * time.Hour),
		AccessCount:       1,
		AccessWindowStart: now.Add(-1 * time.Hour),
		Size:              100,
	})
	m.Set("middle", EntryMeta{
		LastAccess:        now.Add(-30 * time.Minute),
		AccessCount:       1,
		AccessWindowStart: now.Add(-30 * time.Minute),
		Size:              200,
	})
	m.Set("newest", EntryMeta{
		LastAccess:        now.Add(-1 * time.Minute),
		AccessCount:       1,
		AccessWindowStart: now.Add(-1 * time.Minute),
		Size:              300,
	})

	toEvict := CollectLRU(m, 250, hotThreshold, hotWindow)

	if len(toEvict) != 2 {
		t.Fatalf("expected 2 entries to evict, got %d: %v", len(toEvict), toEvict)
	}
}
