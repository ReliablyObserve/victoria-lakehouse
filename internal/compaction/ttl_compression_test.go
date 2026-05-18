package compaction

import (
	"testing"
	"time"
)

func TestCompressionLevelForAge_Hot(t *testing.T) {
	tiers := DefaultCompressionTiers()
	level := CompressionLevelForAge(1*time.Hour, tiers)
	if level != 3 {
		t.Errorf("hot data should use level 3, got %d", level)
	}
}

func TestCompressionLevelForAge_Warm(t *testing.T) {
	tiers := DefaultCompressionTiers()
	level := CompressionLevelForAge(10*24*time.Hour, tiers)
	if level != 7 {
		t.Errorf("warm data (10d) should use level 7, got %d", level)
	}
}

func TestCompressionLevelForAge_Cold(t *testing.T) {
	tiers := DefaultCompressionTiers()
	level := CompressionLevelForAge(60*24*time.Hour, tiers)
	if level != 17 {
		t.Errorf("cold data (60d) should use level 17, got %d", level)
	}
}

func TestCompressionLevelForAge_Boundaries(t *testing.T) {
	tiers := DefaultCompressionTiers()

	tests := []struct {
		age  time.Duration
		want int
	}{
		{0, 3},
		{6*24*time.Hour + 23*time.Hour, 3},
		{7 * 24 * time.Hour, 7},
		{29*24*time.Hour + 23*time.Hour, 7},
		{30 * 24 * time.Hour, 17},
		{365 * 24 * time.Hour, 17},
	}

	for _, tt := range tests {
		got := CompressionLevelForAge(tt.age, tiers)
		if got != tt.want {
			t.Errorf("age=%v: got level %d, want %d", tt.age, got, tt.want)
		}
	}
}

func TestCompressionLevelForAge_EmptyTiers(t *testing.T) {
	level := CompressionLevelForAge(time.Hour, nil)
	if level != 3 {
		t.Errorf("empty tiers should default to 3, got %d", level)
	}
}

func TestDefaultCompressionTiers(t *testing.T) {
	tiers := DefaultCompressionTiers()
	if len(tiers) != 3 {
		t.Fatalf("want 3 tiers, got %d", len(tiers))
	}
	if tiers[0].Level != 3 || tiers[1].Level != 7 || tiers[2].Level != 17 {
		t.Errorf("unexpected tier levels: %v", tiers)
	}
}
