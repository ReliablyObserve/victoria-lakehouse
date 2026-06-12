package compaction

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

func fiLvl(level int, fp string) manifest.FileInfo {
	return manifest.FileInfo{CompactionLevel: level, SchemaFingerprint: fp}
}

// TestRecompactionLevel locks the hint the scheduler consumes to heal areas the
// level policy never re-picks: stale-schema files (re-promotion) and top-level
// fragmentation. Both must produce a level + true; clean/lone cases must not.
func TestRecompactionLevel(t *testing.T) {
	tests := []struct {
		name      string
		files     []manifest.FileInfo
		currentFP string
		wantLevel int
		wantNeeds bool
	}{
		{"stale files trigger at their max level", []manifest.FileInfo{fiLvl(2, "v1"), fiLvl(2, "v1")}, "v2", 2, true},
		{"fragmented top level (2+ at L2)", []manifest.FileInfo{fiLvl(2, "v2"), fiLvl(2, "v2")}, "v2", 2, true},
		{"clean single L2 file — no recompaction", []manifest.FileInfo{fiLvl(2, "v2")}, "v2", 0, false},
		{"clean L0/L1 — left to the level policy", []manifest.FileInfo{fiLvl(0, "v2"), fiLvl(1, "v2")}, "v2", 0, false},
		{"empty currentFP disables the stale check", []manifest.FileInfo{fiLvl(2, "v1")}, "", 0, false},
		{"stale wins the level over fragmentation", []manifest.FileInfo{fiLvl(3, "v1"), fiLvl(2, "v2")}, "v2", 3, true},
		{"no files — nothing to do", nil, "v2", 0, false},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			lvl, needs := recompactionLevel(tt.files, tt.currentFP)
			if needs != tt.wantNeeds || (needs && lvl != tt.wantLevel) {
				t.Errorf("recompactionLevel = (%d, %v), want (%d, %v)", lvl, needs, tt.wantLevel, tt.wantNeeds)
			}
		})
	}
}
