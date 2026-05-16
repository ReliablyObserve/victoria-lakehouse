package retention

import (
	"context"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// mockManifest implements ManifestAccessor for testing.
type mockManifest struct {
	mu    sync.Mutex
	files map[string][]manifest.FileInfo
}

func newMockManifest(files map[string][]manifest.FileInfo) *mockManifest {
	return &mockManifest{files: files}
}

func (m *mockManifest) AllFiles() map[string][]manifest.FileInfo {
	m.mu.Lock()
	defer m.mu.Unlock()
	snap := make(map[string][]manifest.FileInfo, len(m.files))
	for k, v := range m.files {
		cp := make([]manifest.FileInfo, len(v))
		copy(cp, v)
		snap[k] = cp
	}
	return snap
}

func (m *mockManifest) RemoveFile(partition string, key string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	files := m.files[partition]
	for i, fi := range files {
		if fi.Key == key {
			m.files[partition] = append(files[:i], files[i+1:]...)
			if len(m.files[partition]) == 0 {
				delete(m.files, partition)
			}
			return
		}
	}
}

// mockDeleter implements FileDeleter for testing.
type mockDeleter struct {
	mu      sync.Mutex
	deleted []string
	err     error
}

func (d *mockDeleter) DeleteObject(_ context.Context, bucket, key string) error {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.err != nil {
		return d.err
	}
	d.deleted = append(d.deleted, bucket+"/"+key)
	return nil
}

func (d *mockDeleter) getDeleted() []string {
	d.mu.Lock()
	defer d.mu.Unlock()
	cp := make([]string, len(d.deleted))
	copy(cp, d.deleted)
	return cp
}

func testLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelDebug}))
}

// --- parseDuration tests ---

func TestParseDuration_Days(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"7d", 7 * 24 * time.Hour},
		{"90d", 90 * 24 * time.Hour},
		{"365d", 365 * 24 * time.Hour},
		{"1d", 24 * time.Hour},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			d, err := parseDuration(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d != tt.expected {
				t.Fatalf("got %v, want %v", d, tt.expected)
			}
		})
	}
}

func TestParseDuration_GoDuration(t *testing.T) {
	tests := []struct {
		input    string
		expected time.Duration
	}{
		{"168h", 168 * time.Hour},
		{"2160h", 2160 * time.Hour},
		{"30m", 30 * time.Minute},
		{"1h30m", 90 * time.Minute},
	}
	for _, tt := range tests {
		t.Run(tt.input, func(t *testing.T) {
			d, err := parseDuration(tt.input)
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			if d != tt.expected {
				t.Fatalf("got %v, want %v", d, tt.expected)
			}
		})
	}
}

func TestParseDuration_Invalid(t *testing.T) {
	tests := []string{
		"",
		"abc",
		"d",
		"0d",
		"-5d",
		"7x",
	}
	for _, input := range tests {
		t.Run(input, func(t *testing.T) {
			_, err := parseDuration(input)
			if err == nil {
				t.Fatalf("expected error for input %q", input)
			}
		})
	}
}

// --- ResolveTTL tests ---

func TestResolveTTL_NoRules_ReturnsDefault(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	fi := manifest.FileInfo{
		Key:       "test.parquet",
		MaxTimeNs: time.Now().Add(-30 * 24 * time.Hour).UnixNano(),
		Labels: map[string][]string{
			"service.name": {"my-service"},
		},
	}

	ttl := mgr.ResolveTTL(fi)
	expected := 90 * 24 * time.Hour
	if ttl != expected {
		t.Fatalf("got %v, want %v", ttl, expected)
	}
}

func TestResolveTTL_SingleMatch_ReturnsRuleKeep(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{
				Match: map[string]string{"service.name": "critical-app"},
				Keep:  "365d",
			},
		},
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	fi := manifest.FileInfo{
		Key:       "test.parquet",
		MaxTimeNs: time.Now().UnixNano(),
		Labels: map[string][]string{
			"service.name": {"critical-app"},
		},
	}

	ttl := mgr.ResolveTTL(fi)
	expected := 365 * 24 * time.Hour
	if ttl != expected {
		t.Fatalf("got %v, want %v", ttl, expected)
	}
}

func TestResolveTTL_MultipleMatches_LongestWins(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{
				Match: map[string]string{"env": "production"},
				Keep:  "30d",
			},
			{
				Match: map[string]string{"service.name": "critical-app"},
				Keep:  "365d",
			},
			{
				Match: map[string]string{"env": "production"},
				Keep:  "180d",
			},
		},
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	fi := manifest.FileInfo{
		Key:       "test.parquet",
		MaxTimeNs: time.Now().UnixNano(),
		Labels: map[string][]string{
			"service.name": {"critical-app"},
			"env":          {"production"},
		},
	}

	ttl := mgr.ResolveTTL(fi)
	expected := 365 * 24 * time.Hour
	if ttl != expected {
		t.Fatalf("got %v, want %v (longest should win)", ttl, expected)
	}
}

func TestResolveTTL_NoLabels_ReturnsDefault(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{
				Match: map[string]string{"service.name": "app"},
				Keep:  "365d",
			},
		},
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	fi := manifest.FileInfo{
		Key:       "test.parquet",
		MaxTimeNs: time.Now().UnixNano(),
	}

	ttl := mgr.ResolveTTL(fi)
	expected := 90 * 24 * time.Hour
	if ttl != expected {
		t.Fatalf("got %v, want %v (file with no labels should use default)", ttl, expected)
	}
}

// --- matchRule tests ---

func TestMatchRule_ExactMatch(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	rule := parsedRule{
		match: map[string]string{"service.name": "my-app"},
		keep:  7 * 24 * time.Hour,
	}

	fi := manifest.FileInfo{
		Labels: map[string][]string{
			"service.name": {"my-app", "other-app"},
		},
	}
	if !mgr.matchRule(fi, rule) {
		t.Fatal("expected match")
	}

	fiNoMatch := manifest.FileInfo{
		Labels: map[string][]string{
			"service.name": {"other-app"},
		},
	}
	if mgr.matchRule(fiNoMatch, rule) {
		t.Fatal("expected no match")
	}
}

func TestMatchRule_GlobMatch(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	rule := parsedRule{
		match: map[string]string{"service.name": "prod-*"},
		keep:  30 * 24 * time.Hour,
	}

	fi := manifest.FileInfo{
		Labels: map[string][]string{
			"service.name": {"prod-api", "prod-worker"},
		},
	}
	if !mgr.matchRule(fi, rule) {
		t.Fatal("expected glob match")
	}

	fiNoMatch := manifest.FileInfo{
		Labels: map[string][]string{
			"service.name": {"staging-api"},
		},
	}
	if mgr.matchRule(fiNoMatch, rule) {
		t.Fatal("expected no glob match")
	}
}

func TestMatchRule_GlobQuestionMark(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	rule := parsedRule{
		match: map[string]string{"env": "prod?"},
		keep:  30 * 24 * time.Hour,
	}

	fi := manifest.FileInfo{
		Labels: map[string][]string{
			"env": {"prods"},
		},
	}
	if !mgr.matchRule(fi, rule) {
		t.Fatal("expected ? glob match")
	}

	fiNoMatch := manifest.FileInfo{
		Labels: map[string][]string{
			"env": {"production"},
		},
	}
	if mgr.matchRule(fiNoMatch, rule) {
		t.Fatal("expected no ? glob match for 'production'")
	}
}

func TestMatchRule_MultiFieldAND(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	rule := parsedRule{
		match: map[string]string{
			"service.name": "my-app",
			"env":          "production",
		},
		keep: 30 * 24 * time.Hour,
	}

	// Both match
	fi := manifest.FileInfo{
		Labels: map[string][]string{
			"service.name": {"my-app"},
			"env":          {"production"},
		},
	}
	if !mgr.matchRule(fi, rule) {
		t.Fatal("expected match when both fields match")
	}

	// Only one matches
	fiPartial := manifest.FileInfo{
		Labels: map[string][]string{
			"service.name": {"my-app"},
			"env":          {"staging"},
		},
	}
	if mgr.matchRule(fiPartial, rule) {
		t.Fatal("expected no match when only one field matches")
	}
}

func TestMatchRule_MissingLabelField(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	rule := parsedRule{
		match: map[string]string{"service.name": "my-app"},
		keep:  30 * 24 * time.Hour,
	}

	// File has labels but not the required field
	fi := manifest.FileInfo{
		Labels: map[string][]string{
			"env": {"production"},
		},
	}
	if mgr.matchRule(fi, rule) {
		t.Fatal("expected no match when required label field is missing")
	}
}

func TestMatchRule_NilLabels(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}

	rule := parsedRule{
		match: map[string]string{"service.name": "my-app"},
		keep:  30 * 24 * time.Hour,
	}

	fi := manifest.FileInfo{}
	if mgr.matchRule(fi, rule) {
		t.Fatal("expected no match when labels are nil")
	}
}

// --- RunOnce tests ---

func TestRunOnce_DeletesExpiredFiles(t *testing.T) {
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	// File older than 90d default
	oldFile := manifest.FileInfo{
		Key:       "data/old.parquet",
		Size:      1024,
		MaxTimeNs: now.Add(-100 * 24 * time.Hour).UnixNano(),
		Labels: map[string][]string{
			"service.name": {"my-app"},
		},
	}
	// File within 90d default
	newFile := manifest.FileInfo{
		Key:       "data/new.parquet",
		Size:      2048,
		MaxTimeNs: now.Add(-30 * 24 * time.Hour).UnixNano(),
		Labels: map[string][]string{
			"service.name": {"my-app"},
		},
	}

	mf := newMockManifest(map[string][]manifest.FileInfo{
		"dt=2026-02-05/hour=10": {oldFile},
		"dt=2026-04-16/hour=10": {newFile},
	})
	deleter := &mockDeleter{}

	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, mf, deleter, "test-bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }

	deleted, err := mgr.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if deleted != 1 {
		t.Fatalf("expected 1 deleted, got %d", deleted)
	}

	deletedKeys := deleter.getDeleted()
	if len(deletedKeys) != 1 || deletedKeys[0] != "test-bucket/data/old.parquet" {
		t.Fatalf("unexpected deleted keys: %v", deletedKeys)
	}

	// Verify new file still in manifest
	remaining := mf.AllFiles()
	if _, exists := remaining["dt=2026-02-05/hour=10"]; exists {
		t.Fatal("old file partition should be removed from manifest")
	}
	if files, exists := remaining["dt=2026-04-16/hour=10"]; !exists || len(files) != 1 {
		t.Fatal("new file should still be in manifest")
	}
}

func TestRunOnce_FilesWithinTTL_NotDeleted(t *testing.T) {
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	recentFile := manifest.FileInfo{
		Key:       "data/recent.parquet",
		Size:      1024,
		MaxTimeNs: now.Add(-10 * 24 * time.Hour).UnixNano(),
		Labels: map[string][]string{
			"service.name": {"my-app"},
		},
	}

	mf := newMockManifest(map[string][]manifest.FileInfo{
		"dt=2026-05-06/hour=12": {recentFile},
	})
	deleter := &mockDeleter{}

	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, mf, deleter, "test-bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }

	deleted, err := mgr.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if deleted != 0 {
		t.Fatalf("expected 0 deleted, got %d", deleted)
	}
	if len(deleter.getDeleted()) != 0 {
		t.Fatal("no files should have been deleted")
	}
}

func TestRunOnce_RuleOrderDoesNotAffectResult(t *testing.T) {
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	// File is 100 days old. Rule 1 says keep 30d (expired), Rule 2 says keep 365d (not expired).
	// Longest wins, so file should NOT be deleted.
	file := manifest.FileInfo{
		Key:       "data/file.parquet",
		Size:      1024,
		MaxTimeNs: now.Add(-100 * 24 * time.Hour).UnixNano(),
		Labels: map[string][]string{
			"service.name": {"critical-app"},
			"env":          {"production"},
		},
	}

	// Test with short rule first
	cfgShortFirst := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{Match: map[string]string{"env": "production"}, Keep: "30d"},
			{Match: map[string]string{"service.name": "critical-app"}, Keep: "365d"},
		},
	}

	mf1 := newMockManifest(map[string][]manifest.FileInfo{
		"dt=2026-02-05/hour=10": {file},
	})
	deleter1 := &mockDeleter{}
	mgr1, err := New(cfgShortFirst, mf1, deleter1, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr1.nowFunc = func() time.Time { return now }

	deleted1, err := mgr1.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Test with long rule first
	cfgLongFirst := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{Match: map[string]string{"service.name": "critical-app"}, Keep: "365d"},
			{Match: map[string]string{"env": "production"}, Keep: "30d"},
		},
	}

	mf2 := newMockManifest(map[string][]manifest.FileInfo{
		"dt=2026-02-05/hour=10": {file},
	})
	deleter2 := &mockDeleter{}
	mgr2, err := New(cfgLongFirst, mf2, deleter2, "bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr2.nowFunc = func() time.Time { return now }

	deleted2, err := mgr2.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Both should give same result: 0 deleted (365d rule wins)
	if deleted1 != deleted2 {
		t.Fatalf("rule order affected result: short-first deleted %d, long-first deleted %d", deleted1, deleted2)
	}
	if deleted1 != 0 {
		t.Fatalf("expected 0 deleted (longest rule 365d > 100d age), got %d", deleted1)
	}
}

func TestRunOnce_FileWithMaxTimeZero_VeryOld(t *testing.T) {
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	// MaxTimeNs = 0 means time.Unix(0,0) = 1970-01-01, extremely old
	veryOldFile := manifest.FileInfo{
		Key:       "data/ancient.parquet",
		Size:      512,
		MaxTimeNs: 0,
		Labels: map[string][]string{
			"service.name": {"legacy"},
		},
	}

	mf := newMockManifest(map[string][]manifest.FileInfo{
		"dt=1970-01-01/hour=00": {veryOldFile},
	})
	deleter := &mockDeleter{}

	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, mf, deleter, "test-bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }

	deleted, err := mgr.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if deleted != 1 {
		t.Fatalf("expected 1 deleted for MaxTimeNs=0 file, got %d", deleted)
	}
}

func TestRunOnce_FileWithNoLabels_UsesDefault(t *testing.T) {
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	// File is 100 days old, no labels, default is 90d -> should be deleted
	noLabelFile := manifest.FileInfo{
		Key:       "data/nolabels.parquet",
		Size:      1024,
		MaxTimeNs: now.Add(-100 * 24 * time.Hour).UnixNano(),
	}

	mf := newMockManifest(map[string][]manifest.FileInfo{
		"dt=2026-02-05/hour=10": {noLabelFile},
	})
	deleter := &mockDeleter{}

	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{Match: map[string]string{"service.name": "keep-me"}, Keep: "365d"},
		},
	}
	mgr, err := New(cfg, mf, deleter, "test-bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }

	deleted, err := mgr.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	if deleted != 1 {
		t.Fatalf("expected 1 deleted for file with no labels past default TTL, got %d", deleted)
	}
}

func TestRunOnce_DeleteError_ContinuesProcessing(t *testing.T) {
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	file1 := manifest.FileInfo{
		Key:       "data/file1.parquet",
		Size:      1024,
		MaxTimeNs: now.Add(-100 * 24 * time.Hour).UnixNano(),
		Labels:    map[string][]string{"env": {"prod"}},
	}
	file2 := manifest.FileInfo{
		Key:       "data/file2.parquet",
		Size:      2048,
		MaxTimeNs: now.Add(-100 * 24 * time.Hour).UnixNano(),
		Labels:    map[string][]string{"env": {"prod"}},
	}

	mf := newMockManifest(map[string][]manifest.FileInfo{
		"dt=2026-02-05/hour=10": {file1, file2},
	})
	deleter := &mockDeleter{err: fmt.Errorf("s3 error")}

	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, mf, deleter, "test-bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }

	deleted, err := mgr.RunOnce(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	// Should have 0 successful deletes due to error
	if deleted != 0 {
		t.Fatalf("expected 0 deleted due to errors, got %d", deleted)
	}

	// Both files should still be in manifest
	remaining := mf.AllFiles()
	if files, exists := remaining["dt=2026-02-05/hour=10"]; !exists || len(files) != 2 {
		t.Fatal("files should remain in manifest when delete fails")
	}
}

func TestRunOnce_ContextCancellation(t *testing.T) {
	now := time.Date(2026, 5, 16, 12, 0, 0, 0, time.UTC)
	files := make(map[string][]manifest.FileInfo)
	for i := 0; i < 100; i++ {
		key := fmt.Sprintf("dt=2026-01-%02d/hour=10", (i%28)+1)
		files[key] = append(files[key], manifest.FileInfo{
			Key:       fmt.Sprintf("data/file%d.parquet", i),
			Size:      1024,
			MaxTimeNs: now.Add(-100 * 24 * time.Hour).UnixNano(),
			Labels:    map[string][]string{"env": {"prod"}},
		})
	}

	mf := newMockManifest(files)
	deleter := &mockDeleter{}

	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
	}
	mgr, err := New(cfg, mf, deleter, "test-bucket", testLogger())
	if err != nil {
		t.Fatal(err)
	}
	mgr.nowFunc = func() time.Time { return now }

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // cancel immediately

	_, err = mgr.RunOnce(ctx)
	if err != context.Canceled {
		t.Fatalf("expected context.Canceled, got %v", err)
	}
}

// --- New validation tests ---

func TestNew_InvalidDefault(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "invalid",
		CheckInterval: "1h",
	}
	_, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err == nil {
		t.Fatal("expected error for invalid default duration")
	}
}

func TestNew_InvalidCheckInterval(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "bad",
	}
	_, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err == nil {
		t.Fatal("expected error for invalid check_interval")
	}
}

func TestNew_InvalidRuleKeep(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{Match: map[string]string{"env": "prod"}, Keep: "invalid"},
		},
	}
	_, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err == nil {
		t.Fatal("expected error for invalid rule keep duration")
	}
}

func TestNew_EmptyRuleMatch(t *testing.T) {
	cfg := Config{
		Enabled:       true,
		Default:       "90d",
		CheckInterval: "1h",
		Rules: []Rule{
			{Match: map[string]string{}, Keep: "7d"},
		},
	}
	_, err := New(cfg, newMockManifest(nil), &mockDeleter{}, "bucket", testLogger())
	if err == nil {
		t.Fatal("expected error for empty rule match")
	}
}
