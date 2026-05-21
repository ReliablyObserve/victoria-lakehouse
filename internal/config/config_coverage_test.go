package config

import (
	"testing"
	"time"
)

func TestDefaultPort(t *testing.T) {
	cfg := Default()

	cfg.Mode = ModeLogs
	if p := cfg.DefaultPort(); p != "9428" {
		t.Errorf("logs DefaultPort = %q, want 9428", p)
	}

	cfg.Mode = ModeTraces
	if p := cfg.DefaultPort(); p != "10428" {
		t.Errorf("traces DefaultPort = %q, want 10428", p)
	}
}

func TestActiveDeletePrefix_LogsMode_DefaultFallback(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Logs.DeletePrefix = "" // clear it to trigger the fallback path

	p := cfg.ActiveDeletePrefix()
	if p != "/delete/logsql" {
		t.Errorf("ActiveDeletePrefix logs fallback = %q, want /delete/logsql", p)
	}
}

func TestActiveDeletePrefix_TracesMode_DefaultFallback(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeTraces
	cfg.Traces.DeletePrefix = "" // clear it to trigger the fallback path

	p := cfg.ActiveDeletePrefix()
	if p != "/delete/tracessql" {
		t.Errorf("ActiveDeletePrefix traces fallback = %q, want /delete/tracessql", p)
	}
}

func TestActiveDeletePrefix_LogsMode_CustomPrefix(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Logs.DeletePrefix = "/custom/logs"

	p := cfg.ActiveDeletePrefix()
	if p != "/custom/logs" {
		t.Errorf("ActiveDeletePrefix custom logs = %q, want /custom/logs", p)
	}
}

func TestTargetFileSizeN_Default(t *testing.T) {
	ic := &InsertConfig{TargetFileSize: "128MB"}
	got := ic.TargetFileSizeN()
	want := int64(128 * 1024 * 1024)
	if got != want {
		t.Errorf("TargetFileSizeN default = %d, want %d", got, want)
	}
}

func TestTargetFileSizeN_Invalid(t *testing.T) {
	ic := &InsertConfig{TargetFileSize: "invalid"}
	got := ic.TargetFileSizeN()
	want := int64(128 * 1024 * 1024)
	if got != want {
		t.Errorf("TargetFileSizeN invalid = %d, want default %d", got, want)
	}
}

func TestTargetFileSizeN_Empty(t *testing.T) {
	ic := &InsertConfig{TargetFileSize: ""}
	got := ic.TargetFileSizeN()
	want := int64(128 * 1024 * 1024)
	if got != want {
		t.Errorf("TargetFileSizeN empty = %d, want default %d", got, want)
	}
}

func TestWALMaxBytesN_Invalid(t *testing.T) {
	ic := &InsertConfig{WALMaxBytes: "invalid"}
	got := ic.WALMaxBytesN()
	want := int64(512 * 1024 * 1024)
	if got != want {
		t.Errorf("WALMaxBytesN invalid = %d, want default %d", got, want)
	}
}

func TestWALMaxBytesN_Empty(t *testing.T) {
	ic := &InsertConfig{WALMaxBytes: ""}
	got := ic.WALMaxBytesN()
	want := int64(512 * 1024 * 1024)
	if got != want {
		t.Errorf("WALMaxBytesN empty = %d, want default %d", got, want)
	}
}

func TestCacheDiskBytes_Invalid(t *testing.T) {
	cfg := Default()
	cfg.Cache.DiskLimit = "invalid"
	got := cfg.CacheDiskBytes()
	want := int64(50 * 1024 * 1024 * 1024)
	if got != want {
		t.Errorf("CacheDiskBytes invalid = %d, want default %d", got, want)
	}
}

func TestValidate_LeaderElectionModes(t *testing.T) {
	modes := []string{"auto", "k8s", "s3", "none", ""}
	for _, mode := range modes {
		cfg := Default()
		cfg.Mode = ModeLogs
		cfg.S3.Bucket = "test"
		cfg.Compaction.LeaderElection = mode
		if mode == "none" {
			cfg.Compaction.Enabled = false
		}
		if err := cfg.Validate(); err != nil {
			t.Errorf("Validate() with leader election %q: %v", mode, err)
		}
	}
}

func TestValidate_InvalidLeaderElection(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Compaction.LeaderElection = "invalid-mode"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid leader election mode")
	}
}

func TestValidate_CompactionEnabled_InvalidInterval(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Compaction.Enabled = true
	cfg.Compaction.Interval = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for zero compaction interval")
	}
}

func TestValidate_CompactionEnabled_InvalidMaxConcurrent(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Compaction.Enabled = true
	cfg.Compaction.MaxConcurrent = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for zero compaction max concurrent")
	}
}

func TestValidate_CompactionEnabled_InvalidMinFilesL0(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Compaction.Enabled = true
	cfg.Compaction.MinFilesL0 = 1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for compaction MinFilesL0 < 2")
	}
}

func TestValidate_CompactionEnabled_InvalidMinFilesL1(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Compaction.Enabled = true
	cfg.Compaction.MinFilesL1 = 1
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for compaction MinFilesL1 < 2")
	}
}

func TestValidate_CompactionEnabled_Valid(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Compaction.Enabled = true
	cfg.Compaction.Interval = 5 * time.Minute
	cfg.Compaction.MaxConcurrent = 2
	cfg.Compaction.MinFilesL0 = 5
	cfg.Compaction.MinFilesL1 = 5
	if err := cfg.Validate(); err != nil {
		t.Errorf("unexpected error for valid compaction config: %v", err)
	}
}

func TestMergeConfig_WALMaxBytes(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Insert.WALMaxBytes = "1GB"

	result := mergeConfig(base, overlay)
	if result.Insert.WALMaxBytes != "1GB" {
		t.Errorf("WALMaxBytes = %q, want 1GB", result.Insert.WALMaxBytes)
	}
}

func TestMergeConfig_DeleteFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Delete.Enabled = true
	overlay.Delete.DefaultMode = "rewrite"
	overlay.Delete.AutoRewriteClasses = []string{"STANDARD", "GLACIER"}
	overlay.Delete.RewriteDelay = 2 * time.Hour
	overlay.Delete.RewriteBatchSize = 100
	overlay.Delete.RewriteMaxConcurrent = 4
	overlay.Delete.PersistPath = "/data/tombstones"
	overlay.Delete.CostWarningThreshold = 20.0
	overlay.Delete.ForceGlacierHeader = "X-Custom-Header"
	overlay.Delete.VerifyInterval = 12 * time.Hour
	overlay.Delete.LifecycleRules = []LifecycleRuleConfig{
		{TransitionDays: 30, StorageClass: "GLACIER"},
	}

	result := mergeConfig(base, overlay)

	if !result.Delete.Enabled {
		t.Error("Delete.Enabled should be true")
	}
	if result.Delete.DefaultMode != "rewrite" {
		t.Errorf("Delete.DefaultMode = %q, want rewrite", result.Delete.DefaultMode)
	}
	if len(result.Delete.AutoRewriteClasses) != 2 {
		t.Errorf("Delete.AutoRewriteClasses len = %d, want 2", len(result.Delete.AutoRewriteClasses))
	}
	if result.Delete.RewriteDelay != 2*time.Hour {
		t.Errorf("Delete.RewriteDelay = %v, want 2h", result.Delete.RewriteDelay)
	}
	if result.Delete.RewriteBatchSize != 100 {
		t.Errorf("Delete.RewriteBatchSize = %d, want 100", result.Delete.RewriteBatchSize)
	}
	if result.Delete.RewriteMaxConcurrent != 4 {
		t.Errorf("Delete.RewriteMaxConcurrent = %d, want 4", result.Delete.RewriteMaxConcurrent)
	}
	if result.Delete.PersistPath != "/data/tombstones" {
		t.Errorf("Delete.PersistPath = %q", result.Delete.PersistPath)
	}
	if result.Delete.CostWarningThreshold != 20.0 {
		t.Errorf("Delete.CostWarningThreshold = %f, want 20.0", result.Delete.CostWarningThreshold)
	}
	if result.Delete.ForceGlacierHeader != "X-Custom-Header" {
		t.Errorf("Delete.ForceGlacierHeader = %q", result.Delete.ForceGlacierHeader)
	}
	if result.Delete.VerifyInterval != 12*time.Hour {
		t.Errorf("Delete.VerifyInterval = %v, want 12h", result.Delete.VerifyInterval)
	}
	if len(result.Delete.LifecycleRules) != 1 {
		t.Fatalf("Delete.LifecycleRules len = %d, want 1", len(result.Delete.LifecycleRules))
	}
	if result.Delete.LifecycleRules[0].TransitionDays != 30 {
		t.Errorf("LifecycleRules[0].TransitionDays = %d, want 30", result.Delete.LifecycleRules[0].TransitionDays)
	}
}

func TestMergeConfig_CompactionFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Compaction.Enabled = true
	overlay.Compaction.Interval = 10 * time.Minute
	overlay.Compaction.MaxConcurrent = 4
	overlay.Compaction.MinFilesL0 = 20
	overlay.Compaction.MinFilesL1 = 15
	overlay.Compaction.MinAge = 2 * time.Hour
	overlay.Compaction.LeaderElection = "s3"
	overlay.Compaction.LeaseDuration = 30 * time.Second
	overlay.Compaction.S3LockTTL = 120 * time.Second
	overlay.Compaction.S3Heartbeat = 30 * time.Second

	result := mergeConfig(base, overlay)

	if !result.Compaction.Enabled {
		t.Error("Compaction.Enabled should be true")
	}
	if result.Compaction.Interval != 10*time.Minute {
		t.Errorf("Compaction.Interval = %v", result.Compaction.Interval)
	}
	if result.Compaction.MaxConcurrent != 4 {
		t.Errorf("Compaction.MaxConcurrent = %d", result.Compaction.MaxConcurrent)
	}
	if result.Compaction.MinFilesL0 != 20 {
		t.Errorf("Compaction.MinFilesL0 = %d", result.Compaction.MinFilesL0)
	}
	if result.Compaction.MinFilesL1 != 15 {
		t.Errorf("Compaction.MinFilesL1 = %d", result.Compaction.MinFilesL1)
	}
	if result.Compaction.MinAge != 2*time.Hour {
		t.Errorf("Compaction.MinAge = %v", result.Compaction.MinAge)
	}
	if result.Compaction.LeaderElection != "s3" {
		t.Errorf("Compaction.LeaderElection = %q", result.Compaction.LeaderElection)
	}
	if result.Compaction.LeaseDuration != 30*time.Second {
		t.Errorf("Compaction.LeaseDuration = %v", result.Compaction.LeaseDuration)
	}
	if result.Compaction.S3LockTTL != 120*time.Second {
		t.Errorf("Compaction.S3LockTTL = %v", result.Compaction.S3LockTTL)
	}
	if result.Compaction.S3Heartbeat != 30*time.Second {
		t.Errorf("Compaction.S3Heartbeat = %v", result.Compaction.S3Heartbeat)
	}
}

func TestMergeConfig_CrossSignalFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.CrossSignal.Enabled = true
	overlay.CrossSignal.Endpoint = "http://traces:10428"
	overlay.CrossSignal.HeadlessService = "lakehouse-headless"
	overlay.CrossSignal.AuthKey = "secret"
	overlay.CrossSignal.Timeout = 5 * time.Second
	overlay.CrossSignal.MaxBatch = 200
	overlay.CrossSignal.BatchInterval = 1 * time.Second

	result := mergeConfig(base, overlay)

	if !result.CrossSignal.Enabled {
		t.Error("CrossSignal.Enabled should be true")
	}
	if result.CrossSignal.Endpoint != "http://traces:10428" {
		t.Errorf("CrossSignal.Endpoint = %q", result.CrossSignal.Endpoint)
	}
	if result.CrossSignal.HeadlessService != "lakehouse-headless" {
		t.Errorf("CrossSignal.HeadlessService = %q", result.CrossSignal.HeadlessService)
	}
	if result.CrossSignal.AuthKey != "secret" {
		t.Errorf("CrossSignal.AuthKey = %q", result.CrossSignal.AuthKey)
	}
	if result.CrossSignal.Timeout != 5*time.Second {
		t.Errorf("CrossSignal.Timeout = %v", result.CrossSignal.Timeout)
	}
	if result.CrossSignal.MaxBatch != 200 {
		t.Errorf("CrossSignal.MaxBatch = %d", result.CrossSignal.MaxBatch)
	}
	if result.CrossSignal.BatchInterval != 1*time.Second {
		t.Errorf("CrossSignal.BatchInterval = %v", result.CrossSignal.BatchInterval)
	}
}

func TestMergeConfig_SmartCacheAllFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.SmartCache.SnapshotInterval = 30 * time.Second
	overlay.SmartCache.QueryGracePeriod = 10 * time.Minute
	overlay.SmartCache.HotWindow = 20 * time.Minute
	overlay.SmartCache.TargetHours = 48
	overlay.SmartCache.DiskLimitMax = "200GB"
	overlay.SmartCache.IngestionRateHint = "100MB"

	result := mergeConfig(base, overlay)

	if result.SmartCache.SnapshotInterval != 30*time.Second {
		t.Errorf("SmartCache.SnapshotInterval = %v", result.SmartCache.SnapshotInterval)
	}
	if result.SmartCache.QueryGracePeriod != 10*time.Minute {
		t.Errorf("SmartCache.QueryGracePeriod = %v", result.SmartCache.QueryGracePeriod)
	}
	if result.SmartCache.HotWindow != 20*time.Minute {
		t.Errorf("SmartCache.HotWindow = %v", result.SmartCache.HotWindow)
	}
	if result.SmartCache.TargetHours != 48 {
		t.Errorf("SmartCache.TargetHours = %d", result.SmartCache.TargetHours)
	}
	if result.SmartCache.DiskLimitMax != "200GB" {
		t.Errorf("SmartCache.DiskLimitMax = %q", result.SmartCache.DiskLimitMax)
	}
	if result.SmartCache.IngestionRateHint != "100MB" {
		t.Errorf("SmartCache.IngestionRateHint = %q", result.SmartCache.IngestionRateHint)
	}
}

func TestMergeConfig_LogsCompatVersion(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Logs.CompatVersion = "1.50.0"
	overlay.Traces.CompatVersion = "0.8.0"
	overlay.Traces.DeletePrefix = "/custom/trace-del"

	result := mergeConfig(base, overlay)

	if result.Logs.CompatVersion != "1.50.0" {
		t.Errorf("Logs.CompatVersion = %q", result.Logs.CompatVersion)
	}
	if result.Traces.CompatVersion != "0.8.0" {
		t.Errorf("Traces.CompatVersion = %q", result.Traces.CompatVersion)
	}
	if result.Traces.DeletePrefix != "/custom/trace-del" {
		t.Errorf("Traces.DeletePrefix = %q", result.Traces.DeletePrefix)
	}
}

func TestMergeConfig_SelectBufferQueryEnabled(t *testing.T) {
	base := Default()
	base.Select.BufferQueryEnabled = false
	overlay := &Config{}
	overlay.Select.BufferQueryEnabled = true

	result := mergeConfig(base, overlay)
	if !result.Select.BufferQueryEnabled {
		t.Error("Select.BufferQueryEnabled should be true")
	}
}

// TestMergeConfigs_ExportedWrapper exercises the exported MergeConfigs function
// (previously 0% coverage).
func TestMergeConfigs_ExportedWrapper(t *testing.T) {
	base := Default()
	base.Mode = ModeLogs
	base.S3.Bucket = "base-bucket"

	overlay := &Config{}
	overlay.S3.Bucket = "overlay-bucket"

	result := MergeConfigs(base, overlay)
	if result == nil {
		t.Fatal("MergeConfigs returned nil")
	}
	if result.S3.Bucket != "overlay-bucket" {
		t.Errorf("S3.Bucket = %q, want overlay-bucket", result.S3.Bucket)
	}
	// Mode from base should be preserved when overlay is empty.
	if result.Mode != ModeLogs {
		t.Errorf("Mode = %q, want logs", result.Mode)
	}
}
