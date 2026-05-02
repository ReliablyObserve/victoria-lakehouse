package config

import (
	"os"
	"path/filepath"
	"testing"
	"time"
)

func TestDefaultConfig(t *testing.T) {
	cfg := Default()

	if cfg.Topology != TopologyAuto {
		t.Errorf("default topology = %q, want %q", cfg.Topology, TopologyAuto)
	}
	if cfg.S3.Region != "us-east-1" {
		t.Errorf("default S3 region = %q, want %q", cfg.S3.Region, "us-east-1")
	}
	if cfg.S3.MaxConnections != 128 {
		t.Errorf("default S3 max connections = %d, want 128", cfg.S3.MaxConnections)
	}
	if cfg.S3.Timeout != 30*time.Second {
		t.Errorf("default S3 timeout = %v, want 30s", cfg.S3.Timeout)
	}
	if cfg.S3.RetryMax != 3 {
		t.Errorf("default S3 retry max = %d, want 3", cfg.S3.RetryMax)
	}
	if cfg.S3.RetryBaseDelay != 200*time.Millisecond {
		t.Errorf("default S3 retry base delay = %v, want 200ms", cfg.S3.RetryBaseDelay)
	}
	if cfg.Cache.MemoryLimit != "512MB" {
		t.Errorf("default cache memory limit = %q, want 512MB", cfg.Cache.MemoryLimit)
	}
	if cfg.Cache.DiskLimit != "50GB" {
		t.Errorf("default cache disk limit = %q, want 50GB", cfg.Cache.DiskLimit)
	}
	if cfg.Cache.EvictionWatermark != 0.8 {
		t.Errorf("default eviction watermark = %f, want 0.8", cfg.Cache.EvictionWatermark)
	}
	if cfg.Cache.FooterTTL != 1*time.Hour {
		t.Errorf("default footer TTL = %v, want 1h", cfg.Cache.FooterTTL)
	}
	if cfg.Cache.BloomTTL != 1*time.Hour {
		t.Errorf("default bloom TTL = %v, want 1h", cfg.Cache.BloomTTL)
	}
	if cfg.Cache.PageTTL != 10*time.Minute {
		t.Errorf("default page TTL = %v, want 10m", cfg.Cache.PageTTL)
	}
	if cfg.Discovery.RefreshInterval != 5*time.Minute {
		t.Errorf("default discovery refresh = %v, want 5m", cfg.Discovery.RefreshInterval)
	}
	if cfg.Discovery.Timeout != 10*time.Second {
		t.Errorf("default discovery timeout = %v, want 10s", cfg.Discovery.Timeout)
	}
	if cfg.Discovery.PeerRefreshInterval != 30*time.Second {
		t.Errorf("default peer refresh = %v, want 30s", cfg.Discovery.PeerRefreshInterval)
	}
	if cfg.Manifest.RefreshInterval != 5*time.Minute {
		t.Errorf("default manifest refresh = %v, want 5m", cfg.Manifest.RefreshInterval)
	}
	if cfg.Manifest.SQSWaitTime != 20*time.Second {
		t.Errorf("default SQS wait = %v, want 20s", cfg.Manifest.SQSWaitTime)
	}
	if cfg.Manifest.PersistInterval != 5*time.Minute {
		t.Errorf("default persist interval = %v, want 5m", cfg.Manifest.PersistInterval)
	}
	if !cfg.Prefetch.Correlated {
		t.Error("default correlated prefetch should be true")
	}
	if cfg.Prefetch.ReadAheadDepth != 2 {
		t.Errorf("default read ahead = %d, want 2", cfg.Prefetch.ReadAheadDepth)
	}
	if cfg.Prefetch.MaxConcurrent != 4 {
		t.Errorf("default prefetch concurrent = %d, want 4", cfg.Prefetch.MaxConcurrent)
	}
	if cfg.Peer.Timeout != 5*time.Second {
		t.Errorf("default peer timeout = %v, want 5s", cfg.Peer.Timeout)
	}
	if cfg.Peer.MaxConnections != 32 {
		t.Errorf("default peer max connections = %d, want 32", cfg.Peer.MaxConnections)
	}
	if cfg.Startup.ServeStale {
		t.Error("default serve stale should be false")
	}
	if cfg.Startup.WarmupWindow != 24*time.Hour {
		t.Errorf("default warmup window = %v, want 24h", cfg.Startup.WarmupWindow)
	}
	if cfg.Startup.MaxWarmupTime != 5*time.Minute {
		t.Errorf("default max warmup = %v, want 5m", cfg.Startup.MaxWarmupTime)
	}
	if cfg.Query.MaxConcurrent != 32 {
		t.Errorf("default query concurrent = %d, want 32", cfg.Query.MaxConcurrent)
	}
	if cfg.Query.Timeout != 60*time.Second {
		t.Errorf("default query timeout = %v, want 60s", cfg.Query.Timeout)
	}
	if cfg.Query.MaxRows != 10_000_000 {
		t.Errorf("default max rows = %d, want 10000000", cfg.Query.MaxRows)
	}
	if cfg.Query.SlowThreshold != 5*time.Second {
		t.Errorf("default slow threshold = %v, want 5s", cfg.Query.SlowThreshold)
	}
	if cfg.CircuitBreaker.Threshold != 5 {
		t.Errorf("default CB threshold = %d, want 5", cfg.CircuitBreaker.Threshold)
	}
	if cfg.CircuitBreaker.Timeout != 30*time.Second {
		t.Errorf("default CB timeout = %v, want 30s", cfg.CircuitBreaker.Timeout)
	}
	if cfg.CircuitBreaker.SuccessThreshold != 2 {
		t.Errorf("default CB success threshold = %d, want 2", cfg.CircuitBreaker.SuccessThreshold)
	}
	if cfg.Tenant.PrefixTemplate != "{AccountID}/{ProjectID}/" {
		t.Errorf("default tenant prefix template = %q", cfg.Tenant.PrefixTemplate)
	}
}

func TestValidate_MissingMode(t *testing.T) {
	cfg := Default()
	cfg.S3.Bucket = "test"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing mode")
	}
}

func TestValidate_InvalidMode(t *testing.T) {
	cfg := Default()
	cfg.Mode = "invalid"
	cfg.S3.Bucket = "test"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid mode")
	}
}

func TestValidate_MissingBucket(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for missing bucket")
	}
}

func TestValidate_Valid(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test-bucket"
	if err := cfg.Validate(); err != nil {
		t.Errorf("unexpected error: %v", err)
	}
}

func TestValidate_InvalidTopology(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Topology = "bogus"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid topology")
	}
}

func TestValidate_InvalidEvictionWatermark(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Cache.EvictionWatermark = 1.5
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid eviction watermark")
	}
}

func TestListenAddr(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	if addr := cfg.ListenAddr(); addr != ":9428" {
		t.Errorf("logs listen addr = %q, want :9428", addr)
	}
	cfg.Mode = ModeTraces
	if addr := cfg.ListenAddr(); addr != ":10428" {
		t.Errorf("traces listen addr = %q, want :10428", addr)
	}
}

func TestAutoPrefix(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	if p := cfg.AutoPrefix(); p != "logs/" {
		t.Errorf("logs auto prefix = %q, want logs/", p)
	}
	cfg.Mode = ModeTraces
	if p := cfg.AutoPrefix(); p != "traces/" {
		t.Errorf("traces auto prefix = %q, want traces/", p)
	}
	cfg.S3.Prefix = "custom/"
	if p := cfg.AutoPrefix(); p != "custom/" {
		t.Errorf("custom prefix = %q, want custom/", p)
	}
}

func TestLoad_YAML(t *testing.T) {
	content := `
lakehouse:
  mode: traces
  s3:
    bucket: my-archive
    region: eu-west-1
    max_connections: 64
    timeout: 15s
  cache:
    memory_limit: 1GB
    disk_limit: 100GB
  query:
    max_concurrent: 16
    timeout: 30s
    slow_threshold: 3s
  hot_boundary: 14d
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}

	if cfg.Mode != ModeTraces {
		t.Errorf("mode = %q, want traces", cfg.Mode)
	}
	if cfg.S3.Bucket != "my-archive" {
		t.Errorf("bucket = %q, want my-archive", cfg.S3.Bucket)
	}
	if cfg.S3.Region != "eu-west-1" {
		t.Errorf("region = %q, want eu-west-1", cfg.S3.Region)
	}
	if cfg.S3.MaxConnections != 64 {
		t.Errorf("max connections = %d, want 64", cfg.S3.MaxConnections)
	}
	if cfg.S3.Timeout != 15*time.Second {
		t.Errorf("S3 timeout = %v, want 15s", cfg.S3.Timeout)
	}
	if cfg.Cache.MemoryLimit != "1GB" {
		t.Errorf("memory limit = %q, want 1GB", cfg.Cache.MemoryLimit)
	}
	if cfg.Cache.DiskLimit != "100GB" {
		t.Errorf("disk limit = %q, want 100GB", cfg.Cache.DiskLimit)
	}
	if cfg.Query.MaxConcurrent != 16 {
		t.Errorf("max concurrent = %d, want 16", cfg.Query.MaxConcurrent)
	}
	if cfg.Query.Timeout != 30*time.Second {
		t.Errorf("query timeout = %v, want 30s", cfg.Query.Timeout)
	}
	if cfg.Query.SlowThreshold != 3*time.Second {
		t.Errorf("slow threshold = %v, want 3s", cfg.Query.SlowThreshold)
	}
	if cfg.HotBoundary != "14d" {
		t.Errorf("hot boundary = %q, want 14d", cfg.HotBoundary)
	}

	// Verify defaults preserved for unset fields
	if cfg.S3.RetryMax != 3 {
		t.Errorf("retry max = %d, want default 3", cfg.S3.RetryMax)
	}
	if cfg.Peer.Timeout != 5*time.Second {
		t.Errorf("peer timeout = %v, want default 5s", cfg.Peer.Timeout)
	}
}

func TestLoad_EmptyPath(t *testing.T) {
	cfg, err := Load("")
	if err != nil {
		t.Fatalf("Load empty: %v", err)
	}
	if cfg.S3.Region != "us-east-1" {
		t.Error("empty path should return defaults")
	}
}

func TestLoad_InvalidFile(t *testing.T) {
	_, err := Load("/nonexistent/path/config.yaml")
	if err == nil {
		t.Error("expected error for nonexistent file")
	}
}

func TestLoad_InvalidYAML(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "bad.yaml")
	if err := os.WriteFile(path, []byte("{{invalid yaml"), 0644); err != nil {
		t.Fatal(err)
	}
	_, err := Load(path)
	if err == nil {
		t.Error("expected error for invalid YAML")
	}
}
