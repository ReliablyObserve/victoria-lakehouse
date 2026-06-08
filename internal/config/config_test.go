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
	if cfg.Prefetch.MaxConcurrent != 8 {
		t.Errorf("default prefetch concurrent = %d, want 8", cfg.Prefetch.MaxConcurrent)
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
	if p := cfg.AutoPrefix(); p != "0/0/logs/" {
		t.Errorf("logs auto prefix = %q, want 0/0/logs/", p)
	}
	cfg.Mode = ModeTraces
	if p := cfg.AutoPrefix(); p != "0/0/traces/" {
		t.Errorf("traces auto prefix = %q, want 0/0/traces/", p)
	}
	cfg.S3.Prefix = "custom/"
	if p := cfg.AutoPrefix(); p != "custom/" {
		t.Errorf("custom prefix = %q, want custom/", p)
	}

	cfg.S3.Prefix = ""
	cfg.Tenant.DefaultAccount = ""
	cfg.Tenant.DefaultProject = ""
	cfg.Mode = ModeLogs
	if p := cfg.AutoPrefix(); p != "logs/" {
		t.Errorf("no-tenant logs prefix = %q, want logs/", p)
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

func TestLoadWithMode_EmptyPath(t *testing.T) {
	cfg, err := LoadWithMode("", ModeLogs, RoleInsert)
	if err != nil {
		t.Fatalf("LoadWithMode empty path: %v", err)
	}
	if cfg.Mode != ModeLogs {
		t.Errorf("mode = %q, want %q", cfg.Mode, ModeLogs)
	}
	if cfg.Role != RoleInsert {
		t.Errorf("role = %q, want %q", cfg.Role, RoleInsert)
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

func TestParseSizeBytes(t *testing.T) {
	tests := []struct {
		input string
		want  int64
	}{
		{"512MB", 512 * 1024 * 1024},
		{"50GB", 50 * 1024 * 1024 * 1024},
		{"1TB", 1024 * 1024 * 1024 * 1024},
		{"256KB", 256 * 1024},
		{"100B", 100},
		{"1024", 1024},
		{" 512MB ", 512 * 1024 * 1024},
		{"512mb", 512 * 1024 * 1024},
		{"", 0},
	}

	for _, tt := range tests {
		got, err := ParseSizeBytes(tt.input)
		if err != nil {
			t.Errorf("ParseSizeBytes(%q): %v", tt.input, err)
			continue
		}
		if got != tt.want {
			t.Errorf("ParseSizeBytes(%q) = %d, want %d", tt.input, got, tt.want)
		}
	}
}

func TestParseSizeBytes_Invalid(t *testing.T) {
	_, err := ParseSizeBytes("abc")
	if err == nil {
		t.Error("expected error for invalid input")
	}
}

func TestCacheMemoryBytes(t *testing.T) {
	cfg := Default()
	got := cfg.CacheMemoryBytes()
	want := int64(512 * 1024 * 1024)
	if got != want {
		t.Errorf("CacheMemoryBytes = %d, want %d", got, want)
	}
}

func TestCacheDiskBytes(t *testing.T) {
	cfg := Default()
	got := cfg.CacheDiskBytes()
	want := int64(50 * 1024 * 1024 * 1024)
	if got != want {
		t.Errorf("CacheDiskBytes = %d, want %d", got, want)
	}
}

func TestCacheMemoryBytes_Invalid(t *testing.T) {
	cfg := Default()
	cfg.Cache.MemoryLimit = "invalid"
	got := cfg.CacheMemoryBytes()
	want := int64(512 * 1024 * 1024)
	if got != want {
		t.Errorf("CacheMemoryBytes with invalid = %d, want default %d", got, want)
	}
}

func TestDefaultInsertConfig(t *testing.T) {
	cfg := Default()
	if cfg.Role != RoleAll {
		t.Errorf("default role = %q, want %q", cfg.Role, RoleAll)
	}
	if cfg.Insert.FlushInterval != 60*time.Second {
		t.Errorf("default flush interval = %v, want 60s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.MaxBufferRows != 50000 {
		t.Errorf("default max buffer rows = %d, want 50000", cfg.Insert.MaxBufferRows)
	}
	if cfg.Insert.RowGroupSize != 10000 {
		t.Errorf("default row group size = %d, want 10000", cfg.Insert.RowGroupSize)
	}
	if len(cfg.Insert.BloomColumns) != 2 {
		t.Errorf("default bloom columns len = %d, want 2", len(cfg.Insert.BloomColumns))
	}
}

func TestInsertEnabled(t *testing.T) {
	tests := []struct {
		role       Role
		wantInsert bool
		wantSelect bool
	}{
		{RoleAll, true, true},
		{RoleInsert, true, false},
		{RoleSelect, false, true},
	}
	for _, tt := range tests {
		cfg := Default()
		cfg.Role = tt.role
		if got := cfg.InsertEnabled(); got != tt.wantInsert {
			t.Errorf("role=%q InsertEnabled() = %v, want %v", tt.role, got, tt.wantInsert)
		}
		if got := cfg.SelectEnabled(); got != tt.wantSelect {
			t.Errorf("role=%q SelectEnabled() = %v, want %v", tt.role, got, tt.wantSelect)
		}
	}
}

func TestValidate_InvalidRole(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Role = "bogus"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid role")
	}
}

func TestMaxBufferBytesN(t *testing.T) {
	cfg := Default()
	got := cfg.Insert.MaxBufferBytesN()
	want := int64(256 * 1024 * 1024)
	if got != want {
		t.Errorf("MaxBufferBytesN() = %d, want %d", got, want)
	}
}

func TestMaxBufferBytesN_Invalid(t *testing.T) {
	cfg := Default()
	cfg.Insert.MaxBufferBytes = "invalid"
	got := cfg.Insert.MaxBufferBytesN()
	want := int64(256 * 1024 * 1024)
	if got != want {
		t.Errorf("MaxBufferBytesN with invalid = %d, want default %d", got, want)
	}
}

func TestValidate_InsertInvalid(t *testing.T) {
	tests := []struct {
		name   string
		modify func(c *Config)
	}{
		{
			"zero flush interval",
			func(c *Config) { c.Insert.FlushInterval = 0 },
		},
		{
			"zero max buffer rows",
			func(c *Config) { c.Insert.MaxBufferRows = 0 },
		},
		{
			"zero row group size",
			func(c *Config) { c.Insert.RowGroupSize = 0 },
		},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test"
			cfg.Role = RoleInsert
			tt.modify(cfg)
			if err := cfg.Validate(); err == nil {
				t.Errorf("expected error for %s", tt.name)
			}
		})
	}
}

func TestValidate_SelectSkipsInsertValidation(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Role = RoleSelect
	cfg.Insert.FlushInterval = 0
	if err := cfg.Validate(); err != nil {
		t.Errorf("select role should skip insert validation: %v", err)
	}
}

func TestValidate_InvalidMaxConnections(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.S3.MaxConnections = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for zero max connections")
	}
}

func TestValidate_InvalidQueryMaxConcurrent(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Query.MaxConcurrent = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for zero query max concurrent")
	}
}

func TestValidate_InvalidQueryMaxRows(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Query.MaxRows = 0
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for zero query max rows")
	}
}

func TestValidate_EmptyRoleDefaultsToAll(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Role = ""
	if err := cfg.Validate(); err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if cfg.Role != RoleAll {
		t.Errorf("Role = %q, want %q", cfg.Role, RoleAll)
	}
}

func TestMergeConfig_S3Fields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.S3.Bucket = "overlay-bucket"
	overlay.S3.Region = "eu-west-1"
	overlay.S3.Prefix = "data/"
	overlay.S3.Endpoint = "http://minio:9000"
	overlay.S3.AccessKey = "ak"
	overlay.S3.SecretKey = "sk"
	overlay.S3.ForcePathStyle = true
	overlay.S3.MaxConnections = 64
	overlay.S3.Timeout = 15 * time.Second
	overlay.S3.RetryMax = 5
	overlay.S3.RetryBaseDelay = 500 * time.Millisecond

	result := mergeConfig(base, overlay)

	if result.S3.Bucket != "overlay-bucket" {
		t.Errorf("Bucket = %q, want overlay-bucket", result.S3.Bucket)
	}
	if result.S3.Region != "eu-west-1" {
		t.Errorf("Region = %q, want eu-west-1", result.S3.Region)
	}
	if result.S3.Prefix != "data/" {
		t.Errorf("Prefix = %q, want data/", result.S3.Prefix)
	}
	if result.S3.Endpoint != "http://minio:9000" {
		t.Errorf("Endpoint = %q", result.S3.Endpoint)
	}
	if result.S3.AccessKey != "ak" {
		t.Errorf("AccessKey = %q", result.S3.AccessKey)
	}
	if result.S3.SecretKey != "sk" {
		t.Errorf("SecretKey = %q", result.S3.SecretKey)
	}
	if !result.S3.ForcePathStyle {
		t.Error("ForcePathStyle should be true")
	}
	if result.S3.MaxConnections != 64 {
		t.Errorf("MaxConnections = %d, want 64", result.S3.MaxConnections)
	}
	if result.S3.Timeout != 15*time.Second {
		t.Errorf("Timeout = %v", result.S3.Timeout)
	}
	if result.S3.RetryMax != 5 {
		t.Errorf("RetryMax = %d, want 5", result.S3.RetryMax)
	}
	if result.S3.RetryBaseDelay != 500*time.Millisecond {
		t.Errorf("RetryBaseDelay = %v", result.S3.RetryBaseDelay)
	}
}

func TestMergeConfig_CacheFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Cache.MemoryLimit = "1GB"
	overlay.Cache.DiskPath = "/data/cache"
	overlay.Cache.DiskLimit = "200GB"
	overlay.Cache.EvictionWatermark = 0.9
	overlay.Cache.FooterTTL = 2 * time.Hour
	overlay.Cache.BloomTTL = 30 * time.Minute
	overlay.Cache.PageTTL = 5 * time.Minute

	result := mergeConfig(base, overlay)

	if result.Cache.MemoryLimit != "1GB" {
		t.Errorf("MemoryLimit = %q", result.Cache.MemoryLimit)
	}
	if result.Cache.DiskPath != "/data/cache" {
		t.Errorf("DiskPath = %q", result.Cache.DiskPath)
	}
	if result.Cache.DiskLimit != "200GB" {
		t.Errorf("DiskLimit = %q", result.Cache.DiskLimit)
	}
	if result.Cache.EvictionWatermark != 0.9 {
		t.Errorf("EvictionWatermark = %f", result.Cache.EvictionWatermark)
	}
	if result.Cache.FooterTTL != 2*time.Hour {
		t.Errorf("FooterTTL = %v", result.Cache.FooterTTL)
	}
	if result.Cache.BloomTTL != 30*time.Minute {
		t.Errorf("BloomTTL = %v", result.Cache.BloomTTL)
	}
	if result.Cache.PageTTL != 5*time.Minute {
		t.Errorf("PageTTL = %v", result.Cache.PageTTL)
	}
}

func TestMergeConfig_DiscoveryFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Discovery.HeadlessService = "lakehouse-headless"
	overlay.Discovery.StorageNodes = []string{"node1", "node2"}
	overlay.Discovery.PartitionAuthKey = "secret"
	overlay.Discovery.RefreshInterval = 10 * time.Minute
	overlay.Discovery.Timeout = 20 * time.Second
	overlay.Discovery.PeerHeadlessService = "peer-headless"
	overlay.Discovery.PeerRefreshInterval = 1 * time.Minute

	result := mergeConfig(base, overlay)

	if result.Discovery.HeadlessService != "lakehouse-headless" {
		t.Errorf("HeadlessService = %q", result.Discovery.HeadlessService)
	}
	if len(result.Discovery.StorageNodes) != 2 {
		t.Errorf("StorageNodes len = %d", len(result.Discovery.StorageNodes))
	}
	if result.Discovery.PartitionAuthKey != "secret" {
		t.Errorf("PartitionAuthKey = %q", result.Discovery.PartitionAuthKey)
	}
	if result.Discovery.RefreshInterval != 10*time.Minute {
		t.Errorf("RefreshInterval = %v", result.Discovery.RefreshInterval)
	}
	if result.Discovery.Timeout != 20*time.Second {
		t.Errorf("Timeout = %v", result.Discovery.Timeout)
	}
	if result.Discovery.PeerHeadlessService != "peer-headless" {
		t.Errorf("PeerHeadlessService = %q", result.Discovery.PeerHeadlessService)
	}
	if result.Discovery.PeerRefreshInterval != 1*time.Minute {
		t.Errorf("PeerRefreshInterval = %v", result.Discovery.PeerRefreshInterval)
	}
}

func TestMergeConfig_ManifestFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Manifest.RefreshInterval = 1 * time.Minute
	overlay.Manifest.SQSQueueURL = "https://sqs.us-east-1.amazonaws.com/123/queue"
	overlay.Manifest.SQSRegion = "us-east-1"
	overlay.Manifest.SQSWaitTime = 10 * time.Second
	overlay.Manifest.PersistPath = "/data/manifest"
	overlay.Manifest.PersistInterval = 2 * time.Minute

	result := mergeConfig(base, overlay)

	if result.Manifest.RefreshInterval != 1*time.Minute {
		t.Errorf("RefreshInterval = %v", result.Manifest.RefreshInterval)
	}
	if result.Manifest.SQSQueueURL != "https://sqs.us-east-1.amazonaws.com/123/queue" {
		t.Errorf("SQSQueueURL = %q", result.Manifest.SQSQueueURL)
	}
	if result.Manifest.SQSRegion != "us-east-1" {
		t.Errorf("SQSRegion = %q", result.Manifest.SQSRegion)
	}
	if result.Manifest.SQSWaitTime != 10*time.Second {
		t.Errorf("SQSWaitTime = %v", result.Manifest.SQSWaitTime)
	}
	if result.Manifest.PersistPath != "/data/manifest" {
		t.Errorf("PersistPath = %q", result.Manifest.PersistPath)
	}
	if result.Manifest.PersistInterval != 2*time.Minute {
		t.Errorf("PersistInterval = %v", result.Manifest.PersistInterval)
	}
}

func TestMergeConfig_PrefetchFields(t *testing.T) {
	base := Default()
	base.Prefetch.Correlated = false
	overlay := &Config{}
	overlay.Prefetch.Correlated = true
	overlay.Prefetch.ReadAheadDepth = 5
	overlay.Prefetch.MaxConcurrent = 8
	overlay.Prefetch.MaxQueue = 200

	result := mergeConfig(base, overlay)

	if !result.Prefetch.Correlated {
		t.Error("Correlated should be true")
	}
	if result.Prefetch.ReadAheadDepth != 5 {
		t.Errorf("ReadAheadDepth = %d", result.Prefetch.ReadAheadDepth)
	}
	if result.Prefetch.MaxConcurrent != 8 {
		t.Errorf("MaxConcurrent = %d", result.Prefetch.MaxConcurrent)
	}
	if result.Prefetch.MaxQueue != 200 {
		t.Errorf("MaxQueue = %d", result.Prefetch.MaxQueue)
	}
}

func TestMergeConfig_PeerFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Peer.AuthKey = "peer-secret"
	overlay.Peer.Timeout = 10 * time.Second
	overlay.Peer.MaxConnections = 64

	result := mergeConfig(base, overlay)

	if result.Peer.AuthKey != "peer-secret" {
		t.Errorf("AuthKey = %q", result.Peer.AuthKey)
	}
	if result.Peer.Timeout != 10*time.Second {
		t.Errorf("Timeout = %v", result.Peer.Timeout)
	}
	if result.Peer.MaxConnections != 64 {
		t.Errorf("MaxConnections = %d", result.Peer.MaxConnections)
	}
}

func TestMergeConfig_StartupFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Startup.ServeStale = true
	overlay.Startup.WarmupWindow = 48 * time.Hour
	overlay.Startup.MaxWarmupTime = 10 * time.Minute

	result := mergeConfig(base, overlay)

	if !result.Startup.ServeStale {
		t.Error("ServeStale should be true")
	}
	if result.Startup.WarmupWindow != 48*time.Hour {
		t.Errorf("WarmupWindow = %v", result.Startup.WarmupWindow)
	}
	if result.Startup.MaxWarmupTime != 10*time.Minute {
		t.Errorf("MaxWarmupTime = %v", result.Startup.MaxWarmupTime)
	}
}

func TestMergeConfig_QueryFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Query.MaxConcurrent = 64
	overlay.Query.Timeout = 120 * time.Second
	overlay.Query.MaxRows = 5_000_000
	overlay.Query.SlowThreshold = 10 * time.Second

	result := mergeConfig(base, overlay)

	if result.Query.MaxConcurrent != 64 {
		t.Errorf("MaxConcurrent = %d", result.Query.MaxConcurrent)
	}
	if result.Query.Timeout != 120*time.Second {
		t.Errorf("Timeout = %v", result.Query.Timeout)
	}
	if result.Query.MaxRows != 5_000_000 {
		t.Errorf("MaxRows = %d", result.Query.MaxRows)
	}
	if result.Query.SlowThreshold != 10*time.Second {
		t.Errorf("SlowThreshold = %v", result.Query.SlowThreshold)
	}
}

func TestMergeConfig_TenantFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Tenant.DefaultPrefix = "org1/"
	overlay.Tenant.PrefixTemplate = "{OrgID}/"
	overlay.Tenant.Isolation = "bucket"
	overlay.Tenant.BucketTemplate = "obs-{AccountID}-{ProjectID}"
	overlay.Tenant.DefaultAccount = "42"
	overlay.Tenant.DefaultProject = "7"
	overlay.Tenant.HeaderAccount = "X-Custom-Account"
	overlay.Tenant.HeaderProject = "X-Custom-Project"
	overlay.Tenant.GlobalReadHeader = "X-Global-Read"
	overlay.Tenant.GlobalReadValue = "secret-key"

	result := mergeConfig(base, overlay)

	if result.Tenant.DefaultPrefix != "org1/" {
		t.Errorf("DefaultPrefix = %q", result.Tenant.DefaultPrefix)
	}
	if result.Tenant.PrefixTemplate != "{OrgID}/" {
		t.Errorf("PrefixTemplate = %q", result.Tenant.PrefixTemplate)
	}
	if result.Tenant.Isolation != "bucket" {
		t.Errorf("Isolation = %q, want bucket", result.Tenant.Isolation)
	}
	if result.Tenant.BucketTemplate != "obs-{AccountID}-{ProjectID}" {
		t.Errorf("BucketTemplate = %q", result.Tenant.BucketTemplate)
	}
	if result.Tenant.DefaultAccount != "42" {
		t.Errorf("DefaultAccount = %q, want 42", result.Tenant.DefaultAccount)
	}
	if result.Tenant.DefaultProject != "7" {
		t.Errorf("DefaultProject = %q, want 7", result.Tenant.DefaultProject)
	}
	if result.Tenant.HeaderAccount != "X-Custom-Account" {
		t.Errorf("HeaderAccount = %q", result.Tenant.HeaderAccount)
	}
	if result.Tenant.HeaderProject != "X-Custom-Project" {
		t.Errorf("HeaderProject = %q", result.Tenant.HeaderProject)
	}
	if result.Tenant.GlobalReadHeader != "X-Global-Read" {
		t.Errorf("GlobalReadHeader = %q", result.Tenant.GlobalReadHeader)
	}
	if result.Tenant.GlobalReadValue != "secret-key" {
		t.Errorf("GlobalReadValue = %q", result.Tenant.GlobalReadValue)
	}
}

func TestDefaultConfig_TenantDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Tenant.Isolation != "prefix" {
		t.Errorf("default isolation = %q, want prefix", cfg.Tenant.Isolation)
	}
	if cfg.Tenant.DefaultAccount != "0" {
		t.Errorf("default account = %q, want 0", cfg.Tenant.DefaultAccount)
	}
	if cfg.Tenant.DefaultProject != "0" {
		t.Errorf("default project = %q, want 0", cfg.Tenant.DefaultProject)
	}
	if cfg.Tenant.HeaderAccount != "X-Scope-AccountID" {
		t.Errorf("default header account = %q", cfg.Tenant.HeaderAccount)
	}
	if cfg.Tenant.HeaderProject != "X-Scope-ProjectID" {
		t.Errorf("default header project = %q", cfg.Tenant.HeaderProject)
	}
}

func TestMergeConfig_TopLevelFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Mode = ModeTraces
	overlay.Topology = TopologyDirect
	overlay.HotBoundary = "14d"
	overlay.Role = RoleInsert

	result := mergeConfig(base, overlay)

	if result.Mode != ModeTraces {
		t.Errorf("Mode = %q", result.Mode)
	}
	if result.Topology != TopologyDirect {
		t.Errorf("Topology = %q", result.Topology)
	}
	if result.HotBoundary != "14d" {
		t.Errorf("HotBoundary = %q", result.HotBoundary)
	}
	if result.Role != RoleInsert {
		t.Errorf("Role = %q", result.Role)
	}
}

func TestMergeConfig_InsertFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Insert.FlushInterval = 5 * time.Second
	overlay.Insert.MaxBufferRows = 100000
	overlay.Insert.MaxBufferBytes = "512MB"
	overlay.Insert.RowGroupSize = 20000
	overlay.Insert.BloomColumns = []string{"trace_id"}
	overlay.Insert.WALEnabled = true
	overlay.Insert.WALDir = "/data/wal"

	result := mergeConfig(base, overlay)

	if result.Insert.FlushInterval != 5*time.Second {
		t.Errorf("FlushInterval = %v", result.Insert.FlushInterval)
	}
	if result.Insert.MaxBufferRows != 100000 {
		t.Errorf("MaxBufferRows = %d", result.Insert.MaxBufferRows)
	}
	if result.Insert.MaxBufferBytes != "512MB" {
		t.Errorf("MaxBufferBytes = %q", result.Insert.MaxBufferBytes)
	}
	if result.Insert.RowGroupSize != 20000 {
		t.Errorf("RowGroupSize = %d", result.Insert.RowGroupSize)
	}
	if len(result.Insert.BloomColumns) != 1 || result.Insert.BloomColumns[0] != "trace_id" {
		t.Errorf("BloomColumns = %v", result.Insert.BloomColumns)
	}
	if !result.Insert.WALEnabled {
		t.Error("WALEnabled should be true")
	}
	if result.Insert.WALDir != "/data/wal" {
		t.Errorf("WALDir = %q", result.Insert.WALDir)
	}
}

func TestMergeConfig_EmptyOverlayPreservesBase(t *testing.T) {
	base := Default()
	base.Mode = ModeLogs
	base.S3.Bucket = "orig-bucket"
	overlay := &Config{}

	result := mergeConfig(base, overlay)

	if result.Mode != ModeLogs {
		t.Errorf("Mode = %q, want logs", result.Mode)
	}
	if result.S3.Bucket != "orig-bucket" {
		t.Errorf("Bucket = %q, want orig-bucket", result.S3.Bucket)
	}
	if result.S3.Region != "us-east-1" {
		t.Errorf("Region = %q, want default", result.S3.Region)
	}
}

func TestLoad_YAMLWithInsertConfig(t *testing.T) {
	content := `
lakehouse:
  mode: logs
  s3:
    bucket: test-bucket
  role: insert
  insert:
    flush_interval: 5s
    max_buffer_rows: 100000
    max_buffer_bytes: 512MB
    row_group_size: 20000
    bloom_columns:
      - trace_id
    wal_enabled: true
    wal_dir: /data/wal
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

	if cfg.Role != RoleInsert {
		t.Errorf("Role = %q, want insert", cfg.Role)
	}
	if cfg.Insert.FlushInterval != 5*time.Second {
		t.Errorf("FlushInterval = %v, want 5s", cfg.Insert.FlushInterval)
	}
	if cfg.Insert.MaxBufferRows != 100000 {
		t.Errorf("MaxBufferRows = %d, want 100000", cfg.Insert.MaxBufferRows)
	}
	if cfg.Insert.MaxBufferBytes != "512MB" {
		t.Errorf("MaxBufferBytes = %q, want 512MB", cfg.Insert.MaxBufferBytes)
	}
	if cfg.Insert.RowGroupSize != 20000 {
		t.Errorf("RowGroupSize = %d, want 20000", cfg.Insert.RowGroupSize)
	}
	if len(cfg.Insert.BloomColumns) != 1 || cfg.Insert.BloomColumns[0] != "trace_id" {
		t.Errorf("BloomColumns = %v", cfg.Insert.BloomColumns)
	}
	if !cfg.Insert.WALEnabled {
		t.Error("WALEnabled should be true")
	}
	if cfg.Insert.WALDir != "/data/wal" {
		t.Errorf("WALDir = %q", cfg.Insert.WALDir)
	}
}

func TestValidate_ExtraPromotedValid(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Schema.ExtraPromoted = []ExtraPromotedColumn{
		{Name: "http.status_code", Type: "string", Bloom: true},
		{Name: "customer_id", Type: "int64", Bloom: false},
		{Name: "latency", Type: "float64"},
		{Name: "count", Type: "int32"},
	}
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected valid: %v", err)
	}
}

func TestValidate_ExtraPromotedEmptyName(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Schema.ExtraPromoted = []ExtraPromotedColumn{
		{Name: "", Type: "string"},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for empty extra promoted name")
	}
}

func TestValidate_ExtraPromotedInvalidType(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Schema.ExtraPromoted = []ExtraPromotedColumn{
		{Name: "field", Type: "boolean"},
	}
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid extra promoted type")
	}
}

func TestMergeConfig_SchemaFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Schema.ExtraPromoted = []ExtraPromotedColumn{
		{Name: "http.status_code", Type: "string", Bloom: true},
		{Name: "customer_id", Type: "string", Bloom: false},
	}

	result := mergeConfig(base, overlay)

	if len(result.Schema.ExtraPromoted) != 2 {
		t.Fatalf("ExtraPromoted len = %d, want 2", len(result.Schema.ExtraPromoted))
	}
	if result.Schema.ExtraPromoted[0].Name != "http.status_code" {
		t.Errorf("ExtraPromoted[0].Name = %q", result.Schema.ExtraPromoted[0].Name)
	}
	if !result.Schema.ExtraPromoted[0].Bloom {
		t.Error("ExtraPromoted[0].Bloom should be true")
	}
	if result.Schema.ExtraPromoted[1].Name != "customer_id" {
		t.Errorf("ExtraPromoted[1].Name = %q", result.Schema.ExtraPromoted[1].Name)
	}
}

func TestDefaultConfig_DeleteConfig(t *testing.T) {
	cfg := Default()

	if !cfg.Delete.Enabled {
		t.Error("default Delete.Enabled should be true")
	}
	if cfg.Delete.DefaultMode != "auto" {
		t.Errorf("default Delete.DefaultMode = %q, want %q", cfg.Delete.DefaultMode, "auto")
	}
	if len(cfg.Delete.AutoRewriteClasses) != 1 || cfg.Delete.AutoRewriteClasses[0] != "STANDARD" {
		t.Errorf("default Delete.AutoRewriteClasses = %v, want [STANDARD]", cfg.Delete.AutoRewriteClasses)
	}
	if cfg.Delete.RewriteDelay != time.Hour {
		t.Errorf("default Delete.RewriteDelay = %v, want 1h", cfg.Delete.RewriteDelay)
	}
	if cfg.Delete.RewriteBatchSize != 50 {
		t.Errorf("default Delete.RewriteBatchSize = %d, want 50", cfg.Delete.RewriteBatchSize)
	}
	if cfg.Delete.RewriteMaxConcurrent != 2 {
		t.Errorf("default Delete.RewriteMaxConcurrent = %d, want 2", cfg.Delete.RewriteMaxConcurrent)
	}
	if cfg.Delete.PersistPath != "/data/lakehouse/tombstones" {
		t.Errorf("default Delete.PersistPath = %q, want %q", cfg.Delete.PersistPath, "/data/lakehouse/tombstones")
	}
	if cfg.Delete.CostWarningThreshold != 10.0 {
		t.Errorf("default Delete.CostWarningThreshold = %f, want 10.0", cfg.Delete.CostWarningThreshold)
	}
	if cfg.Delete.ForceGlacierHeader != "X-Force-Glacier-Delete" {
		t.Errorf("default Delete.ForceGlacierHeader = %q, want %q", cfg.Delete.ForceGlacierHeader, "X-Force-Glacier-Delete")
	}
	if cfg.Delete.VerifyInterval != 6*time.Hour {
		t.Errorf("default Delete.VerifyInterval = %v, want 6h", cfg.Delete.VerifyInterval)
	}
	if cfg.Delete.LifecycleRules != nil {
		t.Errorf("default Delete.LifecycleRules should be nil, got %v", cfg.Delete.LifecycleRules)
	}
}

func TestMergeConfig_EmptySchemaPreservesBase(t *testing.T) {
	base := Default()
	base.Schema.ExtraPromoted = []ExtraPromotedColumn{
		{Name: "existing", Type: "string"},
	}
	overlay := &Config{}

	result := mergeConfig(base, overlay)

	if len(result.Schema.ExtraPromoted) != 1 {
		t.Fatalf("ExtraPromoted should be preserved, got %d", len(result.Schema.ExtraPromoted))
	}
	if result.Schema.ExtraPromoted[0].Name != "existing" {
		t.Errorf("Name = %q, want existing", result.Schema.ExtraPromoted[0].Name)
	}
}

func TestDefaultConfig_CompressionLevel(t *testing.T) {
	cfg := Default()
	// Insert default dropped from 7 → 3 when progressive compaction
	// compression landed (the L0 slot in CompressionLevelByOutputLevel).
	// At write time we now prefer fast Default-mode zstd, with later
	// compaction passes investing more CPU as files age.
	if cfg.Insert.CompressionLevel != 3 {
		t.Errorf("default CompressionLevel = %d, want 3 (was 7 before progressive compaction)", cfg.Insert.CompressionLevel)
	}
}

func TestDefaultConfig_CompactionCompressionSchedule(t *testing.T) {
	cfg := Default()
	want := []int{3, 7, 11}
	got := cfg.Compaction.CompressionLevelByOutputLevel
	if len(got) != len(want) {
		t.Fatalf("default CompressionLevelByOutputLevel = %v, want %v", got, want)
	}
	for i := range want {
		if got[i] != want[i] {
			t.Errorf("default CompressionLevelByOutputLevel[%d] = %d, want %d", i, got[i], want[i])
		}
	}
}

func TestCompressionLevelForOutput_GlobalSchedule(t *testing.T) {
	cfg := CompactionConfig{CompressionLevelByOutputLevel: []int{3, 7, 11}}
	cases := []struct {
		outputLevel int
		want        int
	}{
		{0, 3},
		{1, 7},
		{2, 11},
		{3, 11}, // saturates to last slot
		{99, 11},
	}
	for _, tc := range cases {
		if got := cfg.CompressionLevelForOutput(tc.outputLevel); got != tc.want {
			t.Errorf("CompressionLevelForOutput(%d) = %d, want %d", tc.outputLevel, got, tc.want)
		}
	}
}

func TestCompressionLevelForOutput_EmptySliceFallsThrough(t *testing.T) {
	// Empty schedule means "no progressive override"; caller must
	// fall back to Insert.CompressionLevel. The helper signals that
	// by returning 0 so the caller can branch.
	cfg := CompactionConfig{}
	if got := cfg.CompressionLevelForOutput(2); got != 0 {
		t.Errorf("empty schedule = %d, want 0 (signal to caller to fall back)", got)
	}
}

func TestValidate_CompressionLevelInvalid(t *testing.T) {
	tests := []struct {
		name  string
		level int
	}{
		{"zero", 0},
		{"negative", -1},
		{"too_high", 23},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test"
			cfg.Insert.CompressionLevel = tt.level
			if err := cfg.Validate(); err == nil {
				t.Error("expected error for invalid compression level")
			}
		})
	}
}

func TestValidate_CompressionLevelValid(t *testing.T) {
	for _, level := range []int{1, 3, 10, 22} {
		cfg := Default()
		cfg.Mode = ModeLogs
		cfg.S3.Bucket = "test"
		cfg.Insert.CompressionLevel = level
		if err := cfg.Validate(); err != nil {
			t.Errorf("level %d should be valid: %v", level, err)
		}
	}
}

func TestMergeConfig_CompressionLevel(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Insert.CompressionLevel = 11

	result := mergeConfig(base, overlay)
	if result.Insert.CompressionLevel != 11 {
		t.Errorf("CompressionLevel = %d, want 11", result.Insert.CompressionLevel)
	}
}

func TestLoad_YAMLWithSchemaConfig(t *testing.T) {
	content := `
lakehouse:
  mode: logs
  s3:
    bucket: test-bucket
  schema:
    extra_promoted:
      - name: http.status_code
        type: string
        bloom: true
      - name: customer_id
        type: string
        bloom: false
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

	if len(cfg.Schema.ExtraPromoted) != 2 {
		t.Fatalf("ExtraPromoted len = %d, want 2", len(cfg.Schema.ExtraPromoted))
	}
	if cfg.Schema.ExtraPromoted[0].Name != "http.status_code" {
		t.Errorf("Name = %q", cfg.Schema.ExtraPromoted[0].Name)
	}
	if cfg.Schema.ExtraPromoted[0].Type != "string" {
		t.Errorf("Type = %q", cfg.Schema.ExtraPromoted[0].Type)
	}
	if !cfg.Schema.ExtraPromoted[0].Bloom {
		t.Error("Bloom should be true")
	}
	if cfg.Schema.ExtraPromoted[1].Name != "customer_id" {
		t.Errorf("Name = %q", cfg.Schema.ExtraPromoted[1].Name)
	}
}

func TestDefaultConfig_TargetFileSize(t *testing.T) {
	cfg := Default()
	if cfg.Insert.TargetFileSize != "128MB" {
		t.Errorf("TargetFileSize = %q, want 128MB", cfg.Insert.TargetFileSize)
	}
}

func TestDefaultConfig_WALMaxBytes(t *testing.T) {
	cfg := Default()
	if cfg.Insert.WALMaxBytes != "512MB" {
		t.Errorf("WALMaxBytes = %q, want 512MB", cfg.Insert.WALMaxBytes)
	}
}

func TestDefaultConfig_WALEnabled(t *testing.T) {
	cfg := Default()
	if !cfg.Insert.WALEnabled {
		t.Error("WALEnabled should default to true")
	}
}

func TestDefaultConfig_SelectConfig(t *testing.T) {
	cfg := Default()
	if !cfg.Select.BufferQueryEnabled {
		t.Error("BufferQueryEnabled should default to true")
	}
	if cfg.Select.BufferQueryTimeout != 2*time.Second {
		t.Errorf("BufferQueryTimeout = %v, want 2s", cfg.Select.BufferQueryTimeout)
	}
}

func TestInsertConfig_TargetFileSizeN(t *testing.T) {
	ic := &InsertConfig{TargetFileSize: "128MB"}
	got := ic.TargetFileSizeN()
	want := int64(128 * 1024 * 1024)
	if got != want {
		t.Errorf("TargetFileSizeN = %d, want %d", got, want)
	}
}

func TestInsertConfig_WALMaxBytesN(t *testing.T) {
	ic := &InsertConfig{WALMaxBytes: "512MB"}
	got := ic.WALMaxBytesN()
	want := int64(512 * 1024 * 1024)
	if got != want {
		t.Errorf("WALMaxBytesN = %d, want %d", got, want)
	}
}

func TestValidate_TargetFileSizeRequired(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Insert.TargetFileSize = ""
	if err := cfg.Validate(); err == nil {
		t.Error("empty TargetFileSize should fail validation")
	}
}

func TestMergeConfig_SelectFields(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Select.BufferQueryTimeout = 5 * time.Second
	overlay.Select.InsertHeadlessService = "lakehouse-insert-headless"

	result := mergeConfig(base, overlay)
	if result.Select.BufferQueryTimeout != 5*time.Second {
		t.Errorf("BufferQueryTimeout = %v", result.Select.BufferQueryTimeout)
	}
	if result.Select.InsertHeadlessService != "lakehouse-insert-headless" {
		t.Errorf("InsertHeadlessService = %q", result.Select.InsertHeadlessService)
	}
}

func TestMergeConfig_TargetFileSize(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Insert.TargetFileSize = "256MB"

	result := mergeConfig(base, overlay)
	if result.Insert.TargetFileSize != "256MB" {
		t.Errorf("TargetFileSize = %q, want 256MB", result.Insert.TargetFileSize)
	}
}

func TestDefaultConfig_ModeConfigs(t *testing.T) {
	cfg := Default()

	if len(cfg.Logs.BloomColumns) != 2 || cfg.Logs.BloomColumns[0] != "service.name" || cfg.Logs.BloomColumns[1] != "trace_id" {
		t.Errorf("Logs.BloomColumns = %v, want [service.name trace_id]", cfg.Logs.BloomColumns)
	}
	if cfg.Logs.DeletePrefix != "/delete/logsql" {
		t.Errorf("Logs.DeletePrefix = %q, want /delete/logsql", cfg.Logs.DeletePrefix)
	}

	if len(cfg.Traces.BloomColumns) != 2 {
		t.Fatalf("Traces.BloomColumns len = %d, want 2", len(cfg.Traces.BloomColumns))
	}
	if cfg.Traces.BloomColumns[0] != "trace_id" || cfg.Traces.BloomColumns[1] != "service.name" {
		t.Errorf("Traces.BloomColumns = %v", cfg.Traces.BloomColumns)
	}
	if cfg.Traces.DeletePrefix != "/delete/tracessql" {
		t.Errorf("Traces.DeletePrefix = %q, want /delete/tracessql", cfg.Traces.DeletePrefix)
	}
	if !cfg.Traces.JaegerEnabled {
		t.Error("Traces.JaegerEnabled should be true by default")
	}
	if cfg.Traces.JaegerGRPCAddr != ":16685" {
		t.Errorf("Traces.JaegerGRPCAddr = %q, want :16685", cfg.Traces.JaegerGRPCAddr)
	}
}

func TestActiveBloomColumns_LogsMode(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs

	cols := cfg.ActiveBloomColumns()
	if len(cols) != 2 || cols[0] != "service.name" || cols[1] != "trace_id" {
		t.Errorf("ActiveBloomColumns(logs) = %v, want [service.name trace_id]", cols)
	}
}

func TestActiveBloomColumns_TracesMode(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeTraces

	cols := cfg.ActiveBloomColumns()
	if len(cols) != 2 || cols[0] != "trace_id" {
		t.Errorf("ActiveBloomColumns(traces) = %v, want [trace_id, service.name]", cols)
	}
}

func TestActiveBloomColumns_OverrideFromModeSection(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Logs.BloomColumns = []string{"custom.field", "trace_id"}

	cols := cfg.ActiveBloomColumns()
	if len(cols) != 2 || cols[0] != "custom.field" {
		t.Errorf("ActiveBloomColumns = %v, want [custom.field, trace_id]", cols)
	}
}

func TestActiveBloomColumns_FallbackToInsert(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Logs.BloomColumns = nil
	cfg.Insert.BloomColumns = []string{"fallback"}

	cols := cfg.ActiveBloomColumns()
	if len(cols) != 1 || cols[0] != "fallback" {
		t.Errorf("ActiveBloomColumns = %v, want [fallback]", cols)
	}
}

func TestActiveDeletePrefix(t *testing.T) {
	cfg := Default()

	cfg.Mode = ModeLogs
	if p := cfg.ActiveDeletePrefix(); p != "/delete/logsql" {
		t.Errorf("logs prefix = %q", p)
	}

	cfg.Mode = ModeTraces
	if p := cfg.ActiveDeletePrefix(); p != "/delete/tracessql" {
		t.Errorf("traces prefix = %q", p)
	}

	cfg.Traces.DeletePrefix = "/custom/delete"
	if p := cfg.ActiveDeletePrefix(); p != "/custom/delete" {
		t.Errorf("custom traces prefix = %q", p)
	}
}

func TestActiveCompatVersion(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs

	if v := cfg.ActiveCompatVersion(); v != "" {
		t.Errorf("default compat = %q, want empty", v)
	}

	cfg.Logs.CompatVersion = "1.50.0"
	if v := cfg.ActiveCompatVersion(); v != "1.50.0" {
		t.Errorf("logs compat = %q, want 1.50.0", v)
	}

	cfg.Mode = ModeTraces
	cfg.Traces.CompatVersion = "0.8.2"
	if v := cfg.ActiveCompatVersion(); v != "0.8.2" {
		t.Errorf("traces compat = %q, want 0.8.2", v)
	}
}

func TestMergeConfig_ModeConfigs(t *testing.T) {
	base := Default()
	overlay := &Config{}
	overlay.Logs.BloomColumns = []string{"custom_field"}
	overlay.Logs.DeletePrefix = "/custom/logs"
	overlay.Traces.BloomColumns = []string{"span_id"}
	overlay.Traces.JaegerEnabled = true
	overlay.Traces.JaegerGRPCAddr = ":9999"

	result := mergeConfig(base, overlay)

	if len(result.Logs.BloomColumns) != 1 || result.Logs.BloomColumns[0] != "custom_field" {
		t.Errorf("Logs.BloomColumns = %v", result.Logs.BloomColumns)
	}
	if result.Logs.DeletePrefix != "/custom/logs" {
		t.Errorf("Logs.DeletePrefix = %q", result.Logs.DeletePrefix)
	}
	if len(result.Traces.BloomColumns) != 1 || result.Traces.BloomColumns[0] != "span_id" {
		t.Errorf("Traces.BloomColumns = %v", result.Traces.BloomColumns)
	}
	if !result.Traces.JaegerEnabled {
		t.Error("Traces.JaegerEnabled should be true")
	}
	if result.Traces.JaegerGRPCAddr != ":9999" {
		t.Errorf("Traces.JaegerGRPCAddr = %q", result.Traces.JaegerGRPCAddr)
	}
}

func TestLoad_YAMLWithModeConfigs(t *testing.T) {
	content := `
lakehouse:
  mode: traces
  s3:
    bucket: test-bucket
  traces:
    bloom_columns:
      - span_id
      - trace_id
    delete_prefix: /delete/custom
    jaeger_enabled: true
    jaeger_grpc_addr: ":17685"
`
	dir := t.TempDir()
	path := filepath.Join(dir, "config.yaml")
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}

	if len(cfg.Traces.BloomColumns) != 2 || cfg.Traces.BloomColumns[0] != "span_id" {
		t.Errorf("Traces.BloomColumns = %v", cfg.Traces.BloomColumns)
	}
	if cfg.Traces.DeletePrefix != "/delete/custom" {
		t.Errorf("Traces.DeletePrefix = %q", cfg.Traces.DeletePrefix)
	}
	if !cfg.Traces.JaegerEnabled {
		t.Error("Traces.JaegerEnabled should be true")
	}
	if cfg.Traces.JaegerGRPCAddr != ":17685" {
		t.Errorf("Traces.JaegerGRPCAddr = %q", cfg.Traces.JaegerGRPCAddr)
	}
}

// --- Task 1: Size string parse error validation ---

func TestValidate_MalformedSizeStrings(t *testing.T) {
	tests := []struct {
		name   string
		modify func(c *Config)
	}{
		{
			"invalid MaxBufferBytes",
			func(c *Config) { c.Insert.MaxBufferBytes = "notasize" },
		},
		{
			"invalid TargetFileSize",
			func(c *Config) { c.Insert.TargetFileSize = "xyz" },
		},
		{
			"invalid WALMaxBytes",
			func(c *Config) { c.Insert.WALMaxBytes = "abc123def" },
		},
		{
			"MaxBufferBytes with bad suffix",
			func(c *Config) { c.Insert.MaxBufferBytes = "256ZZ" },
		},
		{
			"TargetFileSize non-numeric",
			func(c *Config) { c.Insert.TargetFileSize = "twohundredMB" },
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test"
			tt.modify(cfg)
			if err := cfg.Validate(); err == nil {
				t.Error("expected validation error for malformed size string")
			}
		})
	}
}

func TestValidate_ValidSizeStrings(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Insert.MaxBufferBytes = "512MB"
	cfg.Insert.TargetFileSize = "128MB"
	cfg.Insert.WALMaxBytes = "1GB"
	if err := cfg.Validate(); err != nil {
		t.Errorf("expected no error for valid sizes, got: %v", err)
	}
}

// --- Task 1: Cross-field validation ---

// PR A: TestValidate_CompactionEnabledWith*LeaderElection cases removed.
// The leader_election config field has been deleted (spec §7); compaction
// is now always election-free (HRW ownership). Compaction enable/disable
// cross-field semantics remain covered by the broader Validate sweep.

// --- Task 1: HotBoundary format validation ---

func TestValidate_HotBoundaryValid(t *testing.T) {
	tests := []string{"7d", "14d", "30d", "1h", "168h", "24h30m"}
	for _, hb := range tests {
		t.Run(hb, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test"
			cfg.HotBoundary = hb
			if err := cfg.Validate(); err != nil {
				t.Errorf("valid HotBoundary %q should pass, got: %v", hb, err)
			}
		})
	}
}

func TestValidate_HotBoundaryInvalid(t *testing.T) {
	tests := []string{"notaduration", "14x", "abc", "d", "7dd"}
	for _, hb := range tests {
		t.Run(hb, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = ModeLogs
			cfg.S3.Bucket = "test"
			cfg.HotBoundary = hb
			if err := cfg.Validate(); err == nil {
				t.Errorf("invalid HotBoundary %q should fail validation", hb)
			}
		})
	}
}

func TestValidate_HotBoundaryEmpty(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.HotBoundary = ""
	if err := cfg.Validate(); err != nil {
		t.Errorf("empty HotBoundary should be valid, got: %v", err)
	}
}

func TestDefaultConfig_DurabilityFields(t *testing.T) {
	cfg := Default()

	if cfg.Insert.AckMode != "buffer" {
		t.Errorf("default ack_mode = %q, want buffer", cfg.Insert.AckMode)
	}
	if cfg.Insert.FlushLinger != 200*time.Millisecond {
		t.Errorf("default flush_linger = %v, want 200ms", cfg.Insert.FlushLinger)
	}
	if cfg.Insert.FlushMaxRows != 5000 {
		t.Errorf("default flush_max_rows = %d, want 5000", cfg.Insert.FlushMaxRows)
	}
	if cfg.Insert.PeerReplicate {
		t.Error("default peer_replicate should be false")
	}
	if cfg.Insert.PeerReplicateTimeout != 5*time.Millisecond {
		t.Errorf("default peer_replicate_timeout = %v, want 5ms", cfg.Insert.PeerReplicateTimeout)
	}
	if cfg.Insert.PeerReplicateTTL != 30*time.Second {
		t.Errorf("default peer_replicate_ttl = %v, want 30s", cfg.Insert.PeerReplicateTTL)
	}
}

func TestDefaultConfig_GCFields(t *testing.T) {
	cfg := Default()

	if !cfg.GC.Enabled {
		t.Error("default GC should be enabled")
	}
	if cfg.GC.Interval != 6*time.Hour {
		t.Errorf("default GC interval = %v, want 6h", cfg.GC.Interval)
	}
	if cfg.GC.OrphanGracePeriod != 1*time.Hour {
		t.Errorf("default GC orphan grace = %v, want 1h", cfg.GC.OrphanGracePeriod)
	}
}

func TestValidate_AckMode(t *testing.T) {
	for _, mode := range []string{"buffer", "wal", "flush-sync"} {
		cfg := Default()
		cfg.Mode = ModeLogs
		cfg.S3.Bucket = "test"
		cfg.Insert.AckMode = mode
		if err := cfg.Validate(); err != nil {
			t.Errorf("ack_mode=%q should be valid: %v", mode, err)
		}
	}

	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Insert.AckMode = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected error for invalid ack_mode")
	}
}

func TestValidate_PeerReplicateWarning(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.S3.Bucket = "test"
	cfg.Insert.AckMode = "flush-sync"
	cfg.Insert.PeerReplicate = true
	if err := cfg.Validate(); err != nil {
		t.Errorf("flush-sync + peer_replicate should be valid (with warning): %v", err)
	}
}

func TestLoad_DurabilityFields(t *testing.T) {
	content := `
lakehouse:
  mode: logs
  s3:
    bucket: test
  insert:
    ack_mode: flush-sync
    flush_linger: 500ms
    flush_max_rows: 10000
    peer_replicate: true
    peer_replicate_timeout: 10ms
    peer_replicate_ttl: 60s
  gc:
    enabled: true
    interval: 12h
    orphan_grace_period: 2h
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

	if cfg.Insert.AckMode != "flush-sync" {
		t.Errorf("ack_mode = %q, want flush-sync", cfg.Insert.AckMode)
	}
	if cfg.Insert.FlushLinger != 500*time.Millisecond {
		t.Errorf("flush_linger = %v, want 500ms", cfg.Insert.FlushLinger)
	}
	if cfg.Insert.FlushMaxRows != 10000 {
		t.Errorf("flush_max_rows = %d, want 10000", cfg.Insert.FlushMaxRows)
	}
	if !cfg.Insert.PeerReplicate {
		t.Error("peer_replicate should be true")
	}
	if cfg.Insert.PeerReplicateTimeout != 10*time.Millisecond {
		t.Errorf("peer_replicate_timeout = %v, want 10ms", cfg.Insert.PeerReplicateTimeout)
	}
	if cfg.Insert.PeerReplicateTTL != 60*time.Second {
		t.Errorf("peer_replicate_ttl = %v, want 60s", cfg.Insert.PeerReplicateTTL)
	}
	if cfg.GC.Interval != 12*time.Hour {
		t.Errorf("GC interval = %v, want 12h", cfg.GC.Interval)
	}
	if cfg.GC.OrphanGracePeriod != 2*time.Hour {
		t.Errorf("GC orphan grace = %v, want 2h", cfg.GC.OrphanGracePeriod)
	}
}
