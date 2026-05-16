package parquets3

import (
	"testing"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/discovery"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

func newMinimalStorage() *Storage {
	cfg := config.Default()
	cfg.Mode = config.ModeTraces
	return &Storage{
		cfg:        cfg,
		manifest:   manifest.New("test", "traces/"),
		registry:   schema.NewRegistry(schema.TracesProfile),
		memCache:   cache.NewLRU(64 * 1024 * 1024),
		sfGroup:    cache.NewGroup(),
		labelIndex: cache.NewLabelIndex(),
		discovery:  discovery.New("", nil, "", "", "9428", 5*time.Second),
	}
}

func TestManifest_ReturnsInstance(t *testing.T) {
	s := newMinimalStorage()
	if s.Manifest() == nil {
		t.Fatal("Manifest() should not be nil")
	}
}

func TestLabelIndex_ReturnsInstance(t *testing.T) {
	s := newMinimalStorage()
	if s.LabelIndex() == nil {
		t.Fatal("LabelIndex() should not be nil")
	}
}

func TestSchemaRegistry_ReturnsInstance(t *testing.T) {
	s := newMinimalStorage()
	if s.SchemaRegistry() == nil {
		t.Fatal("SchemaRegistry() should not be nil")
	}
}

func TestSelfAZ_DefaultEmpty(t *testing.T) {
	s := newMinimalStorage()
	if s.SelfAZ() != "" {
		t.Errorf("SelfAZ() = %q, want empty", s.SelfAZ())
	}
}

func TestSetSelfAZ_Updates(t *testing.T) {
	s := newMinimalStorage()
	s.SetSelfAZ("us-east-1a")
	if s.SelfAZ() != "us-east-1a" {
		t.Errorf("SelfAZ() = %q, want us-east-1a", s.SelfAZ())
	}
}

func TestSetSelfAZ_NilPeerHandler_NoPanic(t *testing.T) {
	s := newMinimalStorage()
	s.peerHandler = nil
	s.SetSelfAZ("eu-west-1b")
	if s.SelfAZ() != "eu-west-1b" {
		t.Errorf("SelfAZ() = %q, want eu-west-1b", s.SelfAZ())
	}
}

func TestClearCaches_NoPanic(t *testing.T) {
	s := newMinimalStorage()
	s.memCache.Put("key1", []byte("data1"))
	s.ClearCaches()
	if _, ok := s.memCache.Get("key1"); ok {
		t.Error("cache should be cleared after ClearCaches()")
	}
}

func TestClearCaches_NilDiskCache_NoPanic(t *testing.T) {
	s := newMinimalStorage()
	s.diskCache = nil
	s.ClearCaches()
}

func TestMemCacheStats_ReturnsStats(t *testing.T) {
	s := newMinimalStorage()
	s.memCache.Put("k", []byte("v"))
	stats := s.MemCacheStats()
	if stats.Entries == 0 {
		t.Error("expected non-zero entries in cache stats after Put")
	}
}

func TestDiskCacheStats_NilDiskCache_ReturnsNil(t *testing.T) {
	s := newMinimalStorage()
	s.diskCache = nil
	if s.DiskCacheStats() != nil {
		t.Error("DiskCacheStats() should be nil when diskCache is nil")
	}
}

func TestWriter_NilByDefault(t *testing.T) {
	s := newMinimalStorage()
	if s.Writer() != nil {
		t.Error("Writer() should be nil before StartWriter()")
	}
}
