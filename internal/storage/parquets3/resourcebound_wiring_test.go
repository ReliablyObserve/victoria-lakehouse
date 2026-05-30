package parquets3

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// TestNewResourceBoundSet_DefaultsPopulated verifies that constructing
// the full bound set with an empty config populates every per-surface
// request/limit pair with sane built-in defaults — operators that
// haven't explicitly configured any of the new triple flags still see
// non-zero gauges in dashboards.
func TestNewResourceBoundSet_DefaultsPopulated(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs

	set := newResourceBoundSet(cfg)
	if set == nil {
		t.Fatal("newResourceBoundSet returned nil")
	}

	type expect struct {
		name        string
		bound       interface{ Config() Cfg }
		minRequest  int64
		minLimit    int64
		needsCount  bool // S3 + file workers use LimitCount
		countActive bool
	}
	_ = expect{} // shape kept for documentation

	// All five bounds must have non-zero limit so operator dashboards
	// show the contract. Request may equal Limit (flat) when only the
	// legacy alias is set; both must be > 0.
	cases := []struct {
		name      string
		req, lim  int64
	}{
		{"S3Downloads", set.S3Downloads.Config().Request, set.S3Downloads.Config().Limit},
		{"FileWorkers", set.FileWorkers.Config().Request, set.FileWorkers.Config().Limit},
		{"CacheMemory", set.CacheMemory.Config().Request, set.CacheMemory.Config().Limit},
		{"SmartCacheDisk", set.SmartCacheDisk.Config().Request, set.SmartCacheDisk.Config().Limit},
		{"QueryMaxRows", set.QueryMaxRows.Config().Request, set.QueryMaxRows.Config().Limit},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			if c.lim <= 0 {
				t.Errorf("%s.Limit = %d, want > 0", c.name, c.lim)
			}
			if c.req <= 0 {
				t.Errorf("%s.Request = %d, want > 0", c.name, c.req)
			}
			if c.req > c.lim {
				t.Errorf("%s.Request (%d) > Limit (%d)", c.name, c.req, c.lim)
			}
		})
	}
}

// Cfg is a workaround for the test file living in the parquets3
// package — we can't import resourcebounds.Config without a cycle
// risk on certain builds, so we use the type alias here for
// documentation only. The actual type checked at compile time is
// resourcebounds.Config because Config() returns that.
type Cfg = struct {
	Request    int64
	Limit      int64
	LimitCount int
	Policy     int
}

// TestNewResourceBoundSet_S3DeprecatedAliasHonored verifies that
// setting only the legacy MaxConcurrentDownloads alias produces a
// bound with Limit=alias and Request=alias/4 (the operator-implicit
// K8s-style baseline applied when the new triple isn't set explicitly
// — operators see a non-trivial request reservation in dashboards
// without having to migrate immediately).
//
// The deprecation warning is emitted from newS3DownloadsBound to
// logger.Warnf (not asserted here — logger has no test sink wired
// in this package).
func TestNewResourceBoundSet_S3DeprecatedAliasHonored(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.MaxConcurrentDownloads = 32

	set := newResourceBoundSet(cfg)
	got := set.S3Downloads.Config()
	if got.Limit != 32 {
		t.Errorf("S3Downloads.Limit = %d, want 32 (alias honored)", got.Limit)
	}
	if got.Request != 8 {
		t.Errorf("S3Downloads.Request = %d, want 8 (alias=32 → request=alias/4 K8s heuristic)", got.Request)
	}
}

// TestNewResourceBoundSet_S3NewTripleTakesPrecedence verifies that
// the new triple wins over the deprecated alias when both are set —
// the operator has begun migration and the runtime should respect
// the new contract.
func TestNewResourceBoundSet_S3NewTripleTakesPrecedence(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeLogs
	cfg.S3.MaxConcurrentDownloads = 32 // legacy
	cfg.S3.ConcurrentDownloadsRequest = 8
	cfg.S3.ConcurrentDownloadsLimit = 64

	set := newResourceBoundSet(cfg)
	got := set.S3Downloads.Config()
	if got.Request != 8 {
		t.Errorf("S3Downloads.Request = %d, want 8 (new triple overrides legacy)", got.Request)
	}
	if got.Limit != 64 {
		t.Errorf("S3Downloads.Limit = %d, want 64 (new triple overrides legacy)", got.Limit)
	}
}
