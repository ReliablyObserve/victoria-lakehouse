package parquets3

// Mirror of internal/storage/parquets3/resourcebound_wiring_test.go
// in the logs module. Verifies that constructing the full bound set
// with the traces module's config defaults populates every per-surface
// request/limit gauge — operators see the same K8s-style triple in
// dashboards regardless of which Lakehouse binary they query.

import (
	"testing"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// TestNewResourceBoundSet_DefaultsPopulated (traces) — mirror.
func TestNewResourceBoundSet_DefaultsPopulated(t *testing.T) {
	cfg := config.Default()
	cfg.Mode = config.ModeTraces

	set := newResourceBoundSet(cfg)
	if set == nil {
		t.Fatal("newResourceBoundSet returned nil")
	}

	cases := []struct {
		name     string
		req, lim int64
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
