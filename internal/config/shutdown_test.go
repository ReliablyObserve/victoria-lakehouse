package config

import (
	"testing"
	"time"
)

func TestShutdownConfig_Defaults(t *testing.T) {
	cfg := Default()
	if cfg.Shutdown.Delay != 5*time.Second {
		t.Errorf("shutdown delay = %v, want 5s", cfg.Shutdown.Delay)
	}
	if cfg.Shutdown.FlushTimeout != 30*time.Second {
		t.Errorf("flush timeout = %v, want 30s", cfg.Shutdown.FlushTimeout)
	}
	if cfg.Shutdown.PersistTimeout != 10*time.Second {
		t.Errorf("persist timeout = %v, want 10s", cfg.Shutdown.PersistTimeout)
	}
	if cfg.Shutdown.ReleaseTimeout != 5*time.Second {
		t.Errorf("release timeout = %v, want 5s", cfg.Shutdown.ReleaseTimeout)
	}
}

func TestStartupConfig_NewDefaults(t *testing.T) {
	cfg := Default()
	if cfg.Startup.PeerSyncTimeout != 30*time.Second {
		t.Errorf("peer sync timeout = %v, want 30s", cfg.Startup.PeerSyncTimeout)
	}
	if !cfg.Startup.RequireManifestSync {
		t.Error("require_manifest_sync should default to true")
	}
	if cfg.Startup.StaleThreshold != 1*time.Hour {
		t.Errorf("stale threshold = %v, want 1h", cfg.Startup.StaleThreshold)
	}
	if !cfg.Startup.WALReconciliation {
		t.Error("wal_reconciliation should default to true")
	}
	if !cfg.Startup.CacheRevalidation {
		t.Error("cache_revalidation should default to true")
	}
	if cfg.Startup.MaxResyncTime != 10*time.Minute {
		t.Errorf("max resync time = %v, want 10m", cfg.Startup.MaxResyncTime)
	}
}

func TestDiscoveryConfig_RingStabilize(t *testing.T) {
	cfg := Default()
	if cfg.Discovery.RingStabilizeDuration != 60*time.Second {
		t.Errorf("ring stabilize duration = %v, want 60s", cfg.Discovery.RingStabilizeDuration)
	}
	if !cfg.Discovery.RingChangeNotify {
		t.Error("ring_change_notify should default to true")
	}
}

func TestValidateShutdown_Valid(t *testing.T) {
	cfg := Default()
	// Default total = 5+30+10+5 = 50s, terminationGracePeriod=60s, margin=5s -> 50 <= 55 OK
	if err := cfg.ValidateShutdown(60 * time.Second); err != nil {
		t.Errorf("expected valid, got: %v", err)
	}
}

func TestValidateShutdown_ExceedsBudget(t *testing.T) {
	cfg := Default()
	cfg.Shutdown.FlushTimeout = 60 * time.Second // total now 80s
	err := cfg.ValidateShutdown(60 * time.Second)
	if err == nil {
		t.Error("expected error when shutdown phases exceed budget")
	}
}
