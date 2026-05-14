package config

import (
	"testing"
)

func TestDefaultConfig_AZDefaults(t *testing.T) {
	cfg := Default()

	if !cfg.Peer.AZAware {
		t.Error("AZAware should default to true")
	}
	if !cfg.Peer.CrossAZFallback {
		t.Error("CrossAZFallback should default to true")
	}
	if cfg.Peer.AZEnvVar != "LAKEHOUSE_AZ" {
		t.Errorf("AZEnvVar should default to LAKEHOUSE_AZ, got %q", cfg.Peer.AZEnvVar)
	}
}

func TestDefaultConfig_BufferBridgeAZDefaults(t *testing.T) {
	cfg := Default()

	if !cfg.Select.AZAware {
		t.Error("Select.AZAware should default to true")
	}
	if !cfg.Select.CrossAZFallback {
		t.Error("Select.CrossAZFallback should default to true")
	}
}

func TestDefaultConfig_AZMode(t *testing.T) {
	cfg := Default()

	if cfg.Peer.AZMode != "preferred" {
		t.Errorf("AZMode should default to preferred, got %q", cfg.Peer.AZMode)
	}
	if cfg.Peer.AZMinPeersPerAZ != 2 {
		t.Errorf("AZMinPeersPerAZ should default to 2, got %d", cfg.Peer.AZMinPeersPerAZ)
	}
}

func TestValidate_AZMode(t *testing.T) {
	cfg := Default()
	cfg.Mode = "logs"
	cfg.S3.Bucket = "test"

	cfg.Peer.AZMode = "invalid"
	if err := cfg.Validate(); err == nil {
		t.Error("expected validation error for invalid AZMode")
	}

	cfg.Peer.AZMode = "strict"
	if err := cfg.Validate(); err != nil {
		t.Errorf("strict should be valid: %v", err)
	}

	cfg.Peer.AZMode = "preferred"
	if err := cfg.Validate(); err != nil {
		t.Errorf("preferred should be valid: %v", err)
	}
}
