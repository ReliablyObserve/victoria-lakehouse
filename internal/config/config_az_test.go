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

func TestValidate_AZModeEmpty(t *testing.T) {
	cfg := Default()
	cfg.Mode = "logs"
	cfg.S3.Bucket = "test"
	cfg.Peer.AZMode = ""
	if err := cfg.Validate(); err != nil {
		t.Errorf("empty AZMode should be valid: %v", err)
	}
}

func TestMergeConfig_PeerAZFields(t *testing.T) {
	base := Default()

	overlay := &Config{}
	overlay.Peer.AZMode = "strict"
	overlay.Peer.AZEnvVar = "CUSTOM_AZ"
	overlay.Peer.AZMinPeersPerAZ = 5

	merged := mergeConfig(base, overlay)

	if merged.Peer.AZMode != "strict" {
		t.Errorf("AZMode should be overridden to strict, got %q", merged.Peer.AZMode)
	}
	if merged.Peer.AZEnvVar != "CUSTOM_AZ" {
		t.Errorf("AZEnvVar should be overridden, got %q", merged.Peer.AZEnvVar)
	}
	if merged.Peer.AZMinPeersPerAZ != 5 {
		t.Errorf("AZMinPeersPerAZ should be overridden, got %d", merged.Peer.AZMinPeersPerAZ)
	}
}

func TestMergeConfig_SelectAZFields(t *testing.T) {
	base := Default()

	overlay := &Config{}
	overlay.Select.AZAware = true
	overlay.Select.CrossAZFallback = true

	merged := mergeConfig(base, overlay)

	if !merged.Select.AZAware {
		t.Error("Select.AZAware should be true after merge")
	}
	if !merged.Select.CrossAZFallback {
		t.Error("Select.CrossAZFallback should be true after merge")
	}
}

func TestMergeConfig_PeerAZDefaults_NotOverridden(t *testing.T) {
	base := Default()

	overlay := &Config{}

	merged := mergeConfig(base, overlay)

	if merged.Peer.AZMode != "preferred" {
		t.Errorf("AZMode default should be preserved, got %q", merged.Peer.AZMode)
	}
	if merged.Peer.AZEnvVar != "LAKEHOUSE_AZ" {
		t.Errorf("AZEnvVar default should be preserved, got %q", merged.Peer.AZEnvVar)
	}
	if merged.Peer.AZMinPeersPerAZ != 2 {
		t.Errorf("AZMinPeersPerAZ default should be preserved, got %d", merged.Peer.AZMinPeersPerAZ)
	}
}
