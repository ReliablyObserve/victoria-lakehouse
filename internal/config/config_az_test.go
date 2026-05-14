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
