package config

import (
	"testing"
)

func FuzzValidateAZMode(f *testing.F) {
	f.Add("preferred")
	f.Add("strict")
	f.Add("")
	f.Add("invalid")
	f.Add("PREFERRED")
	f.Add("Strict")
	f.Add("pref")
	f.Add("s")
	f.Add("preferred ")
	f.Add(" strict")
	f.Add("preferred\n")

	f.Fuzz(func(t *testing.T, mode string) {
		cfg := Default()
		cfg.Mode = "logs"
		cfg.S3.Bucket = "test"
		cfg.Peer.AZMode = mode

		err := cfg.Validate()

		validModes := map[string]bool{"preferred": true, "strict": true, "": true}
		if validModes[mode] {
			if err != nil {
				t.Errorf("valid mode %q should pass validation: %v", mode, err)
			}
		} else {
			if err == nil {
				t.Errorf("invalid mode %q should fail validation", mode)
			}
		}
	})
}

func TestMergeConfig_AZFields_Comprehensive(t *testing.T) {
	tests := []struct {
		name        string
		baseAZAware bool
		baseMode    string
		overAZAware bool
		overMode    string
		wantAZAware bool
		wantMode    string
	}{
		{"default_base_no_overlay", true, "preferred", false, "", true, "preferred"},
		{"overlay_strict", true, "preferred", false, "strict", true, "strict"},
		{"overlay_mode_only", true, "preferred", false, "strict", true, "strict"},
		{"overlay_azaware_true", false, "preferred", true, "", true, "preferred"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := Default()
			base.Peer.AZAware = tc.baseAZAware
			base.Peer.AZMode = tc.baseMode

			overlay := &Config{}
			overlay.Peer.AZAware = tc.overAZAware
			overlay.Peer.AZMode = tc.overMode

			merged := mergeConfig(base, overlay)

			if merged.Peer.AZAware != tc.wantAZAware {
				t.Errorf("AZAware: want %v, got %v", tc.wantAZAware, merged.Peer.AZAware)
			}
			if merged.Peer.AZMode != tc.wantMode {
				t.Errorf("AZMode: want %q, got %q", tc.wantMode, merged.Peer.AZMode)
			}
		})
	}
}

func TestMergeConfig_AZEnvVar_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		baseVar string
		overVar string
		wantVar string
	}{
		{"default_preserved", "LAKEHOUSE_AZ", "", "LAKEHOUSE_AZ"},
		{"overlay_wins", "LAKEHOUSE_AZ", "CUSTOM_AZ", "CUSTOM_AZ"},
		{"short_name", "LAKEHOUSE_AZ", "A", "A"},
		{"underscores", "LAKEHOUSE_AZ", "MY_CUSTOM_AZ_VAR", "MY_CUSTOM_AZ_VAR"},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := Default()
			base.Peer.AZEnvVar = tc.baseVar

			overlay := &Config{}
			overlay.Peer.AZEnvVar = tc.overVar

			merged := mergeConfig(base, overlay)
			if merged.Peer.AZEnvVar != tc.wantVar {
				t.Errorf("want %q, got %q", tc.wantVar, merged.Peer.AZEnvVar)
			}
		})
	}
}

func TestMergeConfig_AZMinPeersPerAZ_EdgeCases(t *testing.T) {
	tests := []struct {
		name    string
		baseMin int
		overMin int
		wantMin int
	}{
		{"default_preserved", 2, 0, 2},
		{"overlay_wins", 2, 5, 5},
		{"overlay_one", 2, 1, 1},
		{"large_value", 2, 100, 100},
	}

	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			base := Default()
			base.Peer.AZMinPeersPerAZ = tc.baseMin

			overlay := &Config{}
			overlay.Peer.AZMinPeersPerAZ = tc.overMin

			merged := mergeConfig(base, overlay)
			if merged.Peer.AZMinPeersPerAZ != tc.wantMin {
				t.Errorf("want %d, got %d", tc.wantMin, merged.Peer.AZMinPeersPerAZ)
			}
		})
	}
}

func TestDefaultConfig_AllAZFieldsPresent(t *testing.T) {
	cfg := Default()

	// Peer AZ fields
	if cfg.Peer.AZAware != true {
		t.Error("Peer.AZAware default should be true")
	}
	if cfg.Peer.AZMode != "preferred" {
		t.Errorf("Peer.AZMode default should be 'preferred', got %q", cfg.Peer.AZMode)
	}
	if cfg.Peer.CrossAZFallback != true {
		t.Error("Peer.CrossAZFallback default should be true")
	}
	if cfg.Peer.AZEnvVar != "LAKEHOUSE_AZ" {
		t.Errorf("Peer.AZEnvVar default should be 'LAKEHOUSE_AZ', got %q", cfg.Peer.AZEnvVar)
	}
	if cfg.Peer.AZMinPeersPerAZ != 2 {
		t.Errorf("Peer.AZMinPeersPerAZ default should be 2, got %d", cfg.Peer.AZMinPeersPerAZ)
	}

	// Select AZ fields
	if cfg.Select.AZAware != true {
		t.Error("Select.AZAware default should be true")
	}
	if cfg.Select.CrossAZFallback != true {
		t.Error("Select.CrossAZFallback default should be true")
	}
}

func TestValidate_AZMode_AllCases(t *testing.T) {
	validCases := []string{"preferred", "strict", ""}
	invalidCases := []string{"PREFERRED", "Strict", "pref", "s", "auto", "none", " ", "preferred "}

	for _, mode := range validCases {
		t.Run("valid_"+mode, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = "logs"
			cfg.S3.Bucket = "test"
			cfg.Peer.AZMode = mode
			if err := cfg.Validate(); err != nil {
				t.Errorf("mode %q should be valid: %v", mode, err)
			}
		})
	}

	for _, mode := range invalidCases {
		t.Run("invalid_"+mode, func(t *testing.T) {
			cfg := Default()
			cfg.Mode = "logs"
			cfg.S3.Bucket = "test"
			cfg.Peer.AZMode = mode
			if err := cfg.Validate(); err == nil {
				t.Errorf("mode %q should be invalid", mode)
			}
		})
	}
}
