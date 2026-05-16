package config

import (
	"fmt"
	"strings"
	"time"
)

// Profile represents a named configuration preset.
type Profile string

const (
	ProfileBalanced       Profile = "balanced"
	ProfileMaxPerformance Profile = "max-performance"
	ProfileMaxDurability  Profile = "max-durability"
	ProfileMaxCostSavings Profile = "max-cost-savings"
	ProfileDev            Profile = "dev"
)

// ValidProfiles returns all valid profile names.
func ValidProfiles() []Profile {
	return []Profile{
		ProfileBalanced,
		ProfileMaxPerformance,
		ProfileMaxDurability,
		ProfileMaxCostSavings,
		ProfileDev,
	}
}

// IsValidProfile returns true if p is a recognized profile name.
func IsValidProfile(p string) bool {
	for _, valid := range ValidProfiles() {
		if Profile(p) == valid {
			return true
		}
	}
	return false
}

// ValidProfileNames returns a formatted string of valid profile names for error messages.
func ValidProfileNames() string {
	names := make([]string, len(ValidProfiles()))
	for i, p := range ValidProfiles() {
		names[i] = string(p)
	}
	return strings.Join(names, ", ")
}

// ProfileConfig returns a complete Config for the given profile.
// Currently sets INSERT PATH settings (the most impactful path).
// Other settings use Default() values.
func ProfileConfig(p Profile) *Config {
	cfg := Default()

	switch p {
	case ProfileBalanced:
		// balanced is identical to Default() — no changes needed
		return cfg

	case ProfileMaxPerformance:
		cfg.Insert.FlushInterval = 5 * time.Second
		cfg.Insert.WALEnabled = false
		cfg.Insert.WALMaxBytes = "512MB"
		cfg.Insert.CompressionLevel = 3
		cfg.Insert.MaxBufferRows = 100000
		cfg.Insert.MaxBufferBytes = "512MB"
		cfg.Insert.TargetFileSize = "64MB"

	case ProfileMaxDurability:
		cfg.Insert.FlushInterval = 10 * time.Second
		cfg.Insert.WALEnabled = true
		cfg.Insert.WALMaxBytes = "1GB"
		cfg.Insert.CompressionLevel = 7
		cfg.Insert.MaxBufferRows = 50000
		cfg.Insert.MaxBufferBytes = "256MB"
		cfg.Insert.TargetFileSize = "128MB"

	case ProfileMaxCostSavings:
		cfg.Insert.FlushInterval = 30 * time.Second
		cfg.Insert.WALEnabled = false
		cfg.Insert.WALMaxBytes = "256MB"
		cfg.Insert.CompressionLevel = 11
		cfg.Insert.MaxBufferRows = 25000
		cfg.Insert.MaxBufferBytes = "128MB"
		cfg.Insert.TargetFileSize = "256MB"

	case ProfileDev:
		cfg.Insert.FlushInterval = 1 * time.Second
		cfg.Insert.WALEnabled = false
		cfg.Insert.WALMaxBytes = "32MB"
		cfg.Insert.CompressionLevel = 1
		cfg.Insert.MaxBufferRows = 1000
		cfg.Insert.MaxBufferBytes = "32MB"
		cfg.Insert.TargetFileSize = "8MB"
		cfg.S3.ForcePathStyle = true

	default:
		panic(fmt.Sprintf("unknown profile %q", p))
	}

	return cfg
}
