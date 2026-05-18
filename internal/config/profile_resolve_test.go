package config

import "testing"

func TestResolveEffectiveProfile_GlobalOnly(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Role = RoleInsert
	cfg.Profile = ProfileMaxPerformance

	got := cfg.ResolveEffectiveProfile()
	if got != ProfileMaxPerformance {
		t.Errorf("global profile = %q, want max-performance", got)
	}
}

func TestResolveEffectiveProfile_PerSignalOverridesGlobal(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Role = RoleInsert
	cfg.Profile = ProfileBalanced
	cfg.Logs.Profile = ProfileMaxDurability

	got := cfg.ResolveEffectiveProfile()
	if got != ProfileMaxDurability {
		t.Errorf("per-signal should override global: got %q, want max-durability", got)
	}
}

func TestResolveEffectiveProfile_PerRoleOverridesPerSignal(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Role = RoleInsert
	cfg.Profile = ProfileBalanced
	cfg.Logs.Profile = ProfileMaxDurability
	cfg.Logs.Insert.Profile = ProfileMaxPerformance

	got := cfg.ResolveEffectiveProfile()
	if got != ProfileMaxPerformance {
		t.Errorf("per-role should override per-signal: got %q, want max-performance", got)
	}
}

func TestResolveEffectiveProfile_PerRoleSelectPath(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Role = RoleSelect
	cfg.Profile = ProfileBalanced
	cfg.Logs.Profile = ProfileMaxDurability
	cfg.Logs.Insert.Profile = ProfileMaxPerformance
	cfg.Logs.Select.Profile = ProfileMaxCostSavings

	got := cfg.ResolveEffectiveProfile()
	if got != ProfileMaxCostSavings {
		t.Errorf("select role should use select profile: got %q, want max-cost-savings", got)
	}
}

func TestResolveEffectiveProfile_RoleAllIgnoresPerRole(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Role = RoleAll
	cfg.Profile = ProfileBalanced
	cfg.Logs.Profile = ProfileMaxDurability
	cfg.Logs.Insert.Profile = ProfileMaxPerformance

	got := cfg.ResolveEffectiveProfile()
	if got != ProfileMaxDurability {
		t.Errorf("role=all should use per-signal (not per-role): got %q, want max-durability", got)
	}
}

func TestResolveEffectiveProfile_TracesMode(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeTraces
	cfg.Role = RoleInsert
	cfg.Profile = ProfileBalanced
	cfg.Traces.Profile = ProfileDev

	got := cfg.ResolveEffectiveProfile()
	if got != ProfileDev {
		t.Errorf("traces per-signal: got %q, want dev", got)
	}
}

func TestResolveEffectiveProfile_TracesPerRole(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeTraces
	cfg.Role = RoleSelect
	cfg.Profile = ProfileBalanced
	cfg.Traces.Profile = ProfileDev
	cfg.Traces.Select.Profile = ProfileMaxCostSavings

	got := cfg.ResolveEffectiveProfile()
	if got != ProfileMaxCostSavings {
		t.Errorf("traces per-role select: got %q, want max-cost-savings", got)
	}
}

func TestResolveEffectiveProfile_EmptyDefaultsToBalanced(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Role = RoleInsert

	got := cfg.ResolveEffectiveProfile()
	if got != ProfileBalanced {
		t.Errorf("empty profiles should default to balanced: got %q", got)
	}
}

func TestResolveEffectiveProfile_WrongModeIgnored(t *testing.T) {
	cfg := Default()
	cfg.Mode = ModeLogs
	cfg.Role = RoleInsert
	cfg.Profile = ProfileBalanced
	cfg.Traces.Profile = ProfileMaxPerformance

	got := cfg.ResolveEffectiveProfile()
	if got != ProfileBalanced {
		t.Errorf("traces profile should be ignored in logs mode: got %q, want balanced", got)
	}
}
