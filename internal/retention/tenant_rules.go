package retention

import (
	"strconv"
)

// TenantRetentionEntry is the minimal shape needed to synthesize a
// per-tenant retention rule. Defined here to keep the retention
// package leaf-level and avoid an import cycle with internal/tenant.
type TenantRetentionEntry struct {
	AccountID uint32
	ProjectID uint32
	Keep      string // duration string ("168h0m0s", "7d", etc.)
}

// SynthesizeRules turns each TenantRetentionEntry into a match rule
// against the file's `account_id` / `project_id` labels (the labels
// the writer now embeds on every Parquet manifest entry, as of the
// Phase 1 + tenant-labels work). Returns nil when no entries carry a
// keep duration.
//
// Callers append the synthesized rules to the global cfg.Retention.Rules
// before constructing the Manager. The match-rule engine already
// handles multi-rule resolution (longest match wins), so explicit
// tenant rules sit alongside any global rules without special casing.
func SynthesizeRules(entries []TenantRetentionEntry) []Rule {
	if len(entries) == 0 {
		return nil
	}
	out := make([]Rule, 0, len(entries))
	for _, e := range entries {
		if e.Keep == "" {
			continue
		}
		out = append(out, Rule{
			Match: map[string]string{
				"account_id": strconv.FormatUint(uint64(e.AccountID), 10),
				"project_id": strconv.FormatUint(uint64(e.ProjectID), 10),
			},
			Keep: e.Keep,
		})
	}
	return out
}
