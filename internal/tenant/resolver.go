package tenant

import (
	"fmt"
	"sync"
)

type TenantID struct {
	AccountID uint32
	ProjectID uint32
}

type MetricsFormat int

const (
	MetricsFormatID   MetricsFormat = iota // "42:3"
	MetricsFormatName                      // "prod-team-eu_staging"
	MetricsFormatBoth                      // both labels
)

func ParseMetricsFormat(s string) MetricsFormat {
	switch s {
	case "name":
		return MetricsFormatName
	case "both":
		return MetricsFormatBoth
	default:
		return MetricsFormatID
	}
}

type AliasEntry struct {
	OrgID     string `json:"org_id"`
	AccountID uint32 `json:"account_id"`
	ProjectID uint32 `json:"project_id"`
	Source    string `json:"source"`
}

type ResolverConfig struct {
	MetricsFormat MetricsFormat
	AutoRegister  bool
	OrgIDHeader   string
}

type TenantResolver struct {
	forward sync.Map
	reverse sync.Map
	config  ResolverConfig
	mu      sync.Mutex
}

func NewResolver(cfg ResolverConfig) *TenantResolver {
	if cfg.OrgIDHeader == "" {
		cfg.OrgIDHeader = "X-Scope-OrgID"
	}
	return &TenantResolver{config: cfg}
}

func reverseKey(accountID, projectID uint32) string {
	return fmt.Sprintf("%d:%d", accountID, projectID)
}

func (r *TenantResolver) AddAlias(orgID string, tid TenantID) error {
	if err := ValidateOrgID(orgID); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.forward.Store(orgID, tid)
	r.reverse.Store(reverseKey(tid.AccountID, tid.ProjectID), orgID)
	return nil
}

func (r *TenantResolver) RemoveAlias(orgID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.forward.LoadAndDelete(orgID); ok {
		tid := v.(TenantID)
		r.reverse.Delete(reverseKey(tid.AccountID, tid.ProjectID))
	}
}

func (r *TenantResolver) Resolve(orgID string) (TenantID, bool) {
	v, ok := r.forward.Load(orgID)
	if !ok {
		return TenantID{}, false
	}
	return v.(TenantID), true
}

func (r *TenantResolver) DisplayName(accountID, projectID uint32) string {
	v, ok := r.reverse.Load(reverseKey(accountID, projectID))
	if !ok {
		return fmt.Sprintf("%d:%d", accountID, projectID)
	}
	return v.(string)
}

func (r *TenantResolver) MetricLabel(accountID, projectID uint32) string {
	switch r.config.MetricsFormat {
	case MetricsFormatName:
		return r.DisplayName(accountID, projectID)
	default:
		return fmt.Sprintf("%d:%d", accountID, projectID)
	}
}

func (r *TenantResolver) HasAliases() bool {
	has := false
	r.forward.Range(func(_, _ any) bool {
		has = true
		return false
	})
	return has
}

func (r *TenantResolver) AllAliases() []AliasEntry {
	var entries []AliasEntry
	r.forward.Range(func(k, v any) bool {
		orgID := k.(string)
		tid := v.(TenantID)
		entries = append(entries, AliasEntry{
			OrgID:     orgID,
			AccountID: tid.AccountID,
			ProjectID: tid.ProjectID,
		})
		return true
	})
	return entries
}

func (r *TenantResolver) Config() ResolverConfig {
	return r.config
}
