package tenant

import (
	"fmt"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
)

// EffectiveConfig is the per-tenant policy snapshot produced by merging
// global defaults with a TenantOverride entry. Fields are duration- /
// int-typed (not raw strings) so call sites avoid re-parsing on every
// lookup. A nil EffectiveConfig means "no per-tenant overrides — use
// global defaults directly".
//
// Lookup is read-mostly: resolved entries are cached in a sync.Map and
// the alias-sync loop refreshes the cache when new aliases register.
type EffectiveConfig struct {
	// Tenant identity that produced this entry.
	AccountID uint32
	ProjectID uint32

	// Retention.Keep = 0 means "inherit global default".
	Retention time.Duration

	// Cardinality limits; 0 = inherit.
	MaxFields  int
	MaxStreams int

	// Ingest rate caps; 0 = no per-tenant limit.
	MaxBytesPerSec int64
	MaxRowsPerSec  int64

	// Lifecycle override (nil = inherit). Stored as the raw config
	// rules; the storage-class detector parses TransitionDays directly.
	Lifecycle []config.LifecycleRuleConfig
}

// PolicyRegistry resolves per-tenant overrides against the alias map.
// Created once at startup with the global TenantConfig.Overrides and
// the TenantResolver; queries are cheap (sync.Map cache keyed by
// account:project).
type PolicyRegistry struct {
	resolver  *TenantResolver
	rawByID   map[TenantID]config.TenantOverride
	rawByOrg  map[string]config.TenantOverride // unresolved alias-keyed entries
	cache     sync.Map                         // TenantID -> *EffectiveConfig
	cacheMu   sync.Mutex
	mu        sync.RWMutex // guards rawByID rebuild on alias-sync refresh
}

// NewPolicyRegistry wires overrides keyed by either "<account>:<project>"
// or an OrgID alias. Numeric keys resolve immediately; alias keys resolve
// via the resolver and re-resolve on Refresh() so late-registered
// aliases pick up their override without restart.
func NewPolicyRegistry(overrides map[string]config.TenantOverride, resolver *TenantResolver) (*PolicyRegistry, error) {
	pr := &PolicyRegistry{
		resolver: resolver,
		rawByID:  make(map[TenantID]config.TenantOverride, len(overrides)),
		rawByOrg: make(map[string]config.TenantOverride),
	}
	for key, ov := range overrides {
		if tid, ok := parseAccountProject(key); ok {
			pr.rawByID[tid] = ov
			continue
		}
		if resolver != nil {
			if tid, ok := resolver.Resolve(key); ok {
				pr.rawByID[tid] = ov
				continue
			}
		}
		// Alias not yet registered — keep for later refresh.
		pr.rawByOrg[key] = ov
	}
	if err := pr.validate(); err != nil {
		return nil, err
	}
	return pr, nil
}

// Refresh re-resolves alias-keyed overrides against the (possibly
// updated) resolver. Cheap to call on the alias-sync tick — no-op when
// nothing pending.
func (pr *PolicyRegistry) Refresh() {
	if pr == nil || pr.resolver == nil {
		return
	}
	pr.mu.Lock()
	defer pr.mu.Unlock()
	for orgID, ov := range pr.rawByOrg {
		if tid, ok := pr.resolver.Resolve(orgID); ok {
			pr.rawByID[tid] = ov
			delete(pr.rawByOrg, orgID)
			pr.cache.Delete(tid) // force re-derivation
		}
	}
}

// For returns the effective config for a tenant. Returns nil when no
// override is configured (caller should fall back to global defaults).
func (pr *PolicyRegistry) For(accountID, projectID uint32) *EffectiveConfig {
	if pr == nil {
		return nil
	}
	tid := TenantID{AccountID: accountID, ProjectID: projectID}
	if cached, ok := pr.cache.Load(tid); ok {
		return cached.(*EffectiveConfig)
	}

	pr.mu.RLock()
	raw, ok := pr.rawByID[tid]
	pr.mu.RUnlock()
	if !ok {
		return nil
	}

	pr.cacheMu.Lock()
	defer pr.cacheMu.Unlock()
	if cached, ok := pr.cache.Load(tid); ok {
		return cached.(*EffectiveConfig)
	}

	eff := &EffectiveConfig{
		AccountID:      accountID,
		ProjectID:      projectID,
		MaxFields:      raw.Cardinality.MaxFields,
		MaxStreams:     raw.Cardinality.MaxStreams,
		MaxBytesPerSec: raw.Ingest.MaxBytesPerSec,
		MaxRowsPerSec:  raw.Ingest.MaxRowsPerSec,
		Lifecycle:      raw.Lifecycle,
	}
	if raw.Retention.Keep != "" {
		if d, err := ParseDayDuration(raw.Retention.Keep); err == nil {
			eff.Retention = d
		}
	}
	pr.cache.Store(tid, eff)
	return eff
}

// RetentionEntries adapts the registry to retention.TenantPolicy
// without making the retention package import internal/tenant
// (avoids a cycle). Only entries with a non-zero retention duration
// are returned — callers can len()-check before iterating.
type RetentionEntry struct {
	AccountID uint32
	ProjectID uint32
	Retention string
}

// LifecycleEntry adapts a tenant override's lifecycle rules to the
// shape delete.BuildTenantRules expects. Returned slices share length:
// TransitionDays[i] / Classes[i] form the i-th rule.
type LifecycleEntry struct {
	AccountID      uint32
	ProjectID      uint32
	TransitionDays []int
	Classes        []string
}

// LifecycleEntries returns every override carrying a non-empty
// lifecycle rule set. Callers convert to delete.TenantLifecycleOverride
// and install on the StorageClassDetector.
func (pr *PolicyRegistry) LifecycleEntries() []LifecycleEntry {
	if pr == nil {
		return nil
	}
	all := pr.All()
	out := make([]LifecycleEntry, 0, len(all))
	for _, e := range all {
		if len(e.Lifecycle) == 0 {
			continue
		}
		days := make([]int, 0, len(e.Lifecycle))
		classes := make([]string, 0, len(e.Lifecycle))
		for _, r := range e.Lifecycle {
			days = append(days, r.TransitionDays)
			classes = append(classes, r.StorageClass)
		}
		out = append(out, LifecycleEntry{
			AccountID:      e.AccountID,
			ProjectID:      e.ProjectID,
			TransitionDays: days,
			Classes:        classes,
		})
	}
	return out
}

// RetentionEntries returns every override carrying a retention
// duration, formatted as a Go duration string so it round-trips
// through retention.parseDuration unchanged.
func (pr *PolicyRegistry) RetentionEntries() []RetentionEntry {
	if pr == nil {
		return nil
	}
	all := pr.All()
	out := make([]RetentionEntry, 0, len(all))
	for _, e := range all {
		if e.Retention <= 0 {
			continue
		}
		out = append(out, RetentionEntry{
			AccountID: e.AccountID,
			ProjectID: e.ProjectID,
			Retention: e.Retention.String(),
		})
	}
	return out
}

// All returns every resolved EffectiveConfig the registry currently
// holds, sorted by (AccountID, ProjectID). Callers walk this to
// materialize the overrides into downstream subsystems (retention
// rules, cardinality limits, rate caps) at startup or on refresh.
func (pr *PolicyRegistry) All() []*EffectiveConfig {
	if pr == nil {
		return nil
	}
	pr.mu.RLock()
	tids := make([]TenantID, 0, len(pr.rawByID))
	for tid := range pr.rawByID {
		tids = append(tids, tid)
	}
	pr.mu.RUnlock()

	sort.Slice(tids, func(i, j int) bool {
		if tids[i].AccountID != tids[j].AccountID {
			return tids[i].AccountID < tids[j].AccountID
		}
		return tids[i].ProjectID < tids[j].ProjectID
	})

	out := make([]*EffectiveConfig, 0, len(tids))
	for _, tid := range tids {
		if eff := pr.For(tid.AccountID, tid.ProjectID); eff != nil {
			out = append(out, eff)
		}
	}
	return out
}

// PendingAliases lists override keys that haven't been resolved yet —
// useful for surface-level observability ("waiting for alias to
// register") in /healthz or startup logs.
func (pr *PolicyRegistry) PendingAliases() []string {
	if pr == nil {
		return nil
	}
	pr.mu.RLock()
	defer pr.mu.RUnlock()
	out := make([]string, 0, len(pr.rawByOrg))
	for k := range pr.rawByOrg {
		out = append(out, k)
	}
	return out
}

func (pr *PolicyRegistry) validate() error {
	for tid, ov := range pr.rawByID {
		if ov.Retention.Keep != "" {
			if _, err := ParseDayDuration(ov.Retention.Keep); err != nil {
				return fmt.Errorf("tenant %d:%d retention.keep %q: %w",
					tid.AccountID, tid.ProjectID, ov.Retention.Keep, err)
			}
		}
		if ov.Cardinality.MaxFields < 0 || ov.Cardinality.MaxStreams < 0 {
			return fmt.Errorf("tenant %d:%d cardinality: negative limits are invalid",
				tid.AccountID, tid.ProjectID)
		}
		if ov.Ingest.MaxBytesPerSec < 0 || ov.Ingest.MaxRowsPerSec < 0 {
			return fmt.Errorf("tenant %d:%d ingest: negative rate caps are invalid",
				tid.AccountID, tid.ProjectID)
		}
	}
	return nil
}

// parseAccountProject decodes an "account:project" override key. Returns
// false for any non-numeric form so callers can fall back to alias-map
// resolution.
func parseAccountProject(key string) (TenantID, bool) {
	a, p, ok := strings.Cut(key, ":")
	if !ok {
		return TenantID{}, false
	}
	acc, err := strconv.ParseUint(strings.TrimSpace(a), 10, 32)
	if err != nil {
		return TenantID{}, false
	}
	proj, err := strconv.ParseUint(strings.TrimSpace(p), 10, 32)
	if err != nil {
		return TenantID{}, false
	}
	return TenantID{AccountID: uint32(acc), ProjectID: uint32(proj)}, true
}

// ParseDayDuration parses retention-style durations: Go duration ("7d"
// expanded to 168h is supported, plus plain "30d") or standard
// time.ParseDuration syntax.
func ParseDayDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	if strings.HasSuffix(s, "d") {
		n, err := strconv.Atoi(s[:len(s)-1])
		if err != nil {
			return 0, fmt.Errorf("bad day count %q", s)
		}
		return time.Duration(n) * 24 * time.Hour, nil
	}
	return 0, fmt.Errorf("unrecognized duration %q", s)
}
