package tenant

import (
	"context"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Alias sources. Config aliases are authoritative: they are re-applied on
// every start and win over a persisted entry that contradicts them.
const (
	SourceConfig = "config"
	SourceAPI    = "api"
	SourceAuto   = "auto"
	SourceSynced = "synced"
)

// NullAccountID is the AccountID an unknown string tenant resolves to on a
// read. No alias, auto-registration or write can ever use it, so it never
// holds data and every read path answers it like any tenant without data.
const NullAccountID uint32 = 4294967295

// NullTenant is the tenant an unknown OrgID resolves to on reads.
var NullTenant = TenantID{AccountID: NullAccountID}

// ErrAliasConflict is matched (errors.Is) by every refusal to map an OrgID or
// an AccountID/ProjectID pair that is already mapped to something else.
var ErrAliasConflict = errors.New("tenant alias conflict")

// AliasConflictError says which mapping an alias collided with.
type AliasConflictError struct {
	OrgID         string
	Requested     TenantID
	ExistingOrgID string
	Existing      TenantID
	Reason        string
}

func (e *AliasConflictError) Error() string {
	return fmt.Sprintf("tenant alias conflict: %q -> %d:%d: %s", e.OrgID, e.Requested.AccountID, e.Requested.ProjectID, e.Reason)
}

func (e *AliasConflictError) Is(target error) bool { return target == ErrAliasConflict }

// Allocator hands out an AccountID for an unknown OrgID on a write path and
// keeps the resolver's view of the shared registry fresh.
type Allocator interface {
	Allocate(ctx context.Context, orgID string) (TenantID, error)
	RefreshIfStale(ctx context.Context, minAge time.Duration)
}

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
	forward   sync.Map
	reverse   sync.Map
	sources   sync.Map // orgID -> source
	config    ResolverConfig
	mu        sync.Mutex
	allocator atomic.Pointer[Allocator]
}

// SetAllocator installs the allocator used by write-path auto-registration.
func (r *TenantResolver) SetAllocator(a Allocator) {
	if a == nil {
		r.allocator.Store(nil)
		return
	}
	r.allocator.Store(&a)
}

func (r *TenantResolver) getAllocator() Allocator {
	if p := r.allocator.Load(); p != nil {
		return *p
	}
	return nil
}

func NewResolver(cfg ResolverConfig) *TenantResolver {
	if cfg.OrgIDHeader == "" {
		cfg.OrgIDHeader = "X-Scope-OrgID"
	}
	r := &TenantResolver{config: cfg}
	if cfg.AutoRegister {
		// Process-local registry until the binary installs the S3-backed one.
		if _, err := NewRegistry(r, RegistryConfig{}); err != nil {
			panic(err) // the default range is valid
		}
	}
	return r
}

func reverseKey(accountID, projectID uint32) string {
	return fmt.Sprintf("%d:%d", accountID, projectID)
}

// AddAlias maps orgID to tid as an API alias. See AddAliasFrom.
func (r *TenantResolver) AddAlias(orgID string, tid TenantID) error {
	return r.AddAliasFrom(orgID, tid, SourceAPI)
}

// CheckAlias reports what AddAliasFrom would refuse, without changing anything.
func (r *TenantResolver) CheckAlias(orgID string, tid TenantID) error {
	if err := ValidateOrgID(orgID); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.checkLocked(orgID, tid)
}

func (r *TenantResolver) checkLocked(orgID string, tid TenantID) error {
	if tid.AccountID == NullAccountID {
		return &AliasConflictError{OrgID: orgID, Requested: tid, Reason: fmt.Sprintf("AccountID %d is reserved for unknown tenants", NullAccountID)}
	}
	if v, ok := r.forward.Load(orgID); ok {
		if cur := v.(TenantID); cur != tid {
			return &AliasConflictError{OrgID: orgID, Requested: tid, ExistingOrgID: orgID, Existing: cur,
				Reason: fmt.Sprintf("OrgID is already mapped to %d:%d", cur.AccountID, cur.ProjectID)}
		}
	}
	if v, ok := r.reverse.Load(reverseKey(tid.AccountID, tid.ProjectID)); ok {
		if other := v.(string); other != orgID {
			return &AliasConflictError{OrgID: orgID, Requested: tid, ExistingOrgID: other, Existing: tid,
				Reason: fmt.Sprintf("%d:%d already belongs to OrgID %q", tid.AccountID, tid.ProjectID, other)}
		}
	}
	return nil
}

// AddAliasFrom maps orgID to tid. It is idempotent for an identical mapping and
// refuses, with an error matching ErrAliasConflict, a mapping whose OrgID is
// already mapped to another ID or whose ID already belongs to another OrgID:
// the reverse map (display name, org_id, metric label) is never overwritten.
func (r *TenantResolver) AddAliasFrom(orgID string, tid TenantID, source string) error {
	if err := ValidateOrgID(orgID); err != nil {
		return err
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if err := r.checkLocked(orgID, tid); err != nil {
		return err
	}
	r.forward.Store(orgID, tid)
	r.reverse.Store(reverseKey(tid.AccountID, tid.ProjectID), orgID)
	// A config alias is authoritative: it upgrades the source of an identical entry.
	if _, ok := r.sources.Load(orgID); !ok || source == SourceConfig {
		r.sources.Store(orgID, source)
	}
	return nil
}

func (r *TenantResolver) RemoveAlias(orgID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if v, ok := r.forward.LoadAndDelete(orgID); ok {
		tid := v.(TenantID)
		r.reverse.Delete(reverseKey(tid.AccountID, tid.ProjectID))
		r.sources.Delete(orgID)
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
	id := fmt.Sprintf("%d:%d", accountID, projectID)
	switch r.config.MetricsFormat {
	case MetricsFormatName:
		return r.DisplayName(accountID, projectID)
	case MetricsFormatBoth:
		name := r.DisplayName(accountID, projectID)
		if name == id {
			return id
		}
		return id + "/" + name
	default:
		return id
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
	r.mu.Lock()
	defer r.mu.Unlock()
	var entries []AliasEntry
	r.forward.Range(func(k, v any) bool {
		orgID := k.(string)
		tid := v.(TenantID)
		src, _ := r.sources.Load(orgID)
		srcStr, _ := src.(string)
		entries = append(entries, AliasEntry{
			OrgID:     orgID,
			AccountID: tid.AccountID,
			ProjectID: tid.ProjectID,
			Source:    srcStr,
		})
		return true
	})
	return entries
}

func (r *TenantResolver) Config() ResolverConfig {
	return r.config
}

func CountTemplateSegments(template string) int {
	if template == "" {
		return 0
	}
	if !strings.Contains(template, "{") {
		return 0
	}
	clean := strings.TrimSuffix(template, "/")
	return len(strings.Split(clean, "/"))
}

func HasOrgIDTemplate(template string) bool {
	return strings.Contains(template, "{OrgID}")
}

// aliasesFrom returns the aliases that came from the given source.
func (r *TenantResolver) aliasesFrom(source string) []AliasEntry {
	var out []AliasEntry
	for _, e := range r.AllAliases() {
		if e.Source == source {
			out = append(out, e)
		}
	}
	return out
}
