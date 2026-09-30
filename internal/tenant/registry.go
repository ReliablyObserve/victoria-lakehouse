package tenant

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	smithy "github.com/aws/smithy-go"
	smithyhttp "github.com/aws/smithy-go/transport/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Default auto-register range: the upper half of the AccountID space, below
// NullAccountID. Int tenants of VictoriaLogs/VictoriaTraces live far below it.
const (
	DefaultAutoRegisterMinID uint32 = 1 << 31
	DefaultAutoRegisterMaxID uint32 = NullAccountID - 1

	defaultRegistryAttempts = 12
	defaultRegistryBackoff  = 25 * time.Millisecond
	maxRegistryBackoff      = time.Second
)

var (
	// ErrPreconditionFailed is what a ConditionalPool returns when the
	// conditional write lost the race (HTTP 412, or a concurrent conditional
	// request on the same key). Real S3 errors carrying that status are
	// recognised as well.
	ErrPreconditionFailed = errors.New("s3 precondition failed")
	// ErrAllocationConflict means the conditional write kept losing to other
	// writers for the whole retry bound. The insert is answered 503.
	ErrAllocationConflict = errors.New("tenant registry is contended, allocation gave up")
	// ErrRangeExhausted means no free AccountID is left in the auto-register range.
	ErrRangeExhausted = errors.New("auto-register AccountID range exhausted")
	// ErrRegistryUnavailable wraps a failure to read or write the shared registry.
	ErrRegistryUnavailable = errors.New("tenant registry unavailable")
)

// ConditionalPool is the slice of the S3 client the alias registry needs: a
// read that also returns the object's ETag, and a write that only succeeds
// when the object still has that ETag (If-Match) or does not exist yet
// (If-None-Match: *, selected by an empty ifMatch). The S3 backend must honour
// conditional PutObject (AWS S3, RustFS and recent MinIO do).
type ConditionalPool interface {
	DownloadWithETag(ctx context.Context, key string) (data []byte, etag string, found bool, err error)
	UploadConditional(ctx context.Context, key string, data []byte, ifMatch string) error
}

// IsPreconditionFailed reports whether err is a lost conditional write.
func IsPreconditionFailed(err error) bool {
	if err == nil {
		return false
	}
	if errors.Is(err, ErrPreconditionFailed) {
		return true
	}
	var ae smithy.APIError
	if errors.As(err, &ae) {
		switch ae.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return true
		}
	}
	var re *smithyhttp.ResponseError
	if errors.As(err, &re) && re.HTTPStatusCode() == 412 {
		return true
	}
	return false
}

// AutoRange is the AccountID range auto-registration allocates from.
type AutoRange struct {
	Min, Max uint32
}

// DefaultAutoRange returns the default reserved range.
func DefaultAutoRange() AutoRange {
	return AutoRange{Min: DefaultAutoRegisterMinID, Max: DefaultAutoRegisterMaxID}
}

// Validate checks the range is non-empty and stays below NullAccountID.
func (a AutoRange) Validate() error {
	if a.Min == 0 || a.Max == 0 {
		return fmt.Errorf("auto-register range [%d, %d] must not include AccountID 0", a.Min, a.Max)
	}
	if a.Min > a.Max {
		return fmt.Errorf("auto-register range min %d exceeds max %d", a.Min, a.Max)
	}
	if a.Max >= NullAccountID {
		return fmt.Errorf("auto-register range max %d must be below %d", a.Max, NullAccountID)
	}
	return nil
}

// Contains reports whether id lies inside the range.
func (a AutoRange) Contains(id uint32) bool { return id >= a.Min && id <= a.Max }

// DroppedEntry is a persisted alias entry that was refused, with the reason.
type DroppedEntry struct {
	Entry  AliasEntry
	Reason string
}

// SanitizeEntries filters entries read from the persisted registry. Entries
// that contradict a protected (configured) alias are dropped; so are all
// entries of a group that maps one OrgID to several IDs or one ID to several
// OrgIDs (there is no way to tell which one is right). Identical duplicates
// collapse into one. The input is not modified.
func SanitizeEntries(entries []AliasEntry, protected []AliasEntry) (kept []AliasEntry, dropped []DroppedEntry) {
	protOrg := make(map[string]TenantID, len(protected))
	protID := make(map[string]string, len(protected))
	for _, p := range protected {
		protOrg[p.OrgID] = TenantID{AccountID: p.AccountID, ProjectID: p.ProjectID}
		protID[reverseKey(p.AccountID, p.ProjectID)] = p.OrgID
	}

	type cand struct {
		e   AliasEntry
		tid TenantID
	}
	var cands []cand
	seen := make(map[string]struct{}, len(entries))
	for _, e := range entries {
		tid := TenantID{AccountID: e.AccountID, ProjectID: e.ProjectID}
		switch {
		case ValidateOrgID(e.OrgID) != nil:
			dropped = append(dropped, DroppedEntry{e, "invalid org ID"})
			continue
		case tid.AccountID == NullAccountID:
			dropped = append(dropped, DroppedEntry{e, fmt.Sprintf("AccountID %d is reserved for unknown tenants", NullAccountID)})
			continue
		}
		if want, ok := protOrg[e.OrgID]; ok && want != tid {
			dropped = append(dropped, DroppedEntry{e, fmt.Sprintf("contradicts the configured alias %q -> %d:%d", e.OrgID, want.AccountID, want.ProjectID)})
			continue
		}
		if owner, ok := protID[reverseKey(tid.AccountID, tid.ProjectID)]; ok && owner != e.OrgID {
			dropped = append(dropped, DroppedEntry{e, fmt.Sprintf("%d:%d belongs to the configured alias %q", tid.AccountID, tid.ProjectID, owner)})
			continue
		}
		k := e.OrgID + "\x00" + reverseKey(tid.AccountID, tid.ProjectID)
		if _, dup := seen[k]; dup {
			continue
		}
		seen[k] = struct{}{}
		cands = append(cands, cand{e, tid})
	}

	byOrg := make(map[string]map[TenantID]struct{})
	byID := make(map[string]map[string]struct{})
	for _, c := range cands {
		if byOrg[c.e.OrgID] == nil {
			byOrg[c.e.OrgID] = make(map[TenantID]struct{})
		}
		byOrg[c.e.OrgID][c.tid] = struct{}{}
		k := reverseKey(c.tid.AccountID, c.tid.ProjectID)
		if byID[k] == nil {
			byID[k] = make(map[string]struct{})
		}
		byID[k][c.e.OrgID] = struct{}{}
	}
	for _, c := range cands {
		k := reverseKey(c.tid.AccountID, c.tid.ProjectID)
		switch {
		case len(byOrg[c.e.OrgID]) > 1:
			dropped = append(dropped, DroppedEntry{c.e, fmt.Sprintf("OrgID %q is mapped to %d different IDs", c.e.OrgID, len(byOrg[c.e.OrgID]))})
		case len(byID[k]) > 1:
			dropped = append(dropped, DroppedEntry{c.e, fmt.Sprintf("%s is mapped to %d different OrgIDs", k, len(byID[k]))})
		default:
			kept = append(kept, c.e)
		}
	}
	return kept, dropped
}

// ValidateConfiguredAliases refuses a configured alias set that cannot be
// served: two OrgIDs on one ID, an OrgID on two IDs, an alias inside the
// auto-register range or on the reserved null AccountID.
func ValidateConfiguredAliases(aliases []AliasEntry, rng AutoRange) error {
	byOrg := make(map[string]TenantID, len(aliases))
	byID := make(map[string]string, len(aliases))
	for _, a := range aliases {
		if err := ValidateOrgID(a.OrgID); err != nil {
			return fmt.Errorf("tenant alias %q: %w", a.OrgID, err)
		}
		tid := TenantID{AccountID: a.AccountID, ProjectID: a.ProjectID}
		if tid.AccountID == NullAccountID {
			return fmt.Errorf("tenant alias %q maps to AccountID %d, which is reserved for unknown tenants", a.OrgID, NullAccountID)
		}
		if rng.Contains(tid.AccountID) {
			return fmt.Errorf("tenant alias %q maps to AccountID %d, inside the reserved auto-register range [%d, %d]", a.OrgID, tid.AccountID, rng.Min, rng.Max)
		}
		if prev, ok := byOrg[a.OrgID]; ok && prev != tid {
			return fmt.Errorf("tenant alias %q is mapped to two IDs: %d:%d and %d:%d", a.OrgID, prev.AccountID, prev.ProjectID, tid.AccountID, tid.ProjectID)
		}
		k := reverseKey(tid.AccountID, tid.ProjectID)
		if other, ok := byID[k]; ok && other != a.OrgID {
			return fmt.Errorf("tenant aliases %q and %q are both mapped to %s", other, a.OrgID, k)
		}
		byOrg[a.OrgID] = tid
		byID[k] = a.OrgID
	}
	return nil
}

// RegistryConfig configures a Registry.
type RegistryConfig struct {
	// Pool is the S3 client. Nil selects an in-memory pool (single process).
	Pool ConditionalPool
	// Key is the registry object key (for example logs/_meta/tenant-aliases.json).
	Key string
	// Range is the AccountID range auto-registration allocates from.
	Range AutoRange
	// DataTenants lists the AccountIDs of int tenants that hold data (cold tier
	// manifest, unflushed buffer). The allocator never hands those out.
	DataTenants func() []uint32
	// MaxAttempts bounds the read-modify-write retries on a lost conditional
	// write. Zero selects the default.
	MaxAttempts int
	// Backoff is the base of the exponential, fully jittered retry delay.
	Backoff time.Duration
}

// Registry is the shared alias registry: one S3 object holding every confirmed
// alias, updated only with conditional writes, so pods and restarts share one
// allocator. The resolver is its in-memory view.
type Registry struct {
	cfg      RegistryConfig
	pool     ConditionalPool
	resolver *TenantResolver

	allocMu sync.Mutex // serialises allocations inside this process

	refreshMu   sync.Mutex
	lastRefresh atomic.Int64 // unix nanos

	reportedMu sync.Mutex
	reported   map[string]struct{} // dropped entries already logged
}

// NewRegistry builds the registry over resolver and installs itself as the
// resolver's allocator.
func NewRegistry(resolver *TenantResolver, cfg RegistryConfig) (*Registry, error) {
	if cfg.Range == (AutoRange{}) {
		cfg.Range = DefaultAutoRange()
	}
	if err := cfg.Range.Validate(); err != nil {
		return nil, err
	}
	if cfg.Key == "" {
		cfg.Key = "_meta/tenant-aliases.json"
	}
	if cfg.MaxAttempts <= 0 {
		cfg.MaxAttempts = defaultRegistryAttempts
	}
	if cfg.Backoff <= 0 {
		cfg.Backoff = defaultRegistryBackoff
	}
	g := &Registry{cfg: cfg, pool: cfg.Pool, resolver: resolver, reported: make(map[string]struct{})}
	if g.pool == nil {
		g.pool = NewMemConditionalPool()
	}
	resolver.SetAllocator(g)
	return g, nil
}

// Range returns the auto-register range.
func (g *Registry) Range() AutoRange { return g.cfg.Range }

func (g *Registry) backoff(ctx context.Context, attempt int) error {
	d := g.cfg.Backoff << uint(attempt)
	if d <= 0 || d > maxRegistryBackoff {
		d = maxRegistryBackoff
	}
	// #nosec G404 -- jitter, not security
	d = time.Duration(rand.Int63n(int64(d) + 1))
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// load reads and sanitizes the registry object. A corrupt object is an error:
// it is never overwritten blindly.
func (g *Registry) load(ctx context.Context) (kept []AliasEntry, raw []AliasEntry, etag string, err error) {
	data, etag, found, err := g.pool.DownloadWithETag(ctx, g.cfg.Key)
	if err != nil {
		return nil, nil, "", fmt.Errorf("%w: read %s: %v", ErrRegistryUnavailable, g.cfg.Key, err)
	}
	if !found || len(data) == 0 {
		return nil, nil, "", nil
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return nil, nil, "", fmt.Errorf("%w: %s is not valid JSON: %v", ErrRegistryUnavailable, g.cfg.Key, err)
	}
	kept, dropped := SanitizeEntries(raw, g.resolver.aliasesFrom(SourceConfig))
	g.reportDropped(dropped)
	return kept, raw, etag, nil
}

func (g *Registry) reportDropped(dropped []DroppedEntry) {
	for _, d := range dropped {
		metrics.TenantAliasRejectedTotal.Inc("registry")
		k := fmt.Sprintf("%s|%d|%d|%s", d.Entry.OrgID, d.Entry.AccountID, d.Entry.ProjectID, d.Reason)
		g.reportedMu.Lock()
		_, done := g.reported[k]
		g.reported[k] = struct{}{}
		g.reportedMu.Unlock()
		if !done {
			logger.Errorf("tenant registry: refusing persisted alias %q -> %d:%d (%s): %s",
				d.Entry.OrgID, d.Entry.AccountID, d.Entry.ProjectID, d.Entry.Source, d.Reason)
		}
	}
}

func marshalEntries(entries []AliasEntry) ([]byte, error) {
	out := append([]AliasEntry(nil), entries...)
	sort.Slice(out, func(i, j int) bool {
		if out[i].AccountID != out[j].AccountID {
			return out[i].AccountID < out[j].AccountID
		}
		if out[i].ProjectID != out[j].ProjectID {
			return out[i].ProjectID < out[j].ProjectID
		}
		return out[i].OrgID < out[j].OrgID
	})
	if out == nil {
		out = []AliasEntry{}
	}
	return json.Marshal(out)
}

func sameEntries(a, b []AliasEntry) bool {
	if len(a) != len(b) {
		return false
	}
	x, err1 := marshalEntries(a)
	y, err2 := marshalEntries(b)
	return err1 == nil && err2 == nil && string(x) == string(y)
}

// adopt copies registry entries into the resolver. Entries that conflict with
// what the resolver already holds are refused and counted, never overwritten.
func (g *Registry) adopt(entries []AliasEntry) {
	for _, e := range entries {
		src := e.Source
		if src == "" {
			src = SourceSynced
		}
		err := g.resolver.AddAliasFrom(e.OrgID, TenantID{AccountID: e.AccountID, ProjectID: e.ProjectID}, src)
		if err != nil {
			metrics.TenantAliasRejectedTotal.Inc("registry")
			logger.Errorf("tenant registry: not adopting %q -> %d:%d: %s", e.OrgID, e.AccountID, e.ProjectID, err)
		}
	}
}

// Allocate returns the TenantID of orgID, registering it in the reserved range
// when no pod has yet. Concurrent callers, in this process and in other pods,
// get distinct IDs; callers for the same OrgID converge on one.
func (g *Registry) Allocate(ctx context.Context, orgID string) (TenantID, error) {
	if err := ValidateOrgID(orgID); err != nil {
		return TenantID{}, err
	}
	g.allocMu.Lock()
	defer g.allocMu.Unlock()

	if tid, ok := g.resolver.Resolve(orgID); ok {
		return tid, nil
	}
	for attempt := 0; attempt < g.cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			metrics.TenantAllocConflictsTotal.Inc()
			if err := g.backoff(ctx, attempt-1); err != nil {
				metrics.TenantAllocFailedTotal.Inc()
				return TenantID{}, err
			}
		}
		kept, _, etag, err := g.load(ctx)
		if err != nil {
			metrics.TenantAllocFailedTotal.Inc()
			return TenantID{}, err
		}
		g.adopt(kept)
		// Another pod registered orgID, or a configured alias now covers it.
		if tid, ok := g.resolver.Resolve(orgID); ok {
			return tid, nil
		}

		id, err := g.pickID(kept)
		if err != nil {
			metrics.TenantAllocFailedTotal.Inc()
			return TenantID{}, err
		}
		entry := AliasEntry{OrgID: orgID, AccountID: id, Source: SourceAuto}
		data, err := marshalEntries(append(append([]AliasEntry(nil), kept...), entry))
		if err != nil {
			return TenantID{}, err
		}
		if err := g.pool.UploadConditional(ctx, g.cfg.Key, data, etag); err != nil {
			if IsPreconditionFailed(err) {
				continue
			}
			metrics.TenantAllocFailedTotal.Inc()
			return TenantID{}, fmt.Errorf("%w: write %s: %v", ErrRegistryUnavailable, g.cfg.Key, err)
		}
		tid := TenantID{AccountID: id}
		if err := g.resolver.AddAliasFrom(orgID, tid, SourceAuto); err != nil {
			// The registry holds it, the local view contradicts it: do not serve.
			metrics.TenantAllocFailedTotal.Inc()
			return TenantID{}, err
		}
		metrics.TenantAutoRegisteredTotal.Inc()
		logger.Infof("tenant registry: auto-registered %q as %d:0", orgID, id)
		return tid, nil
	}
	metrics.TenantAllocFailedTotal.Inc()
	return TenantID{}, ErrAllocationConflict
}

// pickID returns the next free AccountID of the range: above every taken ID in
// the range (so a deleted alias's ID is not handed out again while newer ones
// exist), or, when the top is reached, the lowest gap. Taken means mapped by
// the registry, by any alias of this process, or held by an int tenant.
func (g *Registry) pickID(registry []AliasEntry) (uint32, error) {
	rng := g.cfg.Range
	taken := make(map[uint32]struct{}, len(registry)+8)
	for _, e := range registry {
		taken[e.AccountID] = struct{}{}
	}
	for _, e := range g.resolver.AllAliases() {
		taken[e.AccountID] = struct{}{}
	}
	if g.cfg.DataTenants != nil {
		for _, id := range g.cfg.DataTenants() {
			taken[id] = struct{}{}
		}
	}
	var top uint32
	inRange := make([]uint32, 0, len(taken))
	for id := range taken {
		if rng.Contains(id) {
			inRange = append(inRange, id)
			if id > top {
				top = id
			}
		}
	}
	if len(inRange) == 0 {
		return rng.Min, nil
	}
	if top < rng.Max {
		return top + 1, nil
	}
	sort.Slice(inRange, func(i, j int) bool { return inRange[i] < inRange[j] })
	cand := rng.Min
	for _, id := range inRange {
		if id == cand {
			cand++
		} else if id > cand {
			break
		}
	}
	if cand > rng.Max {
		return 0, ErrRangeExhausted
	}
	return cand, nil
}

// Register writes an operator-created alias to the registry (conditional
// write) after checking it against what the registry already maps. It returns
// an error matching ErrAliasConflict when the OrgID or the ID is taken.
func (g *Registry) Register(ctx context.Context, e AliasEntry) error {
	if e.Source == "" {
		e.Source = SourceAPI
	}
	tid := TenantID{AccountID: e.AccountID, ProjectID: e.ProjectID}
	if err := g.resolver.CheckAlias(e.OrgID, tid); err != nil {
		return err
	}
	if g.cfg.Range.Contains(tid.AccountID) {
		return &AliasConflictError{OrgID: e.OrgID, Requested: tid,
			Reason: fmt.Sprintf("AccountID %d is inside the reserved auto-register range [%d, %d]", tid.AccountID, g.cfg.Range.Min, g.cfg.Range.Max)}
	}
	g.allocMu.Lock()
	defer g.allocMu.Unlock()
	for attempt := 0; attempt < g.cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			metrics.TenantAllocConflictsTotal.Inc()
			if err := g.backoff(ctx, attempt-1); err != nil {
				return err
			}
		}
		kept, _, etag, err := g.load(ctx)
		if err != nil {
			return err
		}
		for _, k := range kept {
			if k.OrgID == e.OrgID && k.AccountID == e.AccountID && k.ProjectID == e.ProjectID {
				return nil
			}
			if k.OrgID == e.OrgID || (k.AccountID == e.AccountID && k.ProjectID == e.ProjectID) {
				return &AliasConflictError{OrgID: e.OrgID, Requested: tid, ExistingOrgID: k.OrgID,
					Existing: TenantID{AccountID: k.AccountID, ProjectID: k.ProjectID},
					Reason:   fmt.Sprintf("the registry already maps %q to %d:%d", k.OrgID, k.AccountID, k.ProjectID)}
			}
		}
		data, err := marshalEntries(append(append([]AliasEntry(nil), kept...), e))
		if err != nil {
			return err
		}
		if err := g.pool.UploadConditional(ctx, g.cfg.Key, data, etag); err != nil {
			if IsPreconditionFailed(err) {
				continue
			}
			return fmt.Errorf("%w: write %s: %v", ErrRegistryUnavailable, g.cfg.Key, err)
		}
		return nil
	}
	return ErrAllocationConflict
}

// Unregister removes orgID from the registry (conditional write).
func (g *Registry) Unregister(ctx context.Context, orgID string) error {
	g.allocMu.Lock()
	defer g.allocMu.Unlock()
	for attempt := 0; attempt < g.cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			metrics.TenantAllocConflictsTotal.Inc()
			if err := g.backoff(ctx, attempt-1); err != nil {
				return err
			}
		}
		kept, _, etag, err := g.load(ctx)
		if err != nil {
			return err
		}
		out := kept[:0:0]
		for _, k := range kept {
			if k.OrgID != orgID {
				out = append(out, k)
			}
		}
		if len(out) == len(kept) {
			return nil
		}
		data, err := marshalEntries(out)
		if err != nil {
			return err
		}
		if err := g.pool.UploadConditional(ctx, g.cfg.Key, data, etag); err != nil {
			if IsPreconditionFailed(err) {
				continue
			}
			return fmt.Errorf("%w: write %s: %v", ErrRegistryUnavailable, g.cfg.Key, err)
		}
		return nil
	}
	return ErrAllocationConflict
}

// Sync reconciles this process with the registry: configured aliases are
// merged into the object (conditional write) and every registry entry is
// adopted into the resolver. Entries of the object that contradict a
// configured alias, or each other, are refused, logged and counted, and
// removed from the object by the same write. Run it at startup and on a timer.
func (g *Registry) Sync(ctx context.Context) error {
	g.allocMu.Lock()
	defer g.allocMu.Unlock()
	for attempt := 0; attempt < g.cfg.MaxAttempts; attempt++ {
		if attempt > 0 {
			metrics.TenantAllocConflictsTotal.Inc()
			if err := g.backoff(ctx, attempt-1); err != nil {
				return err
			}
		}
		kept, raw, etag, err := g.load(ctx)
		if err != nil {
			return err
		}
		merged := append([]AliasEntry(nil), kept...)
		for _, c := range g.resolver.aliasesFrom(SourceConfig) {
			present := false
			for _, k := range merged {
				if k.OrgID == c.OrgID && k.AccountID == c.AccountID && k.ProjectID == c.ProjectID {
					present = true
					break
				}
			}
			if !present {
				merged = append(merged, c)
			}
		}
		if raw != nil || len(merged) > 0 {
			if !sameEntries(merged, raw) {
				data, err := marshalEntries(merged)
				if err != nil {
					return err
				}
				if err := g.pool.UploadConditional(ctx, g.cfg.Key, data, etag); err != nil {
					if IsPreconditionFailed(err) {
						continue
					}
					return fmt.Errorf("%w: write %s: %v", ErrRegistryUnavailable, g.cfg.Key, err)
				}
			}
		}
		g.adopt(merged)
		g.lastRefresh.Store(time.Now().UnixNano())
		return nil
	}
	return ErrAllocationConflict
}

// RefreshIfStale adopts the registry into the resolver when the last refresh is
// older than minAge. It never writes and never fails a request: on an error
// the resolver keeps what it has.
func (g *Registry) RefreshIfStale(ctx context.Context, minAge time.Duration) {
	if time.Since(time.Unix(0, g.lastRefresh.Load())) < minAge {
		return
	}
	g.refreshMu.Lock()
	defer g.refreshMu.Unlock()
	if time.Since(time.Unix(0, g.lastRefresh.Load())) < minAge {
		return
	}
	kept, _, _, err := g.load(ctx)
	g.lastRefresh.Store(time.Now().UnixNano())
	if err != nil {
		logger.Warnf("tenant registry: refresh failed: %s", err)
		return
	}
	g.adopt(kept)
}

// Run syncs the registry every interval until stop is closed.
func (g *Registry) Run(stop <-chan struct{}, interval time.Duration) {
	if interval <= 0 {
		return
	}
	go func() {
		t := time.NewTicker(interval)
		defer t.Stop()
		for {
			select {
			case <-stop:
				return
			case <-t.C:
				ctx, cancel := context.WithTimeout(context.Background(), interval)
				if err := g.Sync(ctx); err != nil {
					logger.Warnf("tenant registry: sync failed: %s", err)
				}
				cancel()
			}
		}
	}()
}

// MemConditionalPool is an in-memory ConditionalPool with S3's If-Match /
// If-None-Match semantics. It backs single-process use and tests.
type MemConditionalPool struct {
	mu      sync.Mutex
	objects map[string]memObject
	version int
	// Hooks for tests: run before each operation; a non-nil error is returned.
	BeforeRead  func(key string) error
	BeforeWrite func(key string) error
}

type memObject struct {
	data []byte
	etag string
}

// NewMemConditionalPool returns an empty pool.
func NewMemConditionalPool() *MemConditionalPool {
	return &MemConditionalPool{objects: make(map[string]memObject)}
}

// DownloadWithETag implements ConditionalPool.
func (m *MemConditionalPool) DownloadWithETag(_ context.Context, key string) ([]byte, string, bool, error) {
	if m.BeforeRead != nil {
		if err := m.BeforeRead(key); err != nil {
			return nil, "", false, err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	if !ok {
		return nil, "", false, nil
	}
	return append([]byte(nil), o.data...), o.etag, true, nil
}

// UploadConditional implements ConditionalPool.
func (m *MemConditionalPool) UploadConditional(_ context.Context, key string, data []byte, ifMatch string) error {
	if m.BeforeWrite != nil {
		if err := m.BeforeWrite(key); err != nil {
			return err
		}
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	o, exists := m.objects[key]
	if ifMatch == "" && exists {
		return fmt.Errorf("%w: %s exists", ErrPreconditionFailed, key)
	}
	if ifMatch != "" && (!exists || o.etag != ifMatch) {
		return fmt.Errorf("%w: %s etag mismatch", ErrPreconditionFailed, key)
	}
	m.version++
	m.objects[key] = memObject{data: append([]byte(nil), data...), etag: fmt.Sprintf("\"v%d\"", m.version)}
	return nil
}

// Put stores data unconditionally (test setup).
func (m *MemConditionalPool) Put(key string, data []byte) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.version++
	m.objects[key] = memObject{data: append([]byte(nil), data...), etag: fmt.Sprintf("\"v%d\"", m.version)}
}

// Get returns the stored bytes (test inspection).
func (m *MemConditionalPool) Get(key string) ([]byte, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()
	o, ok := m.objects[key]
	return append([]byte(nil), o.data...), ok
}
