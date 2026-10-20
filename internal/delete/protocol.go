package delete

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"math"
	"sort"
	"sync"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// How a pod keeps its view of the deletes current (see keys.go for the keys):
//
//   - every poll interval (one minute) it lists only the keys created in the last
//     pollWindow (ListObjectsV2 StartAfter), reads the definitions it has not seen
//     and applies the undo keys it finds;
//   - every fullEvery intervals, and at start, it lists the whole prefix, which
//     catches anything a poll could have missed (a creator whose clock was more
//     than pollWindow off, a write that took longer than pollWindow); a key learned
//     that way after it was older than the window is counted
//     (lakehouse_delete_tombstones_learned_late_total);
//   - a view that has not completed a sync for staleAfter intervals is stale:
//     reads that must not show deleted rows fail closed (Gate), for every tenant;
//     a definition that cannot be decoded closes the gate only for the tenants it
//     names;
//   - every time here is the pod's own clock. There is no shared clock.
//
// The view is only ever added to from a listing. Nothing a listing does not
// contain is removed, so a truncated or short listing (which an object store can
// return without an error) delays learning about a new delete, but can never
// make a pod forget one, and no pod ever DELETEs a tombstone key because of
// anything it holds locally: the only DELETEs are the retention GC, which acts on
// a deterministic predicate.

const (
	// defaultInterval is the poll interval: a constant, not a knob. One minute
	// costs about $0.22 per pod per month in LIST requests and bounds how long a
	// delete takes to reach every pod. Options.Interval overrides it for tests.
	defaultInterval = time.Minute
	// pollWindow is how far back a poll looks. It must exceed the clock
	// difference between a creating pod and the store plus the longest PUT.
	pollWindow = 10 * time.Minute
	// fullEvery is how many poll intervals pass between full listings.
	fullEvery = 60
	// staleAfter is how many poll intervals without a complete sync make a view
	// stale.
	staleAfter = 3
	// maxStartupWait bounds how long a read waits for the first complete sync.
	maxStartupWait = 30 * time.Second
	// syncTimeout bounds one sync (a listing and the reads it triggers), so a store
	// that never finishes a listing cannot hold the sync lock for ever.
	syncTimeout = 5 * time.Minute
	// fetchWorkers bounds the parallel GETs of one sync.
	fetchWorkers = 16
	// gcBatch bounds the tombstones one GC pass removes.
	gcBatch = 500
	// retentionMargin is added to a tombstone's range end and the longest
	// retention before it expires: a file spans at most a day, so a file whose
	// first row the tombstone covers can hold rows up to a day later. It does not
	// cover clock skew, and it does not cover pods running different retention
	// settings: every pod expires a tombstone by its own policy, so a pod with a
	// shorter retention removes the definition earlier than a pod with a longer one
	// stops serving the rows (known limit; keep retention identical across pods).
	retentionMargin = 24 * time.Hour
)

// ErrViewStale is returned by Gate when the view of the deletes is too old to
// trust.
var ErrViewStale = errors.New("the list of deleted data could not be refreshed recently; retry once the object store is reachable")

// ErrViewNotSynced is returned by Gate before the first complete sync.
var ErrViewNotSynced = errors.New("the list of deleted data has not been read yet; retry shortly")

// ErrNoBackend is returned by an operation that needs the bucket on a view that
// has none.
var ErrNoBackend = errors.New("tombstones are not backed by an object store")

// ErrTenantUnreadable is returned by Gate for a request of a tenant named by a
// tombstone definition that cannot be decoded.
var ErrTenantUnreadable = errors.New("a delete of this tenant cannot be read; retry once it is readable or removed")

// ErrNotReversible is returned by Undelete for a tombstone that is not in hide
// mode. Like upstream's deletes, permanent and auto (the default of run_task)
// cannot be undone.
var ErrNotReversible = errors.New("only a hide-mode delete can be undone; permanent and auto deletes are irreversible")

// Options configures the protocol of a view.
type Options struct {
	Backend Backend
	// Prefix is the signal prefix the tombstones live under (the config's
	// AutoPrefix); the keys are under TombstonePrefix(Prefix).
	Prefix string
	// Interval overrides the poll interval (a constant of one minute); tests only.
	Interval time.Duration
	// Now is the pod's clock (default time.Now); tests inject it.
	Now func() time.Time
	// GateOff turns the read gate off (delete.enabled is false). The pod still
	// polls and enforces every tombstone it knows; nothing is refused.
	GateOff bool
	// CacheDir is the optional directory of cached tombstone objects.
	CacheDir string
	// Expiry returns the time from which tombstone t is no longer needed because
	// the data it covers is past retention. ok is false when it never expires
	// (retention off).
	Expiry func(t Tombstone) (at time.Time, ok bool)
	// Maintenance enables the retention GC on this pod.
	Maintenance bool
}

type protocol struct {
	s           *TombstoneStore
	b           Backend
	prefix      string
	interval    time.Duration
	now         func() time.Time
	cache       *bodyCache
	expiry      func(Tombstone) (time.Time, bool)
	maintenance bool
	gateOn      bool

	syncMu sync.Mutex // serialises syncs; guards the fields below
	// bad holds the definition keys that could not be read (a transport error);
	// the view is not complete while there are any, so the gate closes for every
	// tenant once the view is stale.
	bad map[string]string
	// unreadable holds the definition keys that were read but cannot be decoded
	// (corrupt, or a newer version): the gate closes for the tenants each names.
	unreadable map[string]unreadableDef
	// undoSeen are undo keys listed before the definition they undo is in the
	// view: definition key -> the undo key's time. They apply when that definition
	// is learned.
	undoSeen map[string]int64
	// lastFull is the pod time the last full listing started at.
	lastFull time.Time
	// lastList is the pod time the last successful listing started at: the anchor
	// of the poll window. It advances even while a key cannot be read, so one
	// unreadable object does not turn every poll into a full listing.
	lastList time.Time
	// gone are the definition keys of tombstones that left the view by retention:
	// later listings skip them instead of reading them again.
	gone map[string]struct{}
	// seeded is the cached keys the view was seeded with; the first full listing
	// confirms or forgets them.
	seeded map[string]struct{}
	// expired are the tombstones that left the view by retention and whose keys a
	// maintenance pod still has to remove: id -> definition key.
	expired map[string]string
	// synced reports that a listing succeeded before: keys learned afterwards
	// that are older than the poll window were learned late.
	firstDone bool

	// confirmCursor is where the next bounded HEAD sample starts (confirmAbsent).
	confirmCursor int

	attached time.Time // when the protocol was attached
	stateMu  sync.Mutex
	lastGood time.Time // local monotonic time of the last complete sync
	// unreadableSnapshot is the copy of unreadable that Gate reads (stateMu).
	unreadableSnapshot map[string]unreadableDef
	synced             chan struct{}
	onceSync           sync.Once
}

// unreadableDef is what a definition that cannot be decoded closes the gate for:
// the tenants its body names, or every tenant when even that cannot be read.
type unreadableDef struct {
	tenants []TenantRef
	all     bool
	why     string
}

// Attach connects the view to a bucket. The cached tombstone objects, if any,
// put the tombstones this pod knew in force at once; Gate keeps reads closed
// until the first complete sync has confirmed them.
func (s *TombstoneStore) Attach(opts Options) error {
	if opts.Backend == nil {
		return ErrNoBackend
	}
	if opts.Interval <= 0 {
		opts.Interval = defaultInterval
	}
	if opts.Now == nil {
		opts.Now = time.Now
	}
	p := &protocol{
		s:           s,
		b:           opts.Backend,
		prefix:      TombstonePrefix(opts.Prefix),
		interval:    opts.Interval,
		now:         opts.Now,
		cache:       newBodyCache(opts.CacheDir),
		expiry:      opts.Expiry,
		maintenance: opts.Maintenance,
		gateOn:      !opts.GateOff,
		bad:         map[string]string{},
		unreadable:  map[string]unreadableDef{},
		undoSeen:    map[string]int64{},
		seeded:      map[string]struct{}{},
		expired:     map[string]string{},
		gone:        map[string]struct{}{},
		synced:      make(chan struct{}),
		attached:    time.Now(),
	}
	p.seed()
	s.proto.Store(p)
	activeProtocol.Store(p)
	return nil
}

// seed puts the cached definitions in the view. The first full listing confirms
// each seeded key with a HEAD when the bucket does not list it.
func (p *protocol) seed() {
	p.cache.each(func(key string, body []byte) {
		k, ok := parseKey(p.prefix, key)
		if !ok || k.kind != kindDefinition {
			return
		}
		ts, err := p.decodeFor(k, body)
		if err != nil {
			p.cache.remove(key) // a corrupt cache file is dropped, never trusted
			return
		}
		p.s.mu.Lock()
		p.s.putLocked(&entry{t: ts, defKey: key})
		p.s.mu.Unlock()
		p.seeded[key] = struct{}{}
	})
	p.s.updateActiveGauges()
}

// decodeFor decodes a definition body and checks it belongs to its key.
func (p *protocol) decodeFor(k tombKey, body []byte) (Tombstone, error) {
	ts, err := decodeDefinition(body)
	if err != nil {
		return Tombstone{}, err
	}
	if ts.ID != k.id {
		return Tombstone{}, fmt.Errorf("definition id %q does not match its key id %q", ts.ID, k.id)
	}
	ts.CreatedAt = time.Unix(0, k.timeNs).UTC()
	return ts, nil
}

// applyUndo marks the tombstone the undo key k belongs to as undone. Only a hide
// tombstone can be undone: an undo key for any other mode is not evidence of
// anything (permanent and auto deletes are irreversible) and is counted and
// ignored. It reports whether the tombstone is in the view.
func (p *protocol) applyUndo(k tombKey) bool {
	p.s.mu.Lock()
	defer p.s.mu.Unlock()
	e, ok := p.s.byID[k.id]
	if !ok || e.defKey != k.definitionKey(p.prefix) {
		return false
	}
	if e.t.Mode != "hide" {
		metrics.DeleteTombstoneKeysRejected.Inc()
		return true
	}
	p.s.markUndoneLocked(e)
	return true
}

// viewAge is the time since the last complete sync.
func (p *protocol) viewAge() (time.Duration, bool) {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	if p.lastGood.IsZero() {
		return 0, false
	}
	return time.Since(p.lastGood), true
}

func (p *protocol) markSynced() {
	p.stateMu.Lock()
	p.lastGood = time.Now()
	p.stateMu.Unlock()
	p.onceSync.Do(func() { close(p.synced) })
}

// Gate returns nil when the view can be trusted to know every delete taken so
// far for the given tenants, and an error otherwise. tenants are the tenants of
// the request; none (a read of every tenant) is treated as all of them.
//
// Before the first complete sync it waits, for at most staleAfter intervals, for
// that sync; afterwards it never waits. A view that has not completed a sync for
// staleAfter intervals is stale for every tenant: the list of deletes could not be
// read and the missing delete could be anyone's (the same principle as upstream's
// cluster failing a query when a storage node is unreachable). A definition that
// cannot be decoded closes the gate only for the tenants it names, and for all
// tenants only when even that cannot be read.
//
// The gate does nothing when deletes are disabled (delete.enabled false): the view
// is still kept and enforced, but nothing is refused. A view without a bucket
// (unit tests) is always trusted.
//
// Reads that show rows (cold tier, buffer bridge, enumerations) call it and fail
// closed on an error. Ingest, the durable buffer and flush never do: an object
// store outage must not stop writes.
func (s *TombstoneStore) Gate(ctx context.Context, tenants []TenantRef) error {
	p := s.proto.Load()
	if p == nil || !p.gateOn {
		return nil
	}
	select {
	case <-p.synced:
	default:
		limit := staleAfter * p.interval
		if limit > maxStartupWait {
			limit = maxStartupWait
		}
		wait := time.NewTimer(limit)
		defer wait.Stop()
		select {
		case <-p.synced:
		case <-ctx.Done():
			metrics.DeleteReadsRefused.Inc("not_synced")
			return ErrViewNotSynced
		case <-wait.C:
			metrics.DeleteReadsRefused.Inc("not_synced")
			return ErrViewNotSynced
		}
	}
	if age, ok := p.viewAge(); !ok || age > staleAfter*p.interval {
		metrics.DeleteReadsRefused.Inc("stale")
		return ErrViewStale
	}
	if p.closedFor(tenants) {
		metrics.DeleteReadsRefused.Inc("tenant_unreadable")
		return ErrTenantUnreadable
	}
	return nil
}

// Synced reports whether the view has completed its first sync (or has no bucket
// to sync with). A select pod reports itself ready only then: until it has read
// the deletes, its reads are refused anyway.
func (s *TombstoneStore) Synced() bool {
	p := s.proto.Load()
	if p == nil {
		return true
	}
	select {
	case <-p.synced:
		return true
	default:
		return false
	}
}

// closedFor reports whether an undecodable definition closes the gate for any of
// the tenants.
func (p *protocol) closedFor(tenants []TenantRef) bool {
	p.stateMu.Lock()
	defer p.stateMu.Unlock()
	for _, u := range p.unreadableSnapshot {
		if u.all || len(tenants) == 0 {
			return true
		}
		for _, want := range tenants {
			for _, named := range u.tenants {
				if named.AccountID == want.AccountID && (named.ProjectID == want.ProjectID || named.ProjectID == 0) {
					return true
				}
			}
		}
	}
	return false
}

// Start runs the poll loop until ctx is cancelled and returns when it has
// stopped. The first sync is a full listing, retried until it succeeds.
func (s *TombstoneStore) Start(ctx context.Context) {
	p := s.proto.Load()
	if p == nil {
		return
	}
	retry := p.interval
	if retry > 10*time.Second {
		retry = 10 * time.Second
	}
	for {
		if err := p.sync(ctx, true); err == nil {
			break
		} else if ctx.Err() != nil {
			return
		} else {
			logger.Warnf("tombstone sync failed: %s", err)
		}
		select {
		case <-ctx.Done():
			return
		case <-time.After(retry):
		}
	}
	t := time.NewTicker(p.interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if err := p.sync(ctx, false); err != nil && ctx.Err() == nil {
				logger.Warnf("tombstone sync failed: %s", err)
			}
		}
	}
}

// SyncNow runs one sync: a full listing when full is set, otherwise the poll the
// loop would run (which becomes a full listing when it is due). For tests and
// for a caller that must see the bucket's current state.
func (s *TombstoneStore) SyncNow(ctx context.Context, full bool) error {
	p := s.proto.Load()
	if p == nil {
		return ErrNoBackend
	}
	return p.sync(ctx, full)
}

type listedKey struct {
	key string
	k   tombKey
}

type fetched struct {
	body      []byte
	found     bool
	err       error
	fromCache bool
}

// refetch reads key from the bucket, bypassing and replacing the cache. It is
// what a cached body that fails to decode falls back to: a corrupt cache file
// must never keep the view incomplete.
func (p *protocol) refetch(ctx context.Context, key string) fetched {
	p.cache.remove(key)
	var f fetched
	f.body, f.found, f.err = p.b.Get(ctx, key)
	if f.err == nil && f.found {
		p.cache.put(key, f.body)
	}
	return f
}

// fetch reads the objects at keys, from the cache when it has them, with a
// bounded number of parallel GETs.
func (p *protocol) fetch(ctx context.Context, keys []string) map[string]fetched {
	out := make(map[string]fetched, len(keys))
	var mu sync.Mutex
	work := make(chan string)
	var wg sync.WaitGroup
	n := fetchWorkers
	if len(keys) < n {
		n = len(keys)
	}
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for key := range work {
				var f fetched
				if body, ok := p.cache.get(key); ok {
					f = fetched{body: body, found: true, fromCache: true}
				} else {
					f.body, f.found, f.err = p.b.Get(ctx, key)
					if f.err == nil && f.found {
						p.cache.put(key, f.body)
					}
				}
				mu.Lock()
				out[key] = f
				mu.Unlock()
			}
		}()
	}
	for _, key := range keys {
		work <- key
	}
	close(work)
	wg.Wait()
	return out
}

// sync reads the bucket and brings the view up to date.
func (p *protocol) sync(ctx context.Context, forceFull bool) (err error) {
	p.syncMu.Lock()
	defer p.syncMu.Unlock()
	ctx, cancel := context.WithTimeout(ctx, syncTimeout)
	defer cancel()

	t0 := p.now()
	full := forceFull || p.lastList.IsZero() ||
		t0.Sub(p.lastList) > pollWindow/2 ||
		t0.Sub(p.lastFull) >= fullEvery*p.interval
	kind := "poll"
	if full {
		kind = "full"
	}
	defer func() {
		if err != nil {
			metrics.DeleteTombstoneSyncErrors.Inc(kind)
		}
	}()

	startAfter := ""
	if !full {
		startAfter = startAfterKey(p.prefix, t0.Add(-pollWindow).UnixNano())
	}
	var defs, undos []listedKey
	listedDefs := map[string]struct{}{}
	listedAll := map[string]struct{}{}
	listErr := p.b.ListAfter(ctx, p.prefix, startAfter, func(key string) error {
		k, ok := parseKey(p.prefix, key)
		if !ok {
			metrics.DeleteTombstoneKeysRejected.Inc()
			return nil
		}
		lk := listedKey{key: key, k: k}
		switch k.kind {
		case kindDefinition:
			defs = append(defs, lk)
			listedDefs[key] = struct{}{}
		case kindUndo:
			undos = append(undos, lk)
		}
		if full {
			listedAll[key] = struct{}{}
		}
		return nil
	})
	if listErr != nil {
		return listErr
	}

	// Keys that could not be read before are tried again first.
	for key := range p.bad {
		k, ok := parseKey(p.prefix, key)
		if !ok || k.kind != kindDefinition {
			delete(p.bad, key)
			continue
		}
		if _, dup := listedDefs[key]; !dup {
			defs = append(defs, listedKey{key: key, k: k})
		}
	}

	if err := p.learnDefinitions(ctx, defs, t0); err != nil {
		return err
	}
	p.learnUndos(undos, t0)

	if full {
		p.confirmAbsent(ctx, listedAll)
		for key := range p.gone {
			if _, still := listedAll[key]; !still {
				delete(p.gone, key) // the key is gone from the bucket too
			}
		}
		for key := range p.unreadable {
			if _, still := listedAll[key]; !still {
				delete(p.unreadable, key) // removed from the bucket
			}
		}
		// A definition that was listed (or is being retried) but could not be read
		// in this pass still needs its undo: the undo key may be older than the next
		// poll's window, so dropping it here would leave the tombstone in force
		// until the next full listing.
		wanted := make(map[string]struct{}, len(defs))
		for _, d := range defs {
			if _, expired := p.gone[d.key]; !expired {
				wanted[d.key] = struct{}{}
			}
		}
		for defKey := range p.undoSeen {
			// An undo whose definition is in neither the view nor the listing is
			// not needed.
			in := false
			if k, ok := parseKey(p.prefix, defKey); ok {
				p.s.mu.RLock()
				e, known := p.s.byID[k.id]
				in = known && e.defKey == defKey
				p.s.mu.RUnlock()
			}
			if _, pending := wanted[defKey]; !in && !pending {
				delete(p.undoSeen, defKey)
			}
		}
		p.lastFull = t0
	}
	// Expiry and GC run only after a listing succeeded (above), on the pod's clock:
	// a failed first listing sweeps and collects nothing.
	p.sweepExpired(t0)
	if full && p.maintenance {
		p.gc(ctx, undos)
	}
	p.s.updateActiveGauges()

	metrics.DeleteTombstoneUnreadable.Set(int64(len(p.bad) + len(p.unreadable)))
	metrics.DeleteTombstoneSync.Inc(kind)
	p.lastList = t0
	p.firstDone = true
	p.publishUnreadable()
	if len(p.bad) > 0 {
		return fmt.Errorf("%d tombstone object(s) could not be read; the view is not complete", len(p.bad))
	}
	p.markSynced()
	return nil
}

// publishUnreadable makes the current unreadable set visible to Gate.
func (p *protocol) publishUnreadable() {
	snap := make(map[string]unreadableDef, len(p.unreadable))
	for k, v := range p.unreadable {
		snap[k] = v
	}
	p.stateMu.Lock()
	p.unreadableSnapshot = snap
	p.stateMu.Unlock()
}

// noteLearned records that a key with the given time was just learned. A key
// older than the poll window that a listing delivers only now is one the polls
// could not have listed: its creator's clock is behind this pod's, or this pod was
// not polling. It is counted, and the delay is exported, so skew shows up before
// it matters. The first listing (start, restart) is not "late".
func (p *protocol) noteLearned(keyNs int64, now time.Time) {
	if !p.firstDone {
		return
	}
	delay := now.Sub(time.Unix(0, keyNs))
	metrics.DeleteTombstoneKeySkew.Set(int64(delay / time.Second))
	// Late means the polls could not have listed it: it is older than the window
	// that followed the last successful listing. A key created while this pod's
	// listings were failing is not late, however long the outage lasted.
	if keyNs < p.lastList.Add(-pollWindow).UnixNano() {
		metrics.DeleteTombstonesLearnedLate.Inc()
	}
}

// learnDefinitions reads the definitions the view does not hold yet.
func (p *protocol) learnDefinitions(ctx context.Context, defs []listedKey, now time.Time) error {
	var need []listedKey
	var keys []string
	for _, d := range defs {
		p.s.mu.RLock()
		e, ok := p.s.byID[d.k.id]
		p.s.mu.RUnlock()
		if ok && e.defKey == d.key {
			continue // known
		}
		if _, expired := p.gone[d.key]; expired {
			continue // past retention: not read again
		}
		if ok && e.defKey != "" && e.defKey != d.key {
			metrics.DeleteTombstoneConflicts.Inc()
			if e.defKey < d.key {
				continue
			}
		}
		need = append(need, d)
		keys = append(keys, d.key)
	}
	if len(need) == 0 {
		return nil
	}
	got := p.fetch(ctx, keys)
	for _, d := range need {
		f := got[d.key]
		switch {
		case f.err != nil:
			p.markBad(d.key, "read: "+f.err.Error())
			continue
		case !f.found:
			// Listed, then gone (retention GC, or removed by hand): nothing to
			// enforce, and not a reason to distrust the view.
			delete(p.bad, d.key)
			continue
		}
		ts, err := p.decodeFor(d.k, f.body)
		if err != nil && f.fromCache {
			// A corrupt cache file: drop it and read the bucket's copy.
			f = p.refetch(ctx, d.key)
			switch {
			case f.err != nil:
				p.markBad(d.key, "read: "+f.err.Error())
				continue
			case !f.found:
				delete(p.bad, d.key)
				continue
			}
			ts, err = p.decodeFor(d.k, f.body)
		}
		if err != nil {
			p.markUnreadable(d.key, f.body, err)
			continue
		}
		delete(p.bad, d.key)
		delete(p.unreadable, d.key)
		p.s.mu.Lock()
		if cur, ok := p.s.byID[ts.ID]; ok && cur.defKey != "" && cur.defKey < d.key {
			// Two definitions under one id: the earlier key wins on every pod,
			// whatever order they are learned in.
			p.s.mu.Unlock()
			continue
		}
		p.s.putLocked(&entry{t: ts, defKey: d.key})
		p.s.mu.Unlock()
		delete(p.seeded, d.key)
		p.noteLearned(d.k.timeNs, now)
		if at, pending := p.undoSeen[d.key]; pending {
			if p.applyUndo(tombKey{kind: kindUndo, timeNs: at, id: ts.ID, defNs: d.k.timeNs}) {
				delete(p.undoSeen, d.key)
			}
		}
	}
	return ctx.Err()
}

// markBad records a definition that could not be read at all (a transport error).
func (p *protocol) markBad(key, why string) {
	if _, seen := p.bad[key]; !seen {
		logger.Errorf("tombstone object %s is unreadable (%s); reads fail closed once the view is stale", key, why)
	}
	p.bad[key] = why
}

// markUnreadable records a definition that was read but cannot be decoded. It
// closes the gate for the tenants its body names (best effort), for every tenant
// when the body does not even say.
func (p *protocol) markUnreadable(key string, body []byte, cause error) {
	tenants, ok := namedTenants(body)
	if _, seen := p.unreadable[key]; !seen {
		if ok {
			logger.Errorf("tombstone object %s cannot be decoded (%s); reads of its tenants fail closed until it is readable or removed", key, cause)
		} else {
			logger.Errorf("tombstone object %s cannot be decoded and names no tenant (%s); reads of every tenant fail closed until it is readable or removed", key, cause)
		}
	}
	p.unreadable[key] = unreadableDef{tenants: tenants, all: !ok, why: cause.Error()}
}

// learnUndos applies the undo keys listed. An undo whose definition is not known
// yet waits for it.
func (p *protocol) learnUndos(undos []listedKey, now time.Time) {
	for _, u := range undos {
		defKey := u.k.definitionKey(p.prefix)
		p.s.mu.RLock()
		e, known := p.s.byID[u.k.id]
		// An undo binds to the definition key it names: the view's definition of
		// the id may be another one (two creates of one id), which it does not undo.
		matches := known && e.defKey == defKey
		already := matches && e.undone
		p.s.mu.RUnlock()
		if already {
			continue
		}
		if !matches {
			p.undoSeen[defKey] = u.k.timeNs // waits for its own definition
			continue
		}
		if p.applyUndo(u.k) {
			p.noteLearned(u.k.timeNs, now)
		}
	}
}

// confirmAbsent handles what a full listing did not list: a tombstone of the view
// whose definition key is missing is HEAD-checked, and forgotten only when the
// store answers that the key does not exist. Any other answer keeps it. Cached
// objects the first listing did not list get the same check.
//
// The pass is bounded. A listing that is short without an error leaves most of
// the view "absent"; checking all of it would be a storm of requests inside the
// sync, with the view ageing meanwhile. At most max(100, 1 % of the view) keys are
// checked per listing, taken as a moving window so every key is eventually checked
// (lakehouse_delete_tombstone_confirm_deferred_total counts the rest).
func (p *protocol) confirmAbsent(ctx context.Context, listed map[string]struct{}) {
	type cand struct{ id, key string }
	var cands []cand
	p.s.mu.RLock()
	total := len(p.s.byID)
	for id, e := range p.s.byID {
		if e.defKey == "" {
			continue
		}
		if _, ok := listed[e.defKey]; !ok {
			cands = append(cands, cand{id, e.defKey})
		}
	}
	p.s.mu.RUnlock()
	var seeded []string
	for key := range p.seeded {
		if _, ok := listed[key]; ok {
			delete(p.seeded, key)
			continue
		}
		seeded = append(seeded, key)
	}
	budget := total / 100
	if budget < 100 {
		budget = 100
	}
	// The sample is a window that moves round the keys in a fixed order, so every
	// key is checked within len/budget listings without a random source.
	sort.Slice(cands, func(i, j int) bool { return cands[i].key < cands[j].key })
	sort.Strings(seeded)
	cands = rotated(cands, p.confirmCursor)
	seeded = rotated(seeded, p.confirmCursor)
	if over := len(cands) + len(seeded) - budget; over > 0 {
		p.confirmCursor += budget
		metrics.DeleteTombstoneConfirmDeferred.Add(over)
		if len(cands) > budget {
			cands = cands[:budget]
			seeded = nil
		} else if rest := budget - len(cands); len(seeded) > rest {
			seeded = seeded[:rest]
		}
	}

	var mu sync.Mutex
	var forget []string
	var dropCache []string
	work := make(chan func())
	var wg sync.WaitGroup
	for i := 0; i < fetchWorkers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for f := range work {
				f()
			}
		}()
	}
	for _, c := range cands {
		c := c
		work <- func() {
			if exists, err := p.b.Exists(ctx, c.key); err == nil && !exists {
				mu.Lock()
				forget = append(forget, c.id)
				dropCache = append(dropCache, c.key)
				mu.Unlock()
			}
		}
	}
	for _, key := range seeded {
		key := key
		work <- func() {
			if exists, err := p.b.Exists(ctx, key); err == nil && !exists {
				mu.Lock()
				dropCache = append(dropCache, key)
				mu.Unlock()
			}
		}
	}
	close(work)
	wg.Wait()
	if ctx.Err() != nil {
		return
	}
	p.s.forgetMany(forget)
	for _, id := range forget {
		metrics.DeleteTombstonesForgotten.Inc()
		logger.Warnf("tombstone %q: its definition key no longer exists in the bucket; it is no longer enforced", id)
	}
	for _, key := range dropCache {
		p.cache.remove(key)
		delete(p.seeded, key)
	}
}

// sweepExpired takes the tombstones whose retention has passed out of the view.
// The predicate is deterministic from the definition and the pod's clock, so
// every pod reaches it independently. A maintenance pod keeps their ids to remove
// the keys.
func (p *protocol) sweepExpired(now time.Time) {
	if p.expiry == nil {
		return
	}
	type gone struct{ id, defKey string }
	var drop []gone
	p.s.mu.RLock()
	for id, e := range p.s.byID {
		if at, ok := p.expiry(e.t); ok && now.After(at) {
			drop = append(drop, gone{id, e.defKey})
		}
	}
	p.s.mu.RUnlock()
	ids := make([]string, len(drop))
	for i, g := range drop {
		ids[i] = g.id
		if g.defKey != "" {
			p.gone[g.defKey] = struct{}{}
			if p.maintenance {
				p.expired[g.id] = g.defKey
			}
		}
	}
	p.s.forgetMany(ids)
}

// gc removes the keys of tombstones that left the view by retention: the undo
// keys first, the definition last, so a crash leaves the definition (and the work
// for the next pass), never keys without one. It runs on every maintenance pod
// (no placement: a repeated DELETE is a 404 and costs nothing), and only acts on
// the retention predicate; nothing the pod merely holds locally causes a DELETE.
func (p *protocol) gc(ctx context.Context, undos []listedKey) {
	if len(p.expired) == 0 {
		return
	}
	// Undo keys by the definition key they undo.
	undoByDef := map[string][]string{}
	for _, u := range undos {
		dk := u.k.definitionKey(p.prefix)
		undoByDef[dk] = append(undoByDef[dk], u.key)
	}
	n := 0
	for id, defKey := range p.expired {
		if n >= gcBatch || ctx.Err() != nil {
			return
		}
		n++
		keys := append(append([]string(nil), undoByDef[defKey]...), defKey)
		ok := true
		for _, key := range keys {
			if err := p.b.Delete(ctx, key); err != nil {
				logger.Warnf("tombstone gc: delete %s: %s", key, err)
				ok = false
				break
			}
			p.cache.remove(key)
			metrics.DeleteTombstoneGCDeleted.Inc()
		}
		if ok {
			delete(p.expired, id)
			delete(p.gone, defKey)
		}
	}
}

// ExpiryAt is the standard expiry of a tombstone: its range end plus the longest
// retention that can apply to any file (maxTTL) plus a day. Past that moment no
// file that held a row of the tombstone is still readable (the read path cuts its
// plans at the same retention, see retention.Policy), so the tombstone has
// nothing left to hide. ok is false when the sum overflows (a range that ends
// at the far future never expires).
func ExpiryAt(t Tombstone, maxTTL time.Duration) (time.Time, bool) {
	margin := int64(maxTTL + retentionMargin)
	if t.EndNs > math.MaxInt64-margin {
		return time.Time{}, false
	}
	return time.Unix(0, t.EndNs+margin), true
}

// Create makes a tombstone: it writes the definition key (once) and puts the
// tombstone in force on this pod. Other pods learn of it within one poll
// interval. The key's time is the task's own timestamp when its id is one
// (upstream's ids are the creating node's clock in nanoseconds), so the same task
// always lands on the same key; any other id takes this pod's clock.
//
// The same task created again with identical content is a success (the fan-out
// of one delete task to several nodes writes identical bytes); the same id with
// different content is refused with the error ErrTaskExists wraps.
func (s *TombstoneStore) Create(ctx context.Context, ts Tombstone) (Tombstone, error) {
	p := s.proto.Load()
	if p == nil {
		return Tombstone{}, ErrNoBackend
	}
	if ts.Mode == "" {
		ts.Mode = "hide"
	}
	if err := ts.Validate(); err != nil {
		return Tombstone{}, err
	}
	ts.Tenants = NormalizeTenants(ts.Tenants)
	keyNs, ok := idTime(ts.ID)
	if !ok {
		keyNs = p.now().UnixNano()
	}
	ts.CreatedAt = time.Unix(0, keyNs).UTC()
	body, err := encodeDefinition(ts)
	if err != nil {
		return Tombstone{}, err
	}

	// The id may be known already: from this pod's view, or from another pod's
	// key this pod has not listed yet.
	if cur, ok := s.state(ts.ID); ok {
		return s.sameOrExists(cur, ts, body)
	}
	key := formatKey(p.prefix, tombKey{kind: kindDefinition, timeNs: keyNs, id: ts.ID})
	created, err := p.b.PutIfAbsent(ctx, key, body)
	if err != nil {
		return Tombstone{}, fmt.Errorf("write tombstone: %w", err)
	}
	if !created {
		existing, found, gerr := p.b.Get(ctx, key)
		if gerr != nil || !found {
			return Tombstone{}, fmt.Errorf("read existing tombstone: %v", gerr)
		}
		if !bytes.Equal(existing, body) {
			return Tombstone{}, taskExistsError{id: ts.ID}
		}
	}
	p.cache.put(key, body)
	s.mu.Lock()
	s.putLocked(&entry{t: ts, defKey: key})
	s.mu.Unlock()
	s.updateActiveGauges()
	metrics.DeleteTombstonesTotal.Inc()
	return ts, nil
}

// sameOrExists answers a create whose id is already in the view: identical
// content is idempotent success.
func (s *TombstoneStore) sameOrExists(cur entry, want Tombstone, wantBody []byte) (Tombstone, error) {
	if cur.defKey != "" {
		have := cur.t
		have.CreatedAt = want.CreatedAt
		if b, err := encodeDefinition(have); err == nil && bytes.Equal(b, wantBody) && !cur.undone {
			return cur.t, nil
		}
	}
	return Tombstone{}, taskExistsError{id: want.ID}
}

// Undelete makes the rows of a hide-mode tombstone visible again, on every pod. It
// writes the undo event with If-None-Match, which polling pods learn. Permanent
// and auto tombstones are irreversible, like upstream's deletes: ErrNotReversible.
// An id that is unknown is ErrTombstoneNotFound. Undoing twice is a success.
func (s *TombstoneStore) Undelete(ctx context.Context, id string) error {
	return s.UndeleteIf(ctx, id, nil)
}

// UndeleteIf is Undelete for a caller that may only manage some tombstones: a
// tombstone allow rejects is reported as not found, after the pod has synced to
// learn it and before anything is written, so another tenant's delete can neither
// be seen nor undone through a pod that has not polled it yet. A nil allow
// allows every tombstone.
func (s *TombstoneStore) UndeleteIf(ctx context.Context, id string, allow func(*Tombstone) bool) error {
	p := s.proto.Load()
	if p == nil {
		return ErrNoBackend
	}
	e, ok := s.state(id)
	if !ok {
		// The delete may have been taken on another pod within the last poll
		// interval. One poll answers whether it exists. A poll that fails says
		// nothing about the id: that is an error to the caller (a gateway error),
		// not "not found". A poll that only reports an unreadable object still
		// learned what it could, so the id is looked up again either way.
		syncErr := p.sync(ctx, false)
		if e, ok = s.state(id); !ok && syncErr != nil {
			return fmt.Errorf("look up the delete: %w", syncErr)
		}
	}
	if !ok || (allow != nil && !allow(&e.t)) {
		return ErrTombstoneNotFound
	}
	if e.defKey == "" {
		return ErrNoBackend
	}
	if e.t.Mode != "hide" {
		return ErrNotReversible
	}
	dk, parsed := parseKey(p.prefix, e.defKey)
	if !parsed {
		return ErrNoBackend
	}
	undo := tombKey{kind: kindUndo, timeNs: p.now().UnixNano(), id: id, defNs: dk.timeNs}
	if _, err := p.b.PutIfAbsent(ctx, formatKey(p.prefix, undo), nil); err != nil {
		return fmt.Errorf("write undo: %w", err)
	}
	p.applyUndo(undo)
	s.updateActiveGauges()
	return nil
}

// activeProtocol is the protocol the process-wide gauges read.
var activeProtocol activeProto

type activeProto struct {
	mu sync.Mutex
	p  *protocol
}

func (a *activeProto) Store(p *protocol) {
	a.mu.Lock()
	a.p = p
	a.mu.Unlock()
}

func (a *activeProto) viewAgeSeconds() float64 {
	a.mu.Lock()
	p := a.p
	a.mu.Unlock()
	if p == nil {
		return 0
	}
	age, ok := p.viewAge()
	if !ok {
		// Never synced: the view is as old as the pod.
		return time.Since(p.attached).Seconds()
	}
	return age.Seconds()
}

func init() {
	metrics.NewGaugeFunc("lakehouse_delete_tombstone_view_age_seconds", activeProtocol.viewAgeSeconds)
}

// rotated returns xs starting at offset (taken modulo len(xs)), wrapping round.
func rotated[T any](xs []T, offset int) []T {
	if len(xs) == 0 {
		return xs
	}
	n := offset % len(xs)
	if n < 0 {
		n += len(xs)
	}
	return append(append(make([]T, 0, len(xs)), xs[n:]...), xs[:n]...)
}
