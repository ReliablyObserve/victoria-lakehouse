// Package deletetest is an in-memory object store for the tombstone protocol's
// tests (test-only, never shipped). One Bucket is shared by several Pods, each a
// delete.Backend with its own request counters and fault injection, so tests
// can run several pods (writers and readers) over one bucket and count what
// each of them asks the store.
//
// It models what the protocol depends on: If-None-Match creates (exactly one
// winner per key), lexical key order, ListObjectsV2 StartAfter, pages of a
// configurable size, and a silent short listing (a page that claims to be the
// last while keys are missing, which real stores can return). It has no way to
// overwrite a key: the protocol never needs one.
package deletetest

import (
	"context"
	"errors"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Bucket is the shared store.
type Bucket struct {
	mu       sync.Mutex
	objs     map[string][]byte
	now      time.Time
	pageSize int
	// noConditional makes PutIfAbsent behave like a store that ignores
	// If-None-Match: it overwrites. Used to prove the protocol never depends on
	// an overwrite being refused for correctness of the definition bytes.
	noConditional bool
	log           []Op
}

// Op is one request a pod made.
type Op struct {
	Pod, Kind, Key string
	Created        bool // for a PutIfAbsent: the key was created
	After          string
}

// NewBucket returns an empty bucket whose clock starts at the current time.
func NewBucket() *Bucket {
	return &Bucket{objs: map[string][]byte{}, now: time.Now(), pageSize: 1000}
}

// SetPageSize sets the number of keys a listing page holds.
func (b *Bucket) SetPageSize(n int) {
	b.mu.Lock()
	b.pageSize = n
	b.mu.Unlock()
}

// IgnoreIfNoneMatch makes creates behave like a store that ignores
// If-None-Match: a create of an existing key overwrites it and reports success.
func (b *Bucket) IgnoreIfNoneMatch(on bool) {
	b.mu.Lock()
	b.noConditional = on
	b.mu.Unlock()
}

// Advance moves the store's clock.
func (b *Bucket) Advance(d time.Duration) {
	b.mu.Lock()
	b.now = b.now.Add(d)
	b.mu.Unlock()
}

// Now is the store's clock.
func (b *Bucket) Now() time.Time {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.now
}

// Put writes key unconditionally, without counting it: it seeds a state.
func (b *Bucket) Put(key string, data []byte) {
	b.mu.Lock()
	b.objs[key] = append([]byte(nil), data...)
	b.mu.Unlock()
}

// Remove deletes key without counting it (an operator removing a key by hand).
func (b *Bucket) Remove(key string) {
	b.mu.Lock()
	delete(b.objs, key)
	b.mu.Unlock()
}

// Body returns the content of key.
func (b *Bucket) Body(key string) ([]byte, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	d, ok := b.objs[key]
	return append([]byte(nil), d...), ok
}

// Keys lists the keys under prefix, in order.
func (b *Bucket) Keys(prefix string) []string {
	b.mu.Lock()
	defer b.mu.Unlock()
	var out []string
	for k := range b.objs {
		if strings.HasPrefix(k, prefix) {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// Log returns every request pods made, in order.
func (b *Bucket) Log() []Op {
	b.mu.Lock()
	defer b.mu.Unlock()
	return append([]Op(nil), b.log...)
}

// Pod returns a Backend for one pod over the bucket.
func (b *Bucket) Pod(name string) *Pod { return &Pod{b: b, name: name} }

// Counts are a pod's request counters.
type Counts struct {
	ListPages, Gets, Heads, Puts, Deletes int64
}

// Pod is one pod's view of the bucket (a delete.Backend).
type Pod struct {
	b    *Bucket
	name string

	listPages, gets, heads, puts, deletes atomic.Int64
	skew                                  atomic.Int64 // ns this pod's clock is ahead

	mu      sync.Mutex
	shortN  int // >0: the next listing returns only this many keys, silently
	failOps map[string]int
}

// Counts returns the pod's request counters.
func (p *Pod) Counts() Counts {
	return Counts{p.listPages.Load(), p.gets.Load(), p.heads.Load(), p.puts.Load(), p.deletes.Load()}
}

// ResetCounts zeroes the counters.
func (p *Pod) ResetCounts() {
	p.listPages.Store(0)
	p.gets.Store(0)
	p.heads.Store(0)
	p.puts.Store(0)
	p.deletes.Store(0)
}

// ShortListOnce makes the pod's next listing return only its first n keys while
// claiming to be complete, the way a lagging or buggy store can.
func (p *Pod) ShortListOnce(n int) {
	p.mu.Lock()
	p.shortN = n
	p.mu.Unlock()
}

// FailNext makes the next n requests of kind ("list", "get", "head", "put",
// "delete") fail with an error.
func (p *Pod) FailNext(kind string, n int) {
	p.mu.Lock()
	if p.failOps == nil {
		p.failOps = map[string]int{}
	}
	p.failOps[kind] = n
	p.mu.Unlock()
}

var errInjected = errors.New("deletetest: injected failure")

func (p *Pod) fail(kind string) bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.failOps[kind] > 0 {
		p.failOps[kind]--
		return true
	}
	return false
}

func (p *Pod) record(op Op) {
	op.Pod = p.name
	p.b.mu.Lock()
	p.b.log = append(p.b.log, op)
	p.b.mu.Unlock()
}

// PutIfAbsent implements delete.Backend.
func (p *Pod) PutIfAbsent(_ context.Context, key string, data []byte) (bool, error) {
	p.puts.Add(1)
	if p.fail("put") {
		return false, errInjected
	}
	p.b.mu.Lock()
	_, exists := p.b.objs[key]
	created := !exists || p.b.noConditional
	if created {
		p.b.objs[key] = append([]byte(nil), data...)
	}
	p.b.mu.Unlock()
	p.record(Op{Kind: "PUT", Key: key, Created: created})
	return created, nil
}

// Get implements delete.Backend.
func (p *Pod) Get(_ context.Context, key string) ([]byte, bool, error) {
	p.gets.Add(1)
	if p.fail("get") {
		return nil, false, errInjected
	}
	p.record(Op{Kind: "GET", Key: key})
	d, ok := p.b.Body(key)
	return d, ok, nil
}

// Exists implements delete.Backend.
func (p *Pod) Exists(_ context.Context, key string) (bool, error) {
	p.heads.Add(1)
	if p.fail("head") {
		return false, errInjected
	}
	p.record(Op{Kind: "HEAD", Key: key})
	_, ok := p.b.Body(key)
	return ok, nil
}

// ListAfter implements delete.Backend.
func (p *Pod) ListAfter(_ context.Context, prefix, startAfter string, fn func(string) error) error {
	if p.fail("list") {
		p.listPages.Add(1)
		return errInjected
	}
	p.mu.Lock()
	short := p.shortN
	p.shortN = 0
	p.mu.Unlock()

	p.b.mu.Lock()
	var keys []string
	for k := range p.b.objs {
		if strings.HasPrefix(k, prefix) && k > startAfter {
			keys = append(keys, k)
		}
	}
	size := p.b.pageSize
	p.b.mu.Unlock()
	sort.Strings(keys)
	if short > 0 && short < len(keys) {
		keys = keys[:short]
	}
	pages := (len(keys) + size - 1) / size
	if pages == 0 {
		pages = 1
	}
	p.listPages.Add(int64(pages))
	p.record(Op{Kind: "LIST", Key: prefix, After: startAfter})
	for _, k := range keys {
		if err := fn(k); err != nil {
			return err
		}
	}
	return nil
}

// Delete implements delete.Backend.
func (p *Pod) Delete(_ context.Context, key string) error {
	p.deletes.Add(1)
	if p.fail("delete") {
		return errInjected
	}
	p.record(Op{Kind: "DELETE", Key: key})
	p.b.mu.Lock()
	delete(p.b.objs, key)
	p.b.mu.Unlock()
	return nil
}

// SetSkew makes this pod's clock run d ahead of the bucket's test clock (negative:
// behind), the way a pod with a wrong wall clock does.
func (p *Pod) SetSkew(d time.Duration) { p.skew.Store(int64(d)) }

// Now is this pod's clock: the bucket's test clock plus the pod's skew. Tests
// give it to the view as delete.Options.Now.
func (p *Pod) Now() time.Time { return p.b.Now().Add(time.Duration(p.skew.Load())) }
