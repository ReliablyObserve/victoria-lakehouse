package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/url"
	"strconv"
	"sync"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// checkPeerTenantScope verifies that the peer honoured the tenant scope of the
// request. A peer running a build without tenant-scoped buffer queries does not
// echo the header, so its rows are untrusted: the fan-out fails closed and drops
// the whole answer rather than merging rows it cannot attribute. A mixed-version
// fleet therefore loses the unflushed window from old peers (they are still in
// the cold tier after the next flush) instead of leaking across tenants.
func checkPeerTenantScope(resp *http.Response, scope tenantScope) error {
	got := resp.Header.Get(buffer.TenantScopeHeader)
	if want := bufferScopeString(scope); got != want {
		return fmt.Errorf("peer did not scope /internal/buffer/query to tenant %s (echoed %q); dropping its rows", want, got)
	}
	return nil
}

// bufferScopeString is the tenant-scope value the buffer handler echoes for
// this scope.
func bufferScopeString(scope tenantScope) string {
	if scope.all {
		return buffer.AllTenantsScope
	}
	return scope.account + ":" + scope.project
}

// bufferQueryURL builds the tenant-scoped /internal/buffer/query URL: one
// tenant's account_id/project_id, or all_tenants=true for a cross-tenant read
// that already passed the global-read check.
func (b *BufferBridge) bufferQueryURL(endpoint string, startNs, endNs int64, scope tenantScope) string {
	q := url.Values{}
	q.Set("start", strconv.FormatInt(startNs, 10))
	q.Set("end", strconv.FormatInt(endNs, 10))
	q.Set("mode", string(b.mode))
	q.Set("tenant_scope", buffer.TenantScopeVersion)
	if scope.all {
		q.Set("all_tenants", "true")
	} else {
		q.Set("account_id", scope.account)
		q.Set("project_id", scope.project)
	}
	return endpoint + "/internal/buffer/query?" + q.Encode()
}

// BufferBridge queries insert pods for unflushed data via parallel fan-out.
// Select pods use this to achieve zero-delay reads by merging buffered rows
// from insert pods with already-flushed Parquet data from S3.
type BufferBridge struct {
	cfg              *config.SelectConfig
	mode             config.Mode
	client           *http.Client
	mu               sync.RWMutex
	endpoints        []string
	sameAZEndpoints  []string
	crossAZEndpoints []string
	selfAZ           string

	// selfEndpoint is the local pod's own buffer-query URL, used as
	// a fallback when peer discovery yields no other endpoints (the
	// single-node / topology=all case). Set once at startup from
	// lakehouse-{logs,traces}/main.go; non-empty means "query self
	// when no peers are present, otherwise stay out of the way".
	// Without this, a single-node deployment never serves its own
	// unflushed buffer — queries against the last <flush-interval>
	// window return zero data until the next flush.
	selfEndpoint string
}

// NewBufferBridge creates a BufferBridge configured for the given mode.
func NewBufferBridge(cfg *config.SelectConfig, mode config.Mode) *BufferBridge {
	return &BufferBridge{
		cfg:  cfg,
		mode: mode,
		client: &http.Client{
			Timeout: cfg.BufferQueryTimeout,
		},
	}
}

// SetEndpoints updates the list of insert pod endpoints to query.
// Typically called by the DNS discovery loop when the headless service resolves.
func (b *BufferBridge) SetEndpoints(endpoints []string) {
	b.mu.Lock()
	b.endpoints = endpoints
	b.mu.Unlock()
}

// SetEndpointsWithZones updates insert pod endpoints with AZ classification.
func (b *BufferBridge) SetEndpointsWithZones(epZones map[string]string, selfAZ string) {
	b.mu.Lock()
	defer b.mu.Unlock()

	b.selfAZ = selfAZ
	b.endpoints = make([]string, 0, len(epZones))
	b.sameAZEndpoints = nil
	b.crossAZEndpoints = nil

	for ep, zone := range epZones {
		b.endpoints = append(b.endpoints, ep)
		if zone == selfAZ {
			b.sameAZEndpoints = append(b.sameAZEndpoints, ep)
		} else {
			b.crossAZEndpoints = append(b.crossAZEndpoints, ep)
		}
	}
}

// HasPeers reports whether real peer insert pods have been discovered (i.e.
// this is a multi-node deployment, not the single-node selfEndpoint fallback).
// The Option B read path uses the local logstorage buffer directly ONLY when
// there are no peers; with peers it falls through to the HTTP fan-out so every
// pod's unflushed rows are gathered (each pod's /internal/buffer/query handler
// returns its own buffer), avoiding the need to identify+exclude self from the
// peer list (which DNS discovery returns unfiltered).
func (b *BufferBridge) HasPeers() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return len(b.endpoints) > 0
}

// SetSelfEndpoint records the local pod's own buffer-query URL.
// When peer discovery yields zero endpoints — the single-node
// topology=all case — getQueryEndpoints returns [selfEndpoint]
// so queries still see this pod's unflushed buffer. Once real
// peers appear in SetEndpoints, the self fallback is silently
// suppressed (peer discovery is the source of truth in a cluster).
func (b *BufferBridge) SetSelfEndpoint(endpoint string) {
	b.mu.Lock()
	b.selfEndpoint = endpoint
	b.mu.Unlock()
}

// getQueryEndpoints always returns ALL insert pod endpoints regardless of AZ.
// Buffer queries must reach every insert pod to avoid missing unflushed data —
// with 3 AZs, same-AZ-only would miss 2/3 of buffered rows.
// AZ-aware routing is only appropriate for peer cache (L3), not buffer queries.
//
// Falls back to [selfEndpoint] when endpoints is empty AND selfEndpoint is
// set. Single-node deployments rely on this to serve their own buffer; a
// configured cluster overrides it once SetEndpoints populates real peers.
func (b *BufferBridge) getQueryEndpoints() []string {
	if len(b.endpoints) == 0 && b.selfEndpoint != "" {
		return []string{b.selfEndpoint}
	}
	return b.endpoints
}

// QueryLogs fans out to all insert pod endpoints in parallel and returns
// the merged set of buffered log rows within the given time range.
// Endpoint errors are silently ignored for graceful degradation.
func (b *BufferBridge) QueryLogs(ctx context.Context, startNs, endNs int64, scope tenantScope) ([]schema.LogRow, error) {
	if !b.cfg.BufferQueryEnabled {
		return nil, nil
	}

	b.mu.RLock()
	eps := b.getQueryEndpoints()
	b.mu.RUnlock()

	if len(eps) == 0 {
		return nil, nil
	}

	var mu sync.Mutex
	var all []schema.LogRow
	var wg sync.WaitGroup

	for _, ep := range eps {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			rows, err := b.fetchLogs(ctx, endpoint, startNs, endNs, scope)
			if err != nil {
				return
			}
			mu.Lock()
			all = append(all, rows...)
			mu.Unlock()
		}(ep)
	}
	wg.Wait()

	return all, nil
}

func (b *BufferBridge) fetchLogs(ctx context.Context, endpoint string, startNs, endNs int64, scope tenantScope) ([]schema.LogRow, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.bufferQueryURL(endpoint, startNs, endNs, scope), nil)
	if err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("buffer query returned %d", resp.StatusCode)
	}
	if err := checkPeerTenantScope(resp, scope); err != nil {
		return nil, err
	}

	var rows []schema.LogRow
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var row schema.LogRow
		if err := dec.Decode(&row); err != nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}

// QueryTraces fans out to all insert pod endpoints in parallel and returns
// the merged set of buffered trace rows within the given time range.
// Endpoint errors are silently ignored for graceful degradation.
func (b *BufferBridge) QueryTraces(ctx context.Context, startNs, endNs int64, scope tenantScope) ([]schema.TraceRow, error) {
	if !b.cfg.BufferQueryEnabled {
		return nil, nil
	}

	b.mu.RLock()
	eps := b.getQueryEndpoints()
	b.mu.RUnlock()

	if len(eps) == 0 {
		return nil, nil
	}

	var mu sync.Mutex
	var all []schema.TraceRow
	var wg sync.WaitGroup

	for _, ep := range eps {
		wg.Add(1)
		go func(endpoint string) {
			defer wg.Done()
			rows, err := b.fetchTraces(ctx, endpoint, startNs, endNs, scope)
			if err != nil {
				return
			}
			mu.Lock()
			all = append(all, rows...)
			mu.Unlock()
		}(ep)
	}
	wg.Wait()

	return all, nil
}

func (b *BufferBridge) fetchTraces(ctx context.Context, endpoint string, startNs, endNs int64, scope tenantScope) ([]schema.TraceRow, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.bufferQueryURL(endpoint, startNs, endNs, scope), nil)
	if err != nil {
		return nil, err
	}

	resp, err := b.client.Do(req)
	if err != nil {
		return nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("buffer query returned %d", resp.StatusCode)
	}
	if err := checkPeerTenantScope(resp, scope); err != nil {
		return nil, err
	}

	var rows []schema.TraceRow
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var row schema.TraceRow
		if err := dec.Decode(&row); err != nil {
			break
		}
		rows = append(rows, row)
	}
	return rows, nil
}
