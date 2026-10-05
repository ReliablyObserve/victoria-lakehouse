package parquets3

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/buffer"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
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

// bridgeScopes splits a scope into the scopes one /internal/buffer/query can
// express — every tenant, or exactly one tenant — so a request that names
// several tenants asks each peer once per tenant and never for more.
func bridgeScopes(scope tenantScope) []tenantScope {
	if scope.all || scope.single() {
		return []tenantScope{scope}
	}
	pairs := scope.pairs()
	out := make([]tenantScope, 0, len(pairs))
	for _, p := range pairs {
		out = append(out, tenantScope{account: p.account, project: p.project})
	}
	return out
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

	// authKey is peer.auth_key, sent as Authorization: Bearer on every
	// request. An insert pod with a key refuses a request without it (401),
	// and every pod refuses all_tenants=true to a caller that has none.
	authKey string
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

// newBufferBridgeFor is the bridge a Storage built from cfg reads the insert
// pods' unflushed rows with: nil unless the pod serves selects with
// select.buffer_query_enabled, and carrying peer.auth_key, the key the insert
// pods' /internal/buffer/query checks.
func newBufferBridgeFor(cfg *config.Config) *BufferBridge {
	if !cfg.SelectEnabled() || !cfg.Select.BufferQueryEnabled {
		return nil
	}
	b := NewBufferBridge(&cfg.Select, cfg.Mode)
	b.SetAuthKey(cfg.Peer.AuthKey)
	return b
}

// SetAuthKey sets the peer key the bridge presents to the insert pods.
func (b *BufferBridge) SetAuthKey(key string) {
	b.mu.Lock()
	b.authKey = key
	b.mu.Unlock()
}

// bridgeEndpoint makes a discovered "host:port" a URL the bridge can request:
// DNS discovery yields addresses without a scheme, and a request to
// "host:port/internal/buffer/query" does not parse.
func bridgeEndpoint(ep string) string {
	if strings.Contains(ep, "://") {
		return ep
	}
	return "http://" + ep
}

// SetEndpoints updates the list of insert pod endpoints to query.
// Typically called by the DNS discovery loop when the headless service resolves.
func (b *BufferBridge) SetEndpoints(endpoints []string) {
	eps := make([]string, 0, len(endpoints))
	for _, ep := range endpoints {
		eps = append(eps, bridgeEndpoint(ep))
	}
	b.mu.Lock()
	b.endpoints = eps
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
		ep = bridgeEndpoint(ep)
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

// QueryLogs fans out to every insert pod in parallel and returns their
// unflushed log rows in [startNs, endNs], and the nonces of the buffer
// segments the rows were read from (the caller drops those segments' objects
// from its scan). A peer that fails is left out of both — its unflushed rows
// are missing from the answer, and none of its objects is excluded, so no row
// is answered twice.
func (b *BufferBridge) QueryLogs(ctx context.Context, startNs, endNs int64, scope tenantScope) ([]schema.LogRow, map[string]struct{}) {
	return queryPeers[schema.LogRow](b, ctx, startNs, endNs, scope)
}

// QueryTraces is QueryLogs for spans.
func (b *BufferBridge) QueryTraces(ctx context.Context, startNs, endNs int64, scope tenantScope) ([]schema.TraceRow, map[string]struct{}) {
	return queryPeers[schema.TraceRow](b, ctx, startNs, endNs, scope)
}

func queryPeers[T any](b *BufferBridge, ctx context.Context, startNs, endNs int64, scope tenantScope) ([]T, map[string]struct{}) {
	if !b.cfg.BufferQueryEnabled {
		return nil, nil
	}
	b.mu.RLock()
	eps := b.getQueryEndpoints()
	authKey := b.authKey
	b.mu.RUnlock()
	if len(eps) == 0 {
		return nil, nil
	}

	var mu sync.Mutex
	var all []T
	nonces := map[string]struct{}{}
	var wg sync.WaitGroup
	for _, ep := range eps {
		for _, sub := range bridgeScopes(scope) {
			wg.Add(1)
			go func(endpoint string, sub tenantScope) {
				defer wg.Done()
				rows, segs, err := fetchPeer[T](b, ctx, endpoint, authKey, startNs, endNs, sub)
				if err != nil {
					if ctx.Err() == nil {
						logger.Warnf("buffer bridge: %s; the peer's unflushed rows are missing from this answer", err)
					}
					return
				}
				mu.Lock()
				all = append(all, rows...)
				for _, n := range segs {
					if n != "" {
						nonces[n] = struct{}{}
					}
				}
				mu.Unlock()
			}(ep, sub)
		}
	}
	wg.Wait()
	return all, nonces
}

func fetchPeer[T any](b *BufferBridge, ctx context.Context, endpoint, authKey string, startNs, endNs int64, scope tenantScope) ([]T, []string, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, b.bufferQueryURL(endpoint, startNs, endNs, scope), nil)
	if err != nil {
		return nil, nil, err
	}
	if authKey != "" {
		req.Header.Set("Authorization", "Bearer "+authKey)
	}
	resp, err := b.client.Do(req)
	if err != nil {
		metrics.BufferBridgeErrors.Inc("request")
		return nil, nil, err
	}
	defer func() { _ = resp.Body.Close() }()

	switch resp.StatusCode {
	case http.StatusOK:
	case http.StatusUnauthorized, http.StatusForbidden:
		// The peer refused this pod's credential: peer.auth_key differs
		// between the pods, or an all_tenants read reached a pod without
		// one. That is a configuration error, not a smaller answer, and it
		// is counted apart from other failures so it can be alerted on.
		metrics.BufferBridgeErrors.Inc("auth")
		return nil, nil, fmt.Errorf("buffer query to %s refused this pod's peer key (%d %s): set the same peer.auth_key on every pod",
			endpoint, resp.StatusCode, readErrorBody(resp))
	default:
		metrics.BufferBridgeErrors.Inc("status")
		return nil, nil, fmt.Errorf("buffer query returned %d", resp.StatusCode)
	}
	if err := checkPeerTenantScope(resp, scope); err != nil {
		metrics.BufferBridgeErrors.Inc("scope")
		return nil, nil, err
	}

	var rows []T
	dec := json.NewDecoder(resp.Body)
	for dec.More() {
		var row T
		if err := dec.Decode(&row); err != nil {
			// A stream that breaks off is not a smaller answer: returning
			// the rows read so far would count part of this peer's
			// unflushed window as all of it.
			metrics.BufferBridgeErrors.Inc("decode")
			return nil, nil, fmt.Errorf("buffer query from %s broke off after %d rows: %w", endpoint, len(rows), err)
		}
		rows = append(rows, row)
	}
	return rows, buffer.ParseSegments(resp.Header.Get(buffer.SegmentsHeader)), nil
}

// readErrorBody returns the first line of an error answer, bounded, for the
// log line that reports it.
func readErrorBody(resp *http.Response) string {
	b, _ := io.ReadAll(io.LimitReader(resp.Body, 256))
	line, _, _ := strings.Cut(strings.TrimSpace(string(b)), "\n")
	return line
}
