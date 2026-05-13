package stats

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
	"github.com/valyala/gozstd"
)

// ---------------------------------------------------------------------------
// SyncHandler — HTTP handler for receiving deltas from peers
// ---------------------------------------------------------------------------

// SyncHandler is an http.Handler that accepts TenantDelta payloads from
// peers and merges them into the local TenantRegistry using CRDT rules.
type SyncHandler struct {
	registry *TenantRegistry
	authKey  string
}

// NewSyncHandler creates a handler that merges incoming deltas into registry.
// If authKey is non-empty, requests must carry a matching Bearer token.
func NewSyncHandler(registry *TenantRegistry, authKey string) *SyncHandler {
	return &SyncHandler{
		registry: registry,
		authKey:  authKey,
	}
}

// ServeHTTP implements http.Handler.
func (sh *SyncHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	// 1. Only accept POST.
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	// 2. Auth check.
	if sh.authKey != "" {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+sh.authKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	// 3. Read body.
	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	// 4. Decompress if ZSTD.
	if r.Header.Get("Content-Encoding") == "zstd" {
		body, err = decompressZSTD(body)
		if err != nil {
			http.Error(w, "decompress: "+err.Error(), http.StatusBadRequest)
			return
		}
	}

	// 5. Unmarshal using the JSON-safe mirror type.
	var dj tenantDeltaJSON
	if err := json.Unmarshal(body, &dj); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}

	delta := tenantDeltaFromJSON(dj)

	// 6. Merge.
	sh.registry.Merge(delta)

	// 7. Metrics.
	metrics.StatsMergesTotal.Inc()

	w.WriteHeader(http.StatusOK)
}

// ---------------------------------------------------------------------------
// SyncPusher — pushes deltas to peers
// ---------------------------------------------------------------------------

// SyncPusherConfig holds the configuration for a SyncPusher.
type SyncPusherConfig struct {
	Registry *TenantRegistry
	GetPeers func() []string
	AuthKey  string
	SelfAddr string
	Compress bool
}

// SyncPusher periodically pushes TenantDelta payloads to peer nodes.
type SyncPusher struct {
	cfg        SyncPusherConfig
	httpClient *http.Client
}

// NewSyncPusher creates a pusher with the given configuration.
func NewSyncPusher(cfg SyncPusherConfig) *SyncPusher {
	return &SyncPusher{
		cfg:        cfg,
		httpClient: &http.Client{},
	}
}

// PushDelta sends only the changes since the last successful push.
func (sp *SyncPusher) PushDelta(ctx context.Context) error {
	delta := sp.cfg.Registry.BuildDelta(sp.cfg.Registry.LastPushGen())

	// No changes — nothing to send.
	if len(delta.Tenants) == 0 {
		return nil
	}

	return sp.push(ctx, delta)
}

// PushFull sends the entire registry state (BuildDelta from generation 0).
func (sp *SyncPusher) PushFull(ctx context.Context) error {
	delta := sp.cfg.Registry.BuildDelta(0)

	if len(delta.Tenants) == 0 {
		return nil
	}

	return sp.push(ctx, delta)
}

// push marshals and sends the delta to all peers.
func (sp *SyncPusher) push(ctx context.Context, delta *TenantDelta) error {
	// Marshal via the JSON-safe mirror type.
	dj := tenantDeltaToJSON(delta)
	body, err := json.Marshal(dj)
	if err != nil {
		return fmt.Errorf("marshal delta: %w", err)
	}

	compressed := false
	if sp.cfg.Compress {
		body = compressZSTD(body)
		compressed = true
	}

	peers := sp.cfg.GetPeers()
	var lastErr error

	for _, peer := range peers {
		if peer == sp.cfg.SelfAddr {
			continue
		}

		req, err := http.NewRequestWithContext(ctx, http.MethodPost, peer, bytes.NewReader(body))
		if err != nil {
			metrics.StatsPushErrors.Inc()
			lastErr = err
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if compressed {
			req.Header.Set("Content-Encoding", "zstd")
		}
		if sp.cfg.AuthKey != "" {
			req.Header.Set("Authorization", "Bearer "+sp.cfg.AuthKey)
		}

		resp, err := sp.httpClient.Do(req)
		if err != nil {
			metrics.StatsPushErrors.Inc()
			lastErr = err
			continue
		}
		resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			metrics.StatsPushErrors.Inc()
			lastErr = fmt.Errorf("peer %s returned %d", peer, resp.StatusCode)
			continue
		}

		metrics.StatsPushTotal.Inc()
		metrics.StatsPushBytesTotal.Add(len(body))
	}

	// Update generation tracker on success (at least one peer succeeded or no
	// peers were contacted — both mean we've consumed the delta).
	if lastErr == nil {
		sp.cfg.Registry.SetLastPushGen(delta.Generation)
	}

	return lastErr
}

// ---------------------------------------------------------------------------
// TenantDelta JSON conversion helpers
// ---------------------------------------------------------------------------

// tenantDeltaToJSON converts a TenantDelta (with unexported fields in
// TenantStats) to the fully-serialisable tenantDeltaJSON mirror.
func tenantDeltaToJSON(d *TenantDelta) tenantDeltaJSON {
	tj := tenantDeltaJSON{
		NodeID:     d.NodeID,
		Generation: d.Generation,
		Tenants:    make(map[string]tenantStatsJSON, len(d.Tenants)),
		Timestamp:  d.Timestamp,
	}
	for k, ts := range d.Tenants {
		tj.Tenants[k] = ts.toJSON()
	}
	return tj
}

// tenantDeltaFromJSON converts a tenantDeltaJSON back to a TenantDelta.
func tenantDeltaFromJSON(dj tenantDeltaJSON) *TenantDelta {
	d := &TenantDelta{
		NodeID:     dj.NodeID,
		Generation: dj.Generation,
		Tenants:    make(map[string]*TenantStats, len(dj.Tenants)),
		Timestamp:  dj.Timestamp,
	}
	for k, j := range dj.Tenants {
		d.Tenants[k] = tenantStatsFromJSON(j)
	}
	return d
}

// ---------------------------------------------------------------------------
// ZSTD compression helpers
// ---------------------------------------------------------------------------

// compressZSTD compresses data using ZSTD.
func compressZSTD(data []byte) []byte {
	return gozstd.Compress(nil, data)
}

// decompressZSTD decompresses ZSTD-encoded data.
func decompressZSTD(data []byte) ([]byte, error) {
	return gozstd.Decompress(nil, data)
}
