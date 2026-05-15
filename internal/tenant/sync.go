package tenant

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"
)

type AliasDelta struct {
	NodeID  string       `json:"node_id"`
	Aliases []AliasEntry `json:"aliases"`
}

type SyncHandler struct {
	resolver *TenantResolver
	authKey  string
}

func NewSyncHandler(resolver *TenantResolver, authKey string) *SyncHandler {
	return &SyncHandler{resolver: resolver, authKey: authKey}
}

func (sh *SyncHandler) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "method not allowed", http.StatusMethodNotAllowed)
		return
	}

	if sh.authKey != "" {
		auth := r.Header.Get("Authorization")
		if auth != "Bearer "+sh.authKey {
			http.Error(w, "unauthorized", http.StatusUnauthorized)
			return
		}
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "read body: "+err.Error(), http.StatusBadRequest)
		return
	}

	var delta AliasDelta
	if err := json.Unmarshal(body, &delta); err != nil {
		http.Error(w, "bad json: "+err.Error(), http.StatusBadRequest)
		return
	}

	for _, ae := range delta.Aliases {
		if _, exists := sh.resolver.Resolve(ae.OrgID); !exists {
			_ = sh.resolver.AddAlias(ae.OrgID, TenantID{
				AccountID: ae.AccountID,
				ProjectID: ae.ProjectID,
			})
		}
	}

	w.WriteHeader(http.StatusOK)
}

type SyncPusherConfig struct {
	Resolver *TenantResolver
	GetPeers func() []string
	AuthKey  string
	SelfAddr string
	Interval time.Duration
}

type SyncPusher struct {
	cfg        SyncPusherConfig
	httpClient *http.Client
	mu         sync.Mutex
	lastSnap   map[string]TenantID
}

func NewSyncPusher(cfg SyncPusherConfig) *SyncPusher {
	if cfg.Interval == 0 {
		cfg.Interval = 30 * time.Second
	}
	return &SyncPusher{
		cfg:        cfg,
		httpClient: &http.Client{Timeout: 5 * time.Second},
		lastSnap:   make(map[string]TenantID),
	}
}

func (sp *SyncPusher) Start(ctx context.Context) {
	go func() {
		ticker := time.NewTicker(sp.cfg.Interval)
		defer ticker.Stop()
		for {
			select {
			case <-ticker.C:
				sp.Push(ctx)
			case <-ctx.Done():
				return
			}
		}
	}()
}

func (sp *SyncPusher) Push(ctx context.Context) {
	if ctx == nil {
		ctx = context.Background()
	}
	current := sp.cfg.Resolver.AllAliases()

	sp.mu.Lock()
	var changed []AliasEntry
	currentMap := make(map[string]TenantID, len(current))
	for _, ae := range current {
		tid := TenantID{AccountID: ae.AccountID, ProjectID: ae.ProjectID}
		currentMap[ae.OrgID] = tid
		if prev, ok := sp.lastSnap[ae.OrgID]; !ok || prev != tid {
			changed = append(changed, ae)
		}
	}
	sp.lastSnap = currentMap
	sp.mu.Unlock()

	if len(changed) == 0 {
		return
	}

	delta := AliasDelta{
		NodeID:  sp.cfg.SelfAddr,
		Aliases: changed,
	}

	body, err := json.Marshal(delta)
	if err != nil {
		return
	}

	peers := sp.cfg.GetPeers()
	for _, peer := range peers {
		if peer == sp.cfg.SelfAddr {
			continue
		}

		url := fmt.Sprintf("http://%s/internal/tenant/sync", peer)
		req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, bytes.NewReader(body))
		if err != nil {
			continue
		}
		req.Header.Set("Content-Type", "application/json")
		if sp.cfg.AuthKey != "" {
			req.Header.Set("Authorization", "Bearer "+sp.cfg.AuthKey)
		}

		resp, err := sp.httpClient.Do(req)
		if err != nil {
			continue
		}
		resp.Body.Close()
	}
}
