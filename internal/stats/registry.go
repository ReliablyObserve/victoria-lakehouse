package stats

import (
	"encoding/json"
	"sort"
	"sync"
	"time"
)

// TenantStats holds per-tenant storage statistics. The exported fields are
// the public API; the unexported nodeBytes/nodeRows/nodeFiles maps track
// per-node contributions for CRDT counter merge.
type TenantStats struct {
	AccountID    string           `json:"account_id"`
	ProjectID    string           `json:"project_id"`
	TotalFiles   int64            `json:"total_files"`
	TotalBytes   int64            `json:"total_bytes"`
	RawBytes     int64            `json:"raw_bytes"`
	TotalRows    int64            `json:"total_rows"`
	Partitions   int              `json:"partitions"`
	MinTimeNs    int64            `json:"min_time_ns"`
	MaxTimeNs    int64            `json:"max_time_ns"`
	LastWriteAt  time.Time        `json:"last_write_at"`
	LastQueryAt  time.Time        `json:"last_query_at"`
	Labels       map[string]int   `json:"labels,omitempty"`
	BytesByClass map[string]int64 `json:"bytes_by_class,omitempty"`
	FilesByClass map[string]int64 `json:"files_by_class,omitempty"`
	NodeContribs map[string]int64 `json:"node_contribs,omitempty"`
	// Internal: tracks per-node bytes/rows/files for CRDT sum.
	nodeBytes map[string]int64
	nodeRows  map[string]int64
	nodeFiles map[string]int64
}

// tenantStatsJSON is the JSON-serialisable mirror of TenantStats including
// the unexported per-node tracking maps.
type tenantStatsJSON struct {
	AccountID    string           `json:"account_id"`
	ProjectID    string           `json:"project_id"`
	TotalFiles   int64            `json:"total_files"`
	TotalBytes   int64            `json:"total_bytes"`
	RawBytes     int64            `json:"raw_bytes"`
	TotalRows    int64            `json:"total_rows"`
	Partitions   int              `json:"partitions"`
	MinTimeNs    int64            `json:"min_time_ns"`
	MaxTimeNs    int64            `json:"max_time_ns"`
	LastWriteAt  time.Time        `json:"last_write_at"`
	LastQueryAt  time.Time        `json:"last_query_at"`
	Labels       map[string]int   `json:"labels,omitempty"`
	BytesByClass map[string]int64 `json:"bytes_by_class,omitempty"`
	FilesByClass map[string]int64 `json:"files_by_class,omitempty"`
	NodeContribs map[string]int64 `json:"node_contribs,omitempty"`
	NodeBytes    map[string]int64 `json:"node_bytes,omitempty"`
	NodeRows     map[string]int64 `json:"node_rows,omitempty"`
	NodeFiles    map[string]int64 `json:"node_files,omitempty"`
}

func (ts *TenantStats) toJSON() tenantStatsJSON {
	return tenantStatsJSON{
		AccountID:    ts.AccountID,
		ProjectID:    ts.ProjectID,
		TotalFiles:   ts.TotalFiles,
		TotalBytes:   ts.TotalBytes,
		RawBytes:     ts.RawBytes,
		TotalRows:    ts.TotalRows,
		Partitions:   ts.Partitions,
		MinTimeNs:    ts.MinTimeNs,
		MaxTimeNs:    ts.MaxTimeNs,
		LastWriteAt:  ts.LastWriteAt,
		LastQueryAt:  ts.LastQueryAt,
		Labels:       ts.Labels,
		BytesByClass: ts.BytesByClass,
		FilesByClass: ts.FilesByClass,
		NodeContribs: ts.NodeContribs,
		NodeBytes:    ts.nodeBytes,
		NodeRows:     ts.nodeRows,
		NodeFiles:    ts.nodeFiles,
	}
}

func tenantStatsFromJSON(j tenantStatsJSON) *TenantStats {
	return &TenantStats{
		AccountID:    j.AccountID,
		ProjectID:    j.ProjectID,
		TotalFiles:   j.TotalFiles,
		TotalBytes:   j.TotalBytes,
		RawBytes:     j.RawBytes,
		TotalRows:    j.TotalRows,
		Partitions:   j.Partitions,
		MinTimeNs:    j.MinTimeNs,
		MaxTimeNs:    j.MaxTimeNs,
		LastWriteAt:  j.LastWriteAt,
		LastQueryAt:  j.LastQueryAt,
		Labels:       j.Labels,
		BytesByClass: j.BytesByClass,
		FilesByClass: j.FilesByClass,
		NodeContribs: j.NodeContribs,
		nodeBytes:    j.NodeBytes,
		nodeRows:     j.NodeRows,
		nodeFiles:    j.NodeFiles,
	}
}

// TenantDelta is the unit of gossip exchanged between peers.
type TenantDelta struct {
	NodeID     string                  `json:"node_id"`
	Generation uint64                  `json:"generation"`
	Tenants    map[string]*TenantStats `json:"tenants"`
	Timestamp  time.Time               `json:"timestamp"`
}

// tenantDeltaJSON is the JSON-serialisable mirror of TenantDelta.
type tenantDeltaJSON struct {
	NodeID     string                     `json:"node_id"`
	Generation uint64                     `json:"generation"`
	Tenants    map[string]tenantStatsJSON `json:"tenants"`
	Timestamp  time.Time                  `json:"timestamp"`
}

// GlobalStats aggregates stats across all tenants.
type GlobalStats struct {
	TotalFiles   int64            `json:"total_files"`
	TotalBytes   int64            `json:"total_bytes"`
	RawBytes     int64            `json:"raw_bytes"`
	TotalRows    int64            `json:"total_rows"`
	TenantCount  int              `json:"tenant_count"`
	BytesByClass map[string]int64 `json:"bytes_by_class"`
	FilesByClass map[string]int64 `json:"files_by_class"`
}

// TenantRegistry is a concurrency-safe registry of per-tenant statistics
// with CRDT merge support for multi-node convergence.
type TenantRegistry struct {
	mu               sync.RWMutex
	tenants          map[string]*TenantStats
	nodeID           string
	generation       uint64
	lastPushGen      uint64
	tenantGeneration map[string]uint64 // tenant key -> generation when last changed
}

// NewTenantRegistry creates a new registry for the given node.
// The lifecycle and pricing parameters are accepted for future use but
// not stored — callers wire StorageClassTracker and CostCalculator separately.
func NewTenantRegistry(nodeID string) *TenantRegistry {
	return &TenantRegistry{
		tenants:          make(map[string]*TenantStats),
		nodeID:           nodeID,
		tenantGeneration: make(map[string]uint64),
	}
}

// RecordWrite records a write for the given tenant.
func (r *TenantRegistry) RecordWrite(tenant string, bytes, rawBytes, rows int64, storageClass string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.generation++
	r.tenantGeneration[tenant] = r.generation

	ts := r.getOrCreate(tenant)

	// Per-node tracking for CRDT counters.
	ts.nodeBytes[r.nodeID] += bytes
	ts.nodeRows[r.nodeID] += rows
	ts.nodeFiles[r.nodeID]++

	// Recalculate totals from per-node maps.
	ts.TotalBytes = sumMap(ts.nodeBytes)
	ts.RawBytes += rawBytes
	ts.TotalRows = sumMap(ts.nodeRows)
	ts.TotalFiles = sumMap(ts.nodeFiles)

	if storageClass != "" {
		ts.BytesByClass[storageClass] += bytes
		ts.FilesByClass[storageClass]++
	}

	now := time.Now()
	ts.LastWriteAt = now

	// Initialise time bounds on first write.
	if ts.MinTimeNs == 0 {
		ts.MinTimeNs = now.UnixNano()
	}
	ts.MaxTimeNs = now.UnixNano()

	ts.NodeContribs[r.nodeID] += bytes
}

// RecordQuery updates the last-query timestamp for a tenant.
func (r *TenantRegistry) RecordQuery(tenant string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.generation++
	r.tenantGeneration[tenant] = r.generation

	ts := r.getOrCreate(tenant)
	ts.LastQueryAt = time.Now()
}

// Get returns a deep copy of the stats for the named tenant, or nil.
func (r *TenantRegistry) Get(tenant string) *TenantStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	ts, ok := r.tenants[tenant]
	if !ok {
		return nil
	}
	return deepCopyStats(ts)
}

// All returns deep copies of all tenants sorted by TotalBytes descending.
func (r *TenantRegistry) All() []*TenantStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	out := make([]*TenantStats, 0, len(r.tenants))
	for _, ts := range r.tenants {
		out = append(out, deepCopyStats(ts))
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].TotalBytes > out[j].TotalBytes
	})
	return out
}

// GlobalAggregates sums stats across all tenants.
func (r *TenantRegistry) GlobalAggregates() *GlobalStats {
	r.mu.RLock()
	defer r.mu.RUnlock()

	gs := &GlobalStats{
		TenantCount:  len(r.tenants),
		BytesByClass: make(map[string]int64),
		FilesByClass: make(map[string]int64),
	}
	for _, ts := range r.tenants {
		gs.TotalFiles += ts.TotalFiles
		gs.TotalBytes += ts.TotalBytes
		gs.RawBytes += ts.RawBytes
		gs.TotalRows += ts.TotalRows
		for c, b := range ts.BytesByClass {
			gs.BytesByClass[c] += b
		}
		for c, f := range ts.FilesByClass {
			gs.FilesByClass[c] += f
		}
	}
	return gs
}

// BuildDelta produces a delta containing only tenants whose generation
// exceeds sinceGeneration.
func (r *TenantRegistry) BuildDelta(sinceGeneration uint64) *TenantDelta {
	r.mu.RLock()
	defer r.mu.RUnlock()

	d := &TenantDelta{
		NodeID:     r.nodeID,
		Generation: r.generation,
		Tenants:    make(map[string]*TenantStats),
		Timestamp:  time.Now(),
	}
	for key, gen := range r.tenantGeneration {
		if gen > sinceGeneration {
			if ts, ok := r.tenants[key]; ok {
				d.Tenants[key] = deepCopyStats(ts)
			}
		}
	}
	return d
}

// Merge applies a remote delta using CRDT merge rules.
func (r *TenantRegistry) Merge(delta *TenantDelta) {
	if delta == nil || len(delta.Tenants) == 0 {
		return
	}

	r.mu.Lock()
	defer r.mu.Unlock()

	for key, remote := range delta.Tenants {
		local := r.getOrCreate(key)
		r.mergeTenant(local, remote, delta.NodeID)
		r.generation++
		r.tenantGeneration[key] = r.generation
	}
}

// Generation returns the current generation counter.
func (r *TenantRegistry) Generation() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.generation
}

// TenantCount returns the number of tracked tenants.
func (r *TenantRegistry) TenantCount() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.tenants)
}

// SetLastPushGen records the generation at which the last push occurred.
func (r *TenantRegistry) SetLastPushGen(gen uint64) {
	r.mu.Lock()
	r.lastPushGen = gen
	r.mu.Unlock()
}

// LastPushGen returns the generation recorded by SetLastPushGen.
func (r *TenantRegistry) LastPushGen() uint64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return r.lastPushGen
}

// registrySnapshot is the top-level structure for MarshalSnapshot / LoadSnapshot.
type registrySnapshot struct {
	NodeID           string                     `json:"node_id"`
	Generation       uint64                     `json:"generation"`
	LastPushGen      uint64                     `json:"last_push_gen"`
	Tenants          map[string]tenantStatsJSON `json:"tenants"`
	TenantGeneration map[string]uint64          `json:"tenant_generation"`
}

// MarshalSnapshot serialises the entire registry to JSON.
func (r *TenantRegistry) MarshalSnapshot() ([]byte, error) {
	r.mu.RLock()
	defer r.mu.RUnlock()

	snap := registrySnapshot{
		NodeID:           r.nodeID,
		Generation:       r.generation,
		LastPushGen:      r.lastPushGen,
		Tenants:          make(map[string]tenantStatsJSON, len(r.tenants)),
		TenantGeneration: make(map[string]uint64, len(r.tenantGeneration)),
	}
	for k, ts := range r.tenants {
		snap.Tenants[k] = ts.toJSON()
	}
	for k, v := range r.tenantGeneration {
		snap.TenantGeneration[k] = v
	}
	return json.Marshal(snap)
}

// LoadSnapshot deserialises a JSON snapshot produced by MarshalSnapshot and
// merges it into this registry using the standard CRDT merge rules.
func (r *TenantRegistry) LoadSnapshot(sourceNodeID string, data []byte) error {
	var snap registrySnapshot
	if err := json.Unmarshal(data, &snap); err != nil {
		return err
	}

	delta := &TenantDelta{
		NodeID:  sourceNodeID,
		Tenants: make(map[string]*TenantStats, len(snap.Tenants)),
	}
	for k, j := range snap.Tenants {
		delta.Tenants[k] = tenantStatsFromJSON(j)
	}
	r.Merge(delta)
	return nil
}

// ---- internal helpers ----

// getOrCreate returns the TenantStats for key, creating it if absent.
// Caller must hold r.mu (write lock).
func (r *TenantRegistry) getOrCreate(key string) *TenantStats {
	ts, ok := r.tenants[key]
	if !ok {
		parts := parseTenantKey(key)
		ts = &TenantStats{
			AccountID:    parts[0],
			ProjectID:    parts[1],
			Labels:       make(map[string]int),
			BytesByClass: make(map[string]int64),
			FilesByClass: make(map[string]int64),
			NodeContribs: make(map[string]int64),
			nodeBytes:    make(map[string]int64),
			nodeRows:     make(map[string]int64),
			nodeFiles:    make(map[string]int64),
		}
		r.tenants[key] = ts
	}
	return ts
}

// parseTenantKey splits "account:project" into [account, project].
// If the key has no colon, account = key, project = "".
func parseTenantKey(key string) [2]string {
	for i := 0; i < len(key); i++ {
		if key[i] == ':' {
			return [2]string{key[:i], key[i+1:]}
		}
	}
	return [2]string{key, ""}
}

// ReconcileWithManifest resets per-node tracking to match the actual manifest
// state, pruning contributions from dead nodes (previous container restarts).
// Only the current node's contributions are kept, set to match manifest totals.
func (r *TenantRegistry) ReconcileWithManifest(tenant string, files, bytes, rawBytes, rows int64, minTimeNs, maxTimeNs int64) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.generation++
	r.tenantGeneration[tenant] = r.generation

	ts, ok := r.tenants[tenant]
	if !ok {
		return
	}

	ts.nodeBytes = map[string]int64{r.nodeID: bytes}
	ts.nodeRows = map[string]int64{r.nodeID: rows}
	ts.nodeFiles = map[string]int64{r.nodeID: files}

	ts.TotalBytes = bytes
	ts.TotalRows = rows
	ts.TotalFiles = files
	ts.RawBytes = rawBytes

	ts.NodeContribs = map[string]int64{r.nodeID: bytes}

	if minTimeNs != 0 {
		ts.MinTimeNs = minTimeNs
	}
	if maxTimeNs != 0 {
		ts.MaxTimeNs = maxTimeNs
	}

	ts.BytesByClass = map[string]int64{"STANDARD": bytes}
	ts.FilesByClass = map[string]int64{"STANDARD": files}
}

// mergeTenant applies CRDT merge rules from remote into local.
func (r *TenantRegistry) mergeTenant(local, remote *TenantStats, remoteNodeID string) {
	// 1. Per-node counters: overwrite our view of the remote node's contribution.
	for nodeID, v := range remote.nodeBytes {
		if v > local.nodeBytes[nodeID] {
			local.nodeBytes[nodeID] = v
		}
	}
	for nodeID, v := range remote.nodeRows {
		if v > local.nodeRows[nodeID] {
			local.nodeRows[nodeID] = v
		}
	}
	for nodeID, v := range remote.nodeFiles {
		if v > local.nodeFiles[nodeID] {
			local.nodeFiles[nodeID] = v
		}
	}

	// Recalculate totals from per-node maps.
	local.TotalBytes = sumMap(local.nodeBytes)
	local.TotalRows = sumMap(local.nodeRows)
	local.TotalFiles = sumMap(local.nodeFiles)

	// RawBytes: take the max (monotonically increasing).
	if remote.RawBytes > local.RawBytes {
		local.RawBytes = remote.RawBytes
	}

	// 2. Timestamp extrema.
	if remote.MinTimeNs != 0 && (local.MinTimeNs == 0 || remote.MinTimeNs < local.MinTimeNs) {
		local.MinTimeNs = remote.MinTimeNs
	}
	if remote.MaxTimeNs > local.MaxTimeNs {
		local.MaxTimeNs = remote.MaxTimeNs
	}
	if remote.LastWriteAt.After(local.LastWriteAt) {
		local.LastWriteAt = remote.LastWriteAt
	}
	if remote.LastQueryAt.After(local.LastQueryAt) {
		local.LastQueryAt = remote.LastQueryAt
	}

	// 3. BytesByClass / FilesByClass: recalculate from per-node tracking
	// would require per-node-per-class tracking which is heavier than needed.
	// Instead, take element-wise max as a convergent approximation.
	for c, v := range remote.BytesByClass {
		if v > local.BytesByClass[c] {
			local.BytesByClass[c] = v
		}
	}
	for c, v := range remote.FilesByClass {
		if v > local.FilesByClass[c] {
			local.FilesByClass[c] = v
		}
	}

	// 4. Labels: max per field.
	for field, count := range remote.Labels {
		if count > local.Labels[field] {
			local.Labels[field] = count
		}
	}

	// NodeContribs: max per node.
	for nodeID, v := range remote.NodeContribs {
		if v > local.NodeContribs[nodeID] {
			local.NodeContribs[nodeID] = v
		}
	}

	// Partitions: max.
	if remote.Partitions > local.Partitions {
		local.Partitions = remote.Partitions
	}
}

// sumMap returns the sum of all values in m.
func sumMap(m map[string]int64) int64 {
	var s int64
	for _, v := range m {
		s += v
	}
	return s
}

// deepCopyStats returns a full deep copy of ts.
func deepCopyStats(ts *TenantStats) *TenantStats {
	cp := *ts
	cp.Labels = copyMapStringInt(ts.Labels)
	cp.BytesByClass = copyMapStringInt64(ts.BytesByClass)
	cp.FilesByClass = copyMapStringInt64(ts.FilesByClass)
	cp.NodeContribs = copyMapStringInt64(ts.NodeContribs)
	cp.nodeBytes = copyMapStringInt64(ts.nodeBytes)
	cp.nodeRows = copyMapStringInt64(ts.nodeRows)
	cp.nodeFiles = copyMapStringInt64(ts.nodeFiles)
	return &cp
}

func copyMapStringInt(m map[string]int) map[string]int {
	if m == nil {
		return nil
	}
	cp := make(map[string]int, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}

func copyMapStringInt64(m map[string]int64) map[string]int64 {
	if m == nil {
		return nil
	}
	cp := make(map[string]int64, len(m))
	for k, v := range m {
		cp[k] = v
	}
	return cp
}
