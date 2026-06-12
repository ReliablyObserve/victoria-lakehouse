package stats

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/config"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/tenant"
)

// APIConfig holds all dependencies for the stats API.
type APIConfig struct {
	Registry        *TenantRegistry
	Manifest        *manifest.Manifest
	CostCalc        *CostCalculator
	ClassTracker    *StorageClassTracker
	LabelIndex      *cache.LabelIndex
	SchemaRegistry  *schema.Registry
	Resolver        *tenant.TenantResolver
	Policy          *tenant.PolicyRegistry
	Mode            string // "logs" or "traces"
	Bucket          string
	BloomColumns    []string
	BreakdownLabels []string
	// AlwaysSketchFields are the high-card id columns sketched (HLL) rather than
	// enumerated — trace_id/span_id and the promoted id columns. Combined with the
	// mode's dimensional label set, they define which fields the Cardinality
	// Explorer actually tracks (so a field outside the set reads "—", not 0).
	AlwaysSketchFields []string
	// StatsAggregate is the materialized per-field/per-tenant size cache (storage
	// bytes now; metadata bytes in later phases), read in O(1) instead of scanning
	// the manifest per request. Nil when the size-stats feature is unavailable.
	StatsAggregate *StatsAggregate
	// Retention summary from the global RetentionConfig, surfaced in the overview.
	RetentionEnabled bool
	RetentionDefault string
	RetentionRules   int
	// PmetaCardinality returns the accurate global HLL distinct-value estimate for
	// a field (0 if unavailable). Preferred over the lazily-populated, 100-capped
	// LabelIndex count for the Cardinality Explorer. Nil when pmeta is off.
	PmetaCardinality func(field string) uint64
	// Metadata-size sources for the Storage Overview tiles. Nil-safe (a nil func
	// contributes nothing). ResidentBytes + DiskBytes are THIS node's local
	// footprint; S3Bytes is the cluster-wide on-S3 _meta/ total (the wiring caches
	// it — it needs an S3 LIST).
	MetaResidentBytes func() int64
	MetaDiskBytes     func() int64
	MetaS3Bytes       func() int64
	// MetaBytesByTenant is the exact per-tenant on-S3 metadata footprint, keyed
	// "account:project" — the tenant-isolated pmeta bundles summed incrementally
	// (no S3 scan). Nil-safe (a nil func contributes nothing). Surfaced as each
	// tenant's metadata_bytes in /tenants.
	MetaBytesByTenant func() map[string]int64
}

// API serves JSON endpoints for tenant statistics, cost, cardinality, etc.
type API struct {
	cfg APIConfig
}

// NewAPI creates a new API handler with the given configuration.
func NewAPI(cfg APIConfig) *API {
	return &API{cfg: cfg}
}

// Register wires all API routes to the given ServeMux.
func (a *API) Register(mux *http.ServeMux) {
	mux.HandleFunc("/lakehouse/api/v1/tenants", a.handleTenants)
	mux.HandleFunc("/lakehouse/api/v1/tenants/policy", a.handleTenantPolicy)
	mux.HandleFunc("/lakehouse/api/v1/tenants/", a.handleTenantDetail)
	mux.HandleFunc("/lakehouse/api/v1/stats/overview", a.handleOverview)
	mux.HandleFunc("/lakehouse/api/v1/stats/ingestion", a.handleIngestion)
	mux.HandleFunc("/lakehouse/api/v1/stats/cost", a.handleCost)
	mux.HandleFunc("/lakehouse/api/v1/stats/compression", a.handleCompression)
	mux.HandleFunc("/lakehouse/api/v1/cardinality/fields", a.handleCardinality)
	mux.HandleFunc("/lakehouse/api/v1/stats/breakdown", a.handleBreakdown)
}

// ---- Response types ----

// TenantsResponse is the response for the tenants listing endpoint.
type TenantsResponse struct {
	Tenants      []TenantEntry `json:"tenants"`
	TotalTenants int           `json:"total_tenants"`
	TotalBytes   int64         `json:"total_bytes"`
	TotalFiles   int64         `json:"total_files"`
}

// TenantEntry represents a single tenant in the listing response.
type TenantEntry struct {
	AccountID        string           `json:"account_id"`
	ProjectID        string           `json:"project_id"`
	Name             string           `json:"name,omitempty"`
	OrgID            string           `json:"org_id,omitempty"`
	Source           string           `json:"source,omitempty"`
	TotalFiles       int64            `json:"total_files"`
	TotalBytes       int64            `json:"total_bytes"`
	RawBytes         int64            `json:"raw_bytes"`
	MetadataBytes    int64            `json:"metadata_bytes,omitempty"`
	CompressionRatio float64          `json:"compression_ratio"`
	TotalRows        int64            `json:"total_rows"`
	Partitions       int              `json:"partitions"`
	MinTime          string           `json:"min_time,omitempty"`
	MaxTime          string           `json:"max_time,omitempty"`
	LastWriteAt      string           `json:"last_write_at,omitempty"`
	LastQueryAt      string           `json:"last_query_at,omitempty"`
	StorageByClass   map[string]int64 `json:"storage_by_class,omitempty"`
	MonthlyCostUSD   float64          `json:"monthly_cost_usd"`
	TopLabels        map[string]int   `json:"top_labels,omitempty"`
}

// OverviewResponse is the response for the stats overview endpoint.
type OverviewResponse struct {
	Bucket              string           `json:"bucket"`
	Mode                string           `json:"mode"`
	TotalFiles          int64            `json:"total_files"`
	TotalBytes          int64            `json:"total_bytes"`
	TotalRawBytes       int64            `json:"total_raw_bytes"`
	AvgCompressionRatio float64          `json:"avg_compression_ratio"`
	TotalRows           int64            `json:"total_rows"`
	AvgRowBytes         int64            `json:"avg_row_bytes"`
	PartitionCount      int              `json:"partition_count"`
	OldestData          string           `json:"oldest_data,omitempty"`
	NewestData          string           `json:"newest_data,omitempty"`
	TenantCount         int              `json:"tenant_count"`
	StorageByClass      []ClassBreakdown `json:"storage_by_class"`
	FleetNodes          int              `json:"fleet_nodes"`
	RegistryGeneration  uint64           `json:"registry_generation"`
	// Retention summary (global config) so the UI can show what's configured +
	// applied. RetentionEnabled is whether the retention/deletion loop runs;
	// RetentionDefault is the default keep-duration (e.g. "90d"); RetentionRules
	// is the count of match-based override rules.
	RetentionEnabled bool   `json:"retention_enabled"`
	RetentionDefault string `json:"retention_default,omitempty"`
	RetentionRules   int    `json:"retention_rules,omitempty"`
	// Metadata footprint. ResidentBytes (pmeta RAM) + DiskBytes (disk cache) are
	// THIS node's local usage; S3Bytes is the cluster-wide on-S3 _meta/ footprint.
	MetaResidentBytes int64 `json:"meta_resident_bytes,omitempty"`
	MetaDiskBytes     int64 `json:"meta_disk_bytes,omitempty"`
	MetaS3Bytes       int64 `json:"meta_s3_bytes,omitempty"`
}

// ClassBreakdown is a per-storage-class breakdown of bytes and files.
type ClassBreakdown struct {
	Class string `json:"class"`
	Bytes int64  `json:"bytes"`
	Files int64  `json:"files"`
}

// IngestionResponse is the response for the ingestion stats endpoint.
type IngestionResponse struct {
	Period   string            `json:"period"`
	Range    string            `json:"range"`
	Buckets  []IngestionBucket `json:"buckets"`
	TotalIn  int64             `json:"total_bytes_ingested"`
	TotalOut int64             `json:"total_files_written"`
}

// IngestionBucket represents a single temporal bucket of ingestion data.
type IngestionBucket struct {
	Timestamp string `json:"timestamp"`
	Files     int    `json:"files"`
	Bytes     int64  `json:"bytes"`
}

// CostResponse is the response for the cost breakdown endpoint.
type CostResponse struct {
	TotalMonthlyUSD float64           `json:"total_monthly_usd"`
	ByClass         []ClassCost       `json:"by_class"`
	PerTenant       []TenantCostEntry `json:"per_tenant"`
}

// ClassCost is a per-class cost breakdown.
type ClassCost struct {
	Class   string  `json:"class"`
	Bytes   int64   `json:"bytes"`
	CostUSD float64 `json:"cost_usd"`
}

// TenantCostEntry is a per-tenant cost entry.
type TenantCostEntry struct {
	AccountID  string  `json:"account_id"`
	ProjectID  string  `json:"project_id"`
	Name       string  `json:"name,omitempty"`
	OrgID      string  `json:"org_id,omitempty"`
	CostUSD    float64 `json:"cost_usd"`
	TotalBytes int64   `json:"total_bytes"`
}

// CompressionResponse is the response for the compression stats endpoint.
type CompressionResponse struct {
	AvgRatio  float64                  `json:"avg_compression_ratio"`
	PerTenant []TenantCompressionEntry `json:"per_tenant"`
}

// TenantCompressionEntry is a per-tenant compression ratio entry.
type TenantCompressionEntry struct {
	AccountID        string  `json:"account_id"`
	ProjectID        string  `json:"project_id"`
	Name             string  `json:"name,omitempty"`
	OrgID            string  `json:"org_id,omitempty"`
	CompressionRatio float64 `json:"compression_ratio"`
	TotalBytes       int64   `json:"total_bytes"`
	RawBytes         int64   `json:"raw_bytes"`
}

// CardinalityResponse is the response for the cardinality fields endpoint.
type CardinalityResponse struct {
	Fields                 []FieldEntry `json:"fields"`
	TotalFields            int          `json:"total_fields"`
	TotalPromoted          int          `json:"total_promoted"`
	TotalMap               int          `json:"total_map"`
	HighCardinalityWarning []string     `json:"high_cardinality_warning,omitempty"`
	CardinalityThreshold   int          `json:"cardinality_threshold"`
}

// FieldEntry represents a single field in the cardinality response.
type FieldEntry struct {
	Name        string `json:"name"`
	Cardinality int    `json:"cardinality"`
	Type        string `json:"type"`
	HasBloom    bool   `json:"has_bloom"`
	// Indexed is true when the explorer actually tracks this field's distinct
	// count (a dimensional label column fed to the per-field HLL, or an
	// always-sketch id). When false AND cardinality is 0, the value is "not
	// counted" rather than "zero distinct" — the UI renders it as "—" so a 0
	// isn't mistaken for "no data".
	Indexed bool `json:"indexed"`
	// StorageBytes is the on-S3 compressed footprint of this field's Parquet
	// column(s), summed across all live files (from the size-stats aggregate). 0
	// when the aggregate is unavailable or the field has no dedicated column.
	StorageBytes int64 `json:"storage_bytes,omitempty"`
}

// BreakdownResponse is the response for the storage breakdown endpoint.
type BreakdownResponse struct {
	Labels []BreakdownLabel `json:"labels"`
}

// BreakdownLabel contains per-value stats for a single breakdown label.
type BreakdownLabel struct {
	Name        string           `json:"name"`
	Cardinality int              `json:"cardinality"`
	Type        string           `json:"type"`
	Values      []BreakdownValue `json:"values"`
}

// BreakdownValue is one distinct value of a breakdown label with estimated
// storage share. When the breakdown groups by tenant the OrgID field carries
// the string alias if one is registered, so UIs can surface human-friendly
// names alongside the integer account:project key.
type BreakdownValue struct {
	Value          string  `json:"value"`
	OrgID          string  `json:"org_id,omitempty"`
	EstimatedBytes int64   `json:"estimated_bytes"`
	EstimatedFiles int64   `json:"estimated_files"`
	SharePct       float64 `json:"share_pct"`
}

// ---- Handlers ----

func (a *API) handleTenants(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	sortBy := r.URL.Query().Get("sort")
	switch sortBy {
	case "bytes", "files", "cost", "rows":
		// valid
	default:
		sortBy = "bytes"
	}

	// Build a manifest-derived snapshot keyed by tenant — this is the
	// post-compaction truth for file/byte/row counts. The registry
	// tracks cumulative writes but is never decremented when files
	// are compacted away, so registry totals drift higher than the
	// manifest. We use manifest numbers for storage facts (files,
	// bytes, rows, partitions, time range) and the registry for
	// fields the manifest doesn't track (last_query_at, last_write_at,
	// node_contribs, per-storage-class breakdown).
	//
	// manifestTenantAware is true only when the manifest holds at
	// least one entry whose path parses as <numeric>/<numeric>/...
	// In tenant-aware mode we trust manifest absence as "no current
	// files for this tenant" and zero out registry storage facts
	// accordingly. In legacy single-tenant deployments (paths like
	// "data/dt=…"), the manifest has no tenant-keyed entries, so we
	// fall back to registry numbers unchanged.
	var manifestByKey map[string]manifest.TenantSummary
	var manifestTenantAware bool
	if a.cfg.Manifest != nil {
		summaries := a.cfg.Manifest.TenantSummaries()
		manifestByKey = make(map[string]manifest.TenantSummary, len(summaries))
		for _, s := range summaries {
			if _, err := strconv.ParseUint(s.AccountID, 10, 32); err != nil {
				continue
			}
			if _, err := strconv.ParseUint(s.ProjectID, 10, 32); err != nil {
				continue
			}
			manifestByKey[s.AccountID+":"+s.ProjectID] = s
			manifestTenantAware = true
		}
	}

	all := a.cfg.Registry.All()
	entries := make([]TenantEntry, 0, len(all))
	seenRegistry := make(map[string]bool, len(all))

	var totalBytes int64
	var totalFiles int64

	for _, ts := range all {
		entry := tenantStatsToEntry(ts, a.cfg.CostCalc)
		a.decorateName(&entry)
		seenRegistry[ts.AccountID+":"+ts.ProjectID] = true
		// The manifest is the post-compaction source of truth for
		// storage facts. When it has an entry for this tenant we
		// overlay; when it doesn't (data was compacted away, or
		// historic registry writes never produced current files —
		// e.g. when the writer dropped tenant headers in an older
		// build) we zero out the storage fields so /stats/overview
		// (manifest-derived) and the sum of /tenants entries agree.
		if s, ok := manifestByKey[ts.AccountID+":"+ts.ProjectID]; ok {
			overlayStorageFromManifest(&entry, s, a.cfg.CostCalc)
		} else if manifestTenantAware {
			zeroStorageFacts(&entry)
		}
		entries = append(entries, entry)
		totalBytes += entry.TotalBytes
		totalFiles += entry.TotalFiles
	}

	// Surface manifest-only tenants (data on S3 but no live registry
	// entry — e.g. after a stats-snapshot reset or a fresh process).
	// Only emit entries whose account/project parse as numeric: the
	// manifest's TenantSummaries() splits the S3 key path naively,
	// so single-tenant deployments using a non-tenant-keyed prefix
	// (e.g. "data/") would otherwise synthesize phantom tenants
	// named "data:dt=2026-05-10".
	for k, s := range manifestByKey {
		if seenRegistry[k] {
			continue
		}
		if _, err := strconv.ParseUint(s.AccountID, 10, 32); err != nil {
			continue
		}
		if _, err := strconv.ParseUint(s.ProjectID, 10, 32); err != nil {
			continue
		}
		entry := TenantEntry{
			AccountID:  s.AccountID,
			ProjectID:  s.ProjectID,
			Partitions: s.Partitions,
		}
		overlayStorageFromManifest(&entry, s, a.cfg.CostCalc)
		entries = append(entries, entry)
		totalBytes += entry.TotalBytes
		totalFiles += entry.TotalFiles
	}

	// Decorate entries with OrgID from resolver and add alias-only tenants.
	if a.cfg.Resolver != nil {
		seen := make(map[string]bool, len(entries))
		for i := range entries {
			seen[entries[i].AccountID+":"+entries[i].ProjectID] = true
			entries[i].OrgID = a.resolveOrgID(entries[i].AccountID, entries[i].ProjectID)
			if entries[i].Source == "" {
				entries[i].Source = "manifest"
			}
		}
		for _, alias := range a.cfg.Resolver.AllAliases() {
			key := strconv.FormatUint(uint64(alias.AccountID), 10) + ":" + strconv.FormatUint(uint64(alias.ProjectID), 10)
			if !seen[key] {
				entries = append(entries, TenantEntry{
					AccountID: strconv.FormatUint(uint64(alias.AccountID), 10),
					ProjectID: strconv.FormatUint(uint64(alias.ProjectID), 10),
					Name:      alias.OrgID,
					OrgID:     alias.OrgID,
					Source:    "alias",
				})
				seen[key] = true
			}
		}
	}

	// Per-tenant metadata footprint: exact, from the tenant-scoped pmeta bundles
	// (a.cfg.MetaBytesByTenant — keyed "account:project"). pmeta partitions are
	// tenant-isolated (mirroring the data path), so each tenant's metadata is its
	// own bundles' summed encoded size, tracked incrementally (no S3 scan).
	if a.cfg.MetaBytesByTenant != nil {
		byTenant := a.cfg.MetaBytesByTenant()
		for i := range entries {
			entries[i].MetadataBytes = byTenant[entries[i].AccountID+":"+entries[i].ProjectID]
		}
	}

	sortTenantEntries(entries, sortBy)

	resp := TenantsResponse{
		Tenants:      entries,
		TotalTenants: len(entries),
		TotalBytes:   totalBytes,
		TotalFiles:   totalFiles,
	}

	writeJSON(w, resp)
}

// TenantDetailResponse is the drill-down response for a single tenant.
type TenantDetailResponse struct {
	TenantEntry
	PartitionList     []manifest.PartitionSummary `json:"partition_list,omitempty"`
	FileSizeHistogram *FileSizeHistogram          `json:"file_size_histogram,omitempty"`
	AvgRowsPerFile    int64                       `json:"avg_rows_per_file"`
	Policy            *TenantPolicyEntry          `json:"policy,omitempty"`
}

// TenantPolicyEntry is the JSON shape of a resolved per-tenant override.
// Fields with zero values mean "inheriting global default".
type TenantPolicyEntry struct {
	AccountID         uint32                       `json:"account_id"`
	ProjectID         uint32                       `json:"project_id"`
	OrgID             string                       `json:"org_id,omitempty"`
	Retention         string                       `json:"retention,omitempty"`
	MaxFields         int                          `json:"max_fields,omitempty"`
	MaxStreams        int                          `json:"max_streams,omitempty"`
	MaxBytesPerSec    int64                        `json:"max_bytes_per_sec,omitempty"`
	MaxRowsPerSec     int64                        `json:"max_rows_per_sec,omitempty"`
	LifecycleOverride []config.LifecycleRuleConfig `json:"lifecycle,omitempty"`
}

// TenantPolicyListResponse is the response for /api/v1/tenants/policy.
type TenantPolicyListResponse struct {
	Entries        []TenantPolicyEntry `json:"entries"`
	PendingAliases []string            `json:"pending_aliases,omitempty"`
}

func (a *API) handleTenantPolicy(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}
	resp := TenantPolicyListResponse{}
	if a.cfg.Policy == nil {
		writeJSON(w, resp)
		return
	}
	resp.PendingAliases = a.cfg.Policy.PendingAliases()

	// Iterate the registry's resolved entries by walking every tenant
	// we know about (registry + alias-only). Anyone without an override
	// is simply omitted, keeping the response small.
	seen := make(map[string]bool)
	emit := func(accountID, projectID uint32) {
		key := strconv.FormatUint(uint64(accountID), 10) + ":" + strconv.FormatUint(uint64(projectID), 10)
		if seen[key] {
			return
		}
		seen[key] = true
		eff := a.cfg.Policy.For(accountID, projectID)
		if eff == nil {
			return
		}
		entry := effectiveToJSON(eff)
		if a.cfg.Resolver != nil {
			entry.OrgID = a.cfg.Resolver.DisplayName(accountID, projectID)
			if entry.OrgID == key {
				entry.OrgID = ""
			}
		}
		resp.Entries = append(resp.Entries, entry)
	}

	if a.cfg.Registry != nil {
		for _, ts := range a.cfg.Registry.All() {
			acc, _ := strconv.ParseUint(ts.AccountID, 10, 32)
			proj, _ := strconv.ParseUint(ts.ProjectID, 10, 32)
			emit(uint32(acc), uint32(proj))
		}
	}
	if a.cfg.Resolver != nil {
		for _, alias := range a.cfg.Resolver.AllAliases() {
			emit(alias.AccountID, alias.ProjectID)
		}
	}
	writeJSON(w, resp)
}

func effectiveToJSON(eff *tenant.EffectiveConfig) TenantPolicyEntry {
	entry := TenantPolicyEntry{
		AccountID:         eff.AccountID,
		ProjectID:         eff.ProjectID,
		MaxFields:         eff.MaxFields,
		MaxStreams:        eff.MaxStreams,
		MaxBytesPerSec:    eff.MaxBytesPerSec,
		MaxRowsPerSec:     eff.MaxRowsPerSec,
		LifecycleOverride: eff.Lifecycle,
	}
	if eff.Retention > 0 {
		entry.Retention = eff.Retention.String()
	}
	return entry
}

// FileSizeHistogram groups files into size buckets.
type FileSizeHistogram struct {
	Buckets []string `json:"buckets"`
	Counts  []int    `json:"counts"`
}

func (a *API) handleTenantDetail(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	trimmed := strings.TrimPrefix(r.URL.Path, "/lakehouse/api/v1/tenants/")

	var accountID, projectID string

	parts := strings.SplitN(trimmed, "/", 2)
	if len(parts) == 2 && parts[0] != "" && parts[1] != "" {
		accountID = parts[0]
		projectID = parts[1]
	} else if a.cfg.Resolver != nil && trimmed != "" && !strings.Contains(trimmed, "/") {
		tid, ok := a.cfg.Resolver.Resolve(trimmed)
		if !ok {
			http.Error(w, `{"error":"unknown tenant alias"}`, http.StatusNotFound)
			return
		}
		accountID = strconv.FormatUint(uint64(tid.AccountID), 10)
		projectID = strconv.FormatUint(uint64(tid.ProjectID), 10)
	} else {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	tenantKey := accountID + ":" + projectID

	var entry TenantEntry

	ts := a.cfg.Registry.Get(tenantKey)
	if ts != nil {
		entry = tenantStatsToEntry(ts, a.cfg.CostCalc)
	} else if a.cfg.Manifest != nil {
		found := false
		for _, s := range a.cfg.Manifest.TenantSummaries() {
			if s.AccountID == accountID && s.ProjectID == projectID {
				var ratio float64
				if s.TotalBytes > 0 && s.RawBytes > 0 {
					ratio = float64(s.RawBytes) / float64(s.TotalBytes)
				}
				entry = TenantEntry{
					AccountID:        s.AccountID,
					ProjectID:        s.ProjectID,
					TotalFiles:       int64(s.TotalFiles),
					TotalBytes:       s.TotalBytes,
					RawBytes:         s.RawBytes,
					TotalRows:        s.TotalRows,
					CompressionRatio: ratio,
					Partitions:       s.Partitions,
				}
				if !s.MinTime.IsZero() {
					entry.MinTime = s.MinTime.UTC().Format(time.RFC3339)
				}
				if !s.MaxTime.IsZero() {
					entry.MaxTime = s.MaxTime.UTC().Format(time.RFC3339)
				}
				if a.cfg.CostCalc != nil {
					entry.MonthlyCostUSD = a.cfg.CostCalc.MonthlyStorageCost("STANDARD", s.TotalBytes)
				}
				found = true
				break
			}
		}
		if !found {
			entry = TenantEntry{AccountID: accountID, ProjectID: projectID}
		}
	} else {
		entry = TenantEntry{AccountID: accountID, ProjectID: projectID}
	}

	a.decorateName(&entry)
	resp := TenantDetailResponse{TenantEntry: entry}

	// Attach effective per-tenant policy if an override is configured.
	if a.cfg.Policy != nil {
		acc, _ := strconv.ParseUint(accountID, 10, 32)
		proj, _ := strconv.ParseUint(projectID, 10, 32)
		if eff := a.cfg.Policy.For(uint32(acc), uint32(proj)); eff != nil {
			p := effectiveToJSON(eff)
			p.OrgID = entry.OrgID
			resp.Policy = &p
		}
	}

	// Add partition drill-down from manifest (only for tenants with data).
	if a.cfg.Manifest != nil && entry.TotalFiles > 0 {
		tenantPrefix := accountID + "/" + projectID + "/"
		allFiles := a.cfg.Manifest.AllFiles()

		// File size histogram.
		bucketLabels := []string{"<1MB", "1-10MB", "10-50MB", "50-128MB", ">128MB"}
		counts := make([]int, 5)

		seenParts := make(map[string]bool)
		for partition, files := range allFiles {
			for _, fi := range files {
				if !strings.HasPrefix(fi.Key, tenantPrefix) {
					continue
				}
				seenParts[partition] = true
				switch {
				case fi.Size < 1<<20:
					counts[0]++
				case fi.Size < 10<<20:
					counts[1]++
				case fi.Size < 50<<20:
					counts[2]++
				case fi.Size < 128<<20:
					counts[3]++
				default:
					counts[4]++
				}
			}
		}
		resp.FileSizeHistogram = &FileSizeHistogram{Buckets: bucketLabels, Counts: counts}

		// Partition COUNT scoped to this tenant. The registry-sourced entry
		// reports 0 (the per-tenant stats registry doesn't track partitions), so
		// derive it from the tenant's actual manifest partition keys.
		resp.Partitions = len(seenParts)

		// Partitions for THIS tenant (GetPartitions("","") is global — it
		// returns the same list for every tenant; GetPartitionsForTenant scopes
		// the file/byte counts to accountID/projectID).
		resp.PartitionList = a.cfg.Manifest.GetPartitionsForTenant(accountID, projectID)

		if entry.TotalRows > 0 {
			resp.AvgRowsPerFile = entry.TotalRows / entry.TotalFiles
		}
	}

	writeJSON(w, resp)
}

func (a *API) handleOverview(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	gs := a.cfg.Registry.GlobalAggregates()

	// Per-class breakdown derived from the LIVE manifest file set so the Storage
	// Classes panel reconciles with the manifest-backed headline totals below.
	// The registry's GlobalAggregates class counters are cumulative — never
	// decremented when files compact away — so they drift higher than the live
	// set (e.g. 1,815 cumulative vs 1,425 live files), which made "Storage
	// Classes" report more files/bytes than the "Files"/"Compressed" headline.
	classBD := a.manifestClassBreakdown()
	if len(classBD) == 0 {
		// No manifest or no live files: fall back to the registry's cumulative
		// class counters so a registry-only / read-path deployment still shows a
		// class split.
		for class, bytes := range gs.BytesByClass {
			classBD = append(classBD, ClassBreakdown{
				Class: class,
				Bytes: bytes,
				Files: gs.FilesByClass[class],
			})
		}
		sort.Slice(classBD, func(i, j int) bool {
			return classBD[i].Bytes > classBD[j].Bytes
		})
	}

	var avgRatio float64
	if gs.TotalBytes > 0 {
		avgRatio = float64(gs.RawBytes) / float64(gs.TotalBytes)
	}

	var partitionCount int
	var totalFiles int64
	var totalBytes int64
	var totalRows int64
	var totalRawBytes int64
	var oldestData, newestData string

	if a.cfg.Manifest != nil {
		// Tenant-scoped partition count so the overview reconciles with the sum
		// of the per-tenant detail views (partitions are physically tenant-scoped
		// S3 prefixes; PartitionCount() collapses them across tenants).
		partitionCount = a.cfg.Manifest.TenantPartitionCount()
		// LiveAggregate iterates m.files — same source as
		// TenantSummaries() — so /stats/overview and the sum of
		// /tenants entries can't disagree. The cached
		// m.totalFiles/m.totalBytes can drift if a RefreshFromS3
		// resets them against a partial S3 scan.
		live := a.cfg.Manifest.LiveAggregate()
		totalFiles = int64(live.Files)
		totalBytes = live.Bytes
		totalRows = live.Rows
		totalRawBytes = live.RawBytes
		if live.MinTimeNs > 0 {
			oldestData = time.Unix(0, live.MinTimeNs).UTC().Format(time.RFC3339)
		}
		if live.MaxTimeNs > 0 {
			newestData = time.Unix(0, live.MaxTimeNs).UTC().Format(time.RFC3339)
		}
	}
	if totalFiles == 0 {
		totalFiles = gs.TotalFiles
	}
	if totalBytes == 0 {
		totalBytes = gs.TotalBytes
	}
	if gs.TotalRows > totalRows {
		totalRows = gs.TotalRows
	}
	if gs.RawBytes > totalRawBytes {
		totalRawBytes = gs.RawBytes
	}

	// Count distinct nodes across all tenants.
	fleetNodes := countFleetNodes(a.cfg.Registry)

	tenantCount := gs.TenantCount
	if tenantCount == 0 && a.cfg.Manifest != nil {
		tenantCount = len(a.cfg.Manifest.TenantSummaries())
	}

	if avgRatio == 0 && totalRawBytes > 0 && totalBytes > 0 {
		avgRatio = float64(totalRawBytes) / float64(totalBytes)
	}

	var avgRowBytes int64
	if totalRows > 0 {
		avgRowBytes = totalRawBytes / totalRows
	}

	resp := OverviewResponse{
		Bucket:              a.cfg.Bucket,
		Mode:                a.cfg.Mode,
		TotalFiles:          totalFiles,
		TotalBytes:          totalBytes,
		TotalRawBytes:       totalRawBytes,
		AvgCompressionRatio: avgRatio,
		TotalRows:           totalRows,
		AvgRowBytes:         avgRowBytes,
		PartitionCount:      partitionCount,
		OldestData:          oldestData,
		NewestData:          newestData,
		TenantCount:         tenantCount,
		StorageByClass:      classBD,
		FleetNodes:          fleetNodes,
		RegistryGeneration:  a.cfg.Registry.Generation(),
		RetentionEnabled:    a.cfg.RetentionEnabled,
		RetentionDefault:    a.cfg.RetentionDefault,
		RetentionRules:      a.cfg.RetentionRules,
	}
	if a.cfg.MetaResidentBytes != nil {
		resp.MetaResidentBytes = a.cfg.MetaResidentBytes()
	}
	if a.cfg.MetaDiskBytes != nil {
		resp.MetaDiskBytes = a.cfg.MetaDiskBytes()
	}
	if a.cfg.MetaS3Bytes != nil {
		resp.MetaS3Bytes = a.cfg.MetaS3Bytes()
	}

	writeJSON(w, resp)
}

// manifestClassBreakdown attributes every LIVE manifest file to a storage class
// — age-predicted via the ClassTracker, or STANDARD when no tracker is
// configured — so the per-class bytes AND files sum to the manifest-derived
// headline totals (Files / Compressed). It iterates the same file set as
// LiveAggregate(), guaranteeing the Storage Classes panel reconciles with the
// overview headline. Returns nil when there's no manifest / no live files so the
// caller can fall back to the registry's cumulative counters.
func (a *API) manifestClassBreakdown() []ClassBreakdown {
	if a.cfg.Manifest == nil {
		return nil
	}
	now := time.Now()
	type acc struct {
		bytes int64
		files int64
	}
	byClass := make(map[string]*acc)
	for _, files := range a.cfg.Manifest.AllFiles() {
		for _, fi := range files {
			class := "STANDARD"
			if a.cfg.ClassTracker != nil && !fi.CreatedAt.IsZero() {
				if parts := strings.SplitN(fi.Key, "/", 3); len(parts) >= 2 {
					class = a.cfg.ClassTracker.PredictClassForTenant(fi.CreatedAt, now, parts[0]+":"+parts[1])
				} else {
					class = a.cfg.ClassTracker.PredictClass(fi.CreatedAt, now)
				}
			}
			e := byClass[class]
			if e == nil {
				e = &acc{}
				byClass[class] = e
			}
			e.bytes += fi.Size
			e.files++
		}
	}
	if len(byClass) == 0 {
		return nil
	}
	out := make([]ClassBreakdown, 0, len(byClass))
	for class, e := range byClass {
		out = append(out, ClassBreakdown{Class: class, Bytes: e.bytes, Files: e.files})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].Bytes > out[j].Bytes })
	return out
}

func (a *API) handleIngestion(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	period := r.URL.Query().Get("period")
	switch period {
	case "hour", "day", "month":
		// valid
	default:
		period = "day"
	}

	rangeParam := r.URL.Query().Get("range")
	switch rangeParam {
	case "24h", "7d", "30d":
		// valid
	default:
		rangeParam = "7d"
	}

	// Determine date range from rangeParam.
	now := time.Now().UTC()
	var startDate string
	endDate := now.Format("2006-01-02")
	switch rangeParam {
	case "24h":
		startDate = now.Add(-24 * time.Hour).Format("2006-01-02")
	case "7d":
		startDate = now.Add(-7 * 24 * time.Hour).Format("2006-01-02")
	case "30d":
		startDate = now.Add(-30 * 24 * time.Hour).Format("2006-01-02")
	}

	var buckets []IngestionBucket
	var totalIn int64
	var totalOut int64

	if a.cfg.Manifest != nil {
		partitions := a.cfg.Manifest.GetPartitions(startDate, endDate)

		for _, ps := range partitions {
			var label string
			switch period {
			case "hour":
				for _, h := range ps.Hours {
					hLabel := ps.Date + "T" + padHour(h) + ":00:00Z"
					buckets = append(buckets, IngestionBucket{
						Timestamp: hLabel,
						Files:     ps.Files / max(len(ps.Hours), 1),
						Bytes:     ps.Bytes / int64(max(len(ps.Hours), 1)),
					})
				}
				totalIn += ps.Bytes
				totalOut += int64(ps.Files)
				continue
			case "month":
				if len(ps.Date) >= 7 {
					label = ps.Date[:7]
				} else {
					label = ps.Date
				}
			default: // "day"
				label = ps.Date
			}

			buckets = append(buckets, IngestionBucket{
				Timestamp: label,
				Files:     ps.Files,
				Bytes:     ps.Bytes,
			})
			totalIn += ps.Bytes
			totalOut += int64(ps.Files)
		}

		// Deduplicate month buckets if period=month.
		if period == "month" {
			buckets = deduplicateMonthBuckets(buckets)
		}
	}

	if buckets == nil {
		buckets = []IngestionBucket{}
	}

	resp := IngestionResponse{
		Period:   period,
		Range:    rangeParam,
		Buckets:  buckets,
		TotalIn:  totalIn,
		TotalOut: totalOut,
	}

	writeJSON(w, resp)
}

func (a *API) handleCost(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	gs := a.cfg.Registry.GlobalAggregates()

	var totalCost float64
	byClass := make([]ClassCost, 0, len(gs.BytesByClass))
	perTenant := make([]TenantCostEntry, 0)

	if len(gs.BytesByClass) > 0 {
		for class, bytes := range gs.BytesByClass {
			cost := a.cfg.CostCalc.MonthlyStorageCost(class, bytes)
			byClass = append(byClass, ClassCost{
				Class:   class,
				Bytes:   bytes,
				CostUSD: cost,
			})
			totalCost += cost
		}

		all := a.cfg.Registry.All()
		for _, ts := range all {
			cost := a.cfg.CostCalc.CostPerTenant(ts.BytesByClass)
			perTenant = append(perTenant, TenantCostEntry{
				AccountID:  ts.AccountID,
				ProjectID:  ts.ProjectID,
				CostUSD:    cost,
				TotalBytes: ts.TotalBytes,
			})
		}
	} else if a.cfg.Manifest != nil {
		// Manifest fallback (registry empty — read-only nodes, datagen-only
		// deployments). Previously hardcoded everything as STANDARD which
		// over-estimated the bill for PB-scale deployments with lifecycle
		// rules — IA/GLACIER bytes show as full STANDARD price. Now uses
		// the ClassTracker's age-based prediction per file so the cost
		// view matches what the lifecycle config would actually be billing.
		now := time.Now()
		globalByClass := make(map[string]int64)
		perTenantByClass := make(map[string]map[string]int64)

		for _, fi := range a.cfg.Manifest.GetFilesForRange(0, 1<<62) {
			class := "STANDARD"
			if a.cfg.ClassTracker != nil && !fi.CreatedAt.IsZero() {
				tenantKey := ""
				parts := strings.SplitN(fi.Key, "/", 3)
				if len(parts) >= 2 {
					tenantKey = parts[0] + ":" + parts[1]
				}
				if tenantKey != "" {
					class = a.cfg.ClassTracker.PredictClassForTenant(fi.CreatedAt, now, tenantKey)
				} else {
					class = a.cfg.ClassTracker.PredictClass(fi.CreatedAt, now)
				}
			}
			globalByClass[class] += fi.Size

			parts := strings.SplitN(fi.Key, "/", 3)
			if len(parts) >= 2 {
				tk := parts[0] + ":" + parts[1]
				if _, ok := perTenantByClass[tk]; !ok {
					perTenantByClass[tk] = make(map[string]int64)
				}
				perTenantByClass[tk][class] += fi.Size
			}
		}

		var allBytes int64
		for class, bytes := range globalByClass {
			cost := a.cfg.CostCalc.MonthlyStorageCost(class, bytes)
			byClass = append(byClass, ClassCost{
				Class:   class,
				Bytes:   bytes,
				CostUSD: cost,
			})
			totalCost += cost
			allBytes += bytes
		}

		for tk, byCls := range perTenantByClass {
			parts := strings.SplitN(tk, ":", 2)
			acc, proj := parts[0], ""
			if len(parts) == 2 {
				proj = parts[1]
			}
			cost := a.cfg.CostCalc.CostPerTenant(byCls)
			var tenantBytes int64
			for _, b := range byCls {
				tenantBytes += b
			}
			perTenant = append(perTenant, TenantCostEntry{
				AccountID:  acc,
				ProjectID:  proj,
				CostUSD:    cost,
				TotalBytes: tenantBytes,
			})
		}
	}

	for i := range perTenant {
		a.decorateCostName(&perTenant[i])
	}

	sort.Slice(byClass, func(i, j int) bool {
		return byClass[i].CostUSD > byClass[j].CostUSD
	})
	sort.Slice(perTenant, func(i, j int) bool {
		return perTenant[i].CostUSD > perTenant[j].CostUSD
	})

	resp := CostResponse{
		TotalMonthlyUSD: totalCost,
		ByClass:         byClass,
		PerTenant:       perTenant,
	}

	writeJSON(w, resp)
}

func (a *API) handleCompression(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	all := a.cfg.Registry.All()

	var totalBytes, totalRaw int64
	perTenant := make([]TenantCompressionEntry, 0, len(all))

	if len(all) > 0 {
		for _, ts := range all {
			totalBytes += ts.TotalBytes
			totalRaw += ts.RawBytes

			var ratio float64
			if ts.TotalBytes > 0 {
				ratio = float64(ts.RawBytes) / float64(ts.TotalBytes)
			}
			perTenant = append(perTenant, TenantCompressionEntry{
				AccountID:        ts.AccountID,
				ProjectID:        ts.ProjectID,
				CompressionRatio: ratio,
				TotalBytes:       ts.TotalBytes,
				RawBytes:         ts.RawBytes,
			})
		}
	} else if a.cfg.Manifest != nil {
		// Fall back to manifest. Manifest accumulates raw bytes from all files.
		summaries := a.cfg.Manifest.TenantSummaries()
		for _, s := range summaries {
			totalBytes += s.TotalBytes
			totalRaw += s.RawBytes
			perTenant = append(perTenant, TenantCompressionEntry{
				AccountID:  s.AccountID,
				ProjectID:  s.ProjectID,
				TotalBytes: s.TotalBytes,
				RawBytes:   s.RawBytes,
			})
		}
	}

	for i := range perTenant {
		a.decorateCompressionName(&perTenant[i])
	}

	var avgRatio float64
	if totalBytes > 0 && totalRaw > 0 {
		avgRatio = float64(totalRaw) / float64(totalBytes)
	}

	resp := CompressionResponse{
		AvgRatio:  avgRatio,
		PerTenant: perTenant,
	}

	writeJSON(w, resp)
}

// pmetaCardinalityOf looks up a field's accurate cardinality, trying the field
// name as-is and then the suffix after ":" so a traces label index entry like
// "resource_attr:k8s.cluster.name" matches the bare "k8s.cluster.name" the pmeta
// catalog is keyed by (the same shape hasBloomFilter handles).
func pmetaCardinalityOf(fn func(string) uint64, name string) uint64 {
	if c := fn(name); c > 0 {
		return c
	}
	if idx := strings.LastIndex(name, ":"); idx >= 0 {
		return fn(name[idx+1:])
	}
	return 0
}

// indexedFieldSet returns the field names whose cardinality the explorer actually
// tracks for the current mode: the dimensional label columns fed to the per-field
// HLL on every flush, plus the always-sketch id columns (trace_id/span_id and the
// promoted id columns). A field outside this set has no sketch, so its 0 means
// "not counted", not "no data" — the UI renders it "—".
func (a *API) indexedFieldSet() map[string]struct{} {
	set := make(map[string]struct{})
	if a.cfg.Mode == "traces" {
		for _, c := range schema.TraceLabelColumns {
			set[c.Name] = struct{}{}
		}
	} else {
		for _, c := range schema.LogLabelColumns {
			set[c.Name] = struct{}{}
		}
	}
	for _, f := range schema.DefaultSketchIDColumns {
		set[f] = struct{}{}
	}
	for _, f := range a.cfg.AlwaysSketchFields {
		set[f] = struct{}{}
	}
	return set
}

func (a *API) handleCardinality(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	sortBy := r.URL.Query().Get("sort")
	switch sortBy {
	case "cardinality", "name":
		// valid
	default:
		sortBy = "cardinality"
	}

	limitStr := r.URL.Query().Get("limit")
	limit := 100
	if n, err := strconv.Atoi(limitStr); err == nil && n > 0 {
		limit = n
	}

	tenantFilter := r.URL.Query().Get("tenant")

	var allLabels []*cache.LabelInfo
	if a.cfg.LabelIndex != nil {
		allLabels = a.cfg.LabelIndex.GetAllLabelInfo()
	}

	bloomSet := make(map[string]struct{}, len(a.cfg.BloomColumns))
	for _, col := range a.cfg.BloomColumns {
		bloomSet[col] = struct{}{}
	}
	hasBloomFilter := func(name string) bool {
		if _, ok := bloomSet[name]; ok {
			return true
		}
		// Match "resource_attr:service.name" against bloom column "service.name".
		if idx := strings.LastIndex(name, ":"); idx >= 0 {
			if _, ok := bloomSet[name[idx+1:]]; ok {
				return true
			}
		}
		return false
	}

	const highCardThreshold = 10000

	fields := make([]FieldEntry, 0, len(allLabels))
	var totalPromoted, totalMap int
	var warnings []string

	indexedSet := a.indexedFieldSet()
	isIndexed := func(name string) bool {
		if _, ok := indexedSet[name]; ok {
			return true
		}
		if idx := strings.LastIndex(name, ":"); idx >= 0 {
			if _, ok := indexedSet[name[idx+1:]]; ok {
				return true
			}
		}
		return false
	}
	// Per-field storage is exact for files that carry ColumnBytes; older files
	// (written before the feature) don't yet, so scale the covered per-field bytes
	// up to the full on-S3 total. The column shows real-magnitude storage
	// immediately and converges to exact as compaction/new flushes backfill
	// ColumnBytes (scale → 1 at full coverage).
	storageScale := 1.0
	if a.cfg.StatsAggregate != nil {
		if covered := a.cfg.StatsAggregate.CoveredStorage(); covered > 0 {
			if total := a.cfg.StatsAggregate.TotalStorage(); total > covered {
				storageScale = float64(total) / float64(covered)
			}
		}
	}
	storageOf := func(name string) int64 {
		if a.cfg.StatsAggregate == nil {
			return 0
		}
		return int64(float64(a.cfg.StatsAggregate.StorageBytesOf(name)) * storageScale)
	}

	for _, li := range allLabels {
		card := li.Cardinality
		// Prefer the accurate pmeta HLL estimate (fed on flush, merged in
		// compaction) over the lazily-populated, 100-capped LabelIndex count.
		if a.cfg.PmetaCardinality != nil {
			if pc := pmetaCardinalityOf(a.cfg.PmetaCardinality, li.Name); pc > 0 {
				card = int(pc)
			}
		}

		if tenantFilter != "" && li.PerTenant != nil {
			if tc, ok := li.PerTenant[tenantFilter]; ok {
				card = tc
			} else {
				continue
			}
		}

		fieldType := "map"
		if a.cfg.SchemaRegistry != nil && a.cfg.SchemaRegistry.IsPromoted(li.Name) {
			fieldType = "promoted"
			totalPromoted++
		} else {
			totalMap++
		}

		hasBloom := hasBloomFilter(li.Name)

		if card >= highCardThreshold {
			warnings = append(warnings, li.Name)
		}

		fields = append(fields, FieldEntry{
			Name:         li.Name,
			Cardinality:  card,
			Type:         fieldType,
			HasBloom:     hasBloom,
			Indexed:      isIndexed(li.Name),
			StorageBytes: storageOf(li.Name),
		})
	}

	totalFields := len(fields)

	switch sortBy {
	case "name":
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].Name < fields[j].Name
		})
	default: // "cardinality"
		sort.Slice(fields, func(i, j int) bool {
			return fields[i].Cardinality > fields[j].Cardinality
		})
	}

	if limit > 0 && limit < len(fields) {
		fields = fields[:limit]
	}

	resp := CardinalityResponse{
		Fields:                 fields,
		TotalFields:            totalFields,
		TotalPromoted:          totalPromoted,
		TotalMap:               totalMap,
		HighCardinalityWarning: warnings,
		CardinalityThreshold:   highCardThreshold,
	}

	writeJSON(w, resp)
}

func (a *API) handleBreakdown(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, `{"error":"method not allowed"}`, http.StatusMethodNotAllowed)
		return
	}

	// Optional: request a single label only. Accept both ?label= and
	// ?group_by= because UIs gravitate toward "group_by" wording for
	// faceted breakdowns.
	filterLabel := r.URL.Query().Get("label")
	if filterLabel == "" {
		filterLabel = r.URL.Query().Get("group_by")
	}

	// Special-case the "tenant" breakdown: bytes/files are sourced
	// directly from the per-tenant registry (exact, not estimated),
	// and each value is decorated with org_id so the UI can render
	// the string alias next to the integer account:project key.
	if filterLabel == "tenant" {
		writeJSON(w, BreakdownResponse{Labels: []BreakdownLabel{a.tenantBreakdown()}})
		return
	}

	labels := a.cfg.BreakdownLabels
	if filterLabel != "" {
		labels = []string{filterLabel}
	}

	// Total bytes/files from manifest for proportional estimation. Use the LIVE
	// aggregate (same source as /stats/overview's headline) so a value's
	// estimated_bytes scales against the same total shown up top, instead of the
	// cached counters which can drift after a partial S3 refresh.
	var totalBytes int64
	var totalFiles int64
	if a.cfg.Manifest != nil {
		live := a.cfg.Manifest.LiveAggregate()
		totalBytes = live.Bytes
		totalFiles = int64(live.Files)
	}

	result := make([]BreakdownLabel, 0, len(labels))

	for _, name := range labels {
		// Try the exact name first; fall back to prefix-stripped name
		// (e.g. "service.name" matches label index entry "resource_attr:service.name").
		li := a.cfg.LabelIndex.GetLabelInfo(name)
		if li == nil && a.cfg.LabelIndex != nil {
			for _, candidate := range a.cfg.LabelIndex.GetAllLabelInfo() {
				if idx := strings.LastIndex(candidate.Name, ":"); idx >= 0 {
					if candidate.Name[idx+1:] == name {
						li = candidate
						break
					}
				}
			}
		}

		fieldType := "map"
		if a.cfg.SchemaRegistry != nil && a.cfg.SchemaRegistry.IsPromoted(name) {
			fieldType = "promoted"
		} else if li != nil && a.cfg.SchemaRegistry != nil && a.cfg.SchemaRegistry.IsPromoted(li.Name) {
			fieldType = "promoted"
		}

		bl := BreakdownLabel{
			Name: name,
			Type: fieldType,
		}

		// Per-value row counts from the manifest's LabelAggregates — the REAL,
		// durable distribution (survives restart; covers all dimensional fields
		// incl. dedicated columns). The lazily-populated label index is only a
		// last resort: after a restart it keeps the value list but loses per-value
		// counts, which would render every value at a flat 1/N share.
		var counts map[string]int64
		if a.cfg.Manifest != nil {
			counts = a.cfg.Manifest.LabelValueCounts(name)
		}
		if len(counts) == 0 && li != nil && len(li.Values) > 0 {
			counts = make(map[string]int64, len(li.Values))
			for _, v := range li.Values {
				c := int64(li.ValueCounts[v])
				if c < 1 {
					c = 1
				}
				counts[v] = c
			}
		}

		// Cardinality: the accurate pmeta count (same source as the cardinality
		// endpoint), else the number of enumerated values.
		if a.cfg.PmetaCardinality != nil {
			if pc := pmetaCardinalityOf(a.cfg.PmetaCardinality, name); pc > 0 {
				bl.Cardinality = int(pc)
			}
		}
		if bl.Cardinality == 0 {
			bl.Cardinality = len(counts)
		}

		if len(counts) > 0 {
			type kv struct {
				v string
				c int64
			}
			kvs := make([]kv, 0, len(counts))
			var totalWeight int64
			for v, c := range counts {
				kvs = append(kvs, kv{v, c})
				totalWeight += c
			}
			// Largest share first; cap displayed values.
			sort.Slice(kvs, func(i, j int) bool { return kvs[i].c > kvs[j].c })
			if len(kvs) > 50 {
				kvs = kvs[:50]
			}
			bv := make([]BreakdownValue, 0, len(kvs))
			for _, e := range kvs {
				share := float64(e.c) / float64(max64(totalWeight, 1))
				bv = append(bv, BreakdownValue{
					Value:          e.v,
					EstimatedBytes: int64(share * float64(totalBytes)),
					EstimatedFiles: int64(share * float64(totalFiles)),
					SharePct:       share * 100.0,
				})
			}
			bl.Values = bv
		}

		result = append(result, bl)
	}

	writeJSON(w, BreakdownResponse{Labels: result})
}

// tenantBreakdown builds a "tenant" facet from the per-tenant registry
// snapshot. Used by /api/v1/stats/breakdown?group_by=tenant. The values
// include the registry's exact byte/file totals plus the resolver-decorated
// OrgID so the UI can render the string alias next to the integer key.
func (a *API) tenantBreakdown() BreakdownLabel {
	bl := BreakdownLabel{Name: "tenant", Type: "registry"}
	if a.cfg.Registry == nil {
		return bl
	}
	all := a.cfg.Registry.All()
	var totalBytes int64
	for _, ts := range all {
		totalBytes += ts.TotalBytes
	}
	values := make([]BreakdownValue, 0, len(all))
	for _, ts := range all {
		share := 0.0
		if totalBytes > 0 {
			share = float64(ts.TotalBytes) / float64(totalBytes) * 100.0
		}
		values = append(values, BreakdownValue{
			Value:          ts.AccountID + ":" + ts.ProjectID,
			OrgID:          a.resolveOrgID(ts.AccountID, ts.ProjectID),
			EstimatedBytes: ts.TotalBytes,
			EstimatedFiles: ts.TotalFiles,
			SharePct:       share,
		})
	}
	sort.Slice(values, func(i, j int) bool {
		return values[i].EstimatedBytes > values[j].EstimatedBytes
	})
	bl.Cardinality = len(values)
	bl.Values = values
	return bl
}

func max64(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}

// ---- Helpers ----

func writeJSON(w http.ResponseWriter, v any) {
	w.Header().Set("Content-Type", "application/json")
	// Stats are recomputed every request and change as data flushes/compacts.
	// Without this, a 200 with no Cache-Control gets heuristically cached by the
	// browser, so the UI shows stale numbers (e.g. a pre-fix flat breakdown) even
	// after a reload until a hard refresh. Force revalidation.
	w.Header().Set("Cache-Control", "no-store, must-revalidate")
	_ = json.NewEncoder(w).Encode(v)
}

func (a *API) resolveName(accountID, projectID string) string {
	if a.cfg.Resolver == nil {
		return ""
	}
	accID, _ := strconv.ParseUint(accountID, 10, 32)
	projID, _ := strconv.ParseUint(projectID, 10, 32)
	name := a.cfg.Resolver.DisplayName(uint32(accID), uint32(projID))
	if name == accountID+":"+projectID {
		return ""
	}
	return name
}

func (a *API) resolveOrgID(accountID, projectID string) string {
	if a.cfg.Resolver == nil {
		return ""
	}
	accID, _ := strconv.ParseUint(accountID, 10, 32)
	projID, _ := strconv.ParseUint(projectID, 10, 32)
	name := a.cfg.Resolver.DisplayName(uint32(accID), uint32(projID))
	if name == accountID+":"+projectID {
		return ""
	}
	return name
}

func (a *API) decorateName(entry *TenantEntry) {
	entry.Name = a.resolveName(entry.AccountID, entry.ProjectID)
	entry.OrgID = a.resolveOrgID(entry.AccountID, entry.ProjectID)
}

// zeroStorageFacts wipes the storage fields on a tenant entry whose
// registry tracks historical writes but whose data isn't currently
// in the manifest (compacted away, or historic mis-routing). Keeps
// the entry visible (so operators see the tenant existed) but
// reports honest current-state numbers.
func zeroStorageFacts(entry *TenantEntry) {
	entry.TotalFiles = 0
	entry.TotalBytes = 0
	entry.RawBytes = 0
	entry.TotalRows = 0
	entry.Partitions = 0
	entry.CompressionRatio = 0
	entry.MonthlyCostUSD = 0
	entry.StorageByClass = nil
}

// overlayStorageFromManifest replaces an entry's storage facts (files,
// bytes, rows, partition count, time range, cost) with values derived
// from the manifest — the post-compaction truth. The registry's
// cumulative counters are kept for fields the manifest doesn't track
// (last_write_at, last_query_at, node_contribs, per-class breakdown).
func overlayStorageFromManifest(entry *TenantEntry, s manifest.TenantSummary, cc *CostCalculator) {
	entry.TotalFiles = int64(s.TotalFiles)
	entry.TotalBytes = s.TotalBytes
	entry.RawBytes = s.RawBytes
	entry.TotalRows = s.TotalRows
	entry.Partitions = s.Partitions
	if s.TotalBytes > 0 && s.RawBytes > 0 {
		entry.CompressionRatio = float64(s.RawBytes) / float64(s.TotalBytes)
	}
	if !s.MinTime.IsZero() {
		entry.MinTime = s.MinTime.UTC().Format(time.RFC3339)
	}
	if !s.MaxTime.IsZero() {
		entry.MaxTime = s.MaxTime.UTC().Format(time.RFC3339)
	}
	if cc != nil {
		// Without a class breakdown we attribute everything to STANDARD,
		// matching what the overview endpoint does for the same fallback.
		entry.MonthlyCostUSD = cc.MonthlyStorageCost("STANDARD", s.TotalBytes)
	}
}

func (a *API) decorateCostName(entry *TenantCostEntry) {
	entry.Name = a.resolveName(entry.AccountID, entry.ProjectID)
	entry.OrgID = a.resolveOrgID(entry.AccountID, entry.ProjectID)
}

func (a *API) decorateCompressionName(entry *TenantCompressionEntry) {
	entry.Name = a.resolveName(entry.AccountID, entry.ProjectID)
	entry.OrgID = a.resolveOrgID(entry.AccountID, entry.ProjectID)
}

func tenantStatsToEntry(ts *TenantStats, cc *CostCalculator) TenantEntry {
	var ratio float64
	if ts.TotalBytes > 0 {
		ratio = float64(ts.RawBytes) / float64(ts.TotalBytes)
	}

	entry := TenantEntry{
		AccountID:        ts.AccountID,
		ProjectID:        ts.ProjectID,
		TotalFiles:       ts.TotalFiles,
		TotalBytes:       ts.TotalBytes,
		RawBytes:         ts.RawBytes,
		CompressionRatio: ratio,
		TotalRows:        ts.TotalRows,
		Partitions:       ts.Partitions,
		StorageByClass:   ts.BytesByClass,
		TopLabels:        ts.Labels,
	}

	if ts.MinTimeNs != 0 {
		entry.MinTime = time.Unix(0, ts.MinTimeNs).UTC().Format(time.RFC3339)
	}
	if ts.MaxTimeNs != 0 {
		entry.MaxTime = time.Unix(0, ts.MaxTimeNs).UTC().Format(time.RFC3339)
	}
	if !ts.LastWriteAt.IsZero() {
		entry.LastWriteAt = ts.LastWriteAt.UTC().Format(time.RFC3339)
	}
	if !ts.LastQueryAt.IsZero() {
		entry.LastQueryAt = ts.LastQueryAt.UTC().Format(time.RFC3339)
	}

	if cc != nil {
		entry.MonthlyCostUSD = cc.CostPerTenant(ts.BytesByClass)
	}

	return entry
}

func sortTenantEntries(entries []TenantEntry, sortBy string) {
	switch sortBy {
	case "files":
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].TotalFiles > entries[j].TotalFiles
		})
	case "cost":
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].MonthlyCostUSD > entries[j].MonthlyCostUSD
		})
	case "rows":
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].TotalRows > entries[j].TotalRows
		})
	default: // "bytes"
		sort.Slice(entries, func(i, j int) bool {
			return entries[i].TotalBytes > entries[j].TotalBytes
		})
	}
}

func countFleetNodes(reg *TenantRegistry) int {
	reg.mu.RLock()
	defer reg.mu.RUnlock()

	nodes := make(map[string]struct{})
	for _, ts := range reg.tenants {
		for nodeID := range ts.NodeContribs {
			nodes[nodeID] = struct{}{}
		}
	}
	return len(nodes)
}

func deduplicateMonthBuckets(buckets []IngestionBucket) []IngestionBucket {
	byMonth := make(map[string]*IngestionBucket)
	var order []string
	for _, b := range buckets {
		if existing, ok := byMonth[b.Timestamp]; ok {
			existing.Files += b.Files
			existing.Bytes += b.Bytes
		} else {
			cp := b
			byMonth[b.Timestamp] = &cp
			order = append(order, b.Timestamp)
		}
	}
	sort.Strings(order)
	result := make([]IngestionBucket, 0, len(order))
	for _, k := range order {
		result = append(result, *byMonth[k])
	}
	return result
}

func padHour(h int) string {
	if h < 10 {
		return "0" + strconv.Itoa(h)
	}
	return strconv.Itoa(h)
}
