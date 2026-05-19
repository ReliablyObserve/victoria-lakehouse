package stats

import (
	"encoding/json"
	"net/http"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/cache"
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
	Mode            string // "logs" or "traces"
	Bucket          string
	BloomColumns    []string
	BreakdownLabels []string
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
	PartitionCount      int              `json:"partition_count"`
	OldestData          string           `json:"oldest_data,omitempty"`
	NewestData          string           `json:"newest_data,omitempty"`
	TenantCount         int              `json:"tenant_count"`
	StorageByClass      []ClassBreakdown `json:"storage_by_class"`
	FleetNodes          int              `json:"fleet_nodes"`
	RegistryGeneration  uint64           `json:"registry_generation"`
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

// BreakdownValue is one distinct value of a breakdown label with estimated storage share.
type BreakdownValue struct {
	Value          string  `json:"value"`
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

	all := a.cfg.Registry.All()
	entries := make([]TenantEntry, 0, len(all))

	var totalBytes int64
	var totalFiles int64

	for _, ts := range all {
		entry := tenantStatsToEntry(ts, a.cfg.CostCalc)
		a.decorateName(&entry)
		entries = append(entries, entry)
		totalBytes += ts.TotalBytes
		totalFiles += ts.TotalFiles
	}

	// Fall back to manifest-derived tenants when registry is empty.
	if len(entries) == 0 && a.cfg.Manifest != nil {
		summaries := a.cfg.Manifest.TenantSummaries()
		for _, s := range summaries {
			entry := TenantEntry{
				AccountID:  s.AccountID,
				ProjectID:  s.ProjectID,
				TotalFiles: int64(s.TotalFiles),
				TotalBytes: s.TotalBytes,
				Partitions: s.Partitions,
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
			entries = append(entries, entry)
			totalBytes += s.TotalBytes
			totalFiles += int64(s.TotalFiles)
		}
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
		// Fall back to manifest-derived tenant.
		found := false
		for _, s := range a.cfg.Manifest.TenantSummaries() {
			if s.AccountID == accountID && s.ProjectID == projectID {
				entry = TenantEntry{
					AccountID:  s.AccountID,
					ProjectID:  s.ProjectID,
					TotalFiles: int64(s.TotalFiles),
					TotalBytes: s.TotalBytes,
					Partitions: s.Partitions,
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
			http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
			return
		}
	} else {
		http.Error(w, `{"error":"not found"}`, http.StatusNotFound)
		return
	}

	a.decorateName(&entry)
	resp := TenantDetailResponse{TenantEntry: entry}

	// Add partition drill-down from manifest.
	if a.cfg.Manifest != nil {
		tenantPrefix := accountID + "/" + projectID + "/"
		allFiles := a.cfg.Manifest.AllFiles()

		// File size histogram.
		bucketLabels := []string{"<1MB", "1-10MB", "10-50MB", "50-128MB", ">128MB"}
		counts := make([]int, 5)

		for _, files := range allFiles {
			for _, fi := range files {
				if !strings.HasPrefix(fi.Key, tenantPrefix) {
					continue
				}
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

		// Partitions for this tenant.
		resp.PartitionList = a.cfg.Manifest.GetPartitions("", "")

		if entry.TotalFiles > 0 && entry.TotalRows > 0 {
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

	classBD := make([]ClassBreakdown, 0, len(gs.BytesByClass))
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
	// Fall back: assume STANDARD when registry has no class data but manifest has files.
	if len(classBD) == 0 && a.cfg.Manifest != nil && a.cfg.Manifest.TotalFiles() > 0 {
		classBD = append(classBD, ClassBreakdown{
			Class: "STANDARD",
			Bytes: a.cfg.Manifest.TotalBytes(),
			Files: int64(a.cfg.Manifest.TotalFiles()),
		})
	}

	var avgRatio float64
	if gs.TotalBytes > 0 {
		avgRatio = float64(gs.RawBytes) / float64(gs.TotalBytes)
	}

	var partitionCount int
	var totalFiles int64
	var totalBytes int64
	var oldestData, newestData string

	if a.cfg.Manifest != nil {
		partitionCount = a.cfg.Manifest.PartitionCount()
		totalFiles = int64(a.cfg.Manifest.TotalFiles())
		totalBytes = a.cfg.Manifest.TotalBytes()
		if !a.cfg.Manifest.MinTime().IsZero() {
			oldestData = a.cfg.Manifest.MinTime().UTC().Format(time.RFC3339)
		}
		if !a.cfg.Manifest.MaxTime().IsZero() {
			newestData = a.cfg.Manifest.MaxTime().UTC().Format(time.RFC3339)
		}
	}
	// If manifest has no data, fall back to registry totals.
	if totalFiles == 0 {
		totalFiles = gs.TotalFiles
	}
	if totalBytes == 0 {
		totalBytes = gs.TotalBytes
	}

	// Count distinct nodes across all tenants.
	fleetNodes := countFleetNodes(a.cfg.Registry)

	tenantCount := gs.TenantCount
	if tenantCount == 0 && a.cfg.Manifest != nil {
		tenantCount = len(a.cfg.Manifest.TenantSummaries())
	}

	resp := OverviewResponse{
		Bucket:              a.cfg.Bucket,
		Mode:                a.cfg.Mode,
		TotalFiles:          totalFiles,
		TotalBytes:          totalBytes,
		TotalRawBytes:       gs.RawBytes,
		AvgCompressionRatio: avgRatio,
		TotalRows:           gs.TotalRows,
		PartitionCount:      partitionCount,
		OldestData:          oldestData,
		NewestData:          newestData,
		TenantCount:         tenantCount,
		StorageByClass:      classBD,
		FleetNodes:          fleetNodes,
		RegistryGeneration:  a.cfg.Registry.Generation(),
	}

	writeJSON(w, resp)
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
		// Fall back to manifest when registry is empty (e.g. read-only / datagen).
		summaries := a.cfg.Manifest.TenantSummaries()
		var allBytes int64
		for _, s := range summaries {
			allBytes += s.TotalBytes
			cost := a.cfg.CostCalc.MonthlyStorageCost("STANDARD", s.TotalBytes)
			perTenant = append(perTenant, TenantCostEntry{
				AccountID:  s.AccountID,
				ProjectID:  s.ProjectID,
				CostUSD:    cost,
				TotalBytes: s.TotalBytes,
			})
		}
		totalCost = a.cfg.CostCalc.MonthlyStorageCost("STANDARD", allBytes)
		byClass = append(byClass, ClassCost{
			Class:   "STANDARD",
			Bytes:   allBytes,
			CostUSD: totalCost,
		})
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
		// Fall back to manifest. Without write-path raw bytes, show
		// compressed bytes only so the endpoint is not empty.
		summaries := a.cfg.Manifest.TenantSummaries()
		for _, s := range summaries {
			totalBytes += s.TotalBytes
			perTenant = append(perTenant, TenantCompressionEntry{
				AccountID:  s.AccountID,
				ProjectID:  s.ProjectID,
				TotalBytes: s.TotalBytes,
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

	for _, li := range allLabels {
		card := li.Cardinality

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
			Name:        li.Name,
			Cardinality: card,
			Type:        fieldType,
			HasBloom:    hasBloom,
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

	// Optional: request a single label only.
	filterLabel := r.URL.Query().Get("label")

	labels := a.cfg.BreakdownLabels
	if filterLabel != "" {
		labels = []string{filterLabel}
	}

	// Total bytes/files from manifest for proportional estimation.
	var totalBytes int64
	var totalFiles int64
	if a.cfg.Manifest != nil {
		totalBytes = a.cfg.Manifest.TotalBytes()
		totalFiles = int64(a.cfg.Manifest.TotalFiles())
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

		if li != nil && len(li.Values) > 0 {
			bl.Cardinality = li.Cardinality
			values := li.Values

			// Compute total occurrence weight across all values for proportional split.
			var totalWeight int64
			for _, v := range values {
				c := li.ValueCounts[v]
				if c < 1 {
					c = 1
				}
				totalWeight += int64(c)
			}

			// Cap displayed values after weight calculation.
			if len(values) > 50 {
				values = values[:50]
			}

			bv := make([]BreakdownValue, 0, len(values))
			for _, v := range values {
				c := int64(li.ValueCounts[v])
				if c < 1 {
					c = 1
				}
				share := float64(c) / float64(max64(totalWeight, 1))
				estBytes := int64(share * float64(totalBytes))
				estFiles := int64(share * float64(totalFiles))
				sharePct := share * 100.0
				bv = append(bv, BreakdownValue{
					Value:          v,
					EstimatedBytes: estBytes,
					EstimatedFiles: estFiles,
					SharePct:       sharePct,
				})
			}
			bl.Values = bv
		}

		result = append(result, bl)
	}

	writeJSON(w, BreakdownResponse{Labels: result})
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
}

func (a *API) decorateCostName(entry *TenantCostEntry) {
	entry.Name = a.resolveName(entry.AccountID, entry.ProjectID)
}

func (a *API) decorateCompressionName(entry *TenantCompressionEntry) {
	entry.Name = a.resolveName(entry.AccountID, entry.ProjectID)
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
