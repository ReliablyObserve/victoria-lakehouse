package bloomindex

// StorageTierCost represents per-tier cost parameters.
type StorageTierCost struct {
	Tier             Tier    `json:"tier"`
	S3StorageClass   string  `json:"s3_storage_class"`
	CostPerGBMonth   float64 `json:"cost_per_gb_month"`
	RetrievalPerGB   float64 `json:"retrieval_per_gb"`
	CompressionLevel int     `json:"compression_level"`
}

// DefaultStorageTierCosts returns standard S3 pricing tiers.
func DefaultStorageTierCosts() []StorageTierCost {
	return []StorageTierCost{
		{Tier: TierHot, S3StorageClass: "STANDARD", CostPerGBMonth: 0.023, RetrievalPerGB: 0.0, CompressionLevel: 3},
		{Tier: TierWarm, S3StorageClass: "STANDARD", CostPerGBMonth: 0.023, RetrievalPerGB: 0.0, CompressionLevel: 7},
		{Tier: TierCold, S3StorageClass: "STANDARD_IA", CostPerGBMonth: 0.0125, RetrievalPerGB: 0.01, CompressionLevel: 17},
		{Tier: TierArchive, S3StorageClass: "GLACIER", CostPerGBMonth: 0.004, RetrievalPerGB: 0.03, CompressionLevel: 17},
	}
}

// CostProjection calculates monthly storage cost for given data volumes.
type CostProjection struct {
	TotalGB       float64             `json:"total_gb"`
	TotalCostUSD  float64             `json:"total_cost_usd"`
	PerTier       []TierCostBreakdown `json:"per_tier"`
	SavingsVsFlat float64             `json:"savings_vs_flat_pct"`
}

// TierCostBreakdown shows cost for one tier.
type TierCostBreakdown struct {
	Tier           string  `json:"tier"`
	StorageGB      float64 `json:"storage_gb"`
	S3Class        string  `json:"s3_class"`
	MonthlyCostUSD float64 `json:"monthly_cost_usd"`
}

// ProjectCost calculates storage cost projection given per-tier GB.
func ProjectCost(tierGBs map[Tier]float64, costs []StorageTierCost) CostProjection {
	proj := CostProjection{}
	flatCost := 0.0

	for _, tc := range costs {
		gb := tierGBs[tc.Tier]
		if gb <= 0 {
			continue
		}
		cost := gb * tc.CostPerGBMonth
		proj.TotalGB += gb
		proj.TotalCostUSD += cost
		flatCost += gb * 0.023 // STANDARD price for comparison

		proj.PerTier = append(proj.PerTier, TierCostBreakdown{
			Tier:           tc.Tier.String(),
			StorageGB:      gb,
			S3Class:        tc.S3StorageClass,
			MonthlyCostUSD: cost,
		})
	}

	if flatCost > 0 {
		proj.SavingsVsFlat = (1 - proj.TotalCostUSD/flatCost) * 100
	}

	return proj
}
