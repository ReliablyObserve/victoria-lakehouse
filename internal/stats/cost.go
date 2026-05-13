package stats

// CostCalculator computes S3 storage and request costs based on
// configurable per-class storage prices and per-operation request prices.
type CostCalculator struct {
	storagePrices map[string]float64 // class -> $/GB/month
	requestPrices map[string]float64 // operation -> $/1000 requests
}

// NewCostCalculator creates a CostCalculator with the given pricing tables.
// Nil maps are handled gracefully by substituting empty maps.
func NewCostCalculator(storagePrices, requestPrices map[string]float64) *CostCalculator {
	sp := storagePrices
	if sp == nil {
		sp = make(map[string]float64)
	}
	rp := requestPrices
	if rp == nil {
		rp = make(map[string]float64)
	}
	return &CostCalculator{
		storagePrices: sp,
		requestPrices: rp,
	}
}

// MonthlyStorageCost returns the monthly cost for the given storage class
// and byte count. Bytes are converted to GB (bytes / 1<<30) and multiplied
// by the per-GB price. Unknown classes return $0.
func (cc *CostCalculator) MonthlyStorageCost(class string, bytes int64) float64 {
	price, ok := cc.storagePrices[class]
	if !ok {
		return 0
	}
	gb := float64(bytes) / float64(1<<30)
	return gb * price
}

// TotalMonthlyCost returns the aggregate monthly cost across all storage
// classes in the provided map of class -> byte count.
func (cc *CostCalculator) TotalMonthlyCost(byClass map[string]int64) float64 {
	var total float64
	for class, bytes := range byClass {
		total += cc.MonthlyStorageCost(class, bytes)
	}
	return total
}

// RequestCost returns the cost for count operations of the given type.
// The cost is price * count / 1000. Unknown operations return $0.
func (cc *CostCalculator) RequestCost(operation string, count int64) float64 {
	price, ok := cc.requestPrices[operation]
	if !ok {
		return 0
	}
	return price * float64(count) / 1000
}

// LifecycleSavings computes how much is saved by using tiered storage
// classes compared to storing everything in STANDARD. Returns
// (allStandardCost - actualCost), floored at 0.
func (cc *CostCalculator) LifecycleSavings(byClass map[string]int64) float64 {
	var totalBytes int64
	for _, b := range byClass {
		totalBytes += b
	}
	allStandardCost := cc.MonthlyStorageCost("STANDARD", totalBytes)
	actualCost := cc.TotalMonthlyCost(byClass)
	savings := allStandardCost - actualCost
	if savings < 0 {
		return 0
	}
	return savings
}

// ProjectCost30d projects the 30-day storage cost given daily ingestion
// volumes. It uses a simple linear model: avg(dailyBytes) * 30 / 2
// (average storage over the period) * price-per-GB.
func (cc *CostCalculator) ProjectCost30d(dailyBytes []int64, defaultClass string) float64 {
	if len(dailyBytes) == 0 {
		return 0
	}
	var sum int64
	for _, b := range dailyBytes {
		sum += b
	}
	avgDaily := float64(sum) / float64(len(dailyBytes))
	// Average storage over 30 days assuming linear accumulation
	avgStorage := avgDaily * 30 / 2
	price, ok := cc.storagePrices[defaultClass]
	if !ok {
		return 0
	}
	gb := avgStorage / float64(1<<30)
	return gb * price
}

// CostPerTenant returns the monthly storage cost for a single tenant,
// given their byte counts by storage class. Equivalent to TotalMonthlyCost
// but named for clarity when computing per-tenant breakdowns.
func (cc *CostCalculator) CostPerTenant(tenantBytesByClass map[string]int64) float64 {
	return cc.TotalMonthlyCost(tenantBytesByClass)
}

// StoragePrices returns a copy of the storage price map.
func (cc *CostCalculator) StoragePrices() map[string]float64 {
	cp := make(map[string]float64, len(cc.storagePrices))
	for k, v := range cc.storagePrices {
		cp[k] = v
	}
	return cp
}
