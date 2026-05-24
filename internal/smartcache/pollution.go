package smartcache

// CachePolicy controls scan pollution protection by gating L1 cache
// promotion and bypassing the cache entirely for oversized queries.
type CachePolicy struct {
	HitsThreshold   int
	BypassThreshold int64
}

// ShouldPromoteToL1 returns true if accessCount meets the promotion
// threshold. A zero or negative threshold means always promote.
func (p *CachePolicy) ShouldPromoteToL1(accessCount int) bool {
	if p.HitsThreshold <= 0 {
		return true
	}
	return accessCount >= p.HitsThreshold
}

// ShouldBypassL1 returns true if queryBytes exceeds the bypass
// threshold. A zero or negative threshold means never bypass.
func (p *CachePolicy) ShouldBypassL1(queryBytes int64) bool {
	if p.BypassThreshold <= 0 {
		return false
	}
	return queryBytes > p.BypassThreshold
}
