package smartcache

import (
	"sort"
	"time"
)

func IsHot(meta EntryMeta, threshold int, window time.Duration) bool {
	if time.Since(meta.AccessWindowStart) > window {
		return false
	}
	return meta.AccessCount >= threshold
}

type evictionEntry struct {
	key        string
	priority   int
	lastAccess time.Time
}

func SortByEvictionPriority(entries map[string]EntryMeta, maxAge time.Duration, hotThreshold int, hotWindow time.Duration) []string {
	now := time.Now()
	sorted := make([]evictionEntry, 0, len(entries))

	for key, meta := range entries {
		p := classifyPriority(meta, now, maxAge, hotThreshold, hotWindow)
		sorted = append(sorted, evictionEntry{key: key, priority: p, lastAccess: meta.LastAccess})
	}

	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].priority != sorted[j].priority {
			return sorted[i].priority < sorted[j].priority
		}
		return sorted[i].lastAccess.Before(sorted[j].lastAccess)
	})

	result := make([]string, len(sorted))
	for i, e := range sorted {
		result[i] = e.key
	}
	return result
}

func classifyPriority(meta EntryMeta, now time.Time, maxAge time.Duration, hotThreshold int, hotWindow time.Duration) int {
	pinned := meta.IsPinned()
	hot := IsHot(meta, hotThreshold, hotWindow)

	var isExpired bool
	if hot {
		isExpired = now.Sub(meta.LastAccess) > maxAge
	} else {
		isExpired = now.Sub(meta.CreatedAt) > maxAge
	}

	if pinned {
		return 4
	}
	if isExpired && !hot {
		return 1
	}
	if !hot {
		return 2
	}
	return 3
}

func CollectExpired(m *MetadataMap, maxAge time.Duration, hotThreshold int, hotWindow time.Duration) []string {
	now := time.Now()
	all := m.All()
	var expired []string

	for key, meta := range all {
		if meta.IsPinned() {
			continue
		}
		hot := IsHot(meta, hotThreshold, hotWindow)
		if hot {
			if now.Sub(meta.LastAccess) > maxAge {
				expired = append(expired, key)
			}
		} else {
			if now.Sub(meta.CreatedAt) > maxAge {
				expired = append(expired, key)
			}
		}
	}
	return expired
}

func CollectLRU(m *MetadataMap, bytesNeeded int64, hotThreshold int, hotWindow time.Duration) []string {
	all := m.All()

	type candidate struct {
		key        string
		lastAccess time.Time
		size       int64
		hot        bool
		pinned     bool
	}

	candidates := make([]candidate, 0, len(all))
	for key, meta := range all {
		candidates = append(candidates, candidate{
			key:        key,
			lastAccess: meta.LastAccess,
			size:       meta.Size,
			hot:        IsHot(meta, hotThreshold, hotWindow),
			pinned:     meta.IsPinned(),
		})
	}

	sort.Slice(candidates, func(i, j int) bool {
		pi, pj := 0, 0
		if candidates[i].pinned {
			pi = 2
		} else if candidates[i].hot {
			pi = 1
		}
		if candidates[j].pinned {
			pj = 2
		} else if candidates[j].hot {
			pj = 1
		}
		if pi != pj {
			return pi < pj
		}
		return candidates[i].lastAccess.Before(candidates[j].lastAccess)
	})

	var toEvict []string
	var freed int64
	for _, c := range candidates {
		if freed >= bytesNeeded {
			break
		}
		if c.pinned {
			break
		}
		toEvict = append(toEvict, c.key)
		freed += c.size
	}
	return toEvict
}
