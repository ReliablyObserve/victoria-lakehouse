package manifest

import (
	"strconv"
	"strings"
	"time"
)

// Objects written from the insert buffer carry the nonce of the buffer segment
// they were drained from: their batch id is "<nonce>-<slice>", and the nonce
// is 16 lowercase hex characters whose first 8 are the segment's creation time
// in Unix seconds.
//
// While that segment is live on its insert pod, its rows are served from the
// buffer, and readers drop the segment's objects from their scan. Nothing may
// merge or rewrite such an object into an object without the nonce while the
// segment is live, or its rows would be served twice. The insert pod writes a
// marker, {prefix}_segments/<nonce>, when the segment is committed; the segment
// stays live for a grace period after that.

// SegmentNonceLen is the length of a segment nonce.
const SegmentNonceLen = 16

// SegmentMarkerDir is the directory, under the signal prefix, of the
// "segment committed" markers.
const SegmentMarkerDir = "_segments/"

// SegmentNonceOfKey returns the nonce of the buffer segment an object was
// drained from, or "" for any other object (compaction outputs, delete-rewrite
// replacements, objects of earlier releases).
func SegmentNonceOfKey(key string) string {
	base := key[strings.LastIndexByte(key, '/')+1:]
	if len(base) < SegmentNonceLen+2 || base[SegmentNonceLen] != '-' {
		return ""
	}
	nonce := base[:SegmentNonceLen]
	if !isLowerHex(nonce) {
		return ""
	}
	return nonce
}

// SegmentNonceTime is the creation time a nonce carries (zero if it carries
// none).
func SegmentNonceTime(nonce string) time.Time {
	if len(nonce) != SegmentNonceLen || !isLowerHex(nonce) {
		return time.Time{}
	}
	sec, err := strconv.ParseUint(nonce[:8], 16, 32)
	if err != nil {
		return time.Time{}
	}
	return time.Unix(int64(sec), 0)
}

func isLowerHex(s string) bool {
	for i := 0; i < len(s); i++ {
		c := s[i]
		if (c < '0' || c > '9') && (c < 'a' || c > 'f') {
			return false
		}
	}
	return true
}

// SegmentMarkerKey is the key of the "committed" marker of a segment.
func SegmentMarkerKey(prefix, nonce string) string {
	return prefix + SegmentMarkerDir + nonce
}

// SegmentReleaseAfter is how long after a segment's creation its objects are
// released even without a marker: the owner never committed it (it lost its
// disk, or its marker was collected). A segment drained later than this — an
// object-store outage of days — is the documented residual.
const SegmentReleaseAfter = 7 * 24 * time.Hour

// SegmentGuard decides which buffer-segment objects may be merged or rewritten
// now. Markers maps a committed segment's nonce to its marker's LastModified
// (the object store's clock); Protect is how long after the commit the segment
// can still be live (its grace period, with margin). A nil guard, or one whose
// marker listing failed (Listed false), releases no segment object until
// SegmentReleaseAfter.
type SegmentGuard struct {
	Markers map[string]time.Time
	Listed  bool
	Protect time.Duration
}

// Released reports whether the object key may be merged into, or replaced by,
// an object that does not carry its segment nonce. Objects that carry no nonce
// are always released.
func (g *SegmentGuard) Released(key string, now time.Time) bool {
	nonce := SegmentNonceOfKey(key)
	if nonce == "" {
		return true
	}
	if t := SegmentNonceTime(nonce); !t.IsZero() && now.Sub(t) >= SegmentReleaseAfter {
		return true
	}
	if g == nil || !g.Listed {
		return false
	}
	committed, ok := g.Markers[nonce]
	return ok && now.Sub(committed) >= g.Protect
}

// ReleasedFiles keeps the files the guard releases.
func (g *SegmentGuard) ReleasedFiles(files []FileInfo, now time.Time) []FileInfo {
	out := files[:0:0]
	for _, f := range files {
		if g.Released(f.Key, now) {
			out = append(out, f)
		}
	}
	return out
}
