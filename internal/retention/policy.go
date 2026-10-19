package retention

import (
	"fmt"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
)

// Policy is the retention rules as a pure predicate over files: how long a file
// is kept and whether it has expired. The retention manager deletes the files it
// calls expired; the read path hides the same files from queries before they are
// deleted, so a lagging eviction can never show data past its retention. One
// predicate (and, in the binaries, one Policy object) serves both, which keeps
// them from drifting.
type Policy struct {
	enabled    bool
	defaultTTL time.Duration
	rules      []parsedRule
	minTTL     time.Duration
}

func newPolicy(enabled bool, defaultTTL time.Duration, rules []parsedRule) *Policy {
	p := &Policy{enabled: enabled, defaultTTL: defaultTTL, rules: rules, minTTL: defaultTTL}
	for _, r := range rules {
		if r.keep < p.minTTL {
			p.minTTL = r.keep
		}
	}
	return p
}

// NewPolicy parses the retention config's default and rules. A policy of a
// config with retention disabled never expires anything.
func NewPolicy(cfg Config) (*Policy, error) {
	if !cfg.Enabled {
		return newPolicy(false, 0, nil), nil
	}
	defaultTTL, err := parseDuration(cfg.Default)
	if err != nil {
		return nil, fmt.Errorf("parse default retention %q: %w", cfg.Default, err)
	}
	var rules []parsedRule
	for i, r := range cfg.Rules {
		keep, err := parseDuration(r.Keep)
		if err != nil {
			return nil, fmt.Errorf("parse rule[%d].keep %q: %w", i, r.Keep, err)
		}
		if len(r.Match) == 0 {
			return nil, fmt.Errorf("rule[%d]: match must have at least one field", i)
		}
		rules = append(rules, parsedRule{match: r.Match, keep: keep})
	}
	return newPolicy(true, defaultTTL, rules), nil
}

// Enabled reports whether the policy expires anything at all.
func (p *Policy) Enabled() bool { return p != nil && p.enabled }

// ResolveTTL is the TTL of a file: the longest matching rule, else the default.
func (p *Policy) ResolveTTL(fi manifest.FileInfo) time.Duration {
	var matched bool
	var longest time.Duration
	for _, rule := range p.rules {
		if matchRule(fi, rule) {
			if !matched || rule.keep > longest {
				longest = rule.keep
				matched = true
			}
		}
	}
	if !matched {
		return p.defaultTTL
	}
	return longest
}

// nsPerDay is the length of a partition day.
const nsPerDay = int64(24 * time.Hour)

// dayOf is the UTC day number of a timestamp (floor division, so a negative
// timestamp is in the day before the epoch).
func dayOf(ns int64) int64 {
	d := ns / nsPerDay
	if ns%nsPerDay < 0 {
		d--
	}
	return d
}

// Expired reports whether the file is past its retention at now.
//
// For a TTL of a day or more this is upstream VictoriaLogs/VictoriaTraces' rule,
// which drops whole day partitions: the file's newest row is in a day that is
// before the day of now-TTL (upstream: storage.go getMinAllowedDay,
// `day < (now - retention) / nsPerDay`; its minimum retention is one day). The
// day floor keeps a file until its whole last day is outside the retention, so
// nothing is hidden up to 23 hours early.
//
// For a TTL below a day (a Lakehouse extension: upstream cannot go below one
// day) the rule is exact: the newest row is older than the TTL.
//
// A disabled policy expires nothing.
func (p *Policy) Expired(fi manifest.FileInfo, now time.Time) bool {
	if !p.Enabled() {
		return false
	}
	age := now.Sub(time.Unix(0, fi.MaxTimeNs))
	// Cheap exit for the common case: a file younger than the shortest TTL
	// cannot be expired, and nothing needs matching.
	if age <= p.minTTL {
		return false
	}
	ttl := p.ResolveTTL(fi)
	if age <= ttl {
		return false
	}
	if ttl < 24*time.Hour {
		return true
	}
	return dayOf(fi.MaxTimeNs) < dayOf(now.UnixNano()-ttl.Nanoseconds())
}

// matchRule checks if a file matches all fields in a rule.
func matchRule(fi manifest.FileInfo, rule parsedRule) bool {
	if fi.Labels == nil {
		return false
	}
	for field, pattern := range rule.match {
		values, exists := fi.Labels[field]
		if !exists {
			return false
		}
		if isGlob(pattern) {
			if !matchAnyGlob(values, pattern) {
				return false
			}
		} else {
			if !containsExact(values, pattern) {
				return false
			}
		}
	}
	return true
}
