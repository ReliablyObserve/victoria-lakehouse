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
// predicate serves both, which keeps them from drifting.
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

// Expired reports whether the file is past its retention at now: its newest row
// is older than its TTL. A disabled policy expires nothing.
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
	return age > p.ResolveTTL(fi)
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
