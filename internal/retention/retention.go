package retention

import (
	"context"
	"fmt"
	"log/slog"
	"path"
	"strconv"
	"strings"
	"time"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/manifest"
	"github.com/ReliablyObserve/victoria-lakehouse/internal/metrics"
)

// Config holds the retention policy configuration.
type Config struct {
	Enabled       bool   `yaml:"enabled"`
	Default       string `yaml:"default"`        // e.g. "90d" or "2160h"
	CheckInterval string `yaml:"check_interval"` // e.g. "1h"
	Rules         []Rule `yaml:"rules"`
}

// Rule defines a retention rule that matches files by labels.
type Rule struct {
	Match map[string]string `yaml:"match"` // field -> value or glob pattern
	Keep  string            `yaml:"keep"`  // e.g. "7d", "365d"
}

// ManifestAccessor provides read/write access to manifest file metadata.
type ManifestAccessor interface {
	AllFiles() map[string][]manifest.FileInfo
	RemoveFile(partition string, key string)
}

// FileDeleter deletes objects from storage.
type FileDeleter interface {
	DeleteObject(ctx context.Context, bucket, key string) error
}

type parsedRule struct {
	match map[string]string // field -> value/glob
	keep  time.Duration
}

// Manager manages retention policy enforcement.
type Manager struct {
	cfg           Config
	manifest      ManifestAccessor
	deleter       FileDeleter
	logger        *slog.Logger
	bucket        string
	defaultTTL    time.Duration
	checkInterval time.Duration
	parsedRules   []parsedRule
	nowFunc       func() time.Time // for testing
}

// New creates a new retention Manager, parsing and validating the config.
func New(cfg Config, mf ManifestAccessor, deleter FileDeleter, bucket string, logger *slog.Logger) (*Manager, error) {
	defaultTTL, err := parseDuration(cfg.Default)
	if err != nil {
		return nil, fmt.Errorf("parse default retention %q: %w", cfg.Default, err)
	}

	checkInterval, err := parseDuration(cfg.CheckInterval)
	if err != nil {
		return nil, fmt.Errorf("parse check_interval %q: %w", cfg.CheckInterval, err)
	}
	if checkInterval <= 0 {
		return nil, fmt.Errorf("check_interval must be positive, got %v", checkInterval)
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
		rules = append(rules, parsedRule{
			match: r.Match,
			keep:  keep,
		})
	}

	return &Manager{
		cfg:           cfg,
		manifest:      mf,
		deleter:       deleter,
		logger:        logger,
		bucket:        bucket,
		defaultTTL:    defaultTTL,
		checkInterval: checkInterval,
		parsedRules:   rules,
		nowFunc:       time.Now,
	}, nil
}

// Start runs the retention check loop in the background until ctx is cancelled.
func (m *Manager) Start(ctx context.Context) {
	if !m.cfg.Enabled {
		m.logger.Info("retention manager disabled")
		return
	}

	m.logger.Info("retention manager started",
		"default_ttl", m.defaultTTL,
		"check_interval", m.checkInterval,
		"rules", len(m.parsedRules),
	)

	ticker := time.NewTicker(m.checkInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			m.logger.Info("retention manager stopped")
			return
		case <-ticker.C:
			deleted, err := m.RunOnce(ctx)
			if err != nil {
				m.logger.Error("retention pass failed", "error", err)
			} else if deleted > 0 {
				m.logger.Info("retention pass completed", "deleted", deleted)
			}
		}
	}
}

// RunOnce performs a single retention pass, deleting expired files.
// Returns the number of files deleted.
func (m *Manager) RunOnce(ctx context.Context) (int, error) {
	now := m.nowFunc()
	allFiles := m.manifest.AllFiles()

	var deleted int
	for partition, files := range allFiles {
		for _, fi := range files {
			select {
			case <-ctx.Done():
				return deleted, ctx.Err()
			default:
			}

			ttl := m.ResolveTTL(fi)
			age := m.fileAge(fi, now)

			if age > ttl {
				if err := m.deleter.DeleteObject(ctx, m.bucket, fi.Key); err != nil {
					m.logger.Error("failed to delete expired file",
						"key", fi.Key,
						"partition", partition,
						"age", age,
						"ttl", ttl,
						"error", err,
					)
					continue
				}
				m.manifest.RemoveFile(partition, fi.Key)
				metrics.RetentionFilesDeleted.Inc()
				deleted++
				m.logger.Debug("deleted expired file",
					"key", fi.Key,
					"partition", partition,
					"age", age,
					"ttl", ttl,
				)
			}
		}
	}

	return deleted, nil
}

// ResolveTTL determines the TTL for a file based on matching rules.
// When multiple rules match, the longest (most conservative) TTL wins.
// When no rules match, the default TTL is returned.
func (m *Manager) ResolveTTL(fi manifest.FileInfo) time.Duration {
	var matched bool
	var longest time.Duration

	for _, rule := range m.parsedRules {
		if m.matchRule(fi, rule) {
			if !matched || rule.keep > longest {
				longest = rule.keep
				matched = true
			}
		}
	}

	if !matched {
		return m.defaultTTL
	}
	return longest
}

// matchRule checks if a file matches all fields in a rule.
func (m *Manager) matchRule(fi manifest.FileInfo, rule parsedRule) bool {
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

// fileAge computes how old a file is based on MaxTimeNs.
func (m *Manager) fileAge(fi manifest.FileInfo, now time.Time) time.Duration {
	fileTime := time.Unix(0, fi.MaxTimeNs)
	return now.Sub(fileTime)
}

// parseDuration parses a duration string supporting both Go duration format
// and "Nd" day format (e.g. "7d", "90d", "365d").
func parseDuration(s string) (time.Duration, error) {
	if s == "" {
		return 0, fmt.Errorf("empty duration")
	}

	s = strings.TrimSpace(s)

	// Try standard Go duration first.
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}

	// Support day suffix: positive integer followed by "d".
	if strings.HasSuffix(s, "d") {
		numPart := s[:len(s)-1]
		if numPart == "" {
			return 0, fmt.Errorf("missing numeric value before 'd' in %q", s)
		}
		days, err := strconv.Atoi(numPart)
		if err != nil {
			return 0, fmt.Errorf("invalid duration %q: %w", s, err)
		}
		if days <= 0 {
			return 0, fmt.Errorf("duration must be positive, got %q", s)
		}
		return time.Duration(days) * 24 * time.Hour, nil
	}

	return 0, fmt.Errorf("cannot parse %q as duration (supported: Go durations or Nd for days)", s)
}

// isGlob returns true if the pattern contains glob metacharacters.
func isGlob(pattern string) bool {
	return strings.ContainsAny(pattern, "*?")
}

// matchAnyGlob checks if any value in the slice matches the glob pattern.
func matchAnyGlob(values []string, pattern string) bool {
	for _, v := range values {
		if matched, _ := path.Match(pattern, v); matched {
			return true
		}
	}
	return false
}

// containsExact checks if any value in the slice equals the target.
func containsExact(values []string, target string) bool {
	for _, v := range values {
		if v == target {
			return true
		}
	}
	return false
}
