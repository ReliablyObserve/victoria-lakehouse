package parquets3

import (
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// queryColumns returns the set of parquet column names that a query references.
// Returns nil if all columns are needed (wildcard, empty, or unparseable query).
// Always includes timestamp_unix_nano.
func queryColumns(queryStr string, registry *schema.Registry) map[string]bool {
	if queryStr == "" || queryStr == "*" {
		return nil
	}

	cols := make(map[string]bool)
	cols[registry.TimestampColumn()] = true

	if isFreeTextSearch(queryStr) {
		cols["body"] = true
	}

	for _, fm := range registry.PromotedColumns() {
		if referencesField(queryStr, fm.InternalName) || referencesField(queryStr, fm.ParquetColumn) {
			cols[fm.ParquetColumn] = true
		}
	}

	// If we found only the timestamp, the query likely references fields we
	// can't parse — fall back to reading all columns.
	if len(cols) <= 1 && !isFreeTextSearch(queryStr) {
		return nil
	}

	return cols
}

func referencesField(query, name string) bool {
	patterns := []string{
		name + `:="`,
		name + `:"`,
		name + `:=`,
		name + `:in(`,
		name + `:`,
	}
	for _, p := range patterns {
		if strings.Contains(query, p) {
			return true
		}
	}
	return false
}

func isFreeTextSearch(query string) bool {
	trimmed := strings.TrimSpace(query)
	if trimmed == "" || trimmed == "*" {
		return false
	}
	if trimmed[0] == '"' {
		return true
	}
	if !strings.Contains(trimmed, ":") {
		return true
	}
	return false
}
