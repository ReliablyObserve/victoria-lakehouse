package parquets3

import (
	"strings"

	"github.com/ReliablyObserve/victoria-lakehouse/internal/schema"
)

// queryColumns returns the set of parquet column names needed for a query.
// Returns nil when all columns should be read (the common case).
//
// pipeFields are field names extracted from VL's parsed pipe operators
// (stats by(), uniq by(), top by(), fields) via logstorage.GetQueryPipeFields.
// Using VL's actual parsed representation avoids duplicating query parsing.
func queryColumns(queryStr string, registry *schema.Registry, pipeFields []string) map[string]bool {
	if queryStr == "" || queryStr == "*" {
		return nil
	}

	if len(pipeFields) == 0 && !hasColumnSelectingPipe(queryStr) {
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

	for _, name := range pipeFields {
		if fm := registry.ResolveToParquet(name); fm != nil {
			cols[fm.ParquetColumn] = true
		}
	}

	if len(cols) <= 1 && !isFreeTextSearch(queryStr) {
		return nil
	}

	return cols
}

func hasColumnSelectingPipe(query string) bool {
	idx := strings.Index(query, " | ")
	if idx < 0 {
		return false
	}
	pipes := query[idx:]
	selectingPipes := []string{" | fields ", " | stats ", " | uniq ", " | top "}
	for _, p := range selectingPipes {
		if strings.Contains(pipes, p) {
			return true
		}
	}
	return false
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
