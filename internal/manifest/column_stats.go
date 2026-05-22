package manifest

// ColumnMinMax holds the minimum and maximum string values for a Parquet column
// extracted from the file footer statistics.
type ColumnMinMax struct {
	Min string `json:"min"`
	Max string `json:"max"`
}

// ColumnStatsContains returns true if value falls within [Min, Max] for the
// named column. Returns true (assume match) if no stats exist for the column.
func (fi FileInfo) ColumnStatsContains(column, value string) bool {
	if fi.ColumnStats == nil {
		return true
	}
	stats, ok := fi.ColumnStats[column]
	if !ok {
		return true
	}
	return value >= stats.Min && value <= stats.Max
}
