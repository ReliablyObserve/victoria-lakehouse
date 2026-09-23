package parquets3

import "github.com/VictoriaMetrics/VictoriaLogs/lib/logstorage"

// suppressAllTenants applies every tombstone overlapping [startNs, endNs] to db,
// whatever its tenants. Tests of the row filter itself use it; production code
// has no such variant (see suppressTombstonedRows).
func suppressAllTenants(s *Storage, db *logstorage.DataBlock, startNs, endNs int64) *logstorage.DataBlock {
	if s.tombstones == nil {
		return db
	}
	return suppressTombstonedRows(db, s.tombstones.ForRange(startNs, endNs))
}
