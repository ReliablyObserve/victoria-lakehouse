package parquets3

import "github.com/ReliablyObserve/victoria-lakehouse/internal/fieldpresence"

// tbidInstallComputers installs the field-presence computers for the compacted
// layout of TestTraceByIDColdAB (the build under test before the facet existed
// has an empty copy of this file).
func tbidInstallComputers() {
	fieldpresence.SetComputers(FieldPresenceComputers())
}
