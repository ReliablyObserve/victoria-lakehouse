package logstorage

var _ = map[string]func(){
	"quantile": parseStatsQuantile,
	"count":    parseStatsCount,
}

func parseStatsQuantile() {}
func parseStatsCount()    {}
