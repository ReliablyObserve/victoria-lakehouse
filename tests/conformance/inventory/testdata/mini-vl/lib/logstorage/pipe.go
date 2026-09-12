package logstorage

var _ = map[string]func(){
	"stats":    parsePipeStats,
	"coalesce": parsePipeCoalesce,
	"cp":       parsePipeCopy,
}

func parsePipeStats()    {}
func parsePipeCoalesce() {}
func parsePipeCopy()     {}
