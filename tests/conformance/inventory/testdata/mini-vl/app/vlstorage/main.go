package vlstorage

func RequestHandler(path string) bool {
	switch path {
	case "/internal/force_flush":
		return true
	}
	return false
}
