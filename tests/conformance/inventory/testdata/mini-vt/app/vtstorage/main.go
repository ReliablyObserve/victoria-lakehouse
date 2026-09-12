package vtstorage

func RequestHandler(path string) bool {
	switch path {
	case "/internal/force_merge":
		return true
	}
	return false
}
