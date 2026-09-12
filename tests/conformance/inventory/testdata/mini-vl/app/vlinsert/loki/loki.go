package loki

func RequestHandler(path string) bool {
	switch path {
	case "/insert/loki/api/v1/push":
		return true
	}
	return false
}
