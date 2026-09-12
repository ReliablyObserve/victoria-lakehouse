package vtselect

func RequestHandler(path string) bool {
	switch path {
	case "/select/logsql/query":
		return true
	case "/select/logsql/hits":
		return true
	}
	return false
}
