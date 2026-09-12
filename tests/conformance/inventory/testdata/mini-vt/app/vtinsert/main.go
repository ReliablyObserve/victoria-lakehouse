package vtinsert

import "strings"

func RequestHandler(path string) bool {
	switch path {
	case "/insert/native":
		return true
	}
	if strings.HasPrefix(path, "/insert/opentelemetry/") {
		return true
	}
	return false
}
