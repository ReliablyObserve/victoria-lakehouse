package vtinsert

import "strings"

func RequestHandler(path string) bool {
	switch path {
	case "/insert/native":
		return true
	}
	switch {
	case strings.HasPrefix(path, "/insert/opentelemetry/"):
		return true
	}
	return false
}
