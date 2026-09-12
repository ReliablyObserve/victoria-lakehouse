package vlinsert

import "strings"

func RequestHandler(path string) bool {
	switch path {
	case "/insert/jsonline":
		return true
	}
	switch {
	case strings.HasPrefix(path, "/insert/loki/"):
		return true
	}
	return false
}
