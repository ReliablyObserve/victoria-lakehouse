package vlinsert

import "strings"

func RequestHandler(path string) bool {
	switch path {
	case "/insert/jsonline":
		return true
	}
	if strings.HasPrefix(path, "/insert/loki/") {
		return true
	}
	return false
}
