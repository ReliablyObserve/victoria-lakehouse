package tempo

import "strings"

func RequestHandler(path string) bool {
	switch path {
	case "/select/tempo/api/search":
		return true
	case "/select/tempo/api/v2/search/tags":
		return true
	}
	if strings.HasPrefix(path, "/select/tempo/api/traces/") {
		return true
	}
	return false
}
