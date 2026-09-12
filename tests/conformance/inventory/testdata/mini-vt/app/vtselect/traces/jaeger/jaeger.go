package jaeger

import "strings"

func RequestHandler(path string) bool {
	switch path {
	case "/select/jaeger/api/services":
		return true
	}
	if strings.HasPrefix(path, "/select/jaeger/api/traces/") {
		return true
	} else if strings.HasPrefix(path, "/select/jaeger/api/services/") && strings.HasSuffix(path, "/operations") {
		return true
	}
	return false
}
