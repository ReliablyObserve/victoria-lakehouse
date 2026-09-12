package tempo

import "strings"

func RequestHandler(path string) bool {
	if path == "/select/tempo/api/search" {
		return true
	} else if path == "/select/tempo/api/v2/search/tags" {
		return true
	} else if strings.HasPrefix(path, "/select/tempo/api/traces/") && len(path) > len("/select/tempo/api/traces/") {
		return true
	}
	return false
}
