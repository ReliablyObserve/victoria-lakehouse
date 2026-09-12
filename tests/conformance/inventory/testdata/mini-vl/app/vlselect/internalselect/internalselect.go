package internalselect

var handlers = map[string]func(){
	"/internal/select/query": q,
	"/internal/select/hits":  h,
}

func q() {}
func h() {}
