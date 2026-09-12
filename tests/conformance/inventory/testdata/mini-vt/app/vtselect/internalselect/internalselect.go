package internalselect

var handlers = map[string]func(){
	"/internal/select/trace": f,
}

func f() {}
