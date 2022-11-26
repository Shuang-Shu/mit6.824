package utils

import (
	"os"
	"strings"
)

// reset args in build mode
func ResetArgs() {
	dummyArg := os.Args[0]
	if !strings.Contains(dummyArg, "__debug_bin") {
		return
	}
	firstArg := os.Args[1]
	os.Args = os.Args[1:]
	os.Args[0] = firstArg
}
