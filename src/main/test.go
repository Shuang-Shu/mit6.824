package main

import (
	"fmt"
	"os"
)

func main() {
	a := os.Args
	a = a[1:]
	os.Args[0] = "test"
	fmt.Println()
}
