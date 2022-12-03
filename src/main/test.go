package main

import (
	"encoding/json"
	"io/ioutil"
	"os"

	"6.824/mr"
)

type Work struct {
	WorkType int    // work type, can be map or reduce
	Filename string // name of file that to be processed
}

func main() {
	file, _ := os.Open("mr-8-intermediate")
	content, _ := ioutil.ReadAll(file)
	var keys []mr.KeyValue
	json.Unmarshal(content, &keys)
	println("test")
	defer file.Close()
}
