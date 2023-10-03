package main

import (
	debefix_poc2 "github.com/RangelReale/debefix-poc2"
	"github.com/davecgh/go-spew/spew"
)

func main() {
	provider := debefix_poc2.NewDirectoryFileProvider(`/Users/rangelreale/prog/personal/debefix-poc2/sample/data1`)

	data, err := debefix_poc2.Load(provider)
	if err != nil {
		panic(err)
	}

	spew.Dump(data)
}
