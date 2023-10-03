package main

import (
	"fmt"
	"strings"

	debefix_poc2 "github.com/RangelReale/debefix-poc2"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
)

func main() {
	provider := debefix_poc2.NewDirectoryFileProvider(`/Users/rangelreale/prog/personal/debefix-poc2/sample/data1`)

	data, err := debefix_poc2.Load(provider)
	if err != nil {
		panic(err)
	}

	// spew.Dump(data)

	err = debefix_poc2.Resolve(data, func(tableID, tableName string, fields map[string]any) (map[string]any, error) {
		fmt.Printf("%s %s %s\n", strings.Repeat("=", 10), tableName, strings.Repeat("=", 10))
		spew.Dump(fields)

		ret := map[string]any{}
		for fn, fv := range fields {
			if fresolve, ok := fv.(debefix_poc2.ResolveValue); ok {
				switch fresolve.(type) {
				case *debefix_poc2.ResolveGenerate:
					ret[fn] = uuid.New()
				}
			}
		}

		if len(ret) > 0 {
			fmt.Println("---")
			spew.Dump(ret)
		}

		return ret, nil
	}, debefix_poc2.WithResolveTags([]string{}))
	if err != nil {
		panic(err)
	}
}
