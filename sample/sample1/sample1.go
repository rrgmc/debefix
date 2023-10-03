package main

import (
	"fmt"
	"strings"

	debefix_poc2 "github.com/RangelReale/debefix-poc2"
	"github.com/RangelReale/debefix-poc2/sql/postgres"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
)

func main() {
	data, err := debefix_poc2.LoadDirectory(`/Users/rangelreale/prog/personal/debefix-poc2/sample/data1`)
	if err != nil {
		panic(err)
	}

	// spew.Dump(data)

	// err = resolvePrint(data)
	err = resolveSQL(data)
	if err != nil {
		panic(err)
	}
}

func resolvePrint(data *debefix_poc2.Data) error {
	return debefix_poc2.Resolve(data, func(ctx debefix_poc2.ResolveContext, fields map[string]any) error {
		fmt.Printf("%s %s %s\n", strings.Repeat("=", 10), ctx.TableName(), strings.Repeat("=", 10))
		spew.Dump(fields)

		resolved := map[string]any{}
		for fn, fv := range fields {
			if fresolve, ok := fv.(debefix_poc2.ResolveValue); ok {
				switch fresolve.(type) {
				case *debefix_poc2.ResolveGenerate:
					ctx.ResolveField(fn, uuid.New())
					resolved[fn] = uuid.New()
				}
			}
		}

		if len(resolved) > 0 {
			fmt.Println("---")
			spew.Dump(resolved)
		}

		return nil
	}, debefix_poc2.WithResolveTags([]string{}))
}

type MockDB struct {
}

func (m MockDB) Query(query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	fmt.Println(query)
	fmt.Println(args)
	fmt.Printf("===\n")

	ret := map[string]any{}
	for _, fn := range returnFieldNames {
		ret[fn] = uuid.New()
	}

	return ret, nil
}

func resolveSQL(data *debefix_poc2.Data) error {
	return postgres.Resolve(&MockDB{}, data)
}
