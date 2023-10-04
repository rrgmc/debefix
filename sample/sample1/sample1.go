package main

import (
	"errors"
	"fmt"
	"path/filepath"
	"runtime"
	"strings"

	debefix_poc2 "github.com/RangelReale/debefix-poc2"
	"github.com/RangelReale/debefix-poc2/db/sql"
	"github.com/RangelReale/debefix-poc2/db/sql/postgres"
	"github.com/davecgh/go-spew/spew"
	"github.com/google/uuid"
)

func currentSourceDirectory() (string, error) {
	_, filename, _, ok := runtime.Caller(0)
	if !ok {
		return "", errors.New("unable to get the current filename")
	}
	return filepath.Dir(filename), nil
}

func main() {
	curDir, err := currentSourceDirectory()
	if err != nil {
		panic(err)
	}

	data, err := debefix_poc2.LoadDirectory(filepath.Join(curDir, "..", "data1"),
		debefix_poc2.WithDirectoryAsTag())
	if err != nil {
		panic(err)
	}

	// spew.Dump(data)

	resolveTags := []string{}

	err = debefix_poc2.ResolveCheck(data, debefix_poc2.WithResolveTags(resolveTags))
	if err != nil {
		panic(err)
	}

	// err = resolvePrint(data, resolveTags)
	// if err != nil {
	// 	panic(err)
	// }

	err = resolveSQL(data, resolveTags)
	if err != nil {
		panic(err)
	}
}

func resolvePrint(data *debefix_poc2.Data, resolveTags []string) error {
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
	}, debefix_poc2.WithResolveTags(resolveTags))
}

func resolveSQL(data *debefix_poc2.Data, resolveTags []string) error {
	return postgres.Resolve(&sql.OutputQueryInterface{}, data, debefix_poc2.WithResolveTags(resolveTags))
}
