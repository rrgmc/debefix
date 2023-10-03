package db

import (
	debefix_poc2 "github.com/RangelReale/debefix-poc2"
)

type ResolverDBCallback func(tableName string, fields map[string]any, returnFieldNames []string) (map[string]any, error)

func ResolverFunc(callback ResolverDBCallback) debefix_poc2.ResolveCallback {
	return func(ctx debefix_poc2.ResolveContext, fields map[string]any) error {
		insertFields := map[string]any{}
		var returnFieldNames []string

		for fn, fv := range fields {
			if fresolve, ok := fv.(debefix_poc2.ResolveValue); ok {
				switch fresolve.(type) {
				case *debefix_poc2.ResolveGenerate:
					returnFieldNames = append(returnFieldNames, fn)
				}
			} else {
				insertFields[fn] = fv
			}
		}

		resolved, err := callback(ctx.TableName(), insertFields, returnFieldNames)
		if err != nil {
			return err
		}

		for rn, rv := range resolved {
			ctx.ResolveField(rn, rv)
		}

		return nil
	}
}
