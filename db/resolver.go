package db

import (
	"github.com/RangelReale/debefix"
)

// ResolverFunc is a debefix.ResolveCallback helper to generate database records.
func ResolverFunc(callback ResolverDBCallback) debefix.ResolveCallback {
	return func(ctx debefix.ResolveContext, fields map[string]any) error {
		insertFields := map[string]any{}
		var returnFieldNames []string

		for fn, fv := range fields {
			if fresolve, ok := fv.(debefix.ResolveValue); ok {
				switch fresolve.(type) {
				case *debefix.ResolveGenerate:
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

// ResolverDBCallback will be called for each table row to be inserted.
// fields are the fields to be inserted.
// returnedFieldNames are the fields whose values are expected to be returned in the return map.
type ResolverDBCallback func(tableName string, fields map[string]any,
	returnFieldNames []string) (returnValues map[string]any, err error)
