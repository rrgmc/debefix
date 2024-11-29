package debefix

import (
	"context"
	"time"
)

// ResolvedData is Data after being resolved by the Resolve function.
type ResolvedData struct {
	Data
	BaseTime   time.Time
	TableOrder []string
}

func NewResolvedData() *ResolvedData {
	return &ResolvedData{
		BaseTime: time.Now(),
		Data:     *NewData(),
	}
}

// ResolveArgs resolves a list of arguments.
// It is used by the ValueFormat value to create a string value from other values.
func (d *ResolvedData) ResolveArgs(ctx context.Context, values Values, args ...any) ([]any, bool, error) {
	var resolvedArgs []any
	for argIdx, arg := range args {
		if dv, ok := arg.(Value); ok {
			argValue, argOk, err := dv.ResolveValue(ctx, d, values)
			if err != nil {
				return nil, false, NewResolveErrorf("error getting value of argument %d: %w", argIdx, err)
			}
			if !argOk {
				return nil, false, nil
			}
			resolvedArgs = append(resolvedArgs, argValue)
		} else if _, ok := arg.(ValueMultiple); ok {
			return nil, false, NewResolveErrorf("argument %d cannot be of 'ValueMultiple' type (type is '%T')", argIdx, arg)
		} else if _, ok := arg.(IsNotAValue); ok {
			return nil, false, NewResolveErrorf("argument %d should not be used as a field value (type %T)", argIdx, arg)
		} else {
			resolvedArgs = append(resolvedArgs, arg)
		}
	}
	return resolvedArgs, true, nil
}

// ResolveMapArgs resolves a list of arguments using a map source.
func (d *ResolvedData) ResolveMapArgs(ctx context.Context, values Values, args map[string]any) (map[string]any, bool, error) {
	var akeys []string
	var avalues []any
	for argName, argValue := range args {
		akeys = append(akeys, argName)
		avalues = append(avalues, argValue)
	}
	resolvedArgs, argOk, err := d.ResolveArgs(ctx, values, avalues...)
	if err != nil {
		return nil, false, err
	}
	if !argOk {
		return nil, false, nil
	}

	resolvedMapArgs := make(map[string]any)
	for keyIdx, key := range akeys {
		resolvedMapArgs[key] = resolvedArgs[keyIdx]
	}

	return resolvedMapArgs, true, nil
}
