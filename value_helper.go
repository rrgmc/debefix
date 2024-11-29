package debefix

import (
	"context"
	"errors"
)

// ValueFunc is a functional implementation of Value.
type ValueFunc func(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error)

var _ Value = (ValueFunc)(nil)

func (f ValueFunc) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	return f(ctx, resolvedData, values)
}

// ValueMultipleFunc is a functional implementation of ValueMultiple.
type ValueMultipleFunc func(ctx context.Context, resolvedData *ResolvedData, fieldName string, values ValuesMutable) error

var _ ValueMultipleFunc = (ValueMultipleFunc)(nil)

func (f ValueMultipleFunc) Resolve(ctx context.Context, resolvedData *ResolvedData, fieldName string, values ValuesMutable) error {
	return f(ctx, resolvedData, fieldName, values)
}

// ValueErr is an implementation of Value which always returns the passed error.
type ValueErr struct {
	Err error
}

var _ Value = ValueErr{}

func (v ValueErr) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	if v.Err == nil {
		return nil, false, errors.New("unknown error")
	}
	return nil, false, v.Err
}

// ValueMultipleErr is an implementation of ValueMultiple which always returns the passed error.
type ValueMultipleErr struct {
	Err error
}

var _ ValueMultiple = ValueMultipleErr{}

func (v ValueMultipleErr) Resolve(ctx context.Context, resolvedData *ResolvedData, fieldName string, values ValuesMutable) error {
	if v.Err == nil {
		return errors.New("unknown error")
	}
	return v.Err
}

// isNotAValue marks structs that may be confused with values to avoid using them as such.
type isNotAValue interface {
	isNotAValue()
}

type notAValue struct{}

func (notAValue) isNotAValue() {}
