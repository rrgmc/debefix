package debefix

import "context"

// ResolveValue is a value that should be resolved by the user of the library in a callback.
// Usually this is used to return values generated by the database, like last insert id or created date.
type ResolveValue interface {
	ResolveValueParse(ctx context.Context, value any) (any, error)
}

// ResolveValueFunc is a functional implementation of ResolveValue.
type ResolveValueFunc func(ctx context.Context, value any) (any, error)

func (f ResolveValueFunc) ResolveValueParse(ctx context.Context, value any) (any, error) {
	return f(ctx, value)
}

// ResolveValueResolveData is a value to be resolved, without any parsing.
type ResolveValueResolveData struct {
}

var _ ResolveValue = ResolveValueResolveData{}

// ResolveValueResolve is a value to be resolved, without any parsing.
func ResolveValueResolve() ResolveValueResolveData {
	return ResolveValueResolveData{}
}

func (v ResolveValueResolveData) ResolveValueParse(ctx context.Context, value any) (any, error) {
	return value, nil
}
