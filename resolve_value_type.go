package debefix

import (
	"context"
	"errors"

	"github.com/google/uuid"
)

// ResolveValueUUIDData parses the returned value as [uuid.UUID] if it isn't already.
type ResolveValueUUIDData struct {
	AllowNull  bool
	AllowBlank bool
}

// ResolveValueUUID parses the returned value as [uuid.UUID] if it isn't already.
func ResolveValueUUID(options ...ResolveValueTypeOption) ResolveValueUUIDData {
	var optns resolveValueTypeOptions
	for _, opt := range options {
		opt(&optns)
	}
	return ResolveValueUUIDData{
		AllowNull:  optns.allowNull,
		AllowBlank: optns.allowBlank,
	}
}

func (r ResolveValueUUIDData) ResolveValueParse(ctx context.Context, value any) (any, error) {
	switch v := value.(type) {
	case nil:
		if r.AllowNull {
			return v, nil
		}
		return nil, errors.New("cannot parse nil as UUID")
	case uuid.UUID:
		return v, nil
	case string:
		if v == "" && r.AllowBlank {
			return nil, nil
		}
		vv, err := uuid.Parse(v)
		if err != nil {
			return nil, NewResolveErrorf("error parsing resolved value as UUID ('%s'): %w", v, err)
		}
		return vv, nil
	default:
		return nil, NewResolveErrorf("invalid type conversion to UUID: '%T'", value)
	}
}

type ResolveValueTypeOption func(*resolveValueTypeOptions)

// WithResolveValueTypeAllowNull sets whether to allow null (nil) values.
func WithResolveValueTypeAllowNull(allowNull bool) ResolveValueTypeOption {
	return func(o *resolveValueTypeOptions) {
		o.allowNull = allowNull
	}
}

// WithResolveValueTypeAllowBlank sets whether to allow blank values.
func WithResolveValueTypeAllowBlank(allowBlank bool) ResolveValueTypeOption {
	return func(o *resolveValueTypeOptions) {
		o.allowBlank = allowBlank
	}
}

type resolveValueTypeOptions struct {
	allowNull  bool
	allowBlank bool
}
