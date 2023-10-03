package postgres

import (
	debefix_poc2 "github.com/RangelReale/debefix-poc2"
	"github.com/RangelReale/debefix-poc2/sql/generic"
)

func Resolve(db generic.QueryInterface, data *debefix_poc2.Data, options ...debefix_poc2.ResolveOption) error {
	return debefix_poc2.Resolve(data, ResolverFunc(SQLResolverDBCallback(db)), options...)
}

func ResolverFunc(callback generic.ResolverDBCallback) debefix_poc2.ResolveCallback {
	return generic.ResolverFunc(callback)
}
