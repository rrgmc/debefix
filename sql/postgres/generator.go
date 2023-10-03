package postgres

import (
	"fmt"

	debefix_poc2 "github.com/RangelReale/debefix-poc2"
	"github.com/RangelReale/debefix-poc2/sql/generic"
)

func Resolve(db generic.QueryInterface, data *debefix_poc2.Data, options ...debefix_poc2.ResolveOption) error {
	return debefix_poc2.Resolve(data, ResolverFunc(SQLResolverDBCallback(db)), options...)
}

func ResolverFunc(callback generic.ResolverDBCallback) func(ctx debefix_poc2.ResolveContext, fields map[string]any) error {
	return generic.ResolverFunc(callback)
}

type SQLPlaceholderProvider struct {
	c int
}

func (p *SQLPlaceholderProvider) Next() (placeholder string, argName string) {
	p.c++
	return fmt.Sprintf("$%d", p.c), ""
}

func SQLResolverDBCallback(db generic.QueryInterface) generic.ResolverDBCallback {
	return generic.SQLResolverDBCallback(db, func() generic.SQLPlaceholderProvider {
		return &SQLPlaceholderProvider{}
	}, generic.DefaultSQLBuilder{})
}
