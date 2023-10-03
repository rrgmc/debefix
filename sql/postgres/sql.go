package postgres

import (
	"fmt"

	"github.com/RangelReale/debefix-poc2/sql/generic"
)

type SQLPlaceholderProvider struct {
	c int
}

func (p *SQLPlaceholderProvider) Next() (placeholder string, argName string) {
	p.c++
	return fmt.Sprintf("$%d", p.c), ""
}

func SQLBuilder() generic.SQLBuilder {
	return generic.DefaultSQLBuilder{
		PlaceholderProviderFactory: func() generic.SQLPlaceholderProvider {
			return &SQLPlaceholderProvider{}
		},
		QuoteTable: func(t string) string {
			return `"` + t + `"`
		},
		QuoteField: func(f string) string {
			return `"` + f + `"`
		},
	}
}

func SQLResolverDBCallback(db generic.QueryInterface) generic.ResolverDBCallback {
	return generic.SQLResolverDBCallback(db, SQLBuilder())
}
