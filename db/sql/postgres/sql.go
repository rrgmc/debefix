package postgres

import (
	"fmt"

	"github.com/RangelReale/debefix-poc2/db"
	"github.com/RangelReale/debefix-poc2/db/sql"
)

func SQLResolverDBCallback(db sql.QueryInterface) db.ResolverDBCallback {
	return sql.SQLResolverDBCallback(db, SQLBuilder())
}

type SQLPlaceholderProvider struct {
	c int
}

func (p *SQLPlaceholderProvider) Next() (placeholder string, argName string) {
	p.c++
	return fmt.Sprintf("$%d", p.c), ""
}

func SQLBuilder() sql.SQLBuilder {
	return sql.DefaultSQLBuilder{
		PlaceholderProviderFactory: func() sql.SQLPlaceholderProvider {
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
