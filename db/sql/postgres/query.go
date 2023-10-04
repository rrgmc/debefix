package postgres

import (
	"fmt"

	"github.com/RangelReale/debefix-poc2/db/sql"
)

// PlaceholderProvider generates postgres-compatible placeholders ($1, $2).
type PlaceholderProvider struct {
	c int
}

var _ sql.PlaceholderProvider = (*PlaceholderProvider)(nil)

func (p *PlaceholderProvider) Next() (placeholder string, argName string) {
	p.c++
	return fmt.Sprintf("$%d", p.c), ""
}

// SQLBuilder returns a postgres-compatible sql.QueryBuilder
func SQLBuilder() sql.QueryBuilder {
	return sql.DefaultSQLBuilder{
		PlaceholderProviderFactory: func() sql.PlaceholderProvider {
			return &PlaceholderProvider{}
		},
		QuoteTable: func(t string) string {
			return `"` + t + `"`
		},
		QuoteField: func(f string) string {
			return `"` + f + `"`
		},
	}
}
