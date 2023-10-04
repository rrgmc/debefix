package postgres

import (
	debefix_poc2 "github.com/RangelReale/debefix-poc2"
	"github.com/RangelReale/debefix-poc2/db"
	"github.com/RangelReale/debefix-poc2/db/sql"
)

// Resolve runs SQL INSERT queries on db.
func Resolve(db sql.QueryInterface, data *debefix_poc2.Data, options ...debefix_poc2.ResolveOption) error {
	return debefix_poc2.Resolve(data, ResolverFunc(ResolverDBCallback(db)), options...)
}

// ResolverFunc is the debefix_poc2.ResolveCallback used by Resolve.
func ResolverFunc(callback db.ResolverDBCallback) debefix_poc2.ResolveCallback {
	return db.ResolverFunc(callback)
}

// ResolverDBCallback returns a postgres-compatible db.ResolverDBCallback.
func ResolverDBCallback(db sql.QueryInterface) db.ResolverDBCallback {
	return sql.ResolverDBCallback(db, SQLBuilder())
}
