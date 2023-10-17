package postgres

import (
	"github.com/rrgmc/debefix"
	"github.com/rrgmc/debefix/db"
	"github.com/rrgmc/debefix/db/sql"
)

// Resolve runs SQL INSERT queries on db, and returns the resolved data.
func Resolve(db sql.QueryInterface, data *debefix.Data, options ...debefix.ResolveOption) (*debefix.Data, error) {
	return debefix.Resolve(data, ResolverFunc(db), options...)
}

// ResolverFunc is the debefix.ResolveCallback used by Resolve.
func ResolverFunc(dbi sql.QueryInterface) debefix.ResolveCallback {
	return db.ResolverFunc(ResolverDBCallback(dbi))
}

// ResolverDBCallback returns a postgres-compatible db.ResolverDBCallback.
func ResolverDBCallback(db sql.QueryInterface) db.ResolverDBCallback {
	return sql.ResolverDBCallback(db, SQLBuilder())
}
