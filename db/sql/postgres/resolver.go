package postgres

import (
	"github.com/RangelReale/debefix"
	"github.com/RangelReale/debefix/db"
	"github.com/RangelReale/debefix/db/sql"
)

// Resolve runs SQL INSERT queries on db.
func Resolve(db sql.QueryInterface, data *debefix.Data, options ...debefix.ResolveOption) error {
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
