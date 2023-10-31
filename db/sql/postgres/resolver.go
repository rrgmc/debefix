package postgres

import (
	"context"

	"github.com/rrgmc/debefix"
	"github.com/rrgmc/debefix/db"
	"github.com/rrgmc/debefix/db/sql"
)

// Resolve runs SQL INSERT queries on db, and returns the resolved data.
func Resolve(ctx context.Context, db sql.QueryInterface, data *debefix.Data, options ...debefix.ResolveOption) (*debefix.Data, error) {
	return debefix.Resolve(data, ResolverFunc(ctx, db), options...)
}

// ResolverFunc is the debefix.ResolveCallback used by Resolve.
func ResolverFunc(ctx context.Context, dbi sql.QueryInterface) debefix.ResolveCallback {
	return db.ResolverFunc(ResolverDBCallback(ctx, dbi))
}

// ResolverDBCallback returns a postgres-compatible db.ResolverDBCallback.
func ResolverDBCallback(ctx context.Context, db sql.QueryInterface) db.ResolverDBCallback {
	return sql.ResolverDBCallback(ctx, db, SQLBuilder())
}
