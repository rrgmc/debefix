package postgres

import (
	debefix_poc2 "github.com/RangelReale/debefix-poc2"
	"github.com/RangelReale/debefix-poc2/db"
	"github.com/RangelReale/debefix-poc2/db/sql"
)

func Resolve(db sql.QueryInterface, data *debefix_poc2.Data, options ...debefix_poc2.ResolveOption) error {
	return debefix_poc2.Resolve(data, ResolverFunc(SQLResolverDBCallback(db)), options...)
}

func ResolverFunc(callback db.ResolverDBCallback) debefix_poc2.ResolveCallback {
	return db.ResolverFunc(callback)
}
