package sql

import (
	"database/sql"

	"github.com/RangelReale/debefix-poc2/db"
)

func ResolverDBCallback(db QueryInterface, sqlBuilder QueryBuilder) db.ResolverDBCallback {
	return func(tableName string, fields map[string]any, returnFieldNames []string) (map[string]any, error) {
		var fieldNames []string
		var fieldPlaceholders []string
		var args []any

		placeholderProvider := sqlBuilder.CreatePlaceholderProvider()

		for fn, fv := range fields {
			fieldNames = append(fieldNames, fn)
			placeholder, argName := placeholderProvider.Next()
			fieldPlaceholders = append(fieldPlaceholders, placeholder)
			if argName != "" {
				args = append(args, sql.Named(argName, fv))
			} else {
				args = append(args, fv)
			}
		}

		query := sqlBuilder.BuildInsertSQL(tableName, fieldNames, fieldPlaceholders, returnFieldNames)

		ret, err := db.Query(query, returnFieldNames, args...)
		if err != nil {
			return nil, err
		}

		return ret, nil
	}
}

type QueryInterface interface {
	Query(query string, returnFieldNames []string, args ...any) (map[string]any, error)
}

type PlaceholderProvider interface {
	Next() (placeholder string, argName string)
}

type QueryBuilder interface {
	CreatePlaceholderProvider() PlaceholderProvider
	BuildInsertSQL(tableName string, fieldNames []string, fieldPlaceholders []string, returnFieldNames []string) string
}
