package sql

import (
	"database/sql"
	"fmt"
	"slices"

	"github.com/rrgmc/debefix/db"
)

// ResolverDBCallback is a db.ResolverDBCallback to generate SQL-based database records.
// The parameter "fields" and "returnFieldNames" are always sorted to be deterministic.
func ResolverDBCallback(db QueryInterface, sqlBuilder QueryBuilder) db.ResolverDBCallback {
	return func(tableName string, fields map[string]any, returnFieldNames []string) (map[string]any, error) {
		// build INSERT query
		var fieldNames []string
		var fieldPlaceholders []string
		var args []any
		returnFieldNames = slices.Clone(returnFieldNames)

		for fn, _ := range fields {
			fieldNames = append(fieldNames, fn)
		}

		slices.Sort(fieldNames)
		slices.Sort(returnFieldNames)

		placeholderProvider := sqlBuilder.CreatePlaceholderProvider()

		for _, fn := range fieldNames {
			placeholder, argName := placeholderProvider.Next()
			fieldPlaceholders = append(fieldPlaceholders, placeholder)
			if argName != "" {
				args = append(args, sql.Named(argName, fields[fn]))
			} else {
				args = append(args, fields[fn])
			}
		}

		query := sqlBuilder.BuildInsertSQL(tableName, fieldNames, fieldPlaceholders, returnFieldNames)

		ret, err := db.Query(query, returnFieldNames, args...)
		if err != nil {
			return nil, fmt.Errorf("error executing query `%s`: %w", query, err)
		}

		return ret, nil
	}
}

// QueryInterface abstracts executing a query on a database. The return map should contain values for all fields
// specified in returnFieldNames.
type QueryInterface interface {
	Query(query string, returnFieldNames []string, args ...any) (map[string]any, error)
}

// QueryInterfaceFunc is a func adapter for QueryInterface
type QueryInterfaceFunc func(query string, returnFieldNames []string, args ...any) (map[string]any, error)

func (f QueryInterfaceFunc) Query(query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	return f(query, returnFieldNames, args...)
}

// PlaceholderProvider generates database-specific placeholders, like ? for MySQL, $1 for postgres, or :param1 for MSSQL.
// If the database uses named parameters, its name should be returned in argName, otherwise this should be blank.
type PlaceholderProvider interface {
	Next() (placeholder string, argName string)
}

// QueryBuilder is an abstraction for building INSERT queries.
type QueryBuilder interface {
	CreatePlaceholderProvider() PlaceholderProvider
	BuildInsertSQL(tableName string, fieldNames []string, fieldPlaceholders []string, returnFieldNames []string) string
}
