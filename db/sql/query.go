package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

// sqlQueryInterface is a QueryInterface wrapper for *sql.DB.
type sqlQueryInterface struct {
	DB *sql.DB
}

var _ QueryInterface = (*sqlQueryInterface)(nil)

// NewSQLQueryInterface wraps a *sql.DB on the QueryInterface interface.
func NewSQLQueryInterface(db *sql.DB) QueryInterface {
	return &sqlQueryInterface{db}
}

func (q sqlQueryInterface) Query(query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	if len(returnFieldNames) == 0 {
		_, err := q.DB.Exec(query, args...)
		return nil, err
	}

	rows, err := q.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, errors.New("no records on query")
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	ret, err := rowToMap(cols, rows)
	if err != nil {
		return nil, err
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return ret, nil
}

// defaultSQLPlaceholderProvider returns placeholders using ?
type defaultSQLPlaceholderProvider struct {
}

func (d defaultSQLPlaceholderProvider) Next() (placeholder string, argName string) {
	return "?", ""
}

// DefaultSQLBuilder is the default customizable INSERT SQL builder, using placeholders for values.
// It uses "RETURNING" to get the returnFieldNames.
type DefaultSQLBuilder struct {
	PlaceholderProviderFactory func() PlaceholderProvider // uses defaultSQLPlaceholderProvider if not set
	QuoteTable                 func(t string) string      // don't quote if not set
	QuoteField                 func(f string) string      // don't quote if not set
}

func (d DefaultSQLBuilder) CreatePlaceholderProvider() PlaceholderProvider {
	if d.PlaceholderProviderFactory == nil {
		return &defaultSQLPlaceholderProvider{}
	}
	return d.PlaceholderProviderFactory()
}

func (d DefaultSQLBuilder) BuildInsertSQL(tableName string, fieldNames []string, fieldPlaceholders []string, returnFieldNames []string) string {
	// quote table
	if d.QuoteTable != nil {
		tableName = d.QuoteTable(tableName)
	}

	// quote fields
	if d.QuoteField != nil {
		fieldNames = slices.Clone(fieldNames)
		for fi := range fieldNames {
			fieldNames[fi] = d.QuoteField(fieldNames[fi])
		}

		returnFieldNames = slices.Clone(returnFieldNames)
		for fi := range returnFieldNames {
			returnFieldNames[fi] = d.QuoteField(returnFieldNames[fi])
		}
	}

	ret := fmt.Sprintf("INSERT INTO %s (%s) VALUES (%s)",
		tableName,
		strings.Join(fieldNames, ", "),
		strings.Join(fieldPlaceholders, ", "),
	)

	if len(returnFieldNames) > 0 {
		ret += fmt.Sprintf(" RETURNING %s", strings.Join(returnFieldNames, ","))
	}

	return ret
}
