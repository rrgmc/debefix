package sql

import (
	"database/sql"
	"errors"
	"fmt"
	"slices"
	"strings"
)

type SQLQueryInterface struct {
	DB *sql.DB
}

var _ QueryInterface = (*SQLQueryInterface)(nil)

func (q SQLQueryInterface) Query(query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	rows, err := q.DB.Query(query, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	if !rows.Next() {
		return nil, errors.New("no records")
	}

	cols, err := rows.Columns()
	if err != nil {
		return nil, err
	}

	ret, err := RowToMap(cols, rows)
	if err != nil {
		return nil, err
	}

	if rows.Err() != nil {
		return nil, rows.Err()
	}

	return ret, nil
}

type defaultSQLPlaceholderProvider struct {
}

func (d defaultSQLPlaceholderProvider) Next() (placeholder string, argName string) {
	return "?", ""
}

type DefaultSQLBuilder struct {
	PlaceholderProviderFactory func() PlaceholderProvider
	QuoteTable                 func(t string) string
	QuoteField                 func(f string) string
}

func (d DefaultSQLBuilder) CreatePlaceholderProvider() PlaceholderProvider {
	if d.PlaceholderProviderFactory == nil {
		return &defaultSQLPlaceholderProvider{}
	}
	return d.PlaceholderProviderFactory()
}

func (d DefaultSQLBuilder) BuildInsertSQL(tableName string, fieldNames []string, fieldPlaceholders []string, returnFieldNames []string) string {
	if d.QuoteTable != nil {
		tableName = d.QuoteTable(tableName)
	}

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
