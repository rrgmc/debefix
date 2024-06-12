package sql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"os"
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

func (q sqlQueryInterface) Query(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	if len(returnFieldNames) == 0 {
		_, err := q.DB.Exec(query, args...)
		return nil, err
	}

	rows, err := q.DB.QueryContext(ctx, query, args...)
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

func (d DefaultSQLBuilder) BuildInsertSQL(databaseName, tableName string, fieldNames []string, fieldPlaceholders []string, returnFieldNames []string) string {
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

type multiQueryInterface struct {
	itfs []QueryInterface
}

// NewMultiQueryInterface returns a [QueryInterface] that calls multiple [QueryInterface], returning the result of the
// last one.
func NewMultiQueryInterface(itfs []QueryInterface) QueryInterface {
	return &multiQueryInterface{itfs}
}

func (m multiQueryInterface) Query(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (ret map[string]any, err error) {
	for _, i := range m.itfs {
		ret, err = i.Query(ctx, databaseName, tableName, query, returnFieldNames, args...)
		if err != nil {
			return nil, err
		}
	}
	return ret, err
}

type debugQueryInterface struct {
	out io.Writer
}

// NewDebugQueryInterface returns a QueryInterface that outputs the generated queries.
// If out is nil, [os.Stdout] will be used.
func NewDebugQueryInterface(out io.Writer) QueryInterface {
	return &debugQueryInterface{out}
}

func (m debugQueryInterface) Query(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	out := m.out
	if out == nil {
		out = os.Stdout
	}

	var retErr error

	_, err := fmt.Fprintln(out, query)
	retErr = errors.Join(retErr, err)

	err = dumpSlice(out, args)
	retErr = errors.Join(retErr, err)

	_, _ = fmt.Fprintf(out, "\n")
	retErr = errors.Join(retErr, err)

	outTable := tableName
	if databaseName != "" {
		outTable = fmt.Sprintf("[%s] %s", databaseName, tableName)
	}

	_, err = fmt.Fprintf(out, "=== %s\n", outTable)
	retErr = errors.Join(retErr, err)

	if retErr != nil {
		return nil, retErr
	}

	return QueryInterfaceCheck(ctx, query, returnFieldNames, args...)
}

type debugResultQueryInterface struct {
	qi  QueryInterface
	out io.Writer
}

// NewDebugResultQueryInterface returns a [QueryInterface] calls an inner [QueryInterface] and outputs the generated
// queries and the returned fields. If out is nil, [os.Stdout] will be used.
func NewDebugResultQueryInterface(qi QueryInterface, out io.Writer) QueryInterface {
	return &debugResultQueryInterface{qi, out}
}

func (m debugResultQueryInterface) Query(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	out := m.out
	if out == nil {
		out = os.Stdout
	}

	_, _ = fmt.Fprintln(out, query)
	_ = dumpSlice(out, args)
	_, _ = fmt.Fprintf(out, "\n")

	result, err := m.qi.Query(ctx, databaseName, tableName, query, returnFieldNames, args...)
	if err == nil {
		if len(result) > 0 {
			_, _ = fmt.Fprintf(out, "result: ")
			_ = dumpMap(out, result)
			_, _ = fmt.Fprintf(out, "\n")
		}
	} else {
		_, err = fmt.Fprintf(out, "error: %s\n", err)
	}

	_, _ = fmt.Fprintf(out, "===\n")

	return result, err
}
