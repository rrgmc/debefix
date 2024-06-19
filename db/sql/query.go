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

type SQLDB interface {
	QueryContext(ctx context.Context, query string, args ...any) (*sql.Rows, error)
	ExecContext(ctx context.Context, query string, args ...any) (sql.Result, error)
}

// SQLQueryInterfaceDBCallback is the callback to return a *sql.DB for each query.
type SQLQueryInterfaceDBCallback func(ctx context.Context, databaseName, tableName string) (SQLDB, error)

// sqlQueryInterface is a QueryInterface wrapper for *sql.DB.
type sqlQueryInterface struct {
	callback SQLQueryInterfaceDBCallback
}

var _ QueryInterface = (*sqlQueryInterface)(nil)

// NewSQLQueryInterface wraps a *sql.DB on the QueryInterface interface.
func NewSQLQueryInterface(db SQLDB) QueryInterface {
	return &sqlQueryInterface{callback: func(ctx context.Context, databaseName, tableName string) (SQLDB, error) {
		return db, nil
	}}
}

// NewSQLQueryInterfaceFunc sets a callback to return a *sql.DB for each query.
func NewSQLQueryInterfaceFunc(callback SQLQueryInterfaceDBCallback) QueryInterface {
	return &sqlQueryInterface{callback: callback}
}

func (q sqlQueryInterface) Query(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	db, err := q.callback(ctx, databaseName, tableName)
	if err != nil {
		return nil, err
	}

	if len(returnFieldNames) == 0 {
		_, err := db.ExecContext(ctx, query, args...)
		return nil, err
	}

	rows, err := db.QueryContext(ctx, query, args...)
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
	out              io.Writer
	lastDatabaseName string
	lastTableName    string
}

// NewDebugQueryInterface returns a QueryInterface that outputs the generated queries.
// If out is nil, [os.Stdout] will be used.
func NewDebugQueryInterface(out io.Writer) QueryInterface {
	if out == nil {
		out = os.Stdout
	}
	return &debugQueryInterface{out: out}
}

func (m *debugQueryInterface) Query(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	var retErr error
	var err error

	outTable := tableName
	if databaseName != "" {
		outTable = fmt.Sprintf("[%s] %s", databaseName, tableName)
	}

	if databaseName != m.lastDatabaseName || tableName != m.lastTableName {
		_, err = fmt.Fprintf(m.out, "%s %s %s\n", strings.Repeat("=", 15), outTable, strings.Repeat("=", 15))
		retErr = errors.Join(retErr, err)

		m.lastDatabaseName = databaseName
		m.lastTableName = tableName
	} else {
		_, _ = fmt.Fprint(m.out, strings.Repeat("-", 20))
	}

	_, err = fmt.Fprintln(m.out, query)
	retErr = errors.Join(retErr, err)

	if len(args) > 0 {
		_, err = fmt.Fprintf(m.out, "$$ ARGS: ")
		retErr = errors.Join(retErr, err)

		err = dumpSlice(m.out, args)
		retErr = errors.Join(retErr, err)

		_, err = fmt.Fprintf(m.out, "\n")
		retErr = errors.Join(retErr, err)
	}

	if retErr != nil {
		return nil, retErr
	}

	return QueryInterfaceCheck(ctx, query, returnFieldNames, args...)
}

type debugResultQueryInterface struct {
	qi      QueryInterface
	debugQI *debugQueryInterface
}

// NewDebugResultQueryInterface returns a [QueryInterface] calls an inner [QueryInterface] and outputs the generated
// queries and the returned fields. If out is nil, [os.Stdout] will be used.
func NewDebugResultQueryInterface(qi QueryInterface, out io.Writer) QueryInterface {
	if out == nil {
		out = os.Stdout
	}
	return &debugResultQueryInterface{qi: qi, debugQI: &debugQueryInterface{out: out}}
}

func (m *debugResultQueryInterface) Query(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	_, err := m.debugQI.Query(ctx, databaseName, tableName, query, returnFieldNames, args...)
	if err != nil {
		return nil, err
	}

	var retErr error

	result, queryErr := m.qi.Query(ctx, databaseName, tableName, query, returnFieldNames, args...)
	if queryErr == nil {
		if len(result) > 0 {
			_, err = fmt.Fprintf(m.debugQI.out, "## RESULT: ")
			retErr = errors.Join(retErr, err)

			err = dumpMap(m.debugQI.out, result)
			retErr = errors.Join(retErr, err)

			_, err = fmt.Fprintf(m.debugQI.out, "\n")
			retErr = errors.Join(retErr, err)
		}
	} else {
		_, err = fmt.Fprintf(m.debugQI.out, "@@ ERROR: %s\n", queryErr)
		retErr = errors.Join(retErr, err)
		retErr = errors.Join(retErr, queryErr)
	}

	return result, retErr
}
