package sql

import (
	"context"
	"errors"
	"fmt"
	"io"

	"github.com/google/uuid"
)

type rowInterface interface {
	Scan(dest ...any) error
}

func rowToMap(cols []string, row rowInterface) (map[string]any, error) {
	// Create a slice of interface{}'s to represent each column,
	// and a second slice to contain pointers to each item in the columns slice.
	columns := make([]interface{}, len(cols))
	columnPointers := make([]interface{}, len(cols))
	for i, _ := range columns {
		columnPointers[i] = &columns[i]
	}

	// Scan the result into the column pointers...
	if err := row.Scan(columnPointers...); err != nil {
		return nil, err
	}

	// Create our map, and retrieve the value for each column from the pointers slice,
	// storing it in the map with the name of the column as the key.
	m := make(map[string]interface{})
	for i, colName := range cols {
		val := columnPointers[i].(*interface{})
		m[colName] = *val
	}

	return m, nil
}

func dumpSlice(out io.Writer, s []any) error {
	var allErr error
	var err error

	for i, v := range s {
		prefix := ""
		if i > 0 {
			prefix = " "
		}

		_, err = fmt.Fprintf(out, `%s[%d:"%v"]`, prefix, i, v)
		allErr = errors.Join(allErr, err)
	}

	return allErr
}

func dumpMap(out io.Writer, s map[string]any) error {
	var allErr error
	var err error

	first := true
	for i, v := range s {
		prefix := ""
		if !first {
			prefix = " "
		}
		first = false

		_, err = fmt.Fprintf(out, `%s[%s:"%v"]`, prefix, i, v)
		allErr = errors.Join(allErr, err)
	}

	return allErr
}

// QueryInterfaceCheck generates a smulated response for QueryInterface.Query
func QueryInterfaceCheck(ctx context.Context, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	ret := map[string]any{}
	for _, fn := range returnFieldNames {
		// simulate fields being generated
		ret[fn] = uuid.New()
	}
	return ret, nil
}
