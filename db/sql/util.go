package sql

import (
	"errors"
	"fmt"
	"io"
	"os"

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

// QueryInterfaceCheck generates a smulated response for QueryInterface.Query
func QueryInterfaceCheck(query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	ret := map[string]any{}
	for _, fn := range returnFieldNames {
		// simulate fields being generated
		ret[fn] = uuid.New()
	}
	return ret, nil
}

// DebugQueryInterface is a QueryInterface that outputs the generated queries.
type DebugQueryInterface struct {
	Out io.Writer
}

func (m DebugQueryInterface) Query(query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	out := m.Out
	if out == nil {
		out = os.Stdout
	}

	var retErr error

	_, err := fmt.Fprintln(out, query)
	retErr = errors.Join(retErr, err)

	_, err = fmt.Fprintf(out, "%+v\n", args)
	retErr = errors.Join(retErr, err)

	_, err = fmt.Fprintf(out, "===\n")
	retErr = errors.Join(retErr, err)

	if retErr != nil {
		return nil, retErr
	}

	return QueryInterfaceCheck(query, returnFieldNames, args...)
}
