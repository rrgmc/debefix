package sql

import (
	"fmt"

	"github.com/google/uuid"
)

type RowInterface interface {
	Scan(dest ...any) error
}

func RowToMap(cols []string, row RowInterface) (map[string]any, error) {
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

type OutputQueryInterface struct {
}

func (m OutputQueryInterface) Query(query string, returnFieldNames []string, args ...any) (map[string]any, error) {
	fmt.Println(query)
	fmt.Println(args)
	fmt.Printf("===\n")

	ret := map[string]any{}
	for _, fn := range returnFieldNames {
		ret[fn] = uuid.New()
	}

	return ret, nil
}
