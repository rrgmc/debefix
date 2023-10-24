package filter

import (
	"strings"
	"testing"

	"github.com/rrgmc/debefix"
	"gotest.tools/v3/assert"
)

func TestFilterDataRefID(t *testing.T) {
	expectedDataRefID := map[string]FilterDataRefIDItem[filterDataTestValue]{}
	for idx, data := range allTestData {
		expectedDataRefID[strings.ToLower(data.Name)] = FilterDataRefIDItem[filterDataTestValue]{
			Index: idx,
			Data:  data,
		}
	}

	data, err := FilterDataRefID[filterDataTestValue](&debefix.Data{
		Tables: map[string]*debefix.Table{
			"test1": allTestTable,
		},
	}, "test1", func(row debefix.Row) (filterDataTestValue, error) {
		return fromRow(row.Fields), nil
	}, nil, WithFilterAll(true))
	assert.NilError(t, err)
	assert.DeepEqual(t, allTestData, data.Data)
	assert.DeepEqual(t, expectedDataRefID, data.DataRefID)
}
