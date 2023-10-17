package filter

import (
	"strings"
	"testing"
	"time"

	"github.com/rrgmc/debefix"
	"github.com/stretchr/testify/require"
)

type filterDataTestValue struct {
	Name      string
	Age       int
	City      string
	CreatedAt time.Time
}

func (d filterDataTestValue) toRow() map[string]any {
	return map[string]any{
		"name":       d.Name,
		"age":        d.Age,
		"city":       d.City,
		"created_at": d.CreatedAt,
	}
}

func fromRow(fields map[string]any) filterDataTestValue {
	return filterDataTestValue{
		Name:      fields["name"].(string),
		Age:       fields["age"].(int),
		City:      fields["city"].(string),
		CreatedAt: fields["created_at"].(time.Time),
	}
}

var allTestData = []filterDataTestValue{
	{
		Name:      "Mary",
		Age:       54,
		City:      "SF",
		CreatedAt: time.Now().Add(-200 * time.Hour),
	},
	{
		Name:      "John",
		Age:       32,
		City:      "LA",
		CreatedAt: time.Now(),
	},
	{
		Name:      "Jane",
		Age:       41,
		City:      "SF",
		CreatedAt: time.Now().Add(-100 * time.Hour),
	},
}

var allTestTable *debefix.Table

func init() {
	allTestTable = &debefix.Table{
		ID: "test1",
	}
	for _, data := range allTestData {
		allTestTable.Rows = append(allTestTable.Rows, debefix.Row{
			Config: debefix.RowConfig{
				RefID: strings.ToLower(data.Name),
			},
			Fields: data.toRow(),
		})
	}
}

func TestFilterData(t *testing.T) {
	tests := []struct {
		name     string
		expected []filterDataTestValue
		options  []FilterDataOption
	}{
		{
			name:     "get all",
			expected: allTestData,
			options:  []FilterDataOption{WithFilterAll(true)},
		},
		{
			name: "filter refid",
			expected: []filterDataTestValue{
				allTestData[0],
				allTestData[2],
			},
			options: []FilterDataOption{WithFilterRefIDs([]string{"jane", "mary"})},
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			data, err := FilterData[filterDataTestValue](&debefix.Data{
				Tables: map[string]*debefix.Table{
					"test1": allTestTable,
				},
			}, "test1", func(row debefix.Row) (filterDataTestValue, error) {
				return fromRow(row.Fields), nil
			}, test.options...)
			require.NoError(t, err)
			require.Equal(t, test.expected, data)
		})
	}
}
