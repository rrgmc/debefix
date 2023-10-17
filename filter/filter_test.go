package filter

import (
	"testing"
	"time"

	"github.com/rrgmc/debefix"
	"github.com/stretchr/testify/require"
)

type filterDataTestValue struct {
	Name      string
	Age       int
	CreatedAt time.Time
}

func TestFilterData(t *testing.T) {
	expected := []filterDataTestValue{
		{
			Name:      "John",
			Age:       54,
			CreatedAt: time.Now(),
		},
		{
			Name:      "Jane",
			Age:       54,
			CreatedAt: time.Now().Add(-100 * time.Hour),
		},
	}

	data, err := FilterData[filterDataTestValue](&debefix.Data{
		Tables: map[string]*debefix.Table{
			"test1": &debefix.Table{
				ID: "test1",
				Rows: debefix.Rows{
					debefix.Row{
						Fields: map[string]any{
							"name":       expected[0].Name,
							"age":        expected[0].Age,
							"created_at": expected[0].CreatedAt,
						},
					},
					debefix.Row{
						Fields: map[string]any{
							"name":       expected[1].Name,
							"age":        expected[1].Age,
							"created_at": expected[1].CreatedAt,
						},
					},
				},
			},
		},
	}, "test1", func(row debefix.Row) (filterDataTestValue, error) {
		return filterDataTestValue{
			Name:      row.Fields["name"].(string),
			Age:       row.Fields["age"].(int),
			CreatedAt: row.Fields["created_at"].(time.Time),
		}, nil
	}, WithFilterAll(true))

	require.NoError(t, err)
	require.Equal(t, expected, data)
}
