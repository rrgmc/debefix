package debefix

import (
	"testing"

	"github.com/stretchr/testify/require"
)

func TestDataExtractRows(t *testing.T) {
	data := &Data{
		Tables: map[string]*Table{
			"tags": {
				ID: "tags",
				Rows: Rows{
					Row{Fields: map[string]any{"x": 1}},
					Row{Fields: map[string]any{"x": 2}},
				},
			},
			"posts": {
				ID: "posts",
				Rows: Rows{
					Row{Fields: map[string]any{"a": 5}},
					Row{Fields: map[string]any{"a": 3}},
					Row{Fields: map[string]any{"a": 2}},
				},
			},
		},
	}

	rows, err := data.ExtractRows(func(table *Table, row Row) (bool, error) {
		return (table.ID == "tags" && row.Fields["x"] == 2) ||
				(table.ID == "posts" && row.Fields["a"].(int) <= 3),
			nil
	})
	require.NoError(t, err)

	require.Len(t, rows.Tables["tags"].Rows, 1)
	require.Len(t, rows.Tables["posts"].Rows, 2)

	require.Equal(t, map[string]any{"x": 2}, rows.Tables["tags"].Rows[0].Fields)
	require.Equal(t, map[string]any{"a": 3}, rows.Tables["posts"].Rows[0].Fields)
	require.Equal(t, map[string]any{"a": 2}, rows.Tables["posts"].Rows[1].Fields)
}
