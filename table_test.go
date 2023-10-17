package debefix

import (
	"fmt"
	"testing"

	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
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
	assert.NilError(t, err)

	assert.Assert(t, is.Len(rows.Tables["tags"].Rows, 1))
	assert.Assert(t, is.Len(rows.Tables["posts"].Rows, 2))

	assert.DeepEqual(t, map[string]any{"x": 2}, rows.Tables["tags"].Rows[0].Fields)
	assert.DeepEqual(t, map[string]any{"a": 3}, rows.Tables["posts"].Rows[0].Fields)
	assert.DeepEqual(t, map[string]any{"a": 2}, rows.Tables["posts"].Rows[1].Fields)
}

func TestDataExtractRowsNamed(t *testing.T) {
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

	rows, err := data.ExtractRowsNamed(func(table *Table, row Row) (bool, string, error) {
		if (table.ID == "tags" && row.Fields["x"] == 2) ||
			(table.ID == "posts" && row.Fields["a"].(int) <= 3) {
			if table.ID == "tags" {
				return true, fmt.Sprintf("%s:%d", table.ID, row.Fields["x"]), nil
			}
			return true, fmt.Sprintf("%s:%d", table.ID, row.Fields["a"]), nil
		}
		return false, "", nil
	})
	assert.NilError(t, err)

	assert.Assert(t, is.Len(rows, 3))

	assert.DeepEqual(t, map[string]any{"x": 2}, rows["tags:2"].Fields)
	assert.DeepEqual(t, map[string]any{"a": 3}, rows["posts:3"].Fields)
	assert.DeepEqual(t, map[string]any{"a": 2}, rows["posts:2"].Fields)
}

func TestDataMerge(t *testing.T) {
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

	data2 := &Data{
		Tables: map[string]*Table{
			"tags": {
				ID: "tags",
				Rows: Rows{
					Row{Fields: map[string]any{"x": 3}},
					Row{Fields: map[string]any{"x": 4}},
				},
			},
			"categories": {
				ID: "categories",
				Rows: Rows{
					Row{Fields: map[string]any{"c": 9}},
				},
			},
		},
	}

	newData, err := MergeData(data, data2)

	assert.NilError(t, err)
	assert.Assert(t, is.Len(newData.Tables["tags"].Rows, 4))
	assert.Assert(t, is.Len(newData.Tables["posts"].Rows, 3))
	assert.Assert(t, is.Len(newData.Tables["categories"].Rows, 1))
	assert.Assert(t, newData.Tables["tags"] != data.Tables["tags"], "tables should have been cloned")
	assert.Assert(t, newData.Tables["tags"] != data2.Tables["tags"], "tables should have been cloned")
}
