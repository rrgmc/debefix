package debefix

import (
	"testing"

	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

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
				// ID: "tags", // if data already exists, not setting will keep previous value
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

func TestDataClone(t *testing.T) {
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

	newData, err := data.Clone()
	assert.NilError(t, err)

	assert.Assert(t, is.Len(newData.Tables["tags"].Rows, 2))
	assert.Assert(t, is.Len(newData.Tables["posts"].Rows, 3))
	assert.Assert(t, newData.Tables["tags"] != data.Tables["tags"], "tables should have been cloned")
	assert.Assert(t, newData.Tables["posts"] != data.Tables["posts"], "tables should have been cloned")
}
