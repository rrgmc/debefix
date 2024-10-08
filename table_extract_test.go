package debefix

import (
	"fmt"
	"testing"

	"github.com/google/uuid"
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

func TestDataExtractValues(t *testing.T) {
	tag2InternalID := uuid.New()

	data := &Data{
		Tables: map[string]*Table{
			"tags": {
				ID: "tags",
				Rows: Rows{
					Row{Fields: map[string]any{"tag_id": 1, "name": "this_is_1"}, Config: RowConfig{RefID: "tag1"}},
					Row{Fields: map[string]any{"tag_id": 2, "name": "this_is_2"}, Config: RowConfig{RefID: "tag2"}, InternalID: tag2InternalID},
				},
			},
			"posts": {
				ID: "posts",
				Rows: Rows{
					Row{Fields: map[string]any{"post_id": 5, "tag_id": 1}, Config: RowConfig{RefID: "post5"}},
					Row{Fields: map[string]any{"post_id": 3, "tag_id": 2}, Config: RowConfig{RefID: "post3"},
						Metadata: map[string]any{"post_image": "p1.jpg"},
						Parent: &defaultParentRowInfo{
							parent: &noParentRowInfo{},
							data: &defaultParentRowInfoData{
								tableID:    "tags",
								internalID: tag2InternalID,
							},
						}},
					Row{Fields: map[string]any{"post_id": 2, "tag_id": 1}, Config: RowConfig{RefID: "post2"}},
				},
			},
		},
	}

	values, err := data.ExtractValues(data.Tables["posts"].Rows[1],
		map[string]string{
			"v1": "value:post_id",
			"v2": "refid:tags:tag1:tag_id",
			"v3": "valueref:tag_id:tags:tag_id:name",
			"v4": "metadata:post_image",
			"v5": "metadata:post_noimage:default.jpg",
			"v6": "parent:name",
		})
	assert.NilError(t, err)

	assert.Assert(t, is.Len(values, 6))

	assert.Equal(t, 3, values["v1"])
	assert.Equal(t, 1, values["v2"])
	assert.Equal(t, "this_is_2", values["v3"])
	assert.Equal(t, "p1.jpg", values["v4"])
	assert.Equal(t, "default.jpg", values["v5"])
	assert.Equal(t, "this_is_2", values["v6"])
}
