package debefix

import (
	"testing"

	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

var (
	tableTags     = TableName("public.tags")
	tablePosts    = TableName("public.posts")
	tablePostTags = TableName("public.post_tags")
)

func TestData(t *testing.T) {
	data := NewData()

	data.AddValues(tableTags,
		MapValues{
			"tag_id":   2,
			"_refid":   SetValueRefID("all"),
			"tag_name": "All",
		},
		MapValues{
			"tag_id":   5,
			"_refid":   SetValueRefID("half"),
			"tag_name": "Half",
		},
	)

	data.AddValues(tablePosts,
		MapValues{
			"post_id": 1,
			"_refid":  SetValueRefID("post_1"),
			"title":   "First post",
		},
		MapValues{
			"post_id": 2,
			"_refid":  SetValueRefID("post_2"),
			"title":   "Second post",
		},
	)

	data.AddValues(tablePostTags,
		MapValues{
			"post_id": ValueRefID(tablePosts, "post_1", "post_id"),
			"tag_id":  ValueRefID(tableTags, "all", "tag_id"),
		},
		MapValues{
			"post_id": ValueRefID(tablePosts, "post_2", "post_id"),
			"tag_id":  ValueRefID(tableTags, "half", "tag_id"),
		},
	)

	tagsTable, ok := data.Tables[tableTags.TableID()]
	assert.Assert(t, ok, "tags table not found")

	postsTable, ok := data.Tables[tablePosts.TableID()]
	assert.Assert(t, ok, "posts table not found")

	postTagsTable, ok := data.Tables[tablePostTags.TableID()]
	assert.Assert(t, ok, "post_tags table not found")

	assert.Assert(t, is.Len(tagsTable.Rows, 2))
	assert.Assert(t, is.Len(postsTable.Rows, 2))
	assert.Assert(t, is.Len(postTagsTable.Rows, 2))

	AssertRowValuesDeepEqual(t, []map[string]any{
		{
			"tag_id":   2,
			"tag_name": "All",
		},
		{
			"tag_id":   5,
			"tag_name": "Half",
		},
	}, tagsTable.Rows)

	AssertRowValuesDeepEqual(t, []map[string]any{
		{
			"post_id": 1,
			"title":   "First post",
		},
		{
			"post_id": 2,
			"title":   "Second post",
		},
	}, postsTable.Rows)

	AssertRowValuesDeepEqual(t, []map[string]any{
		{
			"post_id": ValueRefID(tablePosts, "post_1", "post_id"),
			"tag_id":  ValueRefID(tableTags, "all", "tag_id"),
		},
		{
			"post_id": ValueRefID(tablePosts, "post_2", "post_id"),
			"tag_id":  ValueRefID(tableTags, "half", "tag_id"),
		},
	}, postTagsTable.Rows)
}
