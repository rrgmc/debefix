package debefix

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

func TestResolve(t *testing.T) {
	ctx := context.Background()

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

	data.AddDependencies(tablePosts, tableTags)

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

	resolvedData, err := Resolve(ctx, data,
		func(ctx context.Context, resolveInfo ResolveInfo, values ValuesMutable) error {
			return nil
		})
	assert.NilError(t, err)

	tagsTable, ok := resolvedData.Tables[tableTags.TableID()]
	assert.Assert(t, ok, "tags table not found")

	postsTable, ok := resolvedData.Tables[tablePosts.TableID()]
	assert.Assert(t, ok, "posts table not found")

	postTagsTable, ok := resolvedData.Tables[tablePostTags.TableID()]
	assert.Assert(t, ok, "post_tags table not found")

	assert.Assert(t, is.Len(tagsTable.Rows, 2))
	assert.Assert(t, is.Len(postsTable.Rows, 2))
	assert.Assert(t, is.Len(postTagsTable.Rows, 2))

	assert.DeepEqual(t, []string{tableTags.TableID(), tablePosts.TableID(), tablePostTags.TableID()}, resolvedData.TableOrder)

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
			"post_id": 1,
			"tag_id":  2,
		},
		{
			"post_id": 2,
			"tag_id":  5,
		},
	}, postTagsTable.Rows)
}

func TestResolveGenerated(t *testing.T) {
	ctx := context.Background()

	data := NewData()

	data.AddValues(tableTags,
		MapValues{
			"tag_id":   ResolveValueResolve(),
			"tag_name": "All",
		},
	)

	resolvedData, err := Resolve(ctx, data,
		func(ctx context.Context, resolveInfo ResolveInfo, values ValuesMutable) error {
			assert.Equal(t, resolveInfo.TableID.TableID(), tableTags.TableID())
			assert.DeepEqual(t, ResolveValueResolve(), values.GetOrNil("tag_id"))
			values.Set("tag_id", 1)
			return nil
		})
	assert.NilError(t, err)

	AssertRowValuesDeepEqual(t, []map[string]any{
		{
			"tag_id":   1,
			"tag_name": "All",
		},
	}, resolvedData.Tables[tableTags.TableID()].Rows)
}

func TestResolveUnresolvedRefID(t *testing.T) {
	ctx := context.Background()

	data := NewData()

	data.AddValues(tableTags,
		MapValues{
			"tag_id":   2,
			"_refid":   SetValueRefID("all"),
			"tag_name": "All",
		},
	)

	data.AddValues(tablePosts,
		MapValues{
			"post_id": 1,
			"tag_id":  ValueRefID(tableTags, "half", "tag_id"),
			"title":   "First post",
		},
	)

	_, err := Resolve(ctx, data,
		func(ctx context.Context, resolveInfo ResolveInfo, values ValuesMutable) error {
			return nil
		})
	AssertIsResolveError(t, err)
}

func TestResolveInvalidDependency(t *testing.T) {
	ctx := context.Background()

	data := NewData()

	data.AddValues(tableTags,
		MapValues{
			"tag_id":   2,
			"tag_name": "All",
		},
	)

	data.AddDependencies(tableTags, tablePosts)

	_, err := Resolve(ctx, data,
		func(ctx context.Context, resolveInfo ResolveInfo, values ValuesMutable) error {
			return nil
		})
	AssertIsResolveError(t, err)
}

func TestResolveCallback(t *testing.T) {
	ctx := context.Background()

	data := NewData()

	var ctrc int
	var ctrupd int

	drcb := func(ctx context.Context, resolvedData *ResolvedData, resolveInfo ResolveInfo, row *Row) error {
		ctrc++
		if resolveInfo.Type == ResolveTypeUpdate {
			ctrupd++
		}
		return nil
	}

	data.Add(tableTags,
		MapValues{
			"tag_id":   2,
			"_refid":   SetValueRefID("all"),
			"tag_name": "All",
		},
		WithDataAddResolvedCallback(drcb))

	data.Add(tableTags,
		MapValues{
			"tag_id":   5,
			"_refid":   SetValueRefID("half"),
			"tag_name": "Half",
		},
		WithDataAddResolvedCallback(drcb))

	data.Add(tablePosts,
		MapValues{
			"post_id": 1,
			"_refid":  SetValueRefID("post_1"),
			"title":   "First post",
		},
		WithDataAddResolvedCallback(drcb))

	post2IID := data.AddWithID(tablePosts,
		MapValues{
			"post_id": 2,
			"_refid":  SetValueRefID("post_2"),
			"title":   "Second post",
		},
		WithDataAddResolvedCallback(drcb),
	)

	data.UpdateAfter(post2IID,
		post2IID.UpdateQuery([]string{"post_id"}),
		UpdateActionSetValues{Values: MapValues{
			"x": 14,
		}})

	data.AddDependencies(tablePosts, tableTags)

	_, err := Resolve(ctx, data,
		func(ctx context.Context, resolveInfo ResolveInfo, values ValuesMutable) error {
			return nil
		})
	assert.NilError(t, err)

	assert.Equal(t, 5, ctrc)
	assert.Equal(t, 1, ctrupd)
}

func TestResolveNotAValue(t *testing.T) {
	ctx := context.Background()

	data := NewData()

	data.AddValues(tableTags,
		MapValues{
			"tag_id":   2,
			"tag_name": "All",
			"iref":     NewInternalIDRef(tableTags, uuid.New()),
		},
	)

	_, err := Resolve(ctx, data,
		func(ctx context.Context, resolveInfo ResolveInfo, values ValuesMutable) error {
			return nil
		})
	AssertIsResolveError(t, err)
}
