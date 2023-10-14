package db

import (
	"testing"
	"testing/fstest"

	"github.com/RangelReale/debefix"
	"github.com/stretchr/testify/require"
)

func TestResolver(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  config:
    table_name: "public.tags"
  rows:
    - tag_id: 2
      tag_name: "All"
      _dbfconfig:
        refid: "all"
    - tag_id: 5
      tag_name: "Half"
      _dbfconfig:
        refid: "half"
posts:
  config:
    table_name: "public.posts"
    depends: ["tags"]
  rows:
    - post_id: 1
      title: "First post"
      _dbfconfig:
        refid: "post_1"
    - post_id: 2
      title: "Second post"
      _dbfconfig:
        refid: "post_2"
post_tags:
  config:
    table_name: "public.post_tags"
  rows:
    - post_id: !dbfexpr "refid:posts:post_1:post_id"
      tag_id: !dbfexpr "refid:tags:all:tag_id"
    - post_id: !dbfexpr "refid:posts:post_2:post_id"
      tag_id: !dbfexpr "refid:tags:half:tag_id"
`),
		},
	})

	data, err := debefix.Load(provider)
	require.NoError(t, err)

	tables := map[string][]map[string]any{}
	var tableOrder []string

	_, err = debefix.Resolve(data, ResolverFunc(func(tableName string, fields map[string]any, returnFieldNames []string) (returnValues map[string]any, err error) {
		tableOrder = append(tableOrder, tableName)
		tables[tableName] = append(tables[tableName], fields)
		return nil, nil
	}))
	require.NoError(t, err)

	require.Equal(t, []string{"public.tags", "public.tags", "public.posts", "public.posts",
		"public.post_tags", "public.post_tags"}, tableOrder)

	require.Equal(t, []map[string]any{
		{
			"tag_id":   uint64(2),
			"tag_name": "All",
		},
		{
			"tag_id":   uint64(5),
			"tag_name": "Half",
		},
	}, tables["public.tags"])

	require.Equal(t, []map[string]any{
		{
			"post_id": uint64(1),
			"title":   "First post",
		},
		{
			"post_id": uint64(2),
			"title":   "Second post",
		},
	}, tables["public.posts"])

	require.Equal(t, []map[string]any{
		{
			"post_id": uint64(1),
			"tag_id":  uint64(2),
		},
		{
			"post_id": uint64(2),
			"tag_id":  uint64(5),
		},
	}, tables["public.post_tags"])
}

func TestResolverGenerated(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  config:
    table_name: "public.tags"
  rows:
    - tag_id: !dbfexpr "generated"
      tag_name: "All"
`),
		},
	})

	data, err := debefix.Load(provider)
	require.NoError(t, err)

	var tableOrder []string

	_, err = debefix.Resolve(data, ResolverFunc(func(tableName string, fields map[string]any, returnFieldNames []string) (returnValues map[string]any, err error) {
		tableOrder = append(tableOrder, tableName)
		require.Equal(t, tableName, "public.tags")
		require.Equal(t, returnFieldNames, []string{"tag_id"})
		require.NotContains(t, fields, "tag_id")
		return map[string]any{
			"tag_id": 1,
		}, nil
	}))
	require.NoError(t, err)

	require.Equal(t, []string{"public.tags"}, tableOrder)
}
