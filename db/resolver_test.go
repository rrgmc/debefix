package db

import (
	"testing"
	"testing/fstest"

	"github.com/rrgmc/debefix"
	"github.com/stretchr/testify/require"
	"gotest.tools/v3/assert"
)

func TestResolver(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    config:
      table_name: "public.tags"
    rows:
      - tag_id: 2
        _refid: !dbfrefid "all"
        tag_name: "All"
      - tag_id: 5
        _refid: !dbfrefid "half"
        tag_name: "Half"
  posts:
    config:
      table_name: "public.posts"
      depends: ["tags"]
    rows:
      - post_id: 1
        _refid: !dbfrefid "post_1"
        title: "First post"
      - post_id: 2
        _refid: !dbfrefid "post_2"
        title: "Second post"
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
	assert.NilError(t, err)

	tables := map[string][]map[string]any{}
	var tableOrder []string

	_, err = debefix.Resolve(data, ResolverFunc(func(databaseName, tableName string, fields map[string]any, returnFieldNames []string) (returnValues map[string]any, err error) {
		tableOrder = append(tableOrder, tableName)
		tables[tableName] = append(tables[tableName], fields)
		return nil, nil
	}))
	assert.NilError(t, err)

	assert.DeepEqual(t, []string{"public.tags", "public.tags", "public.posts", "public.posts",
		"public.post_tags", "public.post_tags"}, tableOrder)

	assert.DeepEqual(t, []map[string]any{
		{
			"tag_id":   uint64(2),
			"tag_name": "All",
		},
		{
			"tag_id":   uint64(5),
			"tag_name": "Half",
		},
	}, tables["public.tags"])

	assert.DeepEqual(t, []map[string]any{
		{
			"post_id": uint64(1),
			"title":   "First post",
		},
		{
			"post_id": uint64(2),
			"title":   "Second post",
		},
	}, tables["public.posts"])

	assert.DeepEqual(t, []map[string]any{
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
			Data: []byte(`tables:
  tags:
    config:
      table_name: "public.tags"
    rows:
      - tag_id: !dbfexpr "generated"
        tag_name: "All"
`),
		},
	})

	data, err := debefix.Load(provider)
	assert.NilError(t, err)

	var tableOrder []string

	_, err = debefix.Resolve(data, ResolverFunc(func(databaseName, tableName string, fields map[string]any, returnFieldNames []string) (returnValues map[string]any, err error) {
		tableOrder = append(tableOrder, tableName)
		assert.Equal(t, tableName, "public.tags")
		assert.DeepEqual(t, returnFieldNames, []string{"tag_id"})
		require.NotContains(t, fields, "tag_id") // TODO: https://github.com/gotestyourself/gotest.tools/issues/147
		return map[string]any{
			"tag_id": 1,
		}, nil
	}))
	assert.NilError(t, err)

	assert.DeepEqual(t, []string{"public.tags"}, tableOrder)
}
