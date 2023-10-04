package debefix

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestResolve(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      _dbfconfig:
        id: "all"
      _dbfdeps:
        posts:
          rows:
            - post_id: 1
              tag_id: !dbfexpr "parent:tag_id"
              title: "First post"
              _dbfconfig:
                id: "post_1"
    - tag_id: 5
      tag_name: "Half"
post_tags:
  config:
    depends:
      - tags
  rows:
    - post_id: !dbfexpr "refid:posts:post_1:post_id"
      tag_id: !dbfexpr "refid:tags:all:tag_id"
`),
		},
	})

	data, err := Load(provider)
	require.NoError(t, err)

	rowCount := map[string]int{}
	var tableOrder []string

	err = Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		tableOrder = append(tableOrder, ctx.TableID())
		return ResolveCheckCallback(ctx, fields)
	})
	require.NoError(t, err)

	require.Equal(t, map[string]int{
		"tags":      2,
		"posts":     1,
		"post_tags": 1,
	}, rowCount)
	require.Equal(t, []string{"tags", "tags", "posts", "post_tags"}, tableOrder)
}
