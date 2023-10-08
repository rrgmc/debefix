package debefix

import (
	"os"
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
)

func TestGenerate(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      _dbfconfig:
        refid: "all"
      _dbfdeps:
        posts:
          rows:
            - post_id: 1
              tag_id: !dbfexpr "parent:tag_id"
              title: "First post"
    - tag_id: 5
      tag_name: "Half"
post_tags:
  config:
    depends:
      - posts
  rows:
    - post_id: 1
      tag_id: !dbfexpr "refid:tags:all:tag_id"
`),
		},
	})

	rowCount := map[string]int{}
	var tableOrder []string

	err := Generate(provider, func(ctx ResolveContext, fields map[string]any) error {
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

func TestGenerateOptions(t *testing.T) {
	providerData := fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`users:
  config:
    table_name: "public.user"
  rows:
    - user_id: 1
      name: "John Doe"
      _dbfconfig:
        refid: "johndoe"
    - user_id: 2
      name: "Jane Doe"
      _dbfconfig:
        refid: "janedoe"
`),
		},
	}

	called := map[string]bool{}

	expected := map[string]bool{
		"fsfileprovider_option": true,
		"load_option":           true,
		"resolve_option":        true,
	}

	err := GenerateFS(providerData, func(ctx ResolveContext, fields map[string]any) error {
		return ResolveCheckCallback(ctx, fields)
	},
		WithGenerateFSFileProviderOptions(
			WithDirectoryIncludeFunc(func(path string, entry os.DirEntry) bool {
				called["fsfileprovider_option"] = true
				return true
			}),
		),
		WithGenerateLoadOptions(
			WithLoadProgress(func(filename string) {
				called["load_option"] = true
			})),
		WithGenerateResolveOptions(
			WithResolveProgress(func(tableID, tableName string) {
				called["resolve_option"] = true

			})))
	require.NoError(t, err)

	require.Equal(t, expected, called)
}
