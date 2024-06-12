package debefix

import (
	"os"
	"testing"
	"testing/fstest"

	"gotest.tools/v3/assert"
)

func TestGenerate(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 2
        tag_name: "All"
        config:
          !dbfconfig
          refid: "all"
        deps:
          !dbfdeps
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

	_, err := Generate(provider, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		tableOrder = append(tableOrder, ctx.TableID())
		return ResolveCheckCallback(ctx, fields)
	})
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags":      2,
		"posts":     1,
		"post_tags": 1,
	}, rowCount)
	assert.DeepEqual(t, []string{"tags", "tags", "posts", "post_tags"}, tableOrder)
}

func TestGenerateOptions(t *testing.T) {
	providerData := fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    config:
      table_name: "public.user"
    rows:
      - user_id: 1
        name: "John Doe"
        config:
          !dbfconfig
          refid: "johndoe"
      - user_id: 2
        name: "Jane Doe"
        config:
          !dbfconfig
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

	_, err := GenerateFS(providerData, func(ctx ResolveContext, fields map[string]any) error {
		return ResolveCheckCallback(ctx, fields)
	},
		WithDirectoryIncludeFunc(func(path string, entry os.DirEntry) bool {
			called["fsfileprovider_option"] = true
			return true
		}),
		WithLoadProgress(func(filename string) {
			called["load_option"] = true
		}),
		WithResolveProgress(func(tableID, databaseName, tableName string) {
			called["resolve_option"] = true

		}),
		WithGenerateResolveCheck(true))
	assert.NilError(t, err)

	assert.DeepEqual(t, expected, called)
}
