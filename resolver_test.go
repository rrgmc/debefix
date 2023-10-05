package debefix

import (
	"errors"
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

func TestResolveGenerated(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: !dbfexpr generated
      tag_name: "All"
`),
		},
	})

	data, err := Load(provider)
	require.NoError(t, err)

	rowCount := map[string]int{}

	err = Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		require.IsType(t, &ResolveGenerate{}, fields["tag_id"])
		ctx.ResolveField("tag_id", 1)
		return nil
	})
	require.NoError(t, err)

	require.Equal(t, map[string]int{
		"tags": 1,
	}, rowCount)
}

func TestResolveTags(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      _dbfconfig:
        id: "all"
        tags: ["include"]
    - tag_id: 5
      tag_name: "Half"
posts:
  config:
    depends: ["tags"]
  rows:
    - post_id: 1
      title: "First post"
      _dbfconfig:
        id: "post_1"
        tags: ["include"]
    - post_id: 2
      title: "Second post"
      _dbfconfig:
        id: "post_2"
post_tags:
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

		switch ctx.TableID() {
		case "tags":
			require.Equal(t, "All", fields["tag_name"])
		case "posts":
			require.Equal(t, "First post", fields["title"])
		default:
			t.Fatalf("unexpected table id: %s", ctx.TableID())
		}

		return ResolveCheckCallback(ctx, fields)
	}, WithResolveTags([]string{"include"}))
	require.NoError(t, err)

	require.Equal(t, map[string]int{
		"tags":  1,
		"posts": 1,
	}, rowCount)
	require.Equal(t, []string{"tags", "posts"}, tableOrder)
}

func TestResolveUnresolvedRefID(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      _dbfconfig:
        id: "all"
    - tag_id: 5
      tag_name: "Half"
posts:
  rows:
    - post_id: 1
      title: "First post"
      tag_id: !dbfexpr "refid:tags:half:tag_id"
`),
		},
	})

	data, err := Load(provider)
	require.NoError(t, err)

	err = ResolveCheck(data)
	require.ErrorIs(t, err, ResolveValueError)
}

func TestResolveInvalidDependency(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      _dbfconfig:
        id: "all"
posts:
  config:
    depends: ["nothing"]
  rows:
    - post_id: 1
      title: "First post"
      tag_id: !dbfexpr "refid:tags:half:tag_id"
`),
		},
	})

	data, err := Load(provider)
	require.NoError(t, err)

	err = ResolveCheck(data)
	require.ErrorIs(t, err, ResolveError)
}

func TestResolveCallbackError(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: !dbfexpr generated
      tag_name: "All"
`),
		},
	})

	data, err := Load(provider)
	require.NoError(t, err)

	cbError := errors.New("test error")

	rowCount := map[string]int{}

	err = Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		return cbError
	})
	require.ErrorIs(t, err, cbError)

	require.Equal(t, map[string]int{
		"tags": 1,
	}, rowCount)
}
