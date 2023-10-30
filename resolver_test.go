package debefix

import (
	"errors"
	"fmt"
	"strconv"
	"testing"
	"testing/fstest"

	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

func TestResolve(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
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

	data, err := Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}
	var tableOrder []string

	_, err = Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
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
	assert.NilError(t, err)

	rowCount := map[string]int{}

	_, err = Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		assert.DeepEqual(t, &ResolveGenerate{}, fields["tag_id"])
		ctx.ResolveField("tag_id", 1)
		return nil
	})
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
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
      config:
        !dbfconfig
        refid: "all"
        tags: ["include"]
    - tag_id: 5
      tag_name: "Half"
posts:
  config:
    depends: ["tags"]
  rows:
    - post_id: 1
      title: "First post"
      config:
        !dbfconfig
        refid: "post_1"
        tags: ["include"]
    - post_id: 2
      title: "Second post"
      config:
        !dbfconfig
        refid: "post_2"
post_tags:
  rows:
    - post_id: !dbfexpr "refid:posts:post_1:post_id"
      tag_id: !dbfexpr "refid:tags:all:tag_id"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}
	var tableOrder []string

	_, err = Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		tableOrder = append(tableOrder, ctx.TableID())

		switch ctx.TableID() {
		case "tags":
			assert.Equal(t, "All", fields["tag_name"])
		case "posts":
			assert.Equal(t, "First post", fields["title"])
		default:
			t.Fatalf("unexpected table id: %s", ctx.TableID())
		}

		return ResolveCheckCallback(ctx, fields)
	}, WithResolveTags([]string{"include"}))
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags":  1,
		"posts": 1,
	}, rowCount)
	assert.DeepEqual(t, []string{"tags", "posts"}, tableOrder)
}

func TestResolveIgnoreTags(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      config:
        !dbfconfig
        refid: "all"
    - tag_id: 5
      tag_name: "Half"
      config:
        !dbfconfig
        refid: "half"
        ignoreTags: true
posts:
  config:
    depends: ["tags"]
  rows:
    - post_id: 1
      title: "First post"
      config:
        !dbfconfig
        refid: "post_1"
        tags: ["include"]
    - post_id: 2
      title: "Second post"
      config:
        !dbfconfig
        refid: "post_2"
post_tags:
  rows:
    - post_id: !dbfexpr "refid:posts:post_1:post_id"
      tag_id: !dbfexpr "refid:tags:all:tag_id"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}
	var tableOrder []string

	_, err = Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		tableOrder = append(tableOrder, ctx.TableID())

		switch ctx.TableID() {
		case "tags":
			assert.Equal(t, "Half", fields["tag_name"])
		case "posts":
			assert.Equal(t, "First post", fields["title"])
		default:
			t.Fatalf("unexpected table id: %s", ctx.TableID())
		}

		return ResolveCheckCallback(ctx, fields)
	}, WithResolveTags([]string{"include"}))
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags":  1,
		"posts": 1,
	}, rowCount)
	assert.DeepEqual(t, []string{"tags", "posts"}, tableOrder)
}

func TestResolveUnresolvedRefID(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      config:
        !dbfconfig
        refid: "all"
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
	assert.NilError(t, err)

	err = ResolveCheck(data)
	assert.ErrorIs(t, err, ResolveValueError)
}

func TestResolveInvalidDependency(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      config:
        !dbfconfig
        refid: "all"
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
	assert.NilError(t, err)

	err = ResolveCheck(data)
	assert.ErrorIs(t, err, ResolveError)
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
	assert.NilError(t, err)

	cbError := errors.New("test error")

	rowCount := map[string]int{}

	_, err = Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		return cbError
	})
	assert.ErrorIs(t, err, cbError)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)
}

func TestResolveReturnResolved(t *testing.T) {
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
	assert.NilError(t, err)

	rowCount := map[string]int{}

	retData, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		assert.DeepEqual(t, &ResolveGenerate{}, fields["tag_id"])
		ctx.ResolveField("tag_id", 935)
		return nil
	})
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	assert.Assert(t, is.Len(retData.Tables, 1))
	assert.Assert(t, is.Len(retData.Tables["tags"].Rows, 1))
	assert.Equal(t, 935, retData.Tables["tags"].Rows[0].Fields["tag_id"])
}

func TestResolveGeneratedWithType(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: !dbfexpr generated:int
      tag_name: "All"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}

	resolved, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		assert.DeepEqual(t, &ResolveGenerate{Type: "int"}, fields["tag_id"])
		ctx.ResolveField("tag_id", "45")
		return nil
	})
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	assert.DeepEqual(t, map[string]any{
		"tag_id":   int64(45),
		"tag_name": "All",
	}, resolved.Tables["tags"].Rows[0].Fields)
}

func TestResolveGeneratedWithParserType(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: !dbfexpr generated:myint32
      tag_name: "All"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}

	resolved, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		assert.DeepEqual(t, &ResolveGenerate{Type: "myint32"}, fields["tag_id"])
		ctx.ResolveField("tag_id", "95")
		return nil
	}, WithResolvedValueParser(&testValueParserInt32{}))
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	assert.DeepEqual(t, map[string]any{
		"tag_id":   int32(95),
		"tag_name": "All",
	}, resolved.Tables["tags"].Rows[0].Fields)
}

type testValueParserInt32 struct {
}

func (r testValueParserInt32) ParseResolvedValue(typ string, value any) (bool, any, error) {
	if typ != "myint32" {
		return false, nil, nil
	}

	switch vv := value.(type) {
	case int32:
		return true, vv, nil
	default:
		v, err := strconv.ParseInt(fmt.Sprint(value), 10, 32)
		return true, int32(v), err
	}
}
