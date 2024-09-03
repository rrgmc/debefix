package debefix

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/goccy/go-yaml/ast"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

func TestResolve(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 2
        _refid: !refid "all"
        tag_name: "All"
        deps:
          !deps
          posts:
            rows:
              - post_id: 1
                tag_id: !expr "parent:tag_id"
                title: "First post"
      - tag_id: 5
        tag_name: "Half"
  post_tags:
    config:
      depends:
        - posts
    rows:
      - post_id: 1
        tag_id: !expr "refid:tags:all:tag_id"
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
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: !expr generated
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

func TestResolveCalculated(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: !expr calculated:myint32
        tag_name: "All"
`),
		},
	})

	expected := int32(998)

	data, err := Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}

	resolved, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		return nil
	}, WithResolvedValueCalculator(testValueCalculatorInt32{expected}))
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	intVal, isIntVal := resolved.Tables["tags"].Rows[0].Fields["tag_id"].(int32)
	assert.Assert(t, isIntVal, "field value should be of int32 type")
	assert.Equal(t, expected, intVal)
}

func TestResolveNamed(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: !expr resolve:r1
        tag_name: "TAGME"
        slug: !expr resolve:r2
`),
		},
	})

	expectedID := int32(998)
	expectedName := "tagme"

	data, err := Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}

	resolved, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		return nil
	}, WithNamedResolveCallback(NamedResolveCallbackFunc(func(ctx ValueResolveContext, name string) (resolvedValue any, addField bool, err error) {
		switch name {
		case "r1":
			return expectedID, true, nil
		case "r2":
			return strings.ToLower(ctx.Row().Fields["tag_name"].(string)), true, nil
		default:
			return nil, false, fmt.Errorf("unknown field: %s", name)
		}
	})))
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	idVal, isIntVal := resolved.Tables["tags"].Rows[0].Fields["tag_id"].(int32)
	nameVal, isStringVal := resolved.Tables["tags"].Rows[0].Fields["slug"].(string)
	assert.Assert(t, isIntVal, "field id should be of int32 type")
	assert.Assert(t, isStringVal, "field name should be of string type")
	assert.Equal(t, expectedID, idVal)
	assert.Equal(t, expectedName, nameVal)
}

func TestResolveTags(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 2
        _refid: !refid "all"
        _tags: !tags ["include"]
        tag_name: "All"
      - tag_id: 5
        tag_name: "Half"
  posts:
    config:
      depends: ["tags"]
    rows:
      - post_id: 1
        _refid: !refid "post_1"
        _tags: !tags ["include"]
        title: "First post"
      - post_id: 2
        _refid: !refid "post_2"
        title: "Second post"
  post_tags:
    rows:
      - post_id: !expr "refid:posts:post_1:post_id"
        tag_id: !expr "refid:tags:all:tag_id"
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

func TestResolveUnresolvedRefID(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 2
        _refid: !refid "all"
        tag_name: "All"
      - tag_id: 5
        tag_name: "Half"
  posts:
    rows:
      - post_id: 1
        title: "First post"
        tag_id: !expr "refid:tags:half:tag_id"
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
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 2
        _refid: !refid "all"
        tag_name: "All"
  posts:
    config:
      depends: ["nothing"]
    rows:
      - post_id: 1
        title: "First post"
        tag_id: !expr "refid:tags:half:tag_id"
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
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: !expr generated
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
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: !expr generated
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

func TestResolveDefaultValues(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    config:
      default_values:
        tag_id: !expr generated
    rows:
      - tag_name: "Tag 1"
      - tag_name: "Tag 2"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}

	idct := 935

	retData, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		assert.DeepEqual(t, &ResolveGenerate{}, fields["tag_id"])
		ctx.ResolveField("tag_id", idct)
		idct++
		return nil
	})
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 2,
	}, rowCount)

	assert.Assert(t, is.Len(retData.Tables, 1))
	assert.Assert(t, is.Len(retData.Tables["tags"].Rows, 2))
	assert.Equal(t, 935, retData.Tables["tags"].Rows[0].Fields["tag_id"])
	assert.Equal(t, 936, retData.Tables["tags"].Rows[1].Fields["tag_id"])
}

func TestResolveDefaultValuesGenerated(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    config:
      default_values:
        tag_id: !expr generated
    rows:
      - tag_name: "All"
        _refid: !refid "all"
        deps:
          !deps
          posts:
            rows:
              - post_id: 1
                tag_id: !expr "parent:tag_id"
                title: "First post"
  post_tags:
    config:
      depends:
        - posts
    rows:
      - post_id: 1
        tag_id: !expr "refid:tags:all:tag_id"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	retData, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		if ctx.TableID() == "tags" {
			assert.DeepEqual(t, &ResolveGenerate{}, fields["tag_id"])
			ctx.ResolveField("tag_id", 99)
		}
		return nil
	})
	assert.NilError(t, err)

	assert.Assert(t, is.Len(retData.Tables["tags"].Rows, 1))
	assert.Assert(t, is.Len(retData.Tables["posts"].Rows, 1))
	assert.Assert(t, is.Len(retData.Tables["post_tags"].Rows, 1))
	assert.Equal(t, 99, retData.Tables["tags"].Rows[0].Fields["tag_id"])
	assert.Equal(t, 99, retData.Tables["posts"].Rows[0].Fields["tag_id"])
	assert.Equal(t, 99, retData.Tables["post_tags"].Rows[0].Fields["tag_id"])
}

func TestResolveDefaultValuesParentNotSupported(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 12
        tag_name: "All"
        deps:
          !deps
          posts:
            config:
              default_values:
                tag_id: !expr "parent:tag_id"
            rows:
              - post_id: 1
                title: "First post"
              - post_id: 2
                title: "Second post"
`),
		},
	})

	_, err := Load(provider)
	assert.ErrorContains(t, err, "parents not supported")
}

func TestResolveDefaultValuesRefID(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 19
        _refid: !refid "all"
        tag_name: "All"
  posts:
    config:
      depends:
        - posts
      default_values:
        tag_id: !expr "refid:tags:all:tag_id"
    rows:
      - post_id: 1
      - post_id: 2
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	retData, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		return nil
	})
	assert.NilError(t, err)

	assert.Assert(t, is.Len(retData.Tables["tags"].Rows, 1))
	assert.Assert(t, is.Len(retData.Tables["posts"].Rows, 2))
	assert.Equal(t, uint64(19), retData.Tables["posts"].Rows[0].Fields["tag_id"])
	assert.Equal(t, uint64(19), retData.Tables["posts"].Rows[1].Fields["tag_id"])
}

func TestResolveGeneratedWithType(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: !expr generated:int
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
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: !expr generated:myint32
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

func TestResolveCallback(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 1
        tag_name: !callback "a value"
`),
		},
	})

	data, err := Load(provider, WithLoadValueParser(&testValueCallback{}))
	assert.NilError(t, err)

	rowCount := map[string]int{}

	resolvedData, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		return nil
	})
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	assert.Equal(t, "v=a value", resolvedData.Tables["tags"].Rows[0].Fields["tag_name"])
}

func TestResolveCallbackNoAdd(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 1
        tag_name: !callback "a value"
`),
		},
	})

	data, err := Load(provider, WithLoadValueParser(&testValueCallbackNoAdd{}))
	assert.NilError(t, err)

	rowCount := map[string]int{}

	resolvedData, err := Resolve(data, func(ctx ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		return nil
	})
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	assert.Assert(t, resolvedData.Tables["tags"].Rows[0].Fields["tag_name"] == nil)
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

type testValueCalculatorInt32 struct {
	retVal int32
}

func (t testValueCalculatorInt32) CalculateValue(typ string, parameter string) (bool, any, error) {
	if typ != "myint32" {
		return false, nil, nil
	}
	return true, t.retVal, nil
}

type testValueCallback struct {
}

func (t *testValueCallback) ParseValue(tag *ast.TagNode) (bool, any, error) {
	if tag.Start.Value != "!callback" {
		return false, nil, nil
	}

	str, err := getStringNode(tag.Value)
	if err != nil {
		return false, nil, err
	}

	return true, ValueCallbackFunc(func(ctx ValueCallbackResolveContext) (any, bool, error) {
		return fmt.Sprintf("v=%s", str), true, nil
	}), nil
}

type testValueCallbackNoAdd struct {
}

func (t *testValueCallbackNoAdd) ParseValue(tag *ast.TagNode) (bool, any, error) {
	if tag.Start.Value != "!callback" {
		return false, nil, nil
	}
	return true, ValueCallbackFunc(func(ctx ValueCallbackResolveContext) (any, bool, error) {
		return nil, false, nil
	}), nil
}
