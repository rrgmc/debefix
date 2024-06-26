package debefix

import (
	"cmp"
	"errors"
	"strconv"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/goccy/go-yaml/ast"
	"github.com/google/go-cmp/cmp/cmpopts"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

func TestLoad(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    config:
      table_name: "public.user"
    rows:
      - user_id: 1
        _refid: !refid "johndoe"
        name: "John Doe"
      - user_id: 2
        _refid: !refid "janedoe"
        name: "Jane Doe"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Equal(t, "public.user", usersTable.Config.TableName)

	assert.Assert(t, is.Len(usersTable.Rows, 2))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John Doe",
	}, usersTable.Rows[0].Fields)
	assert.Equal(t, "johndoe", usersTable.Rows[0].Config.RefID)

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(2),
		"name":    "Jane Doe",
	}, usersTable.Rows[1].Fields)
	assert.Equal(t, "janedoe", usersTable.Rows[1].Config.RefID)
}

func TestLoadInitialData(t *testing.T) {
	initialProvider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    config:
      table_name: "public.user"
    rows:
      - user_id: 1
        _refid: !refid "johndoe"
        name: "John Doe"
`),
		},
	})

	initialData, err := Load(initialProvider)
	assert.NilError(t, err)

	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    config:
      table_name: "public.user"
    rows:
      - user_id: 2
        _refid: !refid "janedoe"
        name: "Jane Doe"
`),
		},
	})

	data, err := Load(provider,
		WithLoadInitialData(initialData))
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Equal(t, "public.user", usersTable.Config.TableName)

	assert.Assert(t, is.Len(usersTable.Rows, 2))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John Doe",
	}, usersTable.Rows[0].Fields)
	assert.Equal(t, "johndoe", usersTable.Rows[0].Config.RefID)

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(2),
		"name":    "Jane Doe",
	}, usersTable.Rows[1].Fields)
	assert.Equal(t, "janedoe", usersTable.Rows[1].Config.RefID)
}

func TestLoad2TablesSameFile(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 1
        name: "John"
  tags:
    rows:
      - tag_id: 2
        tag_name: "All"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	tagsTable, ok := data.Tables["tags"]
	assert.Assert(t, ok, "tags table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[0].Fields)

	assert.Assert(t, is.Len(tagsTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"tag_id":   uint64(2),
		"tag_name": "All",
	}, tagsTable.Rows[0].Fields)
}

func TestLoad2TablesSeparateFiles(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 1
        name: "John"
`),
		},
		"tags.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 2
        tag_name: "All"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	tagsTable, ok := data.Tables["tags"]
	assert.Assert(t, ok, "tags table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[0].Fields)

	assert.Assert(t, is.Len(tagsTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"tag_id":   uint64(2),
		"tag_name": "All",
	}, tagsTable.Rows[0].Fields)
}

func TestLoadExtValueTypes(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 1
        name: "John Doe"
        attributes:
          gender: "male"
          age: "old"
        tags: ["carpenter", "office"]
      - user_id: 2
        name: "Jane Doe"
        attributes:
          gender: "female"
          age: "mid"
        tags: ["firefighter", "outdoors"]
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 2))

	assert.DeepEqual(t, map[string]any{
		"gender": "male",
		"age":    "old",
	}, usersTable.Rows[0].Fields["attributes"])
	assert.DeepEqual(t, []any{"carpenter", "office"}, usersTable.Rows[0].Fields["tags"])

	assert.DeepEqual(t, map[string]any{
		"gender": "female",
		"age":    "mid",
	}, usersTable.Rows[1].Fields["attributes"])
	assert.DeepEqual(t, []any{"firefighter", "outdoors"}, usersTable.Rows[1].Fields["tags"])
}

func TestLoadExprRefID(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"posts.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  posts:
    rows:
      - post_id: 1
        tag_id: !expr "refid:tags:all:tag_id"
        name: "John"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	postsTable, ok := data.Tables["posts"]
	assert.Assert(t, ok, "posts table not found")

	assert.Assert(t, is.Len(postsTable.Rows, 1))
	assert.DeepEqual(t,
		&ValueRefID{TableID: "tags", RefID: "all", FieldName: "tag_id"},
		postsTable.Rows[0].Fields["tag_id"])
}

func TestLoadDeps(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 2
        tag_name: "All"
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
        - tags
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	postsTable, ok := data.Tables["posts"]
	assert.Assert(t, ok, "posts table not found")

	tagsTable, ok := data.Tables["tags"]
	assert.Assert(t, ok, "tags table not found")

	postTagsTable, ok := data.Tables["post_tags"]
	assert.Assert(t, ok, "post_tags table not found")

	assert.Assert(t, is.Len(tagsTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"tag_id":   uint64(2),
		"tag_name": "All",
	}, tagsTable.Rows[0].Fields)

	assert.Assert(t, is.Len(postsTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"post_id": uint64(1),
		"tag_id":  &ValueInternalID{TableID: "tags", InternalID: tagsTable.Rows[0].InternalID, FieldName: "tag_id"},
		"title":   "First post",
	}, postsTable.Rows[0].Fields)

	assert.DeepEqual(t, []string{"tags"}, postsTable.Config.Depends)

	assert.DeepEqual(t, []string{"tags"}, postTagsTable.Config.Depends)
}

func TestLoadFileOrder(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"05-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 1
        name: "John"
`),
		},
		"04-users/10-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 10
        name: "Mary"
`),
		},
		"03-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 5
        name: "Jane"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 3))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(5),
		"name":    "Jane",
	}, usersTable.Rows[0].Fields)

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[1].Fields)

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(10),
		"name":    "Mary",
	}, usersTable.Rows[2].Fields)
}

func TestLoadTags(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"base/users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 1
        _tags: !tags ["first", "all"]
        name: "John Doe"
      - user_id: 2
        _tags: !tags ["second"]
        name: "Jane Doe"
`),
		},
	}, WithDirectoryAsTag())

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 2))
	assert.Assert(t, is.Len(usersTable.Rows[0].Config.Tags, 3))
	assert.Assert(t, is.Len(usersTable.Rows[1].Config.Tags, 2))
	assert.DeepEqual(t, usersTable.Rows[0].Config.Tags, []string{"base", "first", "all"},
		cmpopts.SortSlices(func(a, b string) bool {
			return strings.Compare(a, b) < 0
		}))
	assert.DeepEqual(t, usersTable.Rows[1].Config.Tags, []string{"base", "second"},
		cmpopts.SortSlices(func(a, b string) bool {
			return strings.Compare(a, b) < 0
		}))
}

func TestLoadInvalid(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    invalid:
      data: 2
`),
		},
	})

	_, err := Load(provider)

	assert.Assert(t, errors.As(err, &ParseError{}), "err (%T) = %v is not *ParseError", err, err)
}

func TestLoadInvalidRowType(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      data: 2
`),
		},
	})

	_, err := Load(provider)
	assert.Assert(t, errors.As(err, &ParseError{}), "err (%T) = %v is not *ParseError", err, err)
}

func TestLoadInvalidKeyType(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - 5: 10
`),
		},
	})

	_, err := Load(provider)
	assert.Assert(t, errors.As(err, &ParseError{}), "err (%T) = %v is not *ParseError", err, err)
}

func TestLoadRowSingleField(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 10
`),
		},
	})

	_, err := Load(provider)
	assert.NilError(t, err)
}

func TestLoadParentLevel(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 2
        tag_name: "All"
        deps:
          !deps
          posts:
            rows:
              - post_id: 1
                title: "First post"
                deps:
                  !deps
                  posts_tags:
                    rows:
                      - post_id: !expr "parent:post_id"
                        tag_id: !expr "parent:2:tag_id"
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	postsTable, ok := data.Tables["posts"]
	assert.Assert(t, ok, "posts table not found")

	tagsTable, ok := data.Tables["tags"]
	assert.Assert(t, ok, "tags table not found")

	postTagsTable, ok := data.Tables["posts_tags"]
	assert.Assert(t, ok, "post_tags table not found")

	assert.Assert(t, is.Len(tagsTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"tag_id":   uint64(2),
		"tag_name": "All",
	}, tagsTable.Rows[0].Fields)

	assert.Assert(t, is.Len(postsTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"post_id": uint64(1),
		"title":   "First post",
	}, postsTable.Rows[0].Fields)

	assert.Assert(t, is.Len(postTagsTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"post_id": &ValueInternalID{TableID: "posts", InternalID: postsTable.Rows[0].InternalID, FieldName: "post_id"},
		"tag_id":  &ValueInternalID{TableID: "tags", InternalID: tagsTable.Rows[0].InternalID, FieldName: "tag_id"},
	}, postTagsTable.Rows[0].Fields)

	assert.DeepEqual(t, []string{"posts", "tags"}, postTagsTable.Config.Depends)
}

func TestLoadNoParent(t *testing.T) {
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
        tag_id: !expr "parent:tag_id"
`),
		},
	})

	_, err := Load(provider)
	assert.ErrorIs(t, err, ValueError)
}

func TestLoadValueParser(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: !myint32 "95"
        name: !!str "John"
`),
		},
	})

	data, err := Load(provider,
		WithLoadValueParser(testParserInt32()))
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 1))

	assert.DeepEqual(t, map[string]any{
		"user_id": int32(95),
		"name":    "John",
	}, usersTable.Rows[0].Fields)
}

func TestLoadStringFileProvider(t *testing.T) {
	provider := NewStringFileProvider([]string{
		`tables:
  users:
    rows:
      - user_id: 1
        name: "John"
`,
		`tables:
  tags:
    rows:
      - tag_id: 2
        tag_name: "All"
`,
	}, WithStringFileProviderTags([][]string{
		{"a", "b"},
		{"c"},
	}))

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	tagsTable, ok := data.Tables["tags"]
	assert.Assert(t, ok, "tags table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[0].Fields)
	assert.DeepEqual(t, []string{"a", "b"}, usersTable.Rows[0].Config.Tags)

	assert.Assert(t, is.Len(tagsTable.Rows, 1))
	assert.DeepEqual(t, map[string]any{
		"tag_id":   uint64(2),
		"tag_name": "All",
	}, tagsTable.Rows[0].Fields)
	assert.DeepEqual(t, []string{"c"}, tagsTable.Rows[0].Config.Tags)
}

func testParserInt32() ValueParser {
	return ValueParserFunc(func(tag *ast.TagNode) (bool, any, error) {
		if tag.Start.Value != "!myint32" {
			return false, nil, nil
		}

		str, err := getStringNode(tag.Value)
		if err != nil {
			return false, nil, err
		}

		v, err := strconv.ParseInt(str, 10, 32)
		if err != nil {
			return false, nil, err
		}

		return true, int32(v), nil
	})
}

func TestLoadFileTags(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"05-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 1
        name: "John"
config:
  tags: ["u1", "u2"]
`),
		},
		"04-users/10-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 10
        name: "Mary"
config:
  tags: ["u3"]
`),
		},
		"03-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 5
        name: "Jane"
config:
  tags: ["u4", "u5", "u6"]
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 3))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(5),
		"name":    "Jane",
	}, usersTable.Rows[0].Fields)
	assert.DeepEqual(t, []string{"u4", "u5", "u6"}, usersTable.Rows[0].Config.Tags,
		cmpopts.SortSlices(cmp.Less[string]))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[1].Fields)
	assert.DeepEqual(t, []string{"u1", "u2"}, usersTable.Rows[1].Config.Tags,
		cmpopts.SortSlices(cmp.Less[string]))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(10),
		"name":    "Mary",
	}, usersTable.Rows[2].Fields)
	assert.DeepEqual(t, []string{"u3"}, usersTable.Rows[2].Config.Tags,
		cmpopts.SortSlices(cmp.Less[string]))
}

func TestLoadDirConfigTags(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"dbfconfig.yaml": &fstest.MapFile{
			Data: []byte(`config:
  tags: ["fldcfg-root-recursive"]
  local_tags: ["fldcfg-root"]
`),
		},
		"05-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 1
        name: "John"
config:
  tags: ["u1", "u2"]
`),
		},
		"04-users/dbfconfig.yaml": &fstest.MapFile{
			Data: []byte(`config:
  tags: ["fldcfg-4users"]
`),
		},
		"04-users/10-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 10
        name: "Mary"
config:
  tags: ["u3"]
`),
		},
		"03-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 5
        name: "Jane"
config:
  tags: ["u4", "u5", "u6"]
`),
		},
	})

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 3))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(5),
		"name":    "Jane",
	}, usersTable.Rows[0].Fields)
	assert.DeepEqual(t, []string{"u4", "u5", "u6", "fldcfg-root", "fldcfg-root-recursive"}, usersTable.Rows[0].Config.Tags,
		cmpopts.SortSlices(cmp.Less[string]))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[1].Fields)
	assert.DeepEqual(t, []string{"u1", "u2", "fldcfg-root", "fldcfg-root-recursive"}, usersTable.Rows[1].Config.Tags,
		cmpopts.SortSlices(cmp.Less[string]))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(10),
		"name":    "Mary",
	}, usersTable.Rows[2].Fields)
	assert.DeepEqual(t, []string{"u3", "fldcfg-4users", "fldcfg-root-recursive"}, usersTable.Rows[2].Config.Tags,
		cmpopts.SortSlices(cmp.Less[string]))
}

func TestLoadSkipDirConfig(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"dbfconfig.yaml": &fstest.MapFile{
			Data: []byte(`config:
  tags: ["fldcfg-root-recursive"]
  local_tags: ["fldcfg-root"]
`),
		},
		"05-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 1
        name: "John"
config:
  tags: ["u1", "u2"]
`),
		},
		"04-users/dbfconfig.yaml": &fstest.MapFile{
			Data: []byte(`config:
  tags: ["fldcfg-4users"]
`),
		},
		"04-users/10-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 10
        name: "Mary"
config:
  tags: ["u3"]
`),
		},
		"03-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  users:
    rows:
      - user_id: 5
        name: "Jane"
config:
  tags: ["u4", "u5", "u6"]
`),
		},
	}, WithSkipDirConfigFile())

	data, err := Load(provider)
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 3))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(5),
		"name":    "Jane",
	}, usersTable.Rows[0].Fields)
	assert.DeepEqual(t, []string{"u4", "u5", "u6"}, usersTable.Rows[0].Config.Tags,
		cmpopts.SortSlices(cmp.Less[string]))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[1].Fields)
	assert.DeepEqual(t, []string{"u1", "u2"}, usersTable.Rows[1].Config.Tags,
		cmpopts.SortSlices(cmp.Less[string]))

	assert.DeepEqual(t, map[string]any{
		"user_id": uint64(10),
		"name":    "Mary",
	}, usersTable.Rows[2].Fields)
	assert.DeepEqual(t, []string{"u3"}, usersTable.Rows[2].Config.Tags,
		cmpopts.SortSlices(cmp.Less[string]))
}
