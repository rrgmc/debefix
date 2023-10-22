package debefix

import (
	"errors"
	"strings"
	"testing"
	"testing/fstest"

	"github.com/google/go-cmp/cmp/cmpopts"
	"github.com/google/uuid"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

func TestLoad(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
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
			Data: []byte(`users:
  config:
    table_name: "public.user"
  rows:
    - user_id: 1
      name: "John Doe"
      _dbfconfig:
        refid: "johndoe"
`),
		},
	})

	initialData, err := Load(initialProvider)
	assert.NilError(t, err)

	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`users:
  config:
    table_name: "public.user"
  rows:
    - user_id: 2
      name: "Jane Doe"
      _dbfconfig:
        refid: "janedoe"
`),
		},
	})

	data, err := Load(provider, WithLoadInitialData(initialData))
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
			Data: []byte(`users:
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
			Data: []byte(`users:
  rows:
    - user_id: 1
      name: "John"
`),
		},
		"tags.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
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
			Data: []byte(`users:
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
			Data: []byte(`posts:
  rows:
    - post_id: 1
      tag_id: !dbfexpr "refid:tags:all:tag_id"
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
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      _dbfdeps:
        posts:
          rows:
            - post_id: 1
              tag_id: !dbfexpr "parent:tag_id"
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
			Data: []byte(`users:
  rows:
    - user_id: 1
      name: "John"
`),
		},
		"04-users/10-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`users:
  rows:
    - user_id: 10
      name: "Mary"
`),
		},
		"03-users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`users:
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
			Data: []byte(`users:
  rows:
    - user_id: 1
      name: "John Doe"
      _dbfconfig:
        tags: ["first", "all"]
    - user_id: 2
      name: "Jane Doe"
      _dbfconfig:
        tags: ["second"]
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
			Data: []byte(`users:
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
			Data: []byte(`users:
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
			Data: []byte(`users:
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
			Data: []byte(`users:
  rows:
    - user_id: 10
`),
		},
	})

	_, err := Load(provider)
	assert.NilError(t, err)
}

func TestLoadNoParent(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: 2
      tag_name: "All"
      _dbfconfig:
        refid: "all"
    - tag_id: 5
      tag_name: "Half"
posts:
  rows:
    - post_id: 1
      title: "First post"
      tag_id: !dbfexpr "parent:tag_id"
`),
		},
	})

	_, err := Load(provider)
	assert.ErrorIs(t, err, ValueError)
}

func TestLoadTaggedDataParser(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`users:
  rows:
    - user_id: !uuid "e850cd47-6a5d-4fc2-aed3-ca917b51577d"
      name: !!str "John"
`),
		},
	})

	data, err := Load(provider,
		WithLoadTaggedValueParser(ValueParserUUID()))
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 1))

	assert.DeepEqual(t, map[string]any{
		"user_id": uuid.MustParse("e850cd47-6a5d-4fc2-aed3-ca917b51577d"),
		"name":    "John",
	}, usersTable.Rows[0].Fields)
}
