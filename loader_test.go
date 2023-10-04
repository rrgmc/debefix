package debefix

import (
	"testing"
	"testing/fstest"

	"github.com/stretchr/testify/require"
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
        id: "johndoe"
    - user_id: 2
      name: "Jane Doe"
      _dbfconfig:
        id: "janedoe"
`),
		},
	})

	data, err := Load(provider)
	require.NoError(t, err)

	usersTable, ok := data.Tables["users"]
	require.True(t, ok, "users table not found")

	require.Equal(t, "public.user", usersTable.Config.TableName)

	require.Len(t, usersTable.Rows, 2)

	require.Equal(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John Doe",
	}, usersTable.Rows[0].Fields)
	require.Equal(t, "johndoe", usersTable.Rows[0].Config.ID)

	require.Equal(t, map[string]any{
		"user_id": uint64(2),
		"name":    "Jane Doe",
	}, usersTable.Rows[1].Fields)
	require.Equal(t, "janedoe", usersTable.Rows[1].Config.ID)
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
	require.NoError(t, err)

	usersTable, ok := data.Tables["users"]
	require.True(t, ok, "users table not found")

	tagsTable, ok := data.Tables["tags"]
	require.True(t, ok, "tags table not found")

	require.Len(t, usersTable.Rows, 1)
	require.Equal(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[0].Fields)

	require.Len(t, tagsTable.Rows, 1)
	require.Equal(t, map[string]any{
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
	require.NoError(t, err)

	usersTable, ok := data.Tables["users"]
	require.True(t, ok, "users table not found")

	tagsTable, ok := data.Tables["tags"]
	require.True(t, ok, "tags table not found")

	require.Len(t, usersTable.Rows, 1)
	require.Equal(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[0].Fields)

	require.Len(t, tagsTable.Rows, 1)
	require.Equal(t, map[string]any{
		"tag_id":   uint64(2),
		"tag_name": "All",
	}, tagsTable.Rows[0].Fields)
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
	require.NoError(t, err)

	postsTable, ok := data.Tables["posts"]
	require.True(t, ok, "posts table not found")

	require.Len(t, postsTable.Rows, 1)
	require.Equal(t,
		&ValueRefID{TableID: "tags", ID: "all", FieldName: "tag_id"},
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
	require.NoError(t, err)

	postsTable, ok := data.Tables["posts"]
	require.True(t, ok, "posts table not found")

	tagsTable, ok := data.Tables["tags"]
	require.True(t, ok, "tags table not found")

	postTagsTable, ok := data.Tables["post_tags"]
	require.True(t, ok, "post_tags table not found")

	require.Len(t, tagsTable.Rows, 1)
	require.Equal(t, map[string]any{
		"tag_id":   uint64(2),
		"tag_name": "All",
	}, tagsTable.Rows[0].Fields)

	require.Len(t, postsTable.Rows, 1)
	require.Equal(t, map[string]any{
		"post_id": uint64(1),
		"tag_id":  &ValueInternalID{TableID: "tags", InternalID: tagsTable.Rows[0].InternalID, FieldName: "tag_id"},
		"title":   "First post",
	}, postsTable.Rows[0].Fields)

	require.Equal(t, []string{"tags"}, postsTable.Config.Depends)

	require.Equal(t, []string{"tags"}, postTagsTable.Config.Depends)
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
	require.NoError(t, err)

	usersTable, ok := data.Tables["users"]
	require.True(t, ok, "users table not found")

	require.Len(t, usersTable.Rows, 3)

	require.Equal(t, map[string]any{
		"user_id": uint64(5),
		"name":    "Jane",
	}, usersTable.Rows[0].Fields)

	require.Equal(t, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	}, usersTable.Rows[1].Fields)

	require.Equal(t, map[string]any{
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
	require.NoError(t, err)

	usersTable, ok := data.Tables["users"]
	require.True(t, ok, "users table not found")

	require.Len(t, usersTable.Rows, 2)
	require.Len(t, usersTable.Rows[0].Config.Tags, 3)
	require.Len(t, usersTable.Rows[1].Config.Tags, 2)
	require.Subset(t, usersTable.Rows[0].Config.Tags, []string{"base", "first", "all"})
	require.Subset(t, usersTable.Rows[1].Config.Tags, []string{"base", "second"})
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
	require.Error(t, err)
}

func TestLoadInvalidType(t *testing.T) {
	provider := NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`users:
  rows:
    data: 2
`),
		},
	})

	_, err := Load(provider)
	require.Error(t, err)
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
	require.Error(t, err)
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
	require.NoError(t, err)
}
