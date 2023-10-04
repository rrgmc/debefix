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
