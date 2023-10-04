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
  rows:
    - user_id: 1
      name: "John"
`),
		},
	})

	data, err := Load(provider)
	require.NoError(t, err)

	usersTable, ok := data.Tables["users"]
	require.True(t, ok, "users table not found")

	require.Len(t, usersTable.Rows, 1)
	require.Equal(t, usersTable.Rows[0].Fields, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	})
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
      tag_name: "all"
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
	require.Equal(t, usersTable.Rows[0].Fields, map[string]any{
		"user_id": uint64(1),
		"name":    "John",
	})

	require.Len(t, tagsTable.Rows, 1)
	require.Equal(t, tagsTable.Rows[0].Fields, map[string]any{
		"tag_id":   uint64(2),
		"tag_name": "all",
	})
}
