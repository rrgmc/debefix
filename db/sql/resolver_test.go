package sql

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/rrgmc/debefix"
	"github.com/rrgmc/debefix/db"
	"gotest.tools/v3/assert"
)

func TestResolve(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    config:
      table_name: "public.tags"
    rows:
      - tag_id: 2
        _refid: !refid "all"
        tag_name: "All"
      - tag_id: 5
        _refid: !refid "half"
        tag_name: "Half"
  posts:
    config:
      table_name: "public.posts"
      depends: ["tags"]
    rows:
      - post_id: 1
        _refid: !refid "post_1"
        title: "First post"
      - post_id: 2
        _refid: !refid "post_2"
        title: "Second post"
  post_tags:
    config:
      table_name: "public.post_tags"
    rows:
      - post_id: !expr "refid:posts:post_1:post_id"
        tag_id: !expr "refid:tags:all:tag_id"
      - post_id: !expr "refid:posts:post_2:post_id"
        tag_id: !expr "refid:tags:half:tag_id"
`),
		},
	})

	data, err := debefix.Load(provider)
	assert.NilError(t, err)

	type sqlQuery struct {
		SQL  string
		Args []any
	}

	expectedQueryList := []sqlQuery{
		{
			SQL:  `INSERT INTO public.tags (tag_id, tag_name) VALUES (?, ?)`,
			Args: []any{uint64(2), "All"},
		},
		{
			SQL:  `INSERT INTO public.tags (tag_id, tag_name) VALUES (?, ?)`,
			Args: []any{uint64(5), "Half"},
		},
		{
			SQL:  `INSERT INTO public.posts (post_id, title) VALUES (?, ?)`,
			Args: []any{uint64(1), "First post"},
		},
		{
			SQL:  `INSERT INTO public.posts (post_id, title) VALUES (?, ?)`,
			Args: []any{uint64(2), "Second post"},
		},
		{
			SQL:  `INSERT INTO public.post_tags (post_id, tag_id) VALUES (?, ?)`,
			Args: []any{uint64(1), uint64(2)},
		},
		{
			SQL:  `INSERT INTO public.post_tags (post_id, tag_id) VALUES (?, ?)`,
			Args: []any{uint64(2), uint64(5)},
		},
	}

	var queryList []sqlQuery

	_, err = debefix.Resolve(data, db.ResolverFunc(ResolverDBCallback(context.Background(),
		QueryInterfaceFunc(func(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
			queryList = append(queryList, sqlQuery{
				SQL:  query,
				Args: args,
			})
			return nil, nil
		}), DefaultSQLBuilder{})))
	assert.NilError(t, err)

	assert.DeepEqual(t, expectedQueryList, queryList)
}

func TestResolveGenerated(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    config:
      table_name: "public.tags"
    rows:
      - tag_id: !expr "generated"
        _refid: !refid "all"
        tag_name: "All"
      - tag_id: !expr "generated"
        _refid: !refid "half"
        tag_name: "Half"
  posts:
    config:
      table_name: "public.posts"
      depends: ["tags"]
    rows:
      - post_id: 1
        _refid: !refid "post_1"
        title: "First post"
      - post_id: 2
        _refid: !refid "post_2"
        title: "Second post"
  post_tags:
    config:
      table_name: "public.post_tags"
    rows:
      - post_id: !expr "refid:posts:post_1:post_id"
        tag_id: !expr "refid:tags:all:tag_id"
      - post_id: !expr "refid:posts:post_2:post_id"
        tag_id: !expr "refid:tags:half:tag_id"
`),
		},
	})

	data, err := debefix.Load(provider)
	assert.NilError(t, err)

	type sqlQuery struct {
		SQL  string
		Args []any
	}

	expectedQueryList := []sqlQuery{
		{
			SQL:  `INSERT INTO public.tags (tag_name) VALUES (?) RETURNING tag_id`,
			Args: []any{"All"},
		},
		{
			SQL:  `INSERT INTO public.tags (tag_name) VALUES (?) RETURNING tag_id`,
			Args: []any{"Half"},
		},
		{
			SQL:  `INSERT INTO public.posts (post_id, title) VALUES (?, ?)`,
			Args: []any{uint64(1), "First post"},
		},
		{
			SQL:  `INSERT INTO public.posts (post_id, title) VALUES (?, ?)`,
			Args: []any{uint64(2), "Second post"},
		},
		{
			SQL:  `INSERT INTO public.post_tags (post_id, tag_id) VALUES (?, ?)`,
			Args: []any{uint64(1), uint64(116)},
		},
		{
			SQL:  `INSERT INTO public.post_tags (post_id, tag_id) VALUES (?, ?)`,
			Args: []any{uint64(2), uint64(117)},
		},
	}

	var queryList []sqlQuery

	retTagID := uint64(115)

	_, err = debefix.Resolve(data, db.ResolverFunc(ResolverDBCallback(context.Background(),
		QueryInterfaceFunc(func(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
			queryList = append(queryList, sqlQuery{
				SQL:  query,
				Args: args,
			})

			ret := map[string]any{}
			for _, rf := range returnFieldNames {
				if rf == "tag_id" {
					retTagID++
					ret["tag_id"] = retTagID
				}
			}

			return ret, nil
		}), DefaultSQLBuilder{})))
	assert.NilError(t, err)

	assert.DeepEqual(t, expectedQueryList, queryList)
}
