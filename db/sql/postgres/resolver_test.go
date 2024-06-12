package postgres

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/rrgmc/debefix"
	"github.com/rrgmc/debefix/db/sql"
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
        tag_name: "All"
        config:
          !dbfconfig
          refid: "all"
      - tag_id: 5
        tag_name: "Half"
        config:
          !dbfconfig
          refid: "half"
  posts:
    config:
      table_name: "public.posts"
      depends: ["tags"]
    rows:
      - post_id: 1
        title: "First post"
        config:
          !dbfconfig
          refid: "post_1"
      - post_id: 2
        title: "Second post"
        config:
          !dbfconfig
          refid: "post_2"
  post_tags:
    config:
      table_name: "public.post_tags"
    rows:
      - post_id: !dbfexpr "refid:posts:post_1:post_id"
        tag_id: !dbfexpr "refid:tags:all:tag_id"
      - post_id: !dbfexpr "refid:posts:post_2:post_id"
        tag_id: !dbfexpr "refid:tags:half:tag_id"
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
			SQL:  `INSERT INTO "public.tags" ("tag_id", "tag_name") VALUES ($1, $2)`,
			Args: []any{uint64(2), "All"},
		},
		{
			SQL:  `INSERT INTO "public.tags" ("tag_id", "tag_name") VALUES ($1, $2)`,
			Args: []any{uint64(5), "Half"},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "title") VALUES ($1, $2)`,
			Args: []any{uint64(1), "First post"},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "title") VALUES ($1, $2)`,
			Args: []any{uint64(2), "Second post"},
		},
		{
			SQL:  `INSERT INTO "public.post_tags" ("post_id", "tag_id") VALUES ($1, $2)`,
			Args: []any{uint64(1), uint64(2)},
		},
		{
			SQL:  `INSERT INTO "public.post_tags" ("post_id", "tag_id") VALUES ($1, $2)`,
			Args: []any{uint64(2), uint64(5)},
		},
	}

	var queryList []sqlQuery

	_, err = debefix.Resolve(data, ResolverFunc(context.Background(),
		sql.QueryInterfaceFunc(func(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
			queryList = append(queryList, sqlQuery{
				SQL:  query,
				Args: args,
			})
			return nil, nil
		})))
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
      - tag_id: !dbfexpr "generated"
        tag_name: "All"
        config:
          !dbfconfig
          refid: "all"
      - tag_id: !dbfexpr "generated"
        tag_name: "Half"
        config:
          !dbfconfig
          refid: "half"
  posts:
    config:
      table_name: "public.posts"
      depends: ["tags"]
    rows:
      - post_id: 1
        title: "First post"
        config:
          !dbfconfig
          refid: "post_1"
      - post_id: 2
        title: "Second post"
        config:
          !dbfconfig
          refid: "post_2"
  post_tags:
    config:
      table_name: "public.post_tags"
    rows:
      - post_id: !dbfexpr "refid:posts:post_1:post_id"
        tag_id: !dbfexpr "refid:tags:all:tag_id"
      - post_id: !dbfexpr "refid:posts:post_2:post_id"
        tag_id: !dbfexpr "refid:tags:half:tag_id"
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
			SQL:  `INSERT INTO "public.tags" ("tag_name") VALUES ($1) RETURNING "tag_id"`,
			Args: []any{"All"},
		},
		{
			SQL:  `INSERT INTO "public.tags" ("tag_name") VALUES ($1) RETURNING "tag_id"`,
			Args: []any{"Half"},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "title") VALUES ($1, $2)`,
			Args: []any{uint64(1), "First post"},
		},
		{
			SQL:  `INSERT INTO "public.posts" ("post_id", "title") VALUES ($1, $2)`,
			Args: []any{uint64(2), "Second post"},
		},
		{
			SQL:  `INSERT INTO "public.post_tags" ("post_id", "tag_id") VALUES ($1, $2)`,
			Args: []any{uint64(1), uint64(116)},
		},
		{
			SQL:  `INSERT INTO "public.post_tags" ("post_id", "tag_id") VALUES ($1, $2)`,
			Args: []any{uint64(2), uint64(117)},
		},
	}

	var queryList []sqlQuery

	retTagID := uint64(115)

	_, err = debefix.Resolve(data, ResolverFunc(context.Background(),
		sql.QueryInterfaceFunc(func(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
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
		})))
	assert.NilError(t, err)

	assert.DeepEqual(t, expectedQueryList, queryList)
}
