package postgres

import (
	"context"
	"testing"
	"testing/fstest"

	"github.com/rrgmc/debefix"
	"github.com/rrgmc/debefix/db/sql"
	"gotest.tools/v3/assert"
)

func TestGenerate(t *testing.T) {
	providerData := fstest.MapFS{
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
`),
		},
	}

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
	}

	var queryList []sqlQuery

	_, err := Generate(context.Background(), debefix.NewFSFileProvider(providerData),
		sql.QueryInterfaceFunc(func(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
			queryList = append(queryList, sqlQuery{
				SQL:  query,
				Args: args,
			})
			return nil, nil
		}))
	assert.NilError(t, err)

	assert.DeepEqual(t, expectedQueryList, queryList)

	// same test using FS

	queryList = nil

	_, err = GenerateFS(context.Background(), providerData,
		sql.QueryInterfaceFunc(func(ctx context.Context, databaseName, tableName string, query string, returnFieldNames []string, args ...any) (map[string]any, error) {
			queryList = append(queryList, sqlQuery{
				SQL:  query,
				Args: args,
			})
			return nil, nil
		}))
	assert.NilError(t, err)

	assert.DeepEqual(t, expectedQueryList, queryList)

}
