package postgres

import (
	"testing"
	"testing/fstest"

	"github.com/rrgmc/debefix"
	"github.com/rrgmc/debefix/db/sql"
	"gotest.tools/v3/assert"
)

func TestGenerate(t *testing.T) {
	providerData := fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  config:
    table_name: "public.tags"
  rows:
    - tag_id: 2
      tag_name: "All"
      _dbfconfig:
        refid: "all"
    - tag_id: 5
      tag_name: "Half"
      _dbfconfig:
        refid: "half"
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

	_, err := Generate(debefix.NewFSFileProvider(providerData), sql.QueryInterfaceFunc(func(query string, returnFieldNames []string, args ...any) (map[string]any, error) {
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

	_, err = GenerateFS(providerData, sql.QueryInterfaceFunc(func(query string, returnFieldNames []string, args ...any) (map[string]any, error) {
		queryList = append(queryList, sqlQuery{
			SQL:  query,
			Args: args,
		})
		return nil, nil
	}))
	assert.NilError(t, err)

	assert.DeepEqual(t, expectedQueryList, queryList)

}
