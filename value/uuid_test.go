package value

import (
	"testing"
	"testing/fstest"

	"github.com/google/uuid"
	"github.com/rrgmc/debefix"
	"gotest.tools/v3/assert"
	is "gotest.tools/v3/assert/cmp"
)

func TestUUIDValue(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`users:
  rows:
    - user_id: !uuid "97d5a50d-d8ac-47b1-bbe1-daaa192f98ea"
      name: !!str "John"
`),
		},
	})

	data, err := debefix.Load(provider,
		debefix.WithLoadValueParser(ValueUUID{}))
	assert.NilError(t, err)

	usersTable, ok := data.Tables["users"]
	assert.Assert(t, ok, "users table not found")

	assert.Assert(t, is.Len(usersTable.Rows, 1))

	assert.DeepEqual(t, map[string]any{
		"user_id": uuid.MustParse("97d5a50d-d8ac-47b1-bbe1-daaa192f98ea"),
		"name":    "John",
	}, usersTable.Rows[0].Fields)
}

func TestUUIDValueResolved(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: !dbfexpr generated:uuid
      tag_name: "All"
`),
		},
	})

	data, err := debefix.Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}

	resolved, err := debefix.Resolve(data, func(ctx debefix.ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		assert.DeepEqual(t, &debefix.ResolveGenerate{Type: "uuid"}, fields["tag_id"])
		ctx.ResolveField("tag_id", "97d5a50d-d8ac-47b1-bbe1-daaa192f98ea")
		return nil
	}, debefix.WithResolvedValueParser(&ValueUUID{}))
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	assert.DeepEqual(t, map[string]any{
		"tag_id":   uuid.MustParse("97d5a50d-d8ac-47b1-bbe1-daaa192f98ea"),
		"tag_name": "All",
	}, resolved.Tables["tags"].Rows[0].Fields)
}

func TestUUIDValueResolvedConcreteType(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tags:
  rows:
    - tag_id: !dbfexpr generated:uuid
      tag_name: "All"
`),
		},
	})

	data, err := debefix.Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}

	resolved, err := debefix.Resolve(data, func(ctx debefix.ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		assert.DeepEqual(t, &debefix.ResolveGenerate{Type: "uuid"}, fields["tag_id"])
		ctx.ResolveField("tag_id", uuid.MustParse("97d5a50d-d8ac-47b1-bbe1-daaa192f98ea"))
		return nil
	}, debefix.WithResolvedValueParser(&ValueUUID{}))
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	assert.DeepEqual(t, map[string]any{
		"tag_id":   uuid.MustParse("97d5a50d-d8ac-47b1-bbe1-daaa192f98ea"),
		"tag_name": "All",
	}, resolved.Tables["tags"].Rows[0].Fields)
}
