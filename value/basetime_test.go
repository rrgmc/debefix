package value

import (
	"testing"
	"testing/fstest"
	"time"

	"github.com/rrgmc/debefix"
	"gotest.tools/v3/assert"
)

func TestBaseTimeValueCalculated(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 12
        tag_name: "All"
        created_at: !expr "calculated:basetime:1d5h"
        updated_at: !expr "calculated:basetime:1d5h10m"
`),
		},
	})

	data, err := debefix.Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}

	baseTime := time.Date(2014, 3, 4, 10, 10, 12, 0, time.UTC)

	resolved, err := debefix.Resolve(data, func(ctx debefix.ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		return nil
	}, debefix.WithResolvedValueCalculator(NewValueBaseTime(baseTime)))
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	ctime, isTime := resolved.Tables["tags"].Rows[0].Fields["created_at"].(time.Time)
	assert.Assert(t, isTime, "field value should be of time.Time value")

	assert.Equal(t, 5, ctime.Day())
	assert.Equal(t, 15, ctime.Hour())
}

func TestBaseTimeValueCalculatedEmptyParameter(t *testing.T) {
	provider := debefix.NewFSFileProvider(fstest.MapFS{
		"users.dbf.yaml": &fstest.MapFile{
			Data: []byte(`tables:
  tags:
    rows:
      - tag_id: 12
        tag_name: "All"
        created_at: !expr "calculated:basetime"
`),
		},
	})

	data, err := debefix.Load(provider)
	assert.NilError(t, err)

	rowCount := map[string]int{}

	baseTime := time.Date(2014, 3, 4, 10, 10, 12, 0, time.UTC)

	resolved, err := debefix.Resolve(data, func(ctx debefix.ResolveContext, fields map[string]any) error {
		rowCount[ctx.TableID()]++
		return nil
	}, debefix.WithResolvedValueCalculator(NewValueBaseTime(baseTime)))
	assert.NilError(t, err)

	assert.DeepEqual(t, map[string]int{
		"tags": 1,
	}, rowCount)

	ctime, isTime := resolved.Tables["tags"].Rows[0].Fields["created_at"].(time.Time)
	assert.Assert(t, isTime, "field value should be of time.Time value")

	assert.Equal(t, 4, ctime.Day())
	assert.Equal(t, 10, ctime.Hour())
}
