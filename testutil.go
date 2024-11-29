package debefix

import (
	"errors"
	"maps"
	"testing"

	"gotest.tools/v3/assert"
)

// AssertValuesDeepEqual asserts that a map is deep-equal to a Values.
func AssertValuesDeepEqual(t *testing.T, x map[string]any, y Values) {
	assert.DeepEqual(t, x, maps.Collect(y.All))
}

// AssertRowValuesDeepEqual asserts that a slice of maps is deep-equal to a list of Row.
func AssertRowValuesDeepEqual(t *testing.T, x []map[string]any, y []*Row) {
	var ym []map[string]any
	for _, row := range y {
		ym = append(ym, maps.Collect(row.Values.All))
	}
	assert.DeepEqual(t, x, ym)
}

// AssertIsResolveError assets that the error is a ResolveError.
func AssertIsResolveError(t *testing.T, err error) {
	var re *ResolveError
	ok := errors.As(err, &re)
	assert.Assert(t, ok, "expected ResolveError, got %T", err)
}
