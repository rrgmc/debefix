package debefix

import (
	"context"
	"testing"

	"gotest.tools/v3/assert"
)

func TestValueTemplate(t *testing.T) {
	ctx := context.Background()

	expected := "test for other test (78 times)"

	rd := NewResolvedData()

	v := ValueTemplate("{{.data}} for {{.other}} ({{.rv}} times)", map[string]any{
		"data":  "test",
		"other": ValueStatic("other test"),
		"rv":    ValueFieldValue("row_value"),
	})
	rv, rok, err := v.ResolveValue(ctx, rd, MapValues{
		"row_value": 78,
	})
	assert.NilError(t, err)
	assert.Assert(t, rok)
	assert.Equal(t, expected, rv)
}

func TestValueTemplateNoField(t *testing.T) {
	ctx := context.Background()

	rd := NewResolvedData()

	v := ValueTemplate("{{.data}}", map[string]any{
		"xdata": "test",
	})
	_, _, err := v.ResolveValue(ctx, rd, MapValues{
		"row_value": 78,
	})
	AssertIsResolveError(t, err)

}
