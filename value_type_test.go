package debefix

import (
	"context"
	"testing"

	"github.com/google/uuid"
	"gotest.tools/v3/assert"
)

func TestValueStatic(t *testing.T) {
	ctx := context.Background()

	expected := "test"

	rd := NewResolvedData()

	v := ValueStatic(expected)
	rv, rok, err := v.ResolveValue(ctx, rd, MapValues{})
	assert.NilError(t, err)
	assert.Assert(t, rok)
	assert.Equal(t, expected, rv)
}

func TestValueUUID(t *testing.T) {
	ctx := context.Background()

	expected := uuid.New()

	rd := NewResolvedData()

	v := ValueUUID(expected)
	rv, rok, err := v.ResolveValue(ctx, rd, MapValues{})
	assert.NilError(t, err)
	assert.Assert(t, rok)
	assert.Equal(t, expected, rv)
}

func TestValueUUIDRandom(t *testing.T) {
	ctx := context.Background()

	rd := NewResolvedData()

	v := ValueUUIDRandom()
	rv, rok, err := v.ResolveValue(ctx, rd, MapValues{})
	assert.NilError(t, err)
	assert.Assert(t, rok)
	rvu, isType := rv.(uuid.UUID)
	assert.Assert(t, isType && rvu != uuid.Nil)
}

func TestValueGenUUID(t *testing.T) {
	ctx := context.Background()

	rd := NewResolvedData()

	v := ValueGenUUID()
	rv, rok, err := v.ResolveValue(ctx, rd, MapValues{})
	assert.NilError(t, err)
	assert.Assert(t, rok)
	rvu, isType := rv.(uuid.UUID)
	assert.Assert(t, isType && rvu != uuid.Nil)
}
