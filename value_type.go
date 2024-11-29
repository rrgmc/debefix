package debefix

import (
	"context"

	"github.com/google/uuid"
)

// ValueStaticData returns the passed static value every time it is resolved.
type ValueStaticData[T any] struct {
	Value T
}

// ValueStatic returns the passed static value every time it is resolved.
func ValueStatic[T any](value T) ValueStaticData[T] {
	return ValueStaticData[T]{Value: value}
}

var _ Value = (*ValueStaticData[any])(nil)

func (u ValueStaticData[T]) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	return u.Value, true, nil
}

// ValueUUIDData returns a fixed [uuid.UUID] value.
type ValueUUIDData struct {
	Value uuid.UUID
}

// ValueUUID returns a fixed [uuid.UUID] value.
func ValueUUID(value uuid.UUID) ValueUUIDData {
	return ValueUUIDData{
		Value: value,
	}
}

// ValueUUIDRandom returns a fixed [uuid.UUID] value that was randomly generated on initialization.
func ValueUUIDRandom() ValueUUIDData {
	return ValueUUIDData{
		Value: uuid.New(),
	}
}

var _ Value = (*ValueUUIDData)(nil)

func (u ValueUUIDData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	return u.Value, true, nil
}

// ValueGenUUIDData returns a new random [uuid.UUID] value every time it is resolved.
type ValueGenUUIDData struct {
}

// ValueGenUUID returns a new random [uuid.UUID] value every time it is resolved.
func ValueGenUUID() ValueGenUUIDData {
	return ValueGenUUIDData{}
}

var _ Value = (*ValueGenUUIDData)(nil)

func (u ValueGenUUIDData) ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error) {
	return uuid.New(), true, nil
}
