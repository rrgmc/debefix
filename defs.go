package debefix

import "context"

// RefID is a string value that uniquely identifies one row of one table. It is meant to be a meaningful string to be
// easy to reference.
type RefID string

// Value represents a field value for a row that can be loaded at resolve time. It can use the current row values,
// or load any previously loaded rows.
type Value interface {
	ResolveValue(ctx context.Context, resolvedData *ResolvedData, values Values) (any, bool, error)
}

// ValueMultiple can set any amount of row field values (even none) at resolve time. It can use the current row values,
// or load any previously loaded rows.
type ValueMultiple interface {
	Resolve(ctx context.Context, resolvedData *ResolvedData, fieldName string, values ValuesMutable) error
}

// ValueDependencies lists value dependencies on other tables, allowing a dependency graph to be built to ensure
// inserts are done in the corrector table order.
type ValueDependencies interface {
	TableDependencies() []TableID
}
