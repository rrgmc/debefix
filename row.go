package debefix

import "github.com/google/uuid"

// Row represents a single row of one table.
type Row struct {
	InternalID        uuid.UUID          // internal id that uniquely identifies this row in its table. It is randomly generated.
	RefID             RefID              // RefID of the row, if set. Should not be duplicated in any other row of the same table.
	Values            ValuesMutable      // the row field values.
	Updates           []Update           // updates to be done after the row is resolved.
	ResolvedCallbacks []ResolvedCallback // a callback called after the row is resolved.
}

// ResolveFieldName resolves the value of a field, or returns an error if the field don't exist.
func (r *Row) ResolveFieldName(fieldName string) (any, error) {
	fv, ok := r.Values.Get(fieldName)
	if !ok {
		return nil, NewResolveErrorf("field %s not found in resolved row", fieldName)
	}
	return fv, nil
}
