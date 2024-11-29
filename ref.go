package debefix

import (
	"github.com/google/uuid"
)

// InternalIDRef is a reference to one table's row by internal id.
// It refers to an entire row instead of a specific field, so it don't implement Value. Use ValueForField to return
// a Value implementation for one of its fields.
// It implements QueryRow, and can return a UpdateQuery.
type InternalIDRef struct {
	notAValue
	TableID    TableID
	InternalID uuid.UUID
}

func NewInternalIDRef(tableID TableID, internalID uuid.UUID) InternalIDRef {
	return InternalIDRef{
		TableID:    tableID,
		InternalID: internalID,
	}
}

var _ QueryRow = (*InternalIDRef)(nil)

// ValueForField returns a Value that resolves one specific field of the referenced table row.
func (v InternalIDRef) ValueForField(fieldName string) ValueInternalIDData {
	return ValueInternalID(v.TableID, v.InternalID, fieldName)
}

// QueryRow is the implementation of the QueryRow interface.
func (v InternalIDRef) QueryRow(data *Data) (QueryRowResult, error) {
	row, err := data.FindInternalIDRow(v.TableID, v.InternalID)
	if err != nil {
		return QueryRowResult{}, err
	}
	return QueryRowResult{TableID: v.TableID, Row: row}, nil
}

// UpdateQuery returns an UpdateQuery targetting the referenced table row.
func (v InternalIDRef) UpdateQuery(keyFields []string) UpdateQuery {
	return UpdateQueryRow(v, keyFields)
}
