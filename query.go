package debefix

import (
	"github.com/google/uuid"
)

// QueryRowResult represents a row and its table.
type QueryRowResult struct {
	TableID TableID
	Row     *Row
}

type QueryRow interface {
	// QueryRow returns a single Row, or ErrNotFound if none found.
	QueryRow(data *Data) (QueryRowResult, error)
}

type QueryRows interface {
	// QueryRows returns a list of rows.
	QueryRows(data *Data) ([]QueryRowResult, error)
}

// implementations

// QueryRowInternalID finds one row by a table and an internal id.
type QueryRowInternalID struct {
	TableID    TableID
	InternalID uuid.UUID
}

func NewQueryRowInternalID(tableID TableID, internalID uuid.UUID) QueryRowInternalID {
	return QueryRowInternalID{
		TableID:    tableID,
		InternalID: internalID,
	}
}

func (u QueryRowInternalID) Row(data *Data) (QueryRowResult, error) {
	row, err := data.FindInternalIDRow(u.TableID, u.InternalID)
	if err != nil {
		return QueryRowResult{}, err
	}
	return QueryRowResult{TableID: u.TableID, Row: row}, nil
}

// QueryRowRefID finds one row by a table and a RefID.
type QueryRowRefID struct {
	TableID TableID
	RefID   RefID
}

func NewQueryRowRefID(tableID TableID, refID RefID) QueryRowRefID {
	return QueryRowRefID{
		TableID: tableID,
		RefID:   refID,
	}
}

func (u QueryRowRefID) Row(data *Data) (QueryRowResult, error) {
	row, err := data.FindRefIDRow(u.TableID, u.RefID)
	if err != nil {
		return QueryRowResult{}, err
	}
	return QueryRowResult{TableID: u.TableID, Row: row}, nil
}
