package debefix

import (
	"context"
	"errors"
)

// UpdateQuery returns the list of rows that should be updated.
type UpdateQuery interface {
	Rows(ctx context.Context, resolvedData *ResolvedData) ([]UpdateData, error)
}

// UpdateAction do an update on a row returned by UpdateQuery.
type UpdateAction interface {
	UpdateRow(ctx context.Context, resolvedData *ResolvedData, tableID TableID, row *Row) error
}

// UpdateData represents one row of one table to be updated.
type UpdateData struct {
	TableID   TableID
	KeyFields []string
	Row       *Row
}

// Update is an updated query and its corresponding action.
type Update struct {
	Query  UpdateQuery
	Action UpdateAction
}

// UpdateQueryQueryRow wraps a QueryRow in an UpdateQuery.
type UpdateQueryQueryRow struct {
	QueryRow  QueryRow
	KeyFields []string
}

// UpdateQueryQueryRows wraps a QueryRows in an UpdateQuery.
type UpdateQueryQueryRows struct {
	QueryRows QueryRows
	KeyFields []string
}

// UpdateQueryRow wraps a QueryRow in an UpdateQuery.
func UpdateQueryRow(queryRow QueryRow, keyFields []string) *UpdateQueryQueryRow {
	return &UpdateQueryQueryRow{
		QueryRow:  queryRow,
		KeyFields: keyFields,
	}
}

// UpdateQueryRows wraps a QueryRows in an UpdateQuery.
func UpdateQueryRows(queryRows QueryRows, keyFields []string) *UpdateQueryQueryRows {
	return &UpdateQueryQueryRows{
		QueryRows: queryRows,
		KeyFields: keyFields,
	}
}

func (u UpdateQueryQueryRow) Rows(ctx context.Context, resolvedData *ResolvedData) ([]UpdateData, error) {
	row, err := u.QueryRow.QueryRow(&resolvedData.Data)
	if err != nil {
		if errors.Is(err, ErrNotFound) {
			return nil, nil
		}
		return nil, err
	}

	return []UpdateData{{
		TableID:   row.TableID,
		KeyFields: u.KeyFields,
		Row:       row.Row,
	}}, nil
}

func (u UpdateQueryQueryRows) Rows(ctx context.Context, resolvedData *ResolvedData) ([]UpdateData, error) {
	rows, err := u.QueryRows.QueryRows(&resolvedData.Data)
	if err != nil {
		return nil, err
	}

	var ret []UpdateData
	for _, row := range rows {
		ret = append(ret, UpdateData{
			TableID:   row.TableID,
			KeyFields: u.KeyFields,
			Row:       row.Row,
		})
	}

	return ret, nil
}

// UpdateActionSetValues is an update action which sets the field values in Values to the row being updated.
type UpdateActionSetValues struct {
	Values Values
}

func (u UpdateActionSetValues) UpdateRow(ctx context.Context, resolvedData *ResolvedData, tableID TableID, row *Row) error {
	for fieldName, fieldValue := range u.Values.All {
		row.Values.Set(fieldName, fieldValue)
	}
	return nil
}
