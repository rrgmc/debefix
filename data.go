package debefix

import (
	"errors"
	"fmt"

	"github.com/google/uuid"
)

// Data contains a list of tables and their row values.
type Data struct {
	Tables  map[string]*Table // map key is TableID.TableID()
	Updates []Update          // list of updates to be executed after all rows were added.
	err     error
}

// NewData creates a new Data instance.
func NewData() *Data {
	return &Data{
		Tables: make(map[string]*Table),
	}
}

// Err returns any error generated in processing.
func (d *Data) Err() error {
	return d.err
}

// Add adds a row to a table.
func (d *Data) Add(tableID TableID, value ValuesMutable, options ...DataAddOption) {
	_ = d.AddWithID(tableID, value, options...)
}

// AddValues adds a list of row to a table.
func (d *Data) AddValues(tableID TableID, values ...ValuesMutable) {
	for _, item := range values {
		_ = d.AddWithID(tableID, item)
	}
}

// AddWithID adds a row to a table, returning a reference to the added row.
func (d *Data) AddWithID(tableID TableID, values ValuesMutable, options ...DataAddOption) InternalIDRef {
	var optns dataAddOptions
	for _, opt := range options {
		opt(&optns)
	}

	if _, ok := d.Tables[tableID.TableID()]; !ok {
		d.Tables[tableID.TableID()] = &Table{
			TableID: tableID,
		}
	}
	row := d.newRow(d.Tables[tableID.TableID()], values)
	d.Tables[tableID.TableID()].Rows = append(d.Tables[tableID.TableID()].Rows, row)

	row.ResolvedCallbacks = optns.resolvedCallbacks

	return NewInternalIDRef(tableID, row.InternalID)
}

// Update adds an update do be done after all records were added.
func (d *Data) Update(query UpdateQuery, action UpdateAction) {
	d.Updates = append(d.Updates, Update{
		Query:  query,
		Action: action,
	})
}

// UpdateAfter will add an update to be done right after the "afterRow" record is added.
func (d *Data) UpdateAfter(afterRow QueryRow, query UpdateQuery, action UpdateAction) {
	row, err := afterRow.QueryRow(d)
	if err != nil {
		d.addError(fmt.Errorf("could not find row for UpdateAfter: %w", err))
		return
	}
	row.Row.Updates = append(row.Row.Updates, Update{
		Query:  query,
		Action: action,
	})
}

// AddDependencies adds dependencies on other tables to the passed table.
func (d *Data) AddDependencies(tableID TableID, dependencies ...TableID) {
	if _, ok := d.Tables[tableID.TableID()]; !ok {
		d.Tables[tableID.TableID()] = &Table{
			TableID: tableID,
		}
	}
	d.Tables[tableID.TableID()].AddDependencies(dependencies...)
}

func (d *Data) newRow(table *Table, values ValuesMutable) *Row {
	ret := &Row{
		InternalID: uuid.New(),
		Values:     values,
	}

	for vname, vvalue := range values.All {
		if vv, ok := vvalue.(SetValueRefIDData); ok {
			ret.RefID = vv.RefID
			ret.Values.Delete(vname)
		} else if vv, ok := vvalue.(*SetValueRefIDData); ok {
			ret.RefID = vv.RefID
			ret.Values.Delete(vname)
		}
		if vv, ok := vvalue.(ValueDependencies); ok {
			table.AddDependencies(vv.TableDependencies()...)
		}
	}

	return ret
}

// WalkRows calls the callback function for all tables and rows, until the callback return false.
func (d *Data) WalkRows(f func(TableID, *Row) bool) {
	for _, table := range d.Tables {
		for _, row := range table.Rows {
			cont := f(table.TableID, row)
			if !cont {
				return
			}
		}
	}
}

// WalkTableRows calls the callback function for all tables and rows, until the callback return false.
func (d *Data) WalkTableRows(tableID TableID, f func(*Row) bool) error {
	t, ok := d.Tables[tableID.TableID()]
	if !ok {
		return NewResolveErrorf("table %s not found", tableID)
	}

	for _, row := range t.Rows {
		cont := f(row)
		if !cont {
			return nil
		}
	}

	return nil
}

// FindTableRows returns a list of rows for a table where the callback returns true.
func (d *Data) FindTableRows(tableID TableID, f func(*Row) (bool, error)) (ret []*Row, err error) {
	walkErr := d.WalkTableRows(tableID, func(row *Row) bool {
		ok, ferr := f(row)
		if ferr != nil && !errors.Is(ferr, ResolveRowsStop) {
			err = ferr
			return false
		}
		if ok {
			ret = append(ret, row)
		}
		if errors.Is(ferr, ResolveRowsStop) {
			return false
		}
		return true
	})
	if walkErr != nil {
		return nil, walkErr
	}
	return
}

// FindTableRow returns one row for a table where the callback returns true.
// If no rows where found, returns the error ResolveNoRows.
func (d *Data) FindTableRow(tableID TableID, f func(*Row) (bool, error)) (*Row, error) {
	rows, err := d.FindTableRows(tableID, func(row *Row) (bool, error) {
		ok, err := f(row)
		if err != nil && !errors.Is(err, ResolveRowsStop) {
			return ok, err
		}
		if ok {
			err = ResolveRowsStop
		}
		return ok, err
	})
	if err != nil {
		return nil, err
	}
	if len(rows) > 0 {
		return rows[0], nil
	}
	return nil, ResolveNoRows
}

// FindTableRowValue uses FindTableRow to find a row, and returns one of its field's value.
// If no rows where found, returns the error ResolveNoRows.
func (d *Data) FindTableRowValue(tableID TableID, fieldName string, f func(*Row) (bool, error)) (any, error) {
	row, err := d.FindTableRow(tableID, f)
	if err != nil {
		return nil, err
	}
	fv, ok := row.Values.Get(fieldName)
	if !ok {
		return nil, ResolveUnknownFieldName
	}
	return fv, nil
}

// FindInternalIDRow returns a row of a table based on its internal id.
func (d *Data) FindInternalIDRow(tableID TableID, internalID uuid.UUID) (*Row, error) {
	row, err := d.FindTableRow(tableID, func(row *Row) (bool, error) {
		if row.InternalID == internalID {
			return true, nil
		}
		return false, nil
	})
	if err != nil && !errors.Is(err, ResolveNoRows) {
		return nil, err
	}
	if err == nil {
		return row, nil
	}
	return nil, NewResolveErrorf("internal ID %v not found in table '%s'", internalID, tableID)
}

// FindInternalIDValue resolves a field value referenced in a ValueInternalID.
func (d *Data) FindInternalIDValue(value ValueInternalIDData) (any, error) {
	row, err := d.FindInternalIDRow(value.TableID, value.InternalID)
	if err != nil {
		return nil, err
	}
	return row.ResolveFieldName(value.FieldName)
}

// FindRefIDRow returns a row from a table using a RefID.
func (d *Data) FindRefIDRow(tableID TableID, refID RefID) (*Row, error) {
	row, err := d.FindTableRow(tableID, func(row *Row) (bool, error) {
		if row.RefID == refID {
			return true, nil
		}
		return false, nil
	})
	if err != nil && !errors.Is(err, ResolveNoRows) {
		return nil, err
	}
	if err == nil {
		return row, nil
	}
	return nil, NewResolveErrorf("refID %v not found in table '%s'", refID, tableID)
}

// FindRefIDRowValue resolves a field value from a table using a RefID.
func (d *Data) FindRefIDRowValue(value ValueRefIDData) (any, error) {
	row, err := d.FindRefIDRow(value.TableID, value.RefID)
	if err != nil {
		return nil, err
	}
	return row.ResolveFieldName(value.FieldName)
}

func (d *Data) addError(err error) {
	d.err = errors.Join(d.err, err)
}

// DataAddOption are options for [Data.Add] and [Data.AddWithID].
type DataAddOption func(options *dataAddOptions)

// WithDataAddResolvedCallback adds a callback to be called when this row is resolved.
func WithDataAddResolvedCallback(f ResolvedCallback) DataAddOption {
	return func(options *dataAddOptions) {
		options.resolvedCallbacks = append(options.resolvedCallbacks, f)
	}
}

type dataAddOptions struct {
	resolvedCallbacks []ResolvedCallback
}
