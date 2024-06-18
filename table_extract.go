package debefix

import (
	"errors"
	"fmt"
	"strings"
)

// ExtractRows extract rows matched by the callback.
func (d *Data) ExtractRows(f func(table *Table, row Row) (bool, error)) (*Data, error) {
	data := &Data{
		Tables: map[string]*Table{},
	}
	var ferr error
	d.WalkRows(func(table *Table, row Row) bool {
		if ok, err := f(table, row); err != nil {
			ferr = err
			return false
		} else if ok {
			if _, hasTable := data.Tables[table.ID]; !hasTable {
				data.Tables[table.ID] = &Table{
					ID:     table.ID,
					Config: table.Config,
				}
			}
			data.Tables[table.ID].Rows = append(data.Tables[table.ID].Rows, row)
		}
		return true
	})
	if ferr != nil {
		return nil, ferr
	}
	return data, nil
}

// ExtractRowsRefID extract rows matched by a ValueRefID.[ValueRefID.FieldName] is ignored.
// The filter map key will be the key in the output map.
func (d *Data) ExtractRowsRefID(filter map[string]ValueRefID, options ...ExtractRefIDOption) (map[string]Row, error) {
	var optns extractRefIDOptions
	for _, option := range options {
		option(&optns)
	}

	ret := map[string]Row{}
	d.WalkRows(func(table *Table, row Row) bool {
		for fn, f := range filter {
			if RowMatchesRefID(table, row, f) {
				ret[fn] = row
			}
		}
		return true
	})
	if !optns.allowMissing && len(filter) != len(ret) {
		return nil, fmt.Errorf("some values were not found")
	}
	return ret, nil
}

// ExtractValuesRefID extract values matched by a ValueRefID.
// The filter map key will be the key in the output map.
func (d *Data) ExtractValuesRefID(filter map[string]ValueRefID, options ...ExtractRefIDOption) (map[string]any, error) {
	var optns extractRefIDOptions
	for _, option := range options {
		option(&optns)
	}

	ret := map[string]any{}
	var err error
	d.WalkRows(func(table *Table, row Row) bool {
		for fn, f := range filter {
			if RowMatchesRefID(table, row, f) {
				if fv, ok := row.Fields[f.FieldName]; ok {
					ret[fn] = fv
				} else {
					err = fmt.Errorf("could not find field %s in table %s", f.FieldName, table.ID)
					return false
				}
			}
		}
		return true
	})
	if err != nil {
		return nil, err
	}
	if !optns.allowMissing && len(filter) != len(ret) {
		return nil, fmt.Errorf("some values were not found")
	}
	return ret, nil
}

// ExtractRowsNamed extract rows matched by the callback into a named map.
func (d *Data) ExtractRowsNamed(f func(table *Table, row Row) (bool, string, error)) (map[string]Row, error) {
	ret := map[string]Row{}
	var ferr error
	d.WalkRows(func(table *Table, row Row) bool {
		if ok, rowName, err := f(table, row); err != nil {
			ferr = err
			return false
		} else if ok {
			ret[rowName] = row
		}
		return true
	})
	if ferr != nil {
		return nil, ferr
	}
	return ret, nil
}

// ExtractTableRows extract rows matched by the callback.
func (d *Data) ExtractTableRows(tableID string, f func(row Row) (bool, error)) (*Table, error) {
	data, err := d.ExtractRows(func(table *Table, row Row) (bool, error) {
		if table.ID != tableID {
			return false, nil
		}
		return f(row)
	})
	if err != nil {
		return nil, err
	}
	return data.Tables[tableID], nil
}

// ExtractTableRowsNamed extract rows matched by the callback into a named map.
func (d *Data) ExtractTableRowsNamed(tableID string, f func(row Row) (bool, string, error)) (map[string]Row, error) {
	return d.ExtractRowsNamed(func(table *Table, row Row) (bool, string, error) {
		if table.ID != tableID {
			return false, "", nil
		}
		return f(row)
	})
}

func (d *Data) ExtractValue(row Row, filter string) (any, error) {
	f, err := ParseExtractFilters(filter)
	if err != nil {
		return nil, err
	}
	return d.ExtractFilterValue(row, f[0])
}

func (d *Data) ExtractFilterValue(row Row, filter ExtractFilter) (any, error) {
	switch ft := filter.(type) {
	case *ExtractFilterValue:
		if fv, ok := row.Fields[ft.FieldName]; ok {
			return fv, nil
		}
		return nil, fmt.Errorf("unknown field '%s' in row", ft.FieldName)
	case *ExtractFilterRefID:
		fv, err := d.WalkTableData(ft.TableID, func(row Row) (bool, any, error) {
			if row.Config.RefID == ft.RefID {
				if rd, ok := row.Fields[ft.FieldName]; ok {
					return true, rd, nil
				}
				return false, nil, fmt.Errorf("unknown field '%s' in row", ft.FieldName)
			}
			return false, nil, nil
		})
		if err != nil {
			return nil, err
		}
		return fv, nil
	case *ExtractFilterValueRef:
		sourceValue, ok := row.Fields[ft.SourceFieldName]
		if !ok {
			return nil, fmt.Errorf("unknown field '%s' in row", ft.SourceFieldName)
		}

		fv, err := d.WalkTableData(ft.TableID, func(row Row) (bool, any, error) {
			targetValue, ok := row.Fields[ft.TargetFieldName]
			if !ok {
				return false, nil, nil
			}

			isEqual, err := valuesAreEqual(sourceValue, targetValue)
			if err != nil {
				return false, nil, err
			}

			if isEqual {
				if rd, ok := row.Fields[ft.ReturnFieldName]; ok {
					return true, rd, nil
				}
				return false, nil, fmt.Errorf("unknown field '%s' in target row", ft.ReturnFieldName)
			}
			return false, nil, nil
		})
		if err != nil {
			return nil, err
		}
		return fv, nil
	default:
		return nil, fmt.Errorf("unknown extract filter %T", filter)
	}
}

func (d *Data) ExtractValues(row Row, filters ...string) (map[string]any, error) {
	ret := map[string]any{}
	for _, filter := range filters {
		fv, err := d.ExtractValue(row, filter)
		if err != nil {
			return nil, err
		}
		ret[filter] = fv
	}
	return ret, nil
}

func RowMatchesRefID(table *Table, row Row, refID ValueRefID) bool {
	return row.Config.RefID != "" &&
		table.ID == refID.TableID &&
		row.Config.RefID == refID.RefID
}

// extract field filters

func ParseExtractFilters(filters ...string) ([]ExtractFilter, error) {
	var ret []ExtractFilter
	for _, filter := range filters {
		fields := strings.Split(filter, ":")
		if len(fields) == 0 {
			return nil, errors.Join(ValueError, fmt.Errorf("invalid filter: %s", filter))
		}

		switch fields[0] {
		case "value": // value:<fieldname>
			if len(fields) != 2 {
				return nil, errors.Join(ValueError, fmt.Errorf("invalid filter value: %s", filter))
			}
			ret = append(ret, &ExtractFilterValue{FieldName: fields[1]})
		case "refid": // refid:<table>:<refid>:<fieldname>
			if len(fields) != 4 {
				return nil, errors.Join(ValueError, fmt.Errorf("invalid filter value: %s", filter))
			}
			ret = append(ret, &ExtractFilterRefID{TableID: fields[1], RefID: fields[2], FieldName: fields[3]})
		case "valueref": // valueref:<source_fieldname>:<table>:<target_fieldname>:<return_fieldname>
			if len(fields) != 5 {
				return nil, errors.Join(ValueError, fmt.Errorf("invalid filter value: %s", filter))
			}
			ret = append(ret, &ExtractFilterValueRef{SourceFieldName: fields[1], TableID: fields[2], TargetFieldName: fields[3], ReturnFieldName: fields[4]})
		default:
			return nil, errors.Join(ValueError, fmt.Errorf("invalid filter value: %s", filter))
		}
	}
	return ret, nil
}

type ExtractFilter interface {
	isExtractFilter()
}

type ExtractFilterValue struct {
	FieldName string
}

type ExtractFilterRefID struct {
	TableID   string
	RefID     string
	FieldName string
}

type ExtractFilterValueRef struct {
	SourceFieldName string
	TableID         string
	TargetFieldName string
	ReturnFieldName string
}

func (ExtractFilterValue) isExtractFilter()    {}
func (ExtractFilterRefID) isExtractFilter()    {}
func (ExtractFilterValueRef) isExtractFilter() {}

// options

type extractRefIDOptions struct {
	allowMissing bool
}

type ExtractRefIDOption func(*extractRefIDOptions)

// WithExtractRefIDAllowMissing sets whether to allow one or more missing fields.
func WithExtractRefIDAllowMissing(allowMissing bool) ExtractRefIDOption {
	return func(o *extractRefIDOptions) {
		o.allowMissing = allowMissing
	}
}
