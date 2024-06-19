package debefix

import (
	"errors"
	"fmt"
	"strings"
)

// ExtractRows extract rows matched by the callback, returning a filtered Data instance.
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

// ExtractRowsNamed extract rows matched by the callback into a named map.
func (d *Data) ExtractRowsNamed(f func(table *Table, row Row) (add bool, name string, err error)) (map[string]Row, error) {
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

// ExtractTableRowsNamed extract rows matched by the callback into a named map.
func (d *Data) ExtractTableRowsNamed(tableID string, f func(row Row) (bool, string, error)) (map[string]Row, error) {
	return d.ExtractRowsNamed(func(table *Table, row Row) (bool, string, error) {
		if table.ID != tableID {
			return false, "", nil
		}
		return f(row)
	})
}

// ExtractValue extracts a field value based on a [ExtractFilter].
func (d *Data) ExtractValue(row Row, filter string) (any, error) {
	f, err := ParseExtractFilters(filter)
	if err != nil {
		return nil, err
	}
	return d.ExtractFilterValue(row, f[0])
}

// ExtractFilterValue extracts a field value based on a [ExtractFilter].
func (d *Data) ExtractFilterValue(row Row, filter ExtractFilter) (any, error) {
	switch ft := filter.(type) {
	case *ExtractFilterValue:
		if fv, ok := row.Fields[ft.FieldName]; ok {
			return fv, nil
		}
		if ft.DefaultValue != nil {
			return *ft.DefaultValue, nil
		}
		return nil, fmt.Errorf("unknown field '%s' in row", ft.FieldName)
	case *ExtractFilterMetadata:
		if fv, ok := row.Metadata[ft.FieldName]; ok {
			return fv, nil
		}
		if ft.DefaultValue != nil {
			return *ft.DefaultValue, nil
		}
		return nil, fmt.Errorf("unknown field '%s' in row metadata", ft.FieldName)
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

// ExtractValues extracts field values based on a list of [ExtractFilter].
func (d *Data) ExtractValues(row Row, filters map[string]string) (map[string]any, error) {
	ret := map[string]any{}
	for filterName, filter := range filters {
		fv, err := d.ExtractValue(row, filter)
		if err != nil {
			return nil, err
		}
		ret[filterName] = fv
	}
	return ret, nil
}

// extract field filters

// ParseExtractFilters parses a list of [ExtractFilter] filters.
func ParseExtractFilters(filters ...string) ([]ExtractFilter, error) {
	var ret []ExtractFilter
	for _, filter := range filters {
		fields := strings.Split(filter, ":")
		if len(fields) == 0 {
			return nil, errors.Join(ValueError, fmt.Errorf("invalid filter: %s", filter))
		}

		switch fields[0] {
		case "value": // value:<fieldname>[:defaultValue]
			if len(fields) < 2 || len(fields) > 3 {
				return nil, errors.Join(ValueError, fmt.Errorf("invalid filter value: %s", filter))
			}
			v := &ExtractFilterValue{FieldName: fields[1]}
			if len(fields) == 3 {
				v.DefaultValue = &fields[2]
			}
			ret = append(ret, v)
		case "metadata": // metadata:<fieldname>[:defaultValue]
			if len(fields) < 2 || len(fields) > 3 {
				return nil, errors.Join(ValueError, fmt.Errorf("invalid filter value: %s", filter))
			}
			v := &ExtractFilterMetadata{FieldName: fields[1]}
			if len(fields) == 3 {
				v.DefaultValue = &fields[2]
			}
			ret = append(ret, v)
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

// ExtractFilter is the base interface for filters that extract data.
type ExtractFilter interface {
	isExtractFilter()
}

// ExtractFilterValue has the format "value:<fieldname>[:defaultValue]"
type ExtractFilterValue struct {
	FieldName    string
	DefaultValue *string
}

// ExtractFilterMetadata has the format "metadata:<fieldname>[:defaultValue]"
type ExtractFilterMetadata struct {
	FieldName    string
	DefaultValue *string
}

// ExtractFilterRefID has the format "refid:<table>:<refid>:<fieldname>"
type ExtractFilterRefID struct {
	TableID   string
	RefID     string
	FieldName string
}

// ExtractFilterValueRef has the format "valueref:<source_fieldname>:<table>:<target_fieldname>:<return_fieldname>"
type ExtractFilterValueRef struct {
	SourceFieldName string
	TableID         string
	TargetFieldName string
	ReturnFieldName string
}

func (ExtractFilterValue) isExtractFilter()    {}
func (ExtractFilterMetadata) isExtractFilter() {}
func (ExtractFilterRefID) isExtractFilter()    {}
func (ExtractFilterValueRef) isExtractFilter() {}
