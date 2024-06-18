package debefix

import (
	"errors"
	"fmt"
	"maps"
	"slices"
	"strings"

	"github.com/google/uuid"
)

// FileConfig stores configs that are specific to a yaml file.
type FileConfig struct {
	Tags []string `yaml:"tags"`
}

// Data stores the entire collection of parsed Table information.
type Data struct {
	Tables map[string]*Table
}

// Merge merges source into the current instance. A deep copy is done to ensure source is never modified.
func (d *Data) Merge(source *Data) error {
	if source.Tables == nil {
		return nil
	}
	if d.Tables == nil {
		d.Tables = map[string]*Table{}
	}

	for sourceTableID, sourceTable := range source.Tables {
		table, ok := d.Tables[sourceTableID]
		if !ok {
			table = &Table{
				ID: sourceTable.ID,
			}
			d.Tables[sourceTableID] = table
		}
		err := table.Merge(sourceTable)
		if err != nil {
			return err
		}
	}
	return nil
}

// Clone creates a deep-copy of the source. The source [Data] is never modified.
func (d *Data) Clone() (*Data, error) {
	newd := &Data{}
	err := newd.Merge(d)
	if err != nil {
		return nil, err
	}
	return newd, nil
}

type Table struct {
	ID     string
	Config TableConfig
	Rows   Rows
}

// Merge merges source into d. A deep copy is done to ensure source is never modified.
func (t *Table) Merge(source *Table) error {
	if source.ID != "" && t.ID == "" {
		t.ID = source.ID
	} else if source.ID == "" && t.ID != "" {
		// do nothing, can't change source
	} else if source.ID != t.ID {
		return fmt.Errorf("table IDs don't match (%s - %s)", source.ID, t.ID)
	}

	err := t.Config.Merge(&source.Config)
	if err != nil {
		return err
	}

	for _, sourceRow := range source.Rows {
		t.Rows = append(t.Rows, sourceRow.Clone())
	}
	return nil
}

type TableConfig struct {
	DatabaseName  string         `yaml:"database_name"`
	TableName     string         `yaml:"table_name"`
	Depends       []string       `yaml:"depends"`
	DefaultValues map[string]any `yaml:"default_values"`
}

type Row struct {
	InternalID uuid.UUID
	Config     RowConfig
	Fields     map[string]any
	Metadata   map[string]any
}

// Clone does a deep copy of the row, to ensure source is never modified.
func (r Row) Clone() Row {
	return Row{
		InternalID: r.InternalID,
		Config:     r.Config.Clone(),
		Fields:     maps.Clone(r.Fields),
		Metadata:   maps.Clone(r.Metadata),
	}
}

type RowConfig struct {
	RefID      string   `yaml:"refid"`
	Tags       []string `yaml:"tags"`
	IgnoreTags bool     `yaml:"ignoreTags"` // if true, always include row ignoring any tag filter.
}

func (r RowConfig) Clone() RowConfig {
	return RowConfig{
		RefID:      r.RefID,
		Tags:       slices.Clone(r.Tags),
		IgnoreTags: r.IgnoreTags,
	}
}

type Rows []Row

// AppendDeps adds table dependencies checking duplicates.
func (t *Table) AppendDeps(deps ...string) {
	t.Config.Depends = appendStringNoRepeat(t.Config.Depends, deps)
}

// Merge checks if merging is allowed before merging.
func (c *TableConfig) Merge(other *TableConfig) error {
	if other.DatabaseName != "" {
		if c.DatabaseName != "" && other.DatabaseName != "" && c.DatabaseName != other.DatabaseName {
			return fmt.Errorf("database name value cannot be changed (current: %s, new: %s)", c.DatabaseName, other.DatabaseName)
		}
	}
	if other.TableName != "" {
		if c.TableName != "" && other.TableName != "" && c.TableName != other.TableName {
			return fmt.Errorf("table name value cannot be changed (current: %s, new: %s)", c.TableName, other.TableName)
		}
	}

	if other.DatabaseName != "" {
		c.DatabaseName = other.DatabaseName
	}
	if other.TableName != "" {
		c.TableName = other.TableName
	}

	if len(other.Depends) > 0 {
		c.Depends = appendStringNoRepeat(c.Depends, other.Depends)
	}
	if len(other.DefaultValues) > 0 {
		if c.DefaultValues == nil {
			c.DefaultValues = maps.Clone(other.DefaultValues)
		} else {
			maps.Copy(c.DefaultValues, other.DefaultValues)
		}
	}

	return nil
}

// WalkTableData searches for a single row value in tables.
func (d *Data) WalkTableData(tableID string, f func(row Row) (bool, any, error)) (any, error) {
	if d.Tables == nil {
		return nil, fmt.Errorf("could not find table %s", tableID)
	}
	vdb, ok := d.Tables[tableID]
	if !ok {
		return nil, fmt.Errorf("could not find table %s", tableID)
	}
	return vdb.WalkData(f)
}

// WalkData searches for a single row value in the table.
func (t *Table) WalkData(f func(row Row) (bool, any, error)) (any, error) {
	for _, vrow := range t.Rows {
		ok, v, err := f(vrow)
		if err != nil {
			return nil, err
		}
		if ok {
			return v, nil
		}
	}
	return nil, RowNotFound
}

// WalkRows calls a callback for each row in each table.
// Return false in the callback to stop walking.
func (d *Data) WalkRows(f func(table *Table, row Row) bool) {
	if d.Tables == nil {
		return
	}
	for _, table := range d.Tables {
		for _, row := range table.Rows {
			if cont := f(table, row); !cont {
				return
			}
		}
	}
}

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

// MergeData merge a list of [Data] objects into a new instance.
// The data is deep-copied, the source [Data] instances are never modified in any way.
func MergeData(list ...*Data) (*Data, error) {
	retData := &Data{
		Tables: map[string]*Table{},
	}
	for _, data := range list {
		err := retData.Merge(data)
		if err != nil {
			return nil, err
		}
	}
	return retData, nil
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
