package debefix

import (
	"fmt"

	"github.com/google/uuid"
)

// Data stores the entire collection of parsed Table information.
type Data struct {
	Tables map[string]*Table
}

type Table struct {
	ID     string
	Config TableConfig
	Rows   Rows
}

type TableConfig struct {
	TableName string   `yaml:"table_name"`
	Depends   []string `yaml:"depends"`
}

type Row struct {
	InternalID uuid.UUID
	Config     RowConfig
	Fields     map[string]any
}

type RowConfig struct {
	RefID string   `yaml:"refid"`
	Tags  []string `yaml:"tags"`
}

type Rows []Row

// AppendDeps adds table dependencies checking duplicates.
func (t *Table) AppendDeps(deps ...string) {
	t.Config.Depends = appendStringNoRepeat(t.Config.Depends, deps)
}

// Merge checks if merging is allowed before merging.
func (c *TableConfig) Merge(other *TableConfig) error {
	if other.TableName != "" {
		if c.TableName != "" && other.TableName != "" && c.TableName != other.TableName {
			return fmt.Errorf("table name value cannot be changed (current: %s, new: %s)", c.TableName, other.TableName)
		}
		c.TableName = other.TableName
	}

	if len(other.Depends) > 0 {
		c.Depends = appendStringNoRepeat(c.Depends, other.Depends)
	}

	return nil
}

// WalkTableData searches for rows in tables.
func (d *Data) WalkTableData(tableID string, f func(row Row) (bool, any, error)) (any, error) {
	vdb, ok := d.Tables[tableID]
	if !ok {
		return nil, fmt.Errorf("could not find table %s", tableID)
	}
	return vdb.WalkData(f)
}

// WalkData searches for rows in the table.
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
func (d *Data) WalkRows(f func(table *Table, row Row) bool) {
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
