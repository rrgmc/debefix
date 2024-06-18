package debefix

import (
	"fmt"
	"maps"
	"slices"

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

// WalkTableRows calls a callback for each row in a table.
// Return false in the callback to stop walking.
func (d *Data) WalkTableRows(tableID string, f func(row Row) bool) {
	table, ok := d.Tables[tableID]
	if !ok {
		return
	}
	for _, row := range table.Rows {
		if cont := f(row); !cont {
			return
		}
	}
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
