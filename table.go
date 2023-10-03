package debefix_poc2

import (
	"fmt"

	"github.com/google/uuid"
)

type Data struct {
	Tables map[string]*Table
}

type Table struct {
	Name   string
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
	ID   string
	Tags []string
}

type Rows []Row

func (t *Table) AppendDeps(deps ...string) {
	t.Config.Depends = appendStringNoRepeat(t.Config.Depends, deps)
}

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
