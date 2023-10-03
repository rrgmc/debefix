package debefix_poc2

import (
	"fmt"
	"slices"
)

type Data struct {
	Tables map[string]*Table
}

type Table struct {
	Config TableConfig
	Rows   Rows
}

type TableConfig struct {
	Key       string   `yaml:"key"`
	TableName string   `yaml:"table_name"`
	Tags      []string `yaml:"tags"`
}

type Row struct {
	Config RowConfig
	Fields map[string]any
}

type RowConfig struct {
	ID   string
	Tags []string
}

type Rows []Row

func (c *TableConfig) Merge(other *TableConfig) error {
	if other.Key != "" {
		if c.Key != "" && other.Key != "" && c.Key != other.Key {
			return fmt.Errorf("table key value cannot be changed (current: %s, new: %s", c.Key, other.Key)
		}
		c.Key = other.Key
	}

	if other.TableName != "" {
		if c.TableName != "" && other.TableName != "" && c.TableName != other.TableName {
			return fmt.Errorf("table name value cannot be changed (current: %s, new: %s", c.TableName, other.TableName)
		}
		c.TableName = other.TableName
	}

	c.AppendTags(other.Tags)

	return nil
}

func (c *TableConfig) AppendTags(tags []string) {
	for _, tag := range tags {
		if !slices.Contains(c.Tags, tag) {
			c.Tags = append(c.Tags, tag)
		}
	}
}
