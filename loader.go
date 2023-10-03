package debefix_poc2

import (
	"fmt"
	"io"
	"slices"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/google/uuid"
)

type loader struct {
	fileProvider FileProvider
	data         Data
}

func Load(fileProvider FileProvider) (*Data, error) {
	loader := &loader{
		fileProvider: fileProvider,
	}
	err := loader.load()
	if err != nil {
		return nil, err
	}
	return &loader.data, nil
}

func LoadDirectory(rootDir string, options ...DirectoryFileProviderOption) (*Data, error) {
	return Load(NewDirectoryFileProvider(rootDir, options...))
}

func (l *loader) load() error {
	return l.fileProvider.Load(func(info FileInfo) error {
		return l.loadFile(info.File, info.Tags)
	})
}

func (l *loader) loadFile(file io.Reader, tags []string) error {
	data, err := io.ReadAll(file)
	if err != nil {
		return err
	}

	fileParser, err := parser.ParseBytes(data, 0)
	if err != nil {
		return err
	}

	for _, doc := range fileParser.Docs {
		err := l.loadTables(doc.Body, tags, &noParentRowInfo{})
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *loader) loadTables(node ast.Node, tags []string, parent parentRowInfo) error {
	switch n := node.(type) {
	case *ast.MappingValueNode:
		tableName, err := getStringNode(n.Key)
		if err != nil {
			return err
		}
		err = l.loadTable(tableName, n.Value, tags, parent)
		if err != nil {
			return err
		}
	case *ast.MappingNode:
		for _, value := range n.Values {
			err := l.loadTables(value, tags, parent)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid file node '%s' at '%s'", n.Type().String(), n.GetPath())
	}

	return nil
}

func (l *loader) loadTable(tableName string, node ast.Node, tags []string, parent parentRowInfo) error {
	if l.data.Tables == nil {
		l.data.Tables = map[string]*Table{}
	}

	table, ok := l.data.Tables[tableName]
	if !ok {
		table = &Table{
			Name: tableName,
		}
		l.data.Tables[tableName] = table
	}

	var values []*ast.MappingValueNode
	switch n := node.(type) {
	case *ast.MappingNode:
		values = n.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{n}
	default:
		return fmt.Errorf("unknown table node at '%s'", node.GetPath())
	}

	for _, value := range values {
		key, err := getStringNode(value.Key)
		if err != nil {
			return fmt.Errorf("error getting table info for '%s': %w", tableName, err)
		}
		switch key {
		case "config":
			var cfg TableConfig
			err := yaml.NodeToValue(value.Value, &cfg)
			if err != nil {
				return fmt.Errorf("error reading table config for '%s': %w", tableName, err)
			}
			err = table.Config.Merge(&cfg)
			if err != nil {
				return fmt.Errorf("error merge table config for '%s': %w", tableName, err)
			}
		case "rows":
			err := l.loadTableRows(value.Value, table, tags, parent)
			if err != nil {
				return fmt.Errorf("error loading table rows for '%s': %w", tableName, err)
			}
		default:
			return fmt.Errorf("invalid table row data: '%s' at '%s' for '%s'", key, value.Path, tableName)
		}
	}
	return nil
}

func (l *loader) loadTableRows(node ast.Node, table *Table, tags []string, parent parentRowInfo) error {
	switch n := node.(type) {
	case *ast.SequenceNode:
		for _, row := range n.Values {
			err := l.loadTableRow(row, table, tags, parent)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid table rows node '%s' at '%s'", n.Type().String(), n.GetPath())
	}
	return nil
}

func (l *loader) loadTableRow(node ast.Node, table *Table, tags []string, parent parentRowInfo) error {
	switch n := node.(type) {
	case *ast.MappingNode:
		err := l.loadTableRowData(n, table, tags, parent)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid table row node '%s' at '%s'", n.Type().String(), n.GetPath())
	}
	return nil
}

func (l *loader) loadTableRowData(node *ast.MappingNode, table *Table, tags []string, parent parentRowInfo) error {
	row := Row{
		InternalID: uuid.New(),
		Config: RowConfig{
			Tags: slices.Clone(tags),
		},
		Fields: map[string]any{},
	}
	for _, field := range node.Values {
		key, err := getStringNode(field.Key)
		if err != nil {
			return err
		}
		if strings.HasPrefix(key, "_dbf") {
			switch key {
			case "_dbfconfig":
				err := yaml.NodeToValue(field.Value, &row.Config)
				if err != nil {
					return fmt.Errorf("error reading row config at '%s': %w", field.GetPath(), err)
				}
			case "_dbfdeps":
				err := l.loadTables(field.Value, tags, &defaultParentRowInfo{
					tableName:  table.Name,
					internalID: row.InternalID,
				})
				if err != nil {
					return fmt.Errorf("error reading row deps at '%s': %w", field.GetPath(), err)
				}
			default:
				return fmt.Errorf("invalid table row field: %s", key)
			}
		} else {
			fieldValue, err := l.loadFieldValue(field.Value, parent)
			if err != nil {
				return err
			}
			row.Fields[key] = fieldValue
			if fd, ok := fieldValue.(valueTableDepends); ok {
				table.AppendDeps(fd.TableDepends())
			}
		}
	}

	if len(tags) > 0 {
		row.Config.Tags = appendStringNoRepeat(row.Config.Tags, tags)
	}
	table.Rows = append(table.Rows, row)
	return nil
}

func (l *loader) loadFieldValue(node ast.Node, parent parentRowInfo) (any, error) {
	switch n := node.(type) {
	case *ast.TagNode:
		if strings.HasPrefix(n.Start.Value, "!dbf") {
			switch n.Start.Value {
			case "!dbfexpr":
				tvalue, err := getStringNode(n.Value)
				if err != nil {
					return nil, err
				}
				return parseValue(tvalue, parent)
			default:
				return nil, fmt.Errorf("unknown value tag: %s", n.Start.Value)
			}
		}
	}

	var value any
	err := yaml.NodeToValue(node, &value)
	if err != nil {
		return nil, err
	}
	return value, nil
}
