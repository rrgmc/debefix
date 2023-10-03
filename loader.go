package debefix_poc2

import (
	"fmt"
	"io"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
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
		err := l.loadDoc(doc.Body, tags)
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *loader) loadDoc(node ast.Node, tags []string) error {
	switch n := node.(type) {
	case *ast.MappingValueNode:
		tableName, err := getStringNode(n.Key)
		if err != nil {
			return err
		}
		err = l.loadTable(tableName, n.Value, tags)
		//
		// switch tn := n.Value.(type) {
		// case *ast.MappingNode:
		// 	err = l.loadTable(tableName, tn, tags)
		// default:
		// 	err = fmt.Errorf("unknown table node at '%s'", node.GetPath())
		// }
		if err != nil {
			return err
		}
	case *ast.MappingNode:
		for _, value := range n.Values {
			err := l.loadDoc(value, tags)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid file node '%s' at '%s'", n.Type().String(), n.GetPath())
	}

	return nil
}

func (l *loader) loadTable(tableName string, node ast.Node, tags []string) error {
	if l.data.Tables == nil {
		l.data.Tables = map[string]*Table{}
	}

	table, ok := l.data.Tables[tableName]
	if !ok {
		table = &Table{}
		l.data.Tables[tableName] = table
	}

	table.Config.AppendTags(tags)

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
			err := l.loadTableRows(value.Value, table)
			if err != nil {
				return fmt.Errorf("error loading table rows for '%s': %w", tableName, err)
			}
		default:
			return fmt.Errorf("invalid table row data: '%s' at '%s' for '%s'", key, value.Path, tableName)
		}
	}
	return nil
}

func (l *loader) loadTableRows(node ast.Node, table *Table) error {
	switch n := node.(type) {
	case *ast.SequenceNode:
		for _, row := range n.Values {
			err := l.loadTableRow(row, table)
			if err != nil {
				return err
			}
		}
	default:
		return fmt.Errorf("invalid table rows node '%s' at '%s'", n.Type().String(), n.GetPath())
	}
	return nil
}

func (l *loader) loadTableRow(node ast.Node, table *Table) error {
	switch n := node.(type) {
	case *ast.MappingNode:
		err := l.loadTableRowData(n, table)
		if err != nil {
			return err
		}
	default:
		return fmt.Errorf("invalid table row node '%s' at '%s'", n.Type().String(), n.GetPath())
	}
	return nil
}

func (l *loader) loadTableRowData(node *ast.MappingNode, table *Table) error {
	row := Row{
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
			case "_dbfdeps":
			default:
				return fmt.Errorf("invalid table row field: %s", key)
			}
		} else {
			fieldValue, err := l.loadFieldValue(field.Value)
			if err != nil {
				return err
			}
			row.Fields[key] = fieldValue
		}
	}

	table.Rows = append(table.Rows, row)
	return nil
}

func (l *loader) loadFieldValue(node ast.Node) (any, error) {
	switch n := node.(type) {
	case *ast.TagNode:
		if strings.HasPrefix(n.Start.Value, "!dbf") {
			switch n.Start.Value {
			case "!dbfexpr":
				tvalue, err := getStringNode(n.Value)
				if err != nil {
					return nil, err
				}
				return ParseValue(tvalue)
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
