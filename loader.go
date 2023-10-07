package debefix

import (
	"fmt"
	"io"
	"strings"

	"github.com/goccy/go-yaml"
	"github.com/goccy/go-yaml/ast"
	"github.com/goccy/go-yaml/parser"
	"github.com/google/uuid"
)

// Load loads the files from the fileProvider and returns the list of loaded tables.
// Rows dependencies are not resolved, use ResolveCheck to check for them.
func Load(fileProvider FileProvider, options ...LoadOption) (*Data, error) {
	l := &loader{
		fileProvider: fileProvider,
	}
	for _, opt := range options {
		opt(l)
	}
	err := l.load()
	if err != nil {
		return nil, err
	}
	return &l.data, nil
}

type LoadOption func(l *loader)

// WithLoadProgress sets a callback to report load progress.
func WithLoadProgress(progress func(filename string)) LoadOption {
	return func(l *loader) {
		l.progress = progress
	}
}

type loader struct {
	fileProvider FileProvider
	data         Data
	progress     func(filename string)
}

func (l *loader) load() error {
	return l.fileProvider.Load(func(info FileInfo) error {
		if l.progress != nil {
			l.progress(info.Name)
		}
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
		tableID, err := getStringNode(n.Key)
		if err != nil {
			return err
		}
		err = l.loadTable(tableID, n.Value, tags, parent)
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
		return NewParseError(fmt.Sprintf("invalid table node '%s'", n.Type().String()),
			n.GetPath(), n.GetToken())
	}

	return nil
}

func (l *loader) loadTable(tableID string, node ast.Node, tags []string, parent parentRowInfo) error {
	if l.data.Tables == nil {
		l.data.Tables = map[string]*Table{}
	}

	table, ok := l.data.Tables[tableID]
	if !ok {
		table = &Table{
			ID: tableID,
		}
		l.data.Tables[tableID] = table
	}

	var values []*ast.MappingValueNode
	switch n := node.(type) {
	case *ast.MappingNode:
		values = n.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{n}
	default:
		return NewParseError(fmt.Sprintf("unknown table node type '%s'", node.Type().String()),
			node.GetPath(), node.GetToken())
	}

	for _, value := range values {
		key, err := getStringNode(value.Key)
		if err != nil {
			return NewParseError(fmt.Sprintf("error getting table info for '%s': %s", tableID, err),
				value.GetPath(), value.GetToken())
		}
		switch key {
		case "config":
			var cfg TableConfig
			err := yaml.NodeToValue(value.Value, &cfg)
			if err != nil {
				return NewParseError(fmt.Sprintf("error reading table config for '%s': %s", tableID, err),
					value.GetPath(), value.GetToken())
			}
			err = table.Config.Merge(&cfg)
			if err != nil {
				return NewParseError(fmt.Sprintf("error merge table config for '%s': %s", tableID, err),
					value.GetPath(), value.GetToken())
			}
		case "rows":
			err := l.loadTableRows(value.Value, table, tags, parent)
			if err != nil {
				return err
			}
		default:
			return NewParseError(fmt.Sprintf("unknown key in table row data: '%s' for '%s'", key, tableID),
				value.GetPath(), value.GetToken())
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
		return NewParseError(fmt.Sprintf("invalid table rows node '%s' (expected Sequence)", n.Type().String()),
			n.GetPath(), n.GetToken())
	}
	return nil
}

func (l *loader) loadTableRow(node ast.Node, table *Table, tags []string, parent parentRowInfo) error {
	var values []*ast.MappingValueNode
	switch n := node.(type) {
	case *ast.MappingNode:
		values = n.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{n}
	default:
		return NewParseError(fmt.Sprintf("unknown table row node type '%s' (expected Mapping)", node.Type().String()),
			node.GetPath(), node.GetToken())
	}

	row := Row{
		InternalID: uuid.New(),
		Fields:     map[string]any{},
	}
	for _, field := range values {
		key, err := getStringNode(field.Key)
		if err != nil {
			return err
		}
		if strings.HasPrefix(key, "_dbf") {
			switch key {
			case "_dbfconfig":
				err := yaml.NodeToValue(field.Value, &row.Config)
				if err != nil {
					return NewParseError(fmt.Sprintf("error reading row config: %s", err),
						field.GetPath(), field.GetToken())
				}
			case "_dbfdeps":
				err := l.loadTables(field.Value, tags, &defaultParentRowInfo{
					tableID:    table.ID,
					internalID: row.InternalID,
				})
				if err != nil {
					return NewParseError(fmt.Sprintf("error reading row deps: %s", err),
						field.GetPath(), field.GetToken())
				}
			default:
				return NewParseError(fmt.Sprintf("invalid table row field: %s", key),
					field.GetPath(), field.GetToken())
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
				return nil, NewParseError(fmt.Sprintf("unknown value tag: %s", n.Start.Value),
					n.GetPath(), n.GetToken())
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
