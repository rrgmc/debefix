package debefix

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

// Load loads the files from the fileProvider and returns the list of loaded tables.
// Rows dependencies are not resolved, use [ResolveCheck] to check for them.
func Load(fileProvider FileProvider, options ...LoadOption) (*Data, error) {
	l := &loader{
		fileProvider: fileProvider,
	}
	for _, opt := range options {
		opt.apply(l)
	}
	err := l.load()
	if err != nil {
		return nil, err
	}
	return l.data, nil
}

// WithLoadInitialData sets the initial data. Can be used to merge more items into an existing data.
// The instance WILL be modified, call [Data.Clone] if you want to load into a copy.
func WithLoadInitialData(initialData *Data) LoadOption {
	return fnLoadOption(func(l *loader) {
		l.data = initialData
	})
}

// WithLoadProgress sets a callback to report load progress.
func WithLoadProgress(progress func(filename string)) LoadOption {
	return fnLoadOption(func(l *loader) {
		l.progress = progress
	})
}

// WithLoadValueParser adds a YAML tag value parser.
func WithLoadValueParser(parser ValueParser) LoadOption {
	return fnLoadOption(func(l *loader) {
		l.valueParser = append(l.valueParser, parser)
	})
}

// WithLoadRowsSetIgnoreTags sets "IgnoreTags" on all rows loaded.
// This is mainly used in tests to be sure the rows will be included.
func WithLoadRowsSetIgnoreTags(rowsSetIgnoreTags bool) LoadOption {
	return fnLoadOption(func(l *loader) {
		l.rowsSetIgnoreTags = rowsSetIgnoreTags
	})
}

type loader struct {
	fileProvider      FileProvider
	data              *Data
	progress          func(filename string)
	valueParser       []ValueParser
	rowsSetIgnoreTags bool
}

func (l *loader) load() error {
	if l.data == nil {
		l.data = &Data{}
	}
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
		err := l.loadRoot(doc.Body, tags, &noParentRowInfo{})
		if err != nil {
			return err
		}
	}

	return nil
}

func (l *loader) loadRoot(node ast.Node, tags []string, parent parentRowInfo) error {
	var values []*ast.MappingValueNode
	switch n := node.(type) {
	case *ast.MappingNode:
		values = n.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{n}
	default:
		return NewParseError(fmt.Sprintf("unknown root node type '%s' (expected Mapping)", node.Type().String()),
			node.GetPath(), node.GetToken().Position)
	}

	// load config first
	for _, field := range values {
		key, err := getStringNode(field.Key)
		if err != nil {
			return err
		}

		switch key {
		case "config":
			var fc FileConfig
			err := yaml.NodeToValue(field.Value, &fc)
			if err != nil {
				return NewParseError(fmt.Sprintf("error reading file config: %s", err),
					field.Value.GetPath(), field.Value.GetToken().Position)
			}
			tags = slices.Concat(tags, fc.Tags)
		}
	}

	// load tables
	for _, field := range values {
		key, err := getStringNode(field.Key)
		if err != nil {
			return err
		}

		switch key {
		case "config":
			// already loaded
		case "tables":
			err := l.loadTables(field.Value, tags, parent)
			if err != nil {
				return err
			}
		default:
			return NewParseError(fmt.Sprintf("unknown root field: %s", key),
				field.Value.GetPath(), field.Value.GetToken().Position)
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
			n.GetPath(), n.GetToken().Position)
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
			node.GetPath(), node.GetToken().Position)
	}

	for _, value := range values {
		key, err := getStringNode(value.Key)
		if err != nil {
			return NewParseError(fmt.Sprintf("error getting table info for '%s': %s", tableID, err),
				value.GetPath(), value.GetToken().Position)
		}
		switch key {
		case "config":
			// load in a separate struct, so we can merge fields later
			var cfg TableConfig
			err = l.loadTableConfig(value.Value, table, &cfg)
			if err != nil {
				return err
			}
			err = table.Config.Merge(&cfg)
			if err != nil {
				return NewParseError(fmt.Sprintf("error merge table config for '%s': %s", tableID, err),
					value.GetPath(), value.GetToken().Position)
			}
		case "rows":
			err := l.loadTableRows(value.Value, table, tags, parent)
			if err != nil {
				return err
			}
		default:
			return NewParseError(fmt.Sprintf("unknown key in table row data: '%s' for '%s'", key, tableID),
				value.GetPath(), value.GetToken().Position)
		}
	}
	return nil
}

func (l *loader) loadTableConfig(node ast.Node, table *Table, cfg *TableConfig) error {
	var values []*ast.MappingValueNode
	switch n := node.(type) {
	case *ast.MappingNode:
		values = n.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{n}
	default:
		return NewParseError(fmt.Sprintf("unknown table config node type '%s' (expected Mapping)", node.Type().String()),
			node.GetPath(), node.GetToken().Position)
	}

	for _, field := range values {
		key, err := getStringNode(field.Key)
		if err != nil {
			return err
		}

		switch key {
		case "table_name":
			err := yaml.NodeToValue(field.Value, &cfg.TableName)
			if err != nil {
				return NewParseError(fmt.Sprintf("error reading table config: %s", err),
					field.Value.GetPath(), field.Value.GetToken().Position)
			}
		case "database_name":
			err := yaml.NodeToValue(field.Value, &cfg.DatabaseName)
			if err != nil {
				return NewParseError(fmt.Sprintf("error reading table config: %s", err),
					field.Value.GetPath(), field.Value.GetToken().Position)
			}
		case "depends":
			err := yaml.NodeToValue(field.Value, &cfg.Depends)
			if err != nil {
				return NewParseError(fmt.Sprintf("error reading table config: %s", err),
					field.Value.GetPath(), field.Value.GetToken().Position)
			}
		case "default_values":
			err := l.loadTableConfigDefaultValues(field.Value, table, cfg)
			if err != nil {
				return err
			}
		default:
			return NewParseError(fmt.Sprintf("unknown table config field: %s", key),
				field.Value.GetPath(), field.Value.GetToken().Position)
		}
	}

	return nil
}

func (l *loader) loadTableConfigDefaultValues(node ast.Node, table *Table, cfg *TableConfig) error {
	var values []*ast.MappingValueNode
	switch n := node.(type) {
	case *ast.MappingNode:
		values = n.Values
	case *ast.MappingValueNode:
		values = []*ast.MappingValueNode{n}
	default:
		return NewParseError(fmt.Sprintf("unknown table config default value node type '%s' (expected Mapping)", node.Type().String()),
			node.GetPath(), node.GetToken().Position)
	}

	if len(values) == 0 {
		return nil
	}

	if cfg.DefaultValues == nil {
		cfg.DefaultValues = map[string]any{}
	}

	for _, field := range values {
		key, err := getStringNode(field.Key)
		if err != nil {
			return err
		}

		fieldValue, err := l.loadFieldValue(field.Value, unsupportedParentRowInfo{})
		if err != nil {
			return err
		}
		cfg.DefaultValues[key] = fieldValue
		if fd, ok := fieldValue.(valueTableDepends); ok {
			table.AppendDeps(fd.TableDepends())
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
			n.GetPath(), n.GetToken().Position)
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
			node.GetPath(), node.GetToken().Position)
	}

	row := Row{
		InternalID: uuid.New(),
		Fields:     map[string]any{},
	}
	for _, field := range values {
		switch n := field.Value.(type) {
		case *ast.TagNode:
			if strings.HasPrefix(n.Start.Value, "!dbf") {
				switch n.Start.Value {
				case "!dbfrefid":
					refid, err := getStringNode(n.Value)
					if err != nil {
						return NewParseError(fmt.Sprintf("error reading row refid: %s", err),
							field.GetPath(), field.GetToken().Position)
					}
					row.Config.RefID = refid
					continue
				case "!dbftags":
					rowTags, err := getStringListNode(n.Value)
					if err != nil {
						return NewParseError(fmt.Sprintf("error reading row refid: %s", err),
							field.GetPath(), field.GetToken().Position)
					}
					row.Config.Tags = rowTags
					continue
				case "!dbfdeps":
					err := l.loadTables(n.Value, tags, &defaultParentRowInfo{
						parent: parent,
						data: &defaultParentRowInfoData{
							tableID:    table.ID,
							internalID: row.InternalID,
						},
					})
					if err != nil {
						return NewParseError(fmt.Sprintf("error reading row deps: %s", err),
							field.GetPath(), field.GetToken().Position)
					}
					continue
				}
			}
		}

		key, err := getStringNode(field.Key)
		if err != nil {
			return err
		}

		fieldValue, err := l.loadFieldValue(field.Value, parent)
		if err != nil {
			return err
		}
		row.Fields[key] = fieldValue
		if fd, ok := fieldValue.(valueTableDepends); ok {
			table.AppendDeps(fd.TableDepends())
		}
	}
	if l.rowsSetIgnoreTags {
		row.Config.IgnoreTags = true
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
					n.GetPath(), n.GetToken().Position)
			}
		}

		if !strings.HasPrefix(n.Start.Value, "!!") { // !! is reserved by YAML.
			if len(l.valueParser) > 0 {
				for _, valueParser := range l.valueParser {
					ok, tvalue, err := valueParser.ParseValue(n)
					if err != nil {
						return nil, err
					} else if ok {
						return tvalue, nil
					}
				}
			}

			return nil, NewParseError(fmt.Sprintf("unknown value tag: %s", n.Start.Value),
				n.GetPath(), n.GetToken().Position)
		}
	}

	var value any
	err := yaml.NodeToValue(node, &value)
	if err != nil {
		return nil, err
	}
	return value, nil
}
