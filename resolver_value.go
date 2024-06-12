package debefix

import "fmt"

// ResolveValue indicates a field value that must be resolved.
type ResolveValue interface {
	isResolveValue()
}

// ResolveGenerate is a [ResolveValue] that indicates a value will be generated and must be returned.
type ResolveGenerate struct {
	Type string
}

func (r ResolveGenerate) isResolveValue() {}

// ResolveContext is the context used to resolve values.
type ResolveContext interface {
	TableID() string
	DatabaseName() string
	TableName() string
	ResolveField(fieldName string, value any)
}

// ResolvedValueParser parses resolved value types, like generated fields.
type ResolvedValueParser interface {
	ParseResolvedValue(typ string, value any) (bool, any, error)
}

// ResolvedValueParserFunc is a func wrapper for [ResolvedValueParser].
type ResolvedValueParserFunc func(typ string, value any) (bool, any, error)

func (p ResolvedValueParserFunc) ParseResolvedValue(typ string, value any) (bool, any, error) {
	return p(typ, value)
}

type defaultResolveContext struct {
	tableID, databaseName, tableName string
	resolved                         map[string]any
}

func (d *defaultResolveContext) TableID() string {
	return d.tableID
}

func (d *defaultResolveContext) TableName() string {
	return d.tableName
}

func (d *defaultResolveContext) DatabaseName() string {
	return d.databaseName
}

func (d *defaultResolveContext) ResolveField(fieldName string, value any) {
	if d.resolved == nil {
		d.resolved = map[string]any{}
	}
	d.resolved[fieldName] = value
}

func DefaultParseResolvedValue(typ string, value any) (bool, any, error) {
	switch typ {
	case "int":
		v, err := castToInt(value)
		return true, v, err
	case "float":
		v, err := castToFloat(value)
		return true, v, err
	case "str":
		return true, fmt.Sprint(value), nil
	case "timestamp":
		v, err := castToTime(value)
		return true, v, err
	}
	return false, nil, nil
}
