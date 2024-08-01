package debefix

import (
	"github.com/goccy/go-yaml/ast"
)

// ValueParser is used to parse YAML tag values.
type ValueParser interface {
	ParseValue(tag *ast.TagNode) (bool, any, error)
}

// ValueCalculator is used to calculate a value.
type ValueCalculator interface {
	CalculateValue(typ string, parameter string) (bool, any, error)
}

// ValueParserFunc is a func adapter for ValueParser
type ValueParserFunc func(tag *ast.TagNode) (bool, any, error)

func (p ValueParserFunc) ParseValue(tag *ast.TagNode) (bool, any, error) {
	return p(tag)
}
