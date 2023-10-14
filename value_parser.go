package debefix

import (
	"github.com/goccy/go-yaml/ast"
)

// TaggedValueParser is used to parse YAML tag values.
type TaggedValueParser interface {
	Parse(tag *ast.TagNode) (bool, any, error)
}

// TaggedValueParserFunc is a func adapter for TaggedValueParser
type TaggedValueParserFunc func(tag *ast.TagNode) (bool, any, error)

func (p TaggedValueParserFunc) Parse(tag *ast.TagNode) (bool, any, error) {
	return p(tag)
}

// taggedValueParserList is a [TaggedValueParser] containing a list of named parsers.
type taggedValueParserList struct {
	list map[string]TaggedValueParser
}

// NewTaggedValueParserList creates a [TaggedValueParser] from a list of named parsers
func NewTaggedValueParserList(list map[string]TaggedValueParser) TaggedValueParser {
	return &taggedValueParserList{list: list}
}

func (t taggedValueParserList) Parse(tag *ast.TagNode) (bool, any, error) {
	if p, ok := t.list[tag.Start.Value]; !ok {
		return false, nil, nil
	} else {
		return p.Parse(tag)
	}
}
