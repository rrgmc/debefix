package debefix

import (
	"github.com/goccy/go-yaml/ast"
	"github.com/google/uuid"
)

// ValueParserUUID is a [TaggedValueParser] to parse "!uuid" tags to [uuid.UUID].
func ValueParserUUID() TaggedValueParser {
	return TaggedValueParserFunc(func(tag *ast.TagNode) (bool, any, error) {
		if tag.Start.Value != "!uuid" {
			return false, nil, nil
		}

		str, err := getStringNode(tag.Value)
		if err != nil {
			return false, nil, err
		}

		v, err := uuid.Parse(str)
		if err != nil {
			return false, nil, err
		}

		return true, v, nil
	})
}
