package value

import (
	"fmt"

	"github.com/goccy/go-yaml/ast"
	"github.com/google/uuid"
	"github.com/rrgmc/debefix"
)

// ValueParserUUID is a [debefix.TaggedValueParser] to parse "!uuid" tags to [uuid.UUID].
func ValueParserUUID() debefix.TaggedValueParser {
	return debefix.TaggedValueParserFunc(func(tag *ast.TagNode) (bool, any, error) {
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

// ResolvedValueParserUUID is a [debefix.ResolvedValueParser] to parse "uuid" type to [uuid.UUID].
type ResolvedValueParserUUID struct {
}

func (r ResolvedValueParserUUID) Parse(typ string, value any) (bool, any, error) {
	if typ != "uuid" {
		return false, nil, nil
	}

	switch vv := value.(type) {
	case uuid.UUID:
		return true, vv, nil
	default:
		v, err := uuid.Parse(fmt.Sprint(value))
		return true, v, err
	}
}
