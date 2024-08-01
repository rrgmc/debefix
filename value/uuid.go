package value

import (
	"fmt"

	"github.com/goccy/go-yaml/ast"
	"github.com/google/uuid"
)

// ValueUUID is a [debefix.ValueParser] to parse "!uuid" tags to [uuid.UUID], and a [debefix.ResolvedValueParser] to
// to parse "uuid" type to [uuid.UUID].
type ValueUUID struct{}

func (v ValueUUID) ParseValue(tag *ast.TagNode) (bool, any, error) {
	if tag.Start.Value != "!uuid" {
		return false, nil, nil
	}

	str, err := getStringNode(tag.Value)
	if err != nil {
		return false, nil, err
	}

	u, err := uuid.Parse(str)
	if err != nil {
		return false, nil, err
	}

	return true, u, nil
}

func (v ValueUUID) CalculateValue(typ string, parameter string) (bool, any, error) {
	if typ != "uuid" {
		return false, nil, nil
	}
	return true, uuid.New(), nil
}

func (v ValueUUID) ParseResolvedValue(typ string, value any) (bool, any, error) {
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
