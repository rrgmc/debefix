package debefix

import (
	"fmt"

	"github.com/google/uuid"
)

// ResolvedValueParserUUID is a [ResolvedValueParser] to parse "uuid" type to [uuid.UUID].
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
