package debefix_poc2

import (
	"fmt"
	"strings"
)

type Value interface {
	isValue()
}

type ValueRefID struct {
	Table string
	ID    string
}

type valueParent struct {
	FieldName string
}

func (v ValueRefID) isValue()  {}
func (v valueParent) isValue() {}

func ParseValue(value string) (Value, error) {
	fields := strings.Split(value, ":")
	if len(fields) == 0 {
		return nil, fmt.Errorf("invalid !dbf tag: %s", value)
	}

	switch fields[0] {
	case "refid":
		if len(fields) != 3 {
			return nil, fmt.Errorf("invalid !dbf tag: %s", value)
		}
		return &ValueRefID{Table: fields[1], ID: fields[2]}, nil
	case "parent":
		if len(fields) != 2 {
			return nil, fmt.Errorf("invalid !dbf tag: %s", value)
		}
		return &valueParent{FieldName: fields[1]}, nil
	default:
		return nil, fmt.Errorf("unknown !dbf tag type: %s", value)
	}
}
