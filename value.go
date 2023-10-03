package debefix_poc2

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

type Value interface {
	isValue()
}

type valueTableDepends interface {
	TableDepends() string
}

type ValueRefID struct {
	Table     string
	ID        string
	FieldName string
}

func (v ValueRefID) TableDepends() string {
	return v.Table
}

type ValueGenerated struct {
}

type ValueInternalID struct {
	Table      string
	InternalID uuid.UUID
	FieldName  string
}

func (v ValueInternalID) TableDepends() string {
	return v.Table
}

func (v ValueRefID) isValue()      {}
func (v ValueGenerated) isValue()  {}
func (v ValueInternalID) isValue() {}

func parseValue(value string, parent parentRowInfo) (Value, error) {
	fields := strings.Split(value, ":")
	if len(fields) == 0 {
		return nil, fmt.Errorf("invalid !dbf tag: %s", value)
	}

	switch fields[0] {
	case "refid":
		if len(fields) != 4 {
			return nil, fmt.Errorf("invalid !dbf tag value: %s", value)
		}
		return &ValueRefID{Table: fields[1], ID: fields[2], FieldName: fields[3]}, nil
	case "parent":
		if !parent.HasParent() {
			return nil, errors.New("value has no parent")
		}
		if len(fields) != 2 {
			return nil, fmt.Errorf("invalid !dbf tag value: %s", value)
		}
		return &ValueInternalID{Table: parent.TableName(), InternalID: parent.InternalID(), FieldName: fields[1]}, nil
	case "generated":
		return &ValueGenerated{}, nil
	default:
		return nil, fmt.Errorf("unknown !dbf tag type: %s", value)
	}
}

type parentRowInfo interface {
	HasParent() bool
	TableName() string
	InternalID() uuid.UUID
}

type noParentRowInfo struct {
}

func (n noParentRowInfo) HasParent() bool {
	return false
}

func (n noParentRowInfo) TableName() string {
	return ""
}

func (n noParentRowInfo) InternalID() uuid.UUID {
	return uuid.UUID{}
}

type defaultParentRowInfo struct {
	tableName  string
	internalID uuid.UUID
}

func (n defaultParentRowInfo) HasParent() bool {
	return true
}

func (n defaultParentRowInfo) TableName() string {
	return n.tableName
}

func (n defaultParentRowInfo) InternalID() uuid.UUID {
	return n.internalID
}
