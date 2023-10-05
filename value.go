package debefix

import (
	"errors"
	"fmt"
	"strings"

	"github.com/google/uuid"
)

// Value indicates a field value requires processing.
type Value interface {
	isValue()
}

// ValueRefID is a Value that references a field value in a table using the RefID (string ID).
type ValueRefID struct {
	TableID   string
	RefID     string
	FieldName string
}

// TableDepends indicates a dependency on another table.
func (v ValueRefID) TableDepends() string {
	return v.TableID
}

// ValueGenerated is a Value that will be generated in the future (possibly by a database).
type ValueGenerated struct {
}

// ValueInternalID is a Value that references a field value in a table using the internal ID.
type ValueInternalID struct {
	TableID    string
	InternalID uuid.UUID
	FieldName  string
}

// TableDepends indicates a dependency on another table.
func (v ValueInternalID) TableDepends() string {
	return v.TableID
}

func (v ValueRefID) isValue()      {}
func (v ValueGenerated) isValue()  {}
func (v ValueInternalID) isValue() {}

// valueTableDepends is an interface to indicate a Value adds a dependency on another table.
type valueTableDepends interface {
	TableDepends() string
}

// parseValue parses !dbfexpr expressions.
func parseValue(value string, parent parentRowInfo) (Value, error) {
	fields := strings.Split(value, ":")
	if len(fields) == 0 {
		return nil, errors.Join(ValueError, fmt.Errorf("invalid !dbf tag: %s", value))
	}

	switch fields[0] {
	case "refid": // refid:<table>:<refid>:<fieldname>
		if len(fields) != 4 {
			return nil, errors.Join(ValueError, fmt.Errorf("invalid !dbf tag value: %s", value))
		}
		return &ValueRefID{TableID: fields[1], RefID: fields[2], FieldName: fields[3]}, nil
	case "parent": // parent:<fieldname>
		if !parent.HasParent() {
			return nil, errors.Join(ValueError, errors.New("value has no parent"))
		}
		if len(fields) != 2 {
			return nil, errors.Join(ValueError, fmt.Errorf("invalid !dbf tag value: %s", value))
		}
		return &ValueInternalID{TableID: parent.TableID(), InternalID: parent.InternalID(), FieldName: fields[1]}, nil
	case "generated": // generated
		return &ValueGenerated{}, nil
	default:
		return nil, errors.Join(ValueError, fmt.Errorf("unknown !dbfexpr tag type: %s", value))
	}
}

// parentRowInfo indicates if a parent exists and its information.
type parentRowInfo interface {
	HasParent() bool
	TableID() string
	InternalID() uuid.UUID
}

// noParentRowInfo indicates that no parent exists in the current context.
type noParentRowInfo struct {
}

func (n noParentRowInfo) HasParent() bool {
	return false
}

func (n noParentRowInfo) TableID() string {
	return ""
}

func (n noParentRowInfo) InternalID() uuid.UUID {
	return uuid.UUID{}
}

// defaultParentRowInfo indicates a parent exists in the current context.
type defaultParentRowInfo struct {
	tableID    string
	internalID uuid.UUID
}

func (n defaultParentRowInfo) HasParent() bool {
	return true
}

func (n defaultParentRowInfo) TableID() string {
	return n.tableID
}

func (n defaultParentRowInfo) InternalID() uuid.UUID {
	return n.internalID
}
